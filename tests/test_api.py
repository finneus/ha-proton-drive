"""Tests for the upload progress estimate in ProtonDriveClient."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from proton_drive import api as api_module
from proton_drive.api import ProtonDriveClient, ProtonDriveError, ProtonDriveInvalidBackupError, _upload_progress
from proton_drive.cli import CLIError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

SIZE = 1000


def test_upload_progress_counts_only_the_second_read_pass() -> None:
    """The CLI reads the archive twice (hash, then upload): only bytes beyond the file size are upload progress."""
    reported: list[int] = []
    on_io = _upload_progress(SIZE, lambda *, bytes_uploaded: reported.append(bytes_uploaded))

    on_io(400)  # hashing pass
    on_io(SIZE)  # hash done, upload not started
    assert reported == []

    on_io(SIZE + 400)  # 40 % of the upload read
    assert reported == [SIZE // 2 + 200]


def test_upload_progress_never_goes_backwards_and_stays_below_done() -> None:
    """A retry restarts the counters; the bar must not drop, and 100 % is left to the real end of the upload."""
    reported: list[int] = []
    on_io = _upload_progress(SIZE, lambda *, bytes_uploaded: reported.append(bytes_uploaded))

    on_io(SIZE + 600)
    on_io(SIZE + 100)  # retry started over
    on_io(3 * SIZE)  # more than the file (CLI overhead)
    assert reported == [SIZE // 2 + 300, SIZE - 1]


# --- ProtonDriveClient: Randfälle -------------------------------------------------------------------------------


def make_client(tmp_path: Path, cli: object, *, instance_id: str = "inst") -> ProtonDriveClient:
    """Build a ProtonDriveClient around a fake CLI, bypassing `create()`."""
    client = object.__new__(ProtonDriveClient)
    client._ProtonDriveClient__cli = cli
    client._ProtonDriveClient__instance_id = instance_id
    client._ProtonDriveClient__backup_folder = Path("/my-files/backups")
    client._ProtonDriveClient__temp_backup_dir = tmp_path
    return client


class FakeCLI:
    """Minimal stand-in for ProtonCLI: a fixed file listing and a pluggable `run`."""

    def __init__(self, files: list[str], run: Callable[..., Awaitable[None]] | None = None) -> None:
        """Initialize with the listing `list_files` returns."""
        self.files = files
        self.run = run

    async def list_files(self, _folder: Path, *, folders_only: bool = False) -> list[str]:  # noqa: ARG002
        """Return the fixed listing."""
        return self.files


async def test_download_of_legacy_backup_uses_plain_archive_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Legacy (pre-chunking) metadata must map to `<name>.tar`, not to a name with a stray `$` in front."""
    client = make_client(tmp_path, FakeCLI(["old-inst.metadata.json", "old-inst.tar"]))

    async def read_metadata(_self: object, _name: str) -> dict:
        return {"legacy": True}

    requested: list[list[str]] = []

    async def stream_files(_self: object, _path: Path, files: list[str]) -> AsyncIterator[bytes]:
        requested.append(files)
        yield b""

    monkeypatch.setattr(ProtonDriveClient, "_ProtonDriveClient__read_metadata", read_metadata)
    monkeypatch.setattr(ProtonDriveClient, "_ProtonDriveClient__stream_files", stream_files)

    _ = [chunk async for chunk in client.download_backup("")]  # backup_id "" -> suffix "-inst.metadata.json"

    assert requested == [["old-inst.tar"]]


async def test_list_backups_skips_unreadable_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One corrupt metadata file must not make the whole backup list fail."""
    files = ["a1-inst.metadata.json", "a1-inst.tar", "b2-inst.metadata.json", "b2-inst.tar"]
    client = make_client(tmp_path, FakeCLI(files))

    async def read_metadata(_self: object, name: str) -> dict:
        if name.startswith("a1"):
            msg = "Failed to parse metadata JSON"
            raise ProtonDriveInvalidBackupError(msg)
        return {"backup_id": "b2"}

    monkeypatch.setattr(ProtonDriveClient, "_ProtonDriveClient__read_metadata", read_metadata)
    monkeypatch.setattr(api_module.AgentBackup, "from_dict", staticmethod(dict), raising=False)

    assert await client.list_backups() == [{"backup_id": "b2"}]


async def test_list_backups_skips_metadata_missing_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Metadata that parses as JSON but lacks fields (KeyError in AgentBackup.from_dict) is skipped, too."""
    client = make_client(tmp_path, FakeCLI(["a1-inst.metadata.json", "a1-inst.tar"]))

    async def read_metadata(_self: object, _name: str) -> dict:
        return {}

    def from_dict(data: dict) -> dict:
        return {"id": data["backup_id"]}

    monkeypatch.setattr(ProtonDriveClient, "_ProtonDriveClient__read_metadata", read_metadata)
    monkeypatch.setattr(api_module.AgentBackup, "from_dict", staticmethod(from_dict), raising=False)

    assert await client.list_backups() == []


async def test_stream_files_cancels_pending_downloads_on_failure(tmp_path: Path) -> None:
    """When one download fails, the others must not keep running against the temp folder that is about to go."""
    cancelled: list[str] = []

    async def run(*args: str, **_kwargs: object) -> None:
        name = Path(args[2]).name
        if name == "fail.tar":
            msg = "boom"
            raise CLIError(msg)
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    client = make_client(tmp_path, FakeCLI([], run))
    stream = client._ProtonDriveClient__stream_files(tmp_path, ["fail.tar", "a.tar", "b.tar"])

    with pytest.raises(ProtonDriveError):
        _ = [chunk async for chunk in stream]

    assert sorted(cancelled) == ["a.tar", "b.tar"]
