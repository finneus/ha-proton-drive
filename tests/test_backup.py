"""Tests for the backup agent's error mapping."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from homeassistant.components.backup import BackupAgentError, BackupNotFound
from proton_drive.api import ProtonDriveError, ProtonDriveMissingBackupError
from proton_drive.backup import ProtonDriveBackupAgent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class FakeClient:
    """Stand-in for ProtonDriveClient with configurable failures."""

    def __init__(self, *, files_error: Exception | None = None, stream_error: Exception | None = None) -> None:
        """Configure where the download fails."""
        self.files_error = files_error
        self.stream_error = stream_error
        self.backup: object | None = None

    async def backup_files(self, _backup_id: str) -> list[str]:
        """Return the archive files or fail like a missing backup."""
        if self.files_error:
            raise self.files_error
        return ["a.tar"]

    async def stream_backup(self, _files: list[str]) -> AsyncIterator[bytes]:
        """Yield one chunk, then fail if configured."""
        yield b"data"
        if self.stream_error:
            raise self.stream_error

    async def get_backup(self, _backup_id: str) -> object | None:
        """Return the configured backup."""
        return self.backup


def make_agent(client: FakeClient) -> ProtonDriveBackupAgent:
    entry = SimpleNamespace(title="me@proton.me", runtime_data=SimpleNamespace(client=client))
    return ProtonDriveBackupAgent(entry)


async def test_missing_backup_is_backup_not_found() -> None:
    """Before, the error only surfaced while iterating and ended up as HTTP 500."""
    agent = make_agent(FakeClient(files_error=ProtonDriveMissingBackupError("gone")))
    with pytest.raises(BackupNotFound):
        await agent.async_download_backup("b1")


async def test_download_error_before_streaming_is_agent_error() -> None:
    agent = make_agent(FakeClient(files_error=ProtonDriveError("broken")))
    with pytest.raises(BackupAgentError):
        await agent.async_download_backup("b1")


async def test_download_error_while_streaming_is_agent_error() -> None:
    agent = make_agent(FakeClient(stream_error=ProtonDriveError("network")))
    stream = await agent.async_download_backup("b1")
    with pytest.raises(BackupAgentError):
        _ = [chunk async for chunk in stream]


async def test_get_backup_reads_only_that_backup() -> None:
    client = FakeClient()
    agent = make_agent(client)
    with pytest.raises(BackupNotFound):
        await agent.async_get_backup("b1")
    client.backup = {"backup_id": "b1"}
    assert await agent.async_get_backup("b1") == {"backup_id": "b1"}
