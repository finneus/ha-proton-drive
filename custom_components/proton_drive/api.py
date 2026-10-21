"""Proton Drive API."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import aiofiles
import aiofiles.os
import aiofiles.ospath
from cachetools import TTLCache
from cachetools_async import cached
from homeassistant.components.backup import AgentBackup, OnProgressCallback
from homeassistant.components.backup.util import suggested_filename

from .cli import (
    CLIError,
    CLITimeoutError,
    NameConflictError,
    ProtonCLI,
)
from .const import DOMAIN, LOGGER

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine

    from homeassistant.core import HomeAssistant


class ProtonDriveError(Exception):
    """Exception to indicate a general error."""


class ProtonDriveInvalidBackupError(ProtonDriveError):
    """Exception to indicate a backup has an invalid format."""


class ProtonDriveMissingBackupError(ProtonDriveError):
    """Exception to indicate a backup is missing."""


@dataclass
class Metadata:
    """Class to represent backup metadata."""

    proton_drive_version: str
    instance_id: str
    backup_id: str
    base_name: str
    metadata: dict  # AgentBackup.as_dict()
    chunks: int

    # Upper bound for `chunks` read from the drive (a backup this size would be > 1 TB with today's chunking)
    MAX_CHUNKS: ClassVar[int] = 10_000

    @classmethod
    def is_valid(cls, data: dict) -> bool:
        """Check if the given JSON string is valid metadata."""
        ver = data.get("proton_drive_version")
        return ver == "1.0.0"

    @classmethod
    def load(cls, data: dict) -> Metadata | dict:
        """
        Load metadata from a JSON string.

        It can either be a new style one with added info and chunking or older type.
        """
        if not cls.is_valid(data):
            return data
        # The metadata comes from the drive: whoever can write there must not be able to steer local paths
        # (`base_name` ends up in `temp_dir / name`) or make us build huge lists (`chunks`).
        base_name, chunks = data["base_name"], data["chunks"]
        if not isinstance(base_name, str) or any(c in base_name for c in "/\\\0"):
            msg = f"Invalid base_name in backup metadata: {base_name!r}"
            raise ProtonDriveInvalidBackupError(msg)
        if isinstance(chunks, bool) or not isinstance(chunks, int) or not 1 <= chunks <= cls.MAX_CHUNKS:
            msg = f"Invalid chunk count in backup metadata: {chunks!r}"
            raise ProtonDriveInvalidBackupError(msg)
        return cls(
            proton_drive_version=data["proton_drive_version"],
            instance_id=data["instance_id"],
            backup_id=data["backup_id"],
            base_name=data["base_name"],
            metadata=data["metadata"],
            chunks=data["chunks"],
        )

    def as_dict(self) -> dict:
        """Return the metadata as a dictionary."""
        return {
            "proton_drive_version": self.proton_drive_version,
            "instance_id": self.instance_id,
            "backup_id": self.backup_id,
            "base_name": self.base_name,
            "metadata": self.metadata,
            "chunks": self.chunks,
        }


_MIN_FOLDER_PARTS = 2  # a drive root plus at least one folder, e.g. /my-files/HomeAssistant


def normalize_backup_folder(raw: str) -> Path | None:
    """
    Turn the folder typed in the config flow into a clean drive path: `/my-files//HA/` -> `/my-files/HA`.

    None for relative paths, a bare drive root, `..` and names starting with `-` (the CLI would read such a name as
    an option when it creates the folder).
    """
    text = raw.strip()
    parts = [p for p in text.split("/") if p not in ("", ".")]
    if not text.startswith("/") or len(parts) < _MIN_FOLDER_PARTS:
        return None
    if any(p == ".." or p.startswith("-") for p in parts):
        return None
    return Path("/" + "/".join(parts))


def _upload_progress(size: int, on_progress: OnProgressCallback) -> Callable[[int], None]:
    """
    Turn the bytes the CLI has read into Home Assistant upload progress.

    Writing the local archive is the first half of the bar (see upload_backup), the upload the second half. The CLI
    reports no progress itself, so it is estimated from what the CLI process has read (kernel I/O counters, see
    ProtonCLI.run(on_io=...)). Measured with CLI 0.9.0 in Home Assistant (1.26 GB backup): the CLI first reads the
    whole file within seconds (content hash), then reads it a second time while uploading, in blocks of ~60 MB with
    backpressure; in total 2 x size + ~0.4 MB. So the upload progress is `read - size`. While `read` is still below
    the file size (hashing, or a future CLI without that pass) no upload progress is claimed at all.
    Never goes backwards (a retry starts again from 0) and stays below 100 % until the upload has really finished.
    """
    reported = size // 2

    def _on_io(read: int) -> None:
        nonlocal reported
        uploaded = min(max(read - size, 0), size)
        estimate = min(size // 2 + uploaded // 2, size - 1)
        if estimate > reported:
            reported = estimate
            on_progress(bytes_uploaded=estimate)

    return _on_io


class ProtonDriveClient:
    """Client for the Proton Drive API."""

    METADATA_EXT = ".metadata.json"
    ARCHIVE_EXT = ".tar"
    PART_EXT = f"{ARCHIVE_EXT}.part"
    READ_CHUNK = 131_072
    DOWNLOAD_WINDOW = 4
    ALLOWED_DELETE_PREFIXES: ClassVar[list[Path]] = [
        Path("/my-files/"),
        Path("/devices/"),
        Path("/shared-with-me/"),
        Path("/photos/"),
    ]

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        cli: ProtonCLI,
        instance_id: str,
        backup_folder: str,
    ) -> None:
        """Use `await ProtonDriveClient.create(...)` to create an instance of this class."""
        self.__cli = cli
        self.__instance_id = instance_id
        self.__backup_folder = Path(backup_folder)
        self.__temp_backup_dir = Path(hass.config.cache_path(DOMAIN, "tmp_backups"))

    def aclose(self) -> None:
        """Release this client's own resources on unload."""
        self.__cli.aclose()

    @classmethod
    async def create(
        cls,
        *,
        hass: HomeAssistant,
        cli: ProtonCLI,
        instance_id: str,
        backup_folder: str,
    ) -> ProtonDriveClient:
        """Create and initialize a ProtonDriveClient instance."""
        me = cls(hass=hass, cli=cli, instance_id=instance_id, backup_folder=backup_folder)
        await me.__prepare()
        return me

    async def __prepare(self) -> None:
        await self.__cli.get_email()

        if not self.__backup_folder.is_absolute() or len(self.__backup_folder.parts) < 2:  # noqa: PLR2004
            msg = f"Invalid backup folder '{self.__backup_folder}': must be a full path like /my-files/HomeAssistant"
            raise ProtonDriveError(msg)
        await self.__ensure_folder(self.__backup_folder)
        await aiofiles.os.makedirs(self.__temp_backup_dir, exist_ok=True)

    async def __ensure_folder(self, folder: Path) -> None:
        """Create `folder` and any missing parents (drive roots like /my-files can't be created)."""
        if await self.__cli.exists(folder):
            return
        if folder.parent == Path("/"):
            msg = f"Proton Drive root '{folder}' does not exist, use one like /my-files"
            raise ProtonDriveError(msg)
        await self.__ensure_folder(folder.parent)
        LOGGER.info("Folder %s not found, creating it", folder)
        await self.__cli.run(
            "filesystem",
            "create-folder",
            str(folder.parent),
            folder.name,
        )

    async def download_backup(self, backup_id: str) -> AsyncIterator[bytes]:
        """Download a Home Assistant backup."""
        LOGGER.info("Downloading backup with ID: %s", backup_id)
        files = await self.backup_files(backup_id)
        async for chunk in self.stream_backup(files):
            yield chunk
        LOGGER.info("Finished downloading backup with ID: %s", backup_id)

    async def stream_backup(self, files: list[str]) -> AsyncIterator[bytes]:
        """Stream the given archive file(s) of a backup (from `backup_files`), downloading ahead."""
        async with self.__temp_folder() as path:
            async for chunk in self.__stream_files(path, files):
                yield chunk

    async def backup_files(self, backup_id: str) -> list[str]:
        """
        Names of the archive file(s) of a backup, in order.

        Separate from the streaming so that a missing or broken backup is reported before the download starts
        (raises ProtonDriveMissingBackupError / ProtonDriveInvalidBackupError).
        """
        metadata_suffix = self.__make_metadata_filename("", backup_id)

        LOGGER.debug("Looking for metadata file with suffix: %s", metadata_suffix)

        metadata_file = None
        files = await self.__cli.list_files(self.__backup_folder)
        for filename in files:
            if filename.endswith(metadata_suffix):
                metadata_file = filename
                break

        LOGGER.debug("Found metadata file: %s", metadata_file)

        if metadata_file is None:
            msg = f"Metadata file not found for backup_id: {backup_id}"
            raise ProtonDriveMissingBackupError(msg)

        metadata = await self.__read_metadata(metadata_file)

        LOGGER.debug("Read metadata: %s", metadata)

        if not isinstance(metadata, Metadata) or metadata.chunks <= 1:
            name = (
                self.__make_backup_filename(metadata.base_name, backup_id)
                if isinstance(metadata, Metadata)
                else f"{metadata_file.removesuffix(self.METADATA_EXT)}{self.ARCHIVE_EXT}"
            )
            LOGGER.debug("Downloading single backup file: %s", name)
            files = [name]
        else:
            files = [self.__make_chunk_filename(metadata.base_name, backup_id, i) for i in range(metadata.chunks)]
            LOGGER.debug("Downloading chunked (%d chunks) backup file", len(files))
        return files

    async def __stream_files(self, path: Path, files: list[str]) -> AsyncIterator[bytes]:
        tasks: dict[int, asyncio.Task[Path | CLIError]] = {}
        next_i = 0

        async def download(i: int) -> Path | CLIError:
            name = files[i]
            LOGGER.debug("Downloading file %d/%d: %s", i + 1, len(files), name)
            try:
                await self.__cli.run(
                    "filesystem",
                    "download",
                    self.__make_filepath(name),
                    str(path),
                    timeout_s=ProtonCLI.TRANSFER_TIMEOUT_S,
                    retries=0,
                )
            except CLIError as e:
                LOGGER.error("Failed to download file %d/%d: %s", i + 1, len(files), name)
                return e
            LOGGER.debug("Downloaded file %d/%d: %s", i + 1, len(files), name)
            return path / name

        try:
            while next_i < min(self.DOWNLOAD_WINDOW, len(files)):
                LOGGER.debug("Scheduling download for file %d/%d: %s", next_i + 1, len(files), files[next_i])
                tasks[next_i] = asyncio.create_task(download(next_i))
                next_i += 1

            for i, name in enumerate(files):
                p = await tasks[i]
                if isinstance(p, CLIError):
                    msg = f"Failed to download backup file {name}: {p}"
                    raise ProtonDriveError(msg) from p

                if next_i < len(files):
                    LOGGER.debug("Scheduling download for file %d/%d: %s", next_i + 1, len(files), files[next_i])
                    tasks[next_i] = asyncio.create_task(download(next_i))
                    next_i += 1

                LOGGER.debug("Yielding file %d/%d: %s", i + 1, len(files), name)
                async with aiofiles.open(p, "rb") as f:
                    while chunk := await f.read(self.READ_CHUNK):
                        yield chunk
                LOGGER.debug("Finished yielding file %d/%d: %s", i + 1, len(files), name)
        finally:
            # On a failure (or an abandoned stream) the read-ahead downloads must not keep running against the temp
            # folder the caller removes right after this generator ends.
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def upload_backup(
        self,
        open_stream: Callable[[], Coroutine[Any, Any, AsyncIterator[bytes]]],
        backup: AgentBackup,
        on_progress: OnProgressCallback,
    ) -> None:
        """Upload a Home Assistant backup."""
        LOGGER.info("Uploading backup with ID: %s", backup.backup_id)
        archive_name = suggested_filename(backup)
        base_name = archive_name.removesuffix(self.ARCHIVE_EXT)
        archive_name = self.__make_backup_filename(base_name, backup.backup_id)
        metadata_name = self.__make_metadata_filename(base_name, backup.backup_id)

        LOGGER.debug("Uploading backup: %s / %s (%s)", metadata_name, archive_name, base_name)

        # TODO: no chunking, no retries, do we still need to do that or will the CLI be stable enough?

        async with self.__temp_folder() as path:
            await aiofiles.os.makedirs(path, exist_ok=True)

            meta_path = path / metadata_name
            archive_path = path / archive_name

            # TODO: not counting the metadata in the progress
            async with aiofiles.open(meta_path, "w") as f:
                await f.write(
                    json.dumps(
                        Metadata(
                            proton_drive_version="1.0.0",
                            instance_id=self.__instance_id,
                            backup_id=backup.backup_id,
                            base_name=base_name,
                            metadata=backup.as_dict(),
                            chunks=1,
                        ).as_dict(),
                    ),
                )

            write_start = time.monotonic()
            async with aiofiles.open(archive_path, "wb") as f:
                async for chunk in await open_stream():
                    await f.write(chunk)
                    on_progress(bytes_uploaded=await f.tell() // 2)
                size = await f.tell()
            LOGGER.debug(
                "Wrote local archive %s (%d bytes) in %.1fs, starting upload",
                archive_name,
                size,
                time.monotonic() - write_start,
            )

            async def _upload(local_path: Path, timeout_s: float, on_io: Callable[[int], None] | None = None) -> None:
                try:
                    await self.__cli.run(
                        "filesystem",
                        "upload",
                        str(local_path),
                        str(self.__backup_folder),
                        timeout_s=timeout_s,
                        retries=2,
                        on_io=on_io,
                    )
                except NameConflictError:
                    await self.__verify_existing_upload(local_path)

            upload_start = time.monotonic()
            meta_task = asyncio.create_task(_upload(meta_path, ProtonCLI.METADATA_TIMEOUT_S))
            archive_task = asyncio.create_task(
                _upload(archive_path, ProtonCLI.TRANSFER_TIMEOUT_S, _upload_progress(size, on_progress))
            )
            try:
                # Cancelled (e.g. Home Assistant stops): gather cancels both uploads and each waits for its CLI
                # process to exit before the temp folder goes away. Partial files left in the drive are skipped as
                # orphans by list_backups and replaced by the next backup.
                await asyncio.gather(meta_task, archive_task)
            except CLIError:
                # asyncio.gather() does NOT cancel a still-running sibling when one task raises on its
                # own (only when gather() itself is cancelled) - without this, the other upload keeps
                # running in the background against files we're about to delete below.
                for task in (meta_task, archive_task):
                    task.cancel()
                await asyncio.gather(meta_task, archive_task, return_exceptions=True)

                filenames = [Path(metadata_name), Path(archive_name)]
                LOGGER.exception("Failed to upload backup: %s, deleting...", filenames)

                try:
                    to_delete = list(map(self.__make_filepath, filenames))
                    to_delete = [f for f in to_delete if await self.__cli.exists(f)]
                    await self.__cli.trash(*to_delete)

                    if to_delete and self.__can_delete_file(self.__backup_folder):
                        trash = Path("/trash")
                        await self.__cli.run("filesystem", "delete", *[str(trash / Path(f).name) for f in to_delete])
                except CLIError:
                    LOGGER.exception("Failed to clean up orphaned files after upload failure")
                raise

            LOGGER.debug("Uploaded %s in %.1fs", archive_name, time.monotonic() - upload_start)
            on_progress(bytes_uploaded=size)

    async def delete_backup(self, backup_id: str) -> None:
        """Delete a Home Assistant backup."""
        metadata_suffix = self.__make_metadata_filename("", backup_id)
        archive_suffix = self.__make_backup_filename("", backup_id)

        LOGGER.debug("Looking for metadata and archive files with suffixes: %s, %s", metadata_suffix, archive_suffix)

        metadata_file = None
        to_delete = []

        files = await self.__cli.list_files(self.__backup_folder)
        for filename in files:
            if filename.endswith(metadata_suffix):
                metadata_file = filename
                to_delete.append(filename)
            elif filename.endswith(archive_suffix):
                to_delete.append(filename)

        LOGGER.debug("Found metadata file: %s", metadata_file)
        LOGGER.debug("Found files to delete: %s", to_delete)

        metadata: Metadata | dict = {}
        if metadata_file is None:
            LOGGER.warning("Metadata file not found for backup_id: %s", backup_id)
        else:
            try:
                metadata = await self.__read_metadata(metadata_file)
            except ProtonDriveInvalidBackupError:
                # still delete what clearly belongs to this backup (matched by name above)
                LOGGER.warning("Invalid metadata for backup_id %s, deleting only the matching files", backup_id)
            LOGGER.debug("Read metadata: %s", metadata)
            if isinstance(metadata, Metadata):
                if metadata.chunks > 1:
                    to_delete.extend(
                        self.__make_chunk_filename(metadata.base_name, backup_id, i) for i in range(metadata.chunks)
                    )
                else:
                    to_delete.append(self.__make_backup_filename(metadata.base_name, backup_id))

        to_delete = list(set(to_delete))
        to_delete = map(self.__make_filepath, to_delete)
        to_delete = [f for f in to_delete if await self.__cli.exists(f)]
        await self.__cli.trash(*to_delete)

    def __can_delete_file(self, filename: Path) -> bool:
        # FIXME: see https://github.com/ProtonDriveApps/sdk/blob/main/cli/src/commands/fileSystem/commandFileSystemTrash.ts#L3
        return any(filename.is_relative_to(prefix) for prefix in self.ALLOWED_DELETE_PREFIXES)

    async def list_backups(self) -> list[AgentBackup]:
        """List Home Assistant backups."""
        files = await self.__cli.list_files(self.__backup_folder)
        metadata_suffix = self.__make_metadata_filename("", "")
        archive_suffix = self.__make_backup_filename("", "")
        LOGGER.debug("Looking for metadata files with suffix: %s", metadata_suffix)
        metadata_files = [f for f in files if f.endswith(metadata_suffix)]
        archive_files = [f for f in files if f.endswith(archive_suffix)]

        # Files without their counterpart are only skipped, never trashed automatically: metadata and archive
        # are uploaded in parallel (the archive can take up to an hour) and Proton Drive lists new files with a
        # delay, so a backup that is still uploading or was just finished can look "orphaned" for a while.
        file_set = set(files)
        orphans = [f for f in metadata_files if not self.__has_matching_archive(f, files)]
        orphans.extend(f for f in archive_files if f.removesuffix(self.ARCHIVE_EXT) + self.METADATA_EXT not in file_set)
        if orphans:
            LOGGER.debug("Skipping backup file(s) without counterpart (uploading, delayed or orphaned): %s", orphans)

        live_metadata_files = [f for f in metadata_files if f not in orphans]

        async def _fetch(filename: str) -> AgentBackup | None:
            try:
                metadata = await self.__read_metadata(filename)
                if isinstance(metadata, Metadata):
                    return AgentBackup.from_dict(metadata.metadata)
                return AgentBackup.from_dict(metadata)
            except CLIError, ProtonDriveInvalidBackupError, KeyError, TypeError, ValueError:
                # One unreadable/incomplete metadata file must not hide all the other backups
                LOGGER.exception("Failed to read metadata %s", filename)
                return None

        results = await asyncio.gather(*[_fetch(f) for f in live_metadata_files])
        return [r for r in results if r is not None]

    async def get_backup(self, backup_id: str) -> AgentBackup | None:
        """One backup by ID (reads only its own metadata), None if it does not exist or is incomplete."""
        files = await self.__cli.list_files(self.__backup_folder)
        suffix = self.__make_metadata_filename("", backup_id)
        metadata_file = next((f for f in files if f.endswith(suffix)), None)
        if metadata_file is None or not self.__has_matching_archive(metadata_file, files):
            return None
        metadata = await self.__read_metadata(metadata_file)
        return AgentBackup.from_dict(metadata.metadata if isinstance(metadata, Metadata) else metadata)

    async def __verify_existing_upload(self, local_path: Path) -> None:
        """
        Accept a name conflict on upload only for a complete file.

        A retry after our own timed-out attempt: our filenames embed the backup_id, so the file is ours - but that
        attempt may have stopped half-way. Raises CLIError (-> cleanup) if the remote file is not the full size.
        """
        expected = (await aiofiles.os.stat(local_path)).st_size
        actual = await self.__remote_size(self.__make_filepath(local_path.name))
        if actual != expected:
            msg = (
                f"{local_path.name} already exists in Proton Drive but is incomplete "
                f"({actual} of {expected} bytes) after an interrupted attempt"
            )
            raise CLIError(msg)
        LOGGER.info(
            "Upload of %s already completed (name conflict on retry, size verified), treating as success",
            local_path.name,
        )

    async def __remote_size(self, path: str) -> int | None:
        """Size of a file in the drive (raw, from its active revision), None if unknown or not there."""
        try:
            info = await self.__cli.stat(path)
        except CLIError:
            return None
        revision = getattr(info, "activeRevision", None)
        revision = getattr(revision, "value", revision)  # some CLI versions wrap fields as {"ok", "value"}
        size = getattr(revision, "claimedSize", None)
        return size if isinstance(size, int) and not isinstance(size, bool) else None

    def __has_matching_archive(self, metadata_file: str, files: list[str]) -> bool:
        # Chunked backups store their archive as numbered `NN.tar.part` files instead of a
        # plain `.tar`, so we can't just check for one exact filename.
        prefix = metadata_file.removesuffix(self.METADATA_EXT)
        return any(f.startswith(prefix) and f.endswith((self.ARCHIVE_EXT, self.PART_EXT)) for f in files)

    @cached(cache=TTLCache(maxsize=1024, ttl=300))
    async def __read_metadata(self, metadata_file: str) -> Metadata | dict:
        attempts = 3
        for attempt in range(1, attempts + 1):
            # Each attempt gets a fresh temp destination: a timed-out download is killed after it may
            # already have created the local file, so retrying into the same path fails with a name
            # conflict (which the CLI's own retry loop cannot recover from).
            try:
                async with self.__temp_folder() as path:
                    await self.__cli.run(
                        "filesystem",
                        "download",
                        self.__make_filepath(metadata_file),
                        str(path),
                        timeout_s=ProtonCLI.METADATA_TIMEOUT_S,
                        retries=0,
                    )
                    async with aiofiles.open(path / metadata_file) as f:
                        data = await f.read()
            except CLITimeoutError, NameConflictError:
                if attempt >= attempts:
                    raise
                LOGGER.debug("Retrying metadata download of %s (%d/%d)", metadata_file, attempt, attempts)
                continue
            try:
                json_data = json.loads(data)
            except json.JSONDecodeError as e:
                LOGGER.exception("Failed to parse metadata JSON: %s", data)
                msg = "Failed to parse metadata JSON"
                raise ProtonDriveInvalidBackupError(msg) from e
            return Metadata.load(json_data)
        msg = f"Failed to download metadata {metadata_file}"
        raise CLIError(msg)

    def __make_metadata_filename(self, base: str, backup_id: str) -> str:
        return self.__make_filename(base, backup_id, self.METADATA_EXT[1:])

    def __make_backup_filename(self, base: str, backup_id: str) -> str:
        return self.__make_filename(base, backup_id, self.ARCHIVE_EXT[1:])

    def __make_chunk_filename(self, base: str, backup_id: str, chunk: int) -> str:
        return self.__make_filename(base, backup_id, f"{chunk:02d}{self.PART_EXT}")

    def __make_filename(self, base: str, backup_id: str, suffix: str) -> str:
        return f"{base}{backup_id}-{self.__instance_id}.{suffix}"

    def __make_filepath(self, filename: str | Path) -> str:
        return str(self.__backup_folder / filename)

    @asynccontextmanager
    async def __temp_folder(self) -> AsyncGenerator[Path]:
        filename = f"temp_{uuid.uuid4().hex}"
        filepath = self.__temp_backup_dir / filename
        try:
            yield filepath
        finally:
            if await aiofiles.ospath.exists(filepath):
                await self.__rmtree(filepath)

    @classmethod
    async def __rmtree(cls, path: Path) -> None:
        if await aiofiles.ospath.islink(path):
            await aiofiles.os.remove(path)
        elif await aiofiles.ospath.isdir(path):
            for entry in await aiofiles.os.listdir(path):
                full_path = path / entry
                await cls.__rmtree(full_path)
            await aiofiles.os.rmdir(path)
        else:
            await aiofiles.os.remove(path)
