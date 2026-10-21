"""Wrapper for the Proton Drive CLI binary."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from http import HTTPStatus
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

import aiofiles
import aiofiles.os
import aiofiles.ospath
import aiohttp
from homeassistant.loader import async_get_integration

from .const import (
    CLI_BASE_URL_FORMAT,
    CLI_CHECKSUMS,
    CLI_VERSION,
    DOMAIN,
    LOGGER,
)

if TYPE_CHECKING:
    from asyncio.subprocess import Process
    from collections.abc import AsyncIterator, Awaitable, Callable

    from awesomeversion import AwesomeVersion
    from homeassistant.core import HomeAssistant

    _IsStaleFn = Callable[[str], Awaitable[tuple[bool, timedelta | None]]]


class CLIStartupError(Exception):
    """Exception to indicate a failure to start the CLI."""


class UnsupportedPlatformError(CLIStartupError):
    """Exception to indicate an unsupported platform."""


class DownloadCLIError(CLIStartupError):
    """Exception to indicate a failure to download the CLI."""


class DownloadCLINetworkError(DownloadCLIError):
    """Exception to indicate a failure to download the CLI because of the network."""


class CLIError(Exception):
    """Exception to indicate a failure while running the CLI."""


class InvalidOutputError(CLIError):
    """Exception to indicate invalid output from the CLI."""


class CLITimeoutError(CLIError):
    """Exception to indicate a CLI command timed out."""


class NodeNotFoundError(CLIError):
    """Exception to indicate a node was not found in Proton Drive."""


class AuthError(CLIError):
    """Exception to indicate an authentication error with Proton Drive."""


class APIUnavailableError(CLIError):
    """Exception to indicate a transient failure reaching the Proton API."""


class CLIDatabaseLockedError(CLIError):
    """Exception to indicate the CLI's local cache database was locked by a concurrent invocation."""


class NameConflictError(CLIError):
    """Exception to indicate a file/folder with the target name already exists."""


def _transform_version(version: AwesomeVersion | None) -> str:
    """
    Map a manifest version into the SDK's `{semver}-{channel}[+{suffix}]` shape.

    See https://github.com/ProtonDriveApps/sdk/blob/main/README.md#operational-requirements.
    """
    if version is None:
        return "unknown-stable"

    channel = "stable"
    if version.beta or version.release_candidate:
        channel = "beta"
    elif version.alpha or version.dev:
        channel = "alpha"
    suffix = ""
    if version.release_candidate:
        suffix = f"+{version.modifier}"
    return f"{version.major or 0}.{version.minor or 0}.{version.patch or 0}-{channel}{suffix}"


def _parse_proc_io(text: str) -> tuple[int, int] | None:
    """Return (rchar, wchar) from the content of /proc/<pid>/io, None if they are missing."""
    values: dict[str, int] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if value.strip().isdigit():
            values[key.strip()] = int(value)
    if "rchar" not in values or "wchar" not in values:
        return None
    return values["rchar"], values["wchar"]


async def _proc_io(pid: int) -> tuple[int, int] | None:
    """Bytes read/written so far by a process (Linux only), None if unavailable."""
    try:
        async with aiofiles.open(f"/proc/{pid}/io") as f:
            return _parse_proc_io(await f.read())
    except OSError:
        return None


# Only what the CLI needs from Home Assistant's environment: locale, temp dir, proxies and TLS trust. Everything else
# (e.g. SUPERVISOR_TOKEN on HAOS/Supervised, add-on secrets) stays out of the downloaded binary's reach.
_PASSTHROUGH_ENV = (
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TZ",
    "TMPDIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NODE_EXTRA_CA_CERTS",
)
_HASH_BLOCK = 1024 * 1024


def _cli_env(extra: dict[str, str]) -> dict[str, str]:
    """Environment for the CLI binary: the allow-listed variables of this process plus `extra`."""
    env = {key: os.environ[key] for key in _PASSTHROUGH_ENV if key in os.environ}
    return {**env, **extra}


def _sha512_file(path: Path) -> str:
    """SHA-512 of a file, read block by block (the binary is ~100 MB; runs in an executor)."""
    digest = hashlib.sha512()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(_HASH_BLOCK), b""):
            digest.update(block)
    return digest.hexdigest()


def stop_process(run: ProtonCLI._CLIRun) -> None:
    """Ask a still-running CLI process to exit (no-op once it is gone)."""
    if run.process.returncode is None:
        with suppress(ProcessLookupError):
            run.process.terminate()


class ProtonCLI:
    """Wrapper for the Proton Drive CLI binary."""

    READ_CHUNK = 65536

    DEFAULT_TIMEOUT_S = 10
    METADATA_TIMEOUT_S = 60
    TRANSFER_TIMEOUT_S = 60 * 60
    TERMINATE_GRACE_S = 5
    # How often a running transfer is sampled for progress (see run(on_io=...))
    IO_POLL_INTERVAL_S = 5

    STALE_AUTH_THRESHOLD = timedelta(hours=24)
    STALE_AUTH_REQUIRED_SUCCESSES = 5

    class _CriticalState:
        def __init__(self) -> None:
            self.__condition = asyncio.Condition()
            self.__lock = asyncio.Lock()
            self.__active = False
            self.__successes_remaining = 0

        async def _is_critical(self, *, uid: str, is_stale: _IsStaleFn) -> bool:
            async with self.__condition:
                if not self.__active and (await is_stale(uid))[0]:
                    LOGGER.debug(
                        "[%s] Auth looks stale, serializing CLI/API calls until %d succeed in a row",
                        uid,
                        ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES,
                    )
                    self.__active = True
                    self.__successes_remaining = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
                while self.__active and self.__lock.locked():
                    await self.__condition.wait()
                return self.__active

        @asynccontextmanager
        async def _lock(self, *, uid: str, is_stale: _IsStaleFn) -> AsyncIterator[None]:
            try:
                async with self.__lock:
                    success = False
                    try:
                        yield
                        success = True
                    except NodeNotFoundError:
                        success = True
                        raise
                    finally:
                        async with self.__condition:
                            if success:
                                self.__successes_remaining -= 1
                                if self.__successes_remaining <= 0:
                                    stale, age = await is_stale(uid)
                                    if stale:
                                        self.__successes_remaining = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
                                        LOGGER.warning(
                                            "[%s] Auth staleness still present, "
                                            "estimated TTL needs tuning (age: %s, ttl: %s)",
                                            uid,
                                            age,
                                            ProtonCLI.STALE_AUTH_THRESHOLD,
                                        )
                                    else:
                                        self.__active = False
                                        LOGGER.debug("[%s] Auth staleness is gone", uid)
                                else:
                                    LOGGER.debug(
                                        "[%s] Auth streak: %d successes remaining", uid, self.__successes_remaining
                                    )
                            else:
                                self.__successes_remaining = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
                                LOGGER.debug(
                                    "[%s] Auth streak reset to %d successes remaining after a failure",
                                    uid,
                                    self.__successes_remaining,
                                )
            finally:
                async with self.__condition:
                    self.__condition.notify_all()

    __critical: ClassVar[_CriticalState] = _CriticalState()

    def __init__(self, hass: HomeAssistant, integration_version: str) -> None:
        """Do not call this directly, use `await ProtonCLI.create(hass)` instead."""
        self.__xdg = hass.config.cache_path(DOMAIN, "xdg")
        self.__path = hass.config.cache_path(DOMAIN, "proton-drive")
        self.__integration_version = integration_version
        # timed-out runs whose process is still being terminated in the background
        self.__reaping: dict[asyncio.Task[None], ProtonCLI._CLIRun] = {}

    @classmethod
    async def create(cls, hass: HomeAssistant) -> ProtonCLI:
        """Create and initialize a ProtonCLI instance."""
        ver = (await async_get_integration(hass, DOMAIN)).version
        me = cls(hass, _transform_version(ver))
        await me.__ainit(hass)
        return me

    def aclose(self) -> None:
        """Stop this instance's background work; CLI processes still being reaped are killed, not left behind."""
        for task, run in list(self.__reaping.items()):
            if run.process.returncode is None:
                with suppress(ProcessLookupError):
                    run.process.kill()
            task.cancel()

    async def __ainit(self, hass: HomeAssistant) -> None:
        cached = Path(self.__path)
        LOGGER.debug("Checking for cached Proton Drive CLI at %s", cached)

        if await aiofiles.ospath.exists(cached) and not await self.__is_valid(cached):
            LOGGER.info("Replacing cached Proton Drive CLI with version %s", CLI_VERSION)
            await aiofiles.os.unlink(cached)

        if not await aiofiles.ospath.exists(cached):
            await self.__download(hass, cached)

        try:
            # Only checks that the binary runs: with --json (always added) `version` prints nothing. The version
            # itself is pinned by the checksum verified above.
            await self.run("version", is_json=False)
        except CLIError as e:
            raise CLIStartupError(str(e)) from e
        LOGGER.info("Proton Drive CLI %s ready (integration %s)", CLI_VERSION, self.__integration_version)

    @classmethod
    async def __get_platform(cls) -> tuple[str, str]:
        machine = platform.machine().lower()
        try:
            names = await aiofiles.os.listdir("/lib")
        except OSError:
            names = []  # no /lib (unusual container): assume glibc, the checksum still guards the download
        is_musl = any(name.startswith("ld-musl-") for name in names)
        variant = "musl" if is_musl else "glibc"
        if machine in ("arm64", "aarch64"):
            arch = "arm64"
        elif machine in ("x86_64", "amd64"):
            arch = "x64"
        else:
            msg = f"Unsupported architecture: {machine}"
            raise UnsupportedPlatformError(msg)
        return variant, arch

    @classmethod
    async def __download(cls, _hass: HomeAssistant, dest: Path) -> None:
        variant, arch = await cls.__get_platform()

        url = CLI_BASE_URL_FORMAT[variant].format(arch=arch, version=CLI_VERSION)

        LOGGER.info("Downloading Proton Drive CLI v%s from %s", CLI_VERSION, url)

        await aiofiles.os.makedirs(dest.parent, exist_ok=True)

        tmp_path = dest.with_suffix(".tmp")

        try:
            async with cls.__http_session() as session, session.get(url) as resp:
                resp.raise_for_status()
                LOGGER.debug("Success, starting download to %s", tmp_path)

                try:
                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(cls.READ_CHUNK):
                            await f.write(chunk)
                except Exception as e:
                    if await aiofiles.ospath.exists(tmp_path):
                        await aiofiles.os.unlink(tmp_path)
                    if isinstance(e, aiohttp.ClientError | TimeoutError):
                        # connection lost mid-body: transient, reported as a network error below
                        raise
                    msg = "Failed to write Proton Drive CLI to disk"
                    raise DownloadCLIError(msg) from e
        except (aiohttp.ClientError, TimeoutError) as e:
            msg = "Failed to download Proton Drive CLI"
            raise DownloadCLINetworkError(msg) from e

        try:
            tmp_path.chmod(0o755)
            await cls.__verify_checksum(tmp_path, variant, arch)
            await aiofiles.os.rename(tmp_path, dest)
        except Exception:
            if await aiofiles.ospath.exists(tmp_path):
                await aiofiles.os.unlink(tmp_path)
            raise

        LOGGER.info("Proton Drive CLI downloaded to %s", dest)

    @classmethod
    def __http_session(cls) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        timeout = aiohttp.ClientTimeout(total=300, connect=30)
        return aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def __is_valid(self, path: Path) -> bool:
        variant, arch = await self.__get_platform()
        try:
            await self.__verify_checksum(path, variant, arch)
        except DownloadCLIError:
            # Expected after an update of CLI_VERSION: the cached binary is the previous version.
            LOGGER.info("Cached Proton Drive CLI does not match version %s (checksum differs)", CLI_VERSION)
            return False
        except CLIStartupError:
            LOGGER.exception("Failed to verify checksum for %s", path)
            return False
        return True

    @classmethod
    async def __verify_checksum(cls, path: Path, variant: str, arch: str) -> None:
        expected = CLI_CHECKSUMS.get(f"{variant}-{arch}")
        if expected is None:
            msg = f"No checksum available for architecture: {arch}"
            raise UnsupportedPlatformError(msg)

        digest = await asyncio.get_running_loop().run_in_executor(None, _sha512_file, path)
        if digest != expected:
            msg = f"Checksum mismatch for {path.name}, expected {expected}, got {digest}"
            raise DownloadCLIError(msg)

    @dataclass
    class _CLIRun:
        id: str
        process: Process

    async def __run(self, *args: str, uid: str | None = None) -> _CLIRun:
        argv = list(args)
        argv.append("--json")

        if uid is None:
            uid = uuid4().hex[:8]
        LOGGER.debug("[%s] Running: %s %s", uid, self.__path, " ".join(argv))

        try:
            proc = await asyncio.create_subprocess_exec(
                self.__path,
                *argv,
                env=_cli_env(
                    {
                        # Home Assistant has no OS keychain: keep the session in a plain file (read by __get_auth).
                        # CLI < 0.9 used PROTON_DRIVE_UNSAFE_SECRETS, 0.9+ uses PROTON_DRIVE_CREDENTIALS_STORE
                        # (keychain | pass | unsafe_file).
                        "PROTON_DRIVE_UNSAFE_SECRETS": "true",
                        "PROTON_DRIVE_CREDENTIALS_STORE": "unsafe_file",
                        "XDG_CACHE_HOME": self.__xdg,
                        "XDG_DATA_HOME": self.__xdg,
                        "XDG_STATE_HOME": self.__xdg,
                    }
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            LOGGER.exception("[%s] Failed to run Proton Drive CLI: %s", uid, e)
            msg = f"Failed to run Proton Drive CLI: {e}"
            raise CLIError(msg) from e
        return self._CLIRun(id=uid, process=proc)

    async def __terminate(self, run: _CLIRun) -> None:
        # Give the CLI a chance to exit on its own terms first: a hard SIGKILL can catch it
        # mid-write to its own on-disk state (e.g. its events lock file), corrupting it and
        # breaking every subsequent invocation until that file is manually removed.
        if run.process.returncode is not None:
            return
        with suppress(ProcessLookupError):  # exited between the check and the signal
            run.process.terminate()
        try:
            async with asyncio.timeout(self.TERMINATE_GRACE_S):
                await run.process.wait()
        except TimeoutError:
            LOGGER.warning("[%s] Did not exit within %ss of SIGTERM, sending SIGKILL", run.id, self.TERMINATE_GRACE_S)
            with suppress(ProcessLookupError):
                run.process.kill()
            # awaiting wait() here can block as long as the timeout itself: https://github.com/python/cpython/issues/139373
            await run.process.wait()
        else:
            LOGGER.debug("[%s] Exited cleanly after SIGTERM", run.id)

    async def __watch_io(self, run: _CLIRun, on_io: Callable[[int], None]) -> None:
        """
        Report how far a running transfer got, from the kernel's I/O counters of the CLI process.

        The CLI prints no machine-readable progress when it is not attached to a terminal. While uploading it reads
        the local file block by block (with backpressure from the upload), which shows up in `rchar` (how that maps
        to upload progress: api._upload_progress). `wchar` does not help: the CLI sends over the network with send(),
        which the kernel does not count there (measured with CLI 0.9.0: ~1.2 MB of wchar for a 1.26 GB upload).
        Stops quietly where /proc is not available (not Linux) or once the process is gone.
        """
        while True:
            await asyncio.sleep(self.IO_POLL_INTERVAL_S)
            io = await _proc_io(run.process.pid)
            if io is None:
                return
            read, written = io
            LOGGER.debug("[%s] CLI I/O so far: read=%d written=%d", run.id, read, written)
            try:
                on_io(read)
            except Exception:  # noqa: BLE001 - a progress callback must never break the transfer
                LOGGER.debug("[%s] Progress callback failed", run.id, exc_info=True)

    def __kill_and_reap(self, run: _CLIRun) -> None:
        # Fire-and-forget: used when a command finally timed out (no retry follows), so the caller gets its
        # error without waiting up to TERMINATE_GRACE_S.
        reap_task = asyncio.create_task(self.__terminate(run))
        self.__reaping[reap_task] = run
        reap_task.add_done_callback(lambda task: self.__reaping.pop(task, None))

    async def __get_auth(self) -> tuple[str, str]:
        """Return the (uid, access token) pair."""
        try:
            async with aiofiles.open(self.__auth_session_path()) as f:
                data = await f.read()
            parsed = json.loads(data, object_hook=lambda d: SimpleNamespace(**d))
            uid = parsed.session.uid
            access_token = parsed.session.accessToken
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, AttributeError) as e:
            msg = f"Missing or corrupt auth session: {e}"
            raise AuthError(msg) from e
        return uid, access_token

    def __auth_session_path(self) -> Path:
        return Path(self.__xdg) / "proton-drive-cli" / "auth-session.json"

    async def __is_auth_stale(self, uid: str) -> tuple[bool, timedelta | None]:
        path = self.__auth_session_path()
        if not await aiofiles.ospath.exists(path):
            return False, None
        try:
            mtime = await aiofiles.ospath.getmtime(path)
        except OSError:
            return True, None
        age = timedelta(seconds=round(time.time() - mtime))
        stale = age > self.STALE_AUTH_THRESHOLD
        if stale:
            LOGGER.debug("[%s] Auth session file is stale (age: %s)", uid, age)
        return stale, age

    @asynccontextmanager
    async def __serialize_if_auth_stale(self, uid: str) -> AsyncIterator[None]:
        """Serialize calls one at a time while auth looks stale."""
        is_critical = await self.__critical._is_critical(uid=uid, is_stale=self.__is_auth_stale)  # noqa: SLF001
        if not is_critical:
            LOGGER.debug("[%s] Starting parallel call", uid)
            yield
            return
        LOGGER.debug("[%s] Starting serialized call", uid)
        async with self.__critical._lock(uid=uid, is_stale=self.__is_auth_stale):  # noqa: SLF001
            yield

    async def list_files(self, folder: Path, *, folders_only: bool = False) -> list[str]:
        """List files in a folder."""
        LOGGER.debug("Listing files in folder: %s", folder)
        files = await self.run("filesystem", "list", str(folder))
        if type(files) is not list:
            msg = f"Expected list of files, got {type(files)}"
            raise InvalidOutputError(msg)
        if folder == Path("/"):
            return [file.path for file in files]
        if folders_only:
            files = [f for f in files if f.type == "folder"]
        return [file.name.value for file in files]

    async def get_email(self) -> str:
        """Get the email address of the authenticated user."""
        result = await self.__api_call("GET", "/core/v4/users")
        return result.User.Email

    async def exists(self, path: Path | str) -> bool:
        """Check if a file or folder exists."""
        try:
            await self.stat(path)
        except NodeNotFoundError:
            return False
        return True

    async def stat(self, path: Path | str) -> SimpleNamespace:
        """Get file or folder metadata."""
        return await self.run("filesystem", "info", str(path))

    async def trash(self, *files: Path | str) -> None:
        """Move files to the trash."""
        if not files:
            return
        LOGGER.info("Trashing files: %s", files)
        await self.run("filesystem", "trash", *[str(f) for f in files])

    async def __api_call(self, verb: str, path: str, body: dict | None = None) -> SimpleNamespace:
        uid = uuid4().hex[:8]
        async with self.__serialize_if_auth_stale(uid):
            try:
                pm_uid, access_token = await self.__get_auth()
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                    "x-pm-uid": pm_uid,
                    "x-pm-appversion": f"external-drive-ha_proton_drive@{self.__integration_version}",
                }
                async with self.__http_session() as session:
                    LOGGER.debug(
                        "[%s] Making API call: %s %s, %s with body: %s",
                        uid,
                        verb,
                        path,
                        {**headers, "Authorization": "******"},
                        body,
                    )
                    res = await session.request(
                        verb,
                        f"https://mail.proton.me/api{path}",
                        headers=headers,
                        **({"json": body} if body is not None else {}),
                    )
                    data = await res.text()
                    # status and size only: the body holds account data (e.g. the e-mail address)
                    LOGGER.debug("[%s] API call response: %s (%d bytes)", uid, res.status, len(data))
                    res.raise_for_status()
                return json.loads(data, object_hook=lambda d: SimpleNamespace(**d))
            except aiohttp.ClientResponseError as e:
                if e.status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                    msg = f"Authentication failed calling {verb} {path}: {e}"
                    raise AuthError(msg) from e
                if e.status >= HTTPStatus.INTERNAL_SERVER_ERROR:
                    msg = f"Proton API unavailable calling {verb} {path}: {e}"
                    raise APIUnavailableError(msg) from e
                msg = f"Failed to make API call: {verb} {path}"
                raise CLIError(msg) from e
            except (aiohttp.ClientError, TimeoutError) as e:
                msg = f"Failed to reach Proton API calling {verb} {path}: {e}"
                raise APIUnavailableError(msg) from e
            except json.JSONDecodeError as e:
                msg = f"Failed to parse API response for {verb} {path}: {e}"
                raise CLIError(msg) from e

    async def run(
        self,
        *args: str,
        is_json: bool = True,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        retries: int = 3,
        on_io: Callable[[int], None] | None = None,
    ) -> Any:
        """
        Run a CLI command and return the parsed JSON output.

        `on_io` (optional) is called every IO_POLL_INTERVAL_S with the bytes the process has read so far - a progress
        estimate for uploads. It starts again from 0 on a retry.
        """
        attempt = 0
        uid = uuid4().hex[:8]
        async with self.__serialize_if_auth_stale(uid):
            while True:
                run = await self.__run(*args, uid=uid)
                watcher = asyncio.create_task(self.__watch_io(run, on_io)) if on_io is not None else None
                try:
                    async with asyncio.timeout(timeout_s):
                        stdout, stderr = await run.process.communicate()
                except TimeoutError as e:
                    if attempt >= retries:
                        self.__kill_and_reap(run)
                        msg = f"[{run.id}] Command timed out after {timeout_s}s"
                        raise CLITimeoutError(msg) from e
                    # Wait until the old process is really gone before retrying: a timed-out upload that is still
                    # running would race the retry (and its leftover draft makes the retry report a name conflict).
                    await self.__terminate(run)
                    attempt += 1
                    LOGGER.warning(
                        "[%s] Command timed out after %ss, retrying (%d/%d)", run.id, timeout_s, attempt, retries
                    )
                    continue
                except asyncio.CancelledError:
                    # e.g. we were explicitly cancelled because a sibling call failed: wait for our own
                    # subprocess to actually die before propagating, so the caller can safely clean up
                    # right after (files/state it deletes won't get pulled out from under a still-running
                    # process). Unlike the timeout path above, blocking here briefly is fine and desired.
                    await asyncio.shield(self.__terminate(run))
                    raise
                finally:
                    if watcher is not None:
                        watcher.cancel()
                try:
                    return self._process_run(run, stdout, stderr, is_json=is_json)
                except CLIDatabaseLockedError:
                    if attempt >= retries:
                        raise
                    attempt += 1
                    delay = min(0.2 * 2 ** (attempt - 1), 2)
                    LOGGER.debug(
                        "[%s] CLI cache database locked by a concurrent call, retrying (%d/%d) in %.1fs",
                        run.id,
                        attempt,
                        retries,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue

    @classmethod
    def _process_run(cls, run: _CLIRun, stdout: bytes, stderr: bytes, *, is_json: bool = True) -> Any:
        if run.process.returncode != 0:
            stdout_text = stdout.decode(errors="replace")
            stderr_text = stderr.decode(errors="replace")

            is_db_locked = "SQLITE_BUSY" in stdout_text or "SQLITE_BUSY" in stderr_text
            is_name_conflict = "Name conflict on" in stdout_text and "already exists" in stdout_text
            is_not_found = "Node not found" in stderr_text

            # These are conditions the caller already expects and handles on its own (a retry, or
            # treating it as non-fatal) - a WARNING for every occurrence is just noise; DEBUG still
            # keeps the detail on hand if it's actually needed.
            log = LOGGER.debug if (is_db_locked or is_name_conflict or is_not_found) else LOGGER.warning
            log(
                "[%s] CLI Result %d: stdout=%s, stderr=%s",
                run.id,
                run.process.returncode,
                stdout_text,
                stderr_text,
            )

            if is_db_locked:
                msg = f"[{run.id}] CLI cache database is locked"
                raise CLIDatabaseLockedError(msg)
            if is_name_conflict:
                msg = f"[{run.id}] Name conflict: {stdout_text}"
                raise NameConflictError(msg)
            msg = stderr_text
            if is_not_found:
                raise NodeNotFoundError(msg)
            # FIXME: odd but that's only explanation I can think of
            if "You need to login first" in msg or "Root node not found" in msg:
                raise AuthError(msg)
            raise CLIError(msg)

        LOGGER.debug(
            "[%s] CLI Result %d: stdout=%s, stderr=%s",
            run.id,
            run.process.returncode,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
        )

        json_output = stdout.decode(errors="replace").strip()
        if not json_output:
            return None

        if not is_json:
            return json_output

        try:
            return json.loads(json_output, object_hook=lambda d: SimpleNamespace(**d))
        except json.JSONDecodeError as e:
            msg = f"Failed to parse JSON output from CLI: {json_output}"
            raise InvalidOutputError(msg) from e

    class AuthFlow:
        """Represents an authentication flow."""

        FLOW_TIMEOUT = 5 * 60

        def __init__(self, *, url: str, run: ProtonCLI._CLIRun) -> None:
            """Initialize an AuthFlow object."""
            self.__url = url
            self.__result = asyncio.create_task(self.__finish(run))

        def get_url(self) -> str:
            """Get the URL to open in the browser for authentication."""
            return self.__url

        def task(self) -> asyncio.Task[Any]:
            """Get the authentication flow task."""
            return self.__result

        async def __finish(self, run: ProtonCLI._CLIRun) -> None:
            stdout_res = b""
            stderr_res = b""

            async def _read_stream(stream: asyncio.StreamReader | None, *, stdout: bool) -> None:
                nonlocal stdout_res, stderr_res
                if stream is None:
                    return
                out = await stream.read()
                if stdout:
                    stdout_res += out
                else:
                    stderr_res += out

            try:
                async with asyncio.timeout(self.FLOW_TIMEOUT):
                    await asyncio.gather(
                        _read_stream(run.process.stdout, stdout=True),
                        _read_stream(run.process.stderr, stdout=False),
                    )
                    await run.process.wait()
            except TimeoutError as e:
                msg = "Auth flow timed out"
                raise CLIError(msg) from e
            finally:
                # timeout or cancelled flow: do not leave `auth login` waiting for a browser nobody uses
                stop_process(run)

            return ProtonCLI._process_run(run, stdout_res, stderr_res)

    async def start_auth_flow(self) -> AuthFlow:
        """Start an authentication flow and return an AuthFlow object."""
        run = await self.__run("auth", "login")

        async def _read_url(run: ProtonCLI._CLIRun) -> str:
            LOGGER.debug("Waiting for auth URL from CLI...")
            assert run.process.stdout is not None  # noqa: S101
            try:
                async with asyncio.timeout(10):
                    line = await run.process.stdout.readline()
                    LOGGER.debug("auth login: %s", line)
                    line_parsed = json.loads(line.decode().strip())
                    return line_parsed["signInUrl"]
            except TimeoutError as e:
                LOGGER.exception("Timed out waiting for auth URL from CLI")
                msg = "Timed out waiting for auth URL from CLI"
                raise AuthError(msg) from e
            except json.JSONDecodeError as e:
                LOGGER.exception("Failed to parse JSON output from CLI")
                msg = "Failed to parse JSON output from CLI"
                raise InvalidOutputError(msg) from e
            except KeyError as e:
                LOGGER.exception("Missing 'signInUrl' in CLI output")
                msg = "Missing 'signInUrl' in CLI output"
                raise InvalidOutputError(msg) from e

        try:
            url = await _read_url(run)
        except BaseException:
            stop_process(run)
            raise
        return self.AuthFlow(url=url, run=run)
