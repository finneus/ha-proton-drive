"""Tests for ProtonCLI's auth-staleness detection and critical-section serialization."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Self

import aiohttp
import pytest
from proton_drive import cli as cli_module
from proton_drive.cli import APIUnavailableError, AuthError, CLIError, ProtonCLI

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

CALL_DURATION_S = 0.3
EXTRA_PARALLEL_CALLS = 5


class FakeResponse:
    """Stand-in for an aiohttp ClientResponse with a fixed status."""

    def __init__(self, status: int) -> None:
        """Initialize with the HTTP status to simulate."""
        self.status = status
        self.headers: dict[str, str] = {}

    async def text(self) -> str:
        """Return an empty JSON body."""
        return "{}"

    def raise_for_status(self) -> None:
        """Raise like aiohttp does for 4xx/5xx statuses."""
        if self.status >= HTTPStatus.BAD_REQUEST:
            raise aiohttp.ClientResponseError(status=self.status)


class FakeSession:
    """Stand-in for an aiohttp ClientSession returning a fixed-status response to any request."""

    def __init__(self, status: int) -> None:
        """Initialize with the HTTP status every request should return."""
        self.__status = status

    async def __aenter__(self) -> Self:
        """Support `async with`."""
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Support `async with`."""
        return

    async def request(self, _verb: str, _url: str, **_kwargs: object) -> FakeResponse:
        """Return the fixed-status fake response, ignoring the request details."""
        return FakeResponse(self.__status)


def make_cli(binary_path: Path, *, xdg: Path | None = None, shared_critical: bool = False) -> ProtonCLI:
    """Build a ProtonCLI, bypassing `create()`; `shared_critical` opts into the real class-shared state."""
    cli = object.__new__(ProtonCLI)
    attrs = {
        "_ProtonCLI__path": str(binary_path),
        "_ProtonCLI__xdg": str(xdg) if xdg is not None else "/tmp",  # noqa: S108
        "_ProtonCLI__integration_version": "test",
        "_ProtonCLI__reap_tasks": set(),
    }
    if not shared_critical:
        attrs["_ProtonCLI__critical"] = ProtonCLI._CriticalState()
    for name, value in attrs.items():
        setattr(cli, name, value)
    return cli


def patch_http_session(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    """Patch ProtonCLI's HTTP session factory to return a fixed-status FakeSession."""
    monkeypatch.setattr(ProtonCLI, "_ProtonCLI__http_session", classmethod(lambda _cls: FakeSession(status)))


def write_mock(tmp_path: Path, name: str, body: str) -> Path:
    """Write an executable one-line shell script standing in for the real CLI binary."""
    script = tmp_path / name
    script.write_text(f"#!/bin/sh\n{body}\n")
    script.chmod(0o700)
    return script


def write_sleep_maybe_fail_mock(tmp_path: Path) -> Path:
    """Write a mock that sleeps `$1` seconds then fails only if `$2` is 'fail'."""
    return write_mock(
        tmp_path,
        "sleep_maybe_fail.sh",
        'sleep "$1"; [ "$2" = "fail" ] && { echo boom >&2; exit 1; }; exit 0',
    )


def write_auth_session(xdg: Path, *, age_s: float | None = None) -> Path:
    """Write a fake auth-session.json, optionally backdating its mtime by `age_s`."""
    session_dir = xdg / "proton-drive-cli"
    session_dir.mkdir(parents=True, exist_ok=True)
    path = session_dir / "auth-session.json"
    path.write_text(json.dumps({"session": {"uid": "u", "accessToken": "t"}, "userKeyPassword": "p"}))
    if age_s is not None:
        stamp = time.time() - age_s
        os.utime(path, (stamp, stamp))
    return path


async def _run_n_parallel(cli: ProtonCLI, n: int, *, flag: str = "ok") -> float:
    """Run `n` calls concurrently against the sleep-maybe-fail mock, returning elapsed wall time."""
    start = time.monotonic()
    calls = (cli.run(str(CALL_DURATION_S), flag, is_json=False, timeout_s=5, retries=0) for _ in range(n))
    await asyncio.gather(*calls)
    return time.monotonic() - start


async def test_no_auth_file_runs_fully_in_parallel(tmp_path: Path) -> None:
    """With no auth-session.json at all, calls must run in parallel, not serialize."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    cli = make_cli(script, xdg=tmp_path)

    elapsed = await _run_n_parallel(cli, ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES + EXTRA_PARALLEL_CALLS)

    assert elapsed < CALL_DURATION_S * 3


async def test_fresh_auth_file_runs_fully_in_parallel(tmp_path: Path) -> None:
    """A recently written auth-session.json must not trigger serialization."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path)
    cli = make_cli(script, xdg=tmp_path)

    elapsed = await _run_n_parallel(cli, ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES + EXTRA_PARALLEL_CALLS)

    assert elapsed < CALL_DURATION_S * 3


async def test_critical_section_serializes_while_stale(tmp_path: Path) -> None:
    """While auth looks stale, calls must serialize instead of running in parallel, streak after streak."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path, age_s=ProtonCLI.STALE_AUTH_THRESHOLD.total_seconds() + 1)
    cli = make_cli(script, xdg=tmp_path)

    required = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
    total_calls = required + EXTRA_PARALLEL_CALLS
    elapsed = await _run_n_parallel(cli, total_calls)

    # The auth file is never refreshed here, so every call (not just the first streak) must serialize.
    assert elapsed >= total_calls * CALL_DURATION_S * 0.75


async def test_critical_section_returns_to_parallel_once_auth_refreshes(tmp_path: Path) -> None:
    """Once the auth session file is refreshed mid-streak, calls must return to running in parallel."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path, age_s=ProtonCLI.STALE_AUTH_THRESHOLD.total_seconds() + 1)
    cli = make_cli(script, xdg=tmp_path)

    required = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
    for _ in range(required - 1):
        await cli.run(str(CALL_DURATION_S), "ok", is_json=False, timeout_s=5, retries=0)
    write_auth_session(tmp_path)  # simulate the CLI refreshing the session before the streak's last call
    await cli.run(str(CALL_DURATION_S), "ok", is_json=False, timeout_s=5, retries=0)

    elapsed = await _run_n_parallel(cli, required + EXTRA_PARALLEL_CALLS)
    assert elapsed < CALL_DURATION_S * 3


async def test_critical_section_rearms_if_still_stale_after_streak(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """If the auth session is still stale once the streak completes, calls must keep serializing and warn."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path, age_s=ProtonCLI.STALE_AUTH_THRESHOLD.total_seconds() + 1)
    cli = make_cli(script, xdg=tmp_path)

    required = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
    with caplog.at_level(logging.WARNING, logger="proton_drive"):
        for _ in range(required):
            await cli.run(str(CALL_DURATION_S), "ok", is_json=False, timeout_s=5, retries=0)

    assert any("TTL needs tuning" in record.message for record in caplog.records)

    elapsed = await _run_n_parallel(cli, required)
    assert elapsed >= required * CALL_DURATION_S * 0.5


async def test_critical_section_requires_fresh_streak_after_failure(tmp_path: Path) -> None:
    """A failure mid-streak must require a full fresh streak of N successes, not just one more."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path, age_s=ProtonCLI.STALE_AUTH_THRESHOLD.total_seconds() + 1)
    cli = make_cli(script, xdg=tmp_path)

    required = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
    for _ in range(required - 1):
        await cli.run(str(CALL_DURATION_S), "ok", is_json=False, timeout_s=5, retries=0)
    with pytest.raises(CLIError):
        await cli.run(str(CALL_DURATION_S), "fail", is_json=False, timeout_s=5, retries=0)

    elapsed = await _run_n_parallel(cli, required)
    assert elapsed >= required * CALL_DURATION_S * 0.5


async def test_critical_section_wakes_waiters_after_failure(tmp_path: Path) -> None:
    """A failure mid-streak must still wake other tasks parked on the condition, not hang them."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path, age_s=ProtonCLI.STALE_AUTH_THRESHOLD.total_seconds() + 1)
    cli = make_cli(script, xdg=tmp_path)

    n_calls = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES + 2

    async def call(index: int) -> None:
        flag = "fail" if index == 1 else "ok"
        await cli.run(str(CALL_DURATION_S), flag, is_json=False, timeout_s=5, retries=0)

    results = await asyncio.wait_for(
        asyncio.gather(*(call(i) for i in range(n_calls)), return_exceptions=True),
        timeout=n_calls * CALL_DURATION_S * 20 + 5,
    )

    assert isinstance(results[1], CLIError)
    for i, result in enumerate(results):
        if i != 1:
            assert result is None


async def test_two_instances_share_critical_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failure on one ProtonCLI instance must reset another instance's shared streak too."""
    monkeypatch.setattr(ProtonCLI, "_ProtonCLI__critical", ProtonCLI._CriticalState())
    script = write_sleep_maybe_fail_mock(tmp_path)
    write_auth_session(tmp_path, age_s=ProtonCLI.STALE_AUTH_THRESHOLD.total_seconds() + 1)
    cli_a = make_cli(script, xdg=tmp_path, shared_critical=True)
    cli_b = make_cli(script, xdg=tmp_path, shared_critical=True)

    required = ProtonCLI.STALE_AUTH_REQUIRED_SUCCESSES
    for _ in range(required - 1):
        await cli_a.run(str(CALL_DURATION_S), "ok", is_json=False, timeout_s=5, retries=0)
    with pytest.raises(CLIError):
        await cli_b.run(str(CALL_DURATION_S), "fail", is_json=False, timeout_s=5, retries=0)

    elapsed = await _run_n_parallel(cli_a, required)
    assert elapsed >= required * CALL_DURATION_S * 0.5


@pytest.mark.parametrize("status", [HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN])
async def test_api_call_401_403_raise_auth_error(
    tmp_path: Path,
    status: HTTPStatus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 401/403 from Proton's API must surface as AuthError."""
    write_auth_session(tmp_path)
    cli = make_cli(tmp_path / "unused", xdg=tmp_path)
    patch_http_session(monkeypatch, status)

    with pytest.raises(AuthError):
        await cli.get_email()


async def test_api_call_500_raises_api_unavailable_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A 5xx from Proton's API must surface as APIUnavailableError, a retryable CLIError subtype."""
    write_auth_session(tmp_path)
    cli = make_cli(tmp_path / "unused", xdg=tmp_path)
    patch_http_session(monkeypatch, HTTPStatus.INTERNAL_SERVER_ERROR)

    with pytest.raises(APIUnavailableError):
        await cli.get_email()


async def test_api_call_400_stays_cli_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-auth, non-5xx error status must stay a plain CLIError."""
    write_auth_session(tmp_path)
    cli = make_cli(tmp_path / "unused", xdg=tmp_path)
    patch_http_session(monkeypatch, HTTPStatus.BAD_REQUEST)

    with pytest.raises(CLIError) as exc_info:
        await cli.get_email()
    assert type(exc_info.value) is CLIError


def test_parse_proc_io() -> None:
    """Only rchar/wchar are taken from /proc/<pid>/io; missing fields give None."""
    text = "rchar: 2048\nwchar: 1024\nsyscr: 5\nsyscw: 3\nread_bytes: 0\nwrite_bytes: 0\ncancelled_write_bytes: 0\n"
    assert cli_module._parse_proc_io(text) == (2048, 1024)
    assert cli_module._parse_proc_io("syscr: 5\n") is None


@pytest.mark.skipif(not pathlib.Path("/proc/self/io").exists(), reason="needs Linux /proc/<pid>/io")
async def test_on_io_reports_transfer_progress(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A process that reads a file must be reported with at least the file size (bytes read, rchar)."""
    data = tmp_path / "archive.tar"
    data.write_bytes(os.urandom(2 * 1024 * 1024))
    script = tmp_path / "read_then_wait.py"
    script.write_text("#!/usr/bin/env python3\nimport sys, time\nsrc = open(sys.argv[1], 'rb').read()\ntime.sleep(1)\n")
    script.chmod(0o700)
    monkeypatch.setattr(ProtonCLI, "IO_POLL_INTERVAL_S", 0.1)
    cli = make_cli(script, xdg=tmp_path)
    seen: list[int] = []

    await cli.run(str(data), is_json=False, timeout_s=10, retries=0, on_io=seen.append)

    assert seen, "on_io was never called"
    assert max(seen) >= data.stat().st_size


async def test_on_io_callback_errors_do_not_break_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing progress callback must not affect the command itself."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    monkeypatch.setattr(ProtonCLI, "IO_POLL_INTERVAL_S", 0.05)

    async def fake_proc_io(_pid: int) -> tuple[int, int]:
        return (10, 20)

    monkeypatch.setattr(cli_module, "_proc_io", fake_proc_io)
    cli = make_cli(script, xdg=tmp_path)

    def broken(_transferred: int) -> None:
        raise RuntimeError

    assert await cli.run("0.3", "ok", is_json=False, timeout_s=5, retries=0, on_io=broken) is None


async def test_on_io_stops_without_proc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without /proc (not Linux) the watcher ends quietly and the callback is never called."""
    script = write_sleep_maybe_fail_mock(tmp_path)
    monkeypatch.setattr(ProtonCLI, "IO_POLL_INTERVAL_S", 0.05)

    async def no_proc_io(_pid: int) -> None:
        return None

    monkeypatch.setattr(cli_module, "_proc_io", no_proc_io)
    cli = make_cli(script, xdg=tmp_path)
    seen: list[int] = []

    await cli.run("0.3", "ok", is_json=False, timeout_s=5, retries=0, on_io=seen.append)

    assert seen == []


# --- Download der CLI, Auth-Flow, Ausgabe-Dekodierung ----------------------------------------------------------


class _BrokenBody:
    """Response body that delivers one chunk and then loses the connection."""

    async def iter_chunked(self, _size: int) -> AsyncIterator[bytes]:
        yield b"partial"
        msg = "connection lost"
        raise aiohttp.ClientError(msg)


class _BrokenResponse:
    content = _BrokenBody()

    def raise_for_status(self) -> None:
        return

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return


class _BrokenDownloadSession:
    def get(self, _url: str) -> _BrokenResponse:
        return _BrokenResponse()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return


async def test_download_network_failure_is_retryable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A dropped connection mid-download is a network error (setup is retried), not a permanent startup error."""

    async def platform_() -> tuple[str, str]:
        return "glibc", "x64"

    monkeypatch.setattr(ProtonCLI, "_ProtonCLI__http_session", classmethod(lambda _cls: _BrokenDownloadSession()))
    monkeypatch.setattr(ProtonCLI, "_ProtonCLI__get_platform", classmethod(lambda _cls: platform_()))
    dest = tmp_path / "proton-drive"

    with pytest.raises(cli_module.DownloadCLINetworkError):
        await ProtonCLI._ProtonCLI__download(None, dest)

    assert not dest.exists()
    assert not dest.with_suffix(".tmp").exists()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def _wait_dead(pid: int, timeout_s: float = 5) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return True
        await asyncio.sleep(0.05)
    return False


async def test_auth_flow_failure_to_start_does_not_leak_the_process(tmp_path: Path) -> None:
    """If the CLI's first line is unusable, `auth login` must not be left running in the background."""
    pidfile = tmp_path / "pid"
    mock = write_mock(tmp_path, "login.sh", f"echo $$ > {pidfile}; echo garbage; sleep 30")
    cli = make_cli(mock)

    with pytest.raises(CLIError):
        await cli.start_auth_flow()

    assert await _wait_dead(int(pidfile.read_text()))


async def test_auth_flow_timeout_terminates_the_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the user never finishes the login, the timed-out `auth login` process must be stopped."""
    pidfile = tmp_path / "pid"
    mock = write_mock(tmp_path, "login.sh", f'echo $$ > {pidfile}; echo \'{{"signInUrl": "https://x"}}\'; sleep 30')
    monkeypatch.setattr(ProtonCLI.AuthFlow, "FLOW_TIMEOUT", 0.5)
    cli = make_cli(mock)

    flow = await cli.start_auth_flow()
    with pytest.raises(CLIError):
        await flow.task()

    assert await _wait_dead(int(pidfile.read_text()))


async def test_non_utf8_output_does_not_crash(tmp_path: Path) -> None:
    """Output that is not valid UTF-8 (e.g. a file name) must surface as a CLIError, not a UnicodeDecodeError."""
    mock = write_mock(tmp_path, "bad.sh", "printf 'bad \\377 name' >&2; exit 1")
    cli = make_cli(mock)

    with pytest.raises(CLIError):
        await cli.run("x", retries=0)
