"""Tests for the upload progress estimate in ProtonDriveClient."""

from __future__ import annotations

from proton_drive.api import _upload_progress

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
