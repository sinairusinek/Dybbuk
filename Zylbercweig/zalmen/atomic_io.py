"""Crash-safe file writes.

Every reviewer decision rewrites its whole TSV. Truncating the real file first
(`open(path, "w")`) means an interrupted write — a Streamlit Cloud redeploy, an
OOM kill, a container restart mid-save — leaves a half-written file on disk. For
the big tables (organizations_clustered.tsv is ~34 MB) the cut usually lands in
the middle of a multi-byte Yiddish character, so the next read dies with a
UnicodeDecodeError and the view is unusable until the container is rebuilt.

`atomic_write` writes to a sibling temp file and `os.replace`s it into position,
which is atomic on POSIX. Readers see either the old file or the new one, never
a truncated one.
"""

from __future__ import annotations

import contextlib
import os
import pathlib
import tempfile
from typing import IO, Iterator


@contextlib.contextmanager
def atomic_write(
    path: os.PathLike | str,
    *,
    encoding: str = "utf-8",
    newline: str = "",
) -> Iterator[IO[str]]:
    """Open `path` for writing so that a failed write leaves it untouched."""
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    tmp = pathlib.Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline=newline) as f:
            yield f
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = None
    finally:
        if tmp is not None:
            with contextlib.suppress(OSError):
                tmp.unlink()
