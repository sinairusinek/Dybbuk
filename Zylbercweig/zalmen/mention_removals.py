"""Mention removals as an append-only overlay.

A reviewer removing a mention used to blank its cluster_id directly in
organizations_clustered.tsv and rewrite all 34 MB — the one save in the app
that touched that file. That was wrong twice over:

  * the rewrite was never pushed to GitHub, unlike every sibling save, so the
    edit lived only on the container's ephemeral disk and vanished on the next
    redeploy; and
  * rewriting 34 MB on a Cloud container is slow enough to be interrupted, and
    an interrupted rewrite left a truncated table that the next read could not
    decode (see atomic_io).

So removals are recorded here instead: one short line per decision in
mention_removals.tsv, pushed like any other decision file, and applied as an
overlay when the clustered rows are read. The big table is never written by
the app at all. `apply_mention_removals.py` folds the overlay into the TSV
permanently when the pipeline is next run locally.

A mention has no id of its own, so it is keyed by a hash of the six fields
that identify it. That combination is unique across all 16,454 rows; the
obvious shorter keys are not — (File, xml:id) alone collides on 13,672 rows,
and adding cluster_id still leaves 35 collisions.
"""

from __future__ import annotations

import csv
import hashlib
import pathlib

from atomic_io import atomic_write

REMOVALS_FILE = (
    pathlib.Path(__file__).resolve().parents[1] / "organizations" / "mention_removals.tsv"
)
REMOVALS_REPO_PATH = "Zylbercweig/organizations/mention_removals.tsv"

HEADERS = [
    "mention_key", "action", "cluster_id", "file", "xml_id",
    "chunk_number", "organization", "reviewer", "ts",
]

# The six fields that together identify one mention row.
_KEY_COLS = (
    "File",
    "_ - xml:id",
    "cluster_id",
    "clustered organization",
    "﻿_ - chunk_number",
    "_ - organizations - _ - relations - _ - original_sentence",
)
_COL_CID = "cluster_id"


def mention_key(row: dict[str, str]) -> str:
    """Stable id for a clustered-organization mention."""
    raw = "␟".join((row.get(c) or "").strip() for c in _KEY_COLS)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def get_mtime() -> float:
    return REMOVALS_FILE.stat().st_mtime if REMOVALS_FILE.exists() else 0.0


def cluster_cache_key(cluster_file: pathlib.Path) -> tuple[float, float]:
    """Cache key for any st.cache_data loader over the clustered TSV.

    Must include the removals mtime, or a fresh removal would not invalidate
    the cached rows and the mention would linger until the next redeploy.
    """
    cm = cluster_file.stat().st_mtime if cluster_file.exists() else 0.0
    return (cm, get_mtime())


def load_removed_keys() -> set[str]:
    """Keys whose most recent recorded action is REMOVE."""
    if not REMOVALS_FILE.exists():
        return set()
    state: dict[str, str] = {}
    with open(REMOVALS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            key = (row.get("mention_key") or "").strip()
            if key:
                state[key] = (row.get("action") or "").strip().upper()
    return {k for k, v in state.items() if v == "REMOVE"}


def apply_to_rows(rows: list[dict[str, str]]) -> int:
    """Blank cluster_id on removed mentions, in place. Returns how many.

    Every reader of the clustered TSV skips rows with an empty cluster_id, so
    blanking is exactly what the old in-place edit did — just not persisted
    into the 34 MB file.
    """
    removed = load_removed_keys()
    if not removed:
        return 0
    n = 0
    for row in rows:
        if row.get(_COL_CID, "").strip() and mention_key(row) in removed:
            row[_COL_CID] = ""
            n += 1
    return n


def record(row: dict[str, str], reviewer: str, action: str = "REMOVE") -> str:
    """Append one decision and return its mention_key. Caller pushes."""
    import datetime as _dt

    key = mention_key(row)
    rec = {
        "mention_key": key,
        "action": action,
        "cluster_id": row.get(_COL_CID, "").strip(),
        "file": row.get("File", "").strip(),
        "xml_id": row.get("_ - xml:id", "").strip(),
        "chunk_number": row.get("﻿_ - chunk_number", "").strip(),
        "organization": row.get("clustered organization", "").strip(),
        "reviewer": reviewer,
        "ts": _dt.datetime.now().isoformat(timespec="seconds"),
    }
    existing: list[dict[str, str]] = []
    if REMOVALS_FILE.exists():
        with open(REMOVALS_FILE, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f, delimiter="\t"))
    existing.append(rec)
    with atomic_write(REMOVALS_FILE) as f:
        w = csv.DictWriter(f, fieldnames=HEADERS, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(existing)
    return key
