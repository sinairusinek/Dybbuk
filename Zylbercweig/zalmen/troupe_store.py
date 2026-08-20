"""Troupe-tag store — the tagger's data lives OFF the deployed branch.

Why this module exists: Streamlit Cloud redeploys (and drops every live
session) on any push to the branch it deploys. The old shape pushed every
troupe-tag save to that branch, so each save restarted the server and the
reviewer's session "jumped" — batching commits only reduced how often. This
store moves the tagger onto a two-plane architecture:

  deploy plane  — the branch Cloud deploys (`github_branch` secret, main).
                  Code lives here. Pushes here restart the server; the store
                  NEVER pushes here.
  data plane    — a data-only branch (`github_data_branch` secret, default
                  "zalmen-data"). Every save goes here. Cloud does not deploy
                  it, so a save never restarts the server: no more jumps from
                  the reviewer's own work.

The precondition github_sync's docstring always named for a branch split is the
boot-time fetch, and this store provides it: once per server process,
`ensure_fresh()` pulls each data file from the data branch and MERGES it with
the checkout copy row by row, newest `reviewed_at` per db_id winning (the data
branch winning ties). Merging rather than overwriting makes the store
self-healing: rows that only exist in the checkout (saves that landed on the
deploy plane before the split, or a stale data-branch seed) and rows that only
exist on the data branch (saves since the last code deploy) both survive, in
whichever direction the divergence runs.

If the boot fetch fails (token expired, API hiccup), reads fall back to the
checkout copy and every save retries the fetch-and-merge under the file lock
before writing, so a stale base is never pushed over newer data-branch rows
while the API is actually reachable.

Mirroring the data plane back into the repo history on main (for pipeline and
analysis work; the app itself never needs this):

    git fetch origin zalmen-data
    git checkout FETCH_HEAD -- \
        Zylbercweig/organizations/troupe_tags.tsv \
        Zylbercweig/organizations/troupe_tag_review.tsv
    git commit -m "chore: mirror troupe tags from zalmen-data" \
        -- Zylbercweig/organizations/troupe_tags.tsv \
           Zylbercweig/organizations/troupe_tag_review.tsv

Main's copies are a point-in-time mirror, not the truth — always re-mirror
before reading them in a pipeline.

One consequence of merging by union: a row DELETED from only one plane comes
back from the other at the next boot. To truly remove a row (a bogus db_id,
say), delete it on the data branch AND in main's mirror in the same sitting —
or, better, keep the row and mark it (that is what "Not a Troupe" is for).
"""

from __future__ import annotations

import csv
import fcntl
import io
import pathlib

import streamlit as st

from atomic_io import atomic_write
import github_sync

ORG = pathlib.Path(__file__).resolve().parents[1] / "organizations"

TROUPE_TAGS = ORG / "troupe_tags.tsv"
TROUPE_TAGS_REPO_PATH = "Zylbercweig/organizations/troupe_tags.tsv"

TROUPE_TAG_HEADERS = [
    "db_id", "tags", "other_tags",
    # `reviewer_notes` is machine provenance ("from draft (edit)") and is
    # overwritten on every save — `comment` is the reviewer's own free text and
    # is carried forward when a later save leaves it blank.
    "comment",
    "reviewer_notes", "reviewer", "reviewed_at",
]

TAG_REVIEW = ORG / "troupe_tag_review.tsv"
TAG_REVIEW_REPO_PATH = "Zylbercweig/organizations/troupe_tag_review.tsv"
TAG_REVIEW_HEADERS = ["db_id", "status", "final_tags", "reviewer", "reviewed_at"]

# One flat vocabulary — any number of tags may apply to a troupe. The former
# Layer A / Layer B split was dropped 2026-07-20: its premise was that the
# structural categories are mutually exclusive, which they are not (a family
# company can be built around one star; an institutional one can be run
# cooperatively). The distinction survives as a `layer` column in the
# definitions TSV, as metadata rather than as a UI constraint.
_TROUPE_TAG_OPTS = [
    # structural
    "Family Company",
    "Impresario Company",
    "Star Company",
    "Ensemble Company",
    "Cooperative Company",
    "Institutional Company",
    "Ad Hoc Company",
    # characteristics
    "Children's Company",
    "Operetta / Opera Company",
    "German-Jewish Company",
    "Amateur Company",
    # added 2026-08-10 (Ruthie): two new characteristic tags from the lexicon
    "Kleinkunst / Revue / Cabaret Company",
    "Marionette / Puppet Company",
    # added 2026-08-19 (Ruthie): the language/ethnicity axis, plus an escape
    # hatch. "Not a Troupe" is a disposition, not a description — it says the row
    # was mis-typed as a troupe upstream, and downstream org_type work should
    # pick it up. Keep it last so it reads as separate from the descriptive tags.
    "Non-Jewish Company",
    "Hebrew-Language Company",
    "Not a Troupe",
    # Dropped 2026-08-10 (Ruthie): Zionist, Socialist, Post-Holocaust, Bilingual
    # — never used, never assigned, unconfirmed leftovers from the 07-19 list.
]

# The store's files, with the headers a write must be normalised to
# (see feedback: save_* must enforce canonical headers at write time).
_FILES: list[tuple[str, pathlib.Path, list[str]]] = [
    (TROUPE_TAGS_REPO_PATH, TROUPE_TAGS, TROUPE_TAG_HEADERS),
    (TAG_REVIEW_REPO_PATH, TAG_REVIEW, TAG_REVIEW_HEADERS),
]
_HEADERS_BY_REPO_PATH = {rp: headers for rp, _lp, headers in _FILES}

# Per-process boot state: repo_path → whether the data-branch copy has been
# merged in. Not session_state — the merged file is process-wide, so one fetch
# serves every session this server hosts.
_BOOT_MERGED: dict[str, bool] = {}


def data_branch() -> str:
    try:
        return st.secrets.get("github_data_branch", "zalmen-data") or "zalmen-data"
    except Exception:  # noqa: BLE001
        return "zalmen-data"


def has_github() -> bool:
    """Whether pushes/fetches can work at all (token + repo configured)."""
    token, repo, _ = github_sync._credentials(None)
    return bool(token and repo)


def _split_tags(raw: str) -> list[str]:
    """Parse the pipe-delimited multi-value convention used across the app."""
    return [t.strip() for t in (raw or "").split("|") if t.strip()]


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


def _read_rows(path: pathlib.Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out[r.get("db_id", "")] = r
    return out


def _parse_rows(data: bytes) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in csv.DictReader(io.StringIO(data.decode("utf-8")), delimiter="\t"):
        out[r.get("db_id", "")] = r
    return out


def _write_rows(path: pathlib.Path, headers: list[str],
                rows: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_write(path) as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        for row in rows.values():
            w.writerow({k: row.get(k, "") for k in headers})


def _merge_newer(local: dict[str, dict], remote: dict[str, dict]) -> dict[str, dict]:
    """Union of both planes; where a db_id exists in both, the row with the
    newer `reviewed_at` wins, the data branch (remote) winning exact ties.
    ISO-8601 UTC strings compare correctly as strings; a missing timestamp
    counts as oldest."""
    merged = dict(local)
    for db_id, r_row in remote.items():
        l_row = merged.get(db_id)
        if l_row is None or (r_row.get("reviewed_at", "") or "") >= (l_row.get("reviewed_at", "") or ""):
            merged[db_id] = r_row
    return merged


def _merge_from_branch(repo_path: str, local_path: pathlib.Path,
                       headers: list[str]) -> bool:
    """Fetch one file from the data branch and merge it into the local copy.
    Caller holds the file lock. Returns False when the fetch failed (the local
    copy is left as it was)."""
    data = github_sync.fetch_file_from_github(repo_path, branch=data_branch())
    if data is None:
        return False
    try:
        remote = _parse_rows(data)
    except Exception:  # noqa: BLE001
        return False
    merged = _merge_newer(_read_rows(local_path), remote)
    _write_rows(local_path, headers, merged)
    return True


def ensure_fresh() -> bool:
    """Merge the data branch into the local files, once per server process.

    Call at the top of any view that reads the store. Idempotent and cheap
    after the first call. Returns True when every file has, at some point this
    process, been merged with its data-branch copy — False means reads are
    serving the checkout snapshot (still safe: saves retry the merge in-lock,
    see `_upsert`)."""
    ok = True
    for repo_path, local_path, headers in _FILES:
        if _BOOT_MERGED.get(repo_path):
            continue
        lock = local_path.with_suffix(".lock")
        lock.parent.mkdir(parents=True, exist_ok=True)
        with open(lock, "w") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                if _merge_from_branch(repo_path, local_path, headers):
                    _BOOT_MERGED[repo_path] = True
                else:
                    ok = False
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)
    if ok:
        load_troupe_tags.clear()
        load_tag_review.clear()
    return ok


def _upsert(repo_path: str, local_path: pathlib.Path, headers: list[str],
            records: list[dict]) -> None:
    """Merge N records (keyed db_id) into the local file under its lock.

    If this process never managed to merge the data branch in (boot fetch
    failed), try once more here, BEFORE applying the records — the push that
    follows a save rewrites the whole file on the data branch, and it must not
    rewrite it from a base that predates other sessions' saves."""
    lock = local_path.with_suffix(".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            if not _BOOT_MERGED.get(repo_path):
                if _merge_from_branch(repo_path, local_path, headers):
                    _BOOT_MERGED[repo_path] = True
            existing = _read_rows(local_path)
            for rec in records:
                existing[rec["db_id"]] = rec
            _write_rows(local_path, headers, existing)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


@st.cache_data(show_spinner=False)
def load_troupe_tags(mtime: float) -> dict[str, dict]:
    """db_id → row. Multi-value columns stay pipe-delimited strings here;
    _split_tags() parses them at the render site.
    Callers pass `_mtime(TROUPE_TAGS)` after calling `ensure_fresh()`."""
    return _read_rows(TROUPE_TAGS)


@st.cache_data(show_spinner=False)
def load_tag_review(mtime: float) -> dict[str, dict]:
    """db_id → audit row (accept/edit/reject/correct per draft)."""
    return _read_rows(TAG_REVIEW)


def save_troupe_tags(records: list[dict], push: bool = True) -> bool:
    """Upsert N troupe-tag rows (keyed on db_id) and push to the DATA branch.

    `push=False` writes the file but leaves the GitHub commit to the caller,
    so an action that also writes the audit file can land both in ONE commit
    (see `push_batch`). Returns False when the push was needed but failed —
    the rows are still saved locally and will ride along with the next
    successful push, since every push sends the whole merged file."""
    if not records:
        return True
    _upsert(TROUPE_TAGS_REPO_PATH, TROUPE_TAGS, TROUPE_TAG_HEADERS, records)
    load_troupe_tags.clear()
    if not push:
        return True
    return push_batch([TROUPE_TAGS_REPO_PATH],
                      f"chore: troupe tags ({len(records)} rows)")


def save_tag_review(records: list[dict], push: bool = True) -> bool:
    """Upsert N review-audit rows (keyed db_id); same contract as
    `save_troupe_tags`."""
    if not records:
        return True
    _upsert(TAG_REVIEW_REPO_PATH, TAG_REVIEW, TAG_REVIEW_HEADERS, records)
    load_tag_review.clear()
    if not push:
        return True
    return push_batch([TAG_REVIEW_REPO_PATH], "chore: troupe_tag_review")


def push_batch(repo_paths: list[str], commit_message: str) -> bool:
    """Push the store's files to the data branch in one commit.

    The one place a troupe-tag save leaves the machine. Never targets the
    deployed branch, so a save never restarts the Cloud server."""
    by_repo_path = {rp: lp for rp, lp, _h in _FILES}
    files = [(rp, by_repo_path[rp]) for rp in repo_paths if by_repo_path[rp].exists()]
    try:
        return github_sync.push_files_to_github(
            files, commit_message, branch=data_branch())
    except Exception:  # noqa: BLE001
        return False
