"""Tests for the troupe-tag data plane (troupe_store).

The store's whole point: saves go to a data-only branch (never the deployed
one, so a save never restarts the server), and the data branch is merged back
into the local files at boot so no restart, stale seed, or transition-window
save on main can lose a row. These pin down the merge semantics, the target
branch, and the save-time re-merge that protects a failed boot fetch.

Run: python -m pytest Zylbercweig/zalmen/test_troupe_store.py
"""
from __future__ import annotations

import pathlib
import sys

import pytest

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

import troupe_store  # noqa: E402


def _tsv(headers: list[str], rows: list[dict]) -> bytes:
    lines = ["\t".join(headers)]
    for r in rows:
        lines.append("\t".join(r.get(h, "") for h in headers))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _tag_row(db_id: str, tags: str, when: str, reviewer: str = "Ruthie") -> dict:
    return {"db_id": db_id, "tags": tags, "other_tags": "", "comment": "",
            "reviewer_notes": "", "reviewer": reviewer, "reviewed_at": when}


@pytest.fixture
def store(monkeypatch, tmp_path):
    """Point the store at tmp files and reset its per-process boot state."""
    tags = tmp_path / "troupe_tags.tsv"
    rev = tmp_path / "troupe_tag_review.tsv"
    monkeypatch.setattr(troupe_store, "TROUPE_TAGS", tags)
    monkeypatch.setattr(troupe_store, "TAG_REVIEW", rev)
    monkeypatch.setattr(troupe_store, "_FILES", [
        (troupe_store.TROUPE_TAGS_REPO_PATH, tags, troupe_store.TROUPE_TAG_HEADERS),
        (troupe_store.TAG_REVIEW_REPO_PATH, rev, troupe_store.TAG_REVIEW_HEADERS),
    ])
    monkeypatch.setattr(troupe_store, "_BOOT_MERGED", {})
    troupe_store.load_troupe_tags.clear()
    troupe_store.load_tag_review.clear()
    return tags, rev


def _write_local(path: pathlib.Path, rows: list[dict]) -> None:
    path.write_bytes(_tsv(troupe_store.TROUPE_TAG_HEADERS, rows))


# ── merge semantics ───────────────────────────────────────────────────────────

def test_merge_keeps_the_newer_row_in_either_direction():
    local = {"1": _tag_row("1", "Star Company", "2026-08-20T10:00:00Z"),
             "2": _tag_row("2", "Family Company", "2026-08-01T00:00:00Z")}
    remote = {"1": _tag_row("1", "Ensemble Company", "2026-08-19T00:00:00Z"),
              "2": _tag_row("2", "Ad Hoc Company", "2026-08-19T00:00:00Z")}
    merged = troupe_store._merge_newer(local, remote)
    assert merged["1"]["tags"] == "Star Company"      # local newer → kept
    assert merged["2"]["tags"] == "Ad Hoc Company"    # remote newer → taken


def test_merge_is_a_union_and_remote_wins_ties():
    local = {"1": _tag_row("1", "Star Company", "2026-08-20T10:00:00Z"),
             "3": _tag_row("3", "Family Company", "2026-08-10T00:00:00Z")}
    remote = {"1": _tag_row("1", "Ensemble Company", "2026-08-20T10:00:00Z"),
              "4": _tag_row("4", "Ad Hoc Company", "")}
    merged = troupe_store._merge_newer(local, remote)
    assert set(merged) == {"1", "3", "4"}             # nothing lost either way
    assert merged["1"]["tags"] == "Ensemble Company"  # tie → the data branch


# ── boot fetch ────────────────────────────────────────────────────────────────

def test_ensure_fresh_merges_the_data_branch_into_the_local_file(
        store, monkeypatch):
    tags, _rev = store
    _write_local(tags, [_tag_row("1", "Star Company", "2026-08-20T10:00:00Z")])
    remote = _tsv(troupe_store.TROUPE_TAG_HEADERS,
                  [_tag_row("2", "Family Company", "2026-08-19T00:00:00Z")])
    monkeypatch.setattr(troupe_store.github_sync, "fetch_file_from_github",
                        lambda rp, branch=None: remote)
    assert troupe_store.ensure_fresh() is True
    rows = troupe_store._read_rows(tags)
    assert set(rows) == {"1", "2"}  # checkout row survived, branch row arrived


def test_ensure_fresh_fetches_once_per_process(store, monkeypatch):
    calls = []
    monkeypatch.setattr(troupe_store.github_sync, "fetch_file_from_github",
                        lambda rp, branch=None: calls.append(rp) or
                        _tsv(troupe_store.TROUPE_TAG_HEADERS, []))
    troupe_store.ensure_fresh()
    troupe_store.ensure_fresh()
    assert len(calls) == 2  # two files, one fetch each — not four


def test_a_failed_fetch_leaves_the_checkout_copy_and_reports_it(
        store, monkeypatch):
    tags, _rev = store
    _write_local(tags, [_tag_row("1", "Star Company", "2026-08-20T10:00:00Z")])
    monkeypatch.setattr(troupe_store.github_sync, "fetch_file_from_github",
                        lambda rp, branch=None: None)
    assert troupe_store.ensure_fresh() is False
    assert troupe_store._read_rows(tags)["1"]["tags"] == "Star Company"


# ── saves ─────────────────────────────────────────────────────────────────────

def test_saves_push_to_the_data_branch_never_the_deployed_one(
        store, monkeypatch):
    troupe_store._BOOT_MERGED[troupe_store.TROUPE_TAGS_REPO_PATH] = True
    pushed = {}
    monkeypatch.setattr(
        troupe_store.github_sync, "push_files_to_github",
        lambda files, msg, branch=None: pushed.update(
            files=files, branch=branch) or True)
    ok = troupe_store.save_troupe_tags(
        [_tag_row("9", "Star Company", "2026-08-20T12:00:00Z")])
    assert ok is True
    assert pushed["branch"] == "zalmen-data"
    assert [rp for rp, _ in pushed["files"]] == [troupe_store.TROUPE_TAGS_REPO_PATH]


def test_a_save_after_a_failed_boot_fetch_remerges_before_writing(
        store, monkeypatch):
    """The push rewrites the whole file on the branch, so a save from a stale
    base must first pull the branch's rows back in — or it would erase them."""
    tags, _rev = store
    _write_local(tags, [_tag_row("1", "Star Company", "2026-08-01T00:00:00Z")])
    remote = _tsv(troupe_store.TROUPE_TAG_HEADERS,
                  [_tag_row("2", "Family Company", "2026-08-19T00:00:00Z")])
    monkeypatch.setattr(troupe_store.github_sync, "fetch_file_from_github",
                        lambda rp, branch=None: remote)
    troupe_store.save_troupe_tags(
        [_tag_row("9", "Ad Hoc Company", "2026-08-20T12:00:00Z")], push=False)
    rows = troupe_store._read_rows(tags)
    assert set(rows) == {"1", "2", "9"}  # branch row 2 was NOT clobbered


def test_an_empty_save_is_a_no_op(store):
    assert troupe_store.save_troupe_tags([]) is True
    assert troupe_store.save_tag_review([]) is True
