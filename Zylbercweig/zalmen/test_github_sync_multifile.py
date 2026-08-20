"""Tests for the single-commit multi-file push.

Streamlit Cloud restarts the server on every push to the deployed branch, so an
action writing two TSVs used to drop every live session twice. These cover the
Git Data API sequence, the retry when a parallel session moves the branch, and
the rule that a save is never lost to the optimisation.

Run: python -m pytest Zylbercweig/zalmen/test_github_sync_multifile.py
"""
from __future__ import annotations

import pathlib
import sys

import pytest

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

import github_sync  # noqa: E402


class _Resp:
    def __init__(self, ok=True, payload=None, status=200):
        self.ok, self._payload, self.status_code = ok, payload or {}, status

    def json(self):
        return self._payload


class _FakeSession:
    """Records calls and replays scripted responses per (method, url-fragment)."""

    def __init__(self, ref_ok=True, patch_ok=True):
        self.calls: list[tuple[str, str]] = []
        self.ref_ok, self.patch_ok = ref_ok, patch_ok
        self.patches = 0

    def get(self, url, **kw):
        self.calls.append(("GET", url))
        if "/git/ref/" in url:
            return _Resp(self.ref_ok, {"object": {"sha": "HEAD1"}})
        if "/git/commits/" in url:
            return _Resp(True, {"tree": {"sha": "TREE0"}})
        return _Resp(True, {"sha": "CONTENTSSHA"})

    def post(self, url, **kw):
        self.calls.append(("POST", url))
        return _Resp(True, {"sha": "NEW"})

    def patch(self, url, **kw):
        self.calls.append(("PATCH", url))
        self.patches += 1
        # patch_ok False models a non-fast-forward: another session pushed first.
        return _Resp(self.patch_ok or self.patches > 1)

    def put(self, url, **kw):
        self.calls.append(("PUT", url))
        return _Resp(True, {"content": {"sha": "S"}})


@pytest.fixture
def files(tmp_path):
    a, b = tmp_path / "a.tsv", tmp_path / "b.tsv"
    a.write_text("a\n", encoding="utf-8")
    b.write_text("b\n", encoding="utf-8")
    return [("repo/a.tsv", a), ("repo/b.tsv", b)]


@pytest.fixture(autouse=True)
def secrets(monkeypatch):
    class _St:
        secrets = {"github_token": "t", "github_repo": "o/r", "github_branch": "main"}
    monkeypatch.setattr(github_sync, "st", _St)


def test_two_files_land_in_exactly_one_commit(monkeypatch, files):
    fake = _FakeSession()
    monkeypatch.setattr(github_sync, "_SESSION", fake)
    assert github_sync.push_files_to_github(files, "msg") is True
    posts = [u for m, u in fake.calls if m == "POST"]
    assert sum("/git/blobs" in u for u in posts) == 2   # one blob per file
    assert sum("/git/trees" in u for u in posts) == 1   # ONE tree
    assert sum("/git/commits" in u for u in posts) == 1  # ONE commit
    assert fake.patches == 1                            # ONE ref move


def test_a_parallel_session_moving_the_branch_is_retried(monkeypatch, files):
    fake = _FakeSession(patch_ok=False)  # first PATCH rejected, second accepted
    monkeypatch.setattr(github_sync, "_SESSION", fake)
    assert github_sync.push_files_to_github(files, "msg") is True
    assert fake.patches == 2


def test_a_failed_push_falls_back_to_one_commit_per_file(monkeypatch, files):
    """Churnier, but the save must never be lost to the optimisation."""
    fake = _FakeSession(ref_ok=False)  # Git Data API unusable
    monkeypatch.setattr(github_sync, "_SESSION", fake)
    called: list[str] = []
    monkeypatch.setattr(github_sync, "push_file_to_github",
                        lambda rp, lp, m: called.append(rp) or True)
    assert github_sync.push_files_to_github(files, "msg") is True
    assert called == ["repo/a.tsv", "repo/b.tsv"]


def test_a_single_file_still_uses_the_contents_api(monkeypatch, files):
    called: list[str] = []
    monkeypatch.setattr(github_sync, "push_file_to_github",
                        lambda rp, lp, m: called.append(rp) or True)
    assert github_sync.push_files_to_github(files[:1], "msg") is True
    assert called == ["repo/a.tsv"]


def test_no_files_is_a_no_op(monkeypatch):
    monkeypatch.setattr(github_sync, "_SESSION", _FakeSession())
    assert github_sync.push_files_to_github([], "msg") is True
