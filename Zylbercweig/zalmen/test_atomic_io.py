"""Unit tests for atomic_io.atomic_write.

Reviewer saves rewrite whole TSVs. Before atomic_write these truncated the real
file first, so a save interrupted by a Streamlit Cloud redeploy left a
half-written table — and because the cut usually lands mid-Yiddish-character,
the next read died with UnicodeDecodeError. These tests pin the guarantee that
an interrupted write leaves the previous file untouched.

Run: python -m pytest Zylbercweig/zalmen/test_atomic_io.py
"""
from __future__ import annotations

import csv
import pathlib
import sys

import pytest

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

from atomic_io import atomic_write  # noqa: E402

YIDDISH = ["ייִדישער אַרטיסטן פֿאַראיין", "װילנער טרופּע"]


def _write(path, rows):
    with atomic_write(path) as f:
        csv.writer(f, delimiter="\t").writerows(rows)


def test_round_trips_yiddish(tmp_path):
    p = tmp_path / "t.tsv"
    _write(p, [["a", "b"], YIDDISH])
    with open(p, newline="", encoding="utf-8") as f:
        assert list(csv.reader(f, delimiter="\t"))[1] == YIDDISH


def test_crash_mid_write_leaves_original_intact(tmp_path):
    p = tmp_path / "t.tsv"
    _write(p, [["a", "b"], YIDDISH])
    before = p.read_bytes()

    with pytest.raises(RuntimeError):
        with atomic_write(p) as f:
            f.write("half a table")
            raise RuntimeError("redeploy mid-save")

    assert p.read_bytes() == before
    # and the partial file must still decode — the symptom we are guarding
    p.read_text(encoding="utf-8")


def test_crash_leaves_no_temp_files(tmp_path):
    p = tmp_path / "t.tsv"
    _write(p, [["a"]])
    with pytest.raises(RuntimeError):
        with atomic_write(p) as f:
            f.write("x")
            raise RuntimeError("boom")
    assert [q.name for q in tmp_path.iterdir()] == ["t.tsv"]


def test_replaces_existing_file(tmp_path):
    p = tmp_path / "t.tsv"
    _write(p, [["old"]])
    _write(p, [["new"]])
    assert p.read_text(encoding="utf-8").startswith("new")


def test_creates_missing_parent_dirs(tmp_path):
    p = tmp_path / "sub" / "dir" / "t.tsv"
    _write(p, [["a"]])
    assert p.exists()


def test_newline_handling_matches_plain_open(tmp_path):
    """csv writes CRLF; atomic_write must pass it through like open(newline="")."""
    a, b = tmp_path / "a.tsv", tmp_path / "b.tsv"
    with atomic_write(a) as f:
        csv.writer(f, delimiter="\t").writerow(["x", "y"])
    with open(b, "w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter="\t").writerow(["x", "y"])
    assert a.read_bytes() == b.read_bytes()
