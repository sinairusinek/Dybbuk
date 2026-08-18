"""Unit tests for the mention-removal overlay.

Removals used to be written straight into organizations_clustered.tsv, which
was never pushed (so the edit died on the next redeploy) and rewrote 34 MB on
a Cloud container (so an interrupted write corrupted the table). They are now
recorded as an append-only overlay and applied at read time.

Run: python -m pytest Zylbercweig/zalmen/test_mention_removals.py
"""
from __future__ import annotations

import csv
import pathlib
import sys

import pytest

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

import mention_removals as mr  # noqa: E402

CID, FILE, XID = "cluster_id", "File", "_ - xml:id"
ORG, CHUNK = "clustered organization", "﻿_ - chunk_number"
SENT = "_ - organizations - _ - relations - _ - original_sentence"


def _row(cid="C1", fle="f.xml", xid="x1", org="װילנער טרופּע", chunk="1", sent="s"):
    return {CID: cid, FILE: fle, XID: xid, ORG: org, CHUNK: chunk, SENT: sent}


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(mr, "REMOVALS_FILE", tmp_path / "mention_removals.tsv")


def test_key_is_stable_and_content_addressed():
    assert mr.mention_key(_row()) == mr.mention_key(_row())
    assert mr.mention_key(_row()) != mr.mention_key(_row(xid="x2"))
    # cluster_id is part of the key: the same mention in another cluster is another key
    assert mr.mention_key(_row()) != mr.mention_key(_row(cid="C2"))


def test_key_survives_unrelated_column_edits():
    r = _row()
    r["_ - heading"] = "changed"
    r["confidence"] = "0.9"
    assert mr.mention_key(r) == mr.mention_key(_row())


def test_record_then_overlay_blanks_the_cluster_id():
    rows = [_row(xid="x1"), _row(xid="x2")]
    mr.record(rows[0], "Ruthie")
    n = mr.apply_to_rows(rows)
    assert n == 1
    assert rows[0][CID] == "" and rows[1][CID] == "C1"


def test_removal_survives_a_reload():
    """The point of the overlay: the decision outlives the container."""
    mr.record(_row(xid="x1"), "Ruthie")
    fresh = [_row(xid="x1")]          # table re-read from git, untouched
    assert mr.apply_to_rows(fresh) == 1
    assert fresh[0][CID] == ""


def test_restore_wins_over_earlier_remove():
    row = _row()
    mr.record(row, "Ruthie", "REMOVE")
    mr.record(row, "Ruthie", "RESTORE")
    rows = [_row()]
    assert mr.apply_to_rows(rows) == 0
    assert rows[0][CID] == "C1"


def test_file_is_append_only_and_well_formed():
    mr.record(_row(xid="x1"), "Ruthie")
    mr.record(_row(xid="x2"), "Noa")
    with open(mr.REMOVALS_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        recs = list(r)
        assert r.fieldnames == mr.HEADERS
    assert len(recs) == 2
    assert [x["reviewer"] for x in recs] == ["Ruthie", "Noa"]
    assert recs[0]["organization"] == "װילנער טרופּע"   # Yiddish round-trips


def test_no_decisions_is_a_cheap_noop():
    rows = [_row()]
    assert mr.apply_to_rows(rows) == 0
    assert rows[0][CID] == "C1"


def test_cache_key_changes_when_a_removal_is_recorded(tmp_path):
    cluster = tmp_path / "c.tsv"
    cluster.write_text("x", encoding="utf-8")
    before = mr.cluster_cache_key(cluster)
    mr.record(_row(), "Ruthie")
    assert mr.cluster_cache_key(cluster) != before
