"""Unit tests for core_db_merge.merge_core_db.

Pins the guarantee that a save from a stale copy carries only its own edits:
on 2026-10-25 one org_type change reverted 16 offline Theatre retypes.

Run: python -m pytest Zylbercweig/zalmen/test_core_db_merge.py
"""
from __future__ import annotations

import pathlib
import sys

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

from core_db_merge import merge_core_db, parse_tsv  # noqa: E402

H = ["db_id", "name", "org_type"]


def _rows(*triples):
    return [dict(zip(H, t)) for t in triples]


def _by(rows):
    return {r["db_id"]: r for r in rows}


def test_stale_save_keeps_remote_edits_and_applies_own():
    base = _rows(("1", "סטודיע", "Theatre"), ("2", "שול", "Education"))
    remote = _rows(("1", "סטודיע", "Theatre education"), ("2", "שול", "Education"))
    mine = _rows(("1", "סטודיע", "Theatre"), ("2", "שול", ""))
    _, out, conflicts = merge_core_db(base, mine, H, remote, H)
    assert _by(out)["1"]["org_type"] == "Theatre education"
    assert _by(out)["2"]["org_type"] == ""
    assert conflicts == []


def test_new_row_appended_and_remote_rows_kept():
    base = _rows(("1", "a", "Theatre"))
    remote = _rows(("1", "a", "Theatre"), ("2", "b", "Amateur"))
    mine = _rows(("1", "a", "Theatre"), ("3", "c", "Kleinkunst"))
    _, out, conflicts = merge_core_db(base, mine, H, remote, H)
    assert [r["db_id"] for r in out] == ["1", "2", "3"]
    assert conflicts == []


def test_minted_id_collision_reported():
    base = _rows(("1", "a", "Theatre"))
    remote = _rows(("1", "a", "Theatre"), ("2", "theirs", "Amateur"))
    mine = _rows(("1", "a", "Theatre"), ("2", "mine", "Kleinkunst"))
    _, out, conflicts = merge_core_db(base, mine, H, remote, H)
    assert conflicts == ["2"]
    assert len(out) == 2


def test_removed_row_removed_from_remote():
    base = _rows(("1", "a", "Theatre"), ("2", "b", "Theatre"))
    remote = _rows(("1", "a", "Kleinkunst"), ("2", "b", "Theatre"))
    mine = _rows(("1", "a", "Theatre"))
    _, out, _ = merge_core_db(base, mine, H, remote, H)
    assert [(r["db_id"], r["org_type"]) for r in out] == [("1", "Kleinkunst")]


def test_new_column_from_either_side_survives():
    base = _rows(("1", "a", "Theatre"))
    mine = _rows(("1", "a", "Theatre"))
    remote = [dict(r, extra="x") for r in _rows(("1", "a", "Theatre"))]
    headers, out, _ = merge_core_db(base, mine, H + ["extra"], remote, H)
    assert headers == H + ["extra"] and out[0]["extra"] == "x"


def test_parse_tsv_crlf_yiddish():
    hdr, rows = parse_tsv("db_id\tname\r\n1\tווילנער פֿילהאַרמאָניע\r\n".encode())
    assert hdr == ["db_id", "name"] and rows[0]["name"] == "ווילנער פֿילהאַרמאָניע"
