"""Tests for the eponym mint helpers.

Run: python -m pytest Zylbercweig/zalmen/test_people_mint.py
"""
from __future__ import annotations

import pathlib
import sys

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

import people_mint as pm  # noqa: E402

CRLF = b"db_id\thebname\tenglish\r\n1\t\xd7\x90\tA\r\n7\t\xd7\x91\tB\r\n"


def test_append_keeps_existing_bytes_and_crlf():
    out = pm.append_rows(CRLF, [{"db_id": "8", "hebname": "x", "english": "C"}])
    assert out.startswith(CRLF)
    assert out[len(CRLF):] == b"8\tx\tC\r\n"


def test_append_adds_missing_final_newline_in_files_own_style():
    lf = b"a\tb\n1\t2"
    assert pm.append_rows(lf, [{"a": "3", "b": "4"}]) == b"a\tb\n1\t2\n3\t4\n"


def test_drop_rows_keeps_header_and_terminator():
    out = pm.drop_rows(CRLF, lambda r: r["db_id"] != "1")
    assert out == b"db_id\thebname\tenglish\r\n7\t\xd7\x91\tB\r\n"


def test_next_db_id_is_high_water_over_all_files():
    other = b"db_id\tx\n692\t-\n"
    assert pm.next_db_id(CRLF, other) == 693
    assert pm.next_db_id(CRLF) == 8


def test_skeleton_ignores_vowel_letters_but_keeps_double_vav():
    assert pm.skeleton("מאגידזאנ") == pm.skeleton("מאַגיידזאָן")
    assert pm.skeleton("קרילאוו") != pm.skeleton("קאַרל")   # Krylov is not Karl


def test_candidates_match_surname_not_given_name():
    people = [
        {"db_id": "3840", "hebname": "ליאַ לאָרגמאַן", "english": "Lea Largman"},
        {"db_id": "1948", "hebname": "קלמן גוטמן", "english": "Kalmen Gutman"},
    ]
    hubs = [{"hub_id": "HUB-D1948", "db_ids": "1948", "canonical_heading": "x"}]
    got = pm.candidates(["לארגמאנ"], "Leo Largman", people, hubs)
    assert [c["db_id"] for c in got] == ["3840"] and got[0]["hub_id"] == ""
    # a given name alone never offers anyone
    assert pm.candidates(["עבעל"], "Kalmen-Moyshe Ebel", people, hubs) == []


def test_not_attested_is_flagged_and_mentioned_is_not():
    no = pm.person_row("9", "י", "", "", "", "", "", "", "no", "s")
    yes = pm.person_row("9", "י", "", "", "", "", "", "", "mentions_only", "s")
    assert no["probably_not_zylbercweig"] == "1"
    assert yes["probably_not_zylbercweig"] == ""


def test_hub_row_carries_the_db_only_markers():
    h = pm.hub_row(["hub_id", "n_entries", "entry_person_ids", "evidence",
                    "db_ids", "non_person"], "3855", "עבעל")
    assert h == {"hub_id": "HUB-D3855", "n_entries": "0", "entry_person_ids": "",
                 "evidence": "db_only_mention_attested", "db_ids": "3855",
                 "non_person": "0"}
