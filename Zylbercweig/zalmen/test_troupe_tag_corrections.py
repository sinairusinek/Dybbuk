"""Unit tests for the Correct-saved-tags diff/validation in troupe_tags_review.

The data_editor itself can't be driven from AppTest, so the logic that decides
what gets written to troupe_tags.tsv is kept in a pure function and tested here.

Run: python -m pytest Zylbercweig/zalmen/test_troupe_tag_corrections.py
"""
from __future__ import annotations

import pathlib
import sys

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

from views.troupe_tags_review import _diff_and_validate  # noqa: E402


def _row(db_id, tags="", other=""):
    return {"db_id": db_id, "tags": tags, "other tags": other}


def test_no_edits_changes_nothing():
    rows = [_row("1", "Family Company"), _row("2", "Star Company")]
    changed, unknown, emptied = _diff_and_validate(rows, [dict(r) for r in rows])
    assert changed == [] and unknown == [] and emptied == []


def test_only_edited_rows_are_returned():
    before = [_row("1", "Family Company"), _row("2", "Star Company")]
    after = [_row("1", "Family Company"), _row("2", "Ensemble Company")]
    changed, _, _ = _diff_and_validate(before, after)
    assert [r["db_id"] for r in changed] == ["2"]


def test_other_tags_edit_counts_as_a_change():
    before = [_row("1", "Family Company", "")]
    after = [_row("1", "Family Company", "Klezmer Company")]
    changed, unknown, _ = _diff_and_validate(before, after)
    assert [r["db_id"] for r in changed] == ["1"]
    assert unknown == []  # free-text column is not vocabulary-checked


def test_unknown_vocabulary_tag_is_flagged():
    before = [_row("1", "Family Company")]
    after = [_row("1", "Family Company | Wandering Company")]
    changed, unknown, _ = _diff_and_validate(before, after)
    assert changed and unknown == [("1", "Wandering Company")]


def test_known_vocabulary_multi_tag_passes():
    before = [_row("1", "")]
    after = [_row("1", "Family Company | Operetta / Opera Company")]
    _, unknown, _ = _diff_and_validate(before, after)
    assert unknown == []


def test_cleared_row_is_reported_as_emptied():
    before = [_row("1", "Family Company", "something")]
    after = [_row("1", "", "")]
    changed, unknown, emptied = _diff_and_validate(before, after)
    assert changed and unknown == [] and emptied == ["1"]


def test_whitespace_and_spacing_are_normalised_not_flagged():
    before = [_row("1", "Family Company")]
    after = [_row("1", "  Family Company  ")]
    changed, unknown, _ = _diff_and_validate(before, after)
    # The raw strings differ, so it counts as an edit, but it must not be
    # reported as an unknown tag -- _split_tags strips before comparing.
    assert [r["db_id"] for r in changed] == ["1"]
    assert unknown == []


def test_rows_filtered_out_of_view_are_never_written():
    """The editor only ever sees the filtered view; a db_id absent from
    `before` must not be treated as an edit (guards against KeyError too)."""
    before = [_row("1", "Family Company")]
    after = [_row("1", "Family Company"), _row("99", "Star Company")]
    changed, _, _ = _diff_and_validate(before, after)
    assert changed == []
