"""Tests for saving a whole page of troupe cards in one commit.

Each save pushes to the branch Streamlit Cloud deploys and Cloud restarts on
every push, so saving 20 cards one at a time restarts the server 20 times. The
page save collapses that into one commit; what needs covering is which cards it
picks up, since the difference between "she changed this" and "she never looked
at it" decides whether a decision gets written about an unread row.

Run: python -m pytest Zylbercweig/zalmen/test_troupe_tag_page_save.py
"""
from __future__ import annotations

import pathlib
import sys

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

from views.troupe_tags_review import _split_page  # noqa: E402


def _card(db_id, tags=""):
    return {"db_id": db_id, "tags": tags}


def test_a_card_never_rendered_is_untouched():
    """No pills key yet — she has not scrolled to it, let alone read it."""
    touched, untouched = _split_page([_card("1", "Star Company")], {})
    assert touched == []
    assert untouched == [("1", ["Star Company"], "accept", "")]


def test_an_unchanged_card_is_untouched_not_edited():
    rows = [_card("1", "Star Company")]
    touched, untouched = _split_page(rows, {"1": (["Star Company"], "")})
    assert touched == []
    assert untouched[0][2] == "accept"


def test_changed_tags_count_as_touched():
    rows = [_card("1", "Star Company")]
    touched, untouched = _split_page(rows, {"1": (["Family Company"], "")})
    assert touched == [("1", ["Family Company"], "edit", "")]
    assert untouched == []


def test_reordered_tags_are_not_a_change():
    rows = [_card("1", "Star Company | Family Company")]
    picks = {"1": (["Family Company", "Star Company"], "")}
    touched, untouched = _split_page(rows, picks)
    assert touched == [] and len(untouched) == 1


def test_a_comment_alone_counts_as_touched():
    rows = [_card("1", "Star Company")]
    touched, _ = _split_page(rows, {"1": (["Star Company"], "  not sure  ")})
    assert touched == [("1", ["Star Company"], "edit", "not sure")]


def test_whitespace_only_comment_is_not_a_change():
    rows = [_card("1", "Star Company")]
    touched, untouched = _split_page(rows, {"1": (["Star Company"], "   ")})
    assert touched == [] and len(untouched) == 1


def test_clearing_every_tag_is_a_change():
    """Emptying a card is a decision, not an untouched draft."""
    rows = [_card("1", "Star Company")]
    touched, _ = _split_page(rows, {"1": ([], "")})
    assert touched == [("1", [], "edit", "")]


def test_tags_outside_the_vocabulary_are_not_treated_as_edits():
    """A stale tag in the draft file is filtered out of what the card shows, so
    it must not make an untouched card look changed."""
    rows = [_card("1", "Star Company | Wandering Company")]
    touched, untouched = _split_page(rows, {"1": (["Star Company"], "")})
    assert touched == [] and len(untouched) == 1


def test_a_mixed_page_splits_both_ways():
    rows = [_card("1", "Star Company"), _card("2", "Family Company"), _card("3")]
    picks = {"1": (["Star Company"], ""), "2": (["Ensemble Company"], "")}
    touched, untouched = _split_page(rows, picks)
    assert [t[0] for t in touched] == ["2"]
    assert sorted(u[0] for u in untouched) == ["1", "3"]


def test_every_tuple_matches_the_commit_batch_shape():
    rows = [_card("1", "Star Company"), _card("2", "Family Company")]
    touched, untouched = _split_page(rows, {"2": (["Star Company"], "why?")})
    for db_id, tags, status, comment in touched + untouched:
        assert isinstance(db_id, str) and isinstance(tags, list)
        assert status in {"accept", "edit", "reject"}
        assert isinstance(comment, str)
