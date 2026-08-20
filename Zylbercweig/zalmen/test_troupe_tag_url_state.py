"""Regression tests for surviving a Streamlit Cloud redeploy in the troupe-tag view.

Every reviewer save pushes to the deployed branch, so the server restarts and the
session is dropped several times an hour. `?view=` (app.py) brings the reviewer
back to this view; these tests cover the two things inside it that would
otherwise reset — the section and the confidence tier — which is what "it jumped
back to the first page" means once the view itself is restored.

The real widgets can't be driven from AppTest here, so `st` is swapped for a
stand-in holding the same two dict-like surfaces the code touches.

Run: python -m pytest Zylbercweig/zalmen/test_troupe_tag_url_state.py
"""
from __future__ import annotations

import pathlib
import sys

import pytest

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parent)]

from views import troupe_tags_review as ttr  # noqa: E402


class _Stub:
    """Just enough Streamlit: session_state and query_params as plain dicts."""

    def __init__(self, query=None, state=None):
        self.query_params = dict(query or {})
        self.session_state = dict(state or {})


@pytest.fixture
def st(monkeypatch):
    stub = _Stub()
    monkeypatch.setattr(ttr, "st", stub)
    return stub


KEYS = ttr._GROUP_KEYS


# ── restore ───────────────────────────────────────────────────────────────────

def test_reconnect_restores_the_tier_she_was_working(st):
    st.query_params = {"section": "queue", "tier": "base"}
    ttr._restore_from_url(KEYS)
    assert st.session_state["ttr_group"] == KEYS.index("low (base only)")
    assert st.session_state["ttr_section"] == "Draft queue"


def test_reconnect_restores_the_corrections_section(st):
    st.query_params = {"section": "correct"}
    ttr._restore_from_url(KEYS)
    assert st.session_state["ttr_section"] == "Correct saved tags"


def test_no_params_touches_nothing(st):
    ttr._restore_from_url(KEYS)
    assert st.session_state == {}


def test_unknown_slug_is_ignored(st):
    st.query_params = {"tier": "nonsense"}
    ttr._restore_from_url(KEYS)
    assert "ttr_group" not in st.session_state


def test_a_stale_param_never_overrides_a_fresh_pick(st):
    """The trap app.py documents for ?view=: the URL still says the old tier on
    the rerun after she clicks a new one, and must not drag her back."""
    st.query_params = {"tier": "high"}
    ttr._restore_from_url(KEYS)
    assert st.session_state["ttr_group"] == KEYS.index("high")
    # she now picks Base only; the URL has not caught up yet
    st.session_state["ttr_group"] = KEYS.index("low (base only)")
    ttr._restore_from_url(KEYS)
    assert st.session_state["ttr_group"] == KEYS.index("low (base only)")


# ── mirror ────────────────────────────────────────────────────────────────────

def test_mirror_writes_both_params(st):
    ttr._mirror_to_url("Draft queue", "flagged")
    assert st.query_params == {"section": "queue", "tier": "flagged"}


def test_mirror_leaves_the_tier_alone_in_the_corrections_section(st):
    st.query_params = {"section": "queue", "tier": "medium"}
    ttr._mirror_to_url("Correct saved tags", None)
    assert st.query_params["section"] == "correct"
    assert st.query_params["tier"] == "medium"  # so going back lands where she was


def test_round_trip_survives_a_restart(st):
    """Mirror what she is working, then boot a fresh session with only the URL —
    the state a reconnect after a redeploy actually starts from."""
    ttr._mirror_to_url("Draft queue", "notext")
    carried = dict(st.query_params)
    st.session_state.clear()          # the redeploy
    st.query_params = carried         # the browser replays the URL
    ttr._restore_from_url(KEYS)
    assert st.session_state["ttr_group"] == KEYS.index("notext")
    assert st.session_state["ttr_section"] == "Draft queue"
