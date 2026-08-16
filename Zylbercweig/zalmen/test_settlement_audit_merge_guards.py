"""Guards against re-merging work a previous session already filed.

The settlement-audit activity log showed 75 repeat actions on 54 targets, and
86 of 86 clusters picked as a merge partner more than once were already aligned
to a DB row when they were re-picked — usually a day later, often into a
different target. These cover the three mechanisms that allowed it.

Run: python3.11 -m pytest Zylbercweig/zalmen/test_settlement_audit_merge_guards.py
(Zalmen runs on anaconda 3.11, not the repo .venv.)
"""

import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from views.settlement_audit import (  # noqa: E402
    _align_clusters_to_db,
    _aligned_target,
    _drop_linked_id,
    _merge_plan,
)


def _fixture():
    a_rows = [
        {"cluster_id": "C1", "canonical_yiddish": "פֿאָלקס-טעאַטער",
         "decision": "", "aligned_db_id": ""},
        {"cluster_id": "C2", "canonical_yiddish": "לאָנדאָנער פֿאָלקס-טעאַטער",
         "decision": "ALIGN", "aligned_db_id": "10"},
        {"cluster_id": "C3", "canonical_yiddish": "פּאַוויליאָן",
         "decision": "ALIGN", "aligned_db_id": "11"},
        {"cluster_id": "C4", "canonical_yiddish": "פֿאַרשוואונדן",
         "decision": "ALIGN", "aligned_db_id": "99"},  # dangling: DB 99 is gone
    ]
    db_rows = [
        {"db_id": "10", "name": "People's Theatre", "name_yiddish": "",
         "linked_cluster_ids": "C2"},
        {"db_id": "11", "name": "Pavilion", "name_yiddish": "",
         "linked_cluster_ids": "C3"},
    ]
    return a_rows, db_rows


def _maps(a_rows, db_rows):
    return ({r["cluster_id"]: r for r in a_rows},
            {r["db_id"]: r for r in db_rows})


# ─── _drop_linked_id ──────────────────────────────────────────────────────

def test_drop_linked_id():
    assert _drop_linked_id("A | B | C", "B") == "A | C"
    assert _drop_linked_id("A", "A") == ""
    assert _drop_linked_id("", "A") == ""
    assert _drop_linked_id("A,B", "A") == "B"       # legacy comma separator
    assert _drop_linked_id("A | B", "Z") == "A | B"  # absent id is a no-op


# ─── _aligned_target ──────────────────────────────────────────────────────

def test_free_cluster_has_no_target():
    a, d = _maps(*_fixture())
    assert _aligned_target("C1", a, d) == ""


def test_consumed_cluster_reports_its_row():
    a, d = _maps(*_fixture())
    assert _aligned_target("C2", a, d) == "10"


def test_dangling_pointer_is_not_resolved():
    """A cluster aimed at a deleted row still needs re-filing, so it must stay
    in the open list rather than hide in the resolved section."""
    a, d = _maps(*_fixture())
    assert _aligned_target("C4", a, d) == ""


# ─── _merge_plan: no-op detection ─────────────────────────────────────────

def test_merging_a_free_cluster_is_real_work():
    a, d = _maps(*_fixture())
    noop, warns = _merge_plan("db", "10", [("cluster", "C1")], a, d)
    assert noop is False and warns == []


def test_repicking_a_cluster_already_on_this_row_is_a_noop():
    a, d = _maps(*_fixture())
    noop, warns = _merge_plan("db", "10", [("cluster", "C2")], a, d)
    assert noop is True and warns == []


def test_two_clusters_already_on_the_same_row_is_a_noop():
    a_rows, db_rows = _fixture()
    a_rows[0]["aligned_db_id"] = "10"
    a_rows[0]["decision"] = "ALIGN"
    a, d = _maps(a_rows, db_rows)
    noop, _ = _merge_plan("cluster", "C1", [("cluster", "C2")], a, d)
    assert noop is True


# ─── _merge_plan: re-assignment warnings ──────────────────────────────────

def test_moving_a_cluster_off_another_row_warns():
    a, d = _maps(*_fixture())
    noop, warns = _merge_plan("db", "10", [("cluster", "C3")], a, d)
    assert noop is False
    assert len(warns) == 1 and "DB 11" in warns[0]


def test_absorbed_db_row_is_named():
    a, d = _maps(*_fixture())
    noop, warns = _merge_plan("db", "10", [("db", "11")], a, d)
    assert noop is False
    assert any("DB 11" in w and "removed" in w for w in warns)


def test_dangling_cluster_merges_without_a_warning():
    a, d = _maps(*_fixture())
    noop, warns = _merge_plan("db", "10", [("cluster", "C4")], a, d)
    assert noop is False and warns == []


def test_cluster_only_merge_targets_lowest_existing_row():
    """_consolidate_clusters keeps the lowest-numbered row, so the plan must
    predict a move off DB 11 — not off DB 10."""
    a, d = _maps(*_fixture())
    noop, warns = _merge_plan("cluster", "C2", [("cluster", "C3")], a, d)
    assert noop is False
    assert len(warns) == 1 and "DB 11" in warns[0]


# ─── _align_clusters_to_db: back-link hygiene ─────────────────────────────

def test_realigning_unlinks_the_previous_row(monkeypatch):
    a_rows, db_rows = _fixture()
    import views.settlement_audit as sa
    monkeypatch.setattr(sa, "_persist_and_clear", lambda *a, **k: None)

    msg = _align_clusters_to_db("10", ["C3"], a_rows, [], db_rows, [])

    by_id = {r["db_id"]: r for r in db_rows}
    assert by_id["11"]["linked_cluster_ids"] == ""       # old owner released it
    assert "C3" in by_id["10"]["linked_cluster_ids"]      # new owner claims it
    assert a_rows[2]["aligned_db_id"] == "10"
    assert "moved 1" in msg
    # The emptied previous row (DB 11 had only C3) is flagged, not silently
    # stranded — this is how the orphan-link backlog accumulated.
    assert "no clusters" in msg and "11" in msg


def test_aligning_a_free_cluster_touches_no_other_row(monkeypatch):
    a_rows, db_rows = _fixture()
    import views.settlement_audit as sa
    monkeypatch.setattr(sa, "_persist_and_clear", lambda *a, **k: None)

    msg = _align_clusters_to_db("10", ["C1"], a_rows, [], db_rows, [])

    by_id = {r["db_id"]: r for r in db_rows}
    assert by_id["11"]["linked_cluster_ids"] == "C3"
    assert by_id["10"]["linked_cluster_ids"] == "C2 | C1"
    assert "moved" not in msg
