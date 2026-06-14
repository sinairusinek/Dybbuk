"""Activity view — per-reviewer audit log.

Primary source: `organizations/activity_log.tsv` — an append-only log written
by every decision-saving view (cluster pairs, alignment, addresses,
settlement audit, …).

Legacy fallback: the per-row `reviewer`/`reviewed_at` stamps on the four
review TSVs are still merged in so historical activity (before the log
existed) remains visible.
"""

import csv
import pathlib
import collections
from datetime import datetime, timedelta, timezone

import streamlit as st

ORG_DIR = pathlib.Path(__file__).parents[2] / "organizations"
ACTIVITY_LOG = ORG_DIR / "activity_log.tsv"

LEGACY_SOURCES = {
    "Alignment":  ORG_DIR / "org_alignment_review.tsv",
    "Pairs":      ORG_DIR / "cluster_pairs_review.tsv",
    "Addresses":  ORG_DIR / "org_addresses_review.tsv",
    "Core DB":    ORG_DIR / "core_db.tsv",
}

ID_COL_CANDIDATES = ("cluster_id", "db_id", "pair_id", "id")


def _row_id(row: dict) -> str:
    for c in ID_COL_CANDIDATES:
        v = row.get(c, "").strip()
        if v:
            return v
    return ""


def _row_label(row: dict) -> str:
    for c in ("canonical_yiddish", "name", "decision", "label"):
        v = row.get(c, "").strip()
        if v:
            return v
    return ""


@st.cache_data(show_spinner=False)
def _load_log(mtime: float) -> list[dict]:
    out: list[dict] = []
    if not ACTIVITY_LOG.exists():
        return out
    with open(ACTIVITY_LOG, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            ts = (row.get("ts") or "").strip()
            rev = (row.get("reviewer") or "").strip()
            if not ts or not rev:
                continue
            out.append({
                "ts": ts,
                "day": ts[:10],
                "reviewer": rev,
                "source": row.get("view", ""),
                "action": row.get("action", ""),
                "id": row.get("target_id", ""),
                "decision": row.get("decision", ""),
                "label": row.get("note", ""),
            })
    return out


@st.cache_data(show_spinner=False)
def _load_legacy(mtimes: tuple) -> list[dict]:
    """Pre-log activity recovered from per-row reviewer/reviewed_at stamps."""
    out: list[dict] = []
    for source, path in LEGACY_SOURCES.items():
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f, delimiter="\t")
            if not r.fieldnames or "reviewer" not in r.fieldnames or "reviewed_at" not in r.fieldnames:
                continue
            for row in r:
                rev = row.get("reviewer", "").strip()
                ts = row.get("reviewed_at", "").strip()
                if not rev or not ts:
                    continue
                out.append({
                    "ts": ts,
                    "day": ts[:10],
                    "reviewer": rev,
                    "source": source,
                    "action": "legacy_stamp",
                    "id": _row_id(row),
                    "decision": row.get("decision", "").strip(),
                    "label": _row_label(row),
                })
    return out


def _legacy_mtimes() -> tuple:
    return tuple((p.stat().st_mtime if p.exists() else 0.0) for p in LEGACY_SOURCES.values())


def _log_mtime() -> float:
    return ACTIVITY_LOG.stat().st_mtime if ACTIVITY_LOG.exists() else 0.0


def render():
    st.header("📋 Activity")
    st.caption(
        "Per-RA audit log. **Primary source:** the central activity log "
        "(`organizations/activity_log.tsv`) — every decision-saving action "
        "in the app writes one row. **Legacy fallback:** `reviewer` + "
        "`reviewed_at` row-stamps on the four review TSVs are merged in for "
        "history that predates the log."
    )

    log_rows = _load_log(_log_mtime())
    legacy_rows = _load_legacy(_legacy_mtimes())

    # Drop legacy rows whose (reviewer, ts) collide with a log row — avoids
    # double-counting the same decision once the log catches up.
    log_keys = {(d["reviewer"], d["ts"]) for d in log_rows}
    legacy_kept = [d for d in legacy_rows if (d["reviewer"], d["ts"]) not in log_keys]
    decisions = log_rows + legacy_kept

    if not decisions:
        st.info("No stamped decisions found yet.")
        return

    reviewers = sorted({d["reviewer"] for d in decisions})
    sources = sorted({d["source"] for d in decisions})
    actions = sorted({d["action"] for d in decisions})

    fcol1, fcol2, fcol3, fcol4 = st.columns([1.2, 1.2, 1.2, 1])
    with fcol1:
        sel_reviewers = st.multiselect("Reviewer", reviewers, default=reviewers, key="act_revs")
    with fcol2:
        sel_sources = st.multiselect("Source / view", sources, default=sources, key="act_srcs")
    with fcol3:
        sel_actions = st.multiselect("Action", actions, default=actions, key="act_acts")
    with fcol4:
        days_back = st.number_input("Days back", min_value=1, max_value=365, value=14, step=1, key="act_days")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(days_back))).strftime("%Y-%m-%d")
    visible = [
        d for d in decisions
        if d["reviewer"] in sel_reviewers
        and d["source"] in sel_sources
        and d["action"] in sel_actions
        and d["day"] >= cutoff
    ]

    if not visible:
        st.info("No decisions match the current filters.")
        return

    st.divider()

    # ── Totals per reviewer ──────────────────────────────────────────────────
    st.subheader("Totals per reviewer")
    by_rev: dict[str, dict[str, int]] = collections.defaultdict(lambda: collections.defaultdict(int))
    for d in visible:
        by_rev[d["reviewer"]]["Total"] += 1
        by_rev[d["reviewer"]][d["source"]] += 1
    totals_rows = []
    for rev in sorted(by_rev, key=lambda r: -by_rev[r]["Total"]):
        row = {"Reviewer": rev, "Total": by_rev[rev]["Total"]}
        for s in sources:
            row[s] = by_rev[rev].get(s, 0)
        totals_rows.append(row)
    st.dataframe(totals_rows, width="stretch", hide_index=True)

    # ── Action mix ───────────────────────────────────────────────────────────
    st.subheader("Actions performed")
    by_act: dict[str, dict[str, int]] = collections.defaultdict(lambda: collections.defaultdict(int))
    for d in visible:
        by_act[d["action"]]["Total"] += 1
        by_act[d["action"]][d["reviewer"]] += 1
    act_rows = []
    for act in sorted(by_act, key=lambda a: -by_act[a]["Total"]):
        row = {"Action": act, "Total": by_act[act]["Total"]}
        for rev in sel_reviewers:
            row[rev] = by_act[act].get(rev, 0)
        act_rows.append(row)
    st.dataframe(act_rows, width="stretch", hide_index=True)

    # ── Per-day breakdown ────────────────────────────────────────────────────
    st.subheader("Decisions per day")
    by_day_rev: dict[str, dict[str, int]] = collections.defaultdict(lambda: collections.defaultdict(int))
    for d in visible:
        by_day_rev[d["day"]][d["reviewer"]] += 1
    day_rows = []
    for day in sorted(by_day_rev.keys(), reverse=True):
        row = {"Day": day, "Total": sum(by_day_rev[day].values())}
        for rev in sel_reviewers:
            row[rev] = by_day_rev[day].get(rev, 0)
        day_rows.append(row)
    st.dataframe(day_rows, width="stretch", hide_index=True)

    # ── Recent decisions feed ────────────────────────────────────────────────
    st.subheader("Recent decisions")
    feed_limit = st.slider("Show last N", min_value=10, max_value=500, value=50, step=10, key="act_feed_n")
    recent = sorted(visible, key=lambda d: d["ts"], reverse=True)[:feed_limit]
    feed_rows = [
        {
            "When (UTC)": d["ts"],
            "Reviewer": d["reviewer"],
            "Source": d["source"],
            "Action": d["action"],
            "ID": d["id"],
            "Decision": d["decision"],
            "Note / label": d["label"],
        }
        for d in recent
    ]
    st.dataframe(feed_rows, width="stretch", hide_index=True)
