"""Activity view — per-reviewer audit log across all review TSVs.

Reads `reviewer` and `reviewed_at` columns from the four review files and
surfaces totals, daily breakdowns, and a recent-decisions feed.
"""

import csv
import pathlib
import collections
from datetime import datetime, timedelta, timezone

import streamlit as st

ORG_DIR = pathlib.Path(__file__).parents[2] / "organizations"

SOURCES = {
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
def _load_decisions(mtimes: tuple) -> list[dict]:
    """Returns one dict per stamped row across all sources."""
    out: list[dict] = []
    for source, path in SOURCES.items():
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
                    "source": source,
                    "reviewer": rev,
                    "ts": ts,
                    "day": ts[:10],
                    "id": _row_id(row),
                    "label": _row_label(row),
                    "decision": row.get("decision", "").strip(),
                })
    return out


def _mtimes() -> tuple:
    return tuple((p.stat().st_mtime if p.exists() else 0.0) for p in SOURCES.values())


def render():
    st.header("📋 Activity")
    st.caption("Per-RA audit log built from the `reviewer` + `reviewed_at` columns in each review TSV.")

    decisions = _load_decisions(_mtimes())
    if not decisions:
        st.info("No stamped decisions found yet.")
        return

    reviewers = sorted({d["reviewer"] for d in decisions})
    sources = sorted({d["source"] for d in decisions})

    fcol1, fcol2, fcol3 = st.columns([1.2, 1.2, 1])
    with fcol1:
        sel_reviewers = st.multiselect("Reviewer", reviewers, default=reviewers, key="act_revs")
    with fcol2:
        sel_sources = st.multiselect("Source", sources, default=sources, key="act_srcs")
    with fcol3:
        days_back = st.number_input("Days back", min_value=1, max_value=365, value=14, step=1, key="act_days")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(days_back))).strftime("%Y-%m-%d")
    visible = [
        d for d in decisions
        if d["reviewer"] in sel_reviewers
        and d["source"] in sel_sources
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
    st.dataframe(totals_rows, use_container_width=True, hide_index=True)

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
    st.dataframe(day_rows, use_container_width=True, hide_index=True)

    # ── Recent decisions feed ────────────────────────────────────────────────
    st.subheader("Recent decisions")
    feed_limit = st.slider("Show last N", min_value=10, max_value=500, value=50, step=10, key="act_feed_n")
    recent = sorted(visible, key=lambda d: d["ts"], reverse=True)[:feed_limit]
    feed_rows = [
        {
            "When (UTC)": d["ts"],
            "Reviewer": d["reviewer"],
            "Source": d["source"],
            "ID": d["id"],
            "Decision": d["decision"],
            "Label": d["label"],
        }
        for d in recent
    ]
    st.dataframe(feed_rows, use_container_width=True, hide_index=True)
