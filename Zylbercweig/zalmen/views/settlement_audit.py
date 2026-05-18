"""Settlement audit — same-city + same-type lens for DB dedup sweeps.

Pick a settlement and an org_type → see every DB row + every cluster in that
bucket. Designed for proactive DB hygiene: spot duplicate DB entries, find
unaligned clusters that share an entity with an existing DB row, surface
cross-cluster merge candidates.

Itinerant types are excluded by the index.
"""
from __future__ import annotations

import pathlib
import sys

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

from organizations.settlement_index import get_index


def _open_url(view: str, entity: str = "") -> str:
    """Build a deep-link URL into another view."""
    parts = [f"view={view}"]
    if entity:
        parts.append(f"entity={entity}")
    return "?" + "&".join(parts)


def render() -> None:
    st.header("Settlement audit")
    st.caption(
        "Browse all DB rows + clusters by (settlement, type). Use for DB dedup "
        "sweeps and to spot unaligned clusters that belong to an existing card. "
        "Itinerant types are excluded."
    )

    try:
        ix = get_index()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not build settlement index: {exc}")
        return

    cities = ix.cities()  # [(qid, english, yiddish), ...]
    if not cities:
        st.warning("No resolved settlements in index — check that kimatch outputs exist.")
        return

    def _city_label(t: tuple[str, str, str]) -> str:
        qid, en, yi = t
        bits = en or qid
        if yi and yi != en:
            bits += f" · {yi}"
        return bits

    # Pre-select from a deep link (set by buttons in Organizations matching).
    target_qid = st.session_state.pop("audit_target_qid", None)
    target_type = st.session_state.pop("audit_target_type", None)
    if target_qid:
        match = next((c for c in cities if c[0] == target_qid), None)
        if match is not None:
            st.session_state["settlement_audit_city"] = match

    col1, col2 = st.columns([2, 2])
    with col1:
        city = st.selectbox(
            "Settlement",
            cities,
            format_func=_city_label,
            key="settlement_audit_city",
        )
    qid = city[0]
    types_here = ix.org_types_in_city(qid)
    if target_type and target_type in types_here:
        st.session_state["settlement_audit_type"] = target_type
    # Annotate each type with totals for that city
    def _type_label(t: str) -> str:
        b = ix.bucket(qid, t)
        n_db = len(b.db_cards) if b else 0
        n_cl = len(b.clusters) if b else 0
        return f"{t}  ·  {n_db} DB / {n_cl} clusters"
    with col2:
        org_type = st.selectbox(
            "Org type",
            types_here,
            format_func=_type_label,
            key="settlement_audit_type",
        )

    bucket = ix.bucket(qid, org_type)
    if not bucket:
        st.info("No entries in this bucket.")
        return

    st.divider()

    top = st.columns(4)
    top[0].metric("DB rows", len(bucket.db_cards))
    top[1].metric("Clusters", len(bucket.clusters))
    n_aligned = sum(1 for c in bucket.clusters if c.decision == "ALIGN")
    n_undecided = sum(1 for c in bucket.clusters if not c.decision)
    top[2].metric("Clusters aligned", n_aligned)
    top[3].metric("Clusters undecided", n_undecided)

    db_col, cl_col = st.columns(2, gap="large")

    with db_col:
        st.subheader(f"DB rows · {len(bucket.db_cards)}")
        st.caption("Same city + same type. Scan for duplicates.")
        if not bucket.db_cards:
            st.info("No DB rows for this bucket. Unaligned clusters here are likely NEW candidates.")
        # Dedupe by db_id (a row can be present once per location; bucket is per-city already, but be safe)
        seen: set[str] = set()
        unique_db = []
        for d in bucket.db_cards:
            if d.db_id in seen:
                continue
            seen.add(d.db_id)
            unique_db.append(d)
        # Sort by name for easy duplicate-spotting
        unique_db.sort(key=lambda d: (d.name or d.name_yiddish or "").lower())

        # How many clusters point to each db_id?
        align_counts: dict[str, int] = {}
        for c in bucket.clusters:
            if c.aligned_db_id:
                align_counts[c.aligned_db_id] = align_counts.get(c.aligned_db_id, 0) + 1

        for d in unique_db:
            with st.container(border=True):
                name = d.name or "(no Latin name)"
                yi = d.name_yiddish or ""
                head = f"**{d.db_id}** · {name}"
                if yi:
                    head += f"  ·  {yi}"
                st.markdown(head)
                meta_bits = []
                if d.confirmed_settlement:
                    meta_bits.append(f"📍 {d.confirmed_settlement}")
                aligned_n = align_counts.get(d.db_id, 0)
                if aligned_n:
                    meta_bits.append(f"🔗 {aligned_n} cluster{'s' if aligned_n != 1 else ''} aligned")
                if meta_bits:
                    st.caption(" · ".join(meta_bits))
                st.link_button(
                    "Open in Organization Cards ↗",
                    _open_url("Organization Cards", d.db_id),
                )

    with cl_col:
        st.subheader(f"Clusters · {len(bucket.clusters)}")
        st.caption("Decision status shown. Undecided clusters first.")
        # Filter
        f_col1, f_col2 = st.columns(2)
        with f_col1:
            decision_filter = st.multiselect(
                "Decision",
                options=["(undecided)", "ALIGN", "NEW", "SPLIT", "DEFER", "DESCRIPTIVE", "DISCUSS"],
                default=["(undecided)", "ALIGN", "NEW"],
                key=f"sa_dec_{qid}_{org_type}",
            )
        with f_col2:
            min_size = st.number_input(
                "Min cluster size", min_value=1, value=1, step=1, key=f"sa_size_{qid}_{org_type}"
            )

        def _show(c) -> bool:
            tag = c.decision or "(undecided)"
            if decision_filter and tag not in decision_filter:
                return False
            if c.cluster_size < min_size:
                return False
            return True

        clusters_visible = [c for c in bucket.clusters if _show(c)]
        clusters_visible.sort(key=lambda c: (bool(c.decision), -c.cluster_size))

        if not clusters_visible:
            st.info("No clusters match filter.")
        for c in clusters_visible[:100]:
            with st.container(border=True):
                title = c.canonical_yiddish or "(no canonical)"
                badge = f"**{c.cluster_id}** · n={c.cluster_size}"
                if c.decision:
                    badge += f" · {c.decision}"
                    if c.aligned_db_id:
                        badge += f" → {c.aligned_db_id}"
                else:
                    badge += " · _undecided_"
                st.markdown(badge)
                st.markdown(
                    f"<div dir='rtl' style='font-size:1.05rem'>{title}</div>",
                    unsafe_allow_html=True,
                )
                st.link_button(
                    "Open in Organizations matching ↗",
                    _open_url("Organizations matching", c.cluster_id),
                )
        if len(clusters_visible) > 100:
            st.caption(f"… and {len(clusters_visible) - 100} more (refine filters)")
