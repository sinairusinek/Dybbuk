"""Settlement audit — same-city + same-type lens for DB dedup sweeps.

Pick a settlement and an org_type → see every DB row + every cluster in that
bucket. Select 2+ items to merge DB rows, align clusters to a DB row, or mint
a new DB entity from clusters — all without leaving the view. Expand any row
to see its mentions, variants, and DB candidates inline.

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

from views.org_alignment import (
    ALIGN_FILE,
    CORE_DB_FILE,
    CLUSTER_FILE,
    load_alignment,
    load_core_db,
    load_samples,
    save_alignment,
    save_core_db,
    get_entry_text,
    _split_pipe,
    _next_db_id,
    _mtime,
    _JSON_TO_XML,
)


def _open_url(view: str, entity: str = "") -> str:
    parts = [f"view={view}"]
    if entity:
        parts.append(f"entity={entity}")
    return "?" + "&".join(parts)


def _merge_linked_ids(*values: str) -> str:
    seen: list[str] = []
    for v in values:
        for piece in (p.strip() for p in (v or "").replace(",", "|").split("|")):
            if piece and piece not in seen:
                seen.append(piece)
    return " | ".join(seen)


def _render_cluster_details(c, a_rows_by_cid, db_by_id, samples) -> None:
    """Inline expander body for a cluster: variants, samples, candidates."""
    row = a_rows_by_cid.get(c.cluster_id)
    if row is None:
        st.caption("No alignment row found.")
        return

    variants = _split_pipe(row.get("name_variants", ""))
    if variants:
        st.markdown("**Variants**")
        st.write(" | ".join(variants))

    for label, key in (
        ("Settlements", "extracted_settlements"),
        ("Addresses", "extracted_addresses"),
        ("Venues", "extracted_venues"),
        ("Countries", "extracted_countries"),
    ):
        val = (row.get(key) or "").strip()
        if val:
            st.markdown(f"**{label}**")
            st.write(val)

    s = samples.get(c.cluster_id, {})
    sample_list = s.get("samples", []) if s else []
    if sample_list:
        st.markdown("**Attestations**")
        for i, (head, sent, fle, xid) in enumerate(sample_list, start=1):
            st.markdown(f"{i}. **{head or '(no heading)'}**")
            if sent:
                st.caption(sent)
            if fle and xid:
                with st.expander(f"Full entry context ({xid})"):
                    full = get_entry_text(fle, xid)
                    if full:
                        st.markdown(
                            f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; "
                            f"line-height:1.6;'>{full}</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.caption(f"Entry not found in XML ({_JSON_TO_XML.get(fle, fle)}).")

    c_ids = _split_pipe(row.get("candidate_db_ids", ""))
    c_scores = _split_pipe(row.get("candidate_scores", ""))
    c_methods = _split_pipe(row.get("candidate_methods", ""))
    if c_ids:
        st.markdown("**DB candidates**")
        for i, dbid in enumerate(c_ids):
            db = db_by_id.get(dbid, {})
            score_txt = c_scores[i] if i < len(c_scores) else ""
            method_txt = c_methods[i] if i < len(c_methods) else ""
            icon = {"exact": "🎯", "phonetic": "🔊", "fuzzy": "🔤"}.get(method_txt, "•")
            st.caption(
                f"{icon} {dbid} · {db.get('name','(missing)')} · "
                f"score {score_txt} · {method_txt}"
            )
    notes = (row.get("reviewer_notes") or "").strip()
    if notes:
        st.markdown(f"**Reviewer notes** · {notes}")


def _render_db_details(d, a_rows, db_rows) -> None:
    db_full = next((r for r in db_rows if r.get("db_id") == d.db_id), {})
    if db_full.get("address"):
        st.markdown(f"**Address** · {db_full['address']}")
    if db_full.get("name"):
        st.markdown(f"**Name (Latin)** · {db_full['name']}")
    if db_full.get("name_yiddish"):
        st.markdown(f"**Name (Yiddish)** · {db_full['name_yiddish']}")
    if db_full.get("linked_cluster_ids"):
        st.markdown(f"**Linked clusters** · {db_full['linked_cluster_ids']}")
    aligned = [
        r for r in a_rows
        if (r.get("aligned_db_id") or "").strip() == d.db_id
    ]
    if aligned:
        st.markdown(f"**Currently aligned ({len(aligned)})**")
        for r in aligned[:10]:
            st.caption(
                f"{r.get('cluster_id','')} · {r.get('canonical_yiddish','')} "
                f"({r.get('decision','')})"
            )


def _action_bar(
    selected_db_ids: list[str],
    selected_cluster_ids: list[str],
    bucket,
    db_by_id: dict[str, dict[str, str]],
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
    sel_key_prefix: str,
) -> None:
    n_db = len(selected_db_ids)
    n_cl = len(selected_cluster_ids)
    st.markdown(f"**Selection** · {n_cl} cluster(s), {n_db} DB row(s)")

    a_rows_by_cid = {r["cluster_id"]: r for r in a_rows}

    cols = st.columns([1, 1, 1, 1])

    # --- Align clusters → one DB row -----------------------------------
    with cols[0]:
        align_disabled = not (n_cl >= 1 and n_db == 1)
        if st.button("Align selected → DB row", disabled=align_disabled,
                     type="primary", use_container_width=True,
                     key=f"{sel_key_prefix}_btn_align"):
            target = selected_db_ids[0]
            for cid in selected_cluster_ids:
                row = a_rows_by_cid.get(cid)
                if not row:
                    continue
                row["decision"] = "ALIGN"
                row["aligned_db_id"] = target
            # Fold cluster ids into the DB row's linked_cluster_ids
            for r in db_rows:
                if r.get("db_id") == target:
                    r["linked_cluster_ids"] = _merge_linked_ids(
                        r.get("linked_cluster_ids", ""),
                        *selected_cluster_ids,
                    )
                    break
            save_alignment(a_headers, a_rows)
            save_core_db(db_headers, db_rows)
            load_alignment.clear()
            load_core_db.clear()
            get_index.cache_clear()
            for cid in selected_cluster_ids:
                st.session_state.pop(f"{sel_key_prefix}_cl_{cid}", None)
            st.session_state.pop(f"{sel_key_prefix}_db_{target}", None)
            st.toast(f"Aligned {n_cl} cluster(s) → {target}", icon="✅")
            st.rerun()

    # --- Create new DB entity from clusters ---------------------------
    with cols[1]:
        new_disabled = not (n_cl >= 1 and n_db == 0)
        if new_disabled:
            st.button("Create new DB entity", disabled=True,
                      use_container_width=True,
                      key=f"{sel_key_prefix}_btn_new_disabled")
        else:
            seed_name = ""
            for cid in selected_cluster_ids:
                row = a_rows_by_cid.get(cid)
                if row and (row.get("canonical_yiddish") or "").strip():
                    seed_name = row["canonical_yiddish"].strip()
                    break
            with st.popover("Create new DB entity",
                            use_container_width=True):
                new_name = st.text_input(
                    "New organization name (Yiddish)", value=seed_name,
                    key=f"{sel_key_prefix}_new_name",
                )
                new_name_latin = st.text_input(
                    "Latin name (optional)", value="",
                    key=f"{sel_key_prefix}_new_latin",
                )
                if st.button("Confirm: create + align all selected",
                             key=f"{sel_key_prefix}_btn_new_confirm",
                             type="primary"):
                    next_id = _next_db_id(db_rows)
                    new_row = {h: "" for h in db_headers}
                    new_row.update({
                        "db_id": str(next_id),
                        "name": new_name_latin.strip(),
                        "name_yiddish": new_name.strip(),
                        "org_type": bucket.org_type.title(),
                        "address": "",
                        "linked_cluster_ids": _merge_linked_ids(*selected_cluster_ids),
                    })
                    db_rows.append(new_row)
                    for cid in selected_cluster_ids:
                        row = a_rows_by_cid.get(cid)
                        if not row:
                            continue
                        row["decision"] = "NEW"
                        row["aligned_db_id"] = str(next_id)
                    save_core_db(db_headers, db_rows)
                    save_alignment(a_headers, a_rows)
                    load_alignment.clear()
                    load_core_db.clear()
                    get_index.cache_clear()
                    for cid in selected_cluster_ids:
                        st.session_state.pop(f"{sel_key_prefix}_cl_{cid}", None)
                    st.toast(f"Created DB {next_id} from {n_cl} cluster(s)", icon="✅")
                    st.rerun()

    # --- Merge DB rows -------------------------------------------------
    with cols[2]:
        merge_disabled = n_db < 2
        if merge_disabled:
            st.button("Merge DB rows", disabled=True,
                      use_container_width=True,
                      key=f"{sel_key_prefix}_btn_merge_disabled")
        else:
            with st.popover(f"Merge {n_db} DB rows", use_container_width=True):
                st.caption("Pick the primary row to keep. Others will be removed "
                           "and their linked clusters re-pointed to the primary.")
                primary = st.radio(
                    "Primary",
                    options=selected_db_ids,
                    format_func=lambda d: f"{d} · {db_by_id.get(d, {}).get('name','')}",
                    key=f"{sel_key_prefix}_merge_primary",
                )
                confirm = st.checkbox(
                    "I understand this removes the other DB rows.",
                    key=f"{sel_key_prefix}_merge_confirm",
                )
                if st.button("Confirm merge", type="primary",
                             disabled=not confirm,
                             key=f"{sel_key_prefix}_btn_merge_confirm"):
                    secondaries = [d for d in selected_db_ids if d != primary]
                    # 1. Collect linked_cluster_ids from all
                    primary_row = next((r for r in db_rows if r.get("db_id") == primary), None)
                    if primary_row is None:
                        st.error("Primary row not found.")
                        st.stop()
                    pieces = [primary_row.get("linked_cluster_ids", "")]
                    for r in db_rows:
                        if r.get("db_id") in secondaries:
                            pieces.append(r.get("linked_cluster_ids", ""))
                    primary_row["linked_cluster_ids"] = _merge_linked_ids(*pieces)
                    # 2. Re-point alignment rows
                    for r in a_rows:
                        if (r.get("aligned_db_id") or "").strip() in secondaries:
                            r["aligned_db_id"] = primary
                    # 3. Remove secondary DB rows
                    db_rows[:] = [r for r in db_rows if r.get("db_id") not in secondaries]
                    save_core_db(db_headers, db_rows)
                    save_alignment(a_headers, a_rows)
                    load_alignment.clear()
                    load_core_db.clear()
                    get_index.cache_clear()
                    for dbid in selected_db_ids:
                        st.session_state.pop(f"{sel_key_prefix}_db_{dbid}", None)
                    st.toast(
                        f"Merged {len(secondaries)} row(s) into {primary}",
                        icon="✅",
                    )
                    st.rerun()

    with cols[3]:
        if st.button("Clear selection", use_container_width=True,
                     key=f"{sel_key_prefix}_btn_clear"):
            for k in list(st.session_state.keys()):
                if isinstance(k, str) and k.startswith(sel_key_prefix + "_cl_"):
                    st.session_state[k] = False
                if isinstance(k, str) and k.startswith(sel_key_prefix + "_db_"):
                    st.session_state[k] = False
            st.rerun()


def render() -> None:
    st.header("Settlement audit")
    st.caption(
        "Browse all DB rows + clusters by (settlement, type). Select 2+ items "
        "to merge DB rows, align clusters to a DB row, or mint a new entity. "
        "Expand any row to see its mentions and candidates. "
        "Itinerant types are excluded."
    )

    try:
        ix = get_index()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not build settlement index: {exc}")
        return

    cities = ix.cities()
    if not cities:
        st.warning("No resolved settlements in index — check that kimatch outputs exist.")
        return

    def _city_label(t: tuple[str, str, str]) -> str:
        qid, en, yi = t
        bits = en or qid
        if yi and yi != en:
            bits += f" · {yi}"
        return bits

    target_qid = st.session_state.pop("audit_target_qid", None)
    target_type = st.session_state.pop("audit_target_type", None)
    if target_qid:
        match = next((c for c in cities if c[0] == target_qid), None)
        if match is not None:
            st.session_state["settlement_audit_city"] = match

    col1, col2 = st.columns([2, 2])
    with col1:
        city = st.selectbox(
            "Settlement", cities, format_func=_city_label,
            key="settlement_audit_city",
        )
    qid = city[0]
    types_here = ix.org_types_in_city(qid)
    if target_type and target_type in types_here:
        st.session_state["settlement_audit_type"] = target_type

    def _type_label(t: str) -> str:
        b = ix.bucket(qid, t)
        n_db = len(b.db_cards) if b else 0
        n_cl = len(b.clusters) if b else 0
        return f"{t}  ·  {n_db} DB / {n_cl} clusters"

    with col2:
        org_type = st.selectbox(
            "Org type", types_here, format_func=_type_label,
            key="settlement_audit_type",
        )

    bucket = ix.bucket(qid, org_type)
    if not bucket:
        st.info("No entries in this bucket.")
        return

    # Load editable data sources (cached on mtime).
    a_headers, a_rows = load_alignment(_mtime(ALIGN_FILE))
    db_headers, db_rows = load_core_db(_mtime(CORE_DB_FILE))
    samples = load_samples(_mtime(CLUSTER_FILE)) if CLUSTER_FILE.exists() else {}
    db_by_id = {r.get("db_id", ""): r for r in db_rows}

    sel_key_prefix = f"sa_sel_{qid}_{org_type}"

    st.divider()

    top = st.columns(4)
    top[0].metric("DB rows", len(bucket.db_cards))
    top[1].metric("Clusters", len(bucket.clusters))
    n_aligned = sum(1 for c in bucket.clusters if c.decision == "ALIGN")
    n_undecided = sum(1 for c in bucket.clusters if not c.decision)
    top[2].metric("Clusters aligned", n_aligned)
    top[3].metric("Clusters undecided", n_undecided)

    # Dedupe DB cards
    seen: set[str] = set()
    unique_db = []
    for d in bucket.db_cards:
        if d.db_id in seen:
            continue
        seen.add(d.db_id)
        unique_db.append(d)
    unique_db.sort(key=lambda d: (d.name or d.name_yiddish or "").lower())

    # Collect current selection from session_state (checkboxes persist by key).
    selected_db_ids = [
        d.db_id for d in unique_db
        if st.session_state.get(f"{sel_key_prefix}_db_{d.db_id}")
    ]
    selected_cluster_ids = [
        c.cluster_id for c in bucket.clusters
        if st.session_state.get(f"{sel_key_prefix}_cl_{c.cluster_id}")
    ]

    if selected_db_ids or selected_cluster_ids:
        with st.container(border=True):
            _action_bar(
                selected_db_ids,
                selected_cluster_ids,
                bucket,
                db_by_id,
                a_rows,
                a_headers,
                db_rows,
                db_headers,
                sel_key_prefix,
            )

    db_col, cl_col = st.columns(2, gap="large")

    align_counts: dict[str, int] = {}
    for c in bucket.clusters:
        if c.aligned_db_id:
            align_counts[c.aligned_db_id] = align_counts.get(c.aligned_db_id, 0) + 1

    with db_col:
        st.subheader(f"DB rows · {len(unique_db)}")
        st.caption("Same city + same type. Scan for duplicates.")
        if not unique_db:
            st.info("No DB rows for this bucket. Unaligned clusters here are likely NEW candidates.")

        for d in unique_db:
            with st.container(border=True):
                head_cols = st.columns([1, 11])
                with head_cols[0]:
                    st.checkbox(
                        " ", key=f"{sel_key_prefix}_db_{d.db_id}",
                        label_visibility="collapsed",
                    )
                with head_cols[1]:
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
                with st.expander("Details"):
                    _render_db_details(d, a_rows, db_rows)
                st.link_button(
                    "Open in Organization Cards ↗",
                    _open_url("Organization Cards", d.db_id),
                )

    with cl_col:
        st.subheader(f"Clusters · {len(bucket.clusters)}")
        st.caption("Decision status shown. Undecided clusters first.")
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
                "Min cluster size", min_value=1, value=1, step=1,
                key=f"sa_size_{qid}_{org_type}",
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

        a_rows_by_cid = {r["cluster_id"]: r for r in a_rows}
        for c in clusters_visible[:100]:
            with st.container(border=True):
                head_cols = st.columns([1, 11])
                with head_cols[0]:
                    st.checkbox(
                        " ", key=f"{sel_key_prefix}_cl_{c.cluster_id}",
                        label_visibility="collapsed",
                    )
                with head_cols[1]:
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
                with st.expander("Details · mentions & candidates"):
                    _render_cluster_details(c, a_rows_by_cid, db_by_id, samples)
                st.link_button(
                    "Open in Organizations matching ↗",
                    _open_url("Organizations matching", c.cluster_id),
                )
        if len(clusters_visible) > 100:
            st.caption(f"… and {len(clusters_visible) - 100} more (refine filters)")
