"""Alternative card-based org merge/align queue.

A denser alternative to the A1 list in `org_review`: every cluster in the review
queue is rendered as a CARD that already shows, without any drill-in:
  - its settlement(s),
  - its sample-text mention lines (heading + sentence),
  - inline action buttons — New entity / Align… / Merge….

Align and Merge open in an `st.popover` that stays on the page (no navigation to
a new browser tab, which is what the old `link_button` deep-links did). A "View
full entries" expander shows each mention's heading + full entry text.

Everything here is additive and reuses existing, battle-tested pieces:
  - data + text: `load_alignment` / `load_core_db` / `load_samples` /
    `get_entry_text` from `org_alignment` (the last via the shared lazy Lexicon
    loader),
  - mutations: `_align_clusters_to_db` / `_mint_db_from_clusters` /
    `_consolidate_clusters` / `_merge_db_rows_op` and `_search_corpus` from
    `settlement_audit`. Those encapsulate the dedup-safe merge logic, so
    re-running a merge can't mint duplicate DB rows.
"""
from __future__ import annotations

import pathlib
import sys

import streamlit as st

from zalmen.activity_log import log_action

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

from views.org_alignment import (  # noqa: E402
    ALIGN_FILE,
    CORE_DB_FILE,
    CLUSTER_FILE,
    load_alignment,
    load_core_db,
    load_samples,
    get_entry_text,
    _split_pipe,
    _mtime,
    _status,
    _JSON_TO_XML,
)
from views.settlement_audit import (  # noqa: E402
    _align_clusters_to_db,
    _mint_db_from_clusters,
    _consolidate_clusters,
    _merge_db_rows_op,  # noqa: F401 — kept for parity / future DB-row merges
    _search_corpus,
)

_PAGE_SIZE = 25
_SAMPLES_COLLAPSED = 3
_DECISION_OPTIONS = [
    "(undecided)", "ALIGN", "NEW", "DESCRIPTIVE", "SPLIT",
    "DEFER", "DISCUSS", "GENERIC", "UNCLUSTER",
]


def _settlements_for(row: dict, samples: dict, cid: str) -> list[str]:
    """Prefer the cluster's own extracted_settlements column; fall back to the
    settlements harvested into the samples index."""
    out: list[str] = []
    for piece in _split_pipe(row.get("extracted_settlements", "")):
        if piece not in out:
            out.append(piece)
    if not out:
        for piece in sorted((samples.get(cid, {}) or {}).get("settlements", []) or []):
            if piece not in out:
                out.append(piece)
    return out


# ─── inline action popover (New entity / Align / Merge) ───────────────────────

def _card_actions(
    *,
    cid: str,
    row: dict,
    org_type: str,
    canonical: str,
    a_rows: list[dict],
    a_headers: list[str],
    db_rows: list[dict],
    db_headers: list[str],
    db_by_id: dict[str, dict],
    samples: dict,
    reviewer: str,
) -> None:
    key = f"omc_{cid}"
    with st.popover("Actions ⋯", use_container_width=True):
        tab_align, tab_merge, tab_new = st.tabs(["Align", "Merge", "New entity"])

        # ── Align: pick a DB candidate or search the DB ──
        with tab_align:
            c_ids = _split_pipe(row.get("candidate_db_ids", ""))
            c_scores = _split_pipe(row.get("candidate_scores", ""))
            c_methods = _split_pipe(row.get("candidate_methods", ""))
            if c_ids:
                st.caption("Suggested DB candidates:")
                for i, dbid in enumerate(c_ids):
                    db = db_by_id.get(dbid, {})
                    name = db.get("name") or db.get("name_yiddish") or "(unnamed)"
                    score = c_scores[i] if i < len(c_scores) else ""
                    method = c_methods[i] if i < len(c_methods) else ""
                    if st.button(
                        f"Align → {dbid} · {name} · {score} {method}".strip(),
                        key=f"{key}_alc_{dbid}", use_container_width=True,
                    ):
                        msg = _align_clusters_to_db(
                            dbid, [cid], a_rows, a_headers, db_rows, db_headers,
                        )
                        log_action("org_merge_cards", "align", target_id=cid,
                                   decision="ALIGN", note=msg, partner=f"db:{dbid}")
                        st.toast(msg, icon="✅")
                        st.rerun()
            else:
                st.caption("No pre-computed DB candidates for this cluster.")
            st.divider()
            st.caption("…or search the DB by name:")
            q = st.text_input(
                "search db", key=f"{key}_dbq", label_visibility="collapsed",
                placeholder="DB name or fragment…",
            )
            if q and q.strip():
                hits = [
                    r for r in _search_corpus(
                        q.strip(), db_rows, a_rows, samples, exclude=("cluster", cid),
                    ) if r[0] == "db"
                ][:10]
                if not hits:
                    st.caption("No DB matches.")
                for _k, r_id, r_label, _s in hits:
                    if st.button(f"Align → {r_id} · {r_label}",
                                 key=f"{key}_als_{r_id}", use_container_width=True):
                        msg = _align_clusters_to_db(
                            r_id, [cid], a_rows, a_headers, db_rows, db_headers,
                        )
                        log_action("org_merge_cards", "align_via_search", target_id=cid,
                                   decision="ALIGN", note=msg, partner=f"db:{r_id}")
                        st.toast(msg, icon="✅")
                        st.rerun()

        # ── Merge: find another cluster or DB row that is the same entity ──
        with tab_merge:
            st.caption(
                "Find another cluster or DB row that is the *same entity* and "
                "merge. Re-running a merge is safe — it won't create duplicates."
            )
            q = st.text_input(
                "search corpus", key=f"{key}_mq", label_visibility="collapsed",
                placeholder="name or fragment…",
            )
            if q and q.strip():
                hits = _search_corpus(
                    q.strip(), db_rows, a_rows, samples, exclude=("cluster", cid),
                )[:15]
                if not hits:
                    st.caption("No matches.")
                for r_kind, r_id, r_label, _s in hits:
                    if st.button(
                        f"Merge with {r_kind} {r_id} · {r_label}",
                        key=f"{key}_mg_{r_kind}_{r_id}", use_container_width=True,
                    ):
                        if r_kind == "db":
                            msg = _align_clusters_to_db(
                                r_id, [cid], a_rows, a_headers, db_rows, db_headers,
                            )
                        else:
                            _new, msg = _consolidate_clusters(
                                [cid, r_id], org_type, canonical, "",
                                a_rows, a_headers, db_rows, db_headers,
                            )
                        log_action("org_merge_cards", "merge", target_id=cid,
                                   decision="MERGE", note=msg, partner=f"{r_kind}:{r_id}")
                        st.toast(msg, icon="✅")
                        st.rerun()

        # ── New entity: mint a fresh DB row (confirm the name first) ──
        with tab_new:
            st.caption("Create a brand-new DB entity from this cluster.")
            new_name = st.text_input(
                "New organization name (Yiddish)", value=canonical,
                key=f"{key}_newname",
            )
            new_latin = st.text_input(
                "Latin name (optional)", value="", key=f"{key}_newlatin",
            )
            if st.button("Create new entity", key=f"{key}_newbtn",
                         type="primary", use_container_width=True):
                new_id, msg = _mint_db_from_clusters(
                    [cid], org_type, new_name, new_latin,
                    a_rows, a_headers, db_rows, db_headers,
                )
                log_action("org_merge_cards", "mint", target_id=cid,
                           decision="NEW", note=msg, new_db_id=new_id)
                st.toast(msg, icon="✅")
                st.rerun()


# ─── one card ─────────────────────────────────────────────────────────────────

def _render_card(
    row: dict,
    samples: dict,
    a_rows: list[dict],
    a_headers: list[str],
    db_rows: list[dict],
    db_headers: list[str],
    db_by_id: dict[str, dict],
    reviewer: str,
) -> None:
    cid = row["cluster_id"]
    org_type = (row.get("org_type") or "").strip()
    canonical = (row.get("canonical_yiddish") or "").strip()
    settlements = _settlements_for(row, samples, cid)
    sample_list = (samples.get(cid, {}) or {}).get("samples", []) or []

    with st.container(border=True):
        head_cols = st.columns([7, 3])
        with head_cols[0]:
            size = (row.get("cluster_size") or "").strip() or "?"
            aligned = (row.get("aligned_db_id") or "").strip()
            meta = f"{_status(row)} · `{cid}` · {org_type or '—'} · n={size}"
            if aligned:
                meta += f" · → DB {aligned}"
            st.markdown(meta)
            st.markdown(
                f"<div dir='rtl' style='font-size:1.15rem; font-weight:600'>"
                f"{canonical or '(no canonical)'}</div>",
                unsafe_allow_html=True,
            )
            if settlements:
                st.caption("📍 " + " · ".join(settlements))
        with head_cols[1]:
            _card_actions(
                cid=cid, row=row, org_type=org_type, canonical=canonical,
                a_rows=a_rows, a_headers=a_headers,
                db_rows=db_rows, db_headers=db_headers,
                db_by_id=db_by_id, samples=samples, reviewer=reviewer,
            )

        if sample_list:
            st.markdown("**Sample texts**")
            for head, sent, _fle, _xid in sample_list[:_SAMPLES_COLLAPSED]:
                head_txt = head or "(no heading)"
                body = f"<b>{head_txt}</b>"
                if sent:
                    body += f" — {sent}"
                st.markdown(f"<div dir='rtl'>{body}</div>", unsafe_allow_html=True)

        if sample_list:
            with st.expander("View full entries"):
                for head, sent, fle, xid in sample_list:
                    st.markdown(f"**{head or '(no heading)'}**  ·  `{xid or '—'}`")
                    full = get_entry_text(fle, xid)
                    if full:
                        st.markdown(
                            f"<div dir='rtl' style='font-size:0.92em; "
                            f"white-space:pre-wrap; line-height:1.6'>{full}</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.caption(f"Entry not found in XML ({_JSON_TO_XML.get(fle, fle)}).")


# ─── view entry point ─────────────────────────────────────────────────────────

def render() -> None:
    st.header("Org merge · cards")
    st.caption(
        "Card queue for organization alignment. Each card shows the cluster's "
        "settlement(s) and sample mention lines, with inline actions — New "
        "entity, Align, Merge — that open in place (no new tab). Expand "
        "\"View full entries\" to read the full entry text."
    )
    _queue()


@st.fragment
def _queue() -> None:
    # Loading inside the fragment means a post-mutation `st.rerun()` (fragment
    # scope) reloads fresh rows — the mutation helpers clear the load_* caches —
    # without re-running the whole app. Decided clusters then drop out of the
    # default "undecided" queue on the next render.
    reviewer = st.session_state.get("reviewer", "")
    a_headers, a_rows = load_alignment(_mtime(ALIGN_FILE))
    db_headers, db_rows = load_core_db(_mtime(CORE_DB_FILE))
    samples = load_samples(_mtime(CLUSTER_FILE)) if CLUSTER_FILE.exists() else {}
    db_by_id = {r.get("db_id", ""): r for r in db_rows}

    # --- Filters ---
    with st.expander("Filters", expanded=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            decisions = st.multiselect(
                "Decision", options=_DECISION_OPTIONS, default=["(undecided)"],
                key="omc_decisions",
            )
            search = st.text_input(
                "Search name / variants / id", key="omc_search",
                placeholder="Yiddish name, variant, or cluster_id…",
            )
        with fc2:
            all_types = sorted({(r.get("org_type") or "").strip() for r in a_rows if (r.get("org_type") or "").strip()})
            types = st.multiselect(
                "Org types (empty = all)", options=all_types, default=[],
                key="omc_types",
            )
            settle_q = st.text_input(
                "Settlement contains", key="omc_settle",
                placeholder="e.g. Warsaw / ווארשע…",
            )

    dec_set = set(decisions)
    type_set = set(types)
    sq = (search or "").strip().lower()
    stq = (settle_q or "").strip().lower()

    def _passes(r: dict) -> bool:
        cid = r.get("cluster_id", "")
        if not cid:
            return False
        tag = (r.get("decision") or "").strip() or "(undecided)"
        if dec_set and tag not in dec_set:
            return False
        if type_set and (r.get("org_type") or "").strip() not in type_set:
            return False
        if sq:
            hay = f"{r.get('canonical_yiddish','')} {r.get('name_variants','')} {cid}".lower()
            if sq not in hay:
                return False
        if stq:
            hay_s = " ".join(_settlements_for(r, samples, cid)).lower()
            if stq not in hay_s:
                return False
        return True

    queue = [r for r in a_rows if _passes(r)]
    total = len(queue)

    # --- Pagination ---
    n_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = int(st.session_state.get("omc_page", 0))
    page = max(0, min(page, n_pages - 1))

    top = st.columns([2, 1, 1, 2])
    top[0].markdown(f"**{total}** cluster(s) match")
    if top[1].button("← Prev", disabled=page <= 0, key="omc_prev", use_container_width=True):
        st.session_state["omc_page"] = page - 1
        st.rerun()
    if top[2].button("Next →", disabled=page >= n_pages - 1, key="omc_next", use_container_width=True):
        st.session_state["omc_page"] = page + 1
        st.rerun()
    top[3].markdown(f"Page **{page + 1}** / {n_pages}")

    if total == 0:
        st.info("No clusters match the current filters.")
        return

    start = page * _PAGE_SIZE
    for row in queue[start:start + _PAGE_SIZE]:
        _render_card(
            row, samples, a_rows, a_headers, db_rows, db_headers, db_by_id, reviewer,
        )
