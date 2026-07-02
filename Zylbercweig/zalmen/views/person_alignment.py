"""B2 · Person → DB — review the Phase B LLM alignment drafts.

Two modes:
  ⚡ Batch confirm — table of high-confidence drafts (ALIGN-high = 325 rows,
     also NEW-high / MERGE-high); tick + confirm in bulk, one save per batch.
  🔍 Card review — everything else (DISAMBIG / DEFER / medium / low), one
     entry per screen with DB candidate cards and the draft's rationale.

Decisions land in `people_alignment_decisions.tsv` keyed by person_id, and
are consumed by build_person_hub.py as `human_align` hub evidence.
"""
from __future__ import annotations

import csv
import datetime as _dt
import pathlib
import sys

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

PEOPLE_DIR = BASE / "people"
DRAFTS_TSV = PEOPLE_DIR / "people_alignment_drafts.tsv"
QUEUE_TSV = PEOPLE_DIR / "people_alignment_queue.tsv"
DB_TSV = PEOPLE_DIR / "people_db.tsv"
EXTRACTED_TSV = PEOPLE_DIR / "people_extracted.tsv"
DECISIONS_TSV = PEOPLE_DIR / "people_alignment_decisions.tsv"
REPO_DECISIONS_PATH = "Zylbercweig/people/people_alignment_decisions.tsv"

DECISION_FIELDS = [
    "person_id", "xml_id", "heading", "decision", "aligned_db_id",
    "merge_person_id", "merge_xml_id", "draft_decision", "draft_confidence",
    "accepted_draft", "reviewer", "reviewed_at", "notes", "mode",
]

_RTL = "<div dir='rtl' style='font-size:{size}rem; font-weight:{w}'>{text}</div>"


def _rtl(text: str, size: float = 1.0, weight: int = 400) -> str:
    return _RTL.format(size=size, w=weight, text=text)


# ── cached loaders (keyed by file mtime so pipeline reruns invalidate) ────────
def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data
def _load_drafts(mtime: float) -> list[dict]:
    with open(DRAFTS_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data
def _load_queue(mtime: float) -> dict[str, dict]:
    with open(QUEUE_TSV) as f:
        return {r["person_id"]: r for r in csv.DictReader(f, delimiter="\t")}


@st.cache_data
def _load_db(mtime: float) -> dict[str, dict]:
    with open(DB_TSV) as f:
        return {r["db_id"]: r for r in csv.DictReader(f, delimiter="\t")}


@st.cache_data
def _load_entries(mtime: float) -> dict[str, dict]:
    """person_id → entry row; also xml_id → [person_id] for MERGE resolution."""
    out: dict[str, dict] = {}
    with open(EXTRACTED_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out[r["person_id"]] = r
    return out


@st.cache_data
def _xml_to_pids(mtime: float) -> dict[str, list[str]]:
    idx: dict[str, list[str]] = {}
    with open(EXTRACTED_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            idx.setdefault(r["xml_id"], []).append(r["person_id"])
    return idx


def _load_decisions() -> dict[str, dict]:
    if not DECISIONS_TSV.exists():
        return {}
    with open(DECISIONS_TSV) as f:
        return {r["person_id"]: r for r in csv.DictReader(f, delimiter="\t")
                if r.get("person_id")}


def _save_decisions(new_rows: list[dict], commit_msg: str) -> None:
    decisions = _load_decisions()
    for row in new_rows:
        decisions[row["person_id"]] = {**decisions.get(row["person_id"], {}), **row}
    with open(DECISIONS_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=DECISION_FIELDS, delimiter="\t",
                           extrasaction="ignore")
        w.writeheader()
        for v in decisions.values():
            w.writerow(v)
    try:
        from zalmen.github_sync import push_file_to_github
        push_file_to_github(REPO_DECISIONS_PATH, DECISIONS_TSV, commit_msg)
    except Exception:
        pass  # local-only fallback


def _decision_row(d: dict, decision: str, accepted: bool, reviewer: str,
                  mode: str, notes: str = "", aligned_db_id: str = "",
                  merge_xml_id: str = "", merge_person_id: str = "") -> dict:
    return {
        "person_id": d["person_id"],
        "xml_id": d["xml_id"],
        "heading": d["heading"],
        "decision": decision,
        "aligned_db_id": aligned_db_id,
        "merge_person_id": merge_person_id,
        "merge_xml_id": merge_xml_id,
        "draft_decision": d.get("draft_decision", ""),
        "draft_confidence": d.get("confidence", ""),
        "accepted_draft": "1" if accepted else "0",
        "reviewer": reviewer,
        "reviewed_at": _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "notes": notes,
        "mode": mode,
    }


def _resolve_merge_pid(xml_id: str, xml_idx: dict[str, list[str]]) -> str:
    pids = xml_idx.get(xml_id, [])
    return pids[0] if len(pids) == 1 else ""


def _db_label(db: dict) -> str:
    heb, eng = (db.get("hebname") or "").strip(), (db.get("english") or "").strip()
    return f"{heb} / {eng}" if heb and eng else (heb or eng or "?")


# ── batch confirm mode ────────────────────────────────────────────────────────
def _render_batch(drafts: list[dict], decisions: dict[str, dict],
                  db: dict[str, dict], xml_idx: dict[str, list[str]]) -> None:
    c1, c2 = st.columns(2)
    with c1:
        verb = st.selectbox("Draft verdict", ["ALIGN", "NEW", "MERGE", "PSEUDONYM"],
                            key="pa_batch_verb")
    with c2:
        conf = st.selectbox("Confidence", ["high", "medium"], key="pa_batch_conf")

    pool = [d for d in drafts
            if d["draft_decision"] == verb and d["confidence"] == conf
            and d["person_id"] not in decisions]
    st.caption(f"{len(pool)} undecided {verb}-{conf} drafts")
    if not pool:
        st.success("Nothing left in this bucket.")
        return

    page_size = 50
    n_pages = (len(pool) + page_size - 1) // page_size
    page = st.number_input("Page", 1, n_pages, 1, key="pa_batch_page") - 1
    chunk = pool[page * page_size:(page + 1) * page_size]

    rows = []
    for d in chunk:
        target = ""
        if verb == "ALIGN":
            target = _db_label(db.get(d.get("draft_aligned_db_id", ""), {}))
        elif verb == "MERGE":
            mx = d.get("draft_merge_xml_id", "")
            pid = _resolve_merge_pid(mx, xml_idx)
            target = f"entry {pid or mx}"
        rows.append({
            "confirm": True,
            "heading": d["heading"],
            "target": target,
            "db_id": d.get("draft_aligned_db_id", ""),
            "rationale": d.get("rationale", ""),
            "person_id": d["person_id"],
        })

    edited = st.data_editor(
        rows,
        column_config={
            "confirm": st.column_config.CheckboxColumn("✓", width="small"),
            "heading": st.column_config.TextColumn("Entry heading", width="medium"),
            "target": st.column_config.TextColumn("Draft target", width="medium"),
            "db_id": st.column_config.TextColumn("db_id", width="small"),
            "rationale": st.column_config.TextColumn("Drafter rationale", width="large"),
            "person_id": None,
        },
        disabled=["heading", "target", "db_id", "rationale"],
        hide_index=True,
        use_container_width=True,
        key=f"pa_batch_editor_{verb}_{conf}_{page}",
    )

    checked = [r for r in edited if r["confirm"]]
    b1, b2 = st.columns([1, 3])
    if b1.button(f"✅ Confirm {len(checked)} drafts", type="primary",
                 disabled=not checked, key="pa_batch_go"):
        by_pid = {d["person_id"]: d for d in chunk}
        reviewer = st.session_state.get("reviewer", "")
        out = []
        for r in checked:
            d = by_pid[r["person_id"]]
            mx = d.get("draft_merge_xml_id", "") if verb == "MERGE" else ""
            out.append(_decision_row(
                d, decision=verb, accepted=True, reviewer=reviewer, mode="batch",
                aligned_db_id=d.get("draft_aligned_db_id", "") if verb == "ALIGN" else "",
                merge_xml_id=mx,
                merge_person_id=_resolve_merge_pid(mx, xml_idx) if mx else "",
            ))
        _save_decisions(out, f"person align: batch-confirm {len(out)} {verb}-{conf} drafts")
        st.success(f"Saved {len(out)} decisions.")
        st.rerun()
    b2.caption("Untick a row to leave it for card review. Unticked rows are NOT saved.")


# ── card review mode ─────────────────────────────────────────────────────────
def _render_entry_card(d: dict, entry: dict) -> None:
    with st.container(border=True):
        st.markdown(f"`{d['person_id']}`")
        st.markdown(_rtl(d["heading"], 1.3, 600), unsafe_allow_html=True)
        bits = []
        for label, key in [("birth", "birth_date"), ("death", "death_date"),
                           ("born in", "birth_place_name"), ("type", "entry_type")]:
            v = (entry.get(key) or "").strip()
            if v:
                bits.append(f"**{label}:** {v}")
        if bits:
            st.markdown(" · ".join(bits))
        nv = (entry.get("names_variants") or "").strip()
        if nv:
            st.markdown(_rtl("וואַריאַנטן: " + nv), unsafe_allow_html=True)
        sub = (entry.get("subheading") or "").strip()
        if sub:
            st.markdown(_rtl(sub), unsafe_allow_html=True)


def _render_candidates(q: dict, db: dict[str, dict], draft_db: str) -> None:
    ids = [x for x in (q.get("candidate_db_ids") or "").split("|") if x]
    scores = (q.get("candidate_scores") or "").split("|")
    for i, db_id in enumerate(ids):
        row = db.get(db_id, {})
        score = scores[i] if i < len(scores) else ""
        is_draft = db_id == draft_db
        with st.container(border=True):
            head = f"**db {db_id}** · score {score}"
            if is_draft:
                head += " · 🤖 **drafter's pick**"
            st.markdown(head)
            st.markdown(_rtl(_db_label(row), 1.1, 500), unsafe_allow_html=True)
            extra = []
            for label, key in [("born", "date_born"), ("died", "date_died"),
                               ("born in", "born_in"), ("alt", "alternative_name")]:
                v = (row.get(key) or "").strip()
                if v:
                    extra.append(f"{label}: {v}")
            if extra:
                st.caption(" · ".join(extra))


def _render_cards(drafts: list[dict], decisions: dict[str, dict],
                  db: dict[str, dict], queue: dict[str, dict],
                  entries: dict[str, dict], xml_idx: dict[str, list[str]]) -> None:
    verbs = sorted({d["draft_decision"] for d in drafts})
    f1, f2, f3 = st.columns(3)
    with f1:
        sel_verbs = st.multiselect("Draft verdict", verbs,
                                   default=[v for v in ("DISAMBIG", "DEFER") if v in verbs],
                                   key="pa_card_verbs")
    with f2:
        sel_conf = st.multiselect("Confidence", ["high", "medium", "low"],
                                  default=["medium", "low"], key="pa_card_conf")
    with f3:
        show_decided = st.checkbox("Show decided", value=False, key="pa_card_decided")

    pool = [d for d in drafts
            if d["draft_decision"] in sel_verbs and d["confidence"] in sel_conf
            and (show_decided or d["person_id"] not in decisions)]
    n_all = sum(1 for d in drafts if d["draft_decision"] in sel_verbs
                and d["confidence"] in sel_conf)
    st.progress((n_all - len(pool)) / max(n_all, 1) if not show_decided else 0.0,
                text=f"{n_all - len(pool)} / {n_all} decided in this filter"
                if not show_decided else f"{len(pool)} rows (decided shown)")
    if not pool:
        st.success("Nothing left with current filters.")
        return

    pos_key = "pa_card_pos"
    if pos_key not in st.session_state or st.session_state[pos_key] >= len(pool):
        st.session_state[pos_key] = 0
    nav = st.columns([1, 4, 1])
    if nav[0].button("← prev", disabled=st.session_state[pos_key] == 0, key="pa_prev"):
        st.session_state[pos_key] -= 1
        st.rerun()
    nav[1].caption(f"entry {st.session_state[pos_key] + 1} of {len(pool)}")
    if nav[2].button("next →", disabled=st.session_state[pos_key] >= len(pool) - 1,
                     key="pa_next"):
        st.session_state[pos_key] += 1
        st.rerun()

    d = pool[st.session_state[pos_key]]
    pid = d["person_id"]
    q = queue.get(pid, {})
    existing = decisions.get(pid, {})

    st.divider()
    conf_icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(d["confidence"], "")
    st.markdown(f"🤖 Draft: **{d['draft_decision']}** {conf_icon} {d['confidence']}")
    if d.get("rationale"):
        st.info(d["rationale"])
    if existing.get("decision"):
        st.warning(f"Already decided: **{existing['decision']}** by "
                   f"{existing.get('reviewer', '?')} at {existing.get('reviewed_at', '?')}")

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Entry")
        _render_entry_card(d, entries.get(pid, {}))
    with right:
        st.subheader("DB candidates")
        _render_candidates(q, db, d.get("draft_aligned_db_id", ""))

    st.divider()
    notes = st.text_input("Notes (optional)", key=f"pa_notes_{pid}")
    reviewer = st.session_state.get("reviewer", "")

    def _advance_save(row: dict, msg: str) -> None:
        _save_decisions([row], msg)
        if st.session_state[pos_key] < len(pool) - 1:
            st.session_state[pos_key] += 1
        st.rerun()

    cand_ids = [x for x in (q.get("candidate_db_ids") or "").split("|") if x]
    a1, a2, a3, a4 = st.columns(4)
    if a1.button("✅ Accept draft", type="primary", key="pa_acc",
                 disabled=d["draft_decision"] in ("DEFER", "DISAMBIG")):
        mx = d.get("draft_merge_xml_id", "")
        _advance_save(_decision_row(
            d, d["draft_decision"], True, reviewer, "card", notes,
            aligned_db_id=d.get("draft_aligned_db_id", ""),
            merge_xml_id=mx, merge_person_id=_resolve_merge_pid(mx, xml_idx)),
            f"person align: accept {d['draft_decision']} {pid}")
    if a2.button("🆕 NEW (not in DB)", key="pa_new"):
        _advance_save(_decision_row(d, "NEW", d["draft_decision"] == "NEW",
                                    reviewer, "card", notes),
                      f"person align: NEW {pid}")
    if a3.button("⏭ Defer", key="pa_defer"):
        _advance_save(_decision_row(d, "DEFER", False, reviewer, "card", notes),
                      f"person align: defer {pid}")
    with a4:
        pick = st.selectbox("ALIGN to…", ["—"] + cand_ids, key=f"pa_pick_{pid}",
                            format_func=lambda x: x if x == "—"
                            else f"{x} · {_db_label(db.get(x, {}))[:40]}")
        if st.button("🔗 ALIGN to selected", key="pa_alignto", disabled=pick == "—"):
            _advance_save(_decision_row(
                d, "ALIGN", pick == d.get("draft_aligned_db_id", ""),
                reviewer, "card", notes, aligned_db_id=pick),
                f"person align: ALIGN {pid}→{pick}")


# ── main ─────────────────────────────────────────────────────────────────────
def render() -> None:
    st.header("B2 · Person → DB (draft review)")
    st.caption(
        "Review the LLM drafter's verdicts on the 568 undecided subject entries. "
        "Confirmed decisions become hub evidence (build_person_hub.py)."
    )
    if not DRAFTS_TSV.exists():
        st.error(f"No drafts found at {DRAFTS_TSV}")
        return

    drafts = _load_drafts(_mtime(DRAFTS_TSV))
    queue = _load_queue(_mtime(QUEUE_TSV))
    db = _load_db(_mtime(DB_TSV))
    entries = _load_entries(_mtime(EXTRACTED_TSV))
    xml_idx = _xml_to_pids(_mtime(EXTRACTED_TSV))
    decisions = _load_decisions()

    n_done = sum(1 for d in drafts if d["person_id"] in decisions)
    st.progress(n_done / max(len(drafts), 1),
                text=f"{n_done} / {len(drafts)} drafts decided")

    mode = st.radio("Mode", ["⚡ Batch confirm", "🔍 Card review"],
                    horizontal=True, key="pa_mode")
    st.divider()
    if mode == "⚡ Batch confirm":
        _render_batch(drafts, decisions, db, xml_idx)
    else:
        _render_cards(drafts, decisions, db, queue, entries, xml_idx)
