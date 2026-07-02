"""B3 · Mentions by surname — disambiguate bare-surname mentions.

PI-ratified review shape (2026-07-02): review happens BY SURNAME. A fixed
candidate-card panel stays on screen (discriminating attributes highlighted);
below it, mentions stream in sentence context with the resolver's suggestion
pre-selected. One-click family-level abstain is first-class. Decisions land
in `mention_resolution_decisions.tsv` keyed by mention_id; the resolver's
rationale chips are stored alongside for drafter calibration.
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
RESOLUTIONS_TSV = PEOPLE_DIR / "mention_surname_resolutions.tsv"
GROUPS_TSV = PEOPLE_DIR / "surname_groups.tsv"
HUB_TSV = PEOPLE_DIR / "person_hub.tsv"
DECISIONS_TSV = PEOPLE_DIR / "mention_resolution_decisions.tsv"
REPO_DECISIONS_PATH = "Zylbercweig/people/mention_resolution_decisions.tsv"

PAGE_SIZE = 15

DECISION_FIELDS = [
    "mention_id", "surname", "host_person_id", "mention_name",
    "decision_kind", "resolved_hub_id", "resolved_heading",
    "resolver_verdict", "resolver_method", "agrees_with_resolver",
    "chips", "reviewer", "reviewed_at", "notes",
]


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data
def _load_resolutions(mtime: float) -> list[dict]:
    csv.field_size_limit(sys.maxsize)
    with open(RESOLUTIONS_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data
def _load_groups(mtime: float) -> list[dict]:
    with open(GROUPS_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data
def _load_hubs(mtime: float) -> dict[str, dict]:
    with open(HUB_TSV) as f:
        return {r["hub_id"]: r for r in csv.DictReader(f, delimiter="\t")}


def _load_decisions() -> dict[str, dict]:
    if not DECISIONS_TSV.exists():
        return {}
    with open(DECISIONS_TSV) as f:
        return {r["mention_id"]: r for r in csv.DictReader(f, delimiter="\t")
                if r.get("mention_id")}


def _save_decisions(new_rows: list[dict], commit_msg: str) -> None:
    decisions = _load_decisions()
    for row in new_rows:
        decisions[row["mention_id"]] = {**decisions.get(row["mention_id"], {}), **row}
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
        pass


def _rtl(text: str, size: float = 1.0, weight: int = 400) -> str:
    return (f"<div dir='rtl' style='font-size:{size}rem; "
            f"font-weight:{weight}'>{text}</div>")


def _candidate_panel(cand_ids: list[str], hubs: dict[str, dict]) -> dict[str, str]:
    """Render fixed candidate cards; return hub_id → short label like '①'."""
    marks = "①②③④⑤⑥⑦⑧⑨⑩⑪⑫"
    label_of: dict[str, str] = {}
    cols = st.columns(min(len(cand_ids), 4))
    for i, hid in enumerate(cand_ids):
        h = hubs.get(hid, {})
        mark = marks[i] if i < len(marks) else str(i + 1)
        label_of[hid] = mark
        with cols[i % len(cols)]:
            with st.container(border=True):
                st.markdown(f"**{mark}** `{hid}`")
                st.markdown(_rtl(h.get("canonical_heading", "?"), 1.15, 600),
                            unsafe_allow_html=True)
                bits = []
                if h.get("birth_date") or h.get("death_date"):
                    bits.append(f"{h.get('birth_date', '?')} – {h.get('death_date', '?')}")
                if h.get("volumes"):
                    bits.append(f"vol {h['volumes']}")
                if h.get("db_ids"):
                    bits.append(f"db {h['db_ids']}")
                if bits:
                    st.caption(" · ".join(bits))
                tops = (h.get("top_surfaces") or "").replace("|", " · ")
                if tops:
                    st.markdown(_rtl(tops, 0.85), unsafe_allow_html=True)
    return label_of


def render() -> None:
    st.header("B3 · Mentions by surname")
    st.caption(
        "Bare-surname mentions resolved per (surname × host entry). The resolver's "
        "suggestion is pre-selected — confirm, correct, or abstain to family level."
    )
    if not RESOLUTIONS_TSV.exists():
        st.error(f"No resolutions found — run resolve_surname_mentions.py first "
                 f"({RESOLUTIONS_TSV}).")
        return

    resolutions = _load_resolutions(_mtime(RESOLUTIONS_TSV))
    groups = _load_groups(_mtime(GROUPS_TSV))
    hubs = _load_hubs(_mtime(HUB_TSV))
    decisions = _load_decisions()

    # ── surname picker ────────────────────────────────────────────────────────
    def _glabel(g: dict) -> str:
        fam = " 👪" if g["family_cluster"] == "1" else ""
        return (f"{g['surname']}{fam} — {g['n_ambiguous']} ambiguous / "
                f"{g['n_mentions']} mentions / {g['n_candidates']} candidates")

    q = st.text_input("Filter surnames", key="sr_filter",
                      placeholder="type part of a surname…")
    pool = [g for g in groups if not q or q.strip() in g["surname"]]
    if not pool:
        st.warning("No surname matches the filter.")
        return
    sel = st.selectbox("Surname group", pool, format_func=_glabel, key="sr_group")
    surname = sel["surname"]

    rows = [r for r in resolutions if r["surname_token"] == surname]
    n_decided = sum(1 for r in rows if r["mention_id"] in decisions)
    st.progress(n_decided / max(len(rows), 1),
                text=f"{n_decided} / {len(rows)} mentions decided for this surname")

    # ── fixed candidate panel ─────────────────────────────────────────────────
    cand_ids: list[str] = []
    for r in rows:
        for h in r["candidate_hub_ids"].split("|"):
            if h and h not in cand_ids:
                cand_ids.append(h)
    if sel["family_cluster"] == "1":
        st.warning("👪 **Family cluster** — fame prior disabled; prefer in-entry "
                   "evidence or abstain to family level.", icon="👪")
    label_of = _candidate_panel(cand_ids, hubs)

    st.divider()

    # ── mention stream ────────────────────────────────────────────────────────
    f1, f2 = st.columns(2)
    with f1:
        verdicts = st.multiselect("Show verdicts", ["AMBIGUOUS", "RESOLVED", "UNKNOWN"],
                                  default=["AMBIGUOUS", "UNKNOWN"], key="sr_verdicts")
    with f2:
        show_decided = st.checkbox("Show decided", value=False, key="sr_decided")

    visible = [r for r in rows if r["verdict"] in verdicts
               and (show_decided or r["mention_id"] not in decisions)]
    if not visible:
        st.success("Nothing left to review here with current filters.")
        return

    n_pages = (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE
    page = st.number_input(f"Page (of {n_pages})", 1, n_pages, 1, key="sr_page") - 1
    chunk = visible[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    FAMILY, OTHER, SKIP = "👪 Family level only", "❓ Other / not listed", "⏭ Skip"
    picks: dict[str, str] = {}
    last_host = None
    for r in chunk:
        mid = r["mention_id"]
        host = r["host_heading"] or r["host_person_id"] or "(unknown entry)"
        if host != last_host:
            st.markdown(_rtl(f"📖 {host}", 1.05, 600), unsafe_allow_html=True)
            last_host = host
        with st.container(border=True):
            top = st.columns([3, 2])
            with top[0]:
                if r["sentence"]:
                    st.markdown(_rtl(f"…{r['sentence']}…"), unsafe_allow_html=True)
                else:
                    st.markdown(_rtl(r["mention_name"], 1.1), unsafe_allow_html=True)
            with top[1]:
                chips = r["chips"].replace("|", " · ")
                sugg = ""
                if r["verdict"] == "RESOLVED":
                    sugg = (f"→ {label_of.get(r['resolved_hub_id'], '?')} "
                            f"({r['method']})")
                elif r["method"] == "family_abstain_suggested":
                    sugg = "→ 👪 family abstain suggested"
                st.caption(f"{chips}\n\n🤖 {sugg}" if sugg else chips)

            r_cands = [h for h in r["candidate_hub_ids"].split("|") if h]
            options = ([f"{label_of.get(h, '?')} {hubs.get(h, {}).get('canonical_heading', h)}"
                        for h in r_cands] + [FAMILY, OTHER, SKIP])
            default = len(options) - 1  # SKIP
            if r["verdict"] == "RESOLVED" and r["resolved_hub_id"] in r_cands:
                default = r_cands.index(r["resolved_hub_id"])
            elif r["method"] == "family_abstain_suggested":
                default = len(options) - 3  # FAMILY
            picks[mid] = st.radio("assign", options, index=default, key=f"sr_pick_{mid}",
                                  horizontal=True, label_visibility="collapsed")

    notes = st.text_input("Notes for this page (optional)", key=f"sr_notes_{surname}_{page}")
    n_actionable = sum(1 for v in picks.values() if v != SKIP)
    if st.button(f"💾 Save {n_actionable} decisions on this page", type="primary",
                 disabled=n_actionable == 0, key="sr_save"):
        by_mid = {r["mention_id"]: r for r in chunk}
        reviewer = st.session_state.get("reviewer", "")
        now = _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        out = []
        for mid, pick in picks.items():
            if pick == SKIP:
                continue
            r = by_mid[mid]
            if pick == FAMILY:
                kind, hub_id = "family", ""
            elif pick == OTHER:
                kind, hub_id = "other", ""
            else:
                kind = "hub"
                mark = pick.split(" ", 1)[0]
                hub_id = next((h for h, m in label_of.items() if m == mark), "")
            out.append({
                "mention_id": mid,
                "surname": surname,
                "host_person_id": r["host_person_id"],
                "mention_name": r["mention_name"],
                "decision_kind": kind,
                "resolved_hub_id": hub_id,
                "resolved_heading": hubs.get(hub_id, {}).get("canonical_heading", ""),
                "resolver_verdict": r["verdict"],
                "resolver_method": r["method"],
                "agrees_with_resolver":
                    "1" if (kind == "hub" and hub_id == r["resolved_hub_id"]) else "0",
                "chips": r["chips"],
                "reviewer": reviewer,
                "reviewed_at": now,
                "notes": notes,
            })
        _save_decisions(out, f"surname review: {surname} — {len(out)} decisions")
        st.success(f"Saved {len(out)} decisions.")
        st.rerun()
