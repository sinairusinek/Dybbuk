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
import html
import pathlib
import re
import sys
import unicodedata

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

PEOPLE_DIR = BASE / "people"
RESOLUTIONS_TSV = PEOPLE_DIR / "mention_surname_resolutions.tsv"
GROUPS_TSV = PEOPLE_DIR / "surname_groups.tsv"
HUB_TSV = PEOPLE_DIR / "person_hub.tsv"
ENTRY_TEXTS_TSV = PEOPLE_DIR / "entry_texts.tsv"
DECISIONS_TSV = PEOPLE_DIR / "mention_resolution_decisions.tsv"
REREVIEW_TSV = PEOPLE_DIR / "mention_rereview_queue.tsv"
REPO_DECISIONS_PATH = "Zylbercweig/people/mention_resolution_decisions.tsv"
TEXT_DISPLAY_CAP = 20_000

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


@st.cache_data
def _load_entry_texts(mtime: float) -> dict[str, str]:
    """person_id → full entry text (⏎ = newline). ~30 MB, loaded once."""
    if not ENTRY_TEXTS_TSV.exists():
        return {}
    csv.field_size_limit(sys.maxsize)
    with open(ENTRY_TEXTS_TSV) as f:
        return {r["person_id"]: r["entry_text"]
                for r in csv.DictReader(f, delimiter="\t")}


def _entry_text_block(text: str, highlights: list[str]) -> None:
    """Render an entry text RTL, with the given surfaces <mark>-highlighted."""
    if not text:
        st.caption("No entry text on file.")
        return
    shown = html.escape(text[:TEXT_DISPLAY_CAP])
    for surface in sorted(set(highlights), key=len, reverse=True):
        esc = html.escape(surface)
        if esc:
            shown = shown.replace(esc, f"<mark>{esc}</mark>")
    st.markdown(_rtl(shown.replace("⏎", "<br>")), unsafe_allow_html=True)
    if len(text) > TEXT_DISPLAY_CAP:
        st.caption(f"…truncated at {TEXT_DISPLAY_CAP:,} of {len(text):,} chars.")


@st.cache_data
def _load_rereview(mtime: float) -> dict[str, dict]:
    """mention_id → queue row for decisions taken before candidates were repaired.

    Built by build_rereview_queue.py. 'high' = the reviewer abstained while
    candidates were missing (likely wrong); 'low' = picked a hub, but a better
    option exists now.
    """
    if not REREVIEW_TSV.exists():
        return {}
    csv.field_size_limit(sys.maxsize)
    with open(REREVIEW_TSV) as f:
        return {r["mention_id"]: r for r in csv.DictReader(f, delimiter="\t")}


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


def _bold_mention(sentence: str, mention: str) -> str:
    """Escape a sentence for display and bold the mention surface inside it.

    The mention is what's being adjudicated, so it carries the emphasis (the
    host heading above is left at regular weight). Two Yiddish-specific wrinkles:
    names inflect in place (טאָמאַשעווסקי → טאָמאַשעווסקין / טאָמאַשעווסקיס), so we match a
    word PREFIX; and the mention and the running text often disagree on nikkud
    (אברהם vs אַברהם), so points are treated as optional between letters.

    Both sides are NFD-normalized first: mention surfaces carry precomposed
    presentation forms (פֿ = U+FB4E) that the running text may spell as base
    letter + point, and decomposing both is what makes them comparable.
    """
    sentence = unicodedata.normalize("NFD", sentence or "")
    base = "".join(c for c in unicodedata.normalize("NFD", mention or "")
                   if not unicodedata.combining(c))
    if not base.strip():
        return html.escape(sentence)
    nik = r"[֑-ׇ]*"          # Hebrew points/accents, optional
    pattern = "".join(re.escape(ch) + nik for ch in base) + r"[\w֑-ׇ]*"
    out, pos = [], 0
    for mo in re.finditer(pattern, sentence):
        out.append(html.escape(sentence[pos:mo.start()]))
        out.append(f"<strong>{html.escape(mo.group(0))}</strong>")
        pos = mo.end()
    out.append(html.escape(sentence[pos:]))
    return "".join(out)


def _candidate_panel(cand_ids: list[str], hubs: dict[str, dict],
                     texts: dict[str, str], surname: str) -> dict[str, str]:
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
                pids = [p for p in (h.get("entry_person_ids") or "").split("|") if p]
                with_text = [p for p in pids if texts.get(p)]
                if with_text:
                    with st.expander("📜 own entry"):
                        for p in with_text:
                            if len(with_text) > 1:
                                st.caption(f"`{p}`")
                            _entry_text_block(texts[p], [surname])
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
    texts = _load_entry_texts(_mtime(ENTRY_TEXTS_TSV))
    decisions = _load_decisions()
    rereview = _load_rereview(_mtime(REREVIEW_TSV))

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
    label_of = _candidate_panel(cand_ids, hubs, texts, surname)

    st.divider()

    # ── mention stream ────────────────────────────────────────────────────────
    queue_here = {r["mention_id"] for r in rows if r["mention_id"] in rereview}
    hi_here = {m for m in queue_here if rereview[m]["priority"] == "high"}

    f1, f2, f3 = st.columns([2, 1, 1.4])
    with f1:
        verdicts = st.multiselect("Show verdicts", ["AMBIGUOUS", "RESOLVED", "UNKNOWN"],
                                  default=["AMBIGUOUS", "UNKNOWN"], key="sr_verdicts")
    with f2:
        show_decided = st.checkbox("Show decided", value=False, key="sr_decided")
    with f3:
        only_rereview = st.checkbox(
            f"⚠ Re-review only ({len(hi_here)})", value=False, key="sr_rereview",
            disabled=not queue_here,
            help="Decisions taken BEFORE the candidate set was repaired — the "
                 "reviewer could not see candidates that exist now.")
        include_low = False
        if only_rereview:
            include_low = st.checkbox(
                f"…include lower-risk hub picks ({len(queue_here) - len(hi_here)})",
                value=False, key="sr_rereview_low")

    if only_rereview:
        wanted = queue_here if include_low else hi_here
        visible = [r for r in rows if r["mention_id"] in wanted]
        st.info(
            f"Re-reviewing {len(visible)} decision(s) made before the candidate "
            f"set was repaired. Each row shows what was decided then and which "
            f"candidates were missing at the time; the prior choice is "
            f"pre-selected, so saving an unchanged row simply reaffirms it.")
    else:
        visible = [r for r in rows if r["verdict"] in verdicts
                   and (show_decided or r["mention_id"] not in decisions)]
        if hi_here:
            st.warning(
                f"⚠ {len(hi_here)} decision(s) on this surname were taken before "
                f"the candidate set was repaired — tick “Re-review only” to work "
                f"through them.", icon="⚠️")
    if not visible:
        st.success("Nothing left to review here with current filters.")
        return

    n_pages = (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE
    page = st.number_input(f"Page (of {n_pages})", 1, n_pages, 1, key="sr_page") - 1
    chunk = visible[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    FAMILY, OTHER, SKIP = "👪 Family level only", "❓ Other / not listed", "⏭ Skip"
    surfaces_by_host: dict[str, list[str]] = {}
    for r in rows:
        if r["host_person_id"]:
            surfaces_by_host.setdefault(r["host_person_id"], []).append(r["mention_name"])
    picks: dict[str, str] = {}
    last_host = None
    for r in chunk:
        mid = r["mention_id"]
        host = r["host_heading"] or r["host_person_id"] or "(unknown entry)"
        if host != last_host:
            st.markdown(_rtl(f"📖 {html.escape(host)}", 1.05), unsafe_allow_html=True)
            host_pid = r["host_person_id"]
            if host_pid and texts.get(host_pid):
                with st.expander("📜 host entry text (mentions highlighted)"):
                    _entry_text_block(texts[host_pid],
                                      surfaces_by_host.get(host_pid, []) + [surname])
            last_host = host
        with st.container(border=True):
            top = st.columns([3, 2])
            with top[0]:
                if r["sentence"]:
                    st.markdown(
                        _rtl(f"…{_bold_mention(r['sentence'], r['mention_name'])}…"),
                        unsafe_allow_html=True)
                else:
                    st.markdown(
                        _rtl(f"<strong>{html.escape(r['mention_name'])}</strong>", 1.1),
                        unsafe_allow_html=True)
            with top[1]:
                chips = r["chips"].replace("|", " · ")
                sugg = ""
                if r["verdict"] == "RESOLVED":
                    sugg = (f"→ {label_of.get(r['resolved_hub_id'], '?')} "
                            f"({r['method']})")
                elif r["method"] == "family_abstain_suggested":
                    sugg = "→ 👪 family abstain suggested"
                st.caption(f"{chips}\n\n🤖 {sugg}" if sugg else chips)

            q = rereview.get(mid)
            if q:
                was = {"family": "👪 family level only", "other": "❓ other",
                       "hub": q["prior_heading"] or q["prior_hub_id"]}.get(
                           q["prior_decision_kind"], q["prior_decision_kind"])
                st.caption(
                    f"⚠ decided {q['reviewed_at'][:10]} as **{was}** — "
                    f"**not on screen at the time:** {q['added_headings'].replace('|', ' · ')}")

            r_cands = [h for h in r["candidate_hub_ids"].split("|") if h]
            options = ([f"{label_of.get(h, '?')} {hubs.get(h, {}).get('canonical_heading', h)}"
                        for h in r_cands] + [FAMILY, OTHER, SKIP])
            default = len(options) - 1  # SKIP
            if r["verdict"] == "RESOLVED" and r["resolved_hub_id"] in r_cands:
                default = r_cands.index(r["resolved_hub_id"])
            elif r["method"] == "family_abstain_suggested":
                default = len(options) - 3  # FAMILY
            # a human decision outranks the resolver's suggestion: pre-select what
            # was chosen before, so re-review starts from the standing answer.
            prior = decisions.get(mid)
            if prior:
                if prior["decision_kind"] == "family":
                    default = len(options) - 3
                elif prior["decision_kind"] == "other":
                    default = len(options) - 2
                elif prior["resolved_hub_id"] in r_cands:
                    default = r_cands.index(prior["resolved_hub_id"])
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
