"""People · family cards — disambiguate a whole surname family in one sitting.

The unit is a FAMILY (all hubs sharing a surname), not a queue. The card shows
each member once -- life years, places by period, troupes and roles, relatives
-- built by reading every member's Leksikon entry, and then works through that
family's open bare-surname mentions (the B3 queue) with a proposed answer.

Proposals come from two INDEPENDENT blind LLM judges reading the same family
sheet. A proposal is pre-selected only when both judges named the same person;
otherwise nothing is pre-selected and both readings are shown. Pilot (Adler,
2026-11-15) measured on masked full-name mentions with known answers -- see
people/family_cards/README.md. A proposal is a suggestion, never a decision.

Saves go to the B3 ledger `people/mention_resolution_decisions.tsv` (one row per
mention_id; a new answer REPLACES the row, exactly as B3 does), built on
GitHub's copy of the file, refused if that row changed since the page loaded,
and pushed as one commit. Inputs are written offline by
people/family_cards/build_family_cards.py.
"""
from __future__ import annotations

import datetime as _dt
import html
import json
import os
import pathlib
import re
import sys
import tempfile

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
from zalmen import people_mint as pm  # noqa: E402
from zalmen.activity_log import log_action  # noqa: E402

CARDS = BASE / "people" / "family_cards"
PROPOSALS = CARDS / "family_proposals.tsv"
DECISIONS = BASE / "people" / "mention_resolution_decisions.tsv"
REPO_DECISIONS = "Zylbercweig/people/mention_resolution_decisions.tsv"
PAGE = 10

FAMILY, OTHER, SKIP = "👪 the family / a couple jointly", "❓ another Adler / not a person", "— skip —"


def _read(p: pathlib.Path) -> list[dict]:
    return pm.parse(p.read_bytes())[1] if p.exists() else []


@st.cache_data(show_spinner=False)
def _proposals(mtime: float) -> list[dict]:
    return _read(PROPOSALS)


@st.cache_data(show_spinner=False)
def _sheet(family: str, mtime: float) -> list[dict]:
    p = CARDS / family / "family_sheet.json"
    return json.loads(p.read_text()) if p.exists() else []


def _decisions() -> dict[str, dict]:
    return {r["mention_id"]: r for r in _read(DECISIONS) if r.get("mention_id")}


def _write_bytes(path: pathlib.Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def _years(m: dict) -> str:
    b = (m.get("birth") or {}).get("year", "") or "?"
    d = (m.get("death") or {}).get("year", "") or ""
    return f"{b}–{d}" if d else f"b. {b}" if b != "?" else ""


def _label(m: dict) -> str:
    head = re.sub(r"\s*\[.*?\]", "", m["heading"]).strip()
    return f"{head} ({_years(m)})" if _years(m) else head


def _context_html(ctx: str) -> str:
    esc = html.escape(ctx)
    esc = re.sub(r"⟦(.*?)⟧", r"<mark style='background:#ffd54f;font-weight:700'>\1</mark>", esc)
    return (f"<div dir='rtl' style='font-size:1.05rem;line-height:1.7;"
            f"border-right:3px solid #ccc;padding-right:.6rem'>{esc}</div>")


def _member_panel(sheet: list[dict]) -> None:
    st.markdown("#### The family")
    for m in sheet:
        places = sorted({t.get("place", "") for t in m.get("timeline", []) if t.get("place")})
        with st.expander(f"{_label(m)} · {m.get('active_years', '')} · {', '.join(places[:5])}"):
            st.caption(m.get("distinguishing", ""))
            if m.get("family"):
                st.markdown("**Relatives:** " + " · ".join(
                    f"{f.get('relation', '')}: {f.get('name', '')}" for f in m["family"]))
            rows = [{"years": f"{t.get('from', '')}–{t.get('to', '')}".strip("–"),
                     "place": t.get("place", ""), "troupe / org": t.get("org", ""),
                     "role": t.get("role", "")} for t in m.get("timeline", [])]
            if rows:
                st.dataframe(rows, hide_index=True, width="stretch")
            if m.get("collaborators"):
                st.caption("Worked with: " + ", ".join(
                    c.get("name", "") if isinstance(c, dict) else c
                    for c in m["collaborators"][:25]))


def _save(rows: list[dict], seen: dict[str, str], reviewer: str, family: str) -> tuple[bool, str]:
    """Replace the saved mentions' rows on GitHub's copy; refuse on conflict."""
    from zalmen.github_sync import fetch_file_from_github, push_files_to_github
    remote = fetch_file_from_github(REPO_DECISIONS)
    if remote is None:
        return False, "could not read the decisions file from GitHub — nothing saved. Try again."
    current = {r["mention_id"]: r for r in pm.parse(remote)[1] if r.get("mention_id")}
    changed = [m for m in (r["mention_id"] for r in rows)
               if (current.get(m, {}).get("reviewed_at", "")) != seen.get(m, "")]
    if changed:
        return False, (f"{len(changed)} of these mentions were decided by someone else "
                       "since this page loaded — nothing saved. Reload the page.")
    ids = {r["mention_id"] for r in rows}
    data = pm.drop_rows(remote, lambda r: r.get("mention_id") not in ids)
    data = pm.append_rows(data, rows)
    _write_bytes(DECISIONS, data)
    ok = push_files_to_github([(REPO_DECISIONS, DECISIONS)],
                              f"family cards: {family} — {len(rows)} mention decisions")
    if not ok:
        return False, "saved on this server but NOT on GitHub — please tell Sinai."
    for i, r in enumerate(rows):
        log_action("family_cards", "mention_decision", target_id=r["mention_id"],
                   decision=r["decision_kind"], note=r.get("notes", ""),
                   push=(i == len(rows) - 1), surname=family,
                   resolved_hub_id=r.get("resolved_hub_id", ""))
    return True, f"saved {len(rows)} decisions"


def render() -> None:
    st.markdown("### 👪 People · family cards")
    st.caption("One surname family at a time: read the family once, then decide its "
               "bare-surname mentions. A proposal is pre-selected only when two "
               "independent model readings agree — always check the quote.")
    reviewer = st.session_state.get("reviewer", "")
    props = _proposals(PROPOSALS.stat().st_mtime if PROPOSALS.exists() else 0.0)
    if not props:
        st.info("No family cards built yet (people/family_cards/family_proposals.tsv).")
        return
    families = sorted({p["family"] for p in props})
    decisions = _decisions()

    def is_open(p):
        d = decisions.get(p["mention_id"])
        return not d or d.get("decision_kind") == "family"

    counts = {f: sum(1 for p in props if p["family"] == f and is_open(p)) for f in families}
    family = st.selectbox("Family", families,
                          format_func=lambda f: f"{f.capitalize()} — {counts[f]} open mentions")
    sheet = _sheet(family, (CARDS / family / "family_sheet.json").stat().st_mtime
                   if (CARDS / family / "family_sheet.json").exists() else 0.0)
    members = {m["hub_id"]: m for m in sheet}
    _member_panel(sheet)

    mine = [p for p in props if p["family"] == family]
    lane = st.radio("Show", ["Judges agree", "Judges disagree / unsure",
                             "Family-level answers to revisit", "Already decided"],
                    horizontal=True, key="fc_lane")

    def lane_of(p):
        d = decisions.get(p["mention_id"])
        if d and d.get("decision_kind") != "family":
            return "Already decided"
        if d and d.get("decision_kind") == "family":
            return "Family-level answers to revisit"
        return "Judges agree" if p["agree"] == "1" else "Judges disagree / unsure"

    rows = [p for p in mine if lane_of(p) == lane]
    st.markdown(f"**{len(rows)}** mentions in this lane")
    if not rows:
        return
    n_pages = (len(rows) - 1) // PAGE + 1
    page = st.number_input("Page", 1, n_pages, 1, key=f"fc_page_{family}_{lane}") - 1
    chunk = rows[page * PAGE:(page + 1) * PAGE]

    hub_opts = [h for h in members]
    options = [_label(members[h]) for h in hub_opts] + [FAMILY, OTHER, SKIP]
    picks, seen = {}, {}
    for p in chunk:
        mid = p["mention_id"]
        prior = decisions.get(mid)
        seen[mid] = (prior or {}).get("reviewed_at", "")
        st.divider()
        st.markdown(f"**In the entry of:** {html.escape(p['host_heading'])}", unsafe_allow_html=True)
        st.markdown(_context_html(p["context"]), unsafe_allow_html=True)
        for j in ("1", "2"):
            ans = p[f"judge{j}_answer"]
            if not ans:
                continue
            who = members.get(ans)
            st.caption(f"🤖 reading {j}: **{_label(who) if who else ans}** "
                       f"({p[f'judge{j}_confidence']}) — {p[f'judge{j}_reason']}")
        if prior:
            st.caption(f"Recorded: {prior.get('decision_kind')} "
                       f"{prior.get('resolved_heading', '')} — {prior.get('reviewer', '')} "
                       f"{prior.get('reviewed_at', '')[:10]}")
        default = len(options) - 1
        if prior and prior.get("decision_kind") == "hub" and prior.get("resolved_hub_id") in hub_opts:
            default = hub_opts.index(prior["resolved_hub_id"])
        elif p["agree"] == "1" and p["proposal_hub"] in hub_opts:
            default = hub_opts.index(p["proposal_hub"])
        picks[mid] = st.radio("who", options, index=default, key=f"fc_pick_{mid}",
                              horizontal=True, label_visibility="collapsed")

    note = st.text_input("Notes for this page (optional)", key=f"fc_note_{family}_{lane}_{page}")
    todo = {m: v for m, v in picks.items() if v != SKIP}
    if st.button(f"💾 Save {len(todo)} decisions", type="primary", disabled=not todo or not reviewer):
        now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        by = {p["mention_id"]: p for p in chunk}
        out = []
        for mid, v in todo.items():
            p = by[mid]
            if v == FAMILY:
                kind, hub = "family", ""
            elif v == OTHER:
                kind, hub = "other", ""
            else:
                kind, hub = "hub", hub_opts[options.index(v)]
            out.append({
                "mention_id": mid, "surname": p["surname"],
                "host_person_id": p["host_person_id"], "mention_name": p["mention_name"],
                "decision_kind": kind, "resolved_hub_id": hub,
                "resolved_heading": members[hub]["heading"] if hub else "",
                "resolver_verdict": p["resolver_verdict"], "resolver_method": p["resolver_method"],
                # B3's meaning: agrees with the B3 RESOLVER's pick, not our judges
                "agrees_with_resolver": "1" if hub and hub == p["resolver_pick"] else "0",
                "chips": p["chips"], "reviewer": reviewer, "reviewed_at": now,
                "notes": "family card" + (f": {note}" if note else "")
                         + (" (accepted agreed proposal)" if hub and hub == p["proposal_hub"]
                            and p["agree"] == "1" else ""),
            })
        ok, msg = _save(out, seen, reviewer, family)
        (st.success if ok else st.error)(msg)
        if ok:
            st.rerun()
    if not reviewer:
        st.caption("Choose your name in the sidebar to save.")
