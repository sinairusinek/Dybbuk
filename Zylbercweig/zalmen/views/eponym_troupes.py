"""Eponym · troupe questions — who is each troupe named after?

The open (org, span) questions of the eponym track, read from
`eponym_open_questions.tsv`, which `build_eponym_ra_workbook.py` writes beside
the workbook. Since 2026-11-01 this view is THE answering surface for them: the
Drive workbook and the claude.ai form are no longer handed to a reviewer, so
there is one place answers come from (the two-surface problem that once left
28 answers unread is why the old in-app view was retired on 10-13).

Each answer is written straight into `eponym_decisions.tsv`, with the same
guards as `ingest_eponym_ra_workbook.py`:
  * a span already CONFIRMed to a DIFFERENT person is refused (db1047),
  * an answer that repeats a recorded decision writes nothing,
  * "not a person" is a REJECT with no hub, which closes the span,
  * a person who is not in the list goes to `eponym_blocked_answers.tsv`, and
    from there to "Eponym · mint people" -- never matched by name here.
Saves are built on GitHub's copy of each file (people_mint.py) and land as one
commit. Edges are applied offline by apply_eponym_decisions.py.
"""
from __future__ import annotations

import collections
import datetime as _dt
import os
import pathlib
import re
import sys
import tempfile

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)
from zalmen.activity_log import log_action  # noqa: E402
from zalmen import people_mint as pm  # noqa: E402

REPO = "Zylbercweig"
FILES = {
    "questions": "organizations/eponym_open_questions.tsv",
    "decisions": "organizations/eponym_decisions.tsv",
    "blocked": "organizations/eponym_blocked_answers.tsv",
    "hubs": "people/person_hub.tsv",
}
PAGE = 8   # troupes per page

CATS = {
    "proposal": ("Model proposed someone",
                 "Check the quote, then Accept — or pick someone else."),
    "choose": ("Choose among candidates",
               "Several people share this surname. Use the Leksikon context."),
    "unknown": ("No candidate found",
                "Nobody in the database has this surname. Name the person if "
                "you can — they go to Eponym · mint people — or mark Not a person."),
    "judge_none": ("Model found no one",
                   "The model read the Leksikon and found no person for this "
                   "word. Confirm Not a person, or identify them."),
}


def _local(key: str) -> pathlib.Path:
    return BASE / FILES[key]


@st.cache_data(show_spinner=False)
def _parse(path_str: str, mtime: float) -> list[dict]:
    p = pathlib.Path(path_str)
    return pm.parse(p.read_bytes())[1] if p.exists() else []


def _rows(key: str) -> list[dict]:
    p = _local(key)
    return _parse(str(p), p.stat().st_mtime if p.exists() else 0.0)


def _write_bytes(path: pathlib.Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def _category(q: dict) -> str:
    if q.get("proposal_hub"):
        return "proposal"
    if q.get("judge") == "NONE":
        return "judge_none"
    return "choose" if q.get("cand_hubs") else "unknown"


def _closed(decisions: list[dict], blocked: list[dict]) -> set[tuple]:
    """(db_id, span) pairs no longer open -- the builder's own rule, plus the
    spans this view sent to the mint queue."""
    out = {(d["db_id"], d.get("span", "")) for d in decisions
           if (d.get("decision") or "").upper() == "CONFIRM"
           or ((d.get("decision") or "").upper() == "REJECT"
               and not (d.get("hub_id") or "").strip())}
    out |= {(b["org_db_id"], b["span"]) for b in blocked}
    return out


def _rtl(s: str) -> str:
    return f'<span dir="rtl" style="font-size:1.05em">{s}</span>' if s else ""


def _highlight(text: str, spans: list[str]) -> str:
    """Mark the eponyms' names in the excerpt, ignoring pointing and finals."""
    import html
    if not text:
        return ""
    folded, idx = [], []
    for i, ch in enumerate(text):
        f = pm.fold(ch)
        folded.append(f)
        idx.extend([i] * len(f))
    ftext = "".join(folded)
    keys = [pm.fold(s) for s in spans]
    keys = [k[:-1] if len(k) > 4 else k for k in keys]   # declined: -ס, -ן
    keys = [k for k in keys if len(k) >= 3]
    out, last = [], 0
    pattern = "|".join(re.escape(k) for k in sorted(keys, key=len, reverse=True))
    for m in re.finditer(pattern, ftext) if pattern else []:
        a, b = idx[m.start()], idx[m.end() - 1] + 1
        out.append(html.escape(text[last:a]))
        out.append(f'<mark>{html.escape(text[a:b])}</mark>')
        last = b
    out.append(html.escape(text[last:]))
    return "".join(out)


# ── save ─────────────────────────────────────────────────────────────────────
def _save(q: dict, action: str, hub_id: str, typed: str, note: str,
          reviewer: str) -> tuple[bool, str]:
    from zalmen.github_sync import fetch_file_from_github, push_files_to_github

    remote = {}
    for k in ("decisions", "blocked", "hubs"):
        data = fetch_file_from_github(f"{REPO}/{FILES[k]}")
        if data is None:
            return False, f"could not read {FILES[k]} from GitHub — nothing saved. Try again."
        remote[k] = data
    decisions = pm.parse(remote["decisions"])[1]
    hubs = {h["hub_id"]: h for h in pm.parse(remote["hubs"])[1]}
    db_id, span = q["db_id"], q["span"]
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    here = [d for d in decisions if d["db_id"] == db_id and d.get("span", "") == span]
    base = {"db_id": db_id, "span": span, "tier": q.get("tier", ""),
            "pattern": q.get("pattern", ""),
            "org_name_yiddish": q.get("troupe_yiddish", ""),
            "reviewer": reviewer, "reviewed_at": now}
    note_txt = "Zalmen troupe questions" + (f": {note}" if note else "")
    out = {}

    if action == "confirm":
        h = hubs.get(hub_id)
        if not h:
            return False, f"{hub_id} is not in person_hub on GitHub — nothing saved."
        other = sorted({d["hub_id"] for d in here
                        if (d.get("decision") or "").upper() == "CONFIRM"} - {hub_id})
        if other:
            return False, (f"this name is already linked to {', '.join(other)} — "
                           "nothing saved. Tell Sinai if that link is wrong.")
        if any(d.get("hub_id") == hub_id and (d.get("decision") or "").upper() == "CONFIRM"
               for d in here):
            return True, "already recorded — nothing to change"
        row = {**base, "hub_id": hub_id, "decision": "CONFIRM",
               "person_heading": h.get("canonical_heading", ""),
               "person_db_ids": h.get("db_ids", ""), "notes": note_txt}
        out["decisions"] = pm.append_rows(remote["decisions"], [row])
        msg = f"db{db_id} {span} → {h.get('canonical_heading', '')}"
    elif action == "none":
        if any((d.get("decision") or "").upper() == "CONFIRM" for d in here):
            return False, "this name is already linked to a person — nothing saved."
        if any((d.get("decision") or "").upper() == "REJECT" and not d.get("hub_id")
               for d in here):
            return True, "already recorded — nothing to change"
        row = {**base, "hub_id": "", "decision": "REJECT", "person_heading": "",
               "person_db_ids": "", "notes": f"{note_txt}: no person"}
        out["decisions"] = pm.append_rows(remote["decisions"], [row])
        msg = f"db{db_id} {span} → not a person"
    elif action == "mint":
        if any((b["org_db_id"], b["span"]) == (db_id, span)
               for b in pm.parse(remote["blocked"])[1]):
            return True, "already waiting in Eponym · mint people"
        row = {"workbook_row": "", "org_db_id": db_id,
               "org_name_yiddish": q.get("troupe_yiddish") or q.get("troupe_latin", ""),
               "span": span, "reviewer_answer": typed, "has_people_db_row": "",
               "needs": "mint, then hub", "mentioned_by": q.get("mentioned_by", ""),
               "roles_dates": q.get("roles_dates", ""),
               "lexicon_context": q.get("context", ""), "answered_by": reviewer}
        out["blocked"] = pm.append_rows(remote["blocked"], [row])
        msg = f"db{db_id} {span} → “{typed}” sent to Eponym · mint people"
    else:
        return False, "unknown action"

    for k, data in out.items():
        _write_bytes(_local(k), data)
    ok = push_files_to_github([(f"{REPO}/{FILES[k]}", _local(k)) for k in out],
                              f"eponym troupe question: {msg}")
    if not ok:
        return False, "saved on this server but NOT on GitHub — please tell Sinai."
    log_action("eponym_troupes", action, target_id=f"{db_id}|{span}",
               decision={"confirm": "CONFIRM", "none": "REJECT", "mint": "BLOCKED"}[action],
               note=msg, hub_id=hub_id)
    _parse.clear()
    return True, msg


# ── render ───────────────────────────────────────────────────────────────────
def _question(q: dict, hub_label: dict, hub_ids: list[str], reviewer: str) -> None:
    k = "ept_" + re.sub(r"\W+", "_", f"{q['db_id']}_{q['span']}")
    st.markdown(f"**Eponym {q['eponym']}** &nbsp; {_rtl(q['span'])}", unsafe_allow_html=True)

    def act(action, hub_id="", typed=""):
        with st.spinner("Saving on top of the latest files on GitHub…"):
            ok, msg = _save(q, action, hub_id, typed,
                            st.session_state.get(k + "_note", "").strip(), reviewer)
        st.session_state["ept_flash"] = ("success" if ok else "error", msg)
        st.rerun()

    can = bool(reviewer)
    if q.get("proposal_hub"):
        with st.container(border=True):
            st.caption(f"Model proposal · {q.get('proposal_conf') or '?'} confidence")
            st.markdown(_rtl(q.get("proposal", "")), unsafe_allow_html=True)
            if q.get("evidence"):
                st.markdown(_rtl("“" + q["evidence"] + "”"), unsafe_allow_html=True)
            if st.button("Accept", key=k + "_acc", type="primary", disabled=not can):
                act("confirm", q["proposal_hub"])
    elif q.get("judge") == "NONE":
        st.info("The model read the Leksikon and found **no person** named by this word.")
    elif q.get("evidence") and q.get("evidence") != "None":
        st.caption(q["evidence"][:400])

    cands = [h for h in (q.get("cand_hubs") or "").split("|") if h]
    if cands:
        st.caption("Candidates")
        cols = st.columns(min(3, len(cands)))
        for i, h in enumerate(cands):
            if cols[i % len(cols)].button(hub_label.get(h, h), key=f"{k}_c{i}",
                                          disabled=not can):
                act("confirm", h)

    c1, c2 = st.columns([3, 1])
    pick = c1.selectbox("Someone else — search all people", [""] + hub_ids,
                        format_func=lambda h: hub_label.get(h, "") if h else "",
                        key=k + "_s", index=0)
    c2.write("")
    if c2.button("Link", key=k + "_link", disabled=not (can and pick)):
        act("confirm", pick)

    c3, c4 = st.columns([3, 1])
    typed = c3.text_input("Not in the list? Name the person (goes to Eponym · mint people)",
                          key=k + "_typed")
    c4.write("")
    if c4.button("Send", key=k + "_mint", disabled=not (can and typed.strip())):
        act("mint", typed=typed.strip())

    c5, c6 = st.columns([3, 1])
    c5.text_input("Note (optional, saved with your answer)", key=k + "_note")
    c6.write("")
    if c6.button("Not a person", key=k + "_none", disabled=not can):
        act("none")


def render() -> None:
    st.header("Eponym · troupe questions")
    st.caption("Each troupe below is named after someone. Pick who: accept the "
               "model's proposal, click a candidate, or search all people. If "
               "the word is not a name, choose Not a person. Every answer saves "
               "immediately; Sinai turns them into links afterwards.")
    flash = st.session_state.pop("ept_flash", None)
    if flash:
        getattr(st, flash[0])(flash[1])

    reviewer = st.session_state.get("reviewer", "")
    if not reviewer:
        st.warning("Log in (sidebar) to answer.")
    questions = _rows("questions")
    if not questions:
        st.warning("eponym_open_questions.tsv is missing — run build_eponym_ra_workbook.py.")
        return
    blocked = _rows("blocked")
    closed = _closed(_rows("decisions"), blocked)
    open_q = [q for q in questions if (q["db_id"], q["span"]) not in closed]
    waiting = {(b["org_db_id"], b["span"]) for b in blocked}
    n_mint = sum(1 for q in questions if (q["db_id"], q["span"]) in waiting)

    hubs = _rows("hubs")
    hub_label = {h["hub_id"]: (f"{h.get('canonical_heading', '')} "
                               f"[{(h.get('db_ids') or '').split(',')[0] or h['hub_id']}]"
                               + (f" {h.get('birth_date', '')}–{h.get('death_date', '')}"
                                  if h.get("birth_date") or h.get("death_date") else ""))
                 for h in hubs if h.get("hub_id")}
    hub_ids = sorted(hub_label, key=hub_label.get)

    by_cat = collections.Counter(_category(q) for q in open_q)
    done = len(questions) - len(open_q) - n_mint
    st.progress(1 - len(open_q) / len(questions),
                text=f"{done} answered · {n_mint} waiting in Eponym · mint people · "
                     f"{len(open_q)} open (of {len(questions)})")
    cat = st.radio("Show", list(CATS), horizontal=True, key="ept_cat",
                   format_func=lambda c: f"{CATS[c][0]} ({by_cat.get(c, 0)})")
    st.caption(CATS[cat][1])

    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for q in open_q:
        if _category(q) == cat:
            groups[q["db_id"]].append(q)
    if not groups:
        st.success("Nothing open here.")
        return
    ids = list(groups)
    pages = (len(ids) + PAGE - 1) // PAGE
    page = st.number_input(f"Page (of {pages})", 1, pages, 1, key=f"ept_page_{cat}") if pages > 1 else 1
    for db_id in ids[(page - 1) * PAGE: page * PAGE]:
        group = groups[db_id]
        t = group[0]
        st.markdown(f"### {_rtl(t.get('troupe_yiddish') or t.get('troupe_latin', ''))}",
                    unsafe_allow_html=True)
        st.caption(" · ".join(x for x in (f"db {db_id}", t.get("troupe_latin", ""),
                                          t.get("org_type", "")) if x))
        if t.get("context"):
            with st.expander("Leksikon context"):
                for part in t["context"].split("\n\n"):
                    st.markdown(_rtl(_highlight(part, [g["span"] for g in group])),
                                unsafe_allow_html=True)
                meta = " · ".join(x for x in (t.get("mentioned_by", ""), t.get("roles_dates", "")) if x)
                if meta:
                    st.caption(meta[:400])
        for q in group:
            _question(q, hub_label, hub_ids, reviewer)
        st.divider()
