"""Eponym · mint people — resolve the blocked eponym answers.

A blocked answer names a real person who has no hub, so its eponym edge cannot
be written. This is the people-side counterpart of minting a NEW org in
Organizations matching: per person, the reviewer either

  * picks someone ALREADY in people_db (the answer was spelled differently --
    Lea Largman 3840, the Magidzohns 2371/2372 were all there), who either has
    a hub (decision only) or gets a db-only hub, or
  * MINTS a new person, who gets a db-only hub if the Leksikon mentions them.

`attested_in_leksikon=no` follows README_new_mints.md: the person is flagged
probably_not_zylbercweig=1 and gets NO hub, so the answer stays blocked with
that reason. A troupe named after someone in the Leksikon's own text is itself
a mention, so `mentions_only` is the default.

Saves are built on the GitHub copy of every file (see people_mint.py) and land
as ONE commit. Edges are NOT written here: apply_eponym_decisions.py turns the
CONFIRM rows into edges offline, so person_org_edges.tsv keeps its two writers.
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
    "people": "people/people_db.tsv",
    "evidence": "people/new_mints_evidence.tsv",
    "hubs": "people/person_hub.tsv",
    "ledger": "people/dbonly_hubs.tsv",
    "decisions": "organizations/eponym_decisions.tsv",
    "blocked": "organizations/eponym_blocked_answers.tsv",
}
STYLISTICS = "people/people_db_hebrew_stylistics.tsv"   # ids only, never written


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


def _rtl(s: str) -> str:
    return f'<span dir="rtl" style="font-size:1.05em">{s}</span>' if s else ""


def _snippet(text: str, span: str, width: int = 260) -> str:
    """The context around the span, so the evidence quote starts useful."""
    t = re.sub(r"\s+", " ", text or "").strip()
    i = pm.fold(t).find(pm.fold(span)[:4]) if span else -1
    if i < 0:
        return t[: 2 * width]
    return t[max(0, i - width): i + width]


# ── save ─────────────────────────────────────────────────────────────────────
def _save(group: list[dict], existing: list[str], mint: dict | None,
          attested: str, quote: str, reviewer: str) -> tuple[bool, str]:
    from zalmen.github_sync import fetch_file_from_github, push_files_to_github

    remote = {}
    for k, rel in FILES.items():
        data = fetch_file_from_github(f"{REPO}/{rel}")
        if data is None:
            return False, f"could not read {rel} from GitHub — nothing saved. Try again."
        remote[k] = data
    styl = fetch_file_from_github(f"{REPO}/{STYLISTICS}") or b""

    people = {r["db_id"].strip(): r for r in pm.parse(remote["people"])[1]
              if (r.get("db_id") or "").strip()}
    hub_fields, hubs = pm.parse(remote["hubs"])
    hub_by_db = pm.hubs_by_db_id(hubs)
    hub_ids = {h.get("hub_id", "") for h in hubs}
    ev_ids = {r.get("db_id", "").strip() for r in pm.parse(remote["evidence"])[1]}
    have = {(d["db_id"], d.get("hub_id", ""), d.get("span", ""))
            for d in pm.parse(remote["decisions"])[1]}

    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    today = now[:10]
    orgs = ", ".join(sorted({b["org_db_id"] for b in group}, key=int))
    new = collections.defaultdict(list)
    targets: list[tuple[str, str, str, str]] = []     # db_id, heading, hub_id, how
    notes: list[str] = []

    for db_id in existing:
        p = people.get(db_id)
        if p is None:
            return False, f"db {db_id} is not in people_db on GitHub — nothing saved."
        h = hub_by_db.get(db_id)
        if h:
            targets.append((db_id, h.get("canonical_heading", ""), h["hub_id"],
                            "existing person with hub"))
            continue
        if (p.get("probably_not_zylbercweig") or "").strip() == "1" or attested == "no":
            notes.append(f"db {db_id}: not attested in the Leksikon, so no hub (policy)")
            continue
        heading = (p.get("hebname") or "").strip() or (p.get("english") or "").strip()
        if db_id not in ev_ids:
            new["evidence"].append(pm.evidence_row(
                db_id, heading, (p.get("english") or "").strip(), attested, quote,
                "", orgs, "hub evidence for a pre-existing people_db row",
                reviewer, today))
        new["hubs"].append(pm.hub_row(hub_fields, db_id, heading))
        new["ledger"].append(pm.ledger_row(db_id, heading, attested, reviewer, now))
        targets.append((db_id, heading, f"HUB-D{db_id}", "existing person, new db-only hub"))

    minted = ""
    if mint:
        minted = str(pm.next_db_id(remote["people"], styl))
        if f"HUB-D{minted}" in hub_ids:
            return False, f"HUB-D{minted} already exists — nothing saved; tell Sinai."
        new["people"].append(pm.person_row(
            minted, mint["yid"], mint["eng"], mint["gender"], mint["born"],
            mint["died"], mint["role"], mint["variants"], attested,
            f"Zalmen eponym mint ({reviewer} {today})"))
        new["evidence"].append(pm.evidence_row(
            minted, mint["yid"], mint["eng"], attested, quote, mint["role"], orgs,
            mint["open_questions"], reviewer, today))
        if attested == "mentions_only":
            new["hubs"].append(pm.hub_row(hub_fields, minted, mint["yid"]))
            new["ledger"].append(pm.ledger_row(minted, mint["yid"], attested, reviewer, now))
            targets.append((minted, mint["yid"], f"HUB-D{minted}", "minted"))
        else:
            notes.append(f"minted db {minted}; not attested in the Leksikon, so no hub (policy)")

    for db_id, heading, hub_id, how in targets:
        for b in group:
            if (b["org_db_id"], hub_id, b["span"]) not in have:
                new["decisions"].append(pm.decision_row(b, hub_id, heading, db_id,
                                                        reviewer, now, how))
    if not (targets or minted):
        return False, "nothing to save: " + ("; ".join(notes) or "no action chosen")

    out = {k: pm.append_rows(remote[k], new[k]) for k in
           ("people", "evidence", "hubs", "ledger", "decisions")}
    keys = {(b["org_db_id"], b["span"]) for b in group}
    blocked = pm.drop_rows(remote["blocked"],
                           lambda r: (r["org_db_id"], r["span"]) not in keys)
    if not targets:
        # minted, but no hub by policy: the answer stays blocked, with why
        stay = [{**b, "has_people_db_row": minted,
                 "needs": "no hub: attested_in_leksikon=no (README_new_mints)"}
                for b in group]
        blocked = pm.append_rows(blocked, stay)
    out["blocked"] = blocked

    changed = {k: v for k, v in out.items() if v != remote[k]}
    for k, data in changed.items():
        _write_bytes(_local(k), data)
    who = group[0].get("reviewer_answer", "")
    msg = (f"eponym mint: {who} — {len(new['decisions'])} decisions"
           + (f", minted db {minted}" if minted else "")
           + (f", {len(new['hubs'])} hub(s)" if new["hubs"] else ""))
    ok = push_files_to_github([(f"{REPO}/{FILES[k]}", _local(k)) for k in changed], msg)
    if not ok:
        return False, ("saved on this server but NOT on GitHub — please tell "
                       "Sinai before continuing.")
    log_action("eponym_mint", "resolve_blocked", target_id=orgs,
               decision="MINT" if minted else "LINK", note=msg,
               minted_db_id=minted, hubs=len(new["hubs"]),
               decisions=len(new["decisions"]))
    _parse.clear()
    return True, msg + ("" if not notes else " · " + "; ".join(notes))


# ── render ───────────────────────────────────────────────────────────────────
def _card(answer: str, group: list[dict], people: list[dict], hubs: list[dict],
          reviewer: str) -> None:
    k = "epm_" + re.sub(r"\W+", "_", f"{group[0]['org_db_id']}_{group[0]['span']}")
    st.markdown(f"### {answer or '(blank answer)'}")
    for b in group:
        st.markdown(f"**db{b['org_db_id']}** {_rtl(b.get('org_name_yiddish', ''))} "
                    f"&nbsp; span {_rtl(b['span'])}", unsafe_allow_html=True)
        meta = " · ".join(x for x in (b.get("mentioned_by", ""),
                                      b.get("roles_dates", "")) if x)
        if meta:
            st.caption(meta[:300])
    ctx = next((b.get("lexicon_context", "") for b in group if b.get("lexicon_context")), "")
    if ctx:
        with st.expander("Leksikon context"):
            st.markdown(_rtl(ctx[:2500]), unsafe_allow_html=True)

    cands = pm.candidates([b["span"] for b in group], answer, people, hubs)
    labels = {
        c["db_id"]: (f"{c['hebname']} · {c['english']} [db {c['db_id']}]"
                     + (f" {c['born']}–{c['died']}" if c["born"] or c["died"] else "")
                     + (f" · hub {c['hub_id']}" if c["hub_id"] else " · NO hub")
                     + (" · not in Leksikon" if c["not_zylbercweig"] else ""))
        for c in cands}
    st.markdown("**1. Is this someone already in the database?**")
    if not cands:
        st.caption("No one in people_db shares this surname (vowel spellings ignored).")
    picked = st.multiselect(
        "Existing people (pick all that apply — e.g. a couple)",
        list(labels), format_func=labels.get, key=k + "_ex",
        help="Shown because the surname matches when vowel letters are "
             "ignored. A shared surname is NOT evidence: check given name, dates "
             "and roles against the Leksikon context.")
    other = st.text_input("…or another people_db id (comma-separated)", key=k + "_oth")
    existing = picked + [x.strip() for x in other.split(",") if x.strip().isdigit()]

    st.markdown("**2. Or mint a new person**")
    do_mint = st.checkbox("Mint a new person for this answer", key=k + "_mint",
                          value=not cands)
    mint = None
    if do_mint:
        yid_default = answer if re.search(r"[֐-׿]", answer) else ""
        eng_default = "" if yid_default else re.sub(r"\s*--.*$", "", answer)
        c1, c2 = st.columns(2)
        yid = c1.text_input("Name in Yiddish (as the Leksikon spells it)",
                            value=yid_default, key=k + "_yid")
        eng = c2.text_input("Name in English", value=eng_default, key=k + "_eng")
        c3, c4, c5 = st.columns(3)
        gender = c3.selectbox("Gender", ["", "m", "f"], key=k + "_g")
        born = c4.text_input("Born", key=k + "_b")
        died = c5.text_input("Died", key=k + "_d")
        role = st.text_input("Role (actor, director, manager…)", key=k + "_r")
        variants = st.text_input(
            "Spelling variants (separate with | )",
            value=" | ".join(dict.fromkeys(b["span"] for b in group)), key=k + "_v")
        oq = st.text_input("Open questions (optional)", key=k + "_oq")
        mint = {"yid": yid.strip(), "eng": eng.strip(), "gender": gender,
                "born": born.strip(), "died": died.strip(), "role": role.strip(),
                "variants": variants.strip(), "open_questions": oq.strip()}

    st.markdown("**3. Attestation**")
    attested = st.radio(
        "Does the Leksikon mention this person?", pm.ATTESTED, key=k + "_att",
        format_func={"mentions_only": "Yes — mentioned (e.g. in this troupe's "
                     "name or another entry), no entry of their own",
                     "no": "No — not in the Leksikon at all (gets no hub, so "
                     "the answer stays blocked)"}.get, horizontal=False)
    quote = st.text_area("Evidence — quote what the Leksikon says",
                         value=_snippet(ctx, group[0]["span"]), key=k + "_q",
                         height=90)

    problems = []
    if not existing and not do_mint:
        problems.append("choose an existing person or mint a new one")
    if do_mint and not re.search(r"[֐-׿]", mint["yid"]):
        problems.append("a new person needs their Yiddish name")
    if not quote.strip():
        problems.append("add the evidence quote")
    if not reviewer:
        problems.append("log in first")
    if problems:
        st.caption("To save: " + "; ".join(problems) + ".")
    if st.button("Save", key=k + "_save", type="primary", disabled=bool(problems)):
        with st.spinner("Saving on top of the latest files on GitHub…"):
            ok, msg = _save(group, existing, mint, attested, quote.strip(), reviewer)
        st.session_state["epm_flash"] = ("success" if ok else "error", msg)
        st.rerun()
    st.divider()


def render() -> None:
    st.header("Eponym · mint people")
    st.caption(
        "Answers that name a real person with no hub — so the troupe cannot be "
        "linked to them yet. For each: pick the person if they are already in "
        "the database under another spelling, or mint them. Each save is one "
        "commit; Sinai turns the decisions into links afterwards.")
    flash = st.session_state.pop("epm_flash", None)
    if flash:
        getattr(st, flash[0])(flash[1])

    blocked = _rows("blocked")
    if not blocked:
        st.success("No blocked eponym answers.")
        return
    reviewer = st.session_state.get("reviewer", "")
    people, hubs = _rows("people"), _rows("hubs")

    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for b in blocked:
        groups[b.get("reviewer_answer", "")].append(b)
    waiting = {a: g for a, g in groups.items()
               if not g[0].get("needs", "").startswith("no hub")}
    parked = {a: g for a, g in groups.items() if a not in waiting}
    st.caption(f"**{len(waiting)} people** to resolve ({sum(map(len, waiting.values()))} "
               f"answers)" + (f" · {len(parked)} minted but not in the Leksikon, "
                              "parked" if parked else ""))
    for answer, group in sorted(waiting.items(), key=lambda kv: -len(kv[1])):
        _card(answer, group, people, hubs, reviewer)
    if parked:
        with st.expander(f"Parked: minted, not in the Leksikon ({len(parked)})"):
            for answer, group in parked.items():
                st.markdown(f"- **{answer}** — db {group[0].get('has_people_db_row', '')}, "
                            + ", ".join(f"db{b['org_db_id']}" for b in group))
