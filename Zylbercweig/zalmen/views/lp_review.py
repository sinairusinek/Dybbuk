"""Typed link review — blind, one mention at a time (historical-kg-link-prediction).

Purpose: grow an UNBIASED gold standard for typed, open-world entity resolution.
Each item is a bare-surname / initials mention whose referent is unknown. The
reviewer decides what the mention IS:

  same       the mention is candidate X
  kin        the mention is a relative of candidate X (parent / child / spouse /
             sibling / other kin), i.e. a different person related to X
  new        a person with no entry (not any candidate, no known relation)
  not_person not a person (a place, a troupe, an extraction error)
  unsure     cannot decide from the available evidence

Differences from B3 (surname review), on purpose:
  * NO suggestion is pre-selected and none is shown (avoids anchoring; the
    existing gold concentrates 97% of decisions on the most common referent).
  * Candidates appear in a fixed random order, given by the queue.
  * Each candidate card shows the evidence relative to THIS mention: shared
    organisations and places with the host, birth-year gap to the host, the
    candidate's age at the relation date, death before the relation.
  * About 10% of items are hidden controls with existing gold (agreement check).

Queue:     Zylbercweig/people/lp_review_queue.tsv      (item, mention_id, candidate_order)
           v2 (2026-11-24): candidates from a phonetic/edit-distance + graph retriever (top 8),
           so the true person can appear even when the surname lookup misses them.
Decisions: Zylbercweig/people/lp_review_decisions.tsv  (pushed like B3 decisions)
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
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
from atomic_io import atomic_write  # noqa: E402

PEOPLE = BASE / "people"
ER_DIR = BASE / "datasets" / "surname_er"
QUEUE_TSV = PEOPLE / "lp_review_queue.tsv"
DECISIONS_TSV = PEOPLE / "lp_review_decisions.tsv"
REPO_DECISIONS_PATH = "Zylbercweig/people/lp_review_decisions.tsv"
TEXT_CAP = 20_000

FIELDS = ["item", "mention_id", "mention_name", "host_hub_id", "decision",
          "target_hub_id", "kin_relation", "confidence", "reviewer",
          "reviewed_at", "notes"]
KIN_LABELS = {"parent": "parent of", "child": "child of", "spouse": "spouse of",
              "sibling": "sibling of", "other_kin": "other relative of"}
MARKS = "①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳"


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


def _read(p: pathlib.Path) -> list[dict]:
    csv.field_size_limit(sys.maxsize)
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


@st.cache_data(show_spinner=False)
def _load_static(_m: tuple) -> dict:
    er = {r["mention_id"]: r for r in _read(ER_DIR / "er_mention_context.tsv")}
    prof = {r["hub_id"]: r for r in _read(ER_DIR / "er_candidate_profiles.tsv")}
    hubs = {r["hub_id"]: r for r in _read(PEOPLE / "person_hub.tsv")}
    english = {}
    db = {r["db_id"]: r for r in _read(PEOPLE / "people_db.tsv") if r.get("db_id")}
    for r in _read(PEOPLE / "person_hub_members.tsv"):
        if r["member_kind"] == "db" and r["member_id"] in db:
            english.setdefault(r["hub_id"], db[r["member_id"]].get("english", ""))
    texts = {}
    p = PEOPLE / "entry_texts.tsv"
    if p.exists():
        texts = {r["person_id"]: r["entry_text"] for r in _read(p)}
    # D-G full-text extraction (datasets-2026-11-24c+): what the host entry says the mention did
    facts = {}
    d = ER_DIR / "defg"
    if (d / "er_mention_defg_links.tsv").exists():
        links = {}
        for r in _read(d / "er_mention_defg_links.tsv"):
            links.setdefault(r["mkey"], []).append(r["mention_id"])
        events = {r["ekey"]: r for r in _read(d / "defg_events.tsv")}
        for r in _read(d / "defg_event_participants.tsv"):
            for mid in links.get(r["person_key"], ()):
                e = events.get(r["ekey"], {})
                facts.setdefault(mid, []).append(" ".join(x for x in (
                    e.get("event_type", "") + ":", e.get("event_text", "")[:140],
                    f"(role: {r['role']})" if r.get("role") else "", e.get("place_en", ""), e.get("date_start", "")) if x))
        for r in _read(d / "defg_performance_facts.tsv"):
            for mid in links.get(r["person_key"], ()):
                facts.setdefault(mid, []).append(" ".join(x for x in (
                    r.get("function", ""), f"as {r['character']}" if r.get("character") else "",
                    f"in {r['work']}" if r.get("work") else "", r.get("org", ""), r.get("venue", ""),
                    r.get("place_en", ""), r.get("date_start", "")) if x))
    return {"er": er, "prof": prof, "hubs": hubs, "english": english, "texts": texts, "facts": facts}


def _load_queue() -> list[dict]:
    return _read(QUEUE_TSV) if QUEUE_TSV.exists() else []


def _load_decisions() -> dict[str, dict]:
    if not DECISIONS_TSV.exists():
        return {}
    return {r["item"]: r for r in _read(DECISIONS_TSV) if r.get("item")}


def _save(row: dict) -> None:
    dec = _load_decisions()
    dec[row["item"]] = row
    with atomic_write(DECISIONS_TSV) as fp:
        w = csv.DictWriter(fp, fieldnames=FIELDS, delimiter="\t", extrasaction="ignore",
                           quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writeheader()
        for v in sorted(dec.values(), key=lambda r: r["item"]):
            w.writerow(v)
    try:
        from zalmen.github_sync import push_file_to_github
        push_file_to_github(REPO_DECISIONS_PATH, DECISIONS_TSV,
                            f"lp review: {row['item']} {row['decision']}")
    except Exception:
        pass
    try:
        from zalmen.activity_log import log_action
        log_action("lp_review", "typed_link_decision", target_id=row["mention_id"],
                   decision=row["decision"], note=row.get("notes", ""), push=True,
                   resolved_hub_id=row.get("target_hub_id", ""))
    except Exception:
        pass


def _m(i: int) -> str:
    return MARKS[i] if i < len(MARKS) else f"({i + 1})"


# ---------------------------------------------------------------- display helpers
def _rtl(text: str, size: float = 1.0, weight: int = 400) -> str:
    return f"<div dir='rtl' style='font-size:{size}rem;font-weight:{weight};line-height:1.6'>{text}</div>"


def _strip(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s or "") if not unicodedata.combining(c))


def _mark(text: str, surface: str, tag="strong") -> str:
    """Escape text; emphasise the surface (prefix match, points optional, inflections allowed)."""
    text = unicodedata.normalize("NFD", text or "")
    base = _strip(surface).strip()
    if not base:
        return html.escape(text)
    pat = "".join(re.escape(ch) + r"[֑-ׇ]*" for ch in base) + r"[\w֑-ׇ]*"
    out, pos = [], 0
    for mo in re.finditer(pat, text):
        out += [html.escape(text[pos:mo.start()]), f"<{tag}>{html.escape(mo.group(0))}</{tag}>"]
        pos = mo.end()
    out.append(html.escape(text[pos:]))
    return "".join(out)


def _year(s: str):
    m = re.search(r"(1[5-9]\d\d|20[0-2]\d)", s or "")
    return int(m.group(1)) if m else None


def _orgs(p: dict) -> dict[str, str]:
    """org id -> 'name [category] years' from a candidate profile."""
    out = {}
    for chunk in (p.get("orgs_detail") or "").split(" ;; "):
        if "=" in chunk:
            oid, rest = chunk.split("=", 1)
            out[oid.strip()] = rest.replace("@", "").strip()
    return out


def _evidence(m: dict, p: dict) -> list[str]:
    """Human-readable evidence chips for candidate p against mention m."""
    chips = []
    host_orgs = set(filter(None, (m.get("host_org_ids") or "").split("|")))
    po = _orgs(p)
    shared = [po[o].split("[")[0].strip() for o in po if o in host_orgs]
    if shared:
        chips.append("🏛 shared with host: " + " · ".join(shared[:4]))
    if m.get("c_org_id") and m["c_org_id"] in po:
        chips.append("🎭 the organisation in the sentence is in this person's record")
    hs = set(filter(None, (m.get("host_active_settlements") or "").split("|")))
    ps = set(filter(None, (p.get("active_settlements") or "").split("|")))
    if hs & ps:
        chips.append("📍 shared places: " + " · ".join(sorted(hs & ps)[:4]))
    pb, hb = _year(p.get("birth_year")), _year(m.get("host_birth_year"))
    if pb and hb:
        gap = hb - pb
        chips.append(f"🕰 born {abs(gap)} y {'before' if gap > 0 else 'after'} the host"
                     if gap else "🕰 born the same year as the host")
    d = _year(m.get("rel_date_start")) or _year(m.get("rel_date_end"))
    if d and pb:
        chips.append(f"🎂 aged ~{d - pb} at the relation date ({d})")
    hq = set(re.findall(r"Q\d+", m.get("host_defg_places_en") or ""))
    shared_st = []
    for st_ in (p.get("defg_places_en") or "").split("|"):
        if set(re.findall(r"Q\d+", st_)) & hq:
            nm = re.sub(r"\s*\(.*?\)", "", st_.rsplit(":", 1)[0] if re.search(r":\d{4}$", st_) else st_).strip()
            if nm and nm not in shared_st:
                shared_st.append(nm)
    if shared_st:
        chips.append("🗺 life stations shared with host: " + " · ".join(shared_st[:5]))
    yy = re.findall(r"\d{4}", p.get("defg_years") or "")
    if d and len(yy) == 2:
        inside = int(yy[0]) <= d <= int(yy[1])
        chips.append(f"{'✅' if inside else '⚠'} relation date {d} is {'within' if inside else 'outside'} "
                     f"this person's documented years {yy[0]}–{yy[1]}")
    pd_ = _year(p.get("death_year"))
    if d and pd_ and pd_ < d - 1:
        chips.append(f"⚠ died {pd_}, before the relation date ({d})")
    if m.get("gender") and p.get("gender") and m["gender"] != p["gender"]:
        chips.append(f"⚥ mention is {m['gender']}, candidate is {p['gender']}")
    return chips or ["no overlapping evidence in the structured data"]


def _entry_block(text: str, surface: str) -> None:
    if not text:
        st.caption("No entry text on file.")
        return
    shown = _mark(text[:TEXT_CAP], surface, "mark").replace("⏎", "<br>")
    st.markdown(_rtl(shown), unsafe_allow_html=True)
    if len(text) > TEXT_CAP:
        st.caption(f"…truncated at {TEXT_CAP:,} of {len(text):,} characters.")


def _entry_text(hub: dict, texts: dict, volume: str = "") -> str:
    pids = [x for x in (hub.get("entry_person_ids") or "").split("|") if x]
    pids.sort(key=lambda x: not x.startswith(f"P-{volume}-"))
    return next((texts[x] for x in pids if texts.get(x)), "")


# ---------------------------------------------------------------- page
def render() -> None:
    st.header("Typed link review (blind)")
    st.caption("Who is this mention? Same person as a candidate, a relative of one, "
               "or someone without an entry. No suggestion is shown on purpose.")
    queue = _load_queue()
    if not queue:
        st.info(f"No queue found at {QUEUE_TSV.relative_to(BASE.parent)}.")
        return
    data = _load_static((_mtime(ER_DIR / "er_mention_context.tsv"), _mtime(PEOPLE / "entry_texts.tsv")))
    decisions = _load_decisions()

    reviewer = st.sidebar.text_input("Reviewer", value=st.session_state.get("lp_reviewer", ""),
                                     key="lp_reviewer")
    only_open = st.sidebar.checkbox("Show only undecided", value=True)
    items = [q for q in queue if not (only_open and q["item"] in decisions)]
    st.progress(len(decisions) / len(queue), text=f"{len(decisions)} / {len(queue)} decided")
    if not items:
        st.success("All items decided. Thank you!")
        return
    idx = st.session_state.get("lp_idx", 0) % len(items)
    nav = st.columns([1, 1, 4])
    if nav[0].button("◀ Previous", use_container_width=True):
        idx = (idx - 1) % len(items)
    if nav[1].button("Next ▶", use_container_width=True):
        idx = (idx + 1) % len(items)
    st.session_state["lp_idx"] = idx
    q = items[idx]
    m = data["er"].get(q["mention_id"])
    if not m:
        st.error(f"Mention {q['mention_id']} not found in er_mention_context.tsv")
        return
    host = data["hubs"].get(m["host_hub_id"], {})
    prior = decisions.get(q["item"], {})

    # ---- the mention in context
    st.subheader(f"{q['item']}")
    st.markdown(_rtl(_mark(m["relation_sentence"], m["mention_name"]), 1.25), unsafe_allow_html=True)
    facts = [f"**Mention:** {m['mention_name']}",
             f"**Relation to host:** {m['relation_category']} / {m['relation_type']}".rstrip(" /")]
    if m.get("person_description"):
        facts.append(f"**Described as:** {m['person_description']}")
    if m.get("rel_settlement") or m.get("rel_country"):
        facts.append(f"**Where:** {m.get('rel_settlement') or m.get('rel_country')}")
    if m.get("rel_date_start"):
        facts.append(f"**When:** {m['rel_date_start']}" + (f"–{m['rel_date_end']}" if m.get("rel_date_end") else ""))
    if m.get("c_org_name"):
        facts.append(f"**Organisation in the sentence:** {m['c_org_name']}")
    if m.get("gender"):
        facts.append(f"**Gender (as extracted):** {m['gender']}")
    st.markdown(" · ".join(facts))
    hb = host.get("birth_date") or m.get("host_birth_year") or "?"
    st.markdown(_rtl(f"<b>Host entry:</b> {html.escape(host.get('canonical_heading', m['host_heading']))} "
                     f"({hb} – {host.get('death_date', '') or '?'})", 1.05), unsafe_allow_html=True)
    if m.get("host_defg_places_en"):
        stations = [re.sub(r"\s*\(.*?\)", "", x) for x in m["host_defg_places_en"].split("|") if x and not x.startswith("?")]
        st.markdown("**Host's life stations:** " + " → ".join(stations[:14]))
    if m.get("in_entry_full_form"):
        st.markdown(_rtl(f"<b>The entry's own expansion of the name</b> "
                         f"<span style='color:#888'>({html.escape(m.get('in_entry_status') or '')}; a model's reading, may be wrong)</span>: "
                         f"{html.escape(m['in_entry_full_form'])}"), unsafe_allow_html=True)
    fx = data["facts"].get(q["mention_id"], [])
    if fx:
        st.markdown("**What the host's entry says this person did:**")
        st.markdown(_rtl("<br>".join(html.escape(f) for f in fx[:8])), unsafe_allow_html=True)
    with st.expander("📜 Host entry (full text, mention highlighted)", expanded=False):
        _entry_block(_entry_text(host, data["texts"], m.get("volume", "")), m["mention_name"])

    # ---- candidates with evidence
    cands = [c for c in (q.get("candidate_order") or "").split("|") if c]
    st.markdown(f"#### Candidates ({len(cands)} shown"
                + (f" of {m['n_candidates']}" if m.get("n_candidates") and int(m['n_candidates']) > len(cands) else "")
                + ")")
    cols = st.columns(min(len(cands), 3) or 1)
    for i, hid in enumerate(cands):
        p, h = data["prof"].get(hid, {}), data["hubs"].get(hid, {})
        with cols[i % len(cols)]:
            with st.container(border=True):
                st.markdown(f"**{_m(i)}** `{hid}`")
                st.markdown(_rtl(html.escape(p.get("heading") or h.get("canonical_heading", "?")), 1.15, 600),
                            unsafe_allow_html=True)
                if data["english"].get(hid):
                    st.markdown(f"*{data['english'][hid]}*")
                st.caption(f"{p.get('birth_year') or h.get('birth_date') or '?'} – "
                           f"{p.get('death_year') or h.get('death_date') or '?'}"
                           + (f" · born {p['birth_place']}" if p.get("birth_place") else "")
                           + (f" · roles {p['roles_db']}" if p.get("roles_db") else ""))
                for chip in _evidence(m, p):
                    st.markdown(f"- {chip}")
                if p.get("defg_places_en"):
                    stn = [re.sub(r"\s*\(.*?\)", "", x) for x in p["defg_places_en"].split("|") if x and not x.startswith("?")]
                    st.caption("🗺 " + " → ".join(stn[:12]) + (f"  ({p['defg_years']})" if p.get("defg_years") else ""))
                works = [w for w in (p.get("defg_works") or "").split(" ;; ") if w]
                if works:
                    with st.expander(f"🎭 works ({len(works)})"):
                        st.markdown(_rtl("<br>".join(html.escape(w) for w in works[:40])), unsafe_allow_html=True)
                orgs = list(_orgs(p).values())
                if orgs:
                    with st.expander(f"🏛 organisations ({len(orgs)})"):
                        st.markdown(_rtl("<br>".join(html.escape(o) for o in orgs)), unsafe_allow_html=True)
                with st.expander("📜 own entry"):
                    _entry_block(_entry_text(h, data["texts"]), m["mention_name"])

    # ---- decision
    st.markdown("#### Decision")
    opts = ([f"same:{c}" for c in cands] + [f"kin:{c}" for c in cands]
            + ["new", "not_person", "unsure"])

    def fmt(o: str) -> str:
        if o.startswith("same:"):
            return f"is {_m(cands.index(o[5:]))} (same person)"
        if o.startswith("kin:"):
            return f"is a relative of {_m(cands.index(o[4:]))}"
        return {"new": "a person with no entry (none of these, no known relation)",
                "not_person": "not a person", "unsure": "can't tell"}[o]

    prior_opt = None
    if prior:
        prior_opt = (f"{prior['decision']}:{prior['target_hub_id']}" if prior["decision"] in ("same", "kin")
                     else prior["decision"])
    choice = st.radio("This mention…", opts, format_func=fmt, key=f"lp_choice_{q['item']}",
                      index=opts.index(prior_opt) if prior_opt in opts else None)
    kin_rel = ""
    if choice and choice.startswith("kin:"):
        kin_rel = st.selectbox("Relation (the mention is the candidate's …)", list(KIN_LABELS),
                               format_func=lambda k: KIN_LABELS[k].replace(" of", ""),
                               key=f"lp_kin_{q['item']}",
                               index=list(KIN_LABELS).index(prior["kin_relation"])
                               if prior.get("kin_relation") in KIN_LABELS else 0)
    conf = st.radio("Confidence", ["sure", "likely"], horizontal=True, key=f"lp_conf_{q['item']}",
                    index=1 if prior.get("confidence") == "likely" else 0)
    note = st.text_input("Note (optional)", value=prior.get("notes", ""), key=f"lp_note_{q['item']}")
    if st.button("💾 Save and next", type="primary", disabled=not (choice and reviewer)):
        dec, target = (choice.split(":", 1) + [""])[:2]
        _save({"item": q["item"], "mention_id": q["mention_id"], "mention_name": m["mention_name"],
               "host_hub_id": m["host_hub_id"], "decision": dec, "target_hub_id": target,
               "kin_relation": kin_rel, "confidence": conf, "reviewer": reviewer,
               "reviewed_at": _dt.datetime.now().isoformat(timespec="seconds"), "notes": note})
        st.session_state["lp_idx"] = idx if only_open else idx + 1
        st.rerun()
    if not reviewer:
        st.caption("Enter your name in the sidebar to save.")
