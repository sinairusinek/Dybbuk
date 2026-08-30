"""KG link review — minted person / place surfaces from the lexicon layers.

Queue unit: one row of plays/kg_link_review_layers.tsv — a name (vol-3 person
mention) or a birth/death/burial place string that build_kg.py could not link
to people_db / a Wikidata place and therefore minted as an unlinked node.
Most person rows carry a DRAFT candidate (auto_status=candidate) from a
name-variant / fuzzy match against people_db; the reviewer confirms or
corrects it.

Decisions (column `decision`):
  ALIGN          decided_link = person:<db_id> | place:<QID>; the minted node
                 is replaced by the real entity at the next build_kg run
  NOT_ENTITY     not a person / not a place ("at the front", "cemetery")
  KEEP_UNLINKED  reviewed, stays an unlinked node (minor figure, no DB row)

Saves go to the zalmen-data branch via troupe_store (never restart the app).
"""
from __future__ import annotations

import csv
import datetime as dt
import html
import pathlib
import re

import streamlit as st

import troupe_store as store
from lexicon import get_entry_text, JSON_TO_XML  # noqa: F401

BASE = pathlib.Path(__file__).resolve().parents[2]
PEOPLE_DIR = BASE / "people"
PEOPLE_DB = PEOPLE_DIR / "people_db.tsv"
MENTIONS_TSV = PEOPLE_DIR / "people_mentions_extracted.tsv"
EXTRACTED_TSV = PEOPLE_DIR / "people_extracted.tsv"
GAZETTEER = BASE / "zibn-shtern" / "data" / "working" / "toponyms_gazetteer.csv"

# volume -> the IIIorg JSON name lexicon.get_entry_text maps to Structured TEI
_VOL_FILE = {"1": "volume_1IIIorg.json", "2": "volume_2IIIorg.json",
             "3": "Volume_3IIIorg.json", "4": "Volume_4IIIorg.json",
             "5": "Volume5IIIorg.json", "6": "volume6IIIorg.json",
             "7": "volume7IIIorg.json"}

_PID_RE = re.compile(r"P-(\d+)-(facs_[^#|:\s]+)")


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


def _rtl(text: str, size: float = 1.0) -> str:
    return (f"<div dir='rtl' style='font-size:{size}em; line-height:1.6'>"
            f"{text}</div>")


@st.cache_data(show_spinner=False)
def _load_people_db(mtime: float) -> dict[str, dict]:
    csv.field_size_limit(10 ** 9)
    with open(PEOPLE_DB, newline="", encoding="utf-8") as f:
        return {r["db_id"]: r for r in csv.DictReader(f, delimiter="\t") if r.get("db_id")}


@st.cache_data(show_spinner=False)
def _load_mentions_by_name(mtime: float) -> dict[str, list[dict]]:
    """mention surface -> [{host_person_id, host_heading, relation, description}]."""
    out: dict[str, list[dict]] = {}
    if not MENTIONS_TSV.exists():
        return out
    with open(MENTIONS_TSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out.setdefault((r.get("name") or "").strip(), []).append(r)
    return out


@st.cache_data(show_spinner=False)
def _load_place_hosts(mtime: float) -> dict[str, list[tuple[str, str, str]]]:
    """place string -> [(context, person_id, heading)] from the subject entries."""
    out: dict[str, list[tuple[str, str, str]]] = {}
    if not EXTRACTED_TSV.exists():
        return out
    with open(EXTRACTED_TSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            for ctx in ("birth", "death", "burial"):
                v = (r.get(f"{ctx}_place_name") or "").strip()
                if v:
                    out.setdefault(v, []).append((ctx, r["person_id"], r["heading"]))
    return out


@st.cache_data(show_spinner=False)
def _load_gazetteer(mtime: float) -> dict[str, dict]:
    if not GAZETTEER.exists():
        return {}
    with open(GAZETTEER, newline="", encoding="utf-8-sig") as f:
        return {r["qid"]: r for r in csv.DictReader(f)}


def _entry_expander(pid: str, heading: str, key: str) -> None:
    """The usual 'open the entire entry' control: Structured-TEI text by xml:id."""
    m = _PID_RE.match(pid or "")
    with st.expander(f"Full entry context — {heading or pid}", expanded=False):
        if not m:
            st.caption("No entry id on this row.")
            return
        vol, xid = m.group(1), m.group(2)
        text = get_entry_text(_VOL_FILE.get(vol, ""), xid)
        if text:
            st.markdown(
                f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; "
                f"line-height:1.6;'>{html.escape(text)}</div>",
                unsafe_allow_html=True)
        else:
            st.caption(f"Entry not found in XML (vol {vol}, {xid}).")


def _db_card(db: dict) -> str:
    bits = [f"<b>{html.escape(db.get('hebname', ''))}</b>"]
    if db.get("english"):
        bits.append(html.escape(db["english"]))
    dates = " – ".join(x for x in (db.get("date_born", ""), db.get("date_died", "")) if x)
    if dates:
        bits.append(dates)
    if db.get("born_in"):
        bits.append(f"b. {html.escape(db['born_in'])}")
    return " · ".join(bits) + f" <span style='color:#888'>(db {db.get('db_id', '')})</span>"


def _save(row: dict, decision: str, link: str, reviewer: str, note: str = "") -> None:
    rec = {k: row.get(k, "") for k in store.KG_LINK_REVIEW_HEADERS}
    rec.update({
        "decision": decision, "decided_link": link,
        "reviewer": reviewer,
        "reviewed_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    if note:
        rec["reviewer_notes"] = ((rec["reviewer_notes"] + " | ") if rec["reviewer_notes"] else "") + note
    ok = store.save_kg_link_review([rec])
    if not ok:
        st.toast("⚠️ Saved locally but the push failed — it will ride the next successful save.",
                 icon="⚠️")
    # Must come BEFORE st.rerun() — rerun raises, so anything after it is dead.
    try:
        from zalmen.activity_log import log_action
        log_action(
            "kg_link_review", "link_decision",
            target_id=row.get("surface", ""),
            decision=decision,
            note=note,
            decided_link=link,
        )
    except Exception:
        pass
    st.rerun()


def _render_person(row: dict, people: dict[str, dict], mentions: dict[str, list[dict]],
                   reviewer: str) -> None:
    surface = row["surface"]
    st.markdown(_rtl(f"<b style='font-size:1.4em'>{html.escape(surface)}</b>"),
                unsafe_allow_html=True)
    st.caption(f"{row.get('n_facts', '')} mention(s) · minted as {row.get('auto_link', '')}")

    # draft candidate(s)
    draft_id = ""
    if row.get("auto_status") == "candidate" and row.get("auto_link", "").startswith("person:"):
        draft_id = row["auto_link"].split(":", 1)[1]
    cand_ids = [draft_id] if draft_id else re.findall(r"person:(\d+)", row.get("reviewer_notes", ""))
    if cand_ids:
        st.markdown("**Draft candidate" + ("s" if len(cand_ids) > 1 else "") + " in people_db**")
        for cid in cand_ids:
            db = people.get(cid)
            st.markdown(_rtl(_db_card(db) if db else f"db {cid} (row missing)"),
                        unsafe_allow_html=True)
    else:
        st.caption("No people_db candidate found automatically.")

    # where it is mentioned
    ms = mentions.get(surface, [])
    st.markdown(f"**Mentioned in {len(ms)} entr{'y' if len(ms) == 1 else 'ies'}**")
    for i, m in enumerate(ms[:12]):
        line = f"<b>{html.escape(m.get('host_heading', ''))}</b>"
        if m.get("person_description"):
            line += f" — {html.escape(m['person_description'])}"
        st.markdown(_rtl(line), unsafe_allow_html=True)
        if m.get("relation"):
            st.markdown(_rtl(f"…{html.escape(m['relation'])}…", 0.9), unsafe_allow_html=True)
        _entry_expander(m.get("host_person_id", ""), m.get("host_heading", ""),
                        key=f"klr_{surface}_{i}")
    if len(ms) > 12:
        st.caption(f"…{len(ms) - 12} more not shown.")

    # actions
    st.markdown("---")
    c1, c2, c3, c4 = st.columns([2, 2, 1.4, 1.4])
    with c1:
        if draft_id and st.button(f"✅ Align to draft (db {draft_id})", key=f"klr_ok_{surface}",
                                  use_container_width=True, type="primary"):
            _save(row, "ALIGN", f"person:{draft_id}", reviewer, "confirmed draft")
    with c2:
        other = st.text_input("Other people_db id", key=f"klr_other_{surface}",
                              placeholder="db_id", label_visibility="collapsed")
        if st.button("↪ Align to that id", key=f"klr_otherbtn_{surface}",
                     use_container_width=True, disabled=not other.strip().isdigit()):
            oid = other.strip()
            if oid in people:
                _save(row, "ALIGN", f"person:{oid}", reviewer, "manual id")
            else:
                st.error(f"db {oid} not in people_db")
    with c3:
        if st.button("🚫 Not a person", key=f"klr_ne_{surface}", use_container_width=True):
            _save(row, "NOT_ENTITY", "", reviewer)
    with c4:
        if st.button("⏭ Keep unlinked", key=f"klr_keep_{surface}", use_container_width=True):
            _save(row, "KEEP_UNLINKED", "", reviewer)


def _render_place(row: dict, hosts: dict[str, list[tuple[str, str, str]]],
                  gaz: dict[str, dict], reviewer: str) -> None:
    surface = row["surface"]
    st.markdown(_rtl(f"<b style='font-size:1.4em'>{html.escape(surface)}</b>"),
                unsafe_allow_html=True)
    st.caption(f"{row.get('n_facts', '')} attestation(s) · minted as {row.get('auto_link', '')}"
               + (f" · {row.get('reviewer_notes', '')}" if row.get("reviewer_notes") else ""))
    hs = hosts.get(surface, [])
    st.markdown(f"**Used as birth/death/burial place in {len(hs)} entr{'y' if len(hs) == 1 else 'ies'}**")
    for i, (ctx, pid, heading) in enumerate(hs[:12]):
        st.markdown(_rtl(f"<b>{html.escape(heading)}</b> — {ctx}"), unsafe_allow_html=True)
        _entry_expander(pid, heading, key=f"klr_{surface}_{i}")
    if len(hs) > 12:
        st.caption(f"…{len(hs) - 12} more not shown.")

    st.markdown("---")
    c1, c2, c3 = st.columns([3, 1.4, 1.4])
    with c1:
        qid = st.text_input("Wikidata QID", key=f"klr_qid_{surface}", placeholder="Q12345",
                            label_visibility="collapsed").strip().upper()
        g = gaz.get(qid) if qid else None
        if g:
            st.caption(f"{g.get('label_en', '')} · {g.get('place_type', '')[:60]}")
        elif qid:
            st.caption("Not in the project gazetteer — will still be accepted; "
                       "build_kg labels it from Wikidata on the next toponym run.")
        if st.button("✅ Align to QID", key=f"klr_qok_{surface}", use_container_width=True,
                     type="primary", disabled=not re.fullmatch(r"Q\d+", qid or "")):
            _save(row, "ALIGN", f"place:{qid}", reviewer, "manual QID")
    with c2:
        if st.button("🚫 Not a place", key=f"klr_ne_{surface}", use_container_width=True):
            _save(row, "NOT_ENTITY", "", reviewer)
    with c3:
        if st.button("⏭ Keep unlinked", key=f"klr_keep_{surface}", use_container_width=True):
            _save(row, "KEEP_UNLINKED", "", reviewer)


def render() -> None:
    st.title("KG link review")
    st.caption("Names and places the lexicon layers could not link — confirm the draft, "
               "type the right id, or mark it not-an-entity. Saves go to the data branch.")
    reviewer = st.session_state.get("reviewer", "") or st.text_input("Your name", key="reviewer")
    if not reviewer:
        st.info("Enter your name to review.")
        return

    store.ensure_fresh()
    rows = list(store.load_kg_link_review(_mtime(store.KG_LINK_REVIEW)).values())
    if not rows:
        st.error("kg_link_review_layers.tsv not found — run build_kg.py --execute.")
        return
    people = _load_people_db(_mtime(PEOPLE_DB))
    mentions = _load_mentions_by_name(_mtime(MENTIONS_TSV))
    hosts = _load_place_hosts(_mtime(EXTRACTED_TSV))
    gaz = _load_gazetteer(_mtime(GAZETTEER))

    f1, f2, f3, f4 = st.columns([1.2, 1.6, 1.6, 1])
    with f1:
        slot = st.segmented_control("Kind", options=["person", "place"], default="person")
    with f2:
        status = st.segmented_control(
            "Show", options=["Undecided", "With draft", "All", "ALIGN", "NOT_ENTITY", "KEEP_UNLINKED"],
            default="Undecided")
    with f3:
        min_facts = st.selectbox("Min. mentions", [1, 2, 3, 5], index=1)
    with f4:
        name_q = st.text_input("Contains", "", label_visibility="visible").strip()

    def keep(r: dict) -> bool:
        if r.get("slot") != slot:
            return False
        d = (r.get("decision") or "").strip()
        if status == "Undecided" and d:
            return False
        if status == "With draft" and (d or r.get("auto_status") != "candidate"):
            return False
        if status in ("ALIGN", "NOT_ENTITY", "KEEP_UNLINKED") and d != status:
            return False
        if int(r.get("n_facts") or 0) < min_facts:
            return False
        if name_q and name_q not in r.get("surface", ""):
            return False
        return True

    visible = [r for r in rows if keep(r)]
    visible.sort(key=lambda r: (-(r.get("auto_status") == "candidate"), -int(r.get("n_facts") or 0),
                                r.get("surface", "")))
    total_slot = [r for r in rows if r.get("slot") == slot]
    done = sum(1 for r in total_slot if (r.get("decision") or "").strip())
    st.progress(done / max(1, len(total_slot)),
                text=f"{slot}: {done}/{len(total_slot)} decided · {len(visible)} in this list")
    if not visible:
        st.success("Nothing to review with these filters.")
        return

    labels = [f"{r['surface']}  ({r.get('n_facts', '')})" + (" ✎" if r.get("auto_status") == "candidate" else "")
              for r in visible]
    pick = st.selectbox("Surface", options=range(len(visible)), format_func=lambda i: labels[i],
                        key=f"klr_pick_{slot}")
    row = visible[pick]
    with st.container(border=True):
        if slot == "person":
            _render_person(row, people, mentions, reviewer)
        else:
            _render_place(row, hosts, gaz, reviewer)
