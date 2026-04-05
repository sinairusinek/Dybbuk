"""
A1 · Org → DB Alignment view.

Reads:
- ../organizations/org_alignment_review.tsv
- ../organizations/core_db.tsv
- ../organizations/organizations_clustered.tsv (samples/context)

Writes:
- ../organizations/org_alignment_review.tsv
- ../organizations/core_db.tsv (on NEW entity)
"""

from __future__ import annotations

import csv
import fcntl
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

import streamlit as st

def _open_url(view: str, entity: str = "") -> str:
    """Build a deep-link URL for opening a specific view+entity in a new tab."""
    import urllib.parse
    params: dict[str, str] = {"view": view}
    if entity:
        params["entity"] = entity
    return "?" + urllib.parse.urlencode(params)
import streamlit.components.v1 as components

csv.field_size_limit(sys.maxsize)

BASE = pathlib.Path(__file__).parents[2]
ALIGN_FILE = BASE / "organizations" / "org_alignment_review.tsv"
CORE_DB_FILE = BASE / "organizations" / "core_db.tsv"
CLUSTER_FILE = BASE / "organizations" / "organizations_clustered.tsv"
ADDR_FILE = BASE / "organizations" / "org_addresses_review.tsv"
LEXICON_DIR = BASE / "The Lexicon"

_COL_CID = "cluster_id"
_COL_SETTLE = "_ - organizations - _ - locations - _ - settlement"
_COL_ADDR = "_ - organizations - _ - locations - _ - address"
_COL_VENUE = "_ - organizations - _ - locations - _ - Venue"
_COL_COUNTRY = "_ - organizations - _ - locations - _ - country"
_COL_SENTENCE = "_ - organizations - _ - relations - _ - original_sentence"
_COL_HEADING = "_ - heading"
_COL_FILE = "File"
_COL_XMLID = "_ - xml:id"

PAGE_SIZE = 50

_JSON_TO_XML = {
    "Volume5IIIorg.json": "Structured_Volume5III.xml",
    "Volume_3IIIorg.json": "Structured_Volume_3III.xml",
    "Volume_4IIIorg.json": "Structured_Volume_4III.xml",
    "volume6IIIorg.json": "Structured_volume6III.xml",
    "volume7IIIorg.json": "Structured_volume7III.xml",
    "volume_1IIIorg.json": "Structured_volume_1III.xml",
    "volume_2IIIorg.json": "Structured_volume_2III.xml",
}

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_ID = "{http://www.w3.org/XML/1998/namespace}id"


@st.cache_resource(show_spinner=False)
def _load_all_xml() -> dict[str, ET.ElementTree]:
    trees: dict[str, ET.ElementTree] = {}
    for json_name, xml_name in _JSON_TO_XML.items():
        xml_path = LEXICON_DIR / xml_name
        if xml_path.exists():
            try:
                trees[json_name] = ET.parse(xml_path)
            except ET.ParseError:
                pass
    return trees


def get_entry_text(json_file: str, xml_id: str) -> str | None:
    if not json_file or not xml_id:
        return None
    tree = _load_all_xml().get(json_file)
    if tree is None:
        return None
    for el in tree.getroot().iter(f"{{{TEI_NS}}}div"):
        if el.get(XML_ID) == xml_id:
            text = " ".join(" ".join(e.itertext()).strip() for e in el.iter() if e.text or e.tail)
            return re.sub(r"\s+", " ", text).strip() or None
    return None


@st.cache_data(show_spinner=False)
def load_alignment(mtime: float):
    with open(ALIGN_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_core_db(mtime: float):
    with open(CORE_DB_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_samples(mtime: float):
    idx = {}
    with open(CLUSTER_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            cid = row.get(_COL_CID, "").strip()
            if not cid:
                continue
            if cid not in idx:
                idx[cid] = {
                    "settlements": set(),
                    "addresses": set(),
                    "venues": set(),
                    "countries": set(),
                    "samples": [],
                    "_seen_xids": set(),
                }
            bucket = idx[cid]
            for col, key in ((_COL_SETTLE, "settlements"), (_COL_ADDR, "addresses"), (_COL_VENUE, "venues"), (_COL_COUNTRY, "countries")):
                v = row.get(col, "").strip()
                if v:
                    bucket[key].add(v)
            sent = row.get(_COL_SENTENCE, "").strip()
            head = row.get(_COL_HEADING, "").strip()
            fle = row.get(_COL_FILE, "").strip()
            xid = row.get(_COL_XMLID, "").strip()
            dedup_key = (fle, xid) if (fle or xid) else None
            if (sent or head) and len(bucket["samples"]) < 4 and (dedup_key is None or dedup_key not in bucket["_seen_xids"]):
                if dedup_key:
                    bucket["_seen_xids"].add(dedup_key)
                bucket["samples"].append((head, sent, fle, xid))
    return idx


@st.cache_data(show_spinner=False)
def load_address_db_ids(mtime: float) -> set[str]:
    if not ADDR_FILE.exists():
        return set()
    ids: set[str] = set()
    with open(ADDR_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            db_id = row.get("db_id", "").strip()
            if db_id:
                ids.add(db_id)
    return ids


def _mtime(path: pathlib.Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


def save_alignment(headers, rows):
    lock = ALIGN_FILE.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(ALIGN_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
                w.writeheader()
                w.writerows(rows)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def save_core_db(headers, rows):
    lock = CORE_DB_FILE.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(CORE_DB_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
                w.writeheader()
                w.writerows(rows)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def _split_pipe(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def _status(row: dict[str, str]) -> str:
    d = row.get("decision", "").strip()
    return {
        "": "⬜ undecided",
        "ALIGN": "🟢 aligned",
        "NEW": "🟣 new",
        "DISCUSS": "💬 discuss",
        "GENERIC": "🔶 generic",
        "UNCLUSTER": "🟥 uncluster",
    }.get(d, "⬜ undecided")


def _ensure_state(visible_rows: list[dict[str, str]]):
    if "a1_selected_cid" not in st.session_state:
        st.session_state.a1_selected_cid = visible_rows[0]["cluster_id"] if visible_rows else ""
    if visible_rows and st.session_state.a1_selected_cid not in {r["cluster_id"] for r in visible_rows}:
        st.session_state.a1_selected_cid = visible_rows[0]["cluster_id"]


def _next_db_id(core_rows: list[dict[str, str]]) -> int:
    vals = []
    for r in core_rows:
        v = r.get("db_id", "").strip()
        if v.isdigit():
            vals.append(int(v))
    return (max(vals) + 1) if vals else 464


def render() -> None:
    st.header("A1 · Org → DB Alignment")

    if not ALIGN_FILE.exists():
        st.error(f"`{ALIGN_FILE}` not found. Run `python organizations/prepare_alignment.py` first.")
        return
    if not CORE_DB_FILE.exists():
        st.error(f"`{CORE_DB_FILE}` not found. Run `python organizations/build_core_db.py` first.")
        return

    a_headers, a_rows = load_alignment(_mtime(ALIGN_FILE))
    db_headers, db_rows = load_core_db(_mtime(CORE_DB_FILE))
    samples = load_samples(_mtime(CLUSTER_FILE)) if CLUSTER_FILE.exists() else {}
    addr_db_ids = load_address_db_ids(_mtime(ADDR_FILE))

    total = len(a_rows)
    by_decision = {
        "": sum(1 for r in a_rows if not r.get("decision", "").strip()),
        "ALIGN": sum(1 for r in a_rows if r.get("decision", "").strip() == "ALIGN"),
        "NEW": sum(1 for r in a_rows if r.get("decision", "").strip() == "NEW"),
        "DISCUSS": sum(1 for r in a_rows if r.get("decision", "").strip() == "DISCUSS"),
        "GENERIC": sum(1 for r in a_rows if r.get("decision", "").strip() == "GENERIC"),
        "UNCLUSTER": sum(1 for r in a_rows if r.get("decision", "").strip() == "UNCLUSTER"),
    }

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Total", total)
    c2.metric("Undecided", by_decision[""])
    c3.metric("Aligned", by_decision["ALIGN"])
    c4.metric("New", by_decision["NEW"])
    c5.metric("Discuss", by_decision["DISCUSS"])
    c6.metric("Generic", by_decision["GENERIC"])
    c7.metric("Uncluster", by_decision["UNCLUSTER"])

    st.divider()

    f1, f2, f3 = st.columns([2, 1, 1])
    with f1:
        status_filter = st.segmented_control(
            "Show",
            options=["Undecided", "All", "ALIGN", "NEW", "DISCUSS", "GENERIC", "UNCLUSTER"],
            default="Undecided",
        )
    with f2:
        sort_by = st.selectbox("Sort by", ["Candidate score ↓", "Cluster size ↓", "Name"], index=0)
    with f3:
        type_filter = st.text_input("Org type contains", "").strip().lower()

    def visible_pred(r: dict[str, str]) -> bool:
        d = r.get("decision", "").strip()
        if status_filter == "Undecided" and d:
            return False
        if status_filter not in ("Undecided", "All") and d != status_filter:
            return False
        if type_filter and type_filter not in r.get("org_type", "").lower():
            return False
        return True

    visible = [r for r in a_rows if visible_pred(r)]

    def score(r: dict[str, str]) -> float:
        vals = _split_pipe(r.get("candidate_scores", ""))
        if not vals:
            return 0.0
        try:
            return float(vals[0])
        except ValueError:
            return 0.0

    if sort_by == "Candidate score ↓":
        visible.sort(key=score, reverse=True)
    elif sort_by == "Cluster size ↓":
        visible.sort(key=lambda r: int(r.get("cluster_size", "0") or "0"), reverse=True)
    else:
        visible.sort(key=lambda r: r.get("canonical_yiddish", ""))

    if not visible:
        st.success("No records in current filter.")
        return

    _ensure_state(visible)

    st.markdown("### Review Queue")
    with st.container():
        page_count = max(1, (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
        start = (int(page) - 1) * PAGE_SIZE
        page_rows = visible[start : start + PAGE_SIZE]

        for r in page_rows:
            cid = r["cluster_id"]
            selected = st.session_state.a1_selected_cid == cid
            st.markdown(f'<div id="row-{cid}"></div>', unsafe_allow_html=True)
            label = f"{_status(r)}  {r.get('canonical_yiddish','')}"
            if st.button(label, key=f"pick-{cid}", use_container_width=True, type="primary" if selected else "secondary"):
                st.session_state.a1_selected_cid = cid
                st.rerun()

        # Keep selected row visible in the middle of the list panel.
        components.html(
            f'<script>var el = parent.document.getElementById("row-{st.session_state.a1_selected_cid}"); if (el) el.scrollIntoView({{block:"center"}});</script>',
            height=0,
        )

    selected_cid = st.session_state.a1_selected_cid
    selected = next((r for r in visible if r.get("cluster_id") == selected_cid), visible[0])
    st.session_state.a1_selected_cid = selected["cluster_id"]

    st.divider()
    st.markdown("### Selected Organization")

    with st.container():
        st.subheader(selected.get("canonical_yiddish", ""))
        st.caption(f"Cluster: {selected.get('cluster_id','')} · Type: {selected.get('org_type','')} · Mentions: {selected.get('cluster_size','')}")

        new_entity_name = st.text_input(
            "New organization name",
            value=selected.get("canonical_yiddish", "").strip(),
            key=f"new-name-{selected['cluster_id']}",
            placeholder="Editable name for creating a new DB organization",
        ).strip()

        variants = _split_pipe(selected.get("name_variants", ""))
        if variants:
            st.markdown("**Variants**")
            st.write(" | ".join(variants))

        for label, key in (
            ("Settlements", "extracted_settlements"),
            ("Addresses", "extracted_addresses"),
            ("Venues", "extracted_venues"),
            ("Countries", "extracted_countries"),
        ):
            val = selected.get(key, "").strip()
            if val:
                st.markdown(f"**{label}**")
                st.write(val)

        s = samples.get(selected["cluster_id"], {})
        if s:
            st.markdown("**Attestations**")
            for i, (head, sent, fle, xid) in enumerate(s.get("samples", []), start=1):
                st.markdown(f"{i}. **{head or '(no heading)'}**")
                if sent:
                    st.caption(sent)
                label = f"Full entry context ({xid})" if xid else "Full entry context"
                with st.expander(label):
                    if fle and xid:
                        full = get_entry_text(fle, xid)
                        if full:
                            st.markdown(
                                f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; "
                                f"line-height:1.6;'>{full}</div>",
                                unsafe_allow_html=True,
                            )
                        else:
                            st.caption(f"Entry not found in XML ({_JSON_TO_XML.get(fle, fle)}).")
                    else:
                        st.caption(f"Entry ID: {xid or '—'} · file reference: {fle or '(missing)'}")

        st.divider()

        st.markdown("**DB candidates**")
        c_ids = _split_pipe(selected.get("candidate_db_ids", ""))
        c_scores = _split_pipe(selected.get("candidate_scores", ""))
        c_methods = _split_pipe(selected.get("candidate_methods", ""))

        db_by_id = {r.get("db_id", ""): r for r in db_rows}

        choice_key = f"a1_choice_{selected['cluster_id']}"
        default_choice = selected.get("aligned_db_id", "").strip()
        if choice_key not in st.session_state:
            st.session_state[choice_key] = default_choice
        chosen_db_id = st.session_state.get(choice_key, "").strip()

        if len(c_ids) == 1 and not chosen_db_id:
            st.session_state[choice_key] = c_ids[0]
            chosen_db_id = c_ids[0]

        if len(c_ids) == 1:
            st.caption("Single candidate found and pre-selected. Use Align to confirm.")

        for i, dbid in enumerate(c_ids):
            db = db_by_id.get(dbid, {})
            score_txt = c_scores[i] if i < len(c_scores) else ""
            method_txt = c_methods[i] if i < len(c_methods) else ""
            icon = {"exact": "🎯", "phonetic": "🔊", "fuzzy": "🔤"}.get(method_txt, "•")
            box = st.container(border=True)
            with box:
                st.write(f"{icon} {dbid} · {db.get('name','(missing)')}")
                st.caption(f"type: {db.get('org_type','')} · score: {score_txt} · method: {method_txt}")
                if db.get("address", ""):
                    st.caption(f"address: {db.get('address','')}")
                if len(c_ids) > 1 and st.button("Select for Align", key=f"sel-{selected['cluster_id']}-{dbid}"):
                    st.session_state[choice_key] = dbid
                    st.rerun()

        if not c_ids:
            st.info("No suggested candidates found. Search the DB manually to align this entry.")

        if not c_ids:
            st.markdown("**Manual DB search**")
        search_q = st.text_input(
            "Search DB by name",
            placeholder="Type part of an organization name",
        )
        if search_q.strip():
            q = search_q.strip().lower()
            hits = [r for r in db_rows if q in r.get("name", "").lower()][:20]
            if hits:
                st.caption("Search results")
                for r in hits:
                    hit_id = r.get("db_id", "")
                    if st.button(f"Use {hit_id} · {r.get('name','')}", key=f"manual-{selected['cluster_id']}-{hit_id}"):
                        st.session_state[choice_key] = hit_id
                        st.rerun()
            else:
                st.caption("No DB matches for this query.")

        chosen_db_id = st.session_state.get(choice_key, "").strip()
        if chosen_db_id:
            chosen_name = db_by_id.get(chosen_db_id, {}).get("name", "")
            if chosen_name:
                st.caption(f"Selected DB target: {chosen_db_id} · {chosen_name}")
            else:
                st.caption(f"Selected DB target: {chosen_db_id}")

            if chosen_db_id in addr_db_ids:
                st.link_button("Open in Organization Cards ↗",
                               _open_url("Organization Cards", chosen_db_id))

        st.divider()
        notes = st.text_area("Reviewer notes", value=selected.get("reviewer_notes", ""), key=f"notes-{selected['cluster_id']}")

        row_idx = next(i for i, r in enumerate(a_rows) if r.get("cluster_id") == selected["cluster_id"])

        col1, col2, col3, col4, col5 = st.columns(5)

        if col1.button("Align", type="primary", disabled=not chosen_db_id):
            a_rows[row_idx]["decision"] = "ALIGN"
            a_rows[row_idx]["aligned_db_id"] = chosen_db_id
            a_rows[row_idx]["reviewer_notes"] = notes
            save_alignment(a_headers, a_rows)
            load_alignment.clear()
            st.session_state.pop(choice_key, None)
            st.rerun()

        if col2.button("New organization"):
            next_id = _next_db_id(db_rows)
            db_rows.append(
                {
                    "db_id": str(next_id),
                    "name": new_entity_name or selected.get("canonical_yiddish", "").strip(),
                    "org_type": selected.get("org_type", "").strip().title(),
                    "address": selected.get("extracted_addresses", "").split("|", 1)[0].strip(),
                    "linked_cluster_ids": selected.get("cluster_id", "").strip(),
                }
            )
            save_core_db(db_headers, db_rows)
            load_core_db.clear()

            a_rows[row_idx]["decision"] = "NEW"
            a_rows[row_idx]["aligned_db_id"] = str(next_id)
            a_rows[row_idx]["reviewer_notes"] = notes
            save_alignment(a_headers, a_rows)
            load_alignment.clear()
            st.rerun()

        if col3.button("Generic"):
            a_rows[row_idx]["decision"] = "GENERIC"
            a_rows[row_idx]["aligned_db_id"] = ""
            a_rows[row_idx]["reviewer_notes"] = notes
            save_alignment(a_headers, a_rows)
            load_alignment.clear()
            st.rerun()

        if col4.button("Uncluster"):
            a_rows[row_idx]["decision"] = "UNCLUSTER"
            a_rows[row_idx]["aligned_db_id"] = ""
            a_rows[row_idx]["reviewer_notes"] = notes
            save_alignment(a_headers, a_rows)
            load_alignment.clear()
            st.rerun()

        if col5.button("Discuss"):
            a_rows[row_idx]["decision"] = "DISCUSS"
            a_rows[row_idx]["aligned_db_id"] = ""
            a_rows[row_idx]["reviewer_notes"] = notes
            save_alignment(a_headers, a_rows)
            load_alignment.clear()
            st.rerun()
