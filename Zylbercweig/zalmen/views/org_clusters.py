"""
A2 · Org Clusters view — confirm or reject fuzzy cluster merges.

Reads:   ../organizations/cluster_pairs_review.tsv
Writes:  ../organizations/cluster_pairs_review.tsv  (decisions in-place)

Decision options:
  MERGE  — treat the two clusters as the same organization
  SPLIT  — keep them separate
  DEFER  — not sure; skip for now
"""

import csv
import fcntl
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

import streamlit as st

csv.field_size_limit(sys.maxsize)

PAIRS_FILE = (
    pathlib.Path(__file__).parents[2] / "organizations" / "cluster_pairs_review.tsv"
)
LEXICON_DIR = pathlib.Path(__file__).parents[2] / "The Lexicon"

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_ID = "{http://www.w3.org/XML/1998/namespace}id"

# JSON filename → Structured XML filename
_JSON_TO_XML = {
    "Volume5IIIorg.json": "Structured_Volume5III.xml",
    "Volume_3IIIorg.json": "Structured_Volume_3III.xml",
    "Volume_4IIIorg.json": "Structured_Volume_4III.xml",
    "volume6IIIorg.json": "Structured_volume6III.xml",
    "volume7IIIorg.json": "Structured_volume7III.xml",
    "volume_1IIIorg.json": "Structured_volume_1III.xml",
    "volume_2IIIorg.json": "Structured_volume_2III.xml",
}


# ── XML entry lookup ──────────────────────────────────────────────────────────


@st.cache_resource(show_spinner=False)
def _load_all_xml() -> dict[str, ET.ElementTree]:
    """Parse all Structured XML files once and cache them."""
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
    """Return full entry text for a given xml:id, or None if not found."""
    if not json_file or not xml_id:
        return None
    trees = _load_all_xml()
    tree = trees.get(json_file)
    if tree is None:
        return None
    root = tree.getroot()
    # Find the div with this xml:id
    for el in root.iter(f"{{{TEI_NS}}}div"):
        if el.get(XML_ID) == xml_id:
            # Collect all text content
            text = " ".join(
                " ".join(e.itertext()).strip()
                for e in el.iter()
                if e.text or e.tail
            )
            # Collapse whitespace
            text = re.sub(r"\s+", " ", text).strip()
            return text or None
    return None


# ── Data loading / saving ─────────────────────────────────────────────────────


@st.cache_data(show_spinner=False)
def load_pairs(mtime: float) -> tuple[list[str], list[dict]]:
    """Load pairs TSV. Cache key includes mtime so edits invalidate cache."""
    with open(PAIRS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames)
        rows = list(reader)
    return headers, rows


def save_pairs(headers: list[str], rows: list[dict]) -> None:
    """Write rows to TSV with an exclusive file lock (safe for concurrent RA instances)."""
    lock_path = PAIRS_FILE.with_suffix(".lock")
    with open(lock_path, "w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            with open(PAIRS_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
                writer.writeheader()
                writer.writerows(rows)
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)


def get_mtime() -> float:
    return PAIRS_FILE.stat().st_mtime if PAIRS_FILE.exists() else 0.0


# ── Helpers ───────────────────────────────────────────────────────────────────


def similarity_colour(sim: float) -> str:
    if sim >= 0.90:
        return "#2ecc71"
    if sim >= 0.85:
        return "#f39c12"
    return "#e74c3c"


def decision_badge(decision: str) -> str:
    return {"MERGE": "🟢", "SPLIT": "🔴", "DEFER": "🟡", "DESCRIPTIVE": "🔵"}.get(decision, "⬜")


# ── Main render ───────────────────────────────────────────────────────────────


def render() -> None:
    st.header("A2 · Org Cluster Review")

    if not PAIRS_FILE.exists():
        st.error(
            f"Pairs file not found: `{PAIRS_FILE}`\n\n"
            "Run `python organizations/cluster_orgs.py` first."
        )
        return

    mtime = get_mtime()
    headers, rows = load_pairs(mtime)

    if not rows:
        st.info("No pairs to review. The file is empty.")
        return

    # ── Progress summary ──────────────────────────────────────────────────────
    total = len(rows)
    decided = sum(1 for r in rows if r.get("decision", "").strip())
    merges = sum(1 for r in rows if r.get("decision", "").strip() == "MERGE")
    splits = sum(1 for r in rows if r.get("decision", "").strip() == "SPLIT")
    defers = sum(1 for r in rows if r.get("decision", "").strip() == "DEFER")

    descriptives = sum(1 for r in rows if r.get("decision", "").strip() == "DESCRIPTIVE")

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total pairs", total)
    col2.metric("Undecided", total - decided)
    col3.metric("🟢 Merge", merges)
    col4.metric("🔴 Split", splits)
    col5.metric("🟡 Defer", defers)
    col6.metric("🔵 Descriptive", descriptives)

    if total > 0:
        st.progress(decided / total, text=f"{decided}/{total} decided")

    st.divider()

    # ── Filter / navigation controls ─────────────────────────────────────────
    col_left, col_right = st.columns([2, 1])
    with col_left:
        filter_mode = st.segmented_control(
            "Show",
            options=["Undecided only", "All", "MERGE", "SPLIT", "DEFER", "DESCRIPTIVE"],
            default="Undecided only",
        )
    with col_right:
        sort_by = st.selectbox(
            "Sort by",
            ["Similarity ↓", "Similarity ↑", "Org type", "Settlement"],
            index=0,
        )

    if filter_mode == "Undecided only":
        visible = [r for r in rows if not r.get("decision", "").strip()]
    elif filter_mode == "All":
        visible = rows
    else:
        visible = [r for r in rows if r.get("decision", "").strip() == filter_mode]

    def sort_key(r):
        sim = float(r.get("similarity", 0))
        if sort_by == "Similarity ↓":
            return -sim
        if sort_by == "Similarity ↑":
            return sim
        if sort_by == "Org type":
            return r.get("org_type", "")
        return r.get("settlement", "")

    visible = sorted(visible, key=sort_key)

    if not visible:
        st.success("Nothing to show for the current filter.")
        return

    # ── Review mode ───────────────────────────────────────────────────────────
    review_mode = st.radio(
        "Review mode",
        ["Queue (one at a time)", "Table (all visible)"],
        horizontal=True,
        index=0,
    )

    if review_mode == "Queue (one at a time)":
        _render_queue(headers, rows, visible)
    else:
        _render_table(headers, rows, visible)


# ── Queue mode ────────────────────────────────────────────────────────────────


def _render_queue(headers, rows, visible):
    if "queue_pos" not in st.session_state:
        st.session_state.queue_pos = 0

    pos = st.session_state.queue_pos
    if pos >= len(visible):
        pos = 0
        st.session_state.queue_pos = 0

    nav_cols = st.columns([1, 6, 1])
    with nav_cols[0]:
        if st.button("◀ Prev", disabled=pos == 0):
            st.session_state.queue_pos -= 1
            st.rerun()
    with nav_cols[1]:
        st.caption(f"Pair {pos + 1} of {len(visible)} visible")
    with nav_cols[2]:
        if st.button("Next ▶", disabled=pos >= len(visible) - 1):
            st.session_state.queue_pos += 1
            st.rerun()

    pair = visible[pos]
    _render_pair_card(pair)

    pair_id = pair.get("pair_id", "")
    row_idx = next((i for i, r in enumerate(rows) if r.get("pair_id") == pair_id), None)

    st.divider()
    current = pair.get("decision", "").strip()
    note = pair.get("reviewer_notes", "").strip()

    dcol1, dcol2, dcol3, dcol4 = st.columns(4)
    merge_clicked = dcol1.button(
        "🟢 MERGE — same organization",
        use_container_width=True,
        type="primary" if current == "MERGE" else "secondary",
    )
    split_clicked = dcol2.button(
        "🔴 SPLIT — different organizations",
        use_container_width=True,
        type="primary" if current == "SPLIT" else "secondary",
    )
    defer_clicked = dcol3.button(
        "🟡 DEFER — not sure",
        use_container_width=True,
        type="primary" if current == "DEFER" else "secondary",
    )
    descriptive_clicked = dcol4.button(
        "🔵 DESCRIPTIVE — generic term",
        use_container_width=True,
        type="primary" if current == "DESCRIPTIVE" else "secondary",
    )

    # Location enrichment — shown when decision is MERGE (or already set to MERGE)
    new_settlement = pair.get("reviewer_settlement", "").strip()
    new_address = pair.get("reviewer_address", "").strip()
    if current == "MERGE" or merge_clicked:
        st.markdown("**Location for merged cluster** *(optional — fill if known from entries)*")
        lcol1, lcol2 = st.columns(2)
        new_settlement = lcol1.text_input(
            "Settlement (city)", value=new_settlement, key=f"settle_{pair_id}"
        )
        new_address = lcol2.text_input(
            "Address / venue", value=new_address, key=f"addr_{pair_id}"
        )

    new_note = st.text_input("Notes (optional)", value=note, key=f"note_{pair_id}")

    decision = None
    if merge_clicked:
        decision = "MERGE"
    elif split_clicked:
        decision = "SPLIT"
    elif defer_clicked:
        decision = "DEFER"
    elif descriptive_clicked:
        decision = "DESCRIPTIVE"

    if decision and row_idx is not None:
        rows[row_idx]["decision"] = decision
        rows[row_idx]["reviewer_settlement"] = new_settlement if decision == "MERGE" else ""
        rows[row_idx]["reviewer_address"] = new_address if decision == "MERGE" else ""
        rows[row_idx]["reviewer_notes"] = new_note
        save_pairs(headers, rows)
        load_pairs.clear()
        if pos < len(visible) - 1:
            st.session_state.queue_pos += 1
        st.rerun()

    if current:
        loc_info = ""
        if current == "MERGE":
            parts = [pair.get("reviewer_settlement",""), pair.get("reviewer_address","")]
            loc_parts = " · ".join(p for p in parts if p.strip())
            if loc_parts:
                loc_info = f" · 📍 {loc_parts}"
        st.info(f"Current decision: **{decision_badge(current)} {current}**{loc_info}")


# ── Table mode ────────────────────────────────────────────────────────────────


def _render_table(headers, rows, visible):
    for pair in visible:
        pair_id = pair.get("pair_id", "?")
        current = pair.get("decision", "").strip()
        sim = float(pair.get("similarity", 0))
        label = (
            f"{decision_badge(current)} **{pair_id}** · "
            f"{pair.get('name_i', '')} ↔ {pair.get('name_j', '')} "
            f"· sim={sim:.2f} · {pair.get('org_type', '')} · {pair.get('settlement', '')}"
        )
        with st.expander(label, expanded=(not current)):
            _render_pair_card(pair)
            row_idx = next(
                (i for i, r in enumerate(rows) if r.get("pair_id") == pair_id), None
            )
            note = pair.get("reviewer_notes", "").strip()
            t_settle = st.text_input(
                "Settlement (if MERGE)", value=pair.get("reviewer_settlement",""), key=f"tsettle_{pair_id}"
            )
            t_addr = st.text_input(
                "Address / venue (if MERGE)", value=pair.get("reviewer_address",""), key=f"taddr_{pair_id}"
            )
            new_note = st.text_input("Notes", value=note, key=f"tnote_{pair_id}")
            tc1, tc2, tc3, tc4 = st.columns(4)
            if tc1.button("🟢 MERGE", key=f"tm_{pair_id}") and row_idx is not None:
                rows[row_idx]["decision"] = "MERGE"
                rows[row_idx]["reviewer_settlement"] = t_settle
                rows[row_idx]["reviewer_address"] = t_addr
                rows[row_idx]["reviewer_notes"] = new_note
                save_pairs(headers, rows)
                load_pairs.clear()
                st.rerun()
            if tc2.button("🔴 SPLIT", key=f"ts_{pair_id}") and row_idx is not None:
                rows[row_idx]["decision"] = "SPLIT"
                rows[row_idx]["reviewer_settlement"] = ""
                rows[row_idx]["reviewer_address"] = ""
                rows[row_idx]["reviewer_notes"] = new_note
                save_pairs(headers, rows)
                load_pairs.clear()
                st.rerun()
            if tc3.button("🟡 DEFER", key=f"td_{pair_id}") and row_idx is not None:
                rows[row_idx]["decision"] = "DEFER"
                rows[row_idx]["reviewer_notes"] = new_note
                save_pairs(headers, rows)
                load_pairs.clear()
                st.rerun()
            if tc4.button("🔵 DESCRIPTIVE", key=f"tdes_{pair_id}") and row_idx is not None:
                rows[row_idx]["decision"] = "DESCRIPTIVE"
                rows[row_idx]["reviewer_settlement"] = ""
                rows[row_idx]["reviewer_address"] = ""
                rows[row_idx]["reviewer_notes"] = new_note
                save_pairs(headers, rows)
                load_pairs.clear()
                st.rerun()


# ── Pair card ─────────────────────────────────────────────────────────────────


def _render_pair_card(pair: dict) -> None:
    sim = float(pair.get("similarity", 0))
    colour = similarity_colour(sim)
    org_type = pair.get("org_type", "—") or "—"
    settlement = pair.get("settlement", "—") or "—"

    # ── Shared metadata (defines the matching block) ──────────────────────────
    block_settlement = f"<code>{settlement}</code>" if settlement != "—" else "<i style='color:#aaa'>none in extraction</i>"
    st.markdown(
        f"<div style='background:#f0f4f8; padding:8px 12px; border-radius:6px; margin-bottom:10px;'>"
        f"<b>Similarity:</b> <span style='color:{colour}; font-weight:bold'>{sim:.2f}</span> &nbsp;·&nbsp; "
        f"<b>Type:</b> <code>{org_type}</code> &nbsp;·&nbsp; "
        f"<b>Block settlement:</b> {block_settlement}"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Two-column name comparison ────────────────────────────────────────────
    col_a, col_b = st.columns(2)

    for col, side, name_key, id_key, file_key, heading_key, sent_key, loc_key in [
        (col_a, "A", "name_i", "entry_id_i", "file_i", "heading_i", "sentence_i", "location_i"),
        (col_b, "B", "name_j", "entry_id_j", "file_j", "heading_j", "sentence_j", "location_j"),
    ]:
        with col:
            st.markdown(f"**Name {side}**")
            name = pair.get(name_key, "")
            st.markdown(
                f"<div dir='rtl' style='font-size:1.25em; margin:4px 0;'>{name}</div>",
                unsafe_allow_html=True,
            )
            entry_id = pair.get(id_key, "")
            json_file = pair.get(file_key, "")
            heading = pair.get(heading_key, "")
            loc = pair.get(loc_key, "").strip()

            if heading:
                st.caption(f"From entry: {heading}")
            if loc:
                st.markdown(
                    f"<div style='font-size:0.85em; color:#2563eb; margin:2px 0;'>📍 {loc}</div>",
                    unsafe_allow_html=True,
                )
            if entry_id:
                st.caption(f"ID: `{entry_id}`")

            # Context sentence
            sent = pair.get(sent_key, "").strip()
            if sent:
                st.markdown(
                    f"<div dir='rtl' style='font-size:0.9em; color:#555; "
                    f"border-left:3px solid #ccc; padding-left:8px; margin-top:6px;'>"
                    f"{sent}</div>",
                    unsafe_allow_html=True,
                )

            # Full entry viewer
            if entry_id and json_file:
                with st.expander(f"📄 Full entry text ({entry_id})", expanded=False):
                    entry_text = get_entry_text(json_file, entry_id)
                    if entry_text:
                        st.markdown(
                            f"<div dir='rtl' style='font-size:0.9em; "
                            f"white-space:pre-wrap; line-height:1.6;'>{entry_text}</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.caption(
                            f"Entry not found in XML "
                            f"({_JSON_TO_XML.get(json_file, json_file)})."
                        )
