"""
Unified Entity Review.

Combines:
- A1 Org -> DB alignment workflow (cluster-level decisions)
- A2 cluster-pair merge workflow (pair-level decisions)

Primary queue unit: one cluster record from org_alignment_review.tsv.
"""

from __future__ import annotations

import csv
import fcntl
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

import streamlit as st

csv.field_size_limit(sys.maxsize)

BASE = pathlib.Path(__file__).parents[2]
ALIGN_FILE = BASE / "organizations" / "org_alignment_review.tsv"
PAIRS_FILE = BASE / "organizations" / "cluster_pairs_review.tsv"
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
ATTESTATION_BASE = 6

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

_TAG_DB_RE = re.compile(r"\[DB:\s*([^\]]+)\]")
_TAG_NAME_RE = re.compile(r"\[Name:\s*([^\]]+)\]")


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
def load_alignment(mtime: float) -> tuple[list[str], list[dict[str, str]]]:
	with open(ALIGN_FILE, newline="", encoding="utf-8") as f:
		r = csv.DictReader(f, delimiter="\t")
		return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_pairs(mtime: float) -> tuple[list[str], list[dict[str, str]]]:
	with open(PAIRS_FILE, newline="", encoding="utf-8") as f:
		r = csv.DictReader(f, delimiter="\t")
		return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_core_db(mtime: float) -> tuple[list[str], list[dict[str, str]]]:
	with open(CORE_DB_FILE, newline="", encoding="utf-8") as f:
		r = csv.DictReader(f, delimiter="\t")
		return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_address_db_ids(mtime: float) -> set[str]:
	if not ADDR_FILE.exists():
		return set()
	out: set[str] = set()
	with open(ADDR_FILE, newline="", encoding="utf-8") as f:
		for row in csv.DictReader(f, delimiter="\t"):
			db_id = row.get("db_id", "").strip()
			if db_id:
				out.add(db_id)
	return out


@st.cache_data(show_spinner=False)
def load_samples(mtime: float) -> dict[str, dict[str, list]]:
	idx: dict[str, dict[str, list]] = {}
	with open(CLUSTER_FILE, newline="", encoding="utf-8") as f:
		for row in csv.DictReader(f, delimiter="\t"):
			cid = row.get(_COL_CID, "").strip()
			if not cid:
				continue
			if cid not in idx:
				idx[cid] = {
					"settlements": [],
					"addresses": [],
					"venues": [],
					"countries": [],
					"samples": [],
				}
			bucket = idx[cid]
			for col, key in (
				(_COL_SETTLE, "settlements"),
				(_COL_ADDR, "addresses"),
				(_COL_VENUE, "venues"),
				(_COL_COUNTRY, "countries"),
			):
				v = row.get(col, "").strip()
				if v and v not in bucket[key]:
					bucket[key].append(v)
			sent = row.get(_COL_SENTENCE, "").strip()
			head = row.get(_COL_HEADING, "").strip()
			fle = row.get(_COL_FILE, "").strip()
			xid = row.get(_COL_XMLID, "").strip()
			if sent or head:
				bucket["samples"].append((head, sent, fle, xid))
	return idx


@st.cache_data(show_spinner=False)
def load_pair_index(mtime: float) -> dict[str, list[dict[str, str]]]:
	out: dict[str, list[dict[str, str]]] = {}
	with open(PAIRS_FILE, newline="", encoding="utf-8") as f:
		for row in csv.DictReader(f, delimiter="\t"):
			cid_i = row.get("cluster_id_i", "").strip()
			cid_j = row.get("cluster_id_j", "").strip()
			if cid_i:
				out.setdefault(cid_i, []).append(row)
			if cid_j:
				out.setdefault(cid_j, []).append(row)
	return out


def _mtime(path: pathlib.Path) -> float:
	return path.stat().st_mtime if path.exists() else 0.0


def save_alignment(headers: list[str], rows: list[dict[str, str]]) -> None:
	lock_path = ALIGN_FILE.with_suffix(".lock")
	with open(lock_path, "w") as lock_fh:
		fcntl.flock(lock_fh, fcntl.LOCK_EX)
		try:
			with open(ALIGN_FILE, "w", newline="", encoding="utf-8") as f:
				w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
				w.writeheader()
				w.writerows(rows)
		finally:
			fcntl.flock(lock_fh, fcntl.LOCK_UN)


def save_pairs(headers: list[str], rows: list[dict[str, str]]) -> None:
	lock_path = PAIRS_FILE.with_suffix(".lock")
	with open(lock_path, "w") as lock_fh:
		fcntl.flock(lock_fh, fcntl.LOCK_EX)
		try:
			with open(PAIRS_FILE, "w", newline="", encoding="utf-8") as f:
				w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
				w.writeheader()
				w.writerows(rows)
		finally:
			fcntl.flock(lock_fh, fcntl.LOCK_UN)


def save_core_db(headers: list[str], rows: list[dict[str, str]]) -> None:
	lock_path = CORE_DB_FILE.with_suffix(".lock")
	with open(lock_path, "w") as lock_fh:
		fcntl.flock(lock_fh, fcntl.LOCK_EX)
		try:
			with open(CORE_DB_FILE, "w", newline="", encoding="utf-8") as f:
				w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
				w.writeheader()
				w.writerows(rows)
		finally:
			fcntl.flock(lock_fh, fcntl.LOCK_UN)


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


def _pair_badge(decision: str) -> str:
	return {
		"MERGE": "🟢 MERGE",
		"SPLIT": "🔴 SPLIT",
		"DEFER": "🟡 DEFER",
		"DESCRIPTIVE": "🔵 DESCRIPTIVE",
		"": "⬜ undecided",
	}.get(decision, "⬜ undecided")


def _score_color(sim: float) -> str:
	if sim >= 0.90:
		return "#2ecc71"
	if sim >= 0.85:
		return "#f39c12"
	return "#e74c3c"


def _ensure_state(visible_rows: list[dict[str, str]]) -> None:
	if "review_selected_cid" not in st.session_state:
		fallback = st.session_state.get("a1_selected_cid", "")
		st.session_state.review_selected_cid = fallback if fallback in {r["cluster_id"] for r in visible_rows} else ""
	if visible_rows and st.session_state.review_selected_cid not in {r["cluster_id"] for r in visible_rows}:
		st.session_state.review_selected_cid = ""


def _next_db_id(core_rows: list[dict[str, str]]) -> int:
	vals = []
	for r in core_rows:
		v = r.get("db_id", "").strip()
		if v.isdigit():
			vals.append(int(v))
	return (max(vals) + 1) if vals else 464


def _extract_note_tag(note: str, tag: str) -> str:
	pattern = _TAG_DB_RE if tag == "DB" else _TAG_NAME_RE
	m = pattern.search(note or "")
	return m.group(1).strip() if m else ""


def _strip_note_tags(note: str) -> str:
	cleaned = _TAG_DB_RE.sub("", note or "")
	cleaned = _TAG_NAME_RE.sub("", cleaned)
	return re.sub(r"\s+", " ", cleaned).strip()


def _build_reviewer_note(base_note: str, db_ref: str = "", entity_name: str = "") -> str:
	parts = []
	if db_ref.strip():
		parts.append(f"[DB: {db_ref.strip()}]")
	if entity_name.strip():
		parts.append(f"[Name: {entity_name.strip()}]")
	if base_note.strip():
		parts.append(base_note.strip())
	return " ".join(parts).strip()


def _render_attestations(selected: dict[str, str], samples: dict[str, dict[str, list]]) -> None:
	cid = selected["cluster_id"]
	sample_rows = samples.get(cid, {}).get("samples", [])
	if not sample_rows:
		return

	show_key = f"show_all_attest_{cid}"
	show_all = st.session_state.get(show_key, False)
	limit = len(sample_rows) if show_all else ATTESTATION_BASE
	shown = sample_rows[:limit]

	st.markdown("**Context mentions**")
	for i, (head, sent, fle, xid) in enumerate(shown, start=1):
		st.markdown(f"{i}. **{head or '(no heading)'}**")
		if sent:
			st.caption(sent)
		with st.expander(f"Full entry context ({xid or 'unknown'})", expanded=False):
			if fle and xid:
				full = get_entry_text(fle, xid)
				if full:
					st.markdown(
						f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; line-height:1.6;'>{full}</div>",
						unsafe_allow_html=True,
					)
				else:
					st.caption(f"Entry not found in XML ({_JSON_TO_XML.get(fle, fle)}).")
			else:
				st.caption("Missing file/xml_id in source mention.")

	if len(sample_rows) > ATTESTATION_BASE:
		remaining = len(sample_rows) - ATTESTATION_BASE
		if not show_all:
			if st.button(f"Show more mentions ({remaining})", key=f"show-more-{cid}"):
				st.session_state[show_key] = True
				st.rerun()
		elif st.button("Show fewer mentions", key=f"show-less-{cid}"):
			st.session_state[show_key] = False
			st.rerun()


def _save_pair_decision(
	selected_cid: str,
	pair: dict[str, str],
	headers: list[str],
	rows: list[dict[str, str]],
	decision: str,
	reviewer_settlement: str,
	reviewer_address: str,
	note_text: str,
	merged_name: str,
) -> None:
	pair_id = pair.get("pair_id", "")
	row_idx = next((i for i, r in enumerate(rows) if r.get("pair_id") == pair_id), None)
	if row_idx is None:
		st.warning("Pair row not found while saving; reload and try again.")
		return

	db_ref_key = f"review_pair_db_ref_{pair_id}_{selected_cid}"
	db_ref = st.session_state.get(db_ref_key, "").strip()
	combined_note = _build_reviewer_note(
		note_text,
		db_ref=db_ref,
		entity_name=(merged_name if decision == "MERGE" else ""),
	)

	rows[row_idx]["decision"] = decision
	rows[row_idx]["reviewer_settlement"] = reviewer_settlement if decision == "MERGE" else ""
	rows[row_idx]["reviewer_address"] = reviewer_address if decision == "MERGE" else ""
	rows[row_idx]["reviewer_notes"] = combined_note
	save_pairs(headers, rows)
	load_pairs.clear()
	load_pair_index.clear()
	st.session_state.pop(db_ref_key, None)
	st.rerun()


def _render_similar_clusters(
	selected: dict[str, str],
	pair_index: dict[str, list[dict[str, str]]],
	pair_headers: list[str],
	pair_rows: list[dict[str, str]],
	align_rows: list[dict[str, str]],
) -> None:
	cid = selected.get("cluster_id", "")
	linked_pairs = sorted(
		pair_index.get(cid, []),
		key=lambda pair: float(pair.get("similarity", "0") or "0"),
		reverse=True,
	)

	if linked_pairs:
		st.markdown("**Suggested similar clusters**")
	else:
		st.caption("No pre-computed cluster pairs found.")

	for pair in linked_pairs:
		pair_id = pair.get("pair_id", "")
		cid_i = pair.get("cluster_id_i", "").strip()
		cid_j = pair.get("cluster_id_j", "").strip()
		this_is_i = (cid == cid_i)
		other_cid = cid_j if this_is_i else cid_i
		other_name = pair.get("name_j", "") if this_is_i else pair.get("name_i", "")
		this_name = pair.get("name_i", "") if this_is_i else pair.get("name_j", "")
		this_sent = pair.get("sentence_i", "") if this_is_i else pair.get("sentence_j", "")
		other_sent = pair.get("sentence_j", "") if this_is_i else pair.get("sentence_i", "")
		this_head = pair.get("heading_i", "") if this_is_i else pair.get("heading_j", "")
		other_head = pair.get("heading_j", "") if this_is_i else pair.get("heading_i", "")
		this_entry = pair.get("entry_id_i", "") if this_is_i else pair.get("entry_id_j", "")
		other_entry = pair.get("entry_id_j", "") if this_is_i else pair.get("entry_id_i", "")
		this_file = pair.get("file_i", "") if this_is_i else pair.get("file_j", "")
		other_file = pair.get("file_j", "") if this_is_i else pair.get("file_i", "")
		sim = float(pair.get("similarity", "0") or "0")
		score_color = _score_color(sim)
		decision = pair.get("decision", "").strip()
		loc_conflict = pair.get("location_conflict", "").strip()

		box = st.container(border=True)
		with box:
			st.markdown(
				f"<div><b>{pair_id}</b> · other cluster: <code>{other_cid or '—'}</code> · "
				f"similarity: <span style='color:{score_color}; font-weight:700'>{sim:.2f}</span> · "
				f"status: {_pair_badge(decision)}</div>",
				unsafe_allow_html=True,
			)
			if loc_conflict:
				st.caption(f"Location conflict flag: {loc_conflict}")

			st.markdown(f"<div dir='rtl' style='font-size:1.08em'>{other_name}</div>", unsafe_allow_html=True)

			col_a, col_b = st.columns(2)
			with col_a:
				st.caption("Current cluster mention")
				if this_head:
					st.write(this_head)
				if this_sent:
					st.caption(this_sent)
				if this_entry and this_file:
					with st.expander(f"Full text current ({this_entry})"):
						full = get_entry_text(this_file, this_entry)
						if full:
							st.markdown(
								f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; line-height:1.6;'>{full}</div>",
								unsafe_allow_html=True,
							)

			with col_b:
				st.caption("Other cluster mention")
				if other_head:
					st.write(other_head)
				if other_sent:
					st.caption(other_sent)
				if other_entry and other_file:
					with st.expander(f"Full text other ({other_entry})"):
						full = get_entry_text(other_file, other_entry)
						if full:
							st.markdown(
								f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; line-height:1.6;'>{full}</div>",
								unsafe_allow_html=True,
							)

			db_ref_key = f"review_pair_db_ref_{pair_id}_{cid}"
			current_note = pair.get("reviewer_notes", "")
			saved_name = _extract_note_tag(current_note, "Name")
			clean_note = _strip_note_tags(current_note)

			with st.expander("Search DB for clustering reference", expanded=False):
				_, db_rows = load_core_db(_mtime(CORE_DB_FILE))
				q = st.text_input("Search DB by name", key=f"pair-db-search-{pair_id}-{cid}")
				if q.strip():
					ql = q.strip().lower()
					hits = [r for r in db_rows if ql in r.get("name", "").lower()][:20]
					for hit in hits:
						hit_id = hit.get("db_id", "")
						hcol1, hcol2 = st.columns([5, 1])
						hcol1.write(f"{hit_id} · {hit.get('name', '')}")
						if hcol2.button("Select", key=f"pair-db-hit-{pair_id}-{cid}-{hit_id}"):
							st.session_state[db_ref_key] = f"{hit_id} · {hit.get('name', '')}"
							st.rerun()
				chosen_ref = st.session_state.get(db_ref_key, "")
				if chosen_ref:
					st.info(f"DB reference noted: {chosen_ref}")

			merge_settlement = st.text_input(
				"Merged settlement (optional)",
				value=pair.get("reviewer_settlement", ""),
				key=f"pair-settlement-{pair_id}-{cid}",
			)
			merge_address = st.text_input(
				"Merged address (optional)",
				value=pair.get("reviewer_address", ""),
				key=f"pair-address-{pair_id}-{cid}",
			)
			merged_name = st.text_input(
				"Merged entity name (optional)",
				value=saved_name,
				key=f"pair-name-{pair_id}-{cid}",
			)
			note_text = st.text_input(
				"Pair notes",
				value=clean_note,
				key=f"pair-note-{pair_id}-{cid}",
			)

			d1, d2, d3, d4 = st.columns(4)
			if d1.button("🟢 MERGE", key=f"pair-merge-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(
					cid,
					pair,
					pair_headers,
					pair_rows,
					"MERGE",
					merge_settlement,
					merge_address,
					note_text,
					merged_name,
				)
			if d2.button("🔴 SPLIT", key=f"pair-split-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(
					cid,
					pair,
					pair_headers,
					pair_rows,
					"SPLIT",
					"",
					"",
					note_text,
					"",
				)
			if d3.button("🟡 DEFER", key=f"pair-defer-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(
					cid,
					pair,
					pair_headers,
					pair_rows,
					"DEFER",
					"",
					"",
					note_text,
					"",
				)
			if d4.button("🔵 DESCRIPTIVE", key=f"pair-descriptive-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(
					cid,
					pair,
					pair_headers,
					pair_rows,
					"DESCRIPTIVE",
					"",
					"",
					note_text,
					"",
				)

	with st.expander("Search clusters by name", expanded=not bool(linked_pairs)):
		q = st.text_input("Search cluster names", key=f"cluster-search-{cid}",
						   placeholder="Type part of an organization name")
		if q.strip():
			ql = q.strip().lower()
			hits = [
				r for r in align_rows
				if ql in r.get("canonical_yiddish", "").lower() and r.get("cluster_id", "") != cid
			][:20]
			if hits:
				for h in hits:
					hcid = h.get("cluster_id", "")
					h_decision = h.get("decision", "").strip()
					h_type = h.get("org_type", "").strip()
					h_size = h.get("cluster_size", "").strip()
					hcol1, hcol2 = st.columns([5, 1])
					hcol1.markdown(
						f"<div class='rtl-block'>{_status(h)}  {h.get('canonical_yiddish', '')}</div>",
						unsafe_allow_html=True,
					)
					hcol1.caption(f"{hcid} · {h_type} · {h_size} mentions")
					if hcol2.button("Open", key=f"open-hit-{cid}-{hcid}"):
						st.session_state.review_selected_cid = hcid
						st.rerun()
			else:
				st.caption("No cluster matches for this query.")


def _render_rtl_style() -> None:
	st.markdown(
		"""
		<style>
		.rtl-block {
			direction: rtl;
			text-align: right;
		}
		.rtl-block p,
		.rtl-block div,
		.rtl-block label,
		.rtl-block li {
			text-align: right;
		}
		div[data-testid="stTextInput"] label,
		div[data-testid="stTextArea"] label,
		div[data-testid="stSelectbox"] label,
		div[data-testid="stMarkdownContainer"] .rtl-title {
			text-align: right;
			width: 100%;
		}
		div[data-testid="stCaptionContainer"] {
			text-align: right;
		}
		</style>
		""",
		unsafe_allow_html=True,
	)


def render() -> None:
	st.header("Entity Review")
	_render_rtl_style()

	if not ALIGN_FILE.exists():
		st.error(f"`{ALIGN_FILE}` not found. Run `python organizations/prepare_alignment.py` first.")
		return
	if not CORE_DB_FILE.exists():
		st.error(f"`{CORE_DB_FILE}` not found. Run `python organizations/build_core_db.py` first.")
		return
	if not PAIRS_FILE.exists():
		st.error(f"`{PAIRS_FILE}` not found. Run `python organizations/cluster_orgs.py` first.")
		return

	a_headers, a_rows = load_alignment(_mtime(ALIGN_FILE))
	pair_headers, pair_rows = load_pairs(_mtime(PAIRS_FILE))
	db_headers, db_rows = load_core_db(_mtime(CORE_DB_FILE))
	samples = load_samples(_mtime(CLUSTER_FILE)) if CLUSTER_FILE.exists() else {}
	pair_index = load_pair_index(_mtime(PAIRS_FILE))
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

	selected_cid = st.session_state.get("review_selected_cid", "").strip()
	selected = next((r for r in visible if r.get("cluster_id") == selected_cid), None)

	if not selected:
		st.markdown("### Review Queue")
		with st.container():
			page_count = max(1, (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE)
			page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
			start = (int(page) - 1) * PAGE_SIZE
			page_rows = visible[start : start + PAGE_SIZE]

			for r in page_rows:
				cid = r["cluster_id"]
				st.markdown(f'<div id="row-{cid}"></div>', unsafe_allow_html=True)
				linked_pairs = len(pair_index.get(cid, []))
				pair_hint = f" · {linked_pairs} pair" + ("s" if linked_pairs != 1 else "") if linked_pairs else ""
				label = f"{_status(r)}  {r.get('canonical_yiddish','')}{pair_hint}"
				if st.button(label, key=f"review-pick-{cid}", use_container_width=True, type="secondary"):
					st.session_state.review_selected_cid = cid
					st.rerun()
		return

	nav_col1, nav_col2 = st.columns([1.4, 4])
	if nav_col1.button("← Back to queue", key="review_back_to_queue"):
		st.session_state.review_selected_cid = ""
		st.rerun()
	nav_col2.markdown("### Selected Entity")

	with st.container():
		sample_rows = samples.get(selected["cluster_id"], {}).get("samples", [])
		show_samples_key = f"show_cluster_samples_{selected['cluster_id']}"
		title_col, toggle_col = st.columns([4, 1.4])
		with title_col:
			st.markdown(
				f"<div class='rtl-title' dir='rtl' style='font-size:1.55rem; font-weight:600'>{selected.get('canonical_yiddish', '')}</div>",
				unsafe_allow_html=True,
			)
		with toggle_col:
			show_samples = st.session_state.get(show_samples_key, False)
			sample_label = "Hide sample texts" if show_samples else "Click to see sample texts"
			if st.button(sample_label, key=f"toggle_cluster_samples_{selected['cluster_id']}", disabled=not sample_rows, use_container_width=True):
				st.session_state[show_samples_key] = not show_samples
				st.rerun()
		st.markdown(
			f"<div class='rtl-block'>Cluster: {selected.get('cluster_id','')} · Type: {selected.get('org_type','')} · Mentions: {selected.get('cluster_size','')}</div>",
			unsafe_allow_html=True,
		)

		left_col, right_col = st.columns([1.15, 1.85], gap="large")

		with left_col:
			if st.session_state.get(show_samples_key, False):
				with st.container(border=True):
					st.markdown("<div class='rtl-title'><b>Sample texts</b></div>", unsafe_allow_html=True)
					_render_attestations(selected, samples)

		with right_col:
			new_entity_name = st.text_input(
				"Entity name",
				value=selected.get("canonical_yiddish", "").strip(),
				key=f"review-name-{selected['cluster_id']}",
				placeholder="Editable canonical name",
			).strip()

			variants = _split_pipe(selected.get("name_variants", ""))
			if variants:
				st.markdown("<div class='rtl-title'><b>Name variants</b></div>", unsafe_allow_html=True)
				st.markdown(f"<div class='rtl-block'>{' | '.join(variants)}</div>", unsafe_allow_html=True)

			for label, key in (
				("Settlements", "extracted_settlements"),
				("Addresses", "extracted_addresses"),
				("Venues", "extracted_venues"),
				("Countries", "extracted_countries"),
			):
				val = selected.get(key, "").strip()
				if val:
					st.markdown(f"<div class='rtl-title'><b>{label}</b></div>", unsafe_allow_html=True)
					st.markdown(f"<div class='rtl-block'>{val}</div>", unsafe_allow_html=True)

			st.divider()
			cand_db_col, cand_cluster_col = st.columns(2, gap="large")

			c_ids = _split_pipe(selected.get("candidate_db_ids", ""))
			c_scores = _split_pipe(selected.get("candidate_scores", ""))
			c_methods = _split_pipe(selected.get("candidate_methods", ""))
			db_by_id = {r.get("db_id", ""): r for r in db_rows}

			choice_key = f"review_choice_{selected['cluster_id']}"
			default_choice = selected.get("aligned_db_id", "").strip()
			if choice_key not in st.session_state:
				st.session_state[choice_key] = default_choice
			chosen_db_id = st.session_state.get(choice_key, "").strip()

			if len(c_ids) == 1 and not chosen_db_id:
				st.session_state[choice_key] = c_ids[0]
				chosen_db_id = c_ids[0]

			with cand_db_col:
				st.markdown("<div class='rtl-title'><b>DB alignment candidates</b></div>", unsafe_allow_html=True)
				for i, dbid in enumerate(c_ids):
					db = db_by_id.get(dbid, {})
					score_txt = c_scores[i] if i < len(c_scores) else ""
					method_txt = c_methods[i] if i < len(c_methods) else ""
					icon = {"exact": "🎯", "phonetic": "🔊", "fuzzy": "🔤"}.get(method_txt, "•")
					with st.container(border=True):
						st.markdown(
							f"<div class='rtl-block'>{icon} {dbid} · {db.get('name', '(missing)')}</div>",
							unsafe_allow_html=True,
						)
						st.caption(f"type: {db.get('org_type','')} · score: {score_txt} · method: {method_txt}")
						if db.get("address", ""):
							st.caption(f"address: {db.get('address','')}")
						if len(c_ids) > 1 and st.button("Select for Align", key=f"review-sel-{selected['cluster_id']}-{dbid}"):
							st.session_state[choice_key] = dbid
							st.rerun()

				with st.expander("Search DB candidates", expanded=not bool(c_ids)):
					search_q = st.text_input(
						"Search DB by name",
						key=f"review-db-search-{selected['cluster_id']}",
						placeholder="Type part of an organization name",
					)
					if search_q.strip():
						q = search_q.strip().lower()
						hits = [r for r in db_rows if q in r.get("name", "").lower()][:20]
						if hits:
							for r in hits:
								hit_id = r.get("db_id", "")
								hcol1, hcol2 = st.columns([5, 1])
								hcol1.markdown(
									f"<div class='rtl-block'>{hit_id} · {r.get('name','')}</div>",
									unsafe_allow_html=True,
								)
								if hcol2.button("Use", key=f"review-manual-{selected['cluster_id']}-{hit_id}"):
									st.session_state[choice_key] = hit_id
									st.rerun()
						else:
							st.caption("No DB matches for this query.")

				chosen_db_id = st.session_state.get(choice_key, "").strip()
				if chosen_db_id:
					chosen_name = db_by_id.get(chosen_db_id, {}).get("name", "")
					st.caption(f"Selected DB target: {chosen_db_id}" + (f" · {chosen_name}" if chosen_name else ""))
					if chosen_db_id in addr_db_ids:
						if st.button("Open in Geo Cards", key=f"open-geo-{selected['cluster_id']}"):
							st.session_state["addr_selected"] = chosen_db_id
							st.session_state["nav_view_target"] = "Geo Cards"
							st.rerun()

			with cand_cluster_col:
				st.markdown("<div class='rtl-title'><b>Clustering candidates</b></div>", unsafe_allow_html=True)
				_render_similar_clusters(selected, pair_index, pair_headers, pair_rows, a_rows)

		st.divider()
		notes = st.text_area(
			"Reviewer notes",
			value=selected.get("reviewer_notes", ""),
			key=f"review-notes-{selected['cluster_id']}",
		)

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

		if col2.button("New entity"):
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
