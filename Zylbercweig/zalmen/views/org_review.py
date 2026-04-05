"""
Unified Organizations matching.

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
from datetime import datetime, timezone
import xml.etree.ElementTree as ET

import streamlit as st

csv.field_size_limit(sys.maxsize)

BASE = pathlib.Path(__file__).parents[2]

# ── Shared Yiddish normalization (for vocalization-insensitive search) ───────
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)
from organizations.org_normalize import normalize_yiddish as _nrm_yid

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

_ORG_TYPE_OPTIONS = ["theatre", "troupe", "publisher", "school", ""]


def _open_url(view: str, entity: str = "") -> str:
	"""Build a deep-link URL for opening a specific view+entity in a new tab."""
	import urllib.parse
	params: dict[str, str] = {"view": view}
	if entity:
		params["entity"] = entity
	return "?" + urllib.parse.urlencode(params)

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
def load_address_details(mtime: float) -> dict[str, dict[str, str]]:
	if not ADDR_FILE.exists():
		return {}
	out: dict[str, dict[str, str]] = {}
	with open(ADDR_FILE, newline="", encoding="utf-8") as f:
		for row in csv.DictReader(f, delimiter="\t"):
			db_id = row.get("db_id", "").strip()
			if not db_id:
				continue
			out[db_id] = {
				"confirmed_settlement": row.get("confirmed_settlement", "").strip(),
				"confirmed_settlement_yiddish": row.get("confirmed_settlement_yiddish", "").strip(),
				"confirmed_address": row.get("confirmed_address", "").strip(),
				"lat": row.get("lat", "").strip(),
				"lon": row.get("lon", "").strip(),
			}
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


def _current_reviewer() -> str:
	return st.session_state.get("reviewer", "")


def _stamp(row: dict[str, str]) -> None:
	"""Stamp reviewer name and ISO timestamp on a row."""
	row["reviewer"] = _current_reviewer()
	row["reviewed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_audit_cols(headers: list[str], rows: list[dict[str, str]], *cols: str) -> None:
	"""Add audit columns to headers + rows if not already present."""
	for col in cols:
		if col not in headers:
			headers.append(col)
			for r in rows:
				r.setdefault(col, "")


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
		"SPLIT": "🔴 split",
		"DEFER": "🟡 deferred",
		"DESCRIPTIVE": "🔵 descriptive",
	}.get(d, "⬜ undecided")


def _pair_badge(decision: str) -> str:
	return {
		"MERGE": "🟢 MERGE",
		"DEFER": "🟡 DEFER",
		"DISMISS": "⬛ DISMISS",
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
	note_text: str,
) -> None:
	pair_id = pair.get("pair_id", "")
	row_idx = next((i for i, r in enumerate(rows) if r.get("pair_id") == pair_id), None)
	if row_idx is None:
		st.warning("Pair row not found while saving; reload and try again.")
		return

	_ensure_audit_cols(headers, rows, "reviewer", "reviewed_at")
	rows[row_idx]["decision"] = decision
	rows[row_idx]["reviewer_notes"] = note_text.strip()
	_stamp(rows[row_idx])
	save_pairs(headers, rows)
	load_pairs.clear()
	load_pair_index.clear()
	st.rerun()


def _merge_clusters_from_search(
	current_cid: str,
	current_row: dict[str, str],
	other_cids_and_rows: list[tuple[str, dict[str, str]]],
	pair_headers: list[str],
	pair_rows: list[dict[str, str]],
) -> None:
	"""Create/update pair records with MERGE decision for multiple clusters."""
	max_num = 0
	for p in pair_rows:
		pid = p.get("pair_id", "")
		if pid.startswith("P") and pid[1:].isdigit():
			max_num = max(max_num, int(pid[1:]))

	_ensure_audit_cols(pair_headers, pair_rows, "reviewer", "reviewed_at")
	for other_cid, other_row in other_cids_and_rows:
		# Check if a pair already exists
		found = False
		for p in pair_rows:
			ci = p.get("cluster_id_i", "").strip()
			cj = p.get("cluster_id_j", "").strip()
			if {ci, cj} == {current_cid, other_cid}:
				p["decision"] = "MERGE"
				p["reviewer_notes"] = "[merged via cluster search]"
				_stamp(p)
				found = True
				break
		if not found:
			max_num += 1
			new_pair = {h: "" for h in pair_headers}
			new_pair.update({
				"pair_id": f"P{max_num}",
				"cluster_id_i": current_cid,
				"cluster_id_j": other_cid,
				"name_i": current_row.get("canonical_yiddish", ""),
				"name_j": other_row.get("canonical_yiddish", ""),
				"org_type": current_row.get("org_type", ""),
				"similarity": "1.00",
				"decision": "MERGE",
				"reviewer_notes": "[merged via cluster search]",
				"reviewer": _current_reviewer(),
				"reviewed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
			})
			pair_rows.append(new_pair)

	save_pairs(pair_headers, pair_rows)
	load_pairs.clear()
	load_pair_index.clear()
	st.rerun()


def _render_similar_clusters(
	selected: dict[str, str],
	pair_index: dict[str, list[dict[str, str]]],
	pair_headers: list[str],
	pair_rows: list[dict[str, str]],
	align_rows: list[dict[str, str]],
) -> None:
	cid = selected.get("cluster_id", "")
	# Deduplicate by pair_id — a pair can appear twice if cid_i == cid_j or pair_id is reused
	_seen_pids: set[str] = set()
	_deduped: list[dict[str, str]] = []
	for _p in pair_index.get(cid, []):
		_pid = _p.get("pair_id", "")
		if _pid and _pid not in _seen_pids:
			_seen_pids.add(_pid)
			_deduped.append(_p)
		elif not _pid:
			_deduped.append(_p)
	all_linked_pairs = sorted(
		_deduped,
		key=lambda pair: float(pair.get("similarity", "0") or "0"),
		reverse=True,
	)
	linked_pairs = [p for p in all_linked_pairs if p.get("decision", "").strip() != "DISMISS"]
	dismissed_pairs = [p for p in all_linked_pairs if p.get("decision", "").strip() == "DISMISS"]

	if linked_pairs:
		st.markdown("**Suggested similar clusters**")
	elif not dismissed_pairs:
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

			current_note = pair.get("reviewer_notes", "")
			clean_note = _strip_note_tags(current_note)

			note_text = st.text_input(
				"Pair notes",
				value=clean_note,
				key=f"pair-note-{pair_id}-{cid}",
			)

			if decision == "MERGE":
				st.success(f"✓ Marked as MERGE")
			d1, d2, d3 = st.columns(3)
			if d1.button("🟢 MERGE", key=f"pair-merge-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(cid, pair, pair_headers, pair_rows, "MERGE", note_text)
			if d2.button("🟡 DEFER", key=f"pair-defer-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(cid, pair, pair_headers, pair_rows, "DEFER", note_text)
			if d3.button("⬛ DISMISS", key=f"pair-dismiss-{pair_id}-{cid}", use_container_width=True):
				_save_pair_decision(cid, pair, pair_headers, pair_rows, "DISMISS", note_text)

	if dismissed_pairs:
		with st.expander(f"Dismissed pairs ({len(dismissed_pairs)})", expanded=False):
			for dp in dismissed_pairs:
				dp_id = dp.get("pair_id", "")
				dp_other = dp.get("cluster_id_j", "") if dp.get("cluster_id_i", "").strip() == cid else dp.get("cluster_id_i", "")
				dp_name = dp.get("name_j", "") if dp.get("cluster_id_i", "").strip() == cid else dp.get("name_i", "")
				rc1, rc2 = st.columns([5, 1])
				rc1.caption(f"{dp_id} · {dp_other} · {dp_name}")
				if rc2.button("Restore", key=f"pair-restore-{dp_id}-{cid}", use_container_width=True):
					_save_pair_decision(cid, dp, pair_headers, pair_rows, "", "")

	with st.expander("Search clusters", expanded=not bool(linked_pairs)):
		q = st.text_input("Search cluster names", key=f"cluster-search-{cid}",
						   placeholder="Type part of an organization name")
		loc_q = st.text_input("Search clusters by location", key=f"cluster-loc-search-{cid}",
							  placeholder="Type part of a settlement, address, or venue name")
		ql = _nrm_yid(q) if q.strip() else ""
		loc_norm = _nrm_yid(loc_q) if loc_q.strip() else ""
		if ql or loc_norm:
			def _cluster_matches(r, _ql=ql, _loc=loc_norm, _cid=cid):
				if r.get("cluster_id", "") == _cid:
					return False
				name_ok = (not _ql) or _ql in _nrm_yid(r.get("canonical_yiddish", ""))
				if not _loc:
					return name_ok
				loc_fields = " ".join(filter(None, [
					_nrm_yid(r.get("extracted_settlements", "")),
					_nrm_yid(r.get("extracted_addresses", "")),
					_nrm_yid(r.get("extracted_venues", "")),
					_nrm_yid(r.get("extracted_countries", "")),
					_nrm_yid(r.get("reviewer_settlement", "")),
					_nrm_yid(r.get("reviewer_address", "")),
				]))
				return name_ok and _loc in loc_fields
			hits = [r for r in align_rows if _cluster_matches(r)][:20]
			if hits:
				for h in hits:
					hcid = h.get("cluster_id", "")
					h_type = h.get("org_type", "").strip()
					h_size = h.get("cluster_size", "").strip()
					hcol0, hcol1, hcol2 = st.columns([0.3, 4.7, 1])
					with hcol0:
						st.checkbox(
							"sel",
							key=f"merge-sel-{cid}-{hcid}",
							label_visibility="collapsed",
						)
					hcol1.markdown(
						f"<div class='rtl-block'>{_status(h)}  {h.get('canonical_yiddish', '')}</div>",
						unsafe_allow_html=True,
					)
					hcol1.caption(f"{hcid} · {h_type} · {h_size} mentions")
					hcol2.link_button("Open ↗", _open_url("Organizations matching", hcid),
									  )

				# Collect checked clusters and show merge button
				checked = [
					(h.get("cluster_id", ""), h)
					for h in hits
					if st.session_state.get(f"merge-sel-{cid}-{h.get('cluster_id', '')}")
				]
				if checked:
					names_preview = ", ".join(h.get("canonical_yiddish", "") for _, h in checked[:5])
					st.caption(f"Selected {len(checked)} cluster(s): {names_preview}")
					if st.button(f"🟢 Merge {len(checked)} selected", key=f"merge-batch-{cid}", type="primary"):
						_merge_clusters_from_search(
							cid, selected,
							[(c, r) for c, r in checked],
							pair_headers, pair_rows,
						)
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
		/* ── Organizations matching panel palette ─────────────────── */
		div[data-testid="stVerticalBlockBorderWrapper"]:has(.panel-samples),
		div[data-testid="stVerticalBlock"]:has(.panel-samples) {
			background-color: #F1E5CF;
			border-color: #D2BE97;
		}
		div[data-testid="column"]:has(.panel-db-cand),
		div[data-testid="stColumn"]:has(.panel-db-cand),
		div[data-testid="stVerticalBlock"]:has(.panel-db-cand) {
			background-color: #DCEAD4;
			border: 1px solid #AFC79F;
			border-radius: 0.5rem;
			padding: 0.5rem;
		}
		div[data-testid="column"]:has(.panel-cluster-cand),
		div[data-testid="stColumn"]:has(.panel-cluster-cand),
		div[data-testid="stVerticalBlock"]:has(.panel-cluster-cand) {
			background-color: #E3DDEA;
			border: 1px solid #BFB0D2;
			border-radius: 0.5rem;
			padding: 0.5rem;
		}
		.section-chip {
			display: inline-block;
			padding: 0.2rem 0.55rem;
			border-radius: 0.4rem;
			border: 1px solid transparent;
			margin-bottom: 0.25rem;
		}
		.section-chip-samples {
			background: #F1E5CF;
			border-color: #D2BE97;
		}
		.section-chip-db {
			background: #DCEAD4;
			border-color: #AFC79F;
		}
		.section-chip-cluster {
			background: #E3DDEA;
			border-color: #BFB0D2;
		}
		</style>
		""",
		unsafe_allow_html=True,
	)


def render() -> None:
	st.header("Organizations matching")
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
	addr_details = load_address_details(_mtime(ADDR_FILE))

	total = len(a_rows)
	by_decision: dict[str, int] = {}
	for r in a_rows:
		d = r.get("decision", "").strip()
		by_decision[d] = by_decision.get(d, 0) + 1
	undecided = by_decision.get("", 0)

	c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
	c1.metric("Total", total)
	c2.metric("Undecided", undecided)
	c3.metric("Aligned", by_decision.get("ALIGN", 0))
	c4.metric("New", by_decision.get("NEW", 0))
	c5.metric("Split", by_decision.get("SPLIT", 0))
	c6.metric("Defer", by_decision.get("DEFER", 0))
	c7.metric("Descriptive", by_decision.get("DESCRIPTIVE", 0))

	st.divider()

	f1, f2, f3 = st.columns([2, 1, 1])
	with f1:
		status_filter = st.segmented_control(
			"Show",
			options=["Undecided", "All", "ALIGN", "NEW", "SPLIT", "DEFER", "DESCRIPTIVE"],
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

	# Keep ordered list of visible cluster IDs for prev/next navigation
	st.session_state["review_visible_ids"] = [r["cluster_id"] for r in visible]

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

	vis_ids = st.session_state.get("review_visible_ids", [])
	cur_idx = vis_ids.index(selected_cid) if selected_cid in vis_ids else -1
	nav_col1, nav_col2, nav_col3, nav_col4 = st.columns([1.4, 0.5, 0.5, 3])
	if nav_col1.button("← Back to queue", key="review_back_to_queue"):
		st.session_state.review_selected_cid = ""
		st.rerun()
	if nav_col2.button("←", key="review_prev", disabled=(cur_idx <= 0)):
		st.session_state.review_selected_cid = vis_ids[cur_idx - 1]
		st.rerun()
	if nav_col3.button("→", key="review_next", disabled=(cur_idx < 0 or cur_idx >= len(vis_ids) - 1)):
		st.session_state.review_selected_cid = vis_ids[cur_idx + 1]
		st.rerun()
	nav_col4.caption(f"{cur_idx + 1} / {len(vis_ids)}" if cur_idx >= 0 else "")

	with st.container():
		sample_rows = samples.get(selected["cluster_id"], {}).get("samples", [])
		show_samples_key = f"show_cluster_samples_{selected['cluster_id']}"
		title_col, action_col, toggle_col = st.columns([3, 2.4, 1.4])
		with title_col:
			st.markdown(
				f"<div class='rtl-title' dir='rtl' style='font-size:1.55rem; font-weight:600'>{selected.get('canonical_yiddish', '')}</div>",
				unsafe_allow_html=True,
			)
		with action_col:
			qa1, qa2, qa3 = st.columns(3)
			if qa1.button("🔴 Split", key=f"entity-split-{selected['cluster_id']}", use_container_width=True):
				st.session_state[f"entity_quick_{selected['cluster_id']}"] = "SPLIT"
			if qa2.button("🟡 Defer", key=f"entity-defer-{selected['cluster_id']}", use_container_width=True):
				st.session_state[f"entity_quick_{selected['cluster_id']}"] = "DEFER"
			if qa3.button("🔵 Descriptive", key=f"entity-descriptive-{selected['cluster_id']}", use_container_width=True):
				st.session_state[f"entity_quick_{selected['cluster_id']}"] = "DESCRIPTIVE"
		with toggle_col:
			show_samples = st.session_state.get(show_samples_key, False)
			sample_label = "Hide sample texts" if show_samples else "Click to see sample texts"
			if st.button(sample_label, key=f"toggle_cluster_samples_{selected['cluster_id']}", disabled=not sample_rows, use_container_width=True):
				st.session_state[show_samples_key] = not show_samples
				st.rerun()
		st.markdown(
			f"<div class='rtl-block'>Cluster: {selected.get('cluster_id','')} · Mentions: {selected.get('cluster_size','')}</div>",
			unsafe_allow_html=True,
		)
		# org_type selectbox — inline editing
		_type_row_idx = next((i for i, r in enumerate(a_rows) if r.get("cluster_id") == selected["cluster_id"]), None)
		_cur_type = selected.get("org_type", "").strip().lower()
		_type_idx = _ORG_TYPE_OPTIONS.index(_cur_type) if _cur_type in _ORG_TYPE_OPTIONS else len(_ORG_TYPE_OPTIONS) - 1
		new_type = st.selectbox(
			"Type",
			_ORG_TYPE_OPTIONS,
			index=_type_idx,
			key=f"review-type-{selected['cluster_id']}",
		)
		if _type_row_idx is not None and new_type != _cur_type:
			a_rows[_type_row_idx]["org_type"] = new_type
			save_alignment(a_headers, a_rows)
			load_alignment.clear()
			st.rerun()

		new_entity_name = st.text_input(
			"Organization name",
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

		# ── Sample texts (optional, full-width above candidates) ──────────
		if show_samples:
			with st.container(border=True):
				st.markdown("<div class='panel-samples'></div>", unsafe_allow_html=True)
				st.markdown("<div class='rtl-title section-chip section-chip-samples'><b>Sample texts</b></div>", unsafe_allow_html=True)
				_render_attestations(selected, samples)

		st.divider()

		# ── Candidate columns: DB on left, Clustering on right ────────────
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

		cand_db_col, cand_cluster_col = st.columns(2, gap="large")

		with cand_db_col:
			st.markdown("<div class='panel-db-cand'></div>", unsafe_allow_html=True)
			st.markdown("<div class='rtl-title section-chip section-chip-db'><b>DB alignment candidates</b></div>", unsafe_allow_html=True)
			dismiss_key = f"review_dismissed_db_{selected['cluster_id']}"
			dismissed_db = st.session_state.get(dismiss_key, set())
			visible_c_ids = [(i, dbid) for i, dbid in enumerate(c_ids) if dbid not in dismissed_db]
			for i, dbid in visible_c_ids:
				db = db_by_id.get(dbid, {})
				score_txt = c_scores[i] if i < len(c_scores) else ""
				method_txt = c_methods[i] if i < len(c_methods) else ""
				icon = {
					"exact": "🎯",
					"phonetic": "🔊",
					"ipa_phonetic": "🔉",
					"fuzzy": "🔤",
				}.get(method_txt, "•")
				with st.container(border=True):
					st.markdown(
						f"<div class='rtl-block'>{icon} {dbid} · {db.get('name', '(missing)')}</div>",
						unsafe_allow_html=True,
					)
					st.caption(f"type: {db.get('org_type','')} · score: {score_txt} · method: {method_txt}")
					if db.get("address", ""):
						st.caption(f"address: {db.get('address','')}")
					loc = addr_details.get(dbid, {})
					loc_parts = []
					if loc.get("confirmed_settlement"):
						loc_parts.append(loc["confirmed_settlement"])
					if loc.get("confirmed_address"):
						loc_parts.append(loc["confirmed_address"])
					if loc.get("lat") and loc.get("lon"):
						loc_parts.append(f"({loc['lat']}, {loc['lon']})")
					if loc_parts:
						st.caption(f"📍 {' · '.join(loc_parts)}")
					is_chosen = (chosen_db_id == dbid)
					if is_chosen:
						st.success("✓ Selected for alignment")
					btn_cols = st.columns(2)
					if btn_cols[0].button("🟢 Align", key=f"review-sel-{selected['cluster_id']}-{dbid}", use_container_width=True, disabled=is_chosen):
						st.session_state[choice_key] = dbid
						st.rerun()
					if btn_cols[1].button("⬛ Dismiss", key=f"review-dismiss-db-{selected['cluster_id']}-{dbid}", use_container_width=True):
						dismissed_db.add(dbid)
						st.session_state[dismiss_key] = dismissed_db
						if chosen_db_id == dbid:
							st.session_state[choice_key] = ""
						st.rerun()

			with st.expander("Search DB candidates", expanded=not bool(c_ids)):
				search_q = st.text_input(
					"Search DB by name",
					key=f"review-db-search-{selected['cluster_id']}",
					placeholder="Type part of an organization name",
				)
				loc_q = st.text_input(
					"Search DB by location",
					key=f"review-db-loc-search-{selected['cluster_id']}",
					placeholder="Type part of a settlement, address, or venue name",
				)
				q_norm = _nrm_yid(search_q) if search_q.strip() else ""
				loc_norm = _nrm_yid(loc_q) if loc_q.strip() else ""
				if q_norm or loc_norm:
					def _db_matches(r, _q=q_norm, _loc=loc_norm):
						name_ok = (not _q) or _q in _nrm_yid(r.get("name", ""))
						if not _loc:
							return name_ok
						addr = _nrm_yid(r.get("address", ""))
						det = addr_details.get(r.get("db_id", ""), {})
						loc_fields = " ".join(filter(None, [
							addr,
							_nrm_yid(det.get("confirmed_settlement", "")),
							_nrm_yid(det.get("confirmed_settlement_yiddish", "")),
							_nrm_yid(det.get("confirmed_address", "")),
						]))
						return name_ok and _loc in loc_fields
					hits = [r for r in db_rows if _db_matches(r)][:20]
					if hits:
						for r in hits:
							hit_id = r.get("db_id", "")
							hcol1, hcol2 = st.columns([5, 1])
							hcol1.markdown(
								f"<div class='rtl-block'>{hit_id} · {r.get('name','')}</div>",
								unsafe_allow_html=True,
							)
							loc = addr_details.get(hit_id, {})
							loc_parts = []
							if loc.get("confirmed_settlement"):
								loc_parts.append(loc["confirmed_settlement"])
							if loc.get("confirmed_address"):
								loc_parts.append(loc["confirmed_address"])
							if loc_parts:
								hcol1.caption(f"📍 {' · '.join(loc_parts)}")
							if hcol2.button("Use", key=f"review-manual-{selected['cluster_id']}-{hit_id}"):
								st.session_state[choice_key] = hit_id
								st.rerun()
					else:
						st.caption("No DB matches for this query.")

			chosen_db_id = st.session_state.get(choice_key, "").strip()
			if chosen_db_id:
				chosen_name = db_by_id.get(chosen_db_id, {}).get("name", "")
				st.caption(f"Selected DB target: {chosen_db_id}" + (f" · {chosen_name}" if chosen_name else ""))
				# org_type selectbox for the chosen DB entity
				_db_row_idx = next((i for i, r in enumerate(db_rows) if r.get("db_id") == chosen_db_id), None)
				if _db_row_idx is not None:
					_db_cur_type = db_rows[_db_row_idx].get("org_type", "").strip().lower()
					_db_type_idx = _ORG_TYPE_OPTIONS.index(_db_cur_type) if _db_cur_type in _ORG_TYPE_OPTIONS else len(_ORG_TYPE_OPTIONS) - 1
					new_db_type = st.selectbox(
						"DB organization type",
						_ORG_TYPE_OPTIONS,
						index=_db_type_idx,
						key=f"review-db-type-{chosen_db_id}",
					)
					if new_db_type != _db_cur_type:
						db_rows[_db_row_idx]["org_type"] = new_db_type
						save_core_db(db_headers, db_rows)
						load_core_db.clear()
						st.rerun()
				if chosen_db_id in addr_db_ids:
					st.link_button("Open in Organization Cards ↗",
								   _open_url("Organization Cards", chosen_db_id))

		with cand_cluster_col:
			st.markdown("<div class='panel-cluster-cand'></div>", unsafe_allow_html=True)
			st.markdown("<div class='rtl-title section-chip section-chip-cluster'><b>Clustering candidates</b></div>", unsafe_allow_html=True)
			_render_similar_clusters(selected, pair_index, pair_headers, pair_rows, a_rows)

		st.divider()

		# ── Unified entity details ────────────────────────────────────────
		detail_cols = st.columns(3)
		with detail_cols[0]:
			review_settlement = st.text_input(
				"Settlement (optional)",
				value=selected.get("reviewer_settlement", ""),
				key=f"review-settlement-{selected['cluster_id']}",
			)
		with detail_cols[1]:
			review_address = st.text_input(
				"Address (optional)",
				value=selected.get("reviewer_address", ""),
				key=f"review-address-{selected['cluster_id']}",
			)
		with detail_cols[2]:
			notes = st.text_area(
				"Reviewer notes",
				value=selected.get("reviewer_notes", ""),
				key=f"review-notes-{selected['cluster_id']}",
			)

		row_idx = next(i for i, r in enumerate(a_rows) if r.get("cluster_id") == selected["cluster_id"])

		# ── Handle entity-level quick actions (Split/Defer/Descriptive from header) ──
		quick_key = f"entity_quick_{selected['cluster_id']}"
		quick_action = st.session_state.pop(quick_key, None)
		if quick_action in ("SPLIT", "DEFER", "DESCRIPTIVE"):
			_ensure_audit_cols(a_headers, a_rows, "reviewer", "reviewed_at")
			a_rows[row_idx]["decision"] = quick_action
			a_rows[row_idx]["aligned_db_id"] = ""
			a_rows[row_idx]["reviewer_notes"] = notes
			_stamp(a_rows[row_idx])
			save_alignment(a_headers, a_rows)
			load_alignment.clear()
			st.rerun()

		def _ensure_alignment_columns() -> None:
			for col in ("reviewer_settlement", "reviewer_address", "reviewer", "reviewed_at"):
				if col not in a_headers:
					a_headers.append(col)
					for r in a_rows:
						r.setdefault(col, "")

		col1, col2 = st.columns(2)

		if col1.button("Align", type="primary", disabled=not chosen_db_id):
			_ensure_alignment_columns()
			a_rows[row_idx]["decision"] = "ALIGN"
			a_rows[row_idx]["aligned_db_id"] = chosen_db_id
			a_rows[row_idx]["reviewer_notes"] = notes
			a_rows[row_idx]["reviewer_settlement"] = review_settlement
			a_rows[row_idx]["reviewer_address"] = review_address
			_stamp(a_rows[row_idx])
			save_alignment(a_headers, a_rows)
			load_alignment.clear()
			st.session_state.pop(choice_key, None)
			st.rerun()

		if col2.button("New organization"):
			_ensure_alignment_columns()
			next_id = _next_db_id(db_rows)
			db_rows.append(
				{
					"db_id": str(next_id),
					"name": new_entity_name or selected.get("canonical_yiddish", "").strip(),
						"org_type": new_type or selected.get("org_type", "").strip().lower(),
					"address": review_address or selected.get("extracted_addresses", "").split("|", 1)[0].strip(),
					"linked_cluster_ids": selected.get("cluster_id", "").strip(),
				}
			)
			save_core_db(db_headers, db_rows)
			load_core_db.clear()

			a_rows[row_idx]["decision"] = "NEW"
			a_rows[row_idx]["aligned_db_id"] = str(next_id)
			a_rows[row_idx]["reviewer_notes"] = notes
			a_rows[row_idx]["reviewer_settlement"] = review_settlement
			a_rows[row_idx]["reviewer_address"] = review_address
			_stamp(a_rows[row_idx])
			save_alignment(a_headers, a_rows)
			load_alignment.clear()
			st.rerun()
