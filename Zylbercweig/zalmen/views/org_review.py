"""
Unified Organizations matching.

Combines:
- A1 Org -> DB alignment workflow (cluster-level decisions)
- A2 cluster-pair merge workflow (pair-level decisions)

Primary queue unit: one cluster record from org_alignment_review.tsv.
"""

from __future__ import annotations

import collections
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
from organizations.settlement_index import get_index as _get_settlement_index
from zalmen.activity_log import log_action

ALIGN_FILE = BASE / "organizations" / "org_alignment_review.tsv"
PAIRS_FILE = BASE / "organizations" / "cluster_pairs_review.tsv"
CORE_DB_FILE = BASE / "organizations" / "core_db.tsv"
CLUSTER_FILE = BASE / "organizations" / "organizations_clustered.tsv"
ADDR_FILE = BASE / "organizations" / "org_addresses_review.tsv"
DRAFTS_FILE = BASE / "organizations" / "org_alignment_drafts.tsv"
CORRO_FILE = BASE / "organizations" / "graph" / "alignment_corroboration.tsv"
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
BATCH_PAGE_SIZE = 25
ATTESTATION_BASE = 6

_ORG_TYPE_OPTIONS = [
	"Theatre",
	"Non-Yiddish Theatre",
	"Traveling Company",
	"Company on Tour",
	"Amateur",
	"Kleinkunst",
	"Circus",
	"Theatre education",
	"Publisher",
	"Printer",
	"Printer/Publisher",
	"Journals/ Newspapers",
	"Media (Radio/ Film/TV)",
	"Library",
	"Heritage Institution",
	"Education",
	"Musical organization",
	"Theatre-related Society/ Union",
	"Religious institutions/organizations",
	"Jewish political bodies",
	"Non-Jewish political bodies",
	"Welfare/Aid organization",
	"Trade Union / Professional Association",
	"Business",
	"Labour (factory/workshop)",
	"Health institutions",
	"Military",
	"Judenrat",
	"Sports/Recreation",
	"Fraternal order",
	"Not an organization",
	"OTHER - elaborate!",
	"",
]


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
_YID_CHAR_RE = re.compile(r"[֐-׿יִ-ﭏ]")


def _has_yiddish(s: str) -> bool:
	return bool(_YID_CHAR_RE.search(s or ""))


# XML entry-text lookup now lives in the shared, lazy, per-volume loader
# (zalmen/lexicon.py) — see the note there. This view previously kept its own
# eager all-volumes parse pinned via cache_resource, which (times four views)
# risked OOM on Streamlit Cloud and silently dropped whole volumes.
from zalmen.lexicon import get_entry_text  # noqa: E402,F401


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
					"_seen_xids": set(),
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
			dedup_key = (fle, xid) if (fle or xid) else None
			if (sent or head) and (dedup_key is None or dedup_key not in bucket["_seen_xids"]):
				if dedup_key:
					bucket["_seen_xids"].add(dedup_key)
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


@st.cache_data(show_spinner=False)
def load_drafts(mtime: float) -> dict[str, dict[str, str]]:
	if not DRAFTS_FILE.exists():
		return {}
	out: dict[str, dict[str, str]] = {}
	with open(DRAFTS_FILE, newline="", encoding="utf-8") as f:
		for row in csv.DictReader(f, delimiter="\t"):
			cid = row.get("cluster_id", "").strip()
			if cid:
				out[cid] = row
	return out


@st.cache_data(show_spinner=False)
def load_corroborations(mtime: float) -> dict[tuple[str, str], dict[str, str]]:
	"""(cluster_id, candidate_db_id) -> person-org graph corroboration row.

	Built by organizations/build_person_org_graph.py: shared host biographies
	between an undecided cluster and a candidate entity's linked clusters.
	"""
	if not CORRO_FILE.exists():
		return {}
	out: dict[tuple[str, str], dict[str, str]] = {}
	with open(CORRO_FILE, newline="", encoding="utf-8") as f:
		for row in csv.DictReader(f, delimiter="\t"):
			cid = row.get("cluster_id", "").strip()
			dbid = row.get("candidate_db_id", "").strip()
			if cid and dbid:
				out[(cid, dbid)] = row
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
	from zalmen.github_sync import push_file_to_github
	ok = push_file_to_github("Zylbercweig/organizations/org_alignment_review.tsv", ALIGN_FILE, "chore: save alignment decisions")
	if not ok:
		st.toast("⚠️ Your decision was recorded but could not be saved permanently. Please contact Sinai before continuing.", icon="⚠️")


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
	from zalmen.github_sync import push_file_to_github
	ok = push_file_to_github("Zylbercweig/organizations/cluster_pairs_review.tsv", PAIRS_FILE, "chore: save cluster pair decisions")
	if not ok:
		st.toast("⚠️ Your decision was recorded but could not be saved permanently. Please contact Sinai before continuing.", icon="⚠️")


CORE_DB_CANONICAL_HEADERS = [
	"db_id", "name", "name_yiddish", "name_yiddish_translit",
	"org_type", "address", "linked_cluster_ids", "parent_db_id",
	# 2026-05-31: dedup-policy fields. `deprecated="true"` marks a row that has
	# been merged into `merged_into`; deprecated rows are hidden from candidate
	# dropdowns and skipped by prepare_alignment / detect_data_defects.
	"deprecated", "merged_into",
	# 2026-06-01: Q10 — pipe-separated alternate attested spellings (Yiddish
	# orthographic/declension variants of the same entity) that aren't the
	# canonical `name` or `name_yiddish`. Initially empty; populated case by
	# case as PI/RAs reconcile variant pairs.
	"name_variants",
	# 2026-06-02: rows that exist in the source data but aren't part of the
	# Zylbercweig corpus's scope (modern Israeli publishers, peripheral
	# entities). Hidden from candidate dropdowns alongside `deprecated`.
	"out_of_project",
]


def is_deprecated_db_row(row: dict[str, str]) -> bool:
	"""True if this core_db row has been marked dedup-deprecated (the
	`merged_into` column points at the canonical id)."""
	return (row.get("deprecated", "") or "").strip().lower() == "true"


def is_out_of_project_db_row(row: dict[str, str]) -> bool:
	"""True if this core_db row is out of the Zylbercweig project scope
	(e.g. modern Israeli publishers). Same hiding behavior as deprecated, but
	a distinct disposition — not merged into anything."""
	return (row.get("out_of_project", "") or "").strip().lower() == "true"


def active_db_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
	"""Filter out deprecated AND out-of-project rows for use in candidate
	dropdowns / search hits. Use this anywhere a user picks a DB target. Don't
	use it for save_core_db or for resolving an existing alignment's name —
	the rows must still exist on disk."""
	return [r for r in rows
	        if not is_deprecated_db_row(r) and not is_out_of_project_db_row(r)]


def _ensure_core_db_schema(headers, rows):
	"""Defensive guard: a long-running Streamlit Cloud instance may have cached
	headers from a pre-schema-change boot. Always emit the canonical column
	set so pipeline-added columns (e.g. name_yiddish_translit) don't get
	silently dropped by the next save."""
	out_headers = list(CORE_DB_CANONICAL_HEADERS)
	for h in headers:
		if h not in out_headers:
			out_headers.append(h)
	for r in rows:
		for h in out_headers:
			r.setdefault(h, "")
	return out_headers, rows


def save_core_db(headers: list[str], rows: list[dict[str, str]]) -> None:
	headers, rows = _ensure_core_db_schema(headers, rows)
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
	from zalmen.github_sync import push_file_to_github
	ok = push_file_to_github("Zylbercweig/organizations/core_db.tsv", CORE_DB_FILE, "chore: save core DB")
	if not ok:
		st.toast("⚠️ Your decision was recorded but could not be saved permanently. Please contact Sinai before continuing.", icon="⚠️")


def append_address_row(db_id: str, name: str, org_type: str, cluster_id: str,
                       settlement: str = "", address: str = "") -> None:
	"""Append a row for a newly-created DB org to org_addresses_review.tsv.

	The address TSV is normally regenerated offline by extract_addresses.py,
	but we append live so cards appear immediately for orgs created in the app.
	"""
	if not ADDR_FILE.exists():
		return
	lock_path = ADDR_FILE.with_suffix(".lock")
	with open(lock_path, "w") as lock_fh:
		fcntl.flock(lock_fh, fcntl.LOCK_EX)
		try:
			with open(ADDR_FILE, newline="", encoding="utf-8") as f:
				reader = csv.DictReader(f, delimiter="\t")
				headers = list(reader.fieldnames or [])
				rows = list(reader)
			if not headers or "db_id" not in headers:
				return
			if any(r.get("db_id", "").strip() == str(db_id) for r in rows):
				return
			new_row = {h: "" for h in headers}
			new_row["db_id"] = str(db_id)
			new_row["canonical_yiddish"] = name
			new_row["org_type"] = org_type
			new_row["linked_cluster_ids"] = cluster_id
			new_row["mentions"] = "0"
			new_row["n_settlements"] = "0"
			if "confirmed_settlement" in headers:
				new_row["confirmed_settlement"] = settlement
			if "confirmed_address" in headers:
				new_row["confirmed_address"] = address
			if "reviewer" in headers:
				new_row["reviewer"] = _current_reviewer()
			if "reviewed_at" in headers:
				new_row["reviewed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
			rows.append(new_row)
			with open(ADDR_FILE, "w", newline="", encoding="utf-8") as f:
				w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
				w.writeheader()
				w.writerows(rows)
		finally:
			fcntl.flock(lock_fh, fcntl.LOCK_UN)
	from zalmen.github_sync import push_file_to_github
	push_file_to_github(
		"Zylbercweig/organizations/org_addresses_review.tsv",
		ADDR_FILE,
		f"chore: append new org {db_id} to addresses",
	)
	load_address_db_ids.clear()
	load_address_details.clear()


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


_ITINERANT_TYPES = {
	"troupe", "טרופּע", "טעאַטער-טרופּע",
	"travelling company", "traveling company", "company on tour",
	"army", "ארמיי", "אַרמיי", "אַרמעע",
	"military", "expedition",
}


def _place_hint(org_type: str, settlements: str) -> str:
	"""Pipe-joined settlements suffix for the queue label.

	Empty for itinerant org_types (troupes/army/etc.) — for those the city
	doesn't disambiguate the entity.
	"""
	s = (settlements or "").strip()
	if not s:
		return ""
	if (org_type or "").strip().lower() in _ITINERANT_TYPES:
		return ""
	return f" — {s}"


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


def render_attestations(selected: dict[str, str], samples: dict[str, dict[str, list]]) -> None:
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
	log_action(
		"org_review", "pair_decision",
		target_id=pair_id, decision=decision, note=note_text.strip(),
		cluster_i=rows[row_idx].get("cluster_id_i", ""),
		cluster_j=rows[row_idx].get("cluster_id_j", ""),
		selected_cid=selected_cid,
	)
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
	merged_cids: list[str] = []
	for other_cid, other_row in other_cids_and_rows:
		merged_cids.append(other_cid)
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
	log_action(
		"org_review", "merge_via_search",
		target_id=current_cid, decision="MERGE",
		note=f"merged with {len(merged_cids)} cluster(s)",
		merged_with=merged_cids,
	)
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
	_align_by_cid = {a.get("cluster_id", ""): a for a in align_rows}

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

			_other_a = _align_by_cid.get(other_cid, {})
			_other_ph = _place_hint(_other_a.get("org_type", ""), _other_a.get("extracted_settlements", ""))
			st.markdown(f"<div dir='rtl' style='font-size:1.08em'>{other_name}{_other_ph}</div>", unsafe_allow_html=True)

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

			if other_cid:
				_render_audit_jump_buttons(
					buckets=_cluster_buckets(other_cid),
					button_key_prefix=f"audit-from-pair-{pair_id}-{cid}-{other_cid}",
				)

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
						f"<div class='rtl-block'>{_status(h)}  {h.get('canonical_yiddish', '')}"
						f"{_place_hint(h.get('org_type',''), h.get('extracted_settlements',''))}</div>",
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
					names_preview = ", ".join(
						f"{h.get('canonical_yiddish', '')}{_place_hint(h.get('org_type',''), h.get('extracted_settlements',''))}"
						for _, h in checked[:5]
					)
					st.caption(f"Selected {len(checked)} cluster(s): {names_preview}")
					if st.button(f"🟢 Merge {len(checked)} selected", key=f"merge-batch-{cid}", type="primary"):
						_merge_clusters_from_search(
							cid, selected,
							[(c, r) for c, r in checked],
							pair_headers, pair_rows,
						)
			else:
				st.caption("No cluster matches for this query.")


_DRAFT_DECISIONS = ("ALIGN", "NEW", "GENERIC", "SPLIT", "DEFER", "DESCRIPTIVE", "DISCUSS")


def _render_batch_confirm(
	a_headers: list[str],
	a_rows: list[dict[str, str]],
	db_headers: list[str],
	db_rows: list[dict[str, str]],
	drafts_by_cid: dict[str, dict[str, str]],
) -> None:
	"""Batch-confirm panel: paginated grid of high-confidence drafter proposals."""
	db_by_id = {r.get("db_id", ""): r for r in db_rows}

	candidates: list[tuple[dict[str, str], dict[str, str]]] = []
	for r in a_rows:
		if r.get("decision", "").strip():
			continue
		d = drafts_by_cid.get(r.get("cluster_id", "").strip())
		if not d:
			continue
		if d.get("confidence", "").strip().lower() != "high":
			continue
		if d.get("draft_decision", "").strip() not in _DRAFT_DECISIONS:
			continue
		candidates.append((r, d))

	st.markdown("### Batch confirm — high-confidence drafts")
	if not candidates:
		st.info("No high-confidence drafts available for undecided clusters. Run the drafter or switch to Single cluster mode.")
		return

	# Decision-type filter
	type_counts: dict[str, int] = collections.Counter()
	for _r, _d in candidates:
		type_counts[_d.get("draft_decision", "").strip()] += 1
	type_opts = sorted(type_counts.keys(), key=lambda t: (-type_counts[t], t))
	sel_decisions = st.pills(
		"Filter by proposed decision",
		options=type_opts,
		format_func=lambda t: f"{t} ({type_counts[t]})",
		selection_mode="multi",
		default=type_opts,
		key="batch_decision_pills",
	)
	sel_decisions_set = set(sel_decisions or type_opts)
	filtered = [(r, d) for r, d in candidates if d.get("draft_decision", "").strip() in sel_decisions_set]

	st.caption(f"{len(filtered)} candidate(s) match filter · {len(candidates)} total high-confidence drafts")
	if not filtered:
		return

	page_count = max(1, (len(filtered) + BATCH_PAGE_SIZE - 1) // BATCH_PAGE_SIZE)
	page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1, key="batch_page")
	start = (int(page) - 1) * BATCH_PAGE_SIZE
	page_items = filtered[start : start + BATCH_PAGE_SIZE]

	with st.form("batch_confirm_form", clear_on_submit=False):
		header = st.columns([0.4, 2.2, 0.9, 0.9, 1.8, 2.4, 0.6])
		header[0].caption("✓")
		header[1].caption("Cluster name")
		header[2].caption("Type")
		header[3].caption("Decision")
		header[4].caption("DB target")
		header[5].caption("Rationale")
		header[6].caption("Open")

		corro_by_key = load_corroborations(_mtime(CORRO_FILE))
		for r, d in page_items:
			cid = r.get("cluster_id", "").strip()
			decision = d.get("draft_decision", "").strip()
			db_id = d.get("draft_aligned_db_id", "").strip()
			db_name = db_by_id.get(db_id, {}).get("name", "") if db_id else ""
			rationale = d.get("rationale", "").strip()
			_corro = corro_by_key.get((cid, db_id)) if db_id else None
			row_cols = st.columns([0.4, 2.2, 0.9, 0.9, 1.8, 2.4, 0.6])
			row_cols[0].checkbox(
				"accept",
				value=True,
				key=f"batch-accept-{cid}",
				label_visibility="collapsed",
			)
			row_cols[1].markdown(
				f"<div class='rtl-block'>{r.get('canonical_yiddish','')}"
				f"{_place_hint(r.get('org_type',''), r.get('extracted_settlements',''))}</div>"
				f"<div style='font-size:0.8em;color:#666'>{cid}</div>",
				unsafe_allow_html=True,
			)
			row_cols[2].caption(r.get("org_type", ""))
			badge = {"ALIGN": "🟢", "NEW": "🟣", "GENERIC": "🔶", "SPLIT": "🔴",
			         "DEFER": "🟡", "DESCRIPTIVE": "🔵", "DISCUSS": "💬"}.get(decision, "·")
			row_cols[3].markdown(f"{badge} **{decision}**")
			if db_id:
				_c_badge = f" · 🕸{_corro.get('shared_hosts','')}" if _corro else ""
				row_cols[4].markdown(f"`{db_id}` {db_name}{_c_badge}")
				if _corro:
					row_cols[4].caption(f"shared biographies: {_corro.get('shared_host_headings','')[:120]}")
			else:
				row_cols[4].caption("—")
			row_cols[5].caption(rationale[:240] + ("…" if len(rationale) > 240 else ""))
			row_cols[6].markdown(f"[↗]({_open_url('Organizations matching', cid)})")

		submitted = st.form_submit_button(
			f"💾 Save accepted on this page",
			type="primary",
		)

	if submitted:
		_apply_batch_accepts(page_items, a_headers, a_rows, db_headers, db_rows)


def _apply_batch_accepts(
	page_items: list[tuple[dict[str, str], dict[str, str]]],
	a_headers: list[str],
	a_rows: list[dict[str, str]],
	db_headers: list[str],
	db_rows: list[dict[str, str]],
) -> None:
	_ensure_audit_cols(a_headers, a_rows, "reviewer", "reviewed_at",
	                   "reviewer_settlement", "reviewer_address")
	idx_by_cid = {r.get("cluster_id", "").strip(): i for i, r in enumerate(a_rows)}
	next_id = _next_db_id(db_rows)

	accepted_aligns = 0
	accepted_news = 0
	accepted_other = 0
	new_core_rows: list[dict[str, str]] = []

	for r, d in page_items:
		cid = r.get("cluster_id", "").strip()
		if not st.session_state.get(f"batch-accept-{cid}", False):
			continue
		row_idx = idx_by_cid.get(cid)
		if row_idx is None:
			continue
		decision = d.get("draft_decision", "").strip()
		db_id = d.get("draft_aligned_db_id", "").strip()
		row = a_rows[row_idx]

		if decision == "ALIGN" and db_id:
			row["decision"] = "ALIGN"
			row["aligned_db_id"] = db_id
			accepted_aligns += 1
		elif decision == "NEW":
			row["decision"] = "NEW"
			row["aligned_db_id"] = str(next_id)
			_cy = r.get("canonical_yiddish", "").strip()
			new_core_rows.append({
				"db_id": str(next_id),
				"name": _cy,
				"name_yiddish": _cy if _has_yiddish(_cy) else "",
				"org_type": r.get("org_type", "").strip().lower(),
				"address": (r.get("extracted_addresses", "").split("|", 1)[0] or "").strip(),
				"linked_cluster_ids": cid,
			})
			next_id += 1
			accepted_news += 1
		else:
			row["decision"] = decision
			row["aligned_db_id"] = ""
			accepted_other += 1

		row["reviewer_notes"] = (d.get("rationale", "").strip()[:500])
		_stamp(row)
		log_action(
			"org_review", "alignment_batch_accept",
			target_id=cid, decision=row["decision"],
			note=row["reviewer_notes"],
			aligned_db_id=row.get("aligned_db_id", ""),
		)

	total = accepted_aligns + accepted_news + accepted_other
	if total == 0:
		st.warning("Nothing checked on this page.")
		return

	# Append any new core_db rows (pad missing columns)
	if new_core_rows:
		for new_row in new_core_rows:
			padded = {h: "" for h in db_headers}
			padded.update({k: v for k, v in new_row.items() if k in db_headers})
			db_rows.append(padded)
		save_core_db(db_headers, db_rows)
		load_core_db.clear()

	save_alignment(a_headers, a_rows)
	load_alignment.clear()

	st.success(
		f"Saved {total} decision(s): {accepted_aligns} ALIGN · {accepted_news} NEW · {accepted_other} other. "
		"NEW entries got auto-allocated db_ids; add addresses in Single cluster mode."
	)
	st.rerun()


def _db_buckets(db_id: str):
	try:
		return _get_settlement_index().siblings_for_db(db_id)
	except Exception:
		return []


def _cluster_buckets(cluster_id: str):
	try:
		return _get_settlement_index().siblings_for_cluster(cluster_id)
	except Exception:
		return []


def _render_audit_jump_buttons(buckets, button_key_prefix: str) -> None:
	"""Render a tight row of '🌆 City · Type' buttons that nav to Settlement audit."""
	if not buckets:
		return
	for b in buckets:
		city_label = b.english or b.yiddish or b.qid
		if st.button(
			f"🌆 {city_label} · {b.org_type} ({len(b.db_cards)} DB / {len(b.clusters)} cl)",
			key=f"{button_key_prefix}-{b.qid}-{b.org_type}",
			use_container_width=True,
		):
			st.session_state["audit_target_qid"] = b.qid
			st.session_state["audit_target_type"] = b.org_type
			st.session_state["nav_view_target"] = "Settlement audit"
			st.rerun()


def _render_settlement_siblings(
	selected: dict[str, str],
	choice_key: str,
	addr_db_ids: set[str],
	a_rows: list[dict[str, str]],
	pair_headers: list[str],
	pair_rows: list[dict[str, str]],
) -> None:
	"""Show same-type, same-settlement DB rows + clusters as alignment/merge candidates.

	Itinerant types are excluded by the index. A cluster can sit in multiple
	settlements — render one expander per (settlement, type) bucket.
	"""
	cid = selected.get("cluster_id", "")
	try:
		ix = _get_settlement_index()
	except Exception as exc:  # noqa: BLE001
		st.caption(f"Siblings index unavailable: {exc}")
		return
	buckets = ix.siblings_for_cluster(cid)
	if not buckets:
		return
	chosen_db_id = st.session_state.get(choice_key, "").strip()
	rows_by_cid = {r.get("cluster_id", ""): r for r in a_rows}
	for bucket in buckets:
		other_dbs = [d for d in bucket.db_cards]
		other_clusters = [c for c in bucket.clusters if c.cluster_id != cid]
		if not other_dbs and not other_clusters:
			continue
		city_label = bucket.english or bucket.yiddish or bucket.qid
		header = f"🌆 Siblings in {city_label} · {bucket.org_type} · {len(other_dbs)} DB · {len(other_clusters)} clusters"
		with st.expander(header, expanded=False):
			if st.button(
				f"Open full audit: {city_label} · {bucket.org_type} ↗",
				key=f"sib-open-audit-{cid}-{bucket.qid}-{bucket.org_type}",
				use_container_width=True,
			):
				st.session_state["audit_target_qid"] = bucket.qid
				st.session_state["audit_target_type"] = bucket.org_type
				st.session_state["nav_view_target"] = "Settlement audit"
				st.rerun()
			if other_dbs:
				st.markdown("**DB rows here**")
				for d in other_dbs:
					name = d.name or d.name_yiddish or "(unnamed)"
					st.markdown(
						f"<div class='rtl-block'>{d.db_id} · {name}"
						+ (f" · <span dir='rtl'>{d.name_yiddish}</span>" if d.name_yiddish and d.name else "")
						+ "</div>",
						unsafe_allow_html=True,
					)
					if d.confirmed_settlement:
						st.caption(f"📍 {d.confirmed_settlement}")
					is_chosen = (chosen_db_id == d.db_id)
					btn_cols = st.columns(2)
					if btn_cols[0].button(
						"✓ chosen" if is_chosen else "🟢 Align",
						key=f"sib-db-{cid}-{bucket.qid}-{bucket.org_type}-{d.db_id}",
						disabled=is_chosen,
						use_container_width=True,
					):
						st.session_state[choice_key] = d.db_id
						st.rerun()
					if d.db_id in addr_db_ids:
						btn_cols[1].link_button(
							"Open details ↗",
							_open_url("Organization Cards", d.db_id),
							use_container_width=True,
						)
					else:
						btn_cols[1].caption("(no address row)")
			if other_clusters:
				st.markdown("**Other clusters here** (potential merges / shared DB target)")
				# Sort: undecided first, then by size desc
				other_clusters.sort(key=lambda c: (bool(c.decision), -c.cluster_size))
				for c in other_clusters[:25]:
					_ph = _place_hint(c.org_type, c.settlement_raw)
					line = f"{c.cluster_id} · {c.canonical_yiddish or '(no canonical)'}{_ph} · n={c.cluster_size}"
					if c.decision:
						line += f" · {c.decision}"
						if c.aligned_db_id:
							line += f" → {c.aligned_db_id}"
					st.markdown(f"<div class='rtl-block'>{line}</div>", unsafe_allow_html=True)
					btn_cols = st.columns(2)
					if btn_cols[0].button(
						"Open ↗",
						key=f"sib-cl-open-{cid}-{bucket.qid}-{bucket.org_type}-{c.cluster_id}",
						use_container_width=True,
					):
						st.session_state["review_selected_cid"] = c.cluster_id
						st.rerun()
					other_row = rows_by_cid.get(c.cluster_id)
					if btn_cols[1].button(
						"🔗 Merge",
						key=f"sib-cl-merge-{cid}-{bucket.qid}-{bucket.org_type}-{c.cluster_id}",
						disabled=(other_row is None),
						use_container_width=True,
					):
						_merge_clusters_from_search(
							cid, selected,
							[(c.cluster_id, other_row)],
							pair_headers, pair_rows,
						)
				if len(other_clusters) > 25:
					st.caption(f"… and {len(other_clusters) - 25} more")


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
	drafts_by_cid = load_drafts(_mtime(DRAFTS_FILE))
	corro_by_key = load_corroborations(_mtime(CORRO_FILE))

	with st.sidebar:
		mode_options = ["Single cluster", "Batch confirm"]
		mode = st.radio(
			"Review mode",
			options=mode_options,
			index=0,
			key="org_review_mode",
			help="Batch mode shows a paginated grid of high-confidence drafter proposals you can accept en masse.",
		)
		if drafts_by_cid:
			st.caption(f"📝 {len(drafts_by_cid)} drafter proposals available")
		else:
			st.caption("No drafter proposals yet.")

	if mode == "Batch confirm":
		_render_batch_confirm(a_headers, a_rows, db_headers, db_rows, drafts_by_cid)
		return

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

	with open(ALIGN_FILE, "rb") as _f:
		st.download_button(
			"⬇ Download decisions TSV (commit to git to persist)",
			data=_f.read(),
			file_name="org_alignment_review.tsv",
			mime="text/tab-separated-values",
			help="Streamlit Cloud resets files on redeploy. Download & commit this file to preserve decisions.",
		)

	st.divider()

	f1, f2 = st.columns([2, 1])
	with f1:
		status_filter = st.segmented_control(
			"Show",
			options=["Undecided", "All", "ALIGN", "NEW", "SPLIT", "DEFER", "DESCRIPTIVE"],
			default="Undecided",
		)
	with f2:
		sort_by = st.selectbox("Sort by", ["Candidate score ↓", "Cluster size ↓", "Name"], index=0)

	type_counts: dict[str, int] = collections.Counter()
	for r in a_rows:
		t = r.get("org_type", "").strip()
		if t:
			type_counts[t.title()] += 1
	type_options = [t for t, _ in sorted(type_counts.items(), key=lambda x: (-x[1], x[0]))]
	type_labels = {t: f"{t} ({type_counts[t]})" for t in type_options}

	st.caption("Filter by org type")
	sel_types = st.pills(
		"Org type",
		options=type_options,
		format_func=lambda t: type_labels[t],
		selection_mode="multi",
		key="review_type_pills",
		label_visibility="collapsed",
	)
	sel_types_norm = {t.lower() for t in sel_types}

	def visible_pred(r: dict[str, str]) -> bool:
		d = r.get("decision", "").strip()
		if status_filter == "Undecided" and d:
			return False
		if status_filter not in ("Undecided", "All") and d != status_filter:
			return False
		if sel_types_norm and r.get("org_type", "").strip().lower() not in sel_types_norm:
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
				place_hint = _place_hint(r.get("org_type", ""), r.get("extracted_settlements", ""))
				label = f"{_status(r)}  {r.get('canonical_yiddish','')}{place_hint}{pair_hint}"
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
		_type_options = list(_ORG_TYPE_OPTIONS)
		if _cur_type and _cur_type not in _type_options:
			_type_options.insert(0, _cur_type)
		_type_idx = _type_options.index(_cur_type) if _cur_type in _type_options else len(_type_options) - 1
		new_type = st.selectbox(
			"Type",
			_type_options,
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
				render_attestations(selected, samples)

		# ── Drafter proposal banner (any confidence) ──────────────────────
		_draft = drafts_by_cid.get(selected.get("cluster_id", "").strip())
		if _draft:
			_conf = _draft.get("confidence", "").strip().lower()
			_dec = _draft.get("draft_decision", "").strip()
			_did = _draft.get("draft_aligned_db_id", "").strip()
			_rat = _draft.get("rationale", "").strip()
			_db_name_hint = ""
			if _did:
				_db_name_hint = next(
					(r.get("name", "") for r in db_rows if r.get("db_id", "") == _did),
					"",
				)
			_target = f"`{_did}` {_db_name_hint}" if _did else "—"
			_msg = f"**Drafter suggests:** {_dec} · target: {_target} · confidence: **{_conf or 'unknown'}**\n\n_{_rat}_"
			if _conf == "high":
				st.success(_msg)
			elif _conf == "medium":
				st.warning(_msg)
			else:
				st.info(_msg)

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
					"person": "👤",
					"person_phonetic": "👤🔉",
				}.get(method_txt, "•")
				with st.container(border=True):
					st.markdown(
						f"<div class='rtl-block'>{icon} {dbid} · {db.get('name', '(missing)')}</div>",
						unsafe_allow_html=True,
					)
					st.caption(f"type: {db.get('org_type','')} · score: {score_txt} · method: {method_txt}")
					_corro = corro_by_key.get((selected.get("cluster_id", "").strip(), dbid))
					if _corro:
						st.markdown(
							f"<div class='rtl-block'>🕸 <b>{_corro.get('shared_hosts','')}</b> shared biographies: "
							f"{_corro.get('shared_host_headings','')}</div>",
							unsafe_allow_html=True,
						)
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
					_render_audit_jump_buttons(
						buckets=_db_buckets(dbid),
						button_key_prefix=f"audit-from-db-{selected['cluster_id']}-{dbid}",
					)

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
					hits = [r for r in active_db_rows(db_rows) if _db_matches(r)][:20]
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
					_db_type_options = list(_ORG_TYPE_OPTIONS)
					if _db_cur_type and _db_cur_type not in _db_type_options:
						_db_type_options.insert(0, _db_cur_type)
					_db_type_idx = _db_type_options.index(_db_cur_type) if _db_cur_type in _db_type_options else len(_db_type_options) - 1
					new_db_type = st.selectbox(
						"DB organization type",
						_db_type_options,
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

			_render_settlement_siblings(
				selected, choice_key, addr_db_ids,
				a_rows=a_rows,
				pair_headers=pair_headers,
				pair_rows=pair_rows,
			)

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
			log_action("org_review", "alignment",
				target_id=selected.get("cluster_id", ""),
				decision=quick_action, note=notes)
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
			log_action("org_review", "alignment",
				target_id=selected.get("cluster_id", ""),
				decision="ALIGN", note=notes,
				aligned_db_id=chosen_db_id)
			load_alignment.clear()
			st.session_state.pop(choice_key, None)
			st.rerun()

		if col2.button("New organization"):
			_ensure_alignment_columns()
			next_id = _next_db_id(db_rows)
			_cluster_yid = selected.get("canonical_yiddish", "").strip()
			_yid_for_db = _cluster_yid if _has_yiddish(_cluster_yid) else ""
			db_rows.append(
				{
					"db_id": str(next_id),
					"name": new_entity_name or _cluster_yid,
					"name_yiddish": _yid_for_db,
						"org_type": new_type or selected.get("org_type", "").strip().lower(),
					"address": review_address or selected.get("extracted_addresses", "").split("|", 1)[0].strip(),
					"linked_cluster_ids": selected.get("cluster_id", "").strip(),
				}
			)
			save_core_db(db_headers, db_rows)
			load_core_db.clear()

			append_address_row(
				db_id=str(next_id),
				name=new_entity_name or selected.get("canonical_yiddish", "").strip(),
				org_type=(new_type or selected.get("org_type", "").strip().lower()),
				cluster_id=selected.get("cluster_id", "").strip(),
				settlement=review_settlement,
				address=review_address,
			)

			a_rows[row_idx]["decision"] = "NEW"
			a_rows[row_idx]["aligned_db_id"] = str(next_id)
			a_rows[row_idx]["reviewer_notes"] = notes
			a_rows[row_idx]["reviewer_settlement"] = review_settlement
			a_rows[row_idx]["reviewer_address"] = review_address
			_stamp(a_rows[row_idx])
			save_alignment(a_headers, a_rows)
			log_action("org_review", "alignment",
				target_id=selected.get("cluster_id", ""),
				decision="NEW", note=notes,
				aligned_db_id=str(next_id))
			load_alignment.clear()
			st.rerun()
