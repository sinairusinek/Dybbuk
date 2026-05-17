"""Full LLM verification pass with Gemini 3 Pro.

Verifies all v3 mapping decisions where `decided_via in {context, context_weak,
named_entity}` across the four mapping TSVs (mentions, clusters, addresses_review,
core_db). When the LLM disagrees with the auto canonical, the LLM's choice
overrides — that decision is recorded with `decided_via=llm_verified`.

For each row, writes back into both:
  - the mapping TSV: adds llm_canonical, llm_agree, llm_concern_theme, llm_reason
    and updates canonical_type if LLM overrode
  - the source data TSV: updates the org_type column if LLM overrode

PI dilemma flags from the named-entity rule are preserved regardless of LLM agreement.

Resumable: skips rows that already have llm_canonical populated.
"""
from __future__ import annotations
import csv
import json
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from google import genai
from google.genai import types

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent
POLICY = HERE / "CLASSIFICATION_POLICY.md"
MODEL = "gemini-3-pro-preview"
MAX_OUTPUT_TOKENS = 2000
TARGET_DECIDED_VIA = {"context", "context_weak", "named_entity"}
PARALLEL = 20  # concurrent in-flight Gemini requests
SAVE_EVERY = 100  # write to disk every N completed rows

# (mapping_tsv, data_tsv, data_tsv_type_column, data_tsv_id_column, data_tsv_name_columns, data_tsv_sentence_column, data_tsv_role_col, data_tsv_specific_relation_col, data_tsv_relation_category_col)
FILES = [
	{
		"map": HERE / "organizations_clustered_canonical_mapping.tsv",
		"data": HERE / "organizations_clustered.tsv",
		"type_col": "_ - organizations - _ - org_type",
		"id_col": "cluster_id",
		"name_cols": ["clustered organization", "_ - organizations - _ - title",
					  "_ - organizations - _ - descriptive_name"],
		"sentence_col": "_ - organizations - _ - relations - _ - original_sentence",
		"role_col": "_ - organizations - _ - relations - _ - role_title",
		"spec_rel_col": "_ - organizations - _ - relations - _ - specific_relation",
		"rel_cat_col": "_ - organizations - _ - relations - _ - category",
		"heading_col": "_ - heading",
	},
	{
		"map": HERE / "org_alignment_review_canonical_mapping.tsv",
		"data": HERE / "org_alignment_review.tsv",
		"type_col": "org_type",
		"id_col": "cluster_id",
		"name_cols": ["canonical_yiddish", "name_variants"],
		"sentence_col": "name_variants",
		"role_col": None, "spec_rel_col": None, "rel_cat_col": None, "heading_col": None,
	},
	{
		"map": HERE / "org_addresses_review_canonical_mapping.tsv",
		"data": HERE / "org_addresses_review.tsv",
		"type_col": "org_type",
		"id_col": "db_id",
		"name_cols": ["canonical_yiddish"],
		"sentence_col": None,
		"role_col": None, "spec_rel_col": None, "rel_cat_col": None, "heading_col": None,
	},
	{
		"map": HERE / "core_db_canonical_mapping.tsv",
		"data": HERE / "core_db.tsv",
		"type_col": "org_type",
		"id_col": "db_id",
		"name_cols": ["name"],
		"sentence_col": None,
		"role_col": None, "spec_rel_col": None, "rel_cat_col": None, "heading_col": None,
	},
]

SYSTEM_TEMPLATE = """You are an expert reviewer of Yiddish theatre-lexicon (Zalmen Zylbercweig) organization classifications. You have native-grade reading in Yiddish, Hebrew, and English, plus solid historical knowledge of Jewish, Eastern European, and theatre history (1880s–1960s).

The fixed canonical typology and decision policy is below. Internalize it before reviewing.

=========== POLICY ===========
{policy}
=========== END POLICY ===========

For each row I send you, you receive richer context than a tag-keyword rule sees:
- Entry heading (the person whose lexicon entry contains the mention, if available)
- Entity name (the organization)
- Original LLM tag (free-text type extracted from source)
- Auto-assigned canonical (the v3 mapper's choice — may be correct or wrong)
- Decision metadata (decided_via, review_reason)
- Role title, specific relation, sentence (if available)

Your job for each row:
1. Decide the correct canonical from the 27 types in the policy.
2. Report whether you agree with the auto-assigned canonical.
3. Tag your finding with ONE concern_theme.
4. Give a concise reason (<=30 words).

concern_theme codes (pick exactly one):
- "agree" — confirm auto.
- "shallow_keyword_overfire" — auto rule fired on a keyword that doesn't fit.
- "shallow_keyword_missed_cue" — context indicates a different bucket but auto missed it.
- "wrong_named_entity" — auto's named-entity match is a false positive.
- "policy_ambiguous" — genuinely ambiguous; policy doesn't direct unambiguously.
- "missing_canonical_type" — entity fits no canonical; may need new type.
- "pi_judgement_needed" — dual/multi-identity case worth PI review.
- "data_quality" — row's name/sentence is malformed (LLM extraction error upstream).
- "minor_drift" — auto picked an acceptable bucket but a different one is slightly better.

Reply with strict JSON on ONE line, no code fences:
{{"canonical": "<exact type string>", "agree": true|false, "concern_theme": "<code>", "reason": "<<=30 words>", "suggested_new_canonical": "<name or empty>"}}

Use EXACTLY these strings for canonical (case and punctuation matter, note the space in "Society/ Union"):
Theatre, Traveling Company, Company on Tour, Amateur, Kleinkunst, Circus, Theatre education, Publisher, Printer, Printer/Publisher, Journals/ Newspapers, Media (Radio/ Film/TV), Library, Heritage Institution, Education, Musical organization, Theatre-related Society/ Union, Religious institutions/organizations, Jewish political bodies, Non-Jewish political bodies, Welfare/Aid organization, Business, Labour (factory/workshop), Health institutions, Military, Not an organization, OTHER - elaborate!"""


def parse_json_loose(text: str) -> dict:
	t = (text or "").strip()
	if t.startswith("```"):
		lines = t.split("\n")
		t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
	i = t.find("{"); j = t.rfind("}")
	if i >= 0 and j > i:
		t = t[i:j + 1]
	try:
		return json.loads(t)
	except Exception:
		return {}


LLM_COLS = ["llm_canonical", "llm_agree", "llm_concern_theme", "llm_reason",
			"llm_suggested_new_canonical"]


def process_file(spec: dict, client: genai.Client, system_prompt: str) -> dict:
	map_path = spec["map"]
	data_path = spec["data"]
	type_col = spec["type_col"]
	name_cols = spec["name_cols"]
	sent_col = spec["sentence_col"]
	role_col = spec["role_col"]
	spec_rel_col = spec["spec_rel_col"]
	rel_cat_col = spec["rel_cat_col"]
	heading_col = spec["heading_col"]

	with map_path.open(newline="", encoding="utf-8") as f:
		mrdr = csv.DictReader(f, delimiter="\t")
		map_fields = list(mrdr.fieldnames or [])
		map_rows = list(mrdr)
	# Ensure LLM columns present
	for c in LLM_COLS:
		if c not in map_fields:
			map_fields.append(c)
	for r in map_rows:
		for c in LLM_COLS:
			r.setdefault(c, "")

	with data_path.open(newline="", encoding="utf-8") as f:
		drdr = csv.DictReader(f, delimiter="\t")
		data_fields = drdr.fieldnames
		data_rows = list(drdr)

	# Find data context for each map row by positional alignment (same row order
	# as v3 produced).
	if len(data_rows) != len(map_rows):
		print(f"  WARN: row count mismatch {len(data_rows)} data vs {len(map_rows)} map")

	to_verify_idx = [
		i for i, r in enumerate(map_rows)
		if r.get("decided_via", "") in TARGET_DECIDED_VIA and not r.get("llm_canonical")
	]
	print(f"\n== {map_path.name} ==")
	print(f"  to verify: {len(to_verify_idx)}")

	stats = Counter()
	overrides = [0]
	stats_lock = threading.Lock()
	rows_lock = threading.Lock()

	def _build_msg(idx: int) -> tuple[str, dict, dict]:
		mrow = map_rows[idx]
		drow = data_rows[idx] if idx < len(data_rows) else {}
		name = mrow.get("name", "")
		if not name:
			for nc in name_cols:
				if nc and drow.get(nc):
					name = drow[nc]; break
		auto = mrow.get("canonical_type", "")
		orig = mrow.get("original_type", "")
		decided_via = mrow.get("decided_via", "")
		review_reason = mrow.get("review_reason", "")
		sentence = (drow.get(sent_col) or "")[:1200] if sent_col else ""
		heading = (drow.get(heading_col) or "") if heading_col else ""
		role = (drow.get(role_col) or "") if role_col else ""
		spec_rel = (drow.get(spec_rel_col) or "") if spec_rel_col else ""
		rel_cat = (drow.get(rel_cat_col) or "") if rel_cat_col else ""
		ctx = {"name": name, "auto": auto, "orig": orig,
			   "decided_via": decided_via, "review_reason": review_reason}
		msg = (
			f"Entry heading (person, if any): {heading}\n"
			f"Entity name (org): {name}\n"
			f"Original LLM tag: {orig}\n"
			f"Relation category: {rel_cat}\n"
			f"Specific relation: {spec_rel}\n"
			f"Role title: {role}\n"
			f"Sentence: {sentence}\n"
			f"Auto-assigned canonical: {auto}\n"
			f"Decided via: {decided_via}\n"
			f"Review reason (if any): {review_reason}\n\n"
			f"Reply with strict JSON only."
		)
		return msg, mrow, ctx

	def _do_row(idx: int) -> tuple[int, str, dict, dict]:
		msg, mrow, ctx = _build_msg(idx)
		text = ""
		for retry in range(3):
			try:
				resp = client.models.generate_content(
					model=MODEL, contents=msg,
					config=types.GenerateContentConfig(
						system_instruction=system_prompt,
						max_output_tokens=MAX_OUTPUT_TOKENS,
						temperature=0.0,
					),
				)
				text = (resp.text or "").strip()
				break
			except Exception as e:
				if retry == 2:
					return idx, f"__error__: {e}", mrow, ctx
				time.sleep(2 * (retry + 1))
		return idx, text, mrow, ctx

	completed = 0
	t0 = time.time()
	with ThreadPoolExecutor(max_workers=PARALLEL) as ex:
		futures = {ex.submit(_do_row, idx): idx for idx in to_verify_idx}
		for fut in as_completed(futures):
			idx, text, mrow, ctx = fut.result()
			data = parse_json_loose(text)
			llm_canon = (data.get("canonical") or "").strip()
			concern = data.get("concern_theme", "") if not text.startswith("__error__") else "api_error"
			reason = data.get("reason", "") if not text.startswith("__error__") else text
			suggested_new = data.get("suggested_new_canonical", "")
			auto = ctx["auto"]
			agree_with_auto = (llm_canon == auto) if llm_canon else None

			with rows_lock:
				mrow["llm_canonical"] = llm_canon
				mrow["llm_agree"] = "yes" if agree_with_auto is True else ("no" if agree_with_auto is False else "")
				mrow["llm_concern_theme"] = concern
				mrow["llm_reason"] = reason
				mrow["llm_suggested_new_canonical"] = suggested_new
				if llm_canon and not agree_with_auto:
					mrow["canonical_type"] = llm_canon
					if idx < len(data_rows):
						data_rows[idx][type_col] = llm_canon
					mrow["decided_via"] = "llm_verified"
					if not (ctx["review_reason"] and ctx["review_reason"].startswith("pi_dilemma:")):
						mrow["review_reason"] = f"llm:{concern}" if concern else "llm:override"
					mrow["changed"] = "yes"
					mrow["needs_review"] = "yes"
					overrides[0] += 1
			with stats_lock:
				stats[concern or "no_concern"] += 1
			completed += 1
			if completed % SAVE_EVERY == 0 or completed == len(to_verify_idx):
				with rows_lock:
					_write_map(map_path, map_fields, map_rows)
					_write_data(data_path, data_fields, data_rows)
				elapsed = time.time() - t0
				rate = completed / max(elapsed, 1)
				eta = (len(to_verify_idx) - completed) / max(rate, 0.01)
				print(f"  {completed}/{len(to_verify_idx)} done, overrides: {overrides[0]}, rate {rate:.1f}/s, eta {eta:.0f}s", flush=True)

	return {"file": map_path.name, "verified": len(to_verify_idx),
			"overrides": overrides[0], "themes": dict(stats)}


def _write_map(path: Path, fields: list[str], rows: list[dict]) -> None:
	with path.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
		w.writeheader()
		for r in rows:
			w.writerow({k: r.get(k, "") for k in fields})


def _write_data(path: Path, fields: list[str], rows: list[dict]) -> None:
	with path.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
		w.writeheader()
		for r in rows:
			w.writerow({k: r.get(k, "") for k in fields})


def main() -> None:
	if not os.environ.get("GOOGLE_API_KEY"):
		sys.exit("GOOGLE_API_KEY not set")
	policy = POLICY.read_text() if POLICY.exists() else ""
	system_prompt = SYSTEM_TEMPLATE.format(policy=policy)
	client = genai.Client()

	all_stats = []
	for spec in FILES:
		stats = process_file(spec, client, system_prompt)
		all_stats.append(stats)

	print("\n========== Verification summary ==========")
	total_verified = 0
	total_overrides = 0
	combined_themes: Counter = Counter()
	for s in all_stats:
		print(f"  {s['file']}: verified={s['verified']}, overrides={s['overrides']}")
		total_verified += s["verified"]
		total_overrides += s["overrides"]
		for t, n in s["themes"].items():
			combined_themes[t] += n
	print(f"\nTotal verified: {total_verified}   Overrides: {total_overrides} ({100*total_overrides/max(total_verified,1):.0f}%)")
	print("\nConcern themes (combined):")
	for t, n in combined_themes.most_common():
		print(f"  {n:5d}  {t}")


if __name__ == "__main__":
	main()
