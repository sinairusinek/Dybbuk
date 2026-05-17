"""Opus 4.7 redo of the remaining hard residual cases.

Targets:
  - context_weak:company_no_cue
  - context_weak:camp_default_not_org
  - unresolved
  - (none) — rows with empty review_reason still flagged
  - llm_redo:ambiguous_keep_flag — Gemini said still ambiguous after the union redo

For each, sends to Opus with the full policy + a focused prompt that the rule
defaulted because no keyword cue was found. Opus reclassifies or confirms.
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

import anthropic

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent
POLICY = HERE / "CLASSIFICATION_POLICY.md"
MODEL = "claude-opus-4-5"
PARALLEL = 10
MAX_TOKENS = 400

TARGET_REASONS = {
	"context_weak:company_no_cue",
	"context_weak:camp_default_not_org",
	"unresolved",
	"",  # rows with empty review_reason still flagged
	"llm_redo:ambiguous_keep_flag",
}

FILES = [
	{
		"map": HERE / "organizations_clustered_canonical_mapping.tsv",
		"data": HERE / "organizations_clustered.tsv",
		"type_col": "_ - organizations - _ - org_type",
		"name_cols": ["clustered organization", "_ - organizations - _ - title"],
		"sentence_col": "_ - organizations - _ - relations - _ - original_sentence",
		"heading_col": "_ - heading",
		"role_col": "_ - organizations - _ - relations - _ - role_title",
		"rel_cat_col": "_ - organizations - _ - relations - _ - category",
	},
	{
		"map": HERE / "org_alignment_review_canonical_mapping.tsv",
		"data": HERE / "org_alignment_review.tsv",
		"type_col": "org_type",
		"name_cols": ["canonical_yiddish", "name_variants"],
		"sentence_col": "name_variants",
		"heading_col": None,
		"role_col": None,
		"rel_cat_col": None,
	},
]

SYSTEM_TEMPLATE = """You are an expert reviewer of Yiddish theatre-lexicon (Zalmen Zylbercweig) organization classifications. Native-grade Yiddish/Hebrew/English; solid historical knowledge of Jewish, Eastern European, and theatre history (1880s–1960s).

The full canonical typology and decision policy is below.

=========== POLICY ===========
{policy}
=========== END POLICY ===========

CONTEXT FOR THIS PASS:

Every row I send has already been through a rule-based cascade and a Gemini 3 Pro verification. Both either defaulted to a fallback bucket (no keyword cue matched) or flagged the row as unresolved. These are the hardest residual cases. Look very carefully at the entity name, the sentence, the relation category, and the historical context. Decide the right canonical from the 31-type list, with your full Yiddish-historical reasoning.

If the entity is genuinely not classifiable into any of the 31 types, return canonical="OTHER - elaborate!" with a clear `reason`.

If the row is clearly a data-quality problem (a person mis-tagged as an org, a place mis-tagged as an org, a film title, etc.), return canonical="Not an organization" with `concern_theme=data_quality`.

Reply with strict JSON on ONE line, no code fences:
{{"canonical": "<exact type string>", "concern_theme": "<code>", "reason": "<<=25 words>"}}

concern_theme codes:
- "resolved" — you found the right bucket.
- "kept_default" — you confirm the rule's default.
- "data_quality" — the row isn't really an organization.
- "still_ambiguous" — best guess applied but row needs human review.

Canonical strings (exact, case + punctuation matter):
Theatre, Traveling Company, Company on Tour, Amateur, Kleinkunst, Circus, Theatre education, Publisher, Printer, Printer/Publisher, Journals/ Newspapers, Media (Radio/ Film/TV), Library, Heritage Institution, Education, Musical organization, Theatre-related Society/ Union, Religious institutions/organizations, Jewish political bodies, Non-Jewish political bodies, Welfare/Aid organization, Business, Labour (factory/workshop), Health institutions, Military, Not an organization, OTHER - elaborate!, Trade Union / Professional Association, Judenrat, Sports/Recreation, Fraternal order"""


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


def process_file(spec: dict, client: anthropic.Anthropic, system_prompt: str) -> dict:
	map_path = spec["map"]; data_path = spec["data"]
	type_col = spec["type_col"]; name_cols = spec["name_cols"]
	sent_col = spec["sentence_col"]; heading_col = spec["heading_col"]
	role_col = spec["role_col"]; rel_cat_col = spec["rel_cat_col"]

	with map_path.open(newline="", encoding="utf-8") as f:
		mrdr = csv.DictReader(f, delimiter="\t")
		map_fields = list(mrdr.fieldnames or [])
		map_rows = list(mrdr)
	with data_path.open(newline="", encoding="utf-8") as f:
		drdr = csv.DictReader(f, delimiter="\t")
		data_fields = list(drdr.fieldnames or [])
		data_rows = list(drdr)

	idxs = [i for i, r in enumerate(map_rows)
			if r.get("needs_review") == "yes" and r.get("review_reason", "") in TARGET_REASONS]
	print(f"\n== {map_path.name} ==\n  to redo: {len(idxs)}", flush=True)
	if not idxs:
		return {"file": map_path.name, "redone": 0, "changed": 0, "themes": {}}

	rows_lock = threading.Lock()
	stats = Counter()
	changed = [0]

	def _do(idx: int):
		mrow = map_rows[idx]
		drow = data_rows[idx] if idx < len(data_rows) else {}
		name = mrow.get("name", "")
		if not name:
			for nc in name_cols:
				if drow.get(nc): name = drow[nc]; break
		sentence = (drow.get(sent_col) or "")[:1200] if sent_col else ""
		heading = (drow.get(heading_col) or "") if heading_col else ""
		role = (drow.get(role_col) or "") if role_col else ""
		rel_cat = (drow.get(rel_cat_col) or "") if rel_cat_col else ""
		auto = mrow.get("canonical_type", "")
		orig = mrow.get("original_type", "")
		decided_via = mrow.get("decided_via", "")
		review_reason = mrow.get("review_reason", "")
		msg = (
			f"Entry heading (person, if any): {heading}\n"
			f"Entity name (org): {name}\n"
			f"Original LLM tag: {orig}\n"
			f"Relation category: {rel_cat}\n"
			f"Role title: {role}\n"
			f"Sentence: {sentence}\n"
			f"Current canonical (rule default or unresolved): {auto}\n"
			f"Why flagged: decided_via={decided_via}, review_reason={review_reason}\n\n"
			f"Reply with strict JSON only."
		)
		text = ""
		for retry in range(3):
			try:
				resp = client.messages.create(
					model=MODEL,
					max_tokens=MAX_TOKENS,
					system=[{"type": "text", "text": system_prompt,
							 "cache_control": {"type": "ephemeral"}}],
					messages=[{"role": "user", "content": msg}],
				)
				text = resp.content[0].text.strip()
				break
			except Exception as e:
				if retry == 2:
					return idx, f"__err__:{e}"
				time.sleep(2 * (retry + 1))
		return idx, text

	t0 = time.time()
	completed = 0
	with ThreadPoolExecutor(max_workers=PARALLEL) as ex:
		futures = {ex.submit(_do, i): i for i in idxs}
		for fut in as_completed(futures):
			idx, text = fut.result()
			mrow = map_rows[idx]
			drow = data_rows[idx] if idx < len(data_rows) else None
			data = parse_json_loose(text)
			llm_canon = (data.get("canonical") or "").strip()
			concern = data.get("concern_theme", "") if not text.startswith("__err__") else "api_error"
			reason = data.get("reason", "") if not text.startswith("__err__") else text
			auto = mrow.get("canonical_type", "")

			with rows_lock:
				if llm_canon:
					if llm_canon != auto:
						mrow["canonical_type"] = llm_canon
						if drow is not None:
							drow[type_col] = llm_canon
						mrow["decided_via"] = "opus_redo"
						mrow["review_reason"] = f"opus_redo:{concern}"
						mrow["changed"] = "yes"
						changed[0] += 1
					else:
						mrow["decided_via"] = "opus_redo"
						mrow["review_reason"] = f"opus_redo:{concern}"
					# Flag retention
					if concern in ("still_ambiguous", "api_error"):
						mrow["needs_review"] = "yes"
					elif concern == "data_quality":
						# data quality issues stay flagged per PI's case-by-case decision
						mrow["needs_review"] = "yes"
					else:
						mrow["needs_review"] = ""
					mrow["llm_canonical"] = llm_canon
					mrow["llm_concern_theme"] = concern
					mrow["llm_reason"] = reason
				stats[concern or "no_concern"] += 1
			completed += 1
			if completed % 20 == 0 or completed == len(idxs):
				with rows_lock:
					with map_path.open("w", newline="", encoding="utf-8") as f:
						w = csv.DictWriter(f, fieldnames=map_fields, delimiter="\t")
						w.writeheader()
						for r in map_rows: w.writerow({k: r.get(k, "") for k in map_fields})
					with data_path.open("w", newline="", encoding="utf-8") as f:
						w = csv.DictWriter(f, fieldnames=data_fields, delimiter="\t")
						w.writeheader()
						for r in data_rows: w.writerow({k: r.get(k, "") for k in data_fields})
				el = time.time() - t0
				rate = completed / max(el, 1)
				print(f"  {completed}/{len(idxs)} done, changed: {changed[0]}, rate {rate:.1f}/s", flush=True)

	return {"file": map_path.name, "redone": len(idxs), "changed": changed[0], "themes": dict(stats)}


def main() -> None:
	if not os.environ.get("ANTHROPIC_API_KEY"):
		sys.exit("ANTHROPIC_API_KEY not set")
	policy = POLICY.read_text() if POLICY.exists() else ""
	system_prompt = SYSTEM_TEMPLATE.format(policy=policy)
	client = anthropic.Anthropic()

	all_stats = []
	for spec in FILES:
		all_stats.append(process_file(spec, client, system_prompt))

	print("\n========== Opus redo summary ==========")
	total_redone = 0; total_changed = 0
	combined: Counter = Counter()
	for s in all_stats:
		print(f"  {s['file']}: redone={s['redone']}, changed={s['changed']}")
		total_redone += s['redone']; total_changed += s['changed']
		for t, n in s['themes'].items():
			combined[t] += n
	print(f"\nTotal redone: {total_redone}   Changed: {total_changed} ({100*total_changed/max(total_redone,1):.0f}%)")
	print("\nConcern themes:")
	for t, n in combined.most_common():
		print(f"  {n:4d}  {t}")


if __name__ == "__main__":
	main()
