"""Focused Gemini pass on the union-default rows that the rule cascade couldn't decide.

Targets every row where review_reason == 'context_weak:union_default_society'.
Sends each to Gemini 3 Pro with a focused prompt that says the rule defaulted
because no keyword matched, and asks for a from-scratch classification with
the full 31-type canonical list available.

Applies the result in place: updates canonical_type in both the mapping TSV and
the source data TSV. Records decided_via=llm_redo_union, review_reason=llm:<theme>.
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
PARALLEL = 20
MAX_OUTPUT_TOKENS = 2000
TARGET_REASON = "context_weak:union_default_society"

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

SYSTEM_TEMPLATE = """You are an expert reviewer of Yiddish theatre-lexicon (Zalmen Zylbercweig) organization classifications. You have native-grade reading in Yiddish, Hebrew, and English, plus solid historical knowledge of Jewish, Eastern European, and theatre history (1880s–1960s).

The fixed canonical typology and decision policy is below. Internalize it before deciding.

=========== POLICY ===========
{policy}
=========== END POLICY ===========

CONTEXT FOR THIS PASS:

Every row I send you has the original LLM tag = "union" (or a Yiddish equivalent like פאַריין, יוניע). A rule-based cascade looked at the name + sentence and couldn't find any clear cue (no theatre, music, writers, or labour-place keyword matched), so it fell back to a default. **The default may well be wrong.** Re-evaluate each row from scratch with the full 31-type canonical list.

Common Yiddish/Hebrew orthographic variants the rule layer may have missed:
- שרייבער / שרײַבער (writers) → Theatre-related Society/Union (if Yiddish-theatre writers' union)
- דראַמאַטורגן (dramatists), דראַמאַ גילד (Drama Guild) → Theatre-related Society/Union
- קאָמפּאָזיטאָרן (composers), חזנים (cantors) → Musical organization
- ראַדיאָ (radio), פילם (film), וואָדעוויל (vaudeville) → Media or Kleinkunst
- קינסטלער (artists), אַרטיסט (artist) → Theatre-related Society/Union if performing artists
- דרוקער (printers), שניידער (tailors), פֿוטער (furriers), שוך (shoe), סיגאַר (cigar), צעך (guild) → Trade Union / Professional Association
- אַרבעטער ring/farband — already named-entity-classified as Jewish political bodies; don't re-litigate
- PEN-club (פּען-קלוב), Hebrew Actors Union (היבריו עקטאָרס), Yiddish Actors Union → Theatre-related Society/Union
- Local + number ("יוניע לאָקאָל 5", "לאָקאל 18") → likely a trade union local; route to Trade Union / Professional Association unless theatre context

For each row, reply with strict JSON on ONE line, no code fences:
{{"canonical": "<exact type string>", "concern_theme": "<code>", "reason": "<<=25 words>"}}

concern_theme codes:
- "kept_default" — you confirm Theatre-related Society/Union is right.
- "moved_theatre_related" — you moved to a more specific theatre/music type (Musical organization, Amateur, etc.)
- "moved_trade_union" — you moved to Trade Union / Professional Association.
- "moved_political" — you moved to Jewish political bodies or Non-Jewish political bodies.
- "moved_other" — you moved to some other canonical.
- "ambiguous_keep_flag" — genuinely ambiguous; keep current canonical but the row deserves human review.
- "missing_canonical" — none of the 31 types fit.

Use EXACTLY these canonical strings (case and punctuation matter):
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


def process_file(spec: dict, client: genai.Client, system_prompt: str) -> dict:
	map_path = spec["map"]; data_path = spec["data"]; type_col = spec["type_col"]
	name_cols = spec["name_cols"]; sent_col = spec["sentence_col"]
	heading_col = spec["heading_col"]; role_col = spec["role_col"]
	rel_cat_col = spec["rel_cat_col"]

	with map_path.open(newline="", encoding="utf-8") as f:
		mrdr = csv.DictReader(f, delimiter="\t")
		map_fields = list(mrdr.fieldnames or [])
		map_rows = list(mrdr)
	with data_path.open(newline="", encoding="utf-8") as f:
		drdr = csv.DictReader(f, delimiter="\t")
		data_fields = list(drdr.fieldnames or [])
		data_rows = list(drdr)

	idxs = [i for i, r in enumerate(map_rows)
			if r.get("review_reason") == TARGET_REASON]
	print(f"\n== {map_path.name} ==\n  to redo: {len(idxs)}")
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
		msg = (
			f"Entry heading (person, if any): {heading}\n"
			f"Entity name (org): {name}\n"
			f"Original LLM tag: {orig}\n"
			f"Relation category: {rel_cat}\n"
			f"Role title: {role}\n"
			f"Sentence: {sentence}\n"
			f"Rule-default canonical (likely wrong): {auto}\n\n"
			f"Reply with strict JSON only."
		)
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
				mrow["llm_canonical"] = llm_canon
				mrow["llm_concern_theme"] = concern
				mrow["llm_reason"] = reason
				if llm_canon and llm_canon != auto:
					mrow["canonical_type"] = llm_canon
					if drow is not None:
						drow[type_col] = llm_canon
					mrow["decided_via"] = "llm_redo_union"
					mrow["review_reason"] = f"llm_redo:{concern}"
					mrow["changed"] = "yes"
					# only keep flagged if LLM said ambiguous or missing
					if concern in ("ambiguous_keep_flag", "missing_canonical"):
						mrow["needs_review"] = "yes"
					else:
						mrow["needs_review"] = ""
					changed[0] += 1
				elif llm_canon == auto:
					mrow["decided_via"] = "llm_redo_union"
					mrow["review_reason"] = f"llm_redo:kept_default"
					mrow["needs_review"] = ""
				stats[concern or "no_concern"] += 1
			completed += 1
			if completed % 25 == 0 or completed == len(idxs):
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
	if not os.environ.get("GOOGLE_API_KEY"):
		sys.exit("GOOGLE_API_KEY not set")
	policy = POLICY.read_text() if POLICY.exists() else ""
	system_prompt = SYSTEM_TEMPLATE.format(policy=policy)
	client = genai.Client()

	all_stats = []
	for spec in FILES:
		all_stats.append(process_file(spec, client, system_prompt))

	print("\n========== Redo summary ==========")
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
