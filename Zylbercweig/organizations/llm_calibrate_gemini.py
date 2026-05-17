"""Gemini 3 Pro calibration pass — run the same prompt+rows Opus 4.7 saw.

Reads llm_deep_review.tsv (Opus's verdicts), sends the same per-row prompts to
Gemini 3 Pro, and writes a side-by-side comparison.

Goal: measure Gemini-vs-Opus agreement on Yiddish theatre-lexicon classification
to decide whether Gemini-only or a hybrid is justified for the larger
verification pass.
"""
from __future__ import annotations
import csv
import json
import os
import sys
from collections import Counter
from pathlib import Path

from google import genai
from google.genai import types

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent
OPUS_TSV = HERE / "llm_deep_review.tsv"
POLICY = HERE / "CLASSIFICATION_POLICY.md"
OUT = HERE / "llm_calibrate_gemini.tsv"

MODEL = "gemini-3-pro-preview"

SYSTEM_TEMPLATE = """You are an expert reviewer of Yiddish theatre-lexicon (Zalmen Zylbercweig) organization classifications. You have native-grade reading in Yiddish, Hebrew, and English, plus solid historical knowledge of Jewish, Eastern European, and theatre history (1880s–1960s).

The fixed canonical typology and decision policy is below. Internalize it before reviewing.

=========== POLICY ===========
{policy}
=========== END POLICY ===========

For each row I send you, you receive richer context than a tag-keyword rule sees:
- Entry heading (the person whose lexicon entry contains the mention)
- Original LLM tag (free-text type extracted from source)
- Auto-assigned canonical (the v3 mapper's choice)
- Decision metadata (decided_via, review_reason)
- Role title (person's role at this org)
- Specific relation (more detail than category)
- Full sentence

Your job:
1. Decide the correct canonical from the 27 types in the policy.
2. Report whether you agree with the auto-assigned canonical.
3. Tag your finding with ONE concern_theme (see list below) so we can aggregate.
4. Give a concise reason (<=30 words).

concern_theme codes (pick exactly one):
- "agree" — you confirm the auto choice.
- "shallow_keyword_overfire" — auto rule fired on a keyword that doesn't fit this specific entity.
- "shallow_keyword_missed_cue" — context clearly indicates a different bucket but auto rule didn't catch the cue.
- "wrong_named_entity" — auto's named-entity match is wrong for this entity (false positive).
- "policy_ambiguous" — the entity is genuinely ambiguous; the policy doesn't unambiguously direct.
- "missing_canonical_type" — entity doesn't fit any of the 27 types; we may need a new canonical.
- "pi_judgement_needed" — entity is a genuine dual/multi-identity case worth flagging to the PI.
- "data_quality" — the row's name/sentence is malformed or wrong (LLM extraction error upstream).
- "minor_drift" — auto picked an acceptable bucket but a different one is slightly better.

Reply with strict JSON on ONE line, no code fences:
{{"canonical": "<exact type string>", "agree": true|false, "concern_theme": "<code>", "reason": "<<=30 words>", "suggested_new_canonical": "<name>"}}

`suggested_new_canonical` is only filled when concern_theme is "missing_canonical_type"; otherwise empty string.

Use EXACTLY these strings for canonical (case and punctuation matter):
Theatre, Traveling Company, Company on Tour, Amateur, Kleinkunst, Circus, Theatre education, Publisher, Printer, Printer/Publisher, Journals/ Newspapers, Media (Radio/ Film/TV), Library, Heritage Institution, Education, Musical organization, Theatre-related Society/ Union, Religious institutions/organizations, Jewish political bodies, Non-Jewish political bodies, Welfare/Aid organization, Business, Labour (factory/workshop), Health institutions, Military, Not an organization, OTHER - elaborate!"""


def parse_json_loose(text: str) -> dict:
	t = text.strip()
	if t.startswith("```"):
		lines = t.split("\n")
		t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
	# Find first { and last }
	i = t.find("{")
	j = t.rfind("}")
	if i >= 0 and j > i:
		t = t[i:j + 1]
	try:
		return json.loads(t)
	except Exception:
		return {}


def main() -> None:
	if not os.environ.get("GOOGLE_API_KEY"):
		sys.exit("GOOGLE_API_KEY not set")
	policy = POLICY.read_text() if POLICY.exists() else ""
	system_prompt = SYSTEM_TEMPLATE.format(policy=policy)

	with OPUS_TSV.open(newline="", encoding="utf-8") as f:
		opus_rows = list(csv.DictReader(f, delimiter="\t"))
	print(f"Calibration sample size: {len(opus_rows)}")
	print(f"Model: {MODEL}")
	print(f"System prompt ~{len(system_prompt):,} chars")

	client = genai.Client()
	results: list[dict] = []
	api_errors = 0
	for i, row in enumerate(opus_rows, 1):
		user_msg = (
			f"Entry heading (person): {row.get('heading', '')}\n"
			f"Entity name (org): {row.get('name', '')}\n"
			f"Original LLM tag: {row.get('original_type', '')}\n"
			f"Relation category: {row.get('relation_category', '')}\n"
			f"Role title: {row.get('role', '')}\n"
			f"Sentence: {row.get('sentence', '')}\n"
			f"Auto-assigned canonical: {row.get('auto_canonical', '')}\n"
			f"Decided via: {row.get('decided_via', '')}\n"
			f"Review reason (if any): {row.get('review_reason', '')}\n\n"
			f"Reply with strict JSON only."
		)
		try:
			resp = client.models.generate_content(
				model=MODEL,
				contents=user_msg,
				config=types.GenerateContentConfig(
					system_instruction=system_prompt,
					max_output_tokens=400,
					temperature=0.0,
				),
			)
			text = (resp.text or "").strip()
		except Exception as e:
			print(f"  row {i}: API error: {e}")
			api_errors += 1
			text = ""

		data = parse_json_loose(text)
		gem_canon = (data.get("canonical") or "").strip()
		gem_agree_with_auto = data.get("agree")
		gem_theme = data.get("concern_theme", "")
		gem_reason = data.get("reason", "")

		opus_canon = (row.get("llm_canonical") or "").strip()
		opus_theme = row.get("concern_theme", "")
		auto_canon = row.get("auto_canonical", "")

		gemini_matches_opus_canonical = (gem_canon == opus_canon) if (gem_canon and opus_canon) else False

		results.append({
			**row,
			"gemini_canonical": gem_canon,
			"gemini_agree_with_auto": "yes" if gem_agree_with_auto is True else ("no" if gem_agree_with_auto is False else ""),
			"gemini_concern_theme": gem_theme,
			"gemini_reason": gem_reason,
			"gemini_vs_opus_canonical": "match" if gemini_matches_opus_canonical else "differ",
		})
		if i % 20 == 0 or i == len(opus_rows):
			print(f"  {i}/{len(opus_rows)} done")

	cols = ["row_id", "name", "original_type", "auto_canonical",
			"decided_via", "review_reason", "sentence",
			"llm_canonical", "llm_agree", "concern_theme", "llm_reason",
			"gemini_canonical", "gemini_agree_with_auto", "gemini_concern_theme",
			"gemini_reason", "gemini_vs_opus_canonical"]
	with OUT.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
		w.writeheader()
		for r in results:
			w.writerow({k: r.get(k, "") for k in cols})

	# Agreement metrics
	n_total = len(results)
	# How often does Gemini match Opus's canonical?
	n_match_opus = sum(1 for r in results if r["gemini_vs_opus_canonical"] == "match")
	# When auto-vs-opus disagreed, how often does Gemini agree with Opus (i.e. catches the same issues)?
	opus_disagrees = [r for r in results if r.get("llm_agree") == "no"]
	gemini_caught_same = sum(1 for r in opus_disagrees if r["gemini_canonical"] == r["llm_canonical"])
	# When Opus agreed with auto, did Gemini also agree?
	opus_agrees = [r for r in results if r.get("llm_agree") == "yes"]
	gemini_also_agrees = sum(1 for r in opus_agrees if r["gemini_canonical"] == r["auto_canonical"])

	# Theme distribution from Gemini
	gem_themes = Counter(r["gemini_concern_theme"] for r in results)

	print(f"\n=== Calibration results ===")
	print(f"API errors: {api_errors}")
	print(f"Total: {n_total}")
	print(f"Gemini canonical == Opus canonical: {n_match_opus}/{n_total} ({100*n_match_opus/n_total:.0f}%)")
	if opus_disagrees:
		print(f"On rows where Opus disagreed with auto: Gemini reached same conclusion in {gemini_caught_same}/{len(opus_disagrees)} ({100*gemini_caught_same/len(opus_disagrees):.0f}%)")
	if opus_agrees:
		print(f"On rows where Opus agreed with auto: Gemini also agreed in {gemini_also_agrees}/{len(opus_agrees)} ({100*gemini_also_agrees/len(opus_agrees):.0f}%)")
	print(f"\nGemini concern themes:")
	for t, n in gem_themes.most_common():
		print(f"  {n:4d}  {t}")
	print(f"\nOutput: {OUT.name}")


if __name__ == "__main__":
	main()
