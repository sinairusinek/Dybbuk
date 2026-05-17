"""Retry Gemini calibration for rows whose JSON didn't parse (truncated thinking budget).

Reads llm_calibrate_gemini.tsv, picks rows with empty gemini_canonical, re-runs
them with a much larger max_output_tokens, and writes the merged result back.
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
IN = HERE / "llm_calibrate_gemini.tsv"
OUT = HERE / "llm_calibrate_gemini.tsv"  # overwrite in place
POLICY = HERE / "CLASSIFICATION_POLICY.md"

MODEL = "gemini-3-pro-preview"

from llm_calibrate_gemini import SYSTEM_TEMPLATE, parse_json_loose


def main() -> None:
	policy = POLICY.read_text() if POLICY.exists() else ""
	system_prompt = SYSTEM_TEMPLATE.format(policy=policy)

	with IN.open(newline="", encoding="utf-8") as f:
		rdr = csv.DictReader(f, delimiter="\t")
		fieldnames = rdr.fieldnames
		all_rows = list(rdr)

	retry_indices = [i for i, r in enumerate(all_rows) if not r.get("gemini_canonical")]
	print(f"Retrying {len(retry_indices)} rows (of {len(all_rows)} total)")

	client = genai.Client()
	fixed = 0
	still_failing = 0
	for k, idx in enumerate(retry_indices, 1):
		row = all_rows[idx]
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
					max_output_tokens=2000,
					temperature=0.0,
				),
			)
			text = (resp.text or "").strip()
		except Exception as e:
			print(f"  row {idx}: API error: {e}")
			still_failing += 1
			continue

		data = parse_json_loose(text)
		gem_canon = (data.get("canonical") or "").strip()
		if not gem_canon:
			still_failing += 1
			# Save the raw text into the reason field for inspection
			row["gemini_reason"] = (text[:200] or "no-canonical-in-response")
		else:
			row["gemini_canonical"] = gem_canon
			row["gemini_agree_with_auto"] = "yes" if data.get("agree") is True else ("no" if data.get("agree") is False else "")
			row["gemini_concern_theme"] = data.get("concern_theme", "")
			row["gemini_reason"] = data.get("reason", "")
			opus_canon = (row.get("llm_canonical") or "").strip()
			row["gemini_vs_opus_canonical"] = "match" if gem_canon == opus_canon else "differ"
			fixed += 1
		if k % 20 == 0 or k == len(retry_indices):
			print(f"  {k}/{len(retry_indices)} retried (fixed: {fixed}, still failing: {still_failing})")

	with OUT.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
		w.writeheader()
		w.writerows(all_rows)

	# Recompute aggregate stats
	n_total = len(all_rows)
	n_filled = sum(1 for r in all_rows if r.get("gemini_canonical"))
	n_match_opus = sum(1 for r in all_rows if r.get("gemini_vs_opus_canonical") == "match")
	opus_disagrees = [r for r in all_rows if r.get("llm_agree") == "no"]
	gemini_caught_same = sum(1 for r in opus_disagrees if r.get("gemini_canonical") == r.get("llm_canonical"))
	opus_agrees = [r for r in all_rows if r.get("llm_agree") == "yes"]
	gemini_also_agrees = sum(1 for r in opus_agrees if r.get("gemini_canonical") == r.get("auto_canonical"))
	gem_themes = Counter(r.get("gemini_concern_theme", "") for r in all_rows)

	print(f"\n=== After retry ===")
	print(f"Filled responses: {n_filled}/{n_total}")
	print(f"Gemini canonical == Opus canonical: {n_match_opus}/{n_total} ({100*n_match_opus/n_total:.0f}%)")
	if opus_disagrees:
		print(f"On rows where Opus disagreed with auto: Gemini reached same conclusion in {gemini_caught_same}/{len(opus_disagrees)} ({100*gemini_caught_same/len(opus_disagrees):.0f}%)")
	if opus_agrees:
		print(f"On rows where Opus agreed with auto: Gemini also agreed in {gemini_also_agrees}/{len(opus_agrees)} ({100*gemini_also_agrees/len(opus_agrees):.0f}%)")
	print(f"\nGemini concern themes:")
	for t, n in gem_themes.most_common():
		print(f"  {n:4d}  {t!r}")


if __name__ == "__main__":
	main()
