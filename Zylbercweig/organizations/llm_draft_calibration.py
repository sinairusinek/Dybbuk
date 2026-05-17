"""Blinded calibration of llm_draft_alignment.py against the 295 already-decided rows.

Runs the same drafter prompt on rows that already have an RA decision (with the
decision blinded from the prompt) and compares draft vs. actual to measure
agreement, especially at confidence=high.

Outputs:
  llm_draft_calibration.tsv — per-row draft + actual side-by-side
  llm_draft_calibration_summary.tsv — agreement metrics by confidence and decision

Usage:
    GOOGLE_API_KEY=... python llm_draft_calibration.py
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types

from llm_draft_alignment import (
	ALIGN_TSV,
	CORE_DB_TSV,
	DECISION_VOCAB,
	DEFAULT_MODEL,
	SYSTEM_TEMPLATE,
	build_db_index,
	fmt_cluster,
	load_tsv,
	parse_json_loose,
	pick_few_shot,
)

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent
OUT_TSV = HERE / "llm_draft_calibration.tsv"
SUMMARY_TSV = HERE / "llm_draft_calibration_summary.tsv"


def main() -> None:
	ap = argparse.ArgumentParser()
	ap.add_argument("--limit", type=int, default=0, help="cap rows (0 = all 295)")
	ap.add_argument("--model", default=DEFAULT_MODEL)
	args = ap.parse_args()

	api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
	if not api_key:
		sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY in env")
	os.environ["GOOGLE_API_KEY"] = api_key

	align_rows = load_tsv(ALIGN_TSV)
	db_index = build_db_index(load_tsv(CORE_DB_TSV))
	decided = [r for r in align_rows if (r.get("decision") or "").strip()]
	print(f"decided rows total: {len(decided)}")

	# For few-shot, hold out a different slice from what we evaluate on. Simple
	# split: first 8 examples for few-shot, evaluate on the rest.
	few_shot_rows = decided[:8]
	eval_rows = decided[8:]
	if args.limit:
		eval_rows = eval_rows[:args.limit]
	print(f"few-shot examples: {len(few_shot_rows)}  evaluating: {len(eval_rows)}")

	few_shot = pick_few_shot(few_shot_rows, db_index)
	system_prompt = SYSTEM_TEMPLATE.format(
		vocab="|".join(DECISION_VOCAB),
		examples=few_shot or "(none available)",
	)

	client = genai.Client()
	cols = [
		"cluster_id", "canonical_yiddish", "org_type",
		"actual_decision", "actual_aligned_db_id",
		"draft_decision", "draft_aligned_db_id", "confidence", "rationale",
		"decision_match", "db_id_match",
		"drafted_at", "model", "raw_response",
	]
	results: list[dict] = []
	errors = 0
	with OUT_TSV.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
		w.writeheader()
		for i, row in enumerate(eval_rows, 1):
			user_msg = fmt_cluster(row, db_index) + "\n\nReply with strict JSON only."
			try:
				resp = client.models.generate_content(
					model=args.model,
					contents=user_msg,
					config=types.GenerateContentConfig(
						system_instruction=system_prompt,
						max_output_tokens=2048,
						temperature=0.0,
						thinking_config=types.ThinkingConfig(thinking_budget=0),
					),
				)
				text = (resp.text or "").strip()
			except Exception as e:
				print(f"  row {i} ({row.get('cluster_id')}): API error: {e}")
				errors += 1
				text = ""

			data = parse_json_loose(text)
			actual = (row.get("decision") or "").strip()
			actual_db = (row.get("aligned_db_id") or "").strip()
			draft = (data.get("draft_decision") or "").strip()
			draft_db = str(data.get("draft_aligned_db_id") or "").strip()
			conf = (data.get("confidence") or "").strip()
			rec = {
				"cluster_id": row.get("cluster_id", ""),
				"canonical_yiddish": row.get("canonical_yiddish", ""),
				"org_type": row.get("org_type", ""),
				"actual_decision": actual,
				"actual_aligned_db_id": actual_db,
				"draft_decision": draft,
				"draft_aligned_db_id": draft_db,
				"confidence": conf,
				"rationale": (data.get("rationale") or "").strip(),
				"decision_match": "yes" if draft == actual else "no",
				"db_id_match": "yes" if (actual == "ALIGN" and draft == "ALIGN" and draft_db and draft_db == actual_db) else "no" if (actual == "ALIGN" and draft == "ALIGN") else "",
				"drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
				"model": args.model,
				"raw_response": text if not data else "",
			}
			results.append(rec)
			w.writerow(rec)
			f.flush()
			if i % 25 == 0 or i == len(eval_rows):
				print(f"  {i}/{len(eval_rows)} done ({errors} errors)")

	print(f"\nWrote {OUT_TSV.name}")

	# ── Summary metrics ─────────────────────────────────────────────────
	n = len(results)
	by_conf: dict[str, list[dict]] = defaultdict(list)
	by_actual: dict[str, list[dict]] = defaultdict(list)
	confusion: Counter = Counter()
	for r in results:
		if r["confidence"]:
			by_conf[r["confidence"]].append(r)
		by_actual[r["actual_decision"]].append(r)
		confusion[(r["actual_decision"], r["draft_decision"] or "(empty)")] += 1

	def rate(rows: list[dict]) -> str:
		if not rows: return "n/a"
		m = sum(1 for r in rows if r["decision_match"] == "yes")
		return f"{m}/{len(rows)} ({100*m/len(rows):.0f}%)"

	print("\n=== Calibration summary ===")
	print(f"Total evaluated: {n}    API errors: {errors}")
	print(f"Overall decision agreement: {rate(results)}")
	print("\nAgreement by draft confidence:")
	for c in ("high", "medium", "low", ""):
		k = c or "(empty)"
		print(f"  {k:>10}: {rate(by_conf.get(c, []))}")
	print("\nAgreement by actual decision:")
	for d in DECISION_VOCAB:
		print(f"  {d:>10}: {rate(by_actual.get(d, []))}")

	align_actual = [r for r in results if r["actual_decision"] == "ALIGN" and r["draft_decision"] == "ALIGN"]
	if align_actual:
		dbm = sum(1 for r in align_actual if r["db_id_match"] == "yes")
		print(f"\nWhen both call ALIGN, draft_aligned_db_id matches RA's: "
			f"{dbm}/{len(align_actual)} ({100*dbm/len(align_actual):.0f}%)")

	# Write summary file
	with SUMMARY_TSV.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["metric", "subset", "matches", "total", "pct"])
		def write_row(metric: str, subset: str, rows: list[dict]) -> None:
			m = sum(1 for r in rows if r["decision_match"] == "yes")
			t = len(rows)
			pct = f"{100*m/t:.1f}" if t else ""
			w.writerow([metric, subset, m, t, pct])
		write_row("overall", "all", results)
		for c in ("high", "medium", "low"):
			write_row("by_confidence", c, by_conf.get(c, []))
		for d in DECISION_VOCAB:
			write_row("by_actual_decision", d, by_actual.get(d, []))
		w.writerow([])
		w.writerow(["confusion_matrix", "", "", "", ""])
		w.writerow(["actual", "drafted", "count", "", ""])
		for (a, d), c in sorted(confusion.items(), key=lambda x: -x[1]):
			w.writerow([a, d, c, "", ""])

	print(f"Wrote {SUMMARY_TSV.name}")


if __name__ == "__main__":
	main()
