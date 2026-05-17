"""LLM-drafted organization alignment decisions.

Reads undecided rows from org_alignment_review.tsv and asks Gemini 3 Flash to
draft a decision per cluster (ALIGN/NEW/SPLIT/DISCUSS/UNCLUSTER/DEFER/GENERIC),
grounded in the cluster's extracted fields and the top-5 candidate DB rows.

Output: org_alignment_drafts.tsv — advisory only; never touches the master
alignment review TSV. Resumable: rows already drafted are skipped.

Usage:
    GOOGLE_API_KEY=... python llm_draft_alignment.py --limit 50 --stratified
    GOOGLE_API_KEY=... python llm_draft_alignment.py            # full undecided
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
ALIGN_TSV = HERE / "org_alignment_review.tsv"
CORE_DB_TSV = HERE / "core_db.tsv"
OUT_TSV = HERE / "org_alignment_drafts.tsv"

DEFAULT_MODEL = "gemini-3-flash-preview"
DECISION_VOCAB = ["ALIGN", "NEW", "SPLIT", "DISCUSS", "UNCLUSTER", "DEFER", "GENERIC"]

SYSTEM_TEMPLATE = """You draft alignment decisions for the Zylbercweig Yiddish theatre lexicon \
organization-matching pipeline. Native-grade reading in Yiddish, Hebrew, and English.

For each cluster you receive:
- canonical_yiddish: cluster's representative name
- org_type: classified type (e.g. Theatre, Publisher, Education)
- name_variants: all surface forms in the cluster
- extracted_settlements / addresses / venues / countries
- cluster_size: number of source mentions in the cluster
- top-5 candidate DB rows (with db_id, name, org_type, address) ranked by the \
matching pipeline (trigram + Daitch-Mokotoff + IPA cross-script phonetic)
- candidate_scores: similarity scores 0..1 produced by the pipeline
- candidate_methods: which signals contributed (exact/phonetic/fuzzy/ipa_phonetic)

Decide which of these decisions applies and reply with strict single-line JSON:

{{"draft_decision": "<one of: {vocab}>", "draft_aligned_db_id": "<db_id or empty>", \
"confidence": "<high|medium|low>", "rationale": "<<=25 words citing the signals>"}}

Decision vocabulary:
- ALIGN: cluster matches one of the candidate DB rows; fill draft_aligned_db_id. \
Require strong name agreement AND place/type agreement (or no contradiction) to \
mark confidence=high.
- NEW: cluster is a real organization not present in DB; no candidate is a true match.
- SPLIT: cluster mixes multiple distinct organizations and must be broken up before alignment.
- DISCUSS: genuinely ambiguous; needs PI judgment.
- UNCLUSTER: cluster is artifactual — the rows shouldn't have been grouped at all.
- DEFER: insufficient information; come back later.
- GENERIC: name is a generic term ("the theatre", "the school") not a specific entity.

Confidence rubric:
- high: signals converge — name + place + type all agree (or for NEW: no candidate \
within plausible range AND name+place are consistent within the cluster).
- medium: one signal disagrees or is missing but the call is still reasonable.
- low: signals conflict, name is sparse, or the candidate list looks weak.

Below are several real decided examples in this dataset to anchor your style. \
Match their reasoning approach.

=========== EXAMPLES ===========
{examples}
=========== END EXAMPLES ===========
"""


def load_tsv(path: Path) -> list[dict]:
	with path.open(newline="", encoding="utf-8") as f:
		return list(csv.DictReader(f, delimiter="\t"))


def build_db_index(core_rows: list[dict]) -> dict[str, dict]:
	return {r["db_id"]: r for r in core_rows if r.get("db_id")}


def fmt_candidates(row: dict, db_index: dict[str, dict]) -> str:
	ids = [x.strip() for x in (row.get("candidate_db_ids") or "").split("|") if x.strip()]
	scores = [x.strip() for x in (row.get("candidate_scores") or "").split("|")]
	methods = [x.strip() for x in (row.get("candidate_methods") or "").split("|")]
	if not ids:
		return "(no candidates)"
	lines = []
	for i, db_id in enumerate(ids[:5]):
		db = db_index.get(db_id, {})
		score = scores[i] if i < len(scores) else ""
		method = methods[i] if i < len(methods) else ""
		lines.append(
			f"  - db_id={db_id} score={score} method={method} "
			f"name={db.get('name', '?')} type={db.get('org_type', '?')} "
			f"address={db.get('address', '')}"
		)
	return "\n".join(lines)


def fmt_cluster(row: dict, db_index: dict[str, dict]) -> str:
	return (
		f"cluster_id: {row.get('cluster_id', '')}\n"
		f"canonical_yiddish: {row.get('canonical_yiddish', '')}\n"
		f"org_type: {row.get('org_type', '')}\n"
		f"cluster_size: {row.get('cluster_size', '')}\n"
		f"name_variants: {row.get('name_variants', '')}\n"
		f"extracted_settlements: {row.get('extracted_settlements', '')}\n"
		f"extracted_addresses: {row.get('extracted_addresses', '')}\n"
		f"extracted_venues: {row.get('extracted_venues', '')}\n"
		f"extracted_countries: {row.get('extracted_countries', '')}\n"
		f"candidates:\n{fmt_candidates(row, db_index)}"
	)


def pick_few_shot(decided: list[dict], db_index: dict[str, dict], max_per_type: int = 2) -> str:
	by_decision: dict[str, list[dict]] = defaultdict(list)
	for r in decided:
		d = (r.get("decision") or "").strip()
		if d in DECISION_VOCAB:
			by_decision[d].append(r)
	picks: list[dict] = []
	for d in DECISION_VOCAB:
		picks.extend(by_decision.get(d, [])[:max_per_type])
	# Cap total to keep prompt manageable.
	picks = picks[:8]
	out = []
	for r in picks:
		ex_json = json.dumps({
			"draft_decision": r.get("decision", ""),
			"draft_aligned_db_id": r.get("aligned_db_id", ""),
			"confidence": "high",
			"rationale": (r.get("reviewer_notes") or "decided by RA")[:120],
		}, ensure_ascii=False)
		out.append(f"INPUT:\n{fmt_cluster(r, db_index)}\nOUTPUT: {ex_json}")
	return "\n\n".join(out)


def stratum(row: dict) -> str:
	ids = [x for x in (row.get("candidate_db_ids") or "").split("|") if x.strip()]
	if not ids:
		return "no_candidates"
	scores = [x.strip() for x in (row.get("candidate_scores") or "").split("|") if x.strip()]
	try:
		top = max(float(s) for s in scores) if scores else 0.0
	except ValueError:
		top = 0.0
	if top >= 0.8:
		return "high_score"
	return "low_score"


def stratified_sample(rows: list[dict], n: int, seed: int = 20260512) -> list[dict]:
	random.seed(seed)
	buckets: dict[str, list[dict]] = defaultdict(list)
	for r in rows:
		buckets[stratum(r)].append(r)
	per = max(1, n // 3)
	picks: list[dict] = []
	for k in ("high_score", "low_score", "no_candidates"):
		b = buckets.get(k, [])
		random.shuffle(b)
		picks.extend(b[:per])
	return picks[:n]


def parse_json_loose(text: str) -> dict:
	t = text.strip()
	if t.startswith("```"):
		lines = t.split("\n")
		t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
	i, j = t.find("{"), t.rfind("}")
	if i >= 0 and j > i:
		t = t[i:j + 1]
	try:
		return json.loads(t)
	except Exception:
		return {}


def main() -> None:
	ap = argparse.ArgumentParser()
	ap.add_argument("--limit", type=int, default=0, help="cap rows processed (0 = no cap)")
	ap.add_argument("--stratified", action="store_true",
		help="sample limit rows stratified by candidate quality (else: first N undecided)")
	ap.add_argument("--model", default=DEFAULT_MODEL)
	ap.add_argument("--out", default=str(OUT_TSV))
	args = ap.parse_args()

	api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
	if not api_key:
		sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY in env")
	os.environ["GOOGLE_API_KEY"] = api_key

	align_rows = load_tsv(ALIGN_TSV)
	db_index = build_db_index(load_tsv(CORE_DB_TSV))

	decided = [r for r in align_rows if (r.get("decision") or "").strip()]
	undecided = [r for r in align_rows if not (r.get("decision") or "").strip()]

	out_path = Path(args.out)
	done_ids: set[str] = set()
	if out_path.exists():
		with out_path.open(newline="", encoding="utf-8") as f:
			done_ids = {r["cluster_id"] for r in csv.DictReader(f, delimiter="\t")}
	work = [r for r in undecided if r.get("cluster_id") not in done_ids]

	if args.limit:
		work = stratified_sample(work, args.limit) if args.stratified else work[:args.limit]

	print(f"decided rows (used for few-shot): {len(decided)}")
	print(f"undecided total: {len(undecided)}  already drafted: {len(done_ids)}  to do: {len(work)}")
	print(f"model: {args.model}")
	if not work:
		print("nothing to do.")
		return

	few_shot = pick_few_shot(decided, db_index)
	system_prompt = SYSTEM_TEMPLATE.format(
		vocab="|".join(DECISION_VOCAB),
		examples=few_shot or "(none available)",
	)
	print(f"system prompt: ~{len(system_prompt):,} chars  few-shot examples: "
		f"{few_shot.count('OUTPUT:')}")

	client = genai.Client()
	cols = [
		"cluster_id", "canonical_yiddish", "org_type", "sampling_stratum",
		"top_candidate_db_id", "top_candidate_score", "n_candidates",
		"draft_decision", "draft_aligned_db_id", "confidence", "rationale",
		"drafted_at", "model", "raw_response",
	]
	mode = "a" if out_path.exists() else "w"
	with out_path.open(mode, newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
		if mode == "w":
			w.writeheader()
		errors = 0
		for i, row in enumerate(work, 1):
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
			ids = [x for x in (row.get("candidate_db_ids") or "").split("|") if x.strip()]
			scores = [x.strip() for x in (row.get("candidate_scores") or "").split("|")]
			w.writerow({
				"cluster_id": row.get("cluster_id", ""),
				"canonical_yiddish": row.get("canonical_yiddish", ""),
				"org_type": row.get("org_type", ""),
				"sampling_stratum": stratum(row),
				"top_candidate_db_id": ids[0] if ids else "",
				"top_candidate_score": scores[0] if scores else "",
				"n_candidates": len(ids),
				"draft_decision": (data.get("draft_decision") or "").strip(),
				"draft_aligned_db_id": str(data.get("draft_aligned_db_id") or "").strip(),
				"confidence": (data.get("confidence") or "").strip(),
				"rationale": (data.get("rationale") or "").strip(),
				"drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
				"model": args.model,
				"raw_response": text if not data else "",
			})
			f.flush()
			if i % 10 == 0 or i == len(work):
				print(f"  {i}/{len(work)} drafted ({errors} errors)")

	print(f"\nWrote {out_path}")
	print(f"API errors: {errors}")


if __name__ == "__main__":
	main()
