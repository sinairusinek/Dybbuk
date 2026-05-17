"""Test 2: blinded LLM drafter calibration on held-out RA decisions.

Splits decided rows by reviewed_at:
- pre-2026-05-11 (296 rows): few-shot pool ONLY
- 2026-05-11+ (44 rows):     evaluated, blinded

Reports overall agreement, agreement by confidence and by actual decision,
plus db_id match rate within ALIGN-ALIGN cases.

Usage:
    GOOGLE_API_KEY=... python test_holdout_llm_drafter.py
"""
from __future__ import annotations
import csv, os, sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types

from llm_draft_alignment import (
    ALIGN_TSV, CORE_DB_TSV, DECISION_VOCAB, DEFAULT_MODEL, SYSTEM_TEMPLATE,
    build_db_index, fmt_cluster, load_tsv, parse_json_loose, pick_few_shot,
)

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
OUT_TSV = HERE / "test_holdout_llm_drafter.tsv"
SUMMARY_TSV = HERE / "test_holdout_llm_drafter_summary.tsv"

HOLDOUT_CUTOFF = "2026-05-11T00:00:00Z"


def main() -> None:
    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        sys.exit("set GOOGLE_API_KEY")
    os.environ["GOOGLE_API_KEY"] = api_key

    align_rows = load_tsv(ALIGN_TSV)
    db_index = build_db_index(load_tsv(CORE_DB_TSV))
    decided = [r for r in align_rows if r.get("decision", "").strip()]

    few_shot_pool = [r for r in decided if r.get("reviewed_at", "") < HOLDOUT_CUTOFF or not r.get("reviewed_at", "").strip()]
    holdouts      = [r for r in decided if r.get("reviewed_at", "") >= HOLDOUT_CUTOFF]
    print(f"few-shot pool (pre-{HOLDOUT_CUTOFF}): {len(few_shot_pool)}")
    print(f"held-out (post-{HOLDOUT_CUTOFF}):     {len(holdouts)}")

    few_shot = pick_few_shot(few_shot_pool, db_index)
    system_prompt = SYSTEM_TEMPLATE.format(
        vocab="|".join(DECISION_VOCAB),
        examples=few_shot or "(none available)",
    )
    print(f"system prompt ~{len(system_prompt):,} chars")

    client = genai.Client()
    cols = [
        "cluster_id", "canonical_yiddish", "org_type",
        "actual_decision", "actual_aligned_db_id", "actual_reviewer", "actual_reviewed_at",
        "draft_decision", "draft_aligned_db_id", "confidence", "rationale",
        "decision_match", "db_id_match",
        "model", "raw_response",
    ]
    results: list[dict] = []
    errors = 0
    with OUT_TSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
        w.writeheader()
        for i, row in enumerate(holdouts, 1):
            # Blind the decision fields in the prompt input
            blinded = {**row}
            for k in ("decision", "aligned_db_id", "reviewer_notes", "reviewer_settlement",
                      "reviewer_address", "reviewer", "reviewed_at"):
                blinded[k] = ""
            user_msg = fmt_cluster(blinded, db_index) + "\n\nReply with strict JSON only."
            try:
                resp = client.models.generate_content(
                    model=DEFAULT_MODEL,
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
                print(f"  row {i} ({row['cluster_id']}): API error: {e}")
                errors += 1
                text = ""
            data = parse_json_loose(text)

            actual = (row.get("decision") or "").strip()
            actual_db = (row.get("aligned_db_id") or "").strip()
            draft = (data.get("draft_decision") or "").strip()
            draft_db = str(data.get("draft_aligned_db_id") or "").strip()
            conf = (data.get("confidence") or "").strip()
            rec = {
                "cluster_id": row["cluster_id"],
                "canonical_yiddish": row.get("canonical_yiddish", ""),
                "org_type": row.get("org_type", ""),
                "actual_decision": actual,
                "actual_aligned_db_id": actual_db,
                "actual_reviewer": row.get("reviewer", ""),
                "actual_reviewed_at": row.get("reviewed_at", ""),
                "draft_decision": draft,
                "draft_aligned_db_id": draft_db,
                "confidence": conf,
                "rationale": (data.get("rationale") or "").strip(),
                "decision_match": "yes" if draft == actual else "no",
                "db_id_match": ("yes" if (actual == "ALIGN" and draft == "ALIGN" and draft_db == actual_db)
                                else "no" if (actual == "ALIGN" and draft == "ALIGN") else ""),
                "model": DEFAULT_MODEL,
                "raw_response": text if not data else "",
            }
            results.append(rec)
            w.writerow(rec); f.flush()
            if i % 10 == 0 or i == len(holdouts):
                print(f"  {i}/{len(holdouts)} done ({errors} errors)")

    # Summary
    n = len(results)
    by_conf: dict[str, list] = defaultdict(list)
    by_actual: dict[str, list] = defaultdict(list)
    confusion: Counter = Counter()
    for r in results:
        if r["confidence"]:
            by_conf[r["confidence"]].append(r)
        by_actual[r["actual_decision"]].append(r)
        confusion[(r["actual_decision"], r["draft_decision"] or "(empty)")] += 1

    def rate(rs):
        if not rs: return "n/a"
        m = sum(1 for r in rs if r["decision_match"] == "yes")
        return f"{m}/{len(rs)} ({100*m/len(rs):.0f}%)"

    print("\n=== Held-out calibration ===")
    print(f"Total: {n}    Errors: {errors}")
    print(f"Overall agreement: {rate(results)}")
    print("\nBy draft confidence:")
    for c in ("high", "medium", "low"):
        print(f"  {c:>8}: {rate(by_conf.get(c, []))}")
    print("\nBy actual decision:")
    for d in DECISION_VOCAB:
        if by_actual.get(d):
            print(f"  {d:>10}: {rate(by_actual.get(d, []))}")
    align_pairs = [r for r in results if r["actual_decision"] == "ALIGN" and r["draft_decision"] == "ALIGN"]
    if align_pairs:
        dbm = sum(1 for r in align_pairs if r["db_id_match"] == "yes")
        print(f"\nWhen both call ALIGN, draft_aligned_db_id == actual: {dbm}/{len(align_pairs)} ({100*dbm/len(align_pairs):.0f}%)")
    print("\nConfusion (actual -> drafted):")
    for (a, d), c in sorted(confusion.items(), key=lambda x: -x[1]):
        print(f"  {a:>10} -> {d:<10}  {c}")

    with SUMMARY_TSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["metric", "subset", "matches", "total", "pct"])
        def wr(metric, subset, rs):
            m = sum(1 for r in rs if r["decision_match"] == "yes")
            t = len(rs)
            w.writerow([metric, subset, m, t, f"{100*m/t:.1f}" if t else ""])
        wr("overall", "all", results)
        for c in ("high", "medium", "low"):
            wr("by_confidence", c, by_conf.get(c, []))
        for d in DECISION_VOCAB:
            if by_actual.get(d):
                wr("by_actual_decision", d, by_actual.get(d, []))
        w.writerow([])
        w.writerow(["confusion_matrix", "", "", "", ""])
        w.writerow(["actual", "drafted", "count", "", ""])
        for (a, d), c in sorted(confusion.items(), key=lambda x: -x[1]):
            w.writerow([a, d, c, "", ""])
    print(f"\nWrote {OUT_TSV.name} and {SUMMARY_TSV.name}")


if __name__ == "__main__":
    main()
