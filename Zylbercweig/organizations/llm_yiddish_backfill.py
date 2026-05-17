"""LLM-suggested Yiddish canonical names for Latin-only DB rows.

Reads db_yiddish_variant_backfill.tsv and, for rows where proposed_canonical_yiddish
is empty (the non-trivial cases — multiple aligned clusters or multiple observed
variants), asks Gemini to pick or propose the best canonical Yiddish form.

Output: writes back to the same TSV, filling proposed_canonical_yiddish, rationale,
confidence, drafted_at. Resumable: skips rows that already have a proposal.

Advisory only — PI reviews and commits via pi_decision / pi_canonical_yiddish.

Usage:
    GOOGLE_API_KEY=... .venv/bin/python3 Zylbercweig/organizations/llm_yiddish_backfill.py
"""
from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
BACKFILL_TSV = HERE / "db_yiddish_variant_backfill.tsv"
MODEL = "gemini-3-flash-preview"

SYSTEM = """You select canonical Yiddish-script names for organizations in the \
Zylbercweig Yiddish theatre lexicon. Native-grade reading in Yiddish, Hebrew, and English.

You receive:
- db_name_latin: the DB entry's Latin-script name (authoritative target identity)
- org_type, db_address (context)
- yiddish_variants_observed: Yiddish-script variants harvested from clusters that \
RAs aligned to this DB row

Task: choose the single best Yiddish canonical form. Prefer:
1. The most orthographically standard / fully-pointed variant.
2. The form most consistent with YIVO spelling conventions.
3. The form least likely to be a typo (no broken letters, no stray spaces \
mid-word, no obvious OCR artifacts).

If all observed variants are flawed but agree on the underlying name, propose a \
corrected canonical form and explain.

Reply with strict single-line JSON:
{"proposed_canonical_yiddish": "<chosen Yiddish form>", "confidence": "<high|medium|low>", \
"rationale": "<<=20 words>"}

Confidence rubric:
- high: variants converge on one clear form, or one is clearly the standard.
- medium: variants disagree on spelling but agree on identity; you picked among them.
- low: variants conflict substantively or look like different entities.
"""


def build_prompt(row: dict[str, str]) -> str:
    return json.dumps({
        "db_name_latin": row["db_name_latin"],
        "org_type": row["org_type"],
        "db_address": row["db_address"],
        "yiddish_variants_observed": row["yiddish_variants_observed"].split("|"),
        "aligned_cluster_canonicals": row["aligned_cluster_canonicals"].split("|"),
    }, ensure_ascii=False)


def main() -> None:
    if not os.environ.get("GOOGLE_API_KEY"):
        sys.exit("GOOGLE_API_KEY not set")

    with open(BACKFILL_TSV, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fields = reader.fieldnames
        rows = list(reader)

    todo = [i for i, r in enumerate(rows) if not r["proposed_canonical_yiddish"].strip()]
    print(f"Total rows: {len(rows)} · undrafted: {len(todo)}")
    if not todo:
        print("Nothing to do.")
        return

    client = genai.Client()
    cfg = types.GenerateContentConfig(
        temperature=0.0,
        response_mime_type="application/json",
        system_instruction=SYSTEM,
    )

    for n, idx in enumerate(todo, 1):
        r = rows[idx]
        prompt = build_prompt(r)
        try:
            resp = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=cfg,
            )
            data = json.loads(resp.text or "{}")
            r["proposed_canonical_yiddish"] = data.get("proposed_canonical_yiddish", "").strip()
            r["confidence"] = data.get("confidence", "").strip()
            r["rationale"] = data.get("rationale", "").strip()
            r["drafted_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"[{n}/{len(todo)}] db_id={r['db_id']} ({r['db_name_latin']!r}) → {r['proposed_canonical_yiddish']!r} ({r['confidence']})")
        except Exception as e:
            print(f"[{n}/{len(todo)}] db_id={r['db_id']} FAILED: {e}", file=sys.stderr)
            continue

        # checkpoint every 5 rows
        if n % 5 == 0:
            with open(BACKFILL_TSV, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
                w.writeheader()
                w.writerows(rows)

    with open(BACKFILL_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"Done. Wrote {BACKFILL_TSV}")


if __name__ == "__main__":
    main()
