"""Test 1: candidate-recall on RA decisions made after 2026-05-11 (held-out set).

For ALIGN holdouts, measures whether the RA's chosen aligned_db_id was present
in the candidate list. Reports recall and breaks down by org_type.

Compares two states:
- BEFORE: the pre-canonical-types candidate set (would need the pre_phase25_backup)
- AFTER:  the current post-canonical-types candidate set (live file)
"""
from __future__ import annotations
import csv, sys
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent

HOLDOUT_CUTOFF = "2026-05-11T00:00:00Z"

LIVE = HERE / "org_alignment_review.tsv"


def load(p: Path) -> list[dict]:
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    rows = load(LIVE)
    holdouts = [r for r in rows if r.get("decision","").strip() and r.get("reviewed_at","") >= HOLDOUT_CUTOFF]
    print(f"Held-out decisions (reviewed_at >= {HOLDOUT_CUTOFF}): {len(holdouts)}")

    by_dec = Counter(r["decision"] for r in holdouts)
    print(f"Decision distribution: {dict(by_dec)}")

    # Focus on ALIGN cases — these have a db_id we can check
    aligns = [r for r in holdouts if r["decision"] == "ALIGN" and r.get("aligned_db_id","").strip()]
    print(f"\nALIGN holdouts with an aligned_db_id: {len(aligns)}")
    if not aligns:
        return

    in_top_n = Counter()
    by_type = defaultdict(lambda: [0, 0])  # [in_candidates, total]
    misses = []
    for r in aligns:
        actual = r["aligned_db_id"].strip()
        cands = [x.strip() for x in (r.get("candidate_db_ids") or "").split("|") if x.strip()]
        otype = r["org_type"]
        by_type[otype][1] += 1
        if not cands:
            in_top_n["no_candidates"] += 1
            misses.append((r["cluster_id"], r["canonical_yiddish"][:30], actual, otype, "no candidates"))
        elif actual not in cands:
            in_top_n["miss"] += 1
            misses.append((r["cluster_id"], r["canonical_yiddish"][:30], actual, otype, f"top={cands[0]}"))
        else:
            rank = cands.index(actual) + 1
            in_top_n[f"rank_{rank}"] += 1
            by_type[otype][0] += 1

    total = len(aligns)
    hits = sum(v for k, v in in_top_n.items() if k.startswith("rank_"))
    print(f"\nRecall@5: {hits}/{total} ({100*hits/total:.0f}%)")
    print("Rank distribution:")
    for k in sorted(in_top_n.keys()):
        print(f"  {k}: {in_top_n[k]}")

    print("\nBy org_type:")
    for t, (hit, tot) in sorted(by_type.items(), key=lambda x: -x[1][1]):
        pct = f"{100*hit/tot:.0f}%" if tot else "-"
        print(f"  {t}: {hit}/{tot} ({pct})")

    print(f"\nMisses ({len(misses)}):")
    for cid, name, actual_db, otype, reason in misses:
        print(f"  {cid:>12}  {otype:>30}  actual_db={actual_db:>4}  {reason:>20}  {name}")


if __name__ == "__main__":
    main()
