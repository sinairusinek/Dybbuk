"""Find candidate cross-volume duplicates among Zylbercweig subject-entries.

Workflow:
  1. Read people_extracted.tsv (3,081 subject-entries).
  2. Block by surname token (last token of normalized heading after comma-flip;
     for single-token headings the whole thing). Empty-surname rows fall into a
     pseudonym block.
  3. Within each block, score every pair using people_similarity.composite_pair_score.
  4. Emit candidates with score >= 0.55 to people_dedup_candidates.tsv, ranked.
  5. Cross-check against the 107 known same-person pairs from
     people_alignment_review.tsv (Duplication Check) — print recall + write a
     gold-evaluation file.

Output: people_dedup_candidates.tsv with one row per candidate pair.

Notes:
  - Same-volume pairs are emitted too (e.g. two vol-5 entries with same surname)
    but flagged via `same_volume=1` so the RA can filter.
  - This is candidate generation only. The drafter + RA pass make the actual
    decision.
"""
from __future__ import annotations
import csv, sys, pathlib
from collections import defaultdict
from itertools import combinations

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))

from people_similarity import (  # type: ignore
    expand_name_variants, person_pair_score,
    date_overlap_score, place_overlap_score, composite_pair_score,
    heading_to_canonical, normalize_person_name, split_name_tokens,
)

PEOPLE_TSV = HERE / "people_extracted.tsv"
REVIEW_TSV = HERE / "people_alignment_review.tsv"
OUT_TSV = HERE / "people_dedup_candidates.tsv"
GOLD_EVAL_TSV = HERE / "people_dedup_gold_eval.tsv"

SCORE_THRESHOLD = 0.55


def blocking_key(heading: str, names_variants: str) -> set[str]:
    """Return a set of blocking keys — surname-like tokens that group entries
    likely to be the same person. Each entry can appear in multiple blocks (e.g.
    married name + maiden name).

    Rules:
      - Heading with a comma → use the part BEFORE the comma (the surname).
      - Heading without a comma → use ALL tokens (we can't tell surname from
        given name reliably for these — better to over-block than miss).
      - For each `names_variants` entry, add all tokens (variants are noisy and
        may be surname-first or given-first).
    """
    keys: set[str] = set()
    import re as _re
    # bracketed aliases in the heading get their tokens added too
    for m in _re.finditer(r"[\[\(]([^\]\)]+)[\]\)]", heading or ""):
        for t in split_name_tokens(m.group(1)):
            if len(t) >= 2:
                keys.add(t)
    # heading
    h_stripped = (heading or "").split("[")[0].split("(")[0].strip()
    if "," in h_stripped:
        surname_part = h_stripped.split(",", 1)[0]
        for t in split_name_tokens(surname_part):
            keys.add(t)
    else:
        for t in split_name_tokens(h_stripped):
            keys.add(t)
    # names_variants
    for v in (names_variants or "").split("|"):
        v = v.strip()
        if not v:
            continue
        for t in split_name_tokens(v):
            if len(t) >= 2:  # skip single-letter initials
                keys.add(t)
    keys.discard("")
    # drop very-short tokens (initials and stop-letters)
    return {k for k in keys if len(k) >= 2}


def load_people():
    with open(PEOPLE_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_gold_pairs():
    """Return set of frozenset({xml_id_a, xml_id_b}) pairs known to be same-person."""
    if not REVIEW_TSV.exists():
        return set()
    by_db = defaultdict(list)
    with open(REVIEW_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["source_sheet"] != "Duplication Check":
                continue
            sp = (r["same_person"] or "").lower()
            if sp not in ("same", "sameperson"):  # accept "same", "Same", "SAME"
                # treat affirmative judgments only
                if not sp.startswith("same"):
                    continue
            if r["db_id"] and r["xml_id"]:
                by_db[r["db_id"]].append(r["xml_id"])
    pairs = set()
    for ids in by_db.values():
        if len(ids) >= 2:
            for a, b in combinations(sorted(set(ids)), 2):
                pairs.add(frozenset((a, b)))
    return pairs


def main():
    people = load_people()
    # build blocking index
    blocks: dict[str, list[dict]] = defaultdict(list)
    for p in people:
        for k in blocking_key(p["heading"], p.get("names_variants", "")):
            blocks[k].append(p)

    # precompute variants per person
    variants_cache: dict[str, set[str]] = {}
    for p in people:
        variants_cache[p["person_id"]] = expand_name_variants(
            p["heading"], p.get("names_variants", ""), p.get("subheading", "")
        )

    # score pairs, dedupe across blocks
    seen: dict[frozenset, dict] = {}
    for k, items in blocks.items():
        if len(items) < 2 or len(items) > 200:
            # skip absurd buckets (e.g. empty key, single-letter)
            if len(items) > 200:
                print(f"[skip large block] key={k!r} size={len(items)}")
            continue
        for a, b in combinations(items, 2):
            key = frozenset((a["person_id"], b["person_id"]))
            if key in seen:
                continue
            va = variants_cache[a["person_id"]]
            vb = variants_cache[b["person_id"]]
            ns, _, _ = person_pair_score(va, vb)
            if ns < 0.35:
                continue
            ds = date_overlap_score(
                a["birth_date"], a["death_date"], b["birth_date"], b["death_date"]
            )
            ps = place_overlap_score(
                [a["birth_place_name"], a["death_place_name"], a["birth_place_province"], a["birth_place_country"]],
                [b["birth_place_name"], b["death_place_name"], b["birth_place_province"], b["birth_place_country"]],
            )
            same_type = bool(a["entry_type"]) and a["entry_type"] == b["entry_type"]
            score = composite_pair_score(ns, ds, ps, same_type)
            if score < SCORE_THRESHOLD:
                continue
            seen[key] = {
                "a_person_id": a["person_id"],
                "b_person_id": b["person_id"],
                "a_volume": a["volume"],
                "b_volume": b["volume"],
                "a_heading": a["heading"],
                "b_heading": b["heading"],
                "a_xml_id": a["xml_id"],
                "b_xml_id": b["xml_id"],
                "a_birth": a["birth_date"],
                "b_birth": b["birth_date"],
                "a_death": a["death_date"],
                "b_death": b["death_date"],
                "a_entry_type": a["entry_type"],
                "b_entry_type": b["entry_type"],
                "name_score": round(ns, 3),
                "date_score": round(ds, 3),
                "place_score": round(ps, 3),
                "same_entry_type": int(same_type),
                "same_volume": int(a["volume"] == b["volume"]),
                "composite_score": round(score, 3),
                "blocking_key": k,
            }

    rows = sorted(seen.values(), key=lambda r: -r["composite_score"])
    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        if rows:
            w = csv.DictWriter(fp, fieldnames=list(rows[0].keys()), delimiter="\t")
            w.writeheader()
            w.writerows(rows)
    print(f"wrote {OUT_TSV} ({len(rows)} candidate pairs)")
    n_cross_vol = sum(1 for r in rows if not r["same_volume"])
    n_same_vol = sum(1 for r in rows if r["same_volume"])
    print(f"  cross-volume: {n_cross_vol}    same-volume: {n_same_vol}")
    print(f"  high-confidence (>=0.85): {sum(1 for r in rows if r['composite_score']>=0.85)}")

    # gold evaluation
    gold = load_gold_pairs()
    if not gold:
        print("no gold pairs available — skipping recall eval")
        return
    cand_pairs = {frozenset((r["a_xml_id"], r["b_xml_id"])) for r in rows}
    tp = gold & cand_pairs
    missed = gold - cand_pairs
    print(f"\nGold pairs (from Duplication Check, db_id-grouped same-person): {len(gold)}")
    print(f"  recovered by candidate pass: {len(tp)} ({100*len(tp)/max(1,len(gold)):.1f}%)")
    print(f"  missed:                       {len(missed)}")

    # write gold eval
    gold_idx = {r["xml_id"]: r for r in people}
    with open(GOLD_EVAL_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp, delimiter="\t")
        w.writerow(["status", "xml_a", "xml_b", "head_a", "head_b", "candidate_score"])
        cand_by_pair = {frozenset((r["a_xml_id"], r["b_xml_id"])): r["composite_score"] for r in rows}
        for pair in sorted(gold):
            xa, xb = sorted(pair)
            ra = gold_idx.get(xa, {})
            rb = gold_idx.get(xb, {})
            sc = cand_by_pair.get(pair, "")
            status = "RECOVERED" if pair in tp else "MISSED"
            w.writerow([status, xa, xb, ra.get("heading", ""), rb.get("heading", ""), sc])
    print(f"wrote {GOLD_EVAL_TSV}")


if __name__ == "__main__":
    main()
