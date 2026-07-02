"""Find candidate cross-volume duplicates among Zylbercweig subject-entries.

Workflow:
  1. Read people_extracted.tsv (3,081 subject-entries).
  2. Block by surname token (last token of normalized heading after comma-flip;
     for single-token headings the whole thing). Empty-surname rows fall into a
     pseudonym block.
  3. Within each block, score every pair using people_similarity.composite_pair_score.
  4. Emit candidates with score >= 0.55 to people_dedup_candidates.tsv, ranked.
  5. Cross-check against the 107 known same-person pairs from
     people_alignment_review.tsv (Duplication Check) -- print recall + write a
     gold-evaluation file.

Phase A gates:
  - Alias hygiene: single-token aliases matching common given names are suppressed.
  - Gender gate: pairs with differing non-empty genders are dropped.
  - DB-contradiction gate: pairs where both sides have different db_ids are
    auto-closed to dedup_auto_closed.tsv.
  - Date-contradiction gate: pairs with >3yr birth or death disagreement dropped.
  - Surname gate: pairs with no surname-token overlap score 0 (via require_surname_overlap).
  - Real ranking + bands (high/review/low) + source tracking (heading/alias).

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
    expand_name_variants, person_pair_score, extract_surname_tokens,
    date_overlap_score, place_overlap_score, composite_pair_score,
    heading_to_canonical, normalize_person_name, split_name_tokens,
    extract_year,
)

PEOPLE_TSV = HERE / "people_extracted.tsv"
REVIEW_TSV = HERE / "people_alignment_review.tsv"
ALIASES_TSV = HERE / "people_aliases.tsv"
DB_TSV = HERE / "people_db.tsv"
OUT_TSV = HERE / "people_dedup_candidates.tsv"
GOLD_EVAL_TSV = HERE / "people_dedup_gold_eval.tsv"
AUTO_CLOSED_TSV = HERE / "dedup_auto_closed.tsv"
SUPPRESSED_TSV = HERE / "aliases_suppressed_phaseA.tsv"

SCORE_THRESHOLD = 0.55


def blocking_key_heading(heading: str, names_variants: str) -> set[str]:
    """Return blocking keys from heading + names_variants (no alias tokens)."""
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
            if len(t) >= 2:
                keys.add(t)
    keys.discard("")
    return {k for k in keys if len(k) >= 2}


def load_people():
    with open(PEOPLE_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def build_given_name_counts(people: list[dict]) -> dict[str, int]:
    """Frequency of each normalized token appearing in the given-part of headings
    (part AFTER comma; if no comma, all tokens). Used to suppress common given
    names from alias blocking keys."""
    from collections import Counter
    counts: Counter = Counter()
    for r in people:
        h = r["heading"]
        if "," in h:
            given_part = h.split(",", 1)[1].strip()
        else:
            given_part = h
        for t in split_name_tokens(given_part):
            counts[normalize_person_name(t)] += 1
    return dict(counts)


def load_db_id_by_xml_id() -> dict[str, str]:
    """xml_id -> aligned db_id (from carried RA decisions). One xml_id can
    appear in multiple review rows; we take the first non-empty db_id."""
    if not REVIEW_TSV.exists():
        return {}
    out: dict[str, str] = {}
    with open(REVIEW_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            xml = (r.get("xml_id") or "").strip()
            did = (r.get("db_id") or "").strip()
            if xml and did and xml not in out:
                out[xml] = did
    return out


def load_gender_by_db_id() -> dict[str, str]:
    if not DB_TSV.exists():
        return {}
    out: dict[str, str] = {}
    with open(DB_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            g = (r.get("gender") or "").strip()
            if g:
                out[r["db_id"]] = g
    return out


def load_aliases(given_name_counts: dict[str, int]) -> tuple[dict[str, list[str]], list[dict]]:
    """person_id -> list of alias surface forms (after suppression of common given names).
    Also returns suppressed rows for logging.
    Suppression rule: single-token alias whose normalized token has given_name_count >= 3.
    """
    if not ALIASES_TSV.exists():
        return {}, []
    out: dict[str, list[str]] = defaultdict(list)
    suppressed: list[dict] = []
    with open(ALIASES_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            pid = r.get("person_id")
            alias = (r.get("alias_form") or "").strip()
            if not (pid and alias):
                continue
            toks = split_name_tokens(alias)
            if len(toks) == 1:
                tok = normalize_person_name(toks[0])
                cnt = given_name_counts.get(tok, 0)
                if cnt >= 3:
                    suppressed.append({
                        "person_id": pid,
                        "alias": alias,
                        "reason": "common_given_name",
                        "count": cnt,
                    })
                    continue  # suppress from variant sets and blocking keys
            out[pid].append(alias)
    return out, suppressed


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
            if sp not in ("same", "sameperson"):
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


def score_band(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.65:
        return "review"
    return "low"


def main():
    people = load_people()
    given_name_counts = build_given_name_counts(people)
    aliases, suppressed_aliases = load_aliases(given_name_counts)
    db_id_by_xml = load_db_id_by_xml_id()
    gender_by_db = load_gender_by_db_id()
    gender_by_xml = {x: gender_by_db[d] for x, d in db_id_by_xml.items() if d in gender_by_db}

    print(f"  db_id resolved for {len(db_id_by_xml)} xml_ids "
          f"({100*len(db_id_by_xml)/max(1,len(people)):.0f}% of subject entries)")
    print(f"  gender resolved for {len(gender_by_xml)} xml_ids")
    print(f"  aliases suppressed (common given names): {len(suppressed_aliases)}")

    # Write suppressed aliases log
    with open(SUPPRESSED_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=["person_id", "alias", "reason", "count"], delimiter="\t")
        w.writeheader()
        w.writerows(suppressed_aliases)
    print(f"  wrote {SUPPRESSED_TSV}")

    # Build TWO separate blocking indexes:
    #   blocks_heading: from heading + names_variants (no alias tokens)
    #   blocks_alias: from filtered aliases only
    blocks_heading: dict[str, list[dict]] = defaultdict(list)
    blocks_alias: dict[str, list[dict]] = defaultdict(list)

    for p in people:
        # heading-based keys (heading + names_variants, no alias tokens)
        hkeys = blocking_key_heading(p["heading"], p.get("names_variants", ""))
        for k in hkeys:
            blocks_heading[k].append(p)

        # alias-based keys (only from filtered/surviving aliases)
        for alias in aliases.get(p["person_id"], ()):
            for t in split_name_tokens(alias):
                if len(t) >= 2:
                    blocks_alias[t].append(p)

    # precompute variants per person
    # full variants: heading variants + surviving aliases
    full_variants_cache: dict[str, set[str]] = {}
    for p in people:
        v_full = expand_name_variants(
            p["heading"], p.get("names_variants", ""), p.get("subheading", "")
        )
        for alias in aliases.get(p["person_id"], ()):
            v_full.add(alias)
        full_variants_cache[p["person_id"]] = v_full

    # people_by_id for fast lookup when scoring
    people_by_id: dict[str, dict] = {p["person_id"]: p for p in people}

    def score_pair(a: dict, b: dict) -> dict | None:
        va = full_variants_cache[a["person_id"]]
        vb = full_variants_cache[b["person_id"]]
        ns, _, _ = person_pair_score(va, vb, require_surname_overlap=True)
        if ns < 0.35:
            return None
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
            return None
        a_db = db_id_by_xml.get(a["xml_id"], "")
        b_db = db_id_by_xml.get(b["xml_id"], "")
        return {
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
            "blocking_key": "",  # filled in deterministically below
            "a_gender": gender_by_xml.get(a["xml_id"], ""),
            "b_gender": gender_by_xml.get(b["xml_id"], ""),
            "a_db_id": a_db,
            "b_db_id": b_db,
            "db_agreement": int(bool(a_db and b_db and a_db == b_db)),
        }

    # Score pairs, track which pairs come from heading vs alias blocks.
    # Also track all blocking keys per pair -> pick min() for determinism.
    seen: dict[frozenset, dict] = {}
    heading_triggered: set[frozenset] = set()
    all_blocking_keys: dict[frozenset, list[str]] = defaultdict(list)

    # Process heading blocks first
    for k in sorted(blocks_heading.keys()):
        items = blocks_heading[k]
        if len(items) < 2 or len(items) > 200:
            if len(items) > 200:
                print(f"[skip large block] key={k!r} size={len(items)}")
            continue
        for a, b in combinations(items, 2):
            pair_key = frozenset((a["person_id"], b["person_id"]))
            heading_triggered.add(pair_key)
            all_blocking_keys[pair_key].append(k)
            if pair_key in seen:
                continue
            row = score_pair(a, b)
            if row is not None:
                seen[pair_key] = row

    # Process alias blocks (may add new pairs not seen via heading blocks)
    for k in sorted(blocks_alias.keys()):
        items = blocks_alias[k]
        if len(items) < 2 or len(items) > 200:
            if len(items) > 200:
                print(f"[skip large alias block] key={k!r} size={len(items)}")
            continue
        for a, b in combinations(items, 2):
            pair_key = frozenset((a["person_id"], b["person_id"]))
            all_blocking_keys[pair_key].append(k)
            if pair_key in seen:
                continue  # already scored (heading or earlier alias block)
            if pair_key in heading_triggered:
                continue  # was considered via heading but didn't pass threshold
            row = score_pair(a, b)
            if row is not None:
                seen[pair_key] = row
                # pair_key not in heading_triggered -> source=alias

    # Assign source and deterministic blocking_key columns
    for pair_key, row in seen.items():
        row["source"] = "heading" if pair_key in heading_triggered else "alias"
        # Pick lexicographically smallest blocking key for determinism
        bkeys = all_blocking_keys.get(pair_key, [])
        row["blocking_key"] = min(bkeys) if bkeys else ""

    # Apply gates (after scoring)
    n_gender_dropped = 0
    n_date_dropped = 0
    n_db_contradiction = 0
    auto_closed_rows: list[dict] = []

    filtered: dict[frozenset, dict] = {}
    for pair_key, row in seen.items():
        # Gate 2: gender gate
        ag = row["a_gender"]
        bg = row["b_gender"]
        if ag and bg and ag != bg:
            n_gender_dropped += 1
            continue

        # Gate 3: DB-contradiction gate
        adb = row["a_db_id"]
        bdb = row["b_db_id"]
        if adb and bdb and adb != bdb:
            n_db_contradiction += 1
            auto_closed_rows.append({
                "a_xml_id": row["a_xml_id"],
                "b_xml_id": row["b_xml_id"],
                "verdict": "different",
                "reason": "db_contradiction",
                "a_db_id": adb,
                "b_db_id": bdb,
                "a_heading": row["a_heading"],
                "b_heading": row["b_heading"],
            })
            continue

        # Gate 4: date-contradiction gate
        # Skip if both sides share the same db_id (db_agreement=1): the date
        # discrepancy is then a data-entry error, not evidence of different people.
        if not row["db_agreement"]:
            ay = extract_year(row["a_birth"])
            by_ = extract_year(row["b_birth"])
            ad = extract_year(row["a_death"])
            bd = extract_year(row["b_death"])
            if ay and by_ and abs(ay - by_) > 3:
                n_date_dropped += 1
                continue
            if ad and bd and abs(ad - bd) > 3:
                n_date_dropped += 1
                continue

        filtered[pair_key] = row

    # Write dedup_auto_closed.tsv (always with header, even if empty)
    ac_fields = ["a_xml_id", "b_xml_id", "verdict", "reason", "a_db_id", "b_db_id", "a_heading", "b_heading"]
    with open(AUTO_CLOSED_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=ac_fields, delimiter="\t")
        w.writeheader()
        w.writerows(auto_closed_rows)
    print(f"  wrote {AUTO_CLOSED_TSV} ({len(auto_closed_rows)} db-contradiction pairs)")

    # Add band column
    for row in filtered.values():
        row["band"] = score_band(row["composite_score"])

    # Sort by composite score descending
    rows = sorted(filtered.values(), key=lambda r: -r["composite_score"])

    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        if rows:
            w = csv.DictWriter(fp, fieldnames=list(rows[0].keys()), delimiter="\t")
            w.writeheader()
            w.writerows(rows)
    print(f"wrote {OUT_TSV} ({len(rows)} candidate pairs)")
    n_cross_vol = sum(1 for r in rows if not r["same_volume"])
    n_same_vol = sum(1 for r in rows if r["same_volume"])
    print(f"  cross-volume: {n_cross_vol}    same-volume: {n_same_vol}")

    # Gate summary
    print(f"\n--- Gate summary ---")
    print(f"  pairs pre-gate (scored):          {len(seen)}")
    print(f"  gender gate dropped:              {n_gender_dropped}")
    print(f"  db-contradiction dropped:         {n_db_contradiction}")
    print(f"  date-contradiction dropped:       {n_date_dropped}")
    print(f"  pairs post-gate (emitted):        {len(rows)}")
    print(f"  aliases suppressed:               {len(suppressed_aliases)}")

    # Band breakdown
    bands = {"high": 0, "review": 0, "low": 0}
    for r in rows:
        bands[r["band"]] += 1
    print(f"\n--- Band breakdown ---")
    for band, cnt in bands.items():
        print(f"  {band}: {cnt}")

    # Source breakdown
    n_heading_src = sum(1 for r in rows if r["source"] == "heading")
    n_alias_src = sum(1 for r in rows if r["source"] == "alias")
    print(f"\n--- Source breakdown ---")
    print(f"  heading: {n_heading_src}  alias: {n_alias_src}")

    # Regenerate pseudonym_candidates_review.tsv from alias-sourced pairs
    # with real scores (replaces old fixed-0.7 file)
    PSEUDO_TSV = HERE / "pseudonym_candidates_review.tsv"
    alias_rows_out = [r for r in rows if r["source"] == "alias"]

    # Build aliases display string per person (from surviving aliases)
    aliases_display: dict[str, str] = {}
    for pid, alias_list in aliases.items():
        aliases_display[pid] = " | ".join(alias_list)

    pseudo_fields = [
        "decision", "composite", "name_score", "band",
        "a_heading", "b_heading", "a_aliases", "b_aliases",
        "a_xml_id", "b_xml_id", "a_volume", "b_volume", "same_volume",
        "a_gender", "b_gender", "a_db_id", "b_db_id",
    ]
    with open(PSEUDO_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=pseudo_fields, delimiter="\t")
        w.writeheader()
        for r in alias_rows_out:
            w.writerow({
                "decision": "",
                "composite": r["composite_score"],
                "name_score": r["name_score"],
                "band": r["band"],
                "a_heading": r["a_heading"],
                "b_heading": r["b_heading"],
                "a_aliases": aliases_display.get(r["a_person_id"], ""),
                "b_aliases": aliases_display.get(r["b_person_id"], ""),
                "a_xml_id": r["a_xml_id"],
                "b_xml_id": r["b_xml_id"],
                "a_volume": r["a_volume"],
                "b_volume": r["b_volume"],
                "same_volume": r["same_volume"],
                "a_gender": r["a_gender"],
                "b_gender": r["b_gender"],
                "a_db_id": r["a_db_id"],
                "b_db_id": r["b_db_id"],
            })
    print(f"\n  wrote {PSEUDO_TSV} ({len(alias_rows_out)} alias-sourced pairs with real scores)")

    # Gold evaluation
    gold = load_gold_pairs()
    if not gold:
        print("no gold pairs available -- skipping recall eval")
        return
    cand_pairs = {frozenset((r["a_xml_id"], r["b_xml_id"])) for r in rows}
    tp = gold & cand_pairs
    missed = gold - cand_pairs
    print(f"\nGold pairs (from Duplication Check, db_id-grouped same-person): {len(gold)}")
    print(f"  recovered by candidate pass: {len(tp)} ({100*len(tp)/max(1,len(gold)):.1f}%)")
    print(f"  missed:                       {len(missed)}")

    # Write gold eval
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

    # Print top 15 review-band pairs for sanity check
    review_rows = [r for r in rows if r["band"] == "review"]
    print(f"\n--- Top 15 review-band pairs (for sanity check) ---")
    for r in review_rows[:15]:
        print(f"  [{r['composite_score']:.3f}] {r['a_heading']!r}  vs  {r['b_heading']!r}  (src={r['source']})")

    # Regression: how many human-decided "different" pairs are still emitted?
    DECISIONS_TSV = HERE / "person_dedup_decisions.tsv"
    if DECISIONS_TSV.exists():
        with open(DECISIONS_TSV) as f:
            decisions = list(csv.DictReader(f, delimiter="\t"))
        diff_pairs = {frozenset((r["a_xml_id"], r["b_xml_id"])) for r in decisions
                      if r.get("decision") == "different"}
        still_emitted = diff_pairs & cand_pairs
        print(f"\nRegression: human 'different' pairs still emitted: "
              f"{len(still_emitted)}/{len(diff_pairs)}")


if __name__ == "__main__":
    main()
