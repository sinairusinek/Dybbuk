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

Phase A gates added:
  1. Alias hygiene: single-token aliases whose token is a common given name
     (given_name_counts >= 3) are not used as blocking keys or scoring variants.
  2. Gender gate: if both sides have non-empty gender and they differ → drop.
  3. DB-contradiction auto-close: if both sides have a db_id and they differ →
     do not emit; write to dedup_auto_closed.tsv instead.
  4. Date-contradiction hard drop: if both sides have birth year differing by >3
     OR both have death year differing by >3 → drop entirely.
  5. Surname gate: implemented in people_similarity.person_pair_score.
  6. Real ranking + band column: high (>=0.85), review (0.65-0.85), low (0.55-0.65).
     No fixed 0.7. Alias-derived pairs use real scores, folded in with source=alias.

Notes:
  - Same-volume pairs are emitted too (e.g. two vol-5 entries with same surname)
    but flagged via `same_volume=1` so the RA can filter.
  - This is candidate generation only. The drafter + RA pass make the actual
    decision.
"""
from __future__ import annotations
import csv, sys, pathlib
from collections import defaultdict, Counter
from itertools import combinations

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))

from people_similarity import (  # type: ignore
    expand_name_variants, person_pair_score, has_surname_overlap,
    set_given_name_counts, variant_ok,
    date_overlap_score, place_overlap_score, composite_pair_score,
    heading_to_canonical, normalize_person_name, split_name_tokens,
    extract_year,
)
from mine_pseudonyms import build_given_name_counts as _mine_given_counts  # type: ignore

PEOPLE_TSV = HERE / "people_extracted.tsv"
REVIEW_TSV = HERE / "people_alignment_review.tsv"
ALIASES_TSV = HERE / "people_aliases.tsv"
DB_TSV = HERE / "people_db.tsv"
OUT_TSV = HERE / "people_dedup_candidates.tsv"
GOLD_EVAL_TSV = HERE / "people_dedup_gold_eval.tsv"
AUTO_CLOSED_TSV = HERE / "dedup_auto_closed.tsv"
NON_PERSON_TSV = HERE / "non_person_entries_phaseA.tsv"

SCORE_THRESHOLD = 0.55

# Phase A: alias hygiene threshold — single-token aliases that appear this many
# times or more in the given-name position are suppressed.
GIVEN_NAME_THRESHOLD = 3

# Phase A fix (defect 2): comma-less headings starting with a Yiddish article /
# poster word are articles/objects, not person entries. Tokens compared after
# normalize_person_name (diacritics stripped): דער, די, דאָס→דאס, אַ→א, אַן→אן,
# אַפיש→אפיש.
NON_PERSON_FIRST_TOKENS = {"דער", "די", "דאס", "א", "אן", "אפיש"}


def is_non_person_heading(heading: str) -> bool:
    """True if the entry heading looks like an article/object, not a person:
    NO comma AND first normalized token is a Yiddish article/poster word."""
    h = (heading or "").strip()
    if not h or "," in h:
        return False
    toks = split_name_tokens(h)
    return bool(toks) and toks[0] in NON_PERSON_FIRST_TOKENS


def build_given_name_counts(people: list[dict]) -> Counter:
    """Frequency of each token in the given-name part of headings (the part
    AFTER the comma). Delegates to mine_pseudonyms so both scripts share one
    definition."""
    return _mine_given_counts(people)


def blocking_key(heading: str, names_variants: str, aliases: list[str],
                 given_name_counts: Counter) -> set[str]:
    """Return a set of blocking keys — surname-like tokens that group entries
    likely to be the same person. Each entry can appear in multiple blocks (e.g.
    married name + maiden name).

    Rules:
      - Heading with a comma → use the part BEFORE the comma (the surname).
      - Heading without a comma → use ALL tokens (we can't tell surname from
        given name reliably for these — better to over-block than miss).
      - Phase A alias hygiene: every alias-like form (heading brackets,
        names_variants entries, mined aliases) that is a SINGLE token equal to
        a common given name (given_name_counts >= GIVEN_NAME_THRESHOLD) is
        suppressed from blocking. Multi-token forms are kept whole.
    """
    keys: set[str] = set()
    import re as _re

    def add_alias_form(form: str) -> None:
        toks = split_name_tokens(form)
        if len(toks) == 1:
            tok = toks[0]
            if given_name_counts.get(tok, 0) >= GIVEN_NAME_THRESHOLD:
                return  # Phase A: suppress single-token common given name
            if len(tok) >= 2:
                keys.add(tok)
        else:
            for t in toks:
                if len(t) >= 2:  # skip single-letter initials
                    keys.add(t)

    # bracketed aliases in the heading get their tokens added too
    for m in _re.finditer(r"[\[\(]([^\]\)]+)[\]\)]", heading or ""):
        add_alias_form(m.group(1))
    # heading proper (never suppressed)
    h_stripped = (heading or "").split("[")[0].split("(")[0].strip()
    if "," in h_stripped:
        surname_part = h_stripped.split(",", 1)[0]
        for t in split_name_tokens(surname_part):
            keys.add(t)
    else:
        for t in split_name_tokens(h_stripped):
            keys.add(t)
    # names_variants — alias-like forms, same hygiene
    for v in (names_variants or "").split("|"):
        v = v.strip()
        if v:
            add_alias_form(v)
    # mined aliases (already filtered upstream; defensive filter anyway)
    for alias in aliases:
        add_alias_form(alias)
    keys.discard("")
    # drop very-short tokens (initials and stop-letters)
    return {k for k in keys if len(k) >= 2}


def load_people():
    with open(PEOPLE_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


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


def load_aliases() -> dict[str, list[str]]:
    """person_id -> list of alias surface forms."""
    if not ALIASES_TSV.exists():
        return {}
    out: dict[str, list[str]] = defaultdict(list)
    with open(ALIASES_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            pid = r.get("person_id")
            alias = (r.get("alias_form") or "").strip()
            if pid and alias:
                out[pid].append(alias)
    return out


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


def score_row(a: dict, b: dict, va: set[str], vb: set[str], k: str,
              gender_by_xml: dict[str, str],
              db_id_by_xml: dict[str, str], source: str) -> dict | None:
    """Score a pair of people entries. Returns a dict row or None if below the
    score floor. NO gating happens here — the ungated score is computed so the
    caller can attribute drops to individual Phase A gates (surname gate
    included, hence surname_gate=False).
    """
    ns, best_a, best_b = person_pair_score(va, vb, surname_gate=False)
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
    # Band assignment
    if score >= 0.85:
        band = "high"
    elif score >= 0.65:
        band = "review"
    else:
        band = "low"
    a_db = db_id_by_xml.get(a["xml_id"], "")
    b_db = db_id_by_xml.get(b["xml_id"], "")
    db_agreement = 1 if (a_db and b_db and a_db == b_db) else 0
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
        "band": band,
        "blocking_key": k,
        "a_gender": gender_by_xml.get(a["xml_id"], ""),
        "b_gender": gender_by_xml.get(b["xml_id"], ""),
        "a_db_id": a_db,
        "b_db_id": b_db,
        "db_agreement": db_agreement,
        "source": source,
    }


def main():
    all_people = load_people()
    aliases = load_aliases()
    db_id_by_xml = load_db_id_by_xml_id()
    gender_by_db = load_gender_by_db_id()
    gender_by_xml = {x: gender_by_db[d] for x, d in db_id_by_xml.items() if d in gender_by_db}

    # ---- Phase A fix (defect 2): exclude non-person entries ----
    non_person = [p for p in all_people if is_non_person_heading(p["heading"])]
    people = [p for p in all_people if not is_non_person_heading(p["heading"])]
    with open(NON_PERSON_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp, delimiter="\t")
        w.writerow(["xml_id", "heading"])
        for p in non_person:  # input order (deterministic)
            w.writerow([p["xml_id"], p["heading"]])
    print(f"  non-person entries excluded: {len(non_person)} (logged to {NON_PERSON_TSV.name})")

    # Build given_name_counts for alias hygiene (Phase A change 1) and install
    # them for the surname gate (Phase A change 5).
    given_name_counts = build_given_name_counts(people)
    set_given_name_counts(given_name_counts)
    print(f"  db_id resolved for {len(db_id_by_xml)} xml_ids "
          f"({100*len(db_id_by_xml)/max(1,len(people)):.0f}% of subject entries)")
    print(f"  gender resolved for {len(gender_by_xml)} xml_ids")

    # build blocking index — fold alias tokens into the per-person key set so
    # pseudonyms surface as candidates even when headings are dissimilar.
    # base_keys (heading + names_variants + heading brackets, no mined aliases)
    # drive the source=heading/alias provenance tag.
    blocks: dict[str, list[dict]] = defaultdict(list)
    base_keys: dict[str, set[str]] = {}
    for p in people:
        pid = p["person_id"]
        p_aliases = aliases.get(pid, [])
        base_keys[pid] = blocking_key(p["heading"], p.get("names_variants", ""),
                                      [], given_name_counts)
        keys = blocking_key(p["heading"], p.get("names_variants", ""), p_aliases,
                            given_name_counts)
        for k in keys:
            blocks[k].append(p)

    # precompute variants per person — alias forms are folded into the variant
    # set so person_pair_score can compare alias-to-heading directly.
    # Phase A fix (defect 1): expand_name_variants now drops initial-like /
    # too-short forms (rule 1a) and single-token common given names from
    # names_variants (rule 1c) internally; mined aliases get the same treatment
    # here before folding.
    variants_cache: dict[str, set[str]] = {}
    variant_drop_log: list[tuple[str, str]] = []
    for p in people:
        v = expand_name_variants(
            p["heading"], p.get("names_variants", ""), p.get("subheading", ""),
            given_name_counts=given_name_counts, drop_log=variant_drop_log,
        )
        for alias in aliases.get(p["person_id"], ()):
            if not variant_ok(alias):
                variant_drop_log.append((alias, "initial_or_short"))
                continue
            toks = split_name_tokens(alias)
            if (len(toks) == 1
                    and given_name_counts.get(toks[0], 0) >= GIVEN_NAME_THRESHOLD):
                variant_drop_log.append((alias, "common_given_name"))
                continue
            v.add(alias)
        variants_cache[p["person_id"]] = v
    n_drop_1a = sum(1 for _, reason in variant_drop_log if reason == "initial_or_short")
    n_drop_1c = sum(1 for _, reason in variant_drop_log if reason == "common_given_name")
    print(f"  variant drops — rule 1a (initial/short): {n_drop_1a}   "
          f"rule 1c (common given name): {n_drop_1c}")

    # ---- collect candidate pairs (deduped across blocks) with provenance ----
    people_by_pid = {p["person_id"]: p for p in people}
    pair_first_key: dict[tuple[str, str], str] = {}
    for k in sorted(blocks):  # sorted for determinism
        items = blocks[k]
        if len(items) < 2 or len(items) > 200:
            # skip absurd buckets (e.g. empty key, single-letter)
            if len(items) > 200:
                print(f"[skip large block] key={k!r} size={len(items)}")
            continue
        for a, b in combinations(sorted(items, key=lambda x: x["person_id"]), 2):
            pa, pb = a["person_id"], b["person_id"]
            key = (pa, pb) if pa < pb else (pb, pa)
            if key not in pair_first_key:
                pair_first_key[key] = k

    # ---- score, then gate (Phase A gates 2-5) ----
    n_would_emit = 0          # pairs passing the score floor before gates
    n_gender_drop = 0
    n_db_contradiction = 0
    n_date_contradiction = 0
    n_surname_drop = 0
    auto_closed_rows = []
    rows = []
    for pa, pb in sorted(pair_first_key):
        k = pair_first_key[(pa, pb)]
        a, b = people_by_pid[pa], people_by_pid[pb]
        va = variants_cache[pa]
        vb = variants_cache[pb]
        source = "heading" if (base_keys[pa] & base_keys[pb]) else "alias"
        row = score_row(a, b, va, vb, k, gender_by_xml, db_id_by_xml, source)
        if row is None:
            continue  # below score floor — never a candidate, not a gate drop
        n_would_emit += 1

        # ---- Phase A Gate 2: Gender gate (hard drop) ----
        ag = gender_by_xml.get(a["xml_id"], "")
        bg = gender_by_xml.get(b["xml_id"], "")
        if ag and bg and ag != bg:
            n_gender_drop += 1
            continue

        # ---- Phase A Gate 3: DB-contradiction auto-close ----
        a_db = db_id_by_xml.get(a["xml_id"], "")
        b_db = db_id_by_xml.get(b["xml_id"], "")
        if a_db and b_db and a_db != b_db:
            n_db_contradiction += 1
            auto_closed_rows.append({
                "a_xml_id": a["xml_id"],
                "b_xml_id": b["xml_id"],
                "verdict": "different",
                "reason": "db_contradiction",
                "a_db_id": a_db,
                "b_db_id": b_db,
                "a_heading": a["heading"],
                "b_heading": b["heading"],
            })
            continue

        # ---- Phase A Gate 4: Date-contradiction hard drop ----
        # Exception: when both sides share the same db_id (confirmed same
        # person by RA), date contradiction may reflect OCR errors across
        # volumes — skip the gate in that case.
        if not (a_db and b_db and a_db == b_db):
            ay = extract_year(a["birth_date"])
            by_ = extract_year(b["birth_date"])
            ad = extract_year(a["death_date"])
            bd = extract_year(b["death_date"])
            if (ay and by_ and abs(ay - by_) > 3) or (ad and bd and abs(ad - bd) > 3):
                n_date_contradiction += 1
                continue

        # ---- Phase A Gate 5: Surname gate ----
        # Same exemption as gate 4: db-agreement pairs are RA-confirmed same
        # person ("keep the pair", spec change 3) — never gate them out.
        if not (a_db and b_db and a_db == b_db):
            if not has_surname_overlap(va, vb):
                n_surname_drop += 1
                continue

        rows.append(row)

    rows.sort(key=lambda r: (-r["composite_score"], r["a_xml_id"], r["b_xml_id"]))

    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        if rows:
            w = csv.DictWriter(fp, fieldnames=list(rows[0].keys()), delimiter="\t")
            w.writeheader()
            w.writerows(rows)
    print(f"wrote {OUT_TSV} ({len(rows)} candidate pairs)")
    n_cross_vol = sum(1 for r in rows if not r["same_volume"])
    n_same_vol = sum(1 for r in rows if r["same_volume"])
    print(f"  cross-volume: {n_cross_vol}    same-volume: {n_same_vol}")

    n_high = sum(1 for r in rows if r["band"] == "high")
    n_review = sum(1 for r in rows if r["band"] == "review")
    n_low = sum(1 for r in rows if r["band"] == "low")
    print(f"  band high (>=0.85): {n_high}  review (0.65-0.85): {n_review}  low (0.55-0.65): {n_low}")
    print(f"\n  Pairs passing score floor before gates: {n_would_emit}")
    print(f"  Gate drops (of would-be candidates):")
    print(f"    gender contradiction:  {n_gender_drop}")
    print(f"    db_id contradiction:   {n_db_contradiction}")
    print(f"    date contradiction:    {n_date_contradiction}")
    print(f"    surname gate:          {n_surname_drop}")

    # Write dedup_auto_closed.tsv (Phase A change 3) — sort for determinism
    auto_closed_rows.sort(key=lambda r: (r["a_xml_id"], r["b_xml_id"]))
    with open(AUTO_CLOSED_TSV, "w", encoding="utf-8", newline="") as fp:
        if auto_closed_rows:
            w = csv.DictWriter(fp, fieldnames=list(auto_closed_rows[0].keys()), delimiter="\t")
        else:
            w = csv.DictWriter(fp, fieldnames=[
                "a_xml_id", "b_xml_id", "verdict", "reason",
                "a_db_id", "b_db_id", "a_heading", "b_heading"
            ], delimiter="\t")
        w.writeheader()
        w.writerows(auto_closed_rows)
    print(f"  wrote {AUTO_CLOSED_TSV} ({len(auto_closed_rows)} auto-closed pairs)")

    # Regenerate pseudonym_candidates_review.tsv from the gated alias pathway
    # with REAL scores (no fixed 0.7 anywhere). Alias-derived pairs are also
    # folded into the main file above with source=alias.
    alias_rows = [r for r in rows if r.get("source") == "alias"]
    heading_rows = [r for r in rows if r.get("source") == "heading"]
    print(f"  source=heading: {len(heading_rows)}  source=alias: {len(alias_rows)}")

    pseudo_cols = [
        "decision", "band", "composite", "name_score",
        "a_heading", "b_heading", "a_aliases", "b_aliases",
        "a_xml_id", "b_xml_id", "a_volume", "b_volume", "same_volume",
        "a_gender", "b_gender", "a_db_id", "b_db_id", "db_agreement",
    ]
    pseudo_out = HERE / "pseudonym_candidates_review.tsv"
    with open(pseudo_out, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=pseudo_cols, delimiter="\t")
        w.writeheader()
        for r in alias_rows:
            w.writerow({
                "decision": "",
                "band": r["band"],
                "composite": r["composite_score"],
                "name_score": r["name_score"],
                "a_heading": r["a_heading"],
                "b_heading": r["b_heading"],
                "a_aliases": " | ".join(aliases.get(r["a_person_id"], [])),
                "b_aliases": " | ".join(aliases.get(r["b_person_id"], [])),
                "a_xml_id": r["a_xml_id"],
                "b_xml_id": r["b_xml_id"],
                "a_volume": r["a_volume"],
                "b_volume": r["b_volume"],
                "same_volume": r["same_volume"],
                "a_gender": r["a_gender"],
                "b_gender": r["b_gender"],
                "a_db_id": r["a_db_id"],
                "b_db_id": r["b_db_id"],
                "db_agreement": r["db_agreement"],
            })
    print(f"  wrote {pseudo_out} ({len(alias_rows)} alias-pathway pairs, real scores)")

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

    # write gold eval — index over ALL entries (incl. non-person-excluded) so
    # a gold pair killed by the non-person filter reports as a real MISS, not
    # a phantom.
    gold_idx = {r["xml_id"]: r for r in all_people}
    # real gold = both xml_ids resolve to subject entries (phantom pairs carry
    # malformed xml_ids like "28.0" and can never be recovered)
    real_gold = {p for p in gold if all(x in gold_idx for x in p)}
    real_tp = real_gold & cand_pairs
    print(f"  REAL gold (both xml_ids resolvable): {len(real_tp)}/{len(real_gold)} recovered")
    for p in sorted((tuple(sorted(x)) for x in (real_gold - cand_pairs))):
        ha = gold_idx.get(p[0], {}).get("heading", "")
        hb = gold_idx.get(p[1], {}).get("heading", "")
        print(f"    MISSED real gold: {p[0]} / {p[1]}   {ha!r} vs {hb!r}")
    with open(GOLD_EVAL_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp, delimiter="\t")
        w.writerow(["status", "xml_a", "xml_b", "head_a", "head_b", "candidate_score"])
        cand_by_pair = {frozenset((r["a_xml_id"], r["b_xml_id"])): r["composite_score"] for r in rows}
        # Sort deterministically by (xa, xb) strings
        gold_sorted = sorted((tuple(sorted(pair)) for pair in gold), key=lambda t: (t[0], t[1]))
        for xa, xb in gold_sorted:
            pair = frozenset((xa, xb))
            ra = gold_idx.get(xa, {})
            rb = gold_idx.get(xb, {})
            sc = cand_by_pair.get(pair, "")
            status = "RECOVERED" if pair in tp else "MISSED"
            w.writerow([status, xa, xb, ra.get("heading", ""), rb.get("heading", ""), sc])
    print(f"wrote {GOLD_EVAL_TSV}")

    # Regression check: how many human "different" pairs are still emitted?
    dedup_dec_tsv = HERE / "person_dedup_decisions.tsv"
    if dedup_dec_tsv.exists():
        human_diff = set()
        with open(dedup_dec_tsv) as f:
            for r in csv.DictReader(f, delimiter="\t"):
                if (r.get("decision") or "").lower() == "different":
                    human_diff.add(frozenset((
                        (r.get("a_xml_id") or "").strip(),
                        (r.get("b_xml_id") or "").strip(),
                    )))
        survivors = human_diff & cand_pairs
        print(f"\nRegression: {len(survivors)}/{len(human_diff)} human 'different' pairs still emitted")
        row_by_pair = {frozenset((r["a_xml_id"], r["b_xml_id"])): r for r in rows}
        for p in sorted((tuple(sorted(x)) for x in survivors)):
            r = row_by_pair[frozenset(p)]
            print(f"    survivor [{r['band']} {r['composite_score']}] "
                  f"{r['a_heading']!r} vs {r['b_heading']!r} (source={r['source']})")


if __name__ == "__main__":
    main()
