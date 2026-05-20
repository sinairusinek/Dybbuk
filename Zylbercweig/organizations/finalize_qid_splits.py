"""Finalize QID-exploded sub-clusters: rename with location, requeue for align.

The QID exploder (qid_explode_clusters.py) splits a multi-location cluster like
ORG-C00360 (Hopkinson Theatre across Brooklyn / NYC / Brownsville / Detroit) into
ORG-C00360_Q01..Q04 — one sub-cluster per settlement — and stamps decision=SPLIT
on all of them. Those sub-clusters are no longer ambiguous; they each describe a
single-location entity. This script:

  1. Picks each `*_Q\\d\\d` row with decision=SPLIT and exactly one settlement.
  2. Renames its canonical_yiddish: prefer the longest attested variant in
     extracted_venues / name_variants that ends with the canonical name and is
     strictly longer (catches forms like "ברוקלינער האָפּקינסאָן-טעאַטער"). Falls
     back to "{canonical} ({settlement})".
  3. Clears decision/aligned_db_id/reviewer_notes so the row re-enters the
     Undecided queue.
  4. Rescores DB candidates against the new name using the same matchers as
     prepare_alignment.py.
  5. Stamps reviewer=auto_finalize_qid + ISO timestamp.

Multi-settlement _Q rows (rare; should be empty after qid_explode) and rows with
no settlement attestation are left as SPLIT for human/cluster-research review.

Dry-run by default; pass --apply to write back to org_alignment_review.tsv.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/finalize_qid_splits.py [--apply] [--limit N]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
ALIGN = HERE / "org_alignment_review.tsv"
CORE_DB = HERE / "core_db.tsv"

sys.path.insert(0, str(HERE))
from prepare_alignment import (  # noqa: E402
    cross_script_similarity,
    dm_codes,
    name_similarity,
    normalize_yiddish,
    organization_name_aliases,
    split_name_variants,
)

QID_RE = re.compile(r"_Q\d{2}$")

# Leading/trailing characters to ignore when judging whether an "attested
# longer form" actually adds a content word (rather than punctuation).
_STRIP_CHARS = " \t „""\"'»«()[]{}·–—-"

# Yiddish definite articles & filler particles that shouldn't count as a
# place-adjective when chosen as the "extra prefix" of an attested form.
_FILLER_TOKENS = {"דאָס", "דער", "די", "דעם", "אַ", "אַן", "פֿון", "פון"}


def _strip_decoration(s: str) -> str:
    # NFKD normalization decomposes Hebrew presentation-form characters
    # (e.g. U+FB2E "Hebrew Letter Alef With Patah") into base+combining-mark
    # so they compare equal to manually-typed Aleph+Patah sequences. Then
    # NFC re-composes so display remains compact.
    s = unicodedata.normalize("NFC", unicodedata.normalize("NFKD", s))
    return s.strip(_STRIP_CHARS).strip()


def _attested_extra_is_meaningful(extra: str) -> bool:
    """Return True if `extra` (the part of an attested variant that precedes
    the canonical name) looks like a real place-adjective rather than just
    punctuation or a Yiddish article."""
    extra = extra.strip(_STRIP_CHARS).strip()
    if not extra:
        return False
    tokens = [t for t in re.split(r"\s+", extra) if t and t not in _FILLER_TOKENS]
    if not tokens:
        return False
    # Require at least one token of length >= 4 to skip articles/initials.
    return any(len(t) >= 4 for t in tokens)


_HEB_FINAL_MAP = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})


def _normalize_settlement(s: str) -> str:
    """Collapse a settlement string to a comparable key so 'ברוקלין' (noun)
    and 'ברוקליינער' (adjective) — or any final/non-final + double-yud
    variant — map to the same place."""
    s = _strip_decoration(s)
    # Drop adjective suffixes (-ער / -ע) so noun and adjective forms align.
    for suf in ("ער", "ע"):
        if s.endswith(suf) and len(s) > len(suf) + 2:
            s = s[: -len(suf)]
            break
    # Normalize Hebrew final-form letters to non-final.
    s = s.translate(_HEB_FINAL_MAP)
    # Collapse double-yud (and double-vav) to a single letter.
    s = s.replace("יי", "י").replace("וו", "ו")
    # Strip combining marks and internal whitespace/hyphens.
    s = re.sub(r"[\s\-]+", "", s)
    return s

TYPE_EQUIVALENCE_CLASSES = [
    frozenset({"Publisher", "Printer", "Printer/Publisher", "Journals/ Newspapers"}),
]
_TYPE_TO_BLOCK: dict[str, str] = {}
for _cls in TYPE_EQUIVALENCE_CLASSES:
    _canon = min(_cls)
    for _t in _cls:
        _TYPE_TO_BLOCK[_t] = _canon


def block_key(org_type: str) -> str:
    t = (org_type or "").strip()
    if not t:
        return ""
    return _TYPE_TO_BLOCK.get(t, t)


def build_db_pool() -> tuple[list[dict[str, object]], dict[str, list[dict[str, object]]], list[dict[str, object]]]:
    with CORE_DB.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    db_entries: list[dict[str, object]] = []
    for row in rows:
        db_id = row.get("db_id", "").strip()
        if not db_id:
            continue
        db_name = row.get("name", "").strip()
        db_name_yid = row.get("name_yiddish", "").strip()
        db_name_yid_translit = row.get("name_yiddish_translit", "").strip()
        variants = split_name_variants(db_name)
        if db_name and db_name not in variants:
            variants.append(db_name)
        if db_name_yid:
            for yv in split_name_variants(db_name_yid):
                if yv and yv not in variants:
                    variants.append(yv)
            if db_name_yid not in variants:
                variants.append(db_name_yid)
        if db_name_yid_translit:
            for yv in split_name_variants(db_name_yid_translit):
                if yv and yv not in variants:
                    variants.append(yv)
            if db_name_yid_translit not in variants:
                variants.append(db_name_yid_translit)
        alias_variants = sorted({a for v in variants for a in organization_name_aliases(v)})
        dm: set[str] = set()
        for v in variants:
            dm |= dm_codes(v)
        db_entries.append({
            "db_id": db_id,
            "name": db_name,
            "org_type": row.get("org_type", "").strip(),
            "variants": variants,
            "alias_variants": alias_variants,
            "dm_codes": dm,
        })
    by_block: dict[str, list[dict[str, object]]] = defaultdict(list)
    any_block: list[dict[str, object]] = []
    for d in db_entries:
        bk = block_key(d["org_type"])  # type: ignore[arg-type]
        if not bk:
            any_block.append(d)
        else:
            by_block[bk].append(d)
    return db_entries, by_block, any_block


def score_candidates(cname: str, org_type: str, db_entries, by_block, any_block) -> list[tuple[str, float, str]]:
    cnorm = normalize_yiddish(cname)
    caliases = organization_name_aliases(cname)
    cdm = dm_codes(cname)
    c_block = block_key(org_type)
    if c_block:
        pool = by_block.get(c_block, []) + any_block
    else:
        pool = db_entries
    scored: dict[str, tuple[float, str]] = {}
    for d in pool:
        db_id = str(d["db_id"])
        best_score, best_method = 0.0, ""
        for nv in d["alias_variants"]:  # type: ignore[index]
            if cnorm and cnorm == nv:
                best_score, best_method = 1.0, "exact"
                break
        if best_score < 1.0:
            for ca in caliases:
                if ca in d["alias_variants"]:  # type: ignore[operator]
                    best_score, best_method = 1.0, "exact"
                    break
        if best_score < 1.0 and cdm and d["dm_codes"]:  # type: ignore[index]
            if cdm & d["dm_codes"]:  # type: ignore[operator]
                best_score, best_method = 0.85, "phonetic"
        fuzzy_best = 0.0
        for ca in caliases:
            for nv in d["alias_variants"]:  # type: ignore[index]
                sim = name_similarity(ca, nv)
                if sim > fuzzy_best:
                    fuzzy_best = sim
        if fuzzy_best > best_score:
            best_score, best_method = fuzzy_best, "fuzzy"
        ipa_best = 0.0
        for ca in caliases:
            for v in d["variants"]:  # type: ignore[index]
                sim = cross_script_similarity(ca, str(v))
                if sim > ipa_best:
                    ipa_best = sim
        if ipa_best > best_score:
            best_score, best_method = ipa_best, "ipa_phonetic"
        min_score = 0.40 if best_method == "ipa_phonetic" else 0.60
        if best_score >= min_score:
            prev = scored.get(db_id)
            if prev is None or best_score > prev[0]:
                scored[db_id] = (best_score, best_method)
    ranked = sorted(scored.items(), key=lambda kv: kv[1][0], reverse=True)[:10]
    return [(k, v[0], v[1]) for k, v in ranked]


def pick_new_name(canonical: str, settlement: str, venues: str, variants: str) -> tuple[str, str]:
    """Return (new_name, source_tag). We always use the parenthetical form so
    the canonical is uniform across the corpus; attested adjective forms
    (e.g. 'ברוקלינער') remain in name_variants/extracted_venues for blocking."""
    return f"{canonical.strip()} ({settlement})", "parenthetical"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write changes back to org_alignment_review.tsv")
    ap.add_argument("--limit", type=int, default=20, help="Preview at most N updated rows in dry-run")
    args = ap.parse_args()

    if not ALIGN.exists():
        raise FileNotFoundError(f"Missing: {ALIGN}")
    if not CORE_DB.exists():
        raise FileNotFoundError(f"Missing: {CORE_DB}")

    with ALIGN.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    print("Building DB candidate pool...")
    db_entries, by_block, any_block = build_db_pool()
    print(f"  {len(db_entries)} DB rows; {len(by_block)} blocks; {len(any_block)} empty-type fail-open.")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    eligible = 0
    skipped_empty_settle = 0
    skipped_multi_settle = 0
    updated: list[tuple[str, str, str, str, list[tuple[str, float, str]]]] = []

    for row in rows:
        cid = row.get("cluster_id", "").strip()
        if not QID_RE.search(cid):
            continue
        if row.get("decision", "").strip() != "SPLIT":
            continue
        settle_raw = row.get("extracted_settlements", "").strip()
        if not settle_raw:
            skipped_empty_settle += 1
            continue
        parts = [p.strip() for p in settle_raw.split("|") if p.strip()]
        # Collapse adjective/noun variants of the same place
        # (e.g. 'ברוקלין' + 'ברוקליינער' -> one settlement).
        norm_groups: dict[str, list[str]] = defaultdict(list)
        for p in parts:
            norm_groups[_normalize_settlement(p)].append(p)
        if len(norm_groups) != 1:
            skipped_multi_settle += 1
            continue
        eligible += 1
        # Prefer the noun form (shortest) over the adjective for the displayed name.
        settlement = min(next(iter(norm_groups.values())), key=len)
        canonical = row.get("canonical_yiddish", "").strip()
        new_name, source = pick_new_name(
            canonical,
            settlement,
            row.get("extracted_venues", ""),
            row.get("name_variants", ""),
        )
        ranked = score_candidates(new_name, row.get("org_type", ""), db_entries, by_block, any_block)
        # Apply updates to the row dict (in-place; we write back the same list).
        row["canonical_yiddish"] = new_name
        row["candidate_db_ids"] = " | ".join(k for k, _, _ in ranked)
        row["candidate_scores"] = " | ".join(f"{s:.3f}" for _, s, _ in ranked)
        row["candidate_methods"] = " | ".join(m for _, _, m in ranked)
        row["decision"] = ""
        row["aligned_db_id"] = ""
        prev_notes = row.get("reviewer_notes", "").strip()
        row["reviewer_notes"] = (
            f"[finalize_qid {source}] renamed from '{canonical}' for settlement '{settlement}'."
            + (f" Prior notes: {prev_notes}" if prev_notes else "")
        )
        row["reviewer"] = "auto_finalize_qid"
        row["reviewed_at"] = now
        updated.append((cid, canonical, new_name, source, ranked))

    print(f"Eligible _Q SPLIT rows with single settlement: {eligible}")
    print(f"  Skipped (no settlement): {skipped_empty_settle}")
    print(f"  Skipped (multi settlement): {skipped_multi_settle}")
    print()
    for cid, old, new, source, ranked in updated[: args.limit]:
        top = ranked[0] if ranked else ("(none)", 0.0, "")
        print(f"  {cid}: '{old}' -> '{new}'  [{source}]  top={top[0]}({top[1]:.2f},{top[2]})")
    if len(updated) > args.limit:
        print(f"  ... +{len(updated) - args.limit} more")

    if not args.apply:
        print("\n(dry-run) re-run with --apply to write to org_alignment_review.tsv")
        return

    with ALIGN.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote {len(updated)} updated rows to {ALIGN}")


if __name__ == "__main__":
    main()
