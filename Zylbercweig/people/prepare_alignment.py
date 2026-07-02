"""Build the people-alignment review queue.

For each subject-entry in people_extracted.tsv:
  - generate up to N candidates from people_db.tsv (the DiJeSt / Zylbercweig-people DB)
  - score candidates with people_similarity
  - emit one row to people_alignment_queue.tsv with top-5 candidates inlined
  - carry over any existing decision from people_alignment_review.tsv (matched
    by xml_id) so we don't lose RA work

Output columns mirror the org_alignment_review.tsv pattern:
  person_id, xml_id, volume, heading, names_variants, birth_year, death_year,
  entry_type, candidate_db_ids, candidate_scores, candidate_names,
  decision, aligned_db_id, reviewer, reviewer_notes, reviewed_at

Blocking strategy: surname-token (same as dedup); plus cross-script fail-open
since the DB has many Latin-only rows that need to match Hebrew entries.
"""
from __future__ import annotations
import csv, sys, pathlib
from collections import defaultdict

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))

from people_similarity import (  # type: ignore
    expand_name_variants, person_pair_score, variant_ok,
    extract_year, is_hebrew, split_name_tokens, heading_to_canonical,
    set_given_name_counts,
)
from mine_pseudonyms import build_given_name_counts  # type: ignore
from find_dedup_candidates import is_non_person_heading  # type: ignore

PEOPLE_TSV = HERE / "people_extracted.tsv"
DB_TSV = HERE / "people_db.tsv"
REVIEW_TSV = HERE / "people_alignment_review.tsv"
ALIASES_TSV = HERE / "people_aliases.tsv"
OUT_TSV = HERE / "people_alignment_queue.tsv"

TOP_N = 5
MIN_SCORE = 0.30  # candidate floor — better to over-recall, drafter filters


def db_blocking_key(name: str) -> set[str]:
    """Tokens to block by. We add ALL tokens since DB names are short."""
    return {t for t in split_name_tokens(name) if len(t) >= 2}


def load_aliases() -> dict[str, list[str]]:
    """person_id -> list of alias surface forms (heading_bracket, subheading_bracket,
    names_variant). Used to broaden blocking + scoring for pseudonyms that pure
    heading-similarity would miss (e.g. שלום-עליכם ↔ שלום ראַבינאָוויטש)."""
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


def load_existing_decisions():
    """xml_id -> dict(decision, aligned_db_id, reviewer, reviewer_notes, reviewed_at).

    Pulls from the imported review TSV; treats non-empty `db_id` rows as the
    closest thing we have to a decision until a richer RA workflow is added.
    """
    if not REVIEW_TSV.exists():
        return {}
    out: dict[str, dict] = {}
    with open(REVIEW_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            xml = r.get("xml_id")
            if not xml:
                continue
            existing = out.get(xml, {})
            # prefer rows that have a db_id
            new_score = (1 if r.get("db_id") else 0) + (1 if r.get("same_person") else 0)
            old_score = (1 if existing.get("aligned_db_id") else 0)
            if not existing or new_score > old_score:
                out[xml] = {
                    "decision": "ALIGN" if r.get("db_id") else "",
                    "aligned_db_id": r.get("db_id", ""),
                    "reviewer": "",
                    "reviewer_notes": r.get("notes", ""),
                    "reviewed_at": "",
                }
    return out


def main():
    with open(PEOPLE_TSV) as f:
        all_people = list(csv.DictReader(f, delimiter="\t"))
    with open(DB_TSV) as f:
        db_rows = list(csv.DictReader(f, delimiter="\t"))

    # Phase A fix (defect 2): exclude non-person entries (articles/objects,
    # e.g. "דער געדענק-טאָוול אין מאַריענבאַד"). The exclusion list is written by
    # find_dedup_candidates.py to non_person_entries_phaseA.tsv.
    people = [p for p in all_people if not is_non_person_heading(p["heading"])]
    print(f"  non-person entries excluded: {len(all_people) - len(people)}")

    # Phase A: install corpus given-name frequencies for the surname gate
    # inside person_pair_score (change 5).
    given_name_counts = build_given_name_counts(people)
    set_given_name_counts(given_name_counts)

    # build DB blocking index
    db_blocks: dict[str, list[dict]] = defaultdict(list)
    db_variants: dict[str, set[str]] = {}
    for d in db_rows:
        name_h = d["hebname"]
        name_e = d["english"]
        # surface forms for this DB row
        variants = set()
        for s in (name_h, name_e):
            if s:
                variants.add(s.strip())
        db_variants[d["db_id"]] = variants
        for surface in (name_h, name_e):
            if not surface:
                continue
            for k in db_blocking_key(surface):
                db_blocks[k].append(d)

    existing = load_existing_decisions()
    aliases = load_aliases()
    db_by_id = {d["db_id"]: d for d in db_rows}
    gender_by_db = {d["db_id"]: (d.get("gender") or "").strip() for d in db_rows}
    n_alias_only_cands = 0
    # Phase A gates 2-4 (spec: apply where both sides have the field)
    n_gate_db = 0
    n_gate_gender = 0
    n_gate_date = 0
    out_rows = []
    n_with_cand = 0
    total = len(people)
    for ii, p in enumerate(people):
        if ii % 200 == 0:
            print(f"  scoring {ii}/{total}", flush=True)
        pid = p["person_id"]
        # candidate DB rows: union of blocks for every blocking token of person
        cand_ids: set[str] = set()
        cand_ids_pre_alias: set[str] = set()
        for tok in db_blocking_key(p["heading"]):
            for d in db_blocks.get(tok, ()):  # noqa: SIM118
                cand_ids.add(d["db_id"])
        for v in (p.get("names_variants", "") or "").split("|"):
            v = v.strip()
            if not v:
                continue
            for tok in db_blocking_key(v):
                for d in db_blocks.get(tok, ()):  # noqa: SIM118
                    cand_ids.add(d["db_id"])
        cand_ids_pre_alias = set(cand_ids)
        # ALIAS PASS: pseudonyms / bracket-aliases / names_variant entries
        # surface DB rows that pure-heading blocking would miss
        # (e.g. שלום-עליכם entry → שלום ראַבינאָוויטש DB row).
        for alias in aliases.get(pid, ()):
            for tok in db_blocking_key(alias):
                for d in db_blocks.get(tok, ()):  # noqa: SIM118
                    cand_ids.add(d["db_id"])
        if cand_ids - cand_ids_pre_alias:
            n_alias_only_cands += 1
        # Cross-script fail-open is intentionally OFF in the initial alignment
        # build — it adds 300+ candidates per Hebrew person and dominates runtime
        # for low marginal recall. Run cross_script_pass.py as a separate step
        # to surface Hebrew↔Latin candidates with the IPA matcher.

        # Phase A fix (defect 1): rules 1a/1c applied inside expand_name_variants
        pvars = expand_name_variants(
            p["heading"], p.get("names_variants", ""), p.get("subheading", ""),
            given_name_counts=given_name_counts,
        )
        # fold alias surface forms into the variant set used for scoring
        # (rule 1a: skip initial-like / too-short aliases)
        for alias in aliases.get(pid, ()):
            if variant_ok(alias):
                pvars.add(alias)
        # ---- Phase A gates 2-4 (subject side fields) ----
        subj_db = existing.get(p["xml_id"], {}).get("aligned_db_id", "")
        subj_gender = gender_by_db.get(subj_db, "") if subj_db else ""
        subj_by = extract_year(p["birth_date"])
        subj_dy = extract_year(p["death_date"])

        scored = []
        for did in sorted(cand_ids):  # sorted for determinism
            dvars = db_variants[did]
            if not dvars:
                continue
            d_row = db_by_id[did]
            if subj_db and did == subj_db:
                # RA-confirmed alignment — keep unconditionally (mirror of the
                # db_agreement exemption in find_dedup_candidates)
                pass
            else:
                # Gate 3: DB-contradiction — subject already aligned elsewhere
                if subj_db and did != subj_db:
                    n_gate_db += 1
                    continue
                # Gate 2: gender (subject gender is DB-derived; fires only when
                # a subject db alignment exists, i.e. never past gate 3 — kept
                # for spec symmetry)
                d_gender = (d_row.get("gender") or "").strip()
                if subj_gender and d_gender and subj_gender != d_gender:
                    n_gate_gender += 1
                    continue
                # Gate 4: date contradiction (>3yr on birth or death)
                d_by = extract_year(d_row.get("date_born") or "")
                d_dy = extract_year(d_row.get("date_died") or "")
                if ((subj_by and d_by and abs(subj_by - d_by) > 3)
                        or (subj_dy and d_dy and abs(subj_dy - d_dy) > 3)):
                    n_gate_date += 1
                    continue
            s, _, _ = person_pair_score(pvars, dvars)
            if s >= MIN_SCORE:
                scored.append((s, did))
        scored.sort(reverse=True)
        top = scored[:TOP_N]
        if top:
            n_with_cand += 1

        cand_ids_str = "|".join(did for _, did in top)
        cand_scores_str = "|".join(f"{s:.3f}" for s, _ in top)
        cand_names_str = "|".join(
            (db_by_id[did].get("hebname") or db_by_id[did].get("english") or "")
            for _, did in top
        )

        d = existing.get(p["xml_id"], {})
        out_rows.append({
            "person_id": pid,
            "xml_id": p["xml_id"],
            "volume": p["volume"],
            "heading": p["heading"],
            "names_variants": p["names_variants"],
            "birth_year": extract_year(p["birth_date"]) or "",
            "death_year": extract_year(p["death_date"]) or "",
            "entry_type": p["entry_type"],
            "candidate_db_ids": cand_ids_str,
            "candidate_scores": cand_scores_str,
            "candidate_names": cand_names_str,
            "decision": d.get("decision", ""),
            "aligned_db_id": d.get("aligned_db_id", ""),
            "reviewer": d.get("reviewer", ""),
            "reviewer_notes": d.get("reviewer_notes", ""),
            "reviewed_at": d.get("reviewed_at", ""),
        })

    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=list(out_rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)
    print(f"wrote {OUT_TSV} ({len(out_rows)} rows)")
    print(f"  rows with at least one candidate: {n_with_cand}")
    print(f"  rows that gained candidates from aliases: {n_alias_only_cands}")
    print(f"  rows with carried-over decision:  {sum(1 for r in out_rows if r['decision'])}")
    print(f"  rows with carried-over db_id:     {sum(1 for r in out_rows if r['aligned_db_id'])}")
    print(f"  Phase A gate drops (candidate level):")
    print(f"    db contradiction (subject aligned elsewhere): {n_gate_db}")
    print(f"    gender contradiction:                          {n_gate_gender}")
    print(f"    date contradiction (>3yr):                     {n_gate_date}")

    # quick gold recall: of carried-over aligned_db_id rows that exist in our
    # DB, how often is the gold id in the top-N candidate list?
    gold_rows = [r for r in out_rows if r["aligned_db_id"] and r["aligned_db_id"] in {d["db_id"] for d in db_rows}]
    if gold_rows:
        hits = sum(1 for r in gold_rows if r["aligned_db_id"] in set(r["candidate_db_ids"].split("|")))
        print(f"  recall@{TOP_N} vs carried decisions: {hits}/{len(gold_rows)} = {100*hits/len(gold_rows):.1f}%")


if __name__ == "__main__":
    main()
