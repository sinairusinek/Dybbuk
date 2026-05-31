#!/usr/bin/env python3
"""Prepare org_alignment_review.tsv with ranked DB candidates per cluster.

Signals:
- exact (normalized equality)
- phonetic (Daitch-Mokotoff; optional if jellyfish is installed)
- fuzzy (trigram Jaccard via Kimatch fallback)
"""

from __future__ import annotations

import csv
import pathlib
import re
import sys
import unicodedata
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

BASE = pathlib.Path(__file__).resolve().parent
CLUSTERED = BASE / "organizations_clustered.tsv"
CORE_DB = BASE / "core_db.tsv"
OUT = BASE / "org_alignment_review.tsv"

COL_CLUSTER_ID = "cluster_id"
COL_CANONICAL = "canonical_yiddish"
COL_CLUSTER_SIZE = "cluster_size"
COL_ORG_TYPE = "_ - organizations - _ - org_type"
COL_TITLE = "_ - organizations - _ - title"
COL_CLUSTERED = "clustered organization"
COL_DESC = "_ - organizations - _ - descriptive_name"

COL_SETTLEMENT = "_ - organizations - _ - locations - _ - settlement"
COL_ADDRESS = "_ - organizations - _ - locations - _ - address"
COL_VENUE = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY = "_ - organizations - _ - locations - _ - country"

# Optional phonetic layer.
try:
    import jellyfish  # type: ignore

    HAS_JELLYFISH = True
except Exception:
    HAS_JELLYFISH = False

# Kimatch import fallback (mirrors cluster_orgs.py style).
_KIMATCH = pathlib.Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
if _KIMATCH.exists():
    sys.path.insert(0, str(_KIMATCH))

_DYBBUK_PHONETIC = pathlib.Path(__file__).resolve().parents[1] / "dybbuk-phonetic" / "src"
if _DYBBUK_PHONETIC.exists():
    sys.path.insert(0, str(_DYBBUK_PHONETIC))

try:
    from dybbuk_phonetic.bridge import cross_script_similarity

    HAS_DYBBUK_PHONETIC = True
except Exception:
    HAS_DYBBUK_PHONETIC = False

    def cross_script_similarity(name_a: str, name_b: str) -> float:
        return 0.0

try:
    from kimatch.core.normalizers import normalize_name, name_similarity
except Exception:
    def normalize_name(name: str) -> str:
        if not name:
            return ""
        nfd = unicodedata.normalize("NFD", name)
        stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
        return re.sub(r"\s+", " ", stripped).strip().lower()

    def name_similarity(a: str, b: str) -> float:
        a, b = normalize_name(a), normalize_name(b)
        if not a or not b:
            return 0.0
        if a == b:
            return 1.0

        def tg(s: str) -> set[str]:
            return {s[i : i + 3] for i in range(len(s) - 2)}

        ta, tb = tg(a), tg(b)
        if not ta or not tb:
            wa, wb = set(a.split()), set(b.split())
            return len(wa & wb) / len(wa | wb) if wa and wb else 0.0
        return len(ta & tb) / len(ta | tb)


from org_normalize import (
    normalize_yiddish,
    organization_name_aliases,
    token_key_set,
)
from translit_latin_to_yiddish import translit_latin_to_yiddish

# Org-type tail keywords used to detect person-named troupes/companies so we
# can also block on a given-name-stripped surface form. Clusters routinely
# refer to these as "<Surname>'s troupe" (e.g. פישזאָןס טרופּע) while the DB
# stores the founder's full name ("Abraham Fiszon Troupe"); the leading given
# name otherwise dominates the trigram similarity and hides the match.
_ORG_TAIL_KEYWORDS = {
    "troupe", "company", "theatre", "theater", "ensemble",
    "players", "opera", "troup", "troupes",
}


def surname_only_variant(latin_name: str) -> str:
    """For a Latin '<Given> <Surname...> <OrgKeyword>' name, return the form
    with the leading given-name token dropped (e.g. 'Abraham Fiszon Troupe' ->
    'Fiszon Troupe'). Returns '' when the pattern doesn't apply."""
    head = re.sub(r"\s*\([^)]*\)\s*", " ", latin_name or "").strip()
    toks = head.split()
    if len(toks) < 3 or toks[-1].lower().strip(".,") not in _ORG_TAIL_KEYWORDS:
        return ""
    return " ".join(toks[1:])


def semantic_identity_key(canonical_yiddish: str, org_type: str, name_variants: str) -> tuple[str, str, str]:
    return (
        normalize_yiddish(canonical_yiddish),
        org_type.strip().lower(),
        normalize_yiddish(name_variants),
    )


def preserved_row_matches_cluster(prev_row: dict[str, str], cluster_row: dict[str, str]) -> bool:
    return semantic_identity_key(
        prev_row.get("canonical_yiddish", ""),
        prev_row.get("org_type", ""),
        prev_row.get("name_variants", ""),
    ) == semantic_identity_key(
        cluster_row.get("canonical_yiddish", ""),
        cluster_row.get("org_type", ""),
        cluster_row.get("name_variants", ""),
    )


def best_name(row: dict[str, str]) -> str:
    for col in (COL_CLUSTERED, COL_TITLE, COL_DESC):
        v = row.get(col, "").strip()
        if v:
            return v
    return ""


def pipe_join_distinct(values: list[str]) -> str:
    seen: dict[str, None] = {}
    for v in values:
        vv = v.strip()
        if vv and vv not in seen:
            seen[vv] = None
    return " | ".join(seen.keys())


from matching_core import script_runs as _core_script_runs

def _yiddish_runs(text: str) -> list[str]:
    """Maximal Hebrew/Yiddish-script runs in `text`, via matching_core.script_runs.
    Was a local _YIDDISH_RUN regex pre-core-0.2.0; replaced with the shared
    primitive per LEDGER.md row 18 (closed)."""
    return _core_script_runs(text, "hebrew") if text else []


def _strip_format_marks(s: str) -> str:
    """Remove Unicode Cf-category characters (bidi marks, isolates, etc.)
    that pollute pasted DB names like 'Der Tog⁩ - ⁨דער טאג'."""
    return "".join(c for c in s if unicodedata.category(c) != "Cf")


def split_name_variants(name: str) -> list[str]:
    """Split a DB name into distinct surface variants.

    - First strips Unicode bidi/format marks (Cf) so pasted DB names don't
      carry hidden chars into similarity comparisons.
    - Splits on ' - ' (existing convention).
    - Strips parentheticals to expose the head name as a clean variant.
    - Adds each parenthetical body as its own variant.
    - Within mixed-script text (e.g. inside parens), extracts the longest
      Yiddish-script run as its own variant so a Yiddish cluster can match
      the Yiddish form inside a Latin-headed DB row.

    Example:
        'Leksikon fun yidishn teater (לעקסיקאן פון יידישן טעאטער Lexicon of ...)'
        -> ['Leksikon fun yidishn teater (לעקסיקאן ...)',  # original
            'Leksikon fun yidishn teater',                  # paren-stripped head
            'לעקסיקאן פון יידישן טעאטער Lexicon of ...',     # paren body
            'לעקסיקאן פון יידישן טעאטער']                    # Yiddish run inside body
    """
    if not name:
        return []
    name = _strip_format_marks(name)
    base_parts = [p.strip() for p in re.split(r"\s+-\s+", name) if p.strip()]
    if not base_parts:
        base_parts = [name.strip()]

    out: list[str] = []
    seen: set[str] = set()

    def push(v: str) -> None:
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)

    for part in base_parts:
        push(part)  # keep the original form too
        # Strip parenthetical(s) to expose the head
        head = re.sub(r"\s*\([^)]*\)\s*", " ", part).strip()
        if head:
            push(head)
        # Top-level mixed-script parts (e.g. 'גאָלדפאדעןס טרופּע -Avraham
        # Goldfaden Troupe', where the en-dash isn't space-delimited so the
        # ' - ' split didn't separate the scripts): surface each Yiddish run so
        # a Yiddish cluster can match the Yiddish form embedded in a Latin-
        # headed DB name. Mirrors the in-parenthetical extraction below.
        # matching-core candidate: the script-run extraction itself is generic
        # (proposed matching_core.normalize.script_runs(text, script), built on
        # detect_script). The ' - ' split + parenthetical conventions + variant
        # assembly stay Orgs domain. See LEDGER.md (2026-05-27, script_runs).
        for m in _yiddish_runs(part):
            push(m.strip())
        # Extract each parenthetical body
        for body in re.findall(r"\(([^)]*)\)", part):
            body = body.strip()
            if not body:
                continue
            push(body)
            # If body mixes scripts, also surface the longest Yiddish run
            for m in _yiddish_runs(body):
                push(m.strip())
    return out


def latin_only(text: str) -> str:
    # DM soundex is most useful on Latin-script forms.
    if not text:
        return ""
    return " ".join(re.findall(r"[A-Za-z]+", text))


def dm_codes(text: str) -> set[str]:
    if not HAS_JELLYFISH:
        return set()
    lt = latin_only(text)
    if not lt:
        return set()
    try:
        out = jellyfish.daitch_mokotoff_soundex(lt)
    except Exception:
        return set()
    if isinstance(out, str):
        return {code for code in out.split("|") if code}
    if isinstance(out, (list, tuple, set)):
        return {str(x) for x in out if str(x)}
    return set()


def main() -> None:
    for p in (CLUSTERED, CORE_DB):
        if not p.exists():
            raise FileNotFoundError(f"Missing required file: {p}")

    with CLUSTERED.open(newline="", encoding="utf-8") as f:
        clustered_rows = list(csv.DictReader(f, delimiter="\t"))

    with CORE_DB.open(newline="", encoding="utf-8") as f:
        core_db_rows = list(csv.DictReader(f, delimiter="\t"))

    prev_by_cluster_id: dict[str, dict[str, str]] = {}
    prev_by_semantic: dict[tuple[str, str, str], dict[str, str]] = {}
    if OUT.exists():
        with OUT.open(newline="", encoding="utf-8") as f:
            prev_rows = list(csv.DictReader(f, delimiter="\t"))
        for r in prev_rows:
            cid = r.get("cluster_id", "").strip()
            if cid:
                prev_by_cluster_id[cid] = r
            sem_key = semantic_identity_key(
                r.get("canonical_yiddish", ""),
                r.get("org_type", ""),
                r.get("name_variants", ""),
            )
            prev_by_semantic[sem_key] = r

    # Aggregate one record per cluster with helpful context fields.
    cluster_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in clustered_rows:
        cid = row.get(COL_CLUSTER_ID, "").strip()
        if cid:
            cluster_groups[cid].append(row)

    cluster_records: list[dict[str, str]] = []
    for cid, rows in cluster_groups.items():
        rep = rows[0]
        name_variants = sorted({best_name(r) for r in rows if best_name(r)})
        settlements = sorted({r.get(COL_SETTLEMENT, "").strip() for r in rows if r.get(COL_SETTLEMENT, "").strip()})
        addresses = sorted({r.get(COL_ADDRESS, "").strip() for r in rows if r.get(COL_ADDRESS, "").strip()})
        venues = sorted({r.get(COL_VENUE, "").strip() for r in rows if r.get(COL_VENUE, "").strip()})
        countries = sorted({r.get(COL_COUNTRY, "").strip() for r in rows if r.get(COL_COUNTRY, "").strip()})
        cluster_records.append(
            {
                "cluster_id": cid,
                "canonical_yiddish": rep.get(COL_CANONICAL, "").strip(),
                "org_type": rep.get(COL_ORG_TYPE, "").strip(),
                "cluster_size": rep.get(COL_CLUSTER_SIZE, "").strip() or str(len(rows)),
                "name_variants": pipe_join_distinct(name_variants),
                "extracted_settlements": " | ".join(settlements),
                "extracted_addresses": " | ".join(addresses),
                "extracted_venues": " | ".join(venues),
                "extracted_countries": " | ".join(countries),
            }
        )

    # ── Blocking design ─────────────────────────────────────────────────
    # Block by org_type with explicit equivalence classes for related types.
    # Empty-type DB rows are "fail-open" — they join every block (so the ~106
    # untyped DB rows can still match any cluster). Clusters with empty
    # org_type also fall back to matching against everything.
    TYPE_EQUIVALENCE_CLASSES = [
        # Publication-adjacent: a Publisher cluster can match a DB row tagged
        # Printer/Publisher etc. (this directly addresses the Leksikon miss).
        frozenset({
            "Publisher", "Printer", "Printer/Publisher", "Journals/ Newspapers",
        }),
    ]
    _TYPE_TO_BLOCK: dict[str, str] = {}
    for cls in TYPE_EQUIVALENCE_CLASSES:
        canonical = min(cls)  # lexicographic representative
        for t in cls:
            _TYPE_TO_BLOCK[t] = canonical

    def block_key(org_type: str) -> str:
        t = (org_type or "").strip()
        if not t:
            return ""  # empty -> fail-open pool
        return _TYPE_TO_BLOCK.get(t, t)

    # Precompute DB variants and phonetic codes.
    db_entries: list[dict[str, object]] = []
    for row in core_db_rows:
        db_id = row.get("db_id", "").strip()
        if not db_id:
            continue
        # Skip deprecated rows: they've been merged into another db_id and
        # should not be proposed as candidates. The `merged_into` column on
        # the deprecated row records the canonical target.
        if (row.get("deprecated", "") or "").strip().lower() == "true":
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
        # name_yiddish_translit is a blocking-only auxiliary Yiddish form
        # (auto-transliterated from Latin). It surfaces Latin-only DB rows to
        # Yiddish clusters; it must NEVER be treated as canonical Yiddish.
        if db_name_yid_translit:
            for yv in split_name_variants(db_name_yid_translit):
                if yv and yv not in variants:
                    variants.append(yv)
            if db_name_yid_translit not in variants:
                variants.append(db_name_yid_translit)
        # Given-name-stripped surface for person-named troupes/companies. Only
        # meaningful when the DB row is Latin-only (no human-curated Yiddish);
        # we add both the Latin "Fiszon Troupe" and its transliteration so the
        # cluster's "<Surname>'s troupe" form blocks against it.
        if not db_name_yid:
            stripped = surname_only_variant(db_name)
            if stripped:
                for v in (stripped, translit_latin_to_yiddish(stripped)):
                    if v and v not in variants:
                        variants.append(v)
        norm_variants = [normalize_yiddish(v) for v in variants if v]
        alias_variants = sorted({a for v in variants for a in organization_name_aliases(v)})
        token_sets = [ts for ts in {token_key_set(v) for v in variants} if ts]
        dm = set()
        for v in variants:
            dm |= dm_codes(v)
        db_entries.append(
            {
                "db_id": db_id,
                "name": db_name,
                "org_type": row.get("org_type", "").strip(),
                "address": row.get("address", "").strip(),
                "variants": variants,
                "norm_variants": norm_variants,
                "alias_variants": alias_variants,
                "token_sets": token_sets,
                "dm_codes": dm,
            }
        )

    # Pre-group DB entries by block + collect the empty-type fail-open pool.
    db_by_block: dict[str, list[dict[str, object]]] = defaultdict(list)
    db_any_block: list[dict[str, object]] = []
    for d in db_entries:
        bk = block_key(d["org_type"])  # type: ignore[arg-type]
        if not bk:
            db_any_block.append(d)
        else:
            db_by_block[bk].append(d)
    print(
        f"Blocking: {len(db_any_block)} DB rows with empty org_type "
        f"(fail-open pool), {sum(len(v) for v in db_by_block.values())} typed across "
        f"{len(db_by_block)} blocks."
    )

    # Reviewer tags whose canonical_yiddish in the alignment TSV should
    # OVERRIDE the upstream kimatch canonical on re-runs. finalize_qid_splits.py
    # renames _Q## sub-cluster canonicals to include a settlement disambiguator
    # (e.g. "ברוקלינער האָפּקינסאָן-טעאַטער"); without this, prepare_alignment
    # would clobber that rename on every re-run.
    _CANONICAL_OVERRIDE_REVIEWERS = {"auto_finalize_qid"}

    out_rows: list[dict[str, str]] = []
    preserved_count = 0
    for c in sorted(cluster_records, key=lambda x: x["cluster_id"]):
        prev_for_id = prev_by_cluster_id.get(c["cluster_id"])
        canonical_overridden = False
        if (
            prev_for_id is not None
            and prev_for_id.get("reviewer", "").strip() in _CANONICAL_OVERRIDE_REVIEWERS
            and prev_for_id.get("canonical_yiddish", "").strip()
        ):
            c["canonical_yiddish"] = prev_for_id["canonical_yiddish"].strip()
            canonical_overridden = True

        cname = c["canonical_yiddish"]
        cnorm = normalize_yiddish(cname)
        caliases = organization_name_aliases(cname)
        cdm = dm_codes(cname)
        # Token-set keys from the canonical plus each surface variant, so an OCR
        # variant or a reordered/possessive form can still align via tokens.
        ctoks_sources = [cname] + [
            v.strip() for v in (c["name_variants"] or "").split("|") if v.strip()
        ]
        ctoken_sets = [ts for ts in {token_key_set(v) for v in ctoks_sources} if ts]

        # Build the candidate pool for this cluster based on its block.
        c_block = block_key(c["org_type"])
        if c_block:
            candidate_pool = db_by_block.get(c_block, []) + db_any_block
        else:
            candidate_pool = db_entries  # empty-type cluster: compare against all

        prev = prev_for_id
        # A human decision is keyed to the stable cluster_id, so never drop it
        # just because the semantic key diverged (e.g. a reviewer renamed the
        # canonical on a _Q sub-cluster). The semantic-key gate still applies to
        # undecided rows, so stale candidate context doesn't ride along when a
        # cluster's content genuinely changed.
        prev_has_decision = bool(
            prev_for_id
            and (
                prev_for_id.get("decision", "").strip()
                or prev_for_id.get("aligned_db_id", "").strip()
                or prev_for_id.get("reviewer_notes", "").strip()
            )
        )
        if (
            prev is not None
            and not canonical_overridden
            and not prev_has_decision
            and not preserved_row_matches_cluster(prev, c)
        ):
            prev = None
        if prev is None:
            sem_key = semantic_identity_key(
                cname,
                c["org_type"],
                c["name_variants"],
            )
            prev = prev_by_semantic.get(sem_key)
        prev_decision = (prev or {}).get("decision", "").strip()
        prev_aligned = (prev or {}).get("aligned_db_id", "").strip()
        prev_notes = (prev or {}).get("reviewer_notes", "").strip()
        prev_settlement = (prev or {}).get("reviewer_settlement", "").strip()
        prev_address = (prev or {}).get("reviewer_address", "").strip()
        prev_reviewer = (prev or {}).get("reviewer", "").strip()
        prev_reviewed_at = (prev or {}).get("reviewed_at", "").strip()
        if prev_decision or prev_aligned or prev_notes:
            preserved_count += 1

        scored: dict[str, tuple[float, str]] = {}

        for d in candidate_pool:
            db_id = str(d["db_id"])
            best_score = 0.0
            best_method = ""

            # exact
            for nv in d["alias_variants"]:  # type: ignore[index]
                if cnorm and cnorm == nv:
                    best_score, best_method = 1.0, "exact"
                    break
            if best_score < 1.0:
                for ca in caliases:
                    if ca in d["alias_variants"]:  # type: ignore[operator]
                        best_score, best_method = 1.0, "exact"
                        break

            # phonetic
            if best_score < 1.0 and cdm and d["dm_codes"]:  # type: ignore[index]
                if cdm & d["dm_codes"]:  # type: ignore[operator]
                    best_score, best_method = 0.85, "phonetic"

            # fuzzy
            fuzzy_best = 0.0
            for ca in caliases:
                for nv in d["alias_variants"]:  # type: ignore[index]
                    sim = name_similarity(ca, nv)
                    if sim > fuzzy_best:
                        fuzzy_best = sim
            if fuzzy_best > best_score:
                best_score, best_method = fuzzy_best, "fuzzy"

            # IPA phonetic (cross-script; Yiddish/Hebrew <-> Latin/English)
            ipa_best = 0.0
            for ca in caliases:
                for v in d["variants"]:  # type: ignore[index]
                    sim = cross_script_similarity(ca, str(v))
                    if sim > ipa_best:
                        ipa_best = sim
            if ipa_best > best_score:
                best_score, best_method = ipa_best, "ipa_phonetic"

            # Token-set (order-independent, generic-head / genitive / city-
            # adjective stripped). Catches reordered + morphological variants
            # the string/phonetic signals miss: "טרופּעס פֿון מאָגולעסקאָ" ↔
            # "מאָגולעסקאָס טרופּע", "אוניווערזיטעט פון בערלין" ↔ "בערלינער
            # אוניווערזיטעט". Compared via the precomputed token_key_set frozensets.
            ts_best = 0.0
            for cts in ctoken_sets:
                for dts in d["token_sets"]:  # type: ignore[index]
                    if not cts or not dts:
                        continue
                    inter = cts & dts
                    if not inter or not any(len(t) >= 3 for t in inter):
                        continue
                    sim = len(inter) / len(cts | dts)
                    if sim > ts_best:
                        ts_best = sim
            if ts_best > best_score:
                best_score, best_method = ts_best, "token_set"

            # Keep only plausible candidates. Cross-script IPA matches use a
            # lower floor because the IPA approximation is lossy — without this,
            # Latin-only DB rows are invisible to Yiddish clusters.
            min_score = 0.40 if best_method == "ipa_phonetic" else 0.60
            if best_score >= min_score:
                prev = scored.get(db_id)
                if prev is None or best_score > prev[0]:
                    scored[db_id] = (best_score, best_method)

        ranked = sorted(scored.items(), key=lambda kv: kv[1][0], reverse=True)[:10]

        out_rows.append(
            {
                "cluster_id": c["cluster_id"],
                "canonical_yiddish": cname,
                "org_type": c["org_type"],
                "cluster_size": c["cluster_size"],
                "name_variants": c["name_variants"],
                "extracted_settlements": c["extracted_settlements"],
                "extracted_addresses": c["extracted_addresses"],
                "extracted_venues": c["extracted_venues"],
                "extracted_countries": c["extracted_countries"],
                "candidate_db_ids": " | ".join(k for k, _ in ranked),
                "candidate_scores": " | ".join(f"{v[0]:.3f}" for _, v in ranked),
                "candidate_methods": " | ".join(v[1] for _, v in ranked),
                "decision": prev_decision,
                "aligned_db_id": prev_aligned,
                "reviewer_notes": prev_notes,
                "reviewer_settlement": prev_settlement,
                "reviewer_address": prev_address,
                "reviewer": prev_reviewer,
                "reviewed_at": prev_reviewed_at,
            }
        )

    with OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "cluster_id",
                "canonical_yiddish",
                "org_type",
                "cluster_size",
                "name_variants",
                "extracted_settlements",
                "extracted_addresses",
                "extracted_venues",
                "extracted_countries",
                "candidate_db_ids",
                "candidate_scores",
                "candidate_methods",
                "decision",
                "aligned_db_id",
                "reviewer_notes",
                "reviewer_settlement",
                "reviewer_address",
                "reviewer",
                "reviewed_at",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Wrote {len(out_rows)} rows -> {OUT.name}")
    print(f"Preserved {preserved_count} existing alignment decisions/notes")
    if not HAS_JELLYFISH:
        print("Note: jellyfish not installed; phonetic signal was skipped.")
    if not HAS_DYBBUK_PHONETIC:
        print("Note: dybbuk-phonetic not installed; IPA phonetic signal was skipped.")


if __name__ == "__main__":
    main()
