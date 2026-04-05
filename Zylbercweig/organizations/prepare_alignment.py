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
)


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


def split_name_variants(name: str) -> list[str]:
    # DB Name often includes variants separated by ' - '.
    parts = [p.strip() for p in re.split(r"\s+-\s+", name or "") if p.strip()]
    if not parts and name:
        parts = [name.strip()]
    return parts


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

    # Precompute DB variants and phonetic codes.
    db_entries: list[dict[str, object]] = []
    for row in core_db_rows:
        db_id = row.get("db_id", "").strip()
        if not db_id:
            continue
        db_name = row.get("name", "").strip()
        variants = split_name_variants(db_name)
        if db_name and db_name not in variants:
            variants.append(db_name)
        norm_variants = [normalize_yiddish(v) for v in variants if v]
        alias_variants = sorted({a for v in variants for a in organization_name_aliases(v)})
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
                "dm_codes": dm,
            }
        )

    out_rows: list[dict[str, str]] = []
    preserved_count = 0
    for c in sorted(cluster_records, key=lambda x: x["cluster_id"]):
        cname = c["canonical_yiddish"]
        cnorm = normalize_yiddish(cname)
        caliases = organization_name_aliases(cname)
        cdm = dm_codes(cname)

        prev = prev_by_cluster_id.get(c["cluster_id"])
        if prev is not None and not preserved_row_matches_cluster(prev, c):
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
        if prev_decision or prev_aligned or prev_notes:
            preserved_count += 1

        scored: dict[str, tuple[float, str]] = {}

        for d in db_entries:
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

            # Keep only plausible candidates.
            if best_score >= 0.60:
                prev = scored.get(db_id)
                if prev is None or best_score > prev[0]:
                    scored[db_id] = (best_score, best_method)

        ranked = sorted(scored.items(), key=lambda kv: kv[1][0], reverse=True)[:5]

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
