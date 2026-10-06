"""Build the resolve-or-mint candidate table for plural-name 'bundle' rows.

A bundle row is a core_db row whose NAME enumerates several troupes
("די טרופּעס פֿון X, Y און Z"). It is a Zylbercweig index heading, not an
organization. Ruthie flagged 6 of these in the troupe-tags sheet; sweeping
core_db for plural head-nouns finds more.

For each named part we must decide RESOLVE (link the org that already exists)
or MINT (create it), because several parts already exist in core_db — minting
blind would duplicate them.

Output: bundle_resolve_candidates.tsv, one row per part, with existing-row
matches so a human picks per part.

NOTE on Yiddish matching: strip Hebrew points from BOTH the text and the
pattern. Stripping only one side silently matches nothing (dagesh U+05BC sits
inside טרופּעס).

Usage: python3 build_bundle_resolve_table.py
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
CLUSTERED = HERE / "organizations_clustered.tsv"
OUT = HERE / "bundle_resolve_candidates.tsv"

POINTS = re.compile(r"[֑-ׇ]")
def sp(s: str) -> str:
    return POINTS.sub("", s or "")

def norm(s: str) -> str:
    s = sp(s)
    s = re.sub(r"[׳״\"'“”„,\.\-–—()\[\]]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

PLURAL = re.compile("|".join(sp(w) for w in
                             ("טרופּעס", "קאָמפּאַניעס", "טעאַטערס", "טעאַטערן")))

# Bundles and their parts. `parts` = the surnames the heading enumerates.
# Rows deliberately EXCLUDED from splitting are recorded with why.
BUNDLES: dict[str, dict] = {
    "1044": {"parts": ["פֿרידמאַן", "וואָרגאַטש", "לאַמפּע"], "src": "Ruthie"},
    "1076": {"parts": ["איציקל גאָלדענבערג", "אַשכנזי", "קענער", "זיגלער"], "src": "Ruthie"},
    "1418": {"parts": ["פּאָטאָצקאַ", "באַראַטאָוו", "משה ליפּמאַן"], "src": "Ruthie"},
    "1419": {"parts": ["גענפער", "סאבּסיי", "קאַזשדאַן", "גראָסמאַן"], "src": "Ruthie"},
    "1420": {"parts": ["לערמאַן", "בערנשטיין", "בעקער"], "src": "Ruthie"},
    "1484": {"parts": ["ס-סלאָוו", "היידעמאַקו", "טשערנאָוו", "סוכאָדאָלסקי"], "src": "Ruthie"},
    "824":  {"parts": ["בעקער"], "src": "sweep"},
    "968":  {"parts": ["קרויזע-גורעוויטש"], "src": "sweep"},
    "1017": {"parts": ["טאַנצמאַן", "סוכמאַן"], "src": "sweep"},
    "1038": {"parts": ["גינאַ זלאָטאַיאָ"], "src": "sweep"},
    "1046": {"parts": ["מאַגידזאָן"], "src": "sweep"},
    "1074": {"parts": ["יידל גאָלדפאַדען", "נפתלי גאָלדפאַדען"], "src": "sweep"},
    "1346": {"parts": ["וויינשטאָק", "וויינשטיין"], "src": "sweep"},
}
NOT_BUNDLES = {
    "629": "All 16 mentions are ONE impresario (Sharavner); the plural appears "
           "in only 2 sentences and is loose phrasing, not an enumeration.",
    "1714": "יידישע קאָאָפּעראַטיווע טרופּעס is a generic category ('Jewish "
            "cooperative troupes'), not named troupes. No names to split on.",
}


def load(p: Path) -> list[dict]:
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    core = load(CORE_DB)
    by_id = {r["db_id"]: r for r in core}
    clustered = load(CLUSTERED)
    ment_by_cid: dict[str, list[dict]] = {}
    for r in clustered:
        ment_by_cid.setdefault(r.get("cluster_id", "").strip(), []).append(r)

    rows_out = []
    for db, spec in BUNDLES.items():
        src_row = by_id.get(db)
        if src_row is None:
            continue
        cids = [c.strip() for c in (src_row["linked_cluster_ids"] or "").split("|") if c.strip()]
        n_ment = sum(len(ment_by_cid.get(c, [])) for c in cids)
        sents = []
        for c in cids:
            for m in ment_by_cid.get(c, []):
                s = m["_ - organizations - _ - relations - _ - original_sentence"].strip()
                if s:
                    sents.append(s)

        for part in spec["parts"]:
            pn = norm(part)
            toks = [t for t in pn.split() if len(t) > 2]
            matches = []
            for r in core:
                if r["db_id"] == db:
                    continue
                if (r.get("deprecated") or "").strip() or (r.get("merged_into") or "").strip():
                    continue
                blob = norm(f"{r['name_yiddish']} {r['name']} {r['name_variants']}")
                if not toks or not all(t in blob for t in toks):
                    continue
                # exact = the existing name is just "<part>s troupe" and nothing else
                bare = re.sub(r"\b(טרופּע|טרופע|פֿון|פון|ס)\b", " ", blob)
                bare = re.sub(r"\s+", " ", bare).strip()
                exact = norm(bare) == pn or blob == norm(f"{part}ס טרופּע")
                matches.append((r["db_id"], r["name_yiddish"] or r["name"],
                                r["org_type"], "exact" if exact else "partial"))
            matches.sort(key=lambda m: (m[3] != "exact", m[0]))
            rows_out.append({
                "bundle_db_id": db,
                "bundle_name": src_row["name_yiddish"] or src_row["name"],
                "bundle_mentions": n_ment,
                "flagged_by": spec["src"],
                "part": part,
                "n_existing_matches": len(matches),
                "best_match_kind": matches[0][3] if matches else "",
                "candidates": " | ".join(f"db{d}={n} ({t}) [{k}]" for d, n, t, k in matches[:5]),
                "suggested": ("RESOLVE" if matches and matches[0][3] == "exact"
                              else "REVIEW" if matches else "MINT"),
                "source_sentence": sents[0][:300] if sents else "",
            })

    fields = list(rows_out[0].keys())
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows_out)

    from collections import Counter
    c = Counter(r["suggested"] for r in rows_out)
    print(f"{len(rows_out)} parts across {len(BUNDLES)} bundles -> {OUT.name}")
    print(f"  RESOLVE (exact existing row): {c['RESOLVE']}")
    print(f"  REVIEW  (partial match only): {c['REVIEW']}")
    print(f"  MINT    (no existing row):    {c['MINT']}")
    print("\nExcluded from splitting:")
    for db, why in NOT_BUNDLES.items():
        print(f"  db{db}: {why}")
    for r in rows_out:
        print(f"\ndb{r['bundle_db_id']} part {r['part']!r} -> {r['suggested']}")
        print(f"   {r['candidates'] or '(no existing row)'}")


if __name__ == "__main__":
    main()
