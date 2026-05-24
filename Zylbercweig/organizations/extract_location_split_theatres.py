#!/usr/bin/env python3
"""
PI-review export: Theatre-type org clusters that look like ONE entity split
across locations — i.e. clusters whose names are the same once the settlement
qualifier is stripped ("Kiever X" / "X in Kiev" / "X fun Kiev" → "X").

These are the candidates for "itinerant despite being typed Theatre" (like the
Moscow GOSET): a touring company the pipeline split per performance city, either
via the QID-explode `_Qnn` splitter or as separate near-duplicate clusters.

For each name-group spanning ≥2 distinct settlements we emit one row per member
cluster, with its aligned DB id (if any) so the PI can decide per entity.

Output: location_split_theatres_for_pi.csv
"""
from __future__ import annotations

import csv
import re
import sys
import collections
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from collapse_state_theaters import _norm, _BASE_RE  # noqa: E402
from settlement_resolver import get_resolver  # noqa: E402

csv.field_size_limit(10**9)

_BASE = Path(__file__).resolve().parent
_CLUSTERED = _BASE / "organizations_clustered.tsv"
_ALIGN = _BASE / "org_alignment_review.tsv"
_CORE = _BASE / "core_db.tsv"
_OUT = _BASE / "location_split_theatres_for_pi.csv"

_TITLE = "_ - organizations - _ - title"
_SETT = "_ - organizations - _ - locations - _ - settlement"
_TYPE = "_ - organizations - _ - org_type"

_FINALS = str.maketrans("ךםןףץ", "כמנפצ")

# Generic descriptors that, on their own, don't identify a single entity. A
# residual name made only of these (after the city is stripped) is "generic"
# — every town had a "Yiddish theater". Anything left over (a proper name like
# Liberty / Lyric / Metropolitan, a quoted title, a person) makes it "specific".
_GENERIC_TOKENS = {
    "יידיש", "יידישנ", "יידישער", "יידישע", "אידיש", "אידישנ",
    "טעאטער", "טעאטר", "טעאטערס", "טעאטראל",
    "פאלקס", "בינע", "דראמאטיש", "דראמאטישער", "דראמאטישנ", "דראמע",
    "ניי", "נייער", "נייע", "ערשט", "ערשטער", "ערשטנ", "ערשטע",
    "גרויס", "גרויסער", "קליינ", "קליינער", "קליינע",
    "אפער", "אפערע", "אפערעט", "אפערעטנ", "אפערעטע",
    "מוזיקאליש", "מוזיקאלישנ", "קונסט", "סאוועטיש", "סאוויעטיש",
    "מלוכה", "מלוכהשנ", "מלוכישנ", "פראלעטאריש", "ארבעטער",
    "פֿאלקס", "אידישער", "אידישע",
}


def _fold(s: str) -> str:
    return _norm(s).translate(_FINALS).lower()


def main() -> None:
    resolver = get_resolver()

    def is_city(tok: str) -> bool:
        return bool(tok) and resolver.resolve(tok) is not None

    def strip_city(name: str) -> str:
        """Remove a leading city-adjective and 'in/fun <city>' phrases."""
        n = name
        # leading adjective token, only if it resolves to a city
        m = re.match(r"\s*([א-תְ-ׇּֿ'’\-]+(?:ישער|ישע|ער))\b\s*(.*)", n, re.S)
        if m and is_city(m.group(1)):
            n = m.group(2)
        # "אין/פֿון/פון <city>" phrases
        def _drop(mm: re.Match) -> str:
            return "" if is_city(mm.group(1)) else mm.group(0)
        n = re.sub(r"(?:אין|פֿון|פון)\s+([א-תְ-ׇּֿ'’\-]+)", _drop, n)
        return n

    def name_key(name: str) -> str:
        stripped = strip_city(name)
        folded = _fold(stripped)
        return re.sub(r"[\s\-־]+", "", folded)

    def is_specific(name: str) -> bool:
        """True if the city-stripped name carries a token beyond generic
        theater-words (i.e. a distinctive proper name)."""
        stripped = strip_city(name)
        if re.search(r"[\"„“”»«]|א['׳]ר?\b|נאמען", _norm(stripped)):
            return True  # quoted title or "named after / a'r"
        toks = re.split(r"[\s\-־]+", _fold(stripped))
        return any(t and t not in _GENERIC_TOKENS for t in toks)

    # ── cluster → aligned db_id ───────────────────────────────────────────────
    align: dict[str, tuple[str, str]] = {}  # cluster_id -> (decision, db_id)
    if _ALIGN.exists():
        with _ALIGN.open(encoding="utf-8") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                cid = r.get("cluster_id", "")
                if cid:
                    align[cid] = (r.get("decision", ""), r.get("aligned_db_id", ""))
    db_name: dict[str, str] = {}
    if _CORE.exists():
        with _CORE.open(encoding="utf-8") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                db_name[r.get("db_id", "")] = r.get("name", "")

    # ── per-cluster reps over Theatre rows ────────────────────────────────────
    clusters: dict[str, dict] = {}
    with _CLUSTERED.open(encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if (r.get(_TYPE) or "").strip() != "Theatre":
                continue
            cid = r["cluster_id"]
            c = clusters.setdefault(cid, {
                "cluster_id": cid, "canon": r.get("canonical_yiddish", "").strip(),
                "rows": 0, "setts": collections.Counter(), "titles": collections.Counter(),
            })
            c["rows"] += 1
            if (r.get(_SETT) or "").strip():
                c["setts"][r[_SETT].strip()] += 1
            if (r.get(_TITLE) or "").strip():
                c["titles"][r[_TITLE].strip()] += 1

    # ── group by name-key ─────────────────────────────────────────────────────
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for c in clusters.values():
        name = c["canon"] or (c["titles"].most_common(1)[0][0] if c["titles"] else "")
        c["name"] = name
        key = name_key(name)
        if len(key) < 4:        # too short to be a meaningful name
            continue
        c["key"] = key
        c["settlement"] = c["setts"].most_common(1)[0][0] if c["setts"] else ""
        groups[key].append(c)

    rows_out = []
    gid = 0
    for key, members in groups.items():
        setts = {m["settlement"] for m in members if m["settlement"]}
        if len(members) < 2 or len(setts) < 2:
            continue          # not "separated across locations"
        gid += 1
        bases = collections.Counter(
            (_BASE_RE.match(m["cluster_id"]).group(1) if _BASE_RE.match(m["cluster_id"]) else m["cluster_id"])
            for m in members
        )
        rep = max(members, key=lambda x: x["rows"])["name"]
        specific = "specific" if is_specific(rep) else "generic"
        for m in sorted(members, key=lambda x: -x["rows"]):
            base = _BASE_RE.match(m["cluster_id"]).group(1) if _BASE_RE.match(m["cluster_id"]) else m["cluster_id"]
            dec, dbid = align.get(m["cluster_id"], ("", ""))
            rows_out.append({
                "group_id": f"G{gid:03d}",
                "n_clusters": len(members),
                "n_settlements": len(setts),
                "specificity": specific,
                "split_family": "YES" if bases[base] > 1 else "",
                "name_key": key,
                "cluster_id": m["cluster_id"],
                "base_cluster": base,
                "settlement": m["settlement"],
                "canonical_name": m["name"],
                "cluster_size": m["rows"],
                "decision": dec,
                "aligned_db_id": dbid,
                "db_name": db_name.get(dbid, ""),
            })

    fields = ["group_id", "n_clusters", "n_settlements", "specificity", "split_family",
              "name_key", "cluster_id", "base_cluster", "settlement", "canonical_name",
              "cluster_size", "decision", "aligned_db_id", "db_name"]
    # specific groups first, then by group size desc
    rows_out.sort(key=lambda r: (r["specificity"] != "specific", -r["n_clusters"], r["group_id"]))
    with _OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows_out)

    n_groups = len({r["group_id"] for r in rows_out})
    n_specific = len({r["group_id"] for r in rows_out if r["specificity"] == "specific"})
    n_aligned = sum(1 for r in rows_out if r["aligned_db_id"])
    print(f"Theatre clusters scanned         : {len(clusters)}")
    print(f"Location-split name-groups (≥2)  : {n_groups}  ({n_specific} specific)")
    print(f"Member cluster rows              : {len(rows_out)}  ({n_aligned} with an aligned db_id)")
    print(f"Wrote → {_OUT.name}")


if __name__ == "__main__":
    main()
