#!/usr/bin/env python3
"""
Collapse מלוכה / state-theater clusters per settlement.

User-defined scope (2026-05-24):
  • Only plain Theatre-type clusters with a generic "state theater" name.
  • Keep functionally distinct theaters separate: children's (קינדער),
    young-viewer (פֿאַרן יונגען צושויער), puppet (פּופּן), opera/operetta/
    philharmonic, mobile/traveling (באַוועגלעך), studios, schools, etc.
  • National / republic-level theaters (Byelorussian, All-Ukrainian, of-Poland,
    of-Ukraine, Moldavian, Latvian, All-Union, …) are EXCLUDED from the per-city
    merge — the national name overrides the settlement — and are instead merged
    among themselves by their national key.

Two passes:
  CITY      — group qualifying clusters by resolved settlement QID  → one entity.
  NATIONAL  — group national clusters by nation key                 → one entity.

Decision is made at the *cluster* level (one row per cluster_id), keyed on the
cluster's canonical_yiddish (its representative name).

Default run writes an audit TSV only. Pass --apply to also rewrite
organizations_clustered.tsv (remapping cluster_id / canonical_yiddish /
cluster_size for merged clusters). A timestamped backup is written first.

Usage:
    python3 collapse_state_theaters.py            # audit only
    python3 collapse_state_theaters.py --apply    # audit + rewrite clustered TSV
"""
from __future__ import annotations

import csv
import re
import sys
import shutil
import datetime
import unicodedata
import collections
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settlement_resolver import get_resolver  # noqa: E402

csv.field_size_limit(10**9)

_BASE = Path(__file__).resolve().parent
_CLUSTERED = _BASE / "organizations_clustered.tsv"
_AUDIT = _BASE / "state_theater_collapse_audit.tsv"

_TITLE_COL = "_ - organizations - _ - title"
_SETT_COL = "_ - organizations - _ - locations - _ - settlement"
_TYPE_COL = "_ - organizations - _ - org_type"

# ── Normalization ────────────────────────────────────────────────────────────
# The corpus mixes precomposed presentation forms (פֿ = U+FB4E) with bare
# consonants, and carries dagesh/rafe/niqqud inconsistently. Strip all of it so
# markers can be written once in bare (point-free) Yiddish.
def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return unicodedata.normalize("NFKC", s)


# A cluster qualifies as a *state theater* if its name carries the מלוכה stem.
_MELUCHE = re.compile(r"מלוכ")

# Functionally distinct institutions that must stay separate even when Theatre-typed.
# Patterns are written against _norm()-ed (point-free) text.
_EXCLUDE_PATTERNS = [
    ("children",      re.compile(r"קינדער")),
    ("young_viewer",  re.compile(r"יונג(ע|ן|ען)\s*צושויער")),
    ("puppet",        re.compile(r"פופ")),                # פּופּן
    ("opera",         re.compile(r"אפער")),               # אָפּערע / אָפּערעט
    ("philharmonic",  re.compile(r"פילהארמ")),
    ("mobile",        re.compile(r"באוועגלע")),           # באַוועגלעכן / באַוועגלעך
    ("chamber",       re.compile(r"קאמער[\-\s]?טעאטער")),
    ("small_forms",   re.compile(r"קליינ")),              # small-forms / Maly ("kleyn") theaters
    # education / non-theatre institutions (also caught by org_type, kept for safety)
    ("school",        re.compile(r"שול|סעמינאר|גימנאזיע")),
    ("institute",     re.compile(r"אינסטיטוט|טעכניקום|קאנסערוואטאריע|קורסן|ווארשטאט")),
    ("studio",        re.compile(r"סטודיע")),
    ("publisher",     re.compile(r"פארלאג")),
]

# National / republic-level qualifiers → canonical nation key.
# Byelorussian must precede Russian (ווייסרוסיש contains רוסיש).
_NATION_PATTERNS = [
    ("Byelorussian", re.compile(r"ווייסרוס")),            # ווייסרוסיש / ווייסרוסלענדיש
    ("Ukrainian",    re.compile(r"אוקרא")),               # אַלאוקראַאינישן / אוקראַאינישן / פֿון אוקראַאינע
    ("Moldavian",    re.compile(r"מאלד")),                # מאָלדאָוואַניש / מאָלדעוואָניש / מאָלדאָוויש
    ("Latvian",      re.compile(r"לעטיש")),
    ("Romanian",     re.compile(r"רומעניש")),
    ("Russian",      re.compile(r"רוסיש")),
    ("Polish",       re.compile(r"פויליש|פון פוילן|אין פוילן")),
    ("Uzbek",        re.compile(r"אוזבעקיש")),
    ("Kirghiz",      re.compile(r"קירגיז")),
    ("AllUnion",     re.compile(r"אלפארבאנד")),
    ("Soviet_state", re.compile(r"ראטנפארבאנד|סאוועטנפארבאנד")),
    ("Federal",      re.compile(r"פעדעראל")),             # US Federal Theatre Project
]

# Itinerant state theaters: touring companies whose location-based QID-explode
# splits (ORG-Cxxxxx_Qnn) must be recombined into ONE entity regardless of the
# performance city. Maps the *base* cluster id → the company's home-city QID so it
# folds into that city's group, overriding the per-cluster name/settlement.
# (PI 2026-05-24: the Moscow GOSET tours, so its _Q02/_Q03/_Q04 stops rejoin.)
_ITINERANT_BASES = {
    "ORG-C00184": ("Q649", "Moscow"),   # Moscow State Yiddish Theatre (GOSET)
}
_BASE_RE = re.compile(r"^(ORG-C\d+)(?:_Q\d+)?$")

# Cities the kima-backed resolver doesn't key; matched on _norm()-ed text so all
# spellings collapse to one group. (Birobidzhan OCR varies wildly: ביראָבידזשאַן /
# ביראָבידזשאָן / "ביראָ ביאָזשאַנער".)
_CITY_ALIASES = [
    ("Baku",        re.compile(r"באקו")),
    ("Birobidzhan", re.compile(r"ביראב?ידזש|ביראב?יאזש|בירא בידזש|בירא ביאזש")),
]


def classify(name: str) -> tuple[str, str]:
    """Return (decision, key). decision ∈ {EXCLUDE, NATIONAL, CITY}."""
    n = _norm(name)
    for reason, pat in _EXCLUDE_PATTERNS:
        if pat.search(n):
            return "EXCLUDE", reason
    for nation, pat in _NATION_PATTERNS:
        if pat.search(n):
            return "NATIONAL", nation
    return "CITY", ""


def main() -> None:
    apply = "--apply" in sys.argv
    resolver = get_resolver()

    with _CLUSTERED.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    # ── Build per-cluster representatives over the מלוכה Theatre universe ──────
    clusters: dict[str, dict] = {}
    for r in rows:
        if not _MELUCHE.search(r.get(_TITLE_COL, "")) and not _MELUCHE.search(r.get("canonical_yiddish", "")):
            continue
        if (r.get(_TYPE_COL) or "").strip() != "Theatre":
            continue
        cid = r["cluster_id"]
        c = clusters.setdefault(cid, {
            "cluster_id": cid,
            "canonical": r.get("canonical_yiddish", "").strip(),
            "rows": 0,
            "settlements": collections.Counter(),
            "titles": collections.Counter(),
        })
        c["rows"] += 1
        s = (r.get(_SETT_COL) or "").strip()
        if s:
            c["settlements"][s] += 1
        t = (r.get(_TITLE_COL) or "").strip()
        if t:
            c["titles"][t] += 1

    # ── Decide each cluster ───────────────────────────────────────────────────
    def city_candidates(c: dict) -> list[str]:
        # The cluster's OWN name wins over the settlement field — a row's
        # settlement is often a tour/evacuation stop (e.g. the Moscow GOSET
        # listed under Kyiv). Name signals first, settlement field last.
        cands: list[str] = []
        name = c["canonical"] or (c["titles"].most_common(1)[0][0] if c["titles"] else "")
        # leading adjective ("קיעווער …") and "אין/פֿון <city>"
        m = re.match(r"\s*([א-תְ-ׇּֿ'’\-]+ער)\b", name)
        if m:
            cands.append(m.group(1))
        for mm in re.finditer(r"(?:אין|פֿון|פון)\s+([א-תְ-ׇּֿ'’\-]+)", name):
            cands.append(mm.group(1))
        cands += [s for s, _ in c["settlements"].most_common()]
        return cands

    for c in clusters.values():
        name = c["canonical"] or (c["titles"].most_common(1)[0][0] if c["titles"] else "")
        c["name"] = name
        c["city_qid"] = c["city_label"] = c["group_key"] = ""

        # Itinerant override: recombine a touring company's _Qnn splits.
        bm = _BASE_RE.match(c["cluster_id"])
        base = bm.group(1) if bm else c["cluster_id"]
        if base in _ITINERANT_BASES:
            qid, label = _ITINERANT_BASES[base]
            c["decision"] = "ITINERANT"
            c["reason"] = base
            c["city_qid"] = qid
            c["city_label"] = label
            c["group_key"] = f"CITY::{qid}"
            continue

        decision, key = classify(name)
        c["decision"] = decision
        c["reason"] = key
        if decision == "NATIONAL":
            c["group_key"] = f"NAT::{key}"
        elif decision == "CITY":
            cands = city_candidates(c)
            qid = label = ""
            for cand in cands:
                res = resolver.resolve(cand)
                if res:
                    qid, label = res.qid, res.english or res.yiddish
                    break
            if qid:
                c["city_qid"] = qid
                c["city_label"] = label
                c["group_key"] = f"CITY::{qid}"
            else:
                # Resolver miss — try the manual alias list on normalized text.
                hay = " ".join(_norm(x) for x in cands)
                alias = next((lbl for lbl, pat in _CITY_ALIASES if pat.search(hay)), "")
                if alias:
                    c["city_label"] = alias
                    c["group_key"] = f"CITY::{alias}"
                else:
                    # No resolvable settlement → cannot safely place; exclude.
                    c["decision"] = "EXCLUDE"
                    c["reason"] = "unresolved_settlement"

    # ── Form merge groups (size ≥ 2 only) ─────────────────────────────────────
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for c in clusters.values():
        if c["group_key"]:
            groups[c["group_key"]].append(c)

    merge_map: dict[str, dict] = {}  # old_cid -> {target_cid, target_canonical}
    for gk, members in groups.items():
        if len(members) < 2:
            continue
        # target cluster = most rows, tie-break lexicographic
        members_sorted = sorted(members, key=lambda c: (-c["rows"], c["cluster_id"]))
        target = members_sorted[0]
        # canonical name = the target cluster's name
        for c in members:
            merge_map[c["cluster_id"]] = {
                "target_cid": target["cluster_id"],
                "target_canonical": target["name"],
            }

    # ── Audit TSV ─────────────────────────────────────────────────────────────
    with _AUDIT.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([
            "cluster_id", "decision", "reason_or_nation", "city_qid", "city_label",
            "n_rows", "name", "group_key", "merged", "target_cluster_id", "target_canonical",
        ])
        for c in sorted(clusters.values(), key=lambda c: (c["decision"], c["group_key"], c["cluster_id"])):
            m = merge_map.get(c["cluster_id"])
            w.writerow([
                c["cluster_id"], c["decision"], c["reason"], c["city_qid"], c["city_label"],
                c["rows"], c["name"], c["group_key"],
                "YES" if m else "", m["target_cid"] if m else "", m["target_canonical"] if m else "",
            ])

    # ── Summary ───────────────────────────────────────────────────────────────
    n_city_groups = sum(1 for gk, ms in groups.items() if gk.startswith("CITY::") and len(ms) >= 2)
    n_nat_groups = sum(1 for gk, ms in groups.items() if gk.startswith("NAT::") and len(ms) >= 2)
    n_merged_clusters = len(merge_map)
    n_targets = len({m["target_cid"] for m in merge_map.values()})
    print(f"מלוכה Theatre clusters considered : {len(clusters)}")
    print(f"  EXCLUDED (functional/unresolved): {sum(1 for c in clusters.values() if c['decision']=='EXCLUDE')}")
    print(f"  CITY merge groups (≥2)          : {n_city_groups}")
    print(f"  NATIONAL merge groups (≥2)      : {n_nat_groups}")
    print(f"  clusters folded                 : {n_merged_clusters} → {n_targets} targets")
    print(f"Audit written → {_AUDIT.relative_to(_BASE.parent.parent)}")

    if not apply:
        print("\n(dry run — pass --apply to rewrite organizations_clustered.tsv)")
        return

    # ── Apply: remap cluster_id / canonical_yiddish / cluster_size ────────────
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = _CLUSTERED.with_name(f"organizations_clustered_pre_state_collapse_{ts}.tsv")
    shutil.copy2(_CLUSTERED, backup)
    print(f"\nBackup → {backup.name}")

    for r in rows:
        m = merge_map.get(r["cluster_id"])
        if m:
            r["cluster_id"] = m["target_cid"]
            r["canonical_yiddish"] = m["target_canonical"]

    sizes = collections.Counter(r["cluster_id"] for r in rows)
    for r in rows:
        if "cluster_size" in r:
            r["cluster_size"] = str(sizes[r["cluster_id"]])

    with _CLUSTERED.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"Rewrote {_CLUSTERED.name}: {n_merged_clusters} clusters folded into {n_targets}.")
    print("Next: re-run prepare_alignment.py (carries decisions by semantic key), "
          "then build_core_db.py.")


if __name__ == "__main__":
    main()
