#!/usr/bin/env python3.11
"""Review sheet for the publishers / periodicals the editions resolver did not
link to core_db (and for core_db rows that look like duplicates of each other).

One row per CLUSTER of extracted strings: spelling variants of one name
("קינדער זשורנאַל" / "קינדער-זשורנאָל" / "קינדער זשורנאל") share the
cross-script skeleton of their norm2 key and are reviewed once. Each row
carries the resolver's basis, the candidate core_db rows (proposals are never
links), heuristic flags, and empty decision columns.

Decision vocabulary (column `decision`):
  MATCH        -> target_db_id = the existing core_db row
  NEW          -> a real org absent from core_db; minted by the APP, never here
  NOT_AN_ORG   -> a phrase, a person, a place, a book title
  GENERIC      -> a real org but unidentifiable from the string ("ליט. פֿאַרלאָג")
  SPLIT        -> the string names several orgs ("דביר, אַחיאַסף"); list ids in note

Second section (`role=core_dup`): one extracted cluster linked to SEVERAL
core_db rows of the same kind -- candidates for a merge in the app.

Writes org_review_<tag>.tsv; reads editions_resolved_<tag>.tsv + core_db.
Deterministic; no LLM.

    python3.11 build_org_review.py [--tag full2]
"""
from __future__ import annotations

import argparse
import collections
import re

from resolve_editions import (GENERIC_TOK, HERE, ZYL, PLAY_KINDS, PRESS_TYPES,
                              PUBLISHER_TYPES, initials, norm2, pc,
                              place_agree, skeleton)

UNRESOLVED = ("no_match", "proposed_", "ambiguous", "place_conflict")
FIELDS = ["role", "n_editions", "n_entries", "surface_forms", "places",
          "basis", "candidates", "flags", "examples", "decision",
          "target_db_id", "note", "cluster_key"]


def flags_for(name: str, places: set[str]) -> list[str]:
    f = []
    if "/" in name or name.count("„") > 1 or re.search(r"[,\s]ו[„\"]", name):
        f.append("multi?")
    if re.search(r"אָדער|אדער|\(|בוכהענדלער|…", name):
        f.append("phrase?")
    if not norm2(name):
        f.append("no_name_left")
    elif any(place_agree(name, p) for p in places if p) and len(name.split()) <= 2:
        f.append("place_name?")
    if initials(name):
        f.append("person_as_firm?")
    return f


def cand_label(d: str, core: dict) -> str:
    r = core.get(d)
    if not r:
        return d
    name = r["name"] or r["name_yiddish"] or r["name_variants"].split("|")[0]
    return f"{d}: {name[:50]} [{r['org_type']}{' | ' + r['address'][:25] if r['address'] else ''}]"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="full2")
    a = ap.parse_args()
    eds = [r for r in pc.read_tsv(HERE / f"editions_resolved_{a.tag}.tsv")
           if r["work_kind"] in PLAY_KINDS]
    core = {r["db_id"]: r for r in pc.read_tsv(ZYL / "organizations" / "core_db.tsv")}

    clusters = collections.defaultdict(list)    # (role, key) -> [(edition, basis)]
    linked = collections.defaultdict(lambda: collections.defaultdict(int))
    for r in eds:
        for role, col in (("publisher", "publisher"), ("periodical", "container")):
            if role == "periodical" and r["pub_kind"] != "periodical":
                continue
            name, basis = r[col], r[f"{col}_match"]
            if not name:
                continue
            n2 = norm2(name)
            key = skeleton(n2[0]) if n2 else "~" + pc.norm_yiddish(name)
            if r[f"{col}_db_id"]:
                linked[(role, key)][r[f"{col}_db_id"]] += 1
            if basis.startswith(UNRESOLVED):
                clusters[(role, key)].append((r, name, basis))

    out = []
    for (role, key), items in clusters.items():
        forms = collections.Counter(n for _, n, _ in items)
        places = collections.Counter(r["place"] for r, _, _ in items if r["place"])
        bases = collections.Counter(b.split(":")[0] for _, _, b in items)
        cands = collections.Counter()
        for _, _, b in items:
            if ":" in b:
                cands.update(b.split(":", 1)[1].split(","))
        # the same cluster linked elsewhere (other spellings matched): the
        # obvious MATCH, shown first
        for d, n in linked.get((role, key), {}).items():
            cands[d] += 100 * n
        ex = []
        for r, _, _ in items[:3]:
            ex.append(f"{r['heading'][:30]}: {r['title'][:30]} {r['year']}".strip())
        out.append({
            "role": role, "n_editions": len(items),
            "n_entries": len({r["xml_id"] for r, _, _ in items}),
            "surface_forms": " ‖ ".join(f"{n} ×{c}" if c > 1 else n
                                        for n, c in forms.most_common(6)),
            "places": " ‖ ".join(p for p, _ in places.most_common(4)),
            "basis": " ".join(f"{b}×{c}" for b, c in bases.most_common()),
            "candidates": " ‖ ".join(cand_label(d, core) for d, _ in cands.most_common(5)),
            "flags": " ".join(sorted({f for n in forms for f in flags_for(n, set(places))})),
            "examples": " ‖ ".join(ex),
            "decision": "", "target_db_id": "", "note": "", "cluster_key": key,
        })

    # core_db duplicate suspects: one cluster, several linked rows
    dups = []
    for (role, key), ids in linked.items():
        if len(ids) > 1:
            dups.append({
                "role": "core_dup", "n_editions": sum(ids.values()), "n_entries": "",
                "surface_forms": role, "places": "", "basis": "linked_to_several",
                "candidates": " ‖ ".join(cand_label(d, core) for d in
                                         sorted(ids, key=lambda d: -ids[d])),
                "flags": "", "examples": "", "decision": "", "target_db_id": "",
                "note": "", "cluster_key": key})

    # ...and rows the editions link to under DIFFERENT spellings that share a
    # rare name token ("ווילנער פֿאַרלאָג פֿון ב. קלעצקין" 952 vs
    # "קלעצקין-פֿאַרלאָג" 1302): the cluster key cannot see these
    seen = {frozenset(ids) for ids in linked.values() if len(ids) > 1}
    df = collections.Counter()
    toks = {}
    for d, r in core.items():
        if r["deprecated"] in ("true", "1") or r["out_of_project"]:
            continue
        toks[d] = {t for f in [r["name"], r["name_yiddish"]] + r["name_variants"].split("|")
                   for k in norm2(f) for t in k.split()
                   if len(t) >= 4 and t not in GENERIC_TOK}
        df.update(toks[d])
    used = collections.Counter()
    for ids in linked.values():
        used.update(ids)
    kind = lambda d: ("pub" if core[d]["org_type"] in PUBLISHER_TYPES else
                      "press" if core[d]["org_type"] in PRESS_TYPES else "")
    by_tok = collections.defaultdict(set)
    for d in used:
        # publishers only: press titles share words ("Kinder-land" / "Dos
        # naye land") without being one paper
        if d in toks and kind(d) == "pub":
            for t in toks[d]:
                if df[t] <= 3:
                    by_tok[(kind(d), t)].add(d)
    for (k, t), ids in sorted(by_tok.items()):
        if len(ids) > 1 and frozenset(ids) not in seen:
            seen.add(frozenset(ids))
            dups.append({
                "role": "core_dup", "n_editions": sum(used[d] for d in ids),
                "n_entries": "", "surface_forms": "publisher" if k == "pub" else "periodical",
                "places": "", "basis": f"shared_token:{t}",
                "candidates": " ‖ ".join(cand_label(d, core) for d in sorted(ids, key=int)),
                "flags": "", "examples": "", "decision": "", "target_db_id": "",
                "note": "", "cluster_key": t})

    out.sort(key=lambda r: (r["role"], -r["n_editions"]))
    dups.sort(key=lambda r: -r["n_editions"])
    pc.write_tsv(HERE / f"org_review_{a.tag}.tsv", out + dups, FIELDS)
    for role in ("publisher", "periodical"):
        rs = [r for r in out if r["role"] == role]
        print(f"{role}: {len(rs)} clusters / {sum(r['n_editions'] for r in rs)} editions;"
              f" with candidates {sum(bool(r['candidates']) for r in rs)};"
              f" singletons {sum(r['n_editions'] == 1 for r in rs)}")
    print(f"core_db duplicate suspects: {len(dups)}  -> org_review_{a.tag}.tsv")


if __name__ == "__main__":
    main()
