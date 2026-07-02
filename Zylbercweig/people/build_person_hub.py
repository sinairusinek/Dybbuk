"""Build the person hub — the Phase C entity model (C1b).

A hub = one real-world person, linking:
  - subject entries across volumes (person_id, from people_extracted.tsv)
  - external DB rows (db_id, from people_db.tsv)
  - validated mention surface forms (from derived_mention_alignments.tsv)

Union-find over CONFIRMED evidence only:
  ra_align      review rows with db_id (volume-aware xml_id resolution;
                alignment_disagreements.tsv xml_ids excluded pending PI)
  ra_dup        Duplication Check same-person groups (shared db_id)
  human_dedup   person_dedup_decisions.tsv decision=same (Zalmen B1)
  human_align   people_alignment_decisions.tsv ALIGN/MERGE (Zalmen B2 —
                written by the draft-review view; consumed when present)

Phase B drafts are NOT hub evidence — they surface as `pending_drafts`
counts on the hub row until confirmed in B2.

Outputs:
  person_hub.tsv           one row per hub that has ≥1 subject entry
  person_hub_members.tsv   long format: hub_id × member (entry/db/surface)
  person_hub_conflicts.tsv hubs with >1 db_id + skipped/ambiguous joins

Run: python3.11 Zylbercweig/people/build_person_hub.py
"""
from __future__ import annotations

import csv
import pathlib
import sys
from collections import defaultdict
from itertools import combinations

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from people_common import (  # noqa: E402
    XmlResolver,
    build_person_db_map,
    load_db_rows,
    load_extracted,
    non_person_person_ids,
    read_tsv,
    write_tsv,
)

REVIEW_TSV = HERE / "people_alignment_review.tsv"
DEDUP_DECISIONS_TSV = HERE / "person_dedup_decisions.tsv"
ALIGN_DECISIONS_TSV = HERE / "people_alignment_decisions.tsv"
DERIVED_TSV = HERE / "derived_mention_alignments.tsv"
DRAFTS_TSV = HERE / "people_alignment_drafts.tsv"

HUB_TSV = HERE / "person_hub.tsv"
MEMBERS_TSV = HERE / "person_hub_members.tsv"
CONFLICTS_TSV = HERE / "person_hub_conflicts.tsv"


class UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # deterministic: smaller string wins as root
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra


def e_node(person_id: str) -> str:
    return f"E:{person_id}"


def d_node(db_id: str) -> str:
    return f"D:{db_id}"


def load_gold_dup_pairs(resolver: XmlResolver) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Duplication Check same-person groups → entry-entry person_id pairs."""
    by_db: dict[str, list[str]] = defaultdict(list)
    skipped: list[tuple[str, str]] = []
    for r in read_tsv(REVIEW_TSV):
        if r.get("source_sheet") != "Duplication Check":
            continue
        if not (r.get("same_person") or "").lower().startswith("same"):
            continue
        xml, db = (r.get("xml_id") or "").strip(), (r.get("db_id") or "").strip()
        if not (xml and db):
            continue
        pid, status = resolver.resolve(xml, r.get("volume", ""))
        if status != "ok":
            skipped.append((xml, f"gold_dup_{status}"))
            continue
        by_db[db].append(pid)
    pairs = []
    for pids in by_db.values():
        for a, b in combinations(sorted(set(pids)), 2):
            pairs.append((a, b))
    return pairs, skipped


def main() -> None:
    extracted = load_extracted()
    db_rows = load_db_rows()
    resolver = XmlResolver(extracted)
    non_person = non_person_person_ids()

    uf = UnionFind()
    evidence: dict[str, set[str]] = defaultdict(set)  # node → evidence tags
    skips: list[dict] = []

    # seed every entry + db row as a singleton
    for r in extracted:
        uf.find(e_node(r["person_id"]))
    for d in db_rows:
        if d.get("db_id"):
            uf.find(d_node(d["db_id"]))

    # 1. ra_align: entry ↔ db
    person_db, db_report = build_person_db_map(extracted)
    for pid, db in person_db.items():
        uf.union(e_node(pid), d_node(db))
        evidence[e_node(pid)].add("ra_align")
    for xml, reason in db_report:
        skips.append({"kind": "join_skip", "id": xml, "detail": reason})

    # 2. ra_dup: entry ↔ entry (Duplication Check groups)
    gold_pairs, gold_skips = load_gold_dup_pairs(resolver)
    for a, b in gold_pairs:
        uf.union(e_node(a), e_node(b))
        evidence[e_node(a)].add("ra_dup")
        evidence[e_node(b)].add("ra_dup")
    for xml, reason in gold_skips:
        skips.append({"kind": "join_skip", "id": xml, "detail": reason})

    # 3. human_dedup: Zalmen B1 "same" decisions
    n_human_dedup = 0
    for r in read_tsv(DEDUP_DECISIONS_TSV):
        if (r.get("decision") or "").strip() != "same":
            continue
        pa, sa = resolver.resolve((r.get("a_xml_id") or "").strip())
        pb, sb = resolver.resolve((r.get("b_xml_id") or "").strip())
        if sa == "ok" and sb == "ok":
            uf.union(e_node(pa), e_node(pb))
            evidence[e_node(pa)].add("human_dedup")
            evidence[e_node(pb)].add("human_dedup")
            n_human_dedup += 1
        else:
            skips.append({"kind": "join_skip",
                          "id": f"{r.get('a_xml_id')}↔{r.get('b_xml_id')}",
                          "detail": f"human_dedup_{sa}_{sb}"})

    # 4. human_align: Zalmen B2 decisions (file appears once B2 is in use)
    n_human_align = 0
    for r in read_tsv(ALIGN_DECISIONS_TSV):
        pid = (r.get("person_id") or "").strip()
        dec = (r.get("decision") or "").strip().upper()
        if not pid or pid not in resolver.by_person_id:
            continue
        if dec == "ALIGN" and (r.get("aligned_db_id") or "").strip():
            uf.union(e_node(pid), d_node(r["aligned_db_id"].strip()))
            evidence[e_node(pid)].add("human_align")
            n_human_align += 1
        elif dec == "MERGE" and (r.get("merge_person_id") or "").strip():
            other = r["merge_person_id"].strip()
            if other in resolver.by_person_id:
                uf.union(e_node(pid), e_node(other))
                evidence[e_node(pid)].add("human_align")
                n_human_align += 1

    # group members by root
    members_by_root: dict[str, list[str]] = defaultdict(list)
    for node in list(uf.parent):
        members_by_root[uf.find(node)].append(node)

    # deterministic hub ids: prefer smallest numeric db_id, else smallest person_id
    def hub_id_for(members: list[str]) -> str:
        dbs = sorted((m[2:] for m in members if m.startswith("D:")),
                     key=lambda x: (len(x), x))
        if dbs:
            return f"HUB-D{dbs[0]}"
        return f"HUB-{sorted(m[2:] for m in members)[0]}"

    # attach validated mention surfaces (unambiguous at hub level)
    surfaces_by_hubroot: dict[str, list[dict]] = defaultdict(list)
    n_surface_ambiguous = 0
    for r in read_tsv(DERIVED_TSV):
        pids = [p for p in (r.get("matched_person_ids") or "").split("|") if p]
        if not pids:
            continue
        roots = {uf.find(e_node(p)) for p in pids}
        if len(roots) > 1:
            n_surface_ambiguous += 1
            skips.append({"kind": "surface_ambiguous",
                          "id": r["mention_surface"],
                          "detail": f"heading '{r['as_heading']}' spans {len(roots)} hubs"})
            continue
        surfaces_by_hubroot[next(iter(roots))].append(r)

    # pending drafts per hub (Phase B, unconfirmed)
    decided_pids = {(r.get("person_id") or "").strip() for r in read_tsv(ALIGN_DECISIONS_TSV)}
    drafts_by_root: dict[str, int] = defaultdict(int)
    for r in read_tsv(DRAFTS_TSV):
        pid = (r.get("person_id") or "").strip()
        if pid and pid in resolver.by_person_id and pid not in decided_pids:
            drafts_by_root[uf.find(e_node(pid))] += 1

    entry_by_pid = resolver.by_person_id
    db_by_id = {d["db_id"]: d for d in db_rows if d.get("db_id")}

    hub_rows: list[dict] = []
    member_rows: list[dict] = []
    conflict_rows: list[dict] = list(skips)

    for root, members in members_by_root.items():
        entry_pids = sorted(m[2:] for m in members if m.startswith("E:"))
        db_ids = sorted((m[2:] for m in members if m.startswith("D:")),
                        key=lambda x: (len(x), x))
        if not entry_pids and not db_ids:
            continue
        hub = hub_id_for(members)
        surfaces = sorted(surfaces_by_hubroot.get(root, []),
                          key=lambda r: -float(r.get("occurrences") or 0))

        entries = [entry_by_pid[p] for p in entry_pids if p in entry_by_pid]
        canonical = entries[0]["heading"] if entries else (
            db_by_id.get(db_ids[0], {}).get("hebname")
            or db_by_id.get(db_ids[0], {}).get("english", "") if db_ids else "")
        volumes = sorted({e["volume"] for e in entries})
        birth = next((e["birth_date"] for e in entries if e.get("birth_date")), "")
        death = next((e["death_date"] for e in entries if e.get("death_date")), "")
        ev = sorted({t for p in entry_pids for t in evidence.get(e_node(p), set())})

        if len(db_ids) > 1:
            conflict_rows.append({"kind": "multi_db_hub", "id": hub,
                                  "detail": f"{len(db_ids)} db_ids: {','.join(db_ids)}"})

        # hubs with no entries are unreferenced DB rows — skip the hub table,
        # they'd triple its size with rows nothing links to.
        if not entry_pids:
            continue

        hub_rows.append({
            "hub_id": hub,
            "canonical_heading": canonical,
            "n_entries": len(entry_pids),
            "n_db_rows": len(db_ids),
            "n_surfaces": len(surfaces),
            "entry_person_ids": "|".join(entry_pids),
            "db_ids": "|".join(db_ids),
            "volumes": "|".join(volumes),
            "birth_date": birth,
            "death_date": death,
            "evidence": "|".join(ev),
            "pending_drafts": drafts_by_root.get(root, 0),
            "multi_db": 1 if len(db_ids) > 1 else 0,
            "non_person": 1 if all(p in non_person for p in entry_pids) else 0,
            "top_surfaces": "|".join(s["mention_surface"] for s in surfaces[:5]),
        })
        for p in entry_pids:
            e = entry_by_pid.get(p, {})
            member_rows.append({"hub_id": hub, "member_kind": "entry", "member_id": p,
                                "label": e.get("heading", ""),
                                "detail": f"vol {e.get('volume', '')}",
                                "evidence": "|".join(sorted(evidence.get(e_node(p), set()))) or "singleton"})
        for db in db_ids:
            d = db_by_id.get(db, {})
            member_rows.append({"hub_id": hub, "member_kind": "db", "member_id": db,
                                "label": d.get("hebname") or d.get("english", ""),
                                "detail": d.get("english", ""),
                                "evidence": ""})
        for s in surfaces:
            member_rows.append({"hub_id": hub, "member_kind": "surface",
                                "member_id": s["mention_surface"],
                                "label": s["mention_surface"],
                                "detail": f"{s['source_sheet']} ×{s['occurrences']}",
                                "evidence": "validated_surface"})

    hub_rows.sort(key=lambda r: r["hub_id"])
    member_rows.sort(key=lambda r: (r["hub_id"], r["member_kind"], r["member_id"]))
    conflict_rows.sort(key=lambda r: (r["kind"], str(r["id"])))

    write_tsv(HUB_TSV, hub_rows, [
        "hub_id", "canonical_heading", "n_entries", "n_db_rows", "n_surfaces",
        "entry_person_ids", "db_ids", "volumes", "birth_date", "death_date",
        "evidence", "pending_drafts", "multi_db", "non_person", "top_surfaces"])
    write_tsv(MEMBERS_TSV, member_rows,
              ["hub_id", "member_kind", "member_id", "label", "detail", "evidence"])
    write_tsv(CONFLICTS_TSV, conflict_rows, ["kind", "id", "detail"])

    n_multi_entry = sum(1 for h in hub_rows if h["n_entries"] > 1)
    n_with_db = sum(1 for h in hub_rows if h["n_db_rows"] > 0)
    n_multi_db = sum(1 for h in hub_rows if h["multi_db"])
    print(f"hubs (≥1 entry): {len(hub_rows)}")
    print(f"  multi-entry (cross-volume same person): {n_multi_entry}")
    print(f"  with DB alignment: {n_with_db}   multi-db (flagged): {n_multi_db}")
    print(f"  with validated surfaces: {sum(1 for h in hub_rows if h['n_surfaces'])}")
    print(f"  pending drafts attached: {sum(h['pending_drafts'] for h in hub_rows)}")
    print(f"edges: ra_align={len(person_db)} ra_dup={len(gold_pairs)} "
          f"human_dedup={n_human_dedup} human_align={n_human_align}")
    print(f"surface skipped as cross-hub ambiguous: {n_surface_ambiguous}")
    print(f"conflicts file: {len(conflict_rows)} rows → {CONFLICTS_TSV.name}")


if __name__ == "__main__":
    main()
