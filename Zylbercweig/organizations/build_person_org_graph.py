#!/usr/bin/env python3.11
"""Materialize the person->org provenance layer and rank neighborhood-overlap merge candidates.

Layer 1 (edges): every org mention in organizations_clustered.tsv was extracted from a
specific host person entry (File + xml:id + heading). That provenance is a ready-made
person->org_cluster edge, with the LLM-extracted relation type/role/dates attached.

Layer 2 (candidates): two clusters mentioned by overlapping sets of host entries are
merge candidates ("shared neighborhood"). Overlap is IDF-weighted so hosts that mention
many orgs (and ubiquitous orgs) contribute less. Pairs already linked to the same
core_db entity are used as a sanity check, not reported; pairs whose entities are in
confirmed_distinct_pairs.tsv are flagged.

Raw co-mention overlap measures shared milieu (actors played at many theaters), not
identity — so two more outputs cross it with compatibility signals:

Outputs (in organizations/graph/):
  person_org_edges.tsv          host -> cluster edges with relation metadata
  org_overlap_candidates.tsv    all ranked cluster pairs not currently same-entity
  org_overlap_shortlist.tsv     subset with compatible names + settlements (merge leads)
  alignment_corroboration.tsv   pending queue clusters whose candidate db entities
                                share hosts with them (queue re-ranking evidence)

Read-only over inputs; safe to re-run.
"""
from __future__ import annotations

import csv
import math
import sys
import unicodedata
from collections import defaultdict
from itertools import combinations
from pathlib import Path

BASE = Path(__file__).resolve().parent
KG_DIR = BASE.parent / "plays" / "kg"
CLUSTERED = BASE / "organizations_clustered.tsv"
CORE_DB = BASE / "core_db.tsv"
REVIEW = BASE / "org_alignment_review.tsv"
DISTINCT = BASE / "confirmed_distinct_pairs.tsv"
PAIRS_REVIEW = BASE / "cluster_pairs_review.tsv"
OUTDIR = BASE / "graph"

MIN_SHARED_HOSTS = 2

# Generic tokens carry no identity signal when comparing org names.
GENERIC_TOKENS = {
    "טעאטער", "טרופע", "יידישער", "יידישע", "יידישן", "אידישער",
    "אידישע", "אידישן", "פאראיין", "פעראיין", "געזעלשאפט", "חברה",
    "קלוב", "אין", "פון", "פאר", "דער", "די", "דאס", "דעם", "און", "ביי",
    "נייעם", "גרויסן", "אלטן", "סטודיא", "סטודיע", "אנסאמבל",
}
FINALS = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ", "ײ": "יי", "װ": "וו"})
HEBREW_MARKS = {c: None for c in range(0x0591, 0x05C8) if chr(c) not in "אבגדהוזחטיכךלמםנןסעפףצץקרשת"}


def norm_tokens(name: str) -> set[str]:
    """Normalized, non-generic name tokens for compatibility checks."""
    # NFKD first: precomposed Yiddish letters (e.g. U+FB2E alef-patah) decompose
    # into base letter + combining mark, which the mark-strip can then remove.
    s = unicodedata.normalize("NFKD", name).translate(HEBREW_MARKS).translate(FINALS)
    for ch in "\"'׳״()[],.·-—/„“”":
        s = s.replace(ch, " ")
    return {t for t in s.split() if len(t) >= 3 and t not in GENERIC_TOKENS}


def names_compatible(a: str, b: str) -> bool:
    ta, tb = norm_tokens(a), norm_tokens(b)
    if not ta or not tb:
        return False
    return len(ta & tb) > 0


def settlements_compatible(a: str, b: str) -> bool:
    """True if settlement sets overlap or either side is unknown."""
    sa = {s.strip() for s in a.split("|") if s.strip()}
    sb = {s.strip() for s in b.split("|") if s.strip()}
    return not sa or not sb or bool(sa & sb)


def read_tsv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    csv.field_size_limit(sys.maxsize)
    OUTDIR.mkdir(exist_ok=True)

    # --- Layer 1: host -> cluster edges -------------------------------------
    # organizations_clustered.tsv columns are the verbose export names; address by index.
    with CLUSTERED.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    header, rows = rows[0], rows[1:]
    IDX = {
        "file": 2, "xmlid": 3, "heading": 4,
        "rel_category": 13, "rel_specific": 14, "role_title": 16,
        "date_start": 17, "date_end": 18,
        "settlement": 19, "cluster_id": 36, "canonical": 37,
    }
    assert header[IDX["cluster_id"]] == "cluster_id", header[IDX["cluster_id"]]
    assert "xml:id" in header[IDX["xmlid"]], header[IDX["xmlid"]]

    # edge key: (host_key, cluster_id) -> aggregated mention metadata
    edges: dict[tuple, dict] = {}
    host_label: dict[tuple, str] = {}
    cluster_name: dict[str, str] = {}
    for r in rows:
        if len(r) <= IDX["cluster_id"]:
            continue
        cid = r[IDX["cluster_id"]].strip()
        file_, xmlid = r[IDX["file"]].strip(), r[IDX["xmlid"]].strip()
        if not cid or not xmlid:
            continue
        host = (file_, xmlid)
        host_label.setdefault(host, r[IDX["heading"]].strip())
        if r[IDX["canonical"]].strip():
            cluster_name.setdefault(cid, r[IDX["canonical"]].strip())
        e = edges.setdefault((host, cid), {
            "n_mentions": 0, "rel_categories": set(), "roles": set(),
            "dates": set(), "settlements": set(),
        })
        e["n_mentions"] += 1
        for key, col in (("rel_categories", "rel_category"), ("roles", "role_title"),
                         ("settlements", "settlement")):
            v = r[IDX[col]].strip()
            if v:
                e[key].add(v)
        for col in ("date_start", "date_end"):
            v = r[IDX[col]].strip()
            if v:
                e["dates"].add(v)

    # --- cluster -> entity map from core_db ---------------------------------
    core = read_tsv(CORE_DB)
    merged_into = {r["db_id"]: r["merged_into"] for r in core if r.get("merged_into")}

    def resolve(db_id: str) -> str:
        seen = set()
        while db_id in merged_into and db_id not in seen:
            seen.add(db_id)
            db_id = merged_into[db_id]
        return db_id

    cluster_to_db: dict[str, str] = {}
    db_name: dict[str, str] = {}
    for r in core:
        db_id = resolve(r["db_id"])
        db_name.setdefault(r["db_id"], r.get("name_yiddish") or r.get("name") or "")
        for cid in (c.strip() for c in (r.get("linked_cluster_ids") or "").split("|")):
            if cid:
                cluster_to_db[cid] = db_id

    review = read_tsv(REVIEW)
    rev_by_cluster = {r["cluster_id"]: r for r in review}
    for r in review:
        if r.get("aligned_db_id") and r["cluster_id"] not in cluster_to_db:
            cluster_to_db[r["cluster_id"]] = resolve(r["aligned_db_id"])

    # --- typed person->org edges derived from the plays KG (pilot scope) ----
    # person -cast_in-> production_event -produced_by/staged_at-> org(_cluster)
    # These are kept out of the overlap computation (different host keyspace);
    # they enrich the edge file with performance-typed relations. The KG scale-up
    # to all playwrights will grow this layer with no change here.
    kg_edges: dict[tuple[str, str], dict] = {}
    kg_label: dict[str, str] = {}
    kg_edges_file = KG_DIR / "edges.tsv"
    if kg_edges_file.exists():
        for n in read_tsv(KG_DIR / "nodes.tsv"):
            kg_label[n["node_id"]] = n.get("label_yiddish") or n.get("label_english") or ""
        kg_rows = read_tsv(kg_edges_file)
        event_orgs: dict[str, set[str]] = defaultdict(set)
        for r in kg_rows:
            if r["edge_type"] in ("produced_by", "staged_at") and \
                    r["target_id"].split(":")[0] in ("org", "org_cluster"):
                event_orgs[r["source_id"]].add(r["target_id"])
        for r in kg_rows:
            if r["edge_type"] != "cast_in":
                continue
            person = r["source_id"]
            for org_node in event_orgs.get(r["target_id"], ()):
                e = kg_edges.setdefault((person, org_node), {
                    "n_mentions": 0, "rel_categories": {"KG_Performance"},
                    "roles": set(), "dates": set(), "settlements": set(),
                })
                e["n_mentions"] += 1
                if r.get("role_detail"):
                    e["roles"].add(r["role_detail"])
                if r.get("date_start"):
                    e["dates"].add(r["date_start"])

    # --- write edges --------------------------------------------------------
    edge_path = OUTDIR / "person_org_edges.tsv"
    with edge_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["edge_source", "host_file", "host_xmlid", "host_heading",
                    "cluster_id", "canonical_yiddish", "db_id", "n_mentions",
                    "rel_categories", "roles", "dates", "settlements"])
        for (host, cid), e in sorted(edges.items()):
            w.writerow(["mention", host[0], host[1], host_label[host], cid,
                        cluster_name.get(cid, ""), cluster_to_db.get(cid, ""),
                        e["n_mentions"],
                        " | ".join(sorted(e["rel_categories"])),
                        " | ".join(sorted(e["roles"])),
                        " | ".join(sorted(e["dates"])),
                        " | ".join(sorted(e["settlements"]))])
        for (person, org_node), e in sorted(kg_edges.items()):
            kind, _, ident = org_node.partition(":")
            cid = ident if kind == "org_cluster" else ""
            db_id = ident if kind == "org" else (
                cluster_to_db.get(cid) or cluster_to_db.get(cid.split("_Q")[0], ""))
            w.writerow(["plays_kg", "plays_kg", person, kg_label.get(person, ""),
                        cid, kg_label.get(org_node, ""), db_id,
                        e["n_mentions"],
                        " | ".join(sorted(e["rel_categories"])),
                        " | ".join(sorted(e["roles"])),
                        " | ".join(sorted(e["dates"])),
                        " | ".join(sorted(e["settlements"]))])

    # --- Layer 2: overlap candidates ----------------------------------------
    host_clusters: dict[tuple, set[str]] = defaultdict(set)
    cluster_hosts: dict[str, set[tuple]] = defaultdict(set)
    for (host, cid) in edges:
        host_clusters[host].add(cid)
        cluster_hosts[cid].add(host)

    def host_weight(h: tuple) -> float:
        return 1.0 / math.log2(1 + len(host_clusters[h]))

    pair_shared: dict[tuple[str, str], set] = defaultdict(set)
    for host, cids in host_clusters.items():
        if len(cids) < 2 or len(cids) > 60:  # a host mentioning 60+ orgs is pure noise
            continue
        for a, b in combinations(sorted(cids), 2):
            pair_shared[(a, b)].add(host)

    distinct_db = set()
    for r in read_tsv(DISTINCT):
        distinct_db.add(frozenset((resolve(r["db_a"]), resolve(r["db_b"]))))

    pair_decision = {}
    for r in read_tsv(PAIRS_REVIEW):
        key = tuple(sorted((r["cluster_id_i"], r["cluster_id_j"])))
        if r.get("decision"):
            pair_decision[key] = r["decision"]

    n_same_entity = 0
    candidates = []
    for (a, b), shared in pair_shared.items():
        if len(shared) < MIN_SHARED_HOSTS:
            continue
        db_a, db_b = cluster_to_db.get(a, ""), cluster_to_db.get(b, "")
        if db_a and db_a == db_b:
            n_same_entity += 1  # sanity-check bucket: signal agrees with known merges
            continue
        score = sum(host_weight(h) for h in shared)
        union = len(cluster_hosts[a] | cluster_hosts[b])
        rev_a, rev_b = rev_by_cluster.get(a, {}), rev_by_cluster.get(b, {})
        flag = ""
        if db_a and db_b and frozenset((db_a, db_b)) in distinct_db:
            flag = "PI_CONFIRMED_DISTINCT"
        candidates.append({
            "cluster_a": a, "cluster_b": b,
            "name_a": cluster_name.get(a, ""), "name_b": cluster_name.get(b, ""),
            "type_a": rev_a.get("org_type", ""), "type_b": rev_b.get("org_type", ""),
            "settlements_a": rev_a.get("extracted_settlements", ""),
            "settlements_b": rev_b.get("extracted_settlements", ""),
            "db_a": db_a, "db_b": db_b,
            "db_name_a": db_name.get(db_a, ""), "db_name_b": db_name.get(db_b, ""),
            "decision_a": rev_a.get("decision", ""), "decision_b": rev_b.get("decision", ""),
            "shared_hosts": len(shared), "jaccard": round(len(shared) / union, 3),
            "idf_score": round(score, 3),
            "shared_host_headings": " | ".join(sorted(host_label[h] for h in shared)[:8]),
            "pair_review_decision": pair_decision.get(tuple(sorted((a, b))), ""),
            "flag": flag,
        })
    candidates.sort(key=lambda c: -c["idf_score"])

    cand_path = OUTDIR / "org_overlap_candidates.tsv"
    with cand_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(candidates[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(candidates)

    # --- shortlist: overlap + compatible names + compatible settlements -----
    shortlist = [
        c for c in candidates
        if names_compatible(c["name_a"], c["name_b"])
        and settlements_compatible(c["settlements_a"], c["settlements_b"])
    ]
    short_path = OUTDIR / "org_overlap_shortlist.tsv"
    with short_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(candidates[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(shortlist)

    # --- corroboration for the pending alignment queue ----------------------
    # For an undecided cluster with candidate db entities, shared hosts between the
    # cluster and the entity's already-linked clusters are independent evidence.
    db_clusters: dict[str, set[str]] = defaultdict(set)
    for cid, db in cluster_to_db.items():
        db_clusters[db].add(cid)

    corro = []
    for r in review:
        if r.get("decision"):
            continue
        cid = r["cluster_id"]
        hosts = cluster_hosts.get(cid, set())
        if not hosts:
            continue
        for raw_db in (d.strip() for d in (r.get("candidate_db_ids") or "").split("|")):
            if not raw_db:
                continue
            db = resolve(raw_db)
            entity_hosts = set()
            for c in db_clusters.get(db, ()):
                if c != cid:
                    entity_hosts |= cluster_hosts.get(c, set())
            shared = hosts & entity_hosts
            if not shared:
                continue
            corro.append({
                "cluster_id": cid,
                "canonical_yiddish": r.get("canonical_yiddish", ""),
                "org_type": r.get("org_type", ""),
                "extracted_settlements": r.get("extracted_settlements", ""),
                "candidate_db_id": raw_db,
                "resolved_db_id": db,
                "candidate_db_name": db_name.get(db, ""),
                "shared_hosts": len(shared),
                "idf_score": round(sum(host_weight(h) for h in shared), 3),
                "shared_host_headings": " | ".join(sorted(host_label[h] for h in shared)[:8]),
            })
    corro.sort(key=lambda c: -c["idf_score"])
    corro_path = OUTDIR / "alignment_corroboration.tsv"
    with corro_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(corro[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(corro)

    print(f"hosts: {len(host_clusters)}  clusters: {len(cluster_hosts)}  "
          f"edges: {len(edges)}  kg-typed edges: {len(kg_edges)}")
    print(f"cluster->entity mapped: {len(cluster_to_db)}")
    print(f"pairs sharing >= {MIN_SHARED_HOSTS} hosts: "
          f"{n_same_entity} already same entity (sanity bucket); "
          f"{len(candidates)} candidates; {len(shortlist)} on shortlist")
    print(f"pending-queue corroborations: {len(corro)} "
          f"({len({c['cluster_id'] for c in corro})} distinct clusters)")
    print(f"wrote {edge_path.name}, {cand_path.name}, {short_path.name}, "
          f"{corro_path.name} in graph/")


if __name__ == "__main__":
    main()
