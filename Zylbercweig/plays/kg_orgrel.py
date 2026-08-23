"""KG org-relations layer — lexicon subject <-> organization affiliations.

Step 2 of the Colab-extraction -> KG integration.  Called from build_kg.py
after the bio layer (kg_bio.py) so all layers share one Graph.

Sources
  Zylbercweig_extraction/*IIIorg.json     per entry: organizations[] each with
                                          relations[]{category, specific_relation,
                                          role_title, date_start, date_end,
                                          original_sentence}  (23,788 relations)
  organizations/organizations_clustered.tsv
                                          the SAME org mentions, one row per org
                                          (first relation only), with cluster_id.
                                          Mentions with no proper name
                                          ("amateurs", "other troupes") were never
                                          clustered and are dropped here too.
  organizations/mention_removals.tsv      reviewer REMOVE overlay (zalmen/mention_removals.py)
  organizations/core_db.tsv               cluster_id -> db_id via linked_cluster_ids
                                          (already embodies REMOVE decisions and
                                          merge chains — the read-side authority;
                                          merged_into is still followed defensively)
  zibn-shtern/.../toponyms_attestations.csv
                                          org-corpus settlement attestations
                                          (cluster_id -> QID) for located_in

Edges (source_layer=orgrel), person -> org, typed from specific_relation:
  performer -> performed_with        artistic_staff -> staff_of
  employee  -> employed_by           executive      -> executive_of
  delegate_official -> delegate_of   owner_manager  -> managed
  founder   -> founded               member         -> member_of
  student   -> studied_at            graduate       -> graduated_from
  produced_by  -> work_produced_by   published_by   -> work_published_by
  (blank)   -> affiliated_with
role_detail = role_title; evidence_sentence = original_sentence.
Plus org -> place located_in edges from the attestation spine.

Org node-id policy: org:<db_id> when the cluster is linked in core_db, else
org_cluster:<cluster_id> (ext_ref_type=org_cluster, match_status=unmatched)
so the review loop can align it later.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from glob import glob

import plays_common as pc
from kg_bio import norm_date, EXTRACTION_MODEL as _BIO_MODEL

EXTRACTION_MODEL = "colab_extraction_IIIorg"
EXTRACTION_DIR = pc.HERE.parent / "Zylbercweig_extraction"
CLUSTERED_TSV = pc.ORGS_DIR / "organizations_clustered.tsv"
CORE_DB_TSV = pc.ORGS_DIR / "core_db.tsv"
ATTESTATIONS_CSV = pc.ZIBN_WORKING / "toponyms_attestations.csv"

REL_EDGE = {
    "performer": "performed_with", "artistic_staff": "staff_of",
    "employee": "employed_by", "executive": "executive_of",
    "delegate_official": "delegate_of", "owner_manager": "managed",
    "founder": "founded", "member": "member_of",
    "student": "studied_at", "graduate": "graduated_from",
    "produced_by": "work_produced_by", "published_by": "work_published_by",
    "": "affiliated_with",
}

# clustered-TSV column names (the flatten's "_ - a - _ - b" paths)
C_FILE = "File"
C_XML = "_ - xml:id"
C_TITLE = "_ - organizations - _ - title"
C_DESC = "_ - organizations - _ - descriptive_name"
C_SENT = "_ - organizations - _ - relations - _ - original_sentence"
C_CID = "cluster_id"
C_CANON = "canonical_yiddish"


def _n(s) -> str:
    return (s or "").strip() if isinstance(s, str) else ""


def _volume_of(filename: str) -> str:
    m = re.search(r"(\d+)", filename)
    return m.group(1) if m else ""


# ---------------------------------------------------------------- loaders
def _load_clustered() -> dict[tuple, list[dict]]:
    """(File, xml:id, title, descriptive_name) -> clustered rows, with the
    reviewer REMOVE overlay applied (removed rows lose their cluster_id)."""
    csv.field_size_limit(10 ** 9)
    with open(CLUSTERED_TSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    # overlay lives in the Zalmen package; optional so the KG builds without it
    sys.path.insert(0, str(pc.HERE.parent / "zalmen"))
    try:
        import mention_removals  # type: ignore
        n_removed = mention_removals.apply_to_rows(rows)
    except Exception as ex:  # noqa: BLE001
        n_removed = f"overlay unavailable ({ex.__class__.__name__})"
    print(f"orgrel: clustered rows {len(rows)}, removals applied: {n_removed}")
    byk: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if not _n(r.get(C_CID)):
            continue
        byk[(r[C_FILE], _n(r[C_XML]), _n(r[C_TITLE]), _n(r[C_DESC]))].append(r)
    return byk


def _load_core_db() -> tuple[dict[str, dict], dict[str, str]]:
    """db_id -> row ; cluster_id -> live db_id (merged_into followed)."""
    rows = {r["db_id"]: r for r in pc.read_tsv(CORE_DB_TSV) if r.get("db_id")}

    def live(db_id: str, hops: int = 0) -> str:
        r = rows.get(db_id)
        if not r or hops > 10:
            return db_id
        if r.get("merged_into") and r["merged_into"] in rows:
            return live(r["merged_into"], hops + 1)
        return db_id

    c2d: dict[str, str] = {}
    for db_id, r in rows.items():
        if r.get("deprecated"):
            tgt = r.get("merged_into")
            if not tgt or tgt not in rows:
                # deleted row, or a merge whose survivor no longer exists
                # (e.g. 716 -> 715): its clusters stay unlinked rather than
                # pointing at a corpse
                if tgt:
                    print(f"orgrel: db {db_id} merged_into missing row {tgt} — "
                          f"clusters left unlinked")
                continue
        for cid in (r.get("linked_cluster_ids") or "").split("|"):
            cid = cid.strip()
            if not cid:
                continue
            tgt = live(db_id)
            prev = c2d.get(cid)
            if prev and prev != tgt:
                # 5 clusters are linked from two live rows; keep the lower id,
                # note it on the edge via stats only
                tgt = min(prev, tgt, key=lambda x: int(x) if x.isdigit() else 10 ** 9)
            c2d[cid] = tgt
    return rows, c2d


def _load_cluster_decisions() -> dict[str, tuple[str, str]]:
    """cluster_id -> (decision, aligned_db_id) from Zalmen's org_alignment_review.
    Stamped on org_cluster nodes as metadata ONLY: an ALIGN there is not a
    link until it reaches core_db.linked_cluster_ids (REMOVE decisions and
    merges override it — several pending ALIGNs are visibly wrong)."""
    out = {}
    for r in pc.read_tsv(pc.ORGS_DIR / "org_alignment_review.tsv"):
        if r.get("cluster_id") and r.get("decision"):
            out[r["cluster_id"]] = (r["decision"], r.get("aligned_db_id") or "")
    return out


def _load_org_places() -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    with open(ATTESTATIONS_CSV, encoding="utf-8-sig") as f:
        for a in csv.DictReader(f):
            if a["source_corpus"] == "org" and a["link_status"] == "linked" and a["qid"]:
                out[a["source_record_id"]].append(a)
    return out


# ---------------------------------------------------------------- layer
def add_orgrel_layer(g, labels, entry_index: dict[str, dict]) -> dict:
    from build_kg import _place_label_yi, _place_label_en

    people, orgs_lbl, _clusters, places = labels
    byk = _load_clustered()
    core, c2d = _load_core_db()
    org_places = _load_org_places()
    decisions = _load_cluster_decisions()
    by_entry_key = {e["entry_key"]: e for e in entry_index.values()}
    stats: Counter = Counter()
    seen_edges: set[tuple] = set()
    org_nodes_touched: dict[str, str] = {}  # node_id -> cluster_id

    def org_node(cid: str, canon: str, org_type_hint: str) -> str:
        db_id = c2d.get(cid)
        if db_id:
            o = core.get(db_id, {})
            nid = f"org:{db_id}"
            attrs = {k: v for k, v in (("org_type", o.get("org_type", "")),
                                       ("address", o.get("address", ""))) if v}
            g.add_node(nid, node_type="org",
                       label_yiddish=o.get("name_yiddish") or canon,
                       label_english=o.get("name", ""),
                       ext_ref_type="org_core_db", ext_ref_id=db_id,
                       secondary_ids=f"cluster_id:{cid}",
                       match_status="matched", source_layer="orgrel",
                       attrs=json.dumps(attrs, ensure_ascii=False))
            stats["org_nodes_db"] += 1
        else:
            nid = f"org_cluster:{cid}"
            dec, adb = decisions.get(cid, ("", ""))
            # DESCRIPTIVE/GENERIC = reviewer says "a kind of thing, not an
            # entity" (חדר, גימנאַזיע): keep the edges, flag the node
            status = "not_entity" if dec in ("DESCRIPTIVE", "GENERIC") else "unmatched"
            notes = (f"review:{dec}" + (f" aligned_db_id:{adb}" if adb else "")) if dec else ""
            if dec:
                stats[f"cluster_review:{dec}"] += 1
            g.add_node(nid, node_type="org", label_yiddish=canon,
                       ext_ref_type="org_cluster", ext_ref_id=cid,
                       match_status=status, notes=notes, source_layer="orgrel",
                       attrs=json.dumps({"org_type": org_type_hint},
                                        ensure_ascii=False) if org_type_hint else "")
            stats["org_nodes_cluster_only"] += 1
        org_nodes_touched.setdefault(nid, cid)
        return nid

    for path in sorted(glob(str(EXTRACTION_DIR / "*IIIorg.json"))):
        fn = os.path.basename(path)
        vol = _volume_of(fn)
        with open(path, encoding="utf-8") as f:
            entries = json.load(f)
        for e in entries:
            xml_id = _n(e.get("xml:id"))
            host = by_entry_key.get(f"{vol}-{xml_id}")
            if not host:
                stats["rel_dropped_no_host_entry"] += 1
                continue
            for o in e.get("organizations", []) or []:
                rels = o.get("relations") or [{}]
                cands = byk.get((fn, xml_id, _n(o.get("title")), _n(o.get("descriptive_name"))), [])
                if not cands:
                    stats["rel_dropped_unclustered" if not _n(o.get("title"))
                          else "rel_dropped_titled_but_no_row"] += len(rels)
                    continue
                for rel in rels:
                    sent = _n(rel.get("original_sentence"))
                    row = next((c for c in cands if _n(c.get(C_SENT)) == sent), cands[0])
                    cid = row[C_CID]
                    onid = org_node(cid, _n(row.get(C_CANON)) or _n(o.get("title")),
                                    _n(o.get("org_type")))
                    spec = _n(rel.get("specific_relation"))
                    etype = REL_EDGE.get(spec, "affiliated_with")
                    d0, d1, prec = norm_date(_n((rel.get("date_start") or {}).get("date")
                                                if isinstance(rel.get("date_start"), dict)
                                                else rel.get("date_start")))
                    e1, _, _ = norm_date(_n((rel.get("date_end") or {}).get("date")
                                            if isinstance(rel.get("date_end"), dict)
                                            else rel.get("date_end")))
                    if e1:
                        d1 = e1
                    role = _n(rel.get("role_title"))
                    key = (host["node_id"], onid, etype, role, d0, d1, sent)
                    if key in seen_edges:
                        stats["rel_duplicate"] += 1
                        continue
                    seen_edges.add(key)
                    g.add_edge(source_id=host["node_id"], target_id=onid,
                               edge_type=etype, role_detail=role, character="",
                               date_start=d0, date_end=d1, date_precision=prec,
                               event_id="", production_key="",
                               provenance_person_id=host["person_id"],
                               provenance_fact_ids=f"{cid}|{_n(rel.get('category'))}/{spec}",
                               evidence_sentence=sent,
                               extraction_model=EXTRACTION_MODEL,
                               confidence="high" if c2d.get(cid) else "medium",
                               match_status="matched" if c2d.get(cid) else "unmatched",
                               review_status="auto", source_layer="orgrel")
                    stats[f"edge:{etype}"] += 1

    # org -> place (settlement attestations, linked only), one edge per pair
    for nid, cid in org_nodes_touched.items():
        done: set[str] = set()
        for a in org_places.get(cid, []):
            qid = a["qid"]
            if qid in done:
                continue
            done.add(qid)
            pl = places.get(qid, {})
            sec = json.dumps({k: pl.get(k, "") for k in ("kima_id", "lat", "lon")},
                             ensure_ascii=False) if pl else ""
            g.add_node(f"place:{qid}", node_type="place",
                       label_yiddish=_place_label_yi(pl, a["source_value"]),
                       label_english=_place_label_en(pl) or a.get("label_en", ""),
                       ext_ref_type="wikidata_qid", ext_ref_id=qid,
                       secondary_ids=sec, match_status="matched",
                       source_layer="orgrel")
            g.add_edge(source_id=nid, target_id=f"place:{qid}", edge_type="located_in",
                       role_detail=a.get("source_field", ""), character="",
                       date_start="", date_end="", date_precision="",
                       event_id="", production_key="", provenance_person_id="",
                       provenance_fact_ids=a["attestation_id"],
                       evidence_sentence=a["source_value"],
                       extraction_model=EXTRACTION_MODEL, confidence="high",
                       match_status="matched", review_status="auto",
                       source_layer="orgrel")
            stats["edge:located_in"] += 1
    return dict(sorted(stats.items()))
