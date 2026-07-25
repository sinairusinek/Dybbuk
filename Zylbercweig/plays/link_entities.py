"""B1 — Deterministic entity-linking cascade over extracted facts.

Reads kg_extraction_flagship.tsv + kg_extraction_drafts.tsv and resolves
every surface to the curated registries:

  people  -> people/people_db.tsv via derived_mention_alignments.tsv (exact
             surface), heading index, then fuzzy vs hebname/name_variants
  orgs    -> organizations/core_db.tsv (exact/normalized name), then cluster
             canonicals in org_alignment_review.tsv (ALIGN rows carry
             aligned_db_id; clusters without an entity link as cluster:<id>)
  venues  -> the org cascade (theatres live in core_db)
  places  -> zibn-shtern gazetteer (label_yi + variants -> QID), fuzzy fallback
  plays   -> plays_db.tsv (validate play_id_hint, else segment/fuzzy match)

Every resolution is stamped match_status in {matched, candidate, unmatched}
plus match_method. Nothing is dropped: unmatched surfaces stay on the row
and become unlinked nodes in build_kg.py.

Outputs (on --execute):
  kg_facts_linked.tsv  — extraction rows + link columns
  kg_link_review.tsv   — unique candidate/unmatched surfaces for adjudication

Usage:
    python3.11 link_entities.py            # dry-run stats
    python3.11 link_entities.py --execute
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from functools import lru_cache

from rapidfuzz import fuzz, process

import plays_common as pc

sys.path.insert(0, str(pc.PEOPLE_DIR))
import people_common  # noqa: E402

LINKED_TSV = pc.HERE / "kg_facts_linked.tsv"

LINK_COLS = [
    "play_link", "play_link_status", "play_link_method",
    "person_link", "person_link_status", "person_link_method",
    "org_link", "org_link_status", "org_link_method",
    "venue_link", "venue_link_status", "venue_link_method",
    "place_link", "place_link_status", "place_link_method",
]
LINKED_FIELDS = pc.EXTRACTION_FIELDS + LINK_COLS

PERSON_FUZZY = 90
ORG_FUZZY = 92
PLACE_FUZZY = 92
PLAY_FUZZY = 85


class Linkers:
    def __init__(self) -> None:
        # --- people ---
        self.surface_to_db: dict[str, tuple[str, str]] = {}
        for r in pc.read_tsv(pc.PEOPLE_DIR / "derived_mention_alignments.tsv"):
            s, db = (r.get("mention_surface") or "").strip(), (r.get("db_id") or "").strip()
            if s and db and r.get("db_status") == "ok":
                self.surface_to_db[s] = (db, "mention_surface")
        extracted = people_common.load_extracted()
        self.heading_idx = people_common.build_heading_index(extracted)
        self.person_db_map, _ = people_common.build_person_db_map(extracted)
        self.people_names: list[tuple[str, str]] = []  # (norm_name, db_id)
        for r in people_common.load_db_rows():
            db = (r.get("db_id") or "").strip()
            if not db:
                continue
            names = [r.get("hebname", "")] + (r.get("name_variants") or "").split(";")
            for n in names:
                nn = pc.norm_yiddish(n)
                if len(nn) >= 4:
                    self.people_names.append((nn, db))
        self.people_norms = [n for n, _ in self.people_names]

        # --- orgs ---
        self.org_by_norm: dict[str, str] = {}
        for r in pc.read_tsv(pc.ORGS_DIR / "core_db.tsv"):
            if (r.get("deprecated") or "").strip():
                continue
            db = (r.get("merged_into") or "").strip() or (r.get("db_id") or "").strip()
            if not db:
                continue
            for n in [r.get("name_yiddish", "")] + (r.get("name_variants") or "").split(";"):
                nn = pc.norm_yiddish(n)
                if len(nn) >= 4:
                    self.org_by_norm.setdefault(nn, db)
        self.cluster_by_norm: dict[str, tuple[str, str]] = {}  # norm -> (cluster_id, aligned_db)
        for r in pc.read_tsv(pc.ORGS_DIR / "org_alignment_review.tsv"):
            cid = (r.get("cluster_id") or "").strip()
            if not cid:
                continue
            aligned = ""
            if (r.get("decision") or "").strip() == "ALIGN":
                aligned = (r.get("aligned_db_id") or "").strip()
            for n in [r.get("canonical_yiddish", "")] + (r.get("name_variants") or "").split(";"):
                nn = pc.norm_yiddish(n)
                if len(nn) >= 4:
                    self.cluster_by_norm.setdefault(nn, (cid, aligned))
        self.org_norms = list(self.org_by_norm)
        self.cluster_norms = list(self.cluster_by_norm)

        # --- places ---
        self.place_by_norm: dict[str, str] = {}
        gaz = pc.ZIBN_WORKING / "toponyms_gazetteer.csv"
        with open(gaz, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                qid = (r.get("qid") or "").strip()
                if not qid:
                    continue
                for n in [r.get("label_yi", "")] + (r.get("variants") or "").split(";"):
                    nn = pc.norm_yiddish(n)
                    if len(nn) >= 3:
                        self.place_by_norm.setdefault(nn, qid)
        self.place_norms = list(self.place_by_norm)

        # --- plays ---
        self.plays = pc.load_plays_db()
        self.play_by_id = {p["play_id"]: p for p in self.plays}
        self.play_seg_index: dict[str, list[str]] = defaultdict(list)
        for p in self.plays:
            for seg in (p["title_segments_norm"] or "").split("|"):
                if seg:
                    self.play_seg_index[seg].append(p["play_id"])
        self.play_segs = list(self.play_seg_index)

    # ---------- cascades (each returns (link, status, method)) ----------

    @lru_cache(maxsize=None)
    def link_person(self, surface: str) -> tuple[str, str, str]:
        s = surface.strip()
        if not s:
            return "", "", ""
        hit = self.surface_to_db.get(s)
        if hit:
            return f"person:{hit[0]}", "matched", "mention_surface"
        pids = self.heading_idx.get(s, [])
        if len(pids) == 1:
            db = self.person_db_map.get(pids[0])
            if db:
                return f"person:{db}", "matched", "heading"
            return f"person_entry:{pids[0]}", "matched", "heading_no_db"
        sn = pc.norm_yiddish(s)
        if len(sn) >= 4:
            m = process.extractOne(sn, self.people_norms, scorer=fuzz.ratio,
                                   score_cutoff=PERSON_FUZZY)
            if m:
                db = self.people_names[m[2]][1]
                return f"person:{db}", "candidate", f"fuzzy_{m[1]:.0f}"
        return "", "unmatched", ""

    @lru_cache(maxsize=None)
    def link_org(self, surface: str) -> tuple[str, str, str]:
        sn = pc.norm_yiddish(surface)
        if len(sn) < 4:
            return ("", "", "") if not surface.strip() else ("", "unmatched", "too_short")
        db = self.org_by_norm.get(sn)
        if db:
            return f"org:{db}", "matched", "core_db_exact"
        cl = self.cluster_by_norm.get(sn)
        if cl:
            cid, aligned = cl
            if aligned:
                return f"org:{aligned}", "matched", "cluster_align"
            return f"org_cluster:{cid}", "candidate", "cluster_no_entity"
        m = process.extractOne(sn, self.org_norms, scorer=fuzz.ratio,
                               score_cutoff=ORG_FUZZY)
        if m:
            return f"org:{self.org_by_norm[m[0]]}", "candidate", f"fuzzy_{m[1]:.0f}"
        m = process.extractOne(sn, self.cluster_norms, scorer=fuzz.ratio,
                               score_cutoff=ORG_FUZZY)
        if m:
            cid, aligned = self.cluster_by_norm[m[0]]
            link = f"org:{aligned}" if aligned else f"org_cluster:{cid}"
            return link, "candidate", f"cluster_fuzzy_{m[1]:.0f}"
        return "", "unmatched", ""

    @lru_cache(maxsize=None)
    def link_place(self, surface: str) -> tuple[str, str, str]:
        sn = pc.norm_yiddish(surface)
        if not surface.strip():
            return "", "", ""
        if len(sn) < 3:
            return "", "unmatched", "too_short"
        qid = self.place_by_norm.get(sn)
        if qid:
            return f"place:{qid}", "matched", "gazetteer_exact"
        m = process.extractOne(sn, self.place_norms, scorer=fuzz.ratio,
                               score_cutoff=PLACE_FUZZY)
        if m:
            return f"place:{self.place_by_norm[m[0]]}", "candidate", f"fuzzy_{m[1]:.0f}"
        return "", "unmatched", ""

    @lru_cache(maxsize=None)
    def link_play(self, surface: str, hint: str) -> tuple[str, str, str]:
        if not surface.strip() and not hint:
            return "", "", ""
        sn = pc.norm_yiddish(surface)
        segs = set(pc.title_segments(surface)) | ({sn} if sn else set())
        if hint and hint in self.play_by_id:
            hint_segs = set((self.play_by_id[hint]["title_segments_norm"] or "").split("|"))
            if segs & hint_segs:
                return f"play:{hint}", "matched", "hint_exact"
            best = max((fuzz.ratio(sn, hs) for hs in hint_segs if hs), default=0)
            if best >= PLAY_FUZZY:
                return f"play:{hint}", "matched", f"hint_fuzzy_{best:.0f}"
            return f"play:{hint}", "candidate", "hint_unverified"
        ids = {pid for seg in segs for pid in self.play_seg_index.get(seg, [])}
        if len(ids) == 1:
            return f"play:{ids.pop()}", "matched", "title_exact"
        if len(ids) > 1:
            return "|".join(f"play:{i}" for i in sorted(ids)), "candidate", "title_multi"
        if sn:
            m = process.extractOne(sn, self.play_segs, scorer=fuzz.ratio,
                                   score_cutoff=PLAY_FUZZY)
            if m:
                ids = self.play_seg_index[m[0]]
                status = "candidate"
                return "|".join(f"play:{i}" for i in sorted(ids)), status, f"fuzzy_{m[1]:.0f}"
        return "", "unmatched", ""


def link_row(r: dict, lk: Linkers) -> dict:
    out = dict(r)
    if r.get("person_surface") == "[HOST]":
        db = lk.person_db_map.get(r.get("person_id", ""))
        if db:
            out.update(person_link=f"person:{db}", person_link_status="matched",
                       person_link_method="host")
        else:
            out.update(person_link=f"person_entry:{r.get('person_id', '')}",
                       person_link_status="matched", person_link_method="host_no_db")
    else:
        link, st, me = lk.link_person(r.get("person_surface", ""))
        out.update(person_link=link, person_link_status=st, person_link_method=me)
    for slot, fn, args in (
        ("org", lk.link_org, (r.get("org_surface", ""),)),
        ("venue", lk.link_org, (r.get("venue_surface", ""),)),
        ("place", lk.link_place, (r.get("settlement_surface", ""),)),
        ("play", lk.link_play, (r.get("play_title_surface", ""), r.get("play_id_hint", ""))),
    ):
        link, st, me = fn(*args)
        out[f"{slot}_link"], out[f"{slot}_link_status"], out[f"{slot}_link_method"] = link, st, me
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    rows = []
    for path in (pc.FLAGSHIP_TSV, pc.DRAFTS_TSV):
        got = pc.read_tsv(path)
        print(f"{path.name}: {len(got)} rows")
        rows.extend(r for r in got if r.get("fact_type") not in ("", "none"))
    if not rows:
        raise SystemExit("no extraction rows found — run A3/A4 first")

    lk = Linkers()
    print(f"linkers: {len(lk.surface_to_db)} people surfaces, "
          f"{len(lk.org_by_norm)} org names, {len(lk.cluster_by_norm)} cluster names, "
          f"{len(lk.place_by_norm)} place names, {len(lk.plays)} plays")

    linked = [link_row(r, lk) for r in rows]

    stats = {slot: Counter(r[f"{slot}_link_status"] for r in linked if r[f"{slot}_link_status"])
             for slot in ("play", "person", "org", "venue", "place")}
    for slot, c in stats.items():
        print(f"  {slot:7s}: {dict(c)}")

    # Review sheet: unique surfaces that are candidate/unmatched.
    review: dict[tuple[str, str], dict] = {}
    for r in linked:
        for slot, surf_col in (("person", "person_surface"), ("org", "org_surface"),
                               ("venue", "venue_surface"), ("place", "settlement_surface"),
                               ("play", "play_title_surface")):
            st = r[f"{slot}_link_status"]
            if st not in ("candidate", "unmatched"):
                continue
            surf = r.get(surf_col, "")
            if not surf.strip() or surf == "[HOST]":
                continue
            key = (slot, surf)
            rv = review.setdefault(key, {
                "slot": slot, "surface": surf,
                "auto_link": r[f"{slot}_link"], "auto_status": st,
                "auto_method": r[f"{slot}_link_method"],
                "n_facts": 0, "example_fact_id": r["fact_id"],
                "example_evidence": (r.get("evidence_quote") or "")[:200],
                "decision": "", "decided_link": "", "reviewer_notes": "",
            })
            rv["n_facts"] += 1
    review_rows = sorted(review.values(), key=lambda x: (x["slot"], -x["n_facts"]))
    print(f"review sheet: {len(review_rows)} unique surfaces "
          f"({sum(r['n_facts'] for r in review_rows)} fact-slots)")

    if not args.execute:
        print("\ndry-run — pass --execute to write kg_facts_linked.tsv / kg_link_review.tsv")
        return

    pc.write_tsv(LINKED_TSV, linked, LINKED_FIELDS)
    prior = {(r["slot"], r["surface"]): r for r in pc.read_tsv(pc.LINK_REVIEW_TSV)}
    for r in review_rows:
        old = prior.get((r["slot"], r["surface"]))
        if old:
            for col in ("decision", "decided_link", "reviewer_notes"):
                r[col] = old.get(col, "")
    pc.write_tsv(pc.LINK_REVIEW_TSV, review_rows, list(review_rows[0].keys()))
    print(f"wrote {LINKED_TSV} ({len(linked)}) and {pc.LINK_REVIEW_TSV} ({len(review_rows)})")


if __name__ == "__main__":
    main()
