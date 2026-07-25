"""C2 — Evaluate the extracted KG against the held-out catalogue.

No-recall principle: the catalogue was built from EXTERNAL sources, so
absence in the lexicon-derived KG is uninformative. Buckets:

  CORROBORATED   both sides assert the same value (confidence signal)
  CONTRADICTED   both assert, values differ -> eval_findings.tsv for
                 adjudication (extraction error OR genuine lexicon-vs-
                 catalogue divergence — the latter is a research finding)
  LEXICON_ONLY   KG-only facts (the KG's added value; informational)
  REFERENCE_ONLY reference-only rows (expected; informational)

Aspects compared: play attribution (registry vs works catalogue), premiere
year, venue-per-production, person roles. Alignment bridges: Yiddish title
segments (registry <-> works.title_yi), then works.title_en <-> production
play_key (romanized, token-based).

Outputs (on --execute): eval/eval_findings.tsv, eval/eval_report.md.

Usage:
    python3.11 evaluate_kg.py [--execute]
"""
from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict

from rapidfuzz import fuzz

import plays_common as pc

NODES_TSV = pc.KG_DIR / "nodes.tsv"
EDGES_TSV = pc.KG_DIR / "edges.tsv"
EVENTS_TSV = pc.KG_DIR / "events.tsv"
FINDINGS_TSV = pc.EVAL_DIR / "eval_findings.tsv"
REPORT_MD = pc.EVAL_DIR / "eval_report.md"

FINDING_FIELDS = ["finding_id", "bucket", "aspect", "kg_ref", "reference_ref",
                  "play", "kg_value", "ref_value", "evidence", "adjudication",
                  "classification", "notes"]


def rom_key(s: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", (s or "").lower()).split())


def align_plays(plays: list[dict], works: list[dict]):
    """registry play_id -> (works row, method) via Yiddish segments then fuzzy."""
    seg_to_work: dict[str, list[dict]] = defaultdict(list)
    for w in works:
        for seg in (w["title_segments_norm"] or "").split("|"):
            if seg:
                seg_to_work[seg].append(w)
    out: dict[str, tuple[dict, str]] = {}
    for p in plays:
        segs = [s for s in (p["title_segments_norm"] or "").split("|") if s]
        hits = {id(w): w for s in segs for w in seg_to_work.get(s, [])}
        if len(hits) >= 1:
            # prefer same-author works row
            rows = sorted(hits.values(),
                          key=lambda w: 0 if _author_match(p, w) else 1)
            out[p["play_id"]] = (rows[0], "segment_exact")
            continue
        best, best_r = None, 0.0
        for w in works:
            for ws in (w["title_segments_norm"] or "").split("|"):
                for s in segs:
                    r = fuzz.ratio(s, ws)
                    if r >= 88 and r > best_r:
                        best, best_r = w, r
        if best:
            out[p["play_id"]] = (best, f"segment_fuzzy_{best_r:.0f}")
    return out


def _author_match(play: dict, work: dict) -> bool:
    # catalogue sheets record the author either by name or by people_db id
    a = (work.get("author") or "").lower().split(".")[0].strip()
    if play["author_db_id"] == "683":
        return a == "683" or "lateiner" in a
    if play["author_db_id"] == "684":
        return a == "684" or "hurwitz" in a or "horowitz" in a
    return False


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    plays = pc.load_plays_db()
    nodes = pc.read_tsv(NODES_TSV)
    edges = pc.read_tsv(EDGES_TSV)
    events = pc.read_tsv(EVENTS_TSV)
    works = pc.read_tsv(pc.EVAL_DIR / "eval_reference_works.tsv")
    ref_prods = pc.read_tsv(pc.EVAL_DIR / "eval_reference_productions.tsv")
    ref_roles = pc.read_tsv(pc.EVAL_DIR / "eval_reference_roles.tsv")
    node_label = {n["node_id"]: n.get("label_yiddish") or n.get("label_english", "")
                  for n in nodes}

    findings: list[dict] = []

    def add(bucket, aspect, kg_ref, ref_ref, play, kg_value, ref_value,
            evidence="", notes=""):
        findings.append({
            "bucket": bucket, "aspect": aspect, "kg_ref": kg_ref,
            "reference_ref": ref_ref, "play": play, "kg_value": kg_value,
            "ref_value": ref_value, "evidence": evidence[:300],
            "adjudication": "", "classification": "", "notes": notes,
        })

    # ---- 1. play registry vs works catalogue ----
    play_align = align_plays(plays, works)
    matched_work_ids = set()
    for p in plays:
        al = play_align.get(p["play_id"])
        if not al:
            add("LEXICON_ONLY", "work_coverage", f"play:{p['play_id']}", "",
                p["title_yiddish"], "in created_expressions", "not in works catalogue")
            continue
        w, method = al
        matched_work_ids.add(w["ref_id"])
        if _author_match(p, w):
            add("CORROBORATED", "attribution", f"play:{p['play_id']}", w["ref_id"],
                p["title_yiddish"], p["author_heading"], w.get("author", ""),
                notes=method)
        else:
            add("CONTRADICTED", "attribution", f"play:{p['play_id']}", w["ref_id"],
                p["title_yiddish"],
                f"lexicon author db {p['author_db_id']}", w.get("author", ""),
                notes=method)
    for w in works:
        if w["ref_id"] not in matched_work_ids:
            add("REFERENCE_ONLY", "work_coverage", "", w["ref_id"],
                w.get("title_yi") or w.get("title_en"), "",
                w.get("author", ""))

    # ---- bridge: works english title -> reference productions play_key ----
    work_by_id = {w["ref_id"]: w for w in works}
    prods_by_key: dict[str, list[dict]] = defaultdict(list)
    for rp in ref_prods:
        k = rom_key(rp["play_key"])
        if k:
            prods_by_key[k].append(rp)
        # Yiddish titles in PerformanceEvents/editions.json rows
    seg_to_prods: dict[str, list[dict]] = defaultdict(list)
    for rp in ref_prods:
        for seg in (rp["title_segments_norm"] or "").split("|"):
            if seg:
                seg_to_prods[seg].append(rp)

    def ref_prods_for_play(p: dict) -> list[dict]:
        got: dict[str, dict] = {}
        segs = [s for s in (p["title_segments_norm"] or "").split("|") if s]
        for s in segs:
            for rp in seg_to_prods.get(s, []):
                got[rp["ref_id"]] = rp
        al = play_align.get(p["play_id"])
        if al:
            wk = rom_key(al[0].get("title_en", ""))
            if wk:
                for k, rps in prods_by_key.items():
                    if k and (k == wk or fuzz.token_set_ratio(k, wk) >= 90):
                        for rp in rps:
                            got[rp["ref_id"]] = rp
        return list(got.values())

    # ---- 2. premiere years + venues ----
    plays_by_node = {f"play:{p['play_id']}": p for p in plays}
    kg_events_by_play: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        kg_events_by_play[ev["play_id"]].append(ev)

    used_ref_prods = set()
    for play_node, evs in kg_events_by_play.items():
        p = plays_by_node.get(play_node)
        if not p:
            continue
        refs = ref_prods_for_play(p)
        ref_prem_years = sorted({r["year"] for r in refs
                                 if r["year"] and "prem" in r["event_type"].lower()})
        for r in refs:
            used_ref_prods.add(r["ref_id"])
        for ev in evs:
            y = (ev["date_start"] or "")[:4]
            if not y:
                continue
            if ev["event_type"] == "premiere" and ref_prem_years:
                if y in ref_prem_years or any(abs(int(y) - int(ry)) <= 1
                                              for ry in ref_prem_years):
                    add("CORROBORATED", "premiere_year", ev["event_id"],
                        ";".join(r["ref_id"] for r in refs[:3]),
                        p["title_yiddish"], y, ",".join(ref_prem_years),
                        ev["evidence_sentence"])
                else:
                    add("CONTRADICTED", "premiere_year", ev["event_id"],
                        ";".join(r["ref_id"] for r in refs[:3]),
                        p["title_yiddish"], y, ",".join(ref_prem_years),
                        ev["evidence_sentence"])
            elif ref_prem_years and y and all(int(y) < int(ry) - 1
                                              for ry in ref_prem_years):
                add("CONTRADICTED", "production_before_premiere", ev["event_id"],
                    ";".join(r["ref_id"] for r in refs[:3]),
                    p["title_yiddish"], y, ",".join(ref_prem_years),
                    ev["evidence_sentence"],
                    notes="KG production predates catalogued premiere")
            # venue check: same year, both venues present
            venue = node_label.get(ev["venue_id"], "")
            if venue:
                same_year = [r for r in refs if r["year"] == y and r["theatre"]]
                for r in same_year:
                    sim = fuzz.token_set_ratio(rom_key(r["theatre"]),
                                               rom_key(venue)) if venue.isascii() \
                        else 0
                    bucket = "CORROBORATED" if sim >= 80 else "CONTRADICTED"
                    if not venue.isascii():
                        bucket = "LEXICON_ONLY"  # cross-script venue compare needs adjudication
                    add(bucket, "venue", ev["event_id"], r["ref_id"],
                        p["title_yiddish"], venue, r["theatre"],
                        ev["evidence_sentence"],
                        notes="cross-script venue — adjudicate manually"
                        if not venue.isascii() else f"sim={sim}")
    for rp in ref_prods:
        if rp["ref_id"] not in used_ref_prods:
            add("REFERENCE_ONLY", "production", "", rp["ref_id"],
                rp.get("title_yi") or rp.get("play_key"), "",
                f"{rp['event_type']} {rp['year']} {rp['theatre']}".strip())

    # ---- 3. roles ----
    ref_roles_by_seg: dict[str, list[dict]] = defaultdict(list)
    for rr in ref_roles:
        for seg in (rr["title_segments_norm"] or "").split("|"):
            ref_roles_by_seg[seg].append(rr)
    cast_edges = [e for e in edges if e["edge_type"] in
                  ("performed_in", "directed", "composed_music", "prompted",
                   "translated_adapted", "cast_in")]
    n_role_checked = 0
    for e in cast_edges:
        play_node = e["target_id"] if e["target_id"].startswith("play:") else ""
        p = plays_by_node.get(play_node)
        if not p:
            continue
        refs = {id(r): r for s in (p["title_segments_norm"] or "").split("|")
                for r in ref_roles_by_seg.get(s, [])}
        if not refs:
            continue
        n_role_checked += 1
        person_label = node_label.get(e["source_id"], "")
        hits = [r for r in refs.values()
                if person_label and (person_label in r["person"]
                                     or fuzz.partial_ratio(person_label, r["person"]) >= 85)]
        if hits:
            add("CORROBORATED", "person_role", e["edge_id"], hits[0]["ref_id"],
                p["title_yiddish"], f"{person_label} ({e['role_detail']})",
                hits[0]["person"], e["evidence_sentence"])

    # ---- 4. gold recall + homonym precision (drafts vs hand gold) ----
    gold_rows = pc.read_tsv(pc.GOLD_DIR / "gold_entries.tsv")
    coverage = {r["person_id"]: r["status"]
                for r in pc.read_tsv(pc.GOLD_DIR / "gold_coverage.tsv")}
    drafts = [r for r in pc.read_tsv(pc.DRAFTS_TSV)
              if r.get("fact_type") not in ("", "none")]
    drafts_by_pid = defaultdict(list)
    for d in drafts:
        drafts_by_pid[d["person_id"]].append(d)

    def same_play(a: dict, b: dict) -> bool:
        if a.get("play_id_hint") and a.get("play_id_hint") == b.get("play_id_hint"):
            return True
        ta = set(pc.title_segments(a.get("play_title_surface", "")))
        tb = set(pc.title_segments(b.get("play_title_surface", "")))
        return bool(ta & tb)

    gold_facts = [g for g in gold_rows
                  if g["fact_type"] in pc.FACT_TYPES
                  and coverage.get(g["person_id"]) == "complete"]
    recall_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for g in gold_facts:
        cand = drafts_by_pid.get(g["person_id"], [])
        hit = any(same_play(g, d)
                  and (g["person_surface"] == "[HOST]") == (d["person_surface"] == "[HOST]")
                  for d in cand)
        recall_stats[g["fact_type"]][1] += 1
        if hit:
            recall_stats[g["fact_type"]][0] += 1
        else:
            add("LEXICON_ONLY", "gold_missed", "", g["fact_id"],
                g.get("play_title_surface", ""),
                "", f"{g['fact_type']}/{g['person_role']}",
                g.get("evidence_quote", ""), notes="gold fact not recovered by pipeline")
    gold_homonyms = [g for g in gold_rows if g["fact_type"] == "excluded_homonym"
                     and coverage.get(g["person_id"]) == "complete"]
    n_homonym_viol = 0
    for g in gold_homonyms:
        for d in drafts_by_pid.get(g["person_id"], []):
            if same_play(g, d) and d.get("play_id_hint") \
                    and d["fact_type"] not in ("mention_only",):
                n_homonym_viol += 1
                add("CONTRADICTED", "homonym_false_positive", d["fact_id"],
                    g["fact_id"], g.get("play_title_surface", ""),
                    f"{d['fact_type']} -> {d['play_id_hint']}",
                    "gold: another author's play",
                    d.get("evidence_quote", ""), notes=g.get("notes", ""))
    print("\ngold recall (complete entries only):")
    for ft, (hit, tot) in sorted(recall_stats.items()):
        print(f"  {ft:24s} {hit}/{tot}")
    tot_hit = sum(v[0] for v in recall_stats.values())
    tot_all = sum(v[1] for v in recall_stats.values())
    print(f"  overall: {tot_hit}/{tot_all}"
          f"  | homonym false-positives: {n_homonym_viol}/{len(gold_homonyms)}")

    counts = Counter(f["bucket"] for f in findings)
    by_aspect = Counter((f["bucket"], f["aspect"]) for f in findings)
    print(f"findings: {len(findings)}  {dict(counts)}")
    for (b, a), n in sorted(by_aspect.items()):
        print(f"  {b:14s} {a:26s} {n}")

    if not args.execute:
        print("dry-run — pass --execute to write eval_findings.tsv / eval_report.md")
        return

    # flag attribution conflicts back into the registry (flag only — the
    # lexicon-derived author assignment is kept; PI adjudicates)
    conflicted_ids = {f["kg_ref"].removeprefix("play:") for f in findings
                      if f["bucket"] == "CONTRADICTED" and f["aspect"] == "attribution"}
    n_flagged = 0
    for p in plays:
        if p["play_id"] in conflicted_ids and p["attribution_status"] == "single":
            p["attribution_status"] = "catalogue_conflict"
            n_flagged += 1
    if n_flagged:
        from build_plays_db import PLAYS_FIELDS
        pc.write_tsv(pc.PLAYS_DB_TSV, plays, PLAYS_FIELDS)
        print(f"flagged {n_flagged} plays attribution_status=catalogue_conflict in plays_db")

    for i, f in enumerate(findings, 1):
        f["finding_id"] = f"F-{i:04d}"
    prior = {(f["bucket"], f["aspect"], f["kg_ref"], f["reference_ref"]): f
             for f in pc.read_tsv(FINDINGS_TSV)}
    for f in findings:
        old = prior.get((f["bucket"], f["aspect"], f["kg_ref"], f["reference_ref"]))
        if old:
            f["adjudication"] = old.get("adjudication", "")
            f["classification"] = old.get("classification", "")
    pc.write_tsv(FINDINGS_TSV, findings, FINDING_FIELDS)

    lines = ["# KG evaluation report", "",
             f"findings: {len(findings)}", ""]
    for b in ("CORROBORATED", "CONTRADICTED", "LEXICON_ONLY", "REFERENCE_ONLY"):
        lines.append(f"- **{b}**: {counts.get(b, 0)}")
    lines += ["", "## By aspect", ""]
    for (b, a), n in sorted(by_aspect.items()):
        lines.append(f"- {b} / {a}: {n}")
    lines += ["", "## Contradictions requiring adjudication", ""]
    for f in findings:
        if f["bucket"] == "CONTRADICTED":
            lines.append(f"- `{f['finding_id']}` [{f['aspect']}] {f['play']}: "
                         f"KG={f['kg_value']!r} vs ref={f['ref_value']!r} "
                         f"({f['kg_ref']} / {f['reference_ref']})")
    REPORT_MD.parent.mkdir(exist_ok=True)
    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {FINDINGS_TSV} and {REPORT_MD}")


if __name__ == "__main__":
    main()
