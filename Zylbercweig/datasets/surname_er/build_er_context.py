"""Step 8a: entity-resolution context from ALREADY-STRUCTURED data (no LLM).

Builds two tables for resolving surname-only / initials person mentions:

  er_candidate_profiles.tsv  one row per person hub (who a surname could mean):
      gender, birth/death years, birth/death/burial places (Wikidata QIDs),
      organizations (core_db id or org cluster, relation category, dates, settlement),
      people_db roles + Wikidata id.
  er_mention_context.tsv     one row per surname-only / initials mention:
      the host entry's person (resolved) with its orgs/places/years, the mention's
      relation, place and date, job-A in-entry full form, job-C organization
      (mapped to core_db by joining the sentence to the Feb person→org relations),
      candidate hubs, the existing resolver's verdict, and RA gold where it exists.

  python3.11 build_er_context.py
"""
import csv, re, sys
from collections import defaultdict, Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ZYL = HERE.parent.parent
sys.path.insert(0, str(ZYL / "people"))
csv.field_size_limit(10**9)

from people_common import load_mentions_with_host, mention_volume, read_tsv  # noqa: E402
from people_similarity import normalize_person_name  # noqa: E402
from resolve_surname_mentions import HubIndex, extract_year  # noqa: E402

V2 = ZYL / "extraction_v2"
HONORIFICS = {"מר.", "מר", "מרס.", "מרס", "מיס", "הער", "פֿרוי", "ד״ר", 'ד"ר', "דר.", "מאַדאַם"}


def tsv(path):
    return list(csv.DictReader(open(path, encoding="utf-8"), delimiter="\t"))


def toks(s):
    return set(normalize_person_name(s).split())


def main():
    print("loading hub index…")
    idx = HubIndex()
    hubs = {h: r for h, r in idx.hubs.items() if r.get("non_person") != "1"}
    pid2hub = idx.hub_of_pid
    hub_surnames = defaultdict(set)
    for tok, hs in idx.surname_to_hubs.items():
        for h in hs:
            hub_surnames[h].add(tok)

    # ---- organizations: core_db cluster → db_id ----
    core = tsv(ZYL / "organizations" / "core_db.tsv")
    cl2db, dbname = {}, {}
    for r in core:
        if r.get("deprecated", "").strip():
            continue
        dbname[r["db_id"]] = r["name_yiddish"] or r["name"]
        for c in (r.get("linked_cluster_ids") or "").replace(",", "|").split("|"):
            if c.strip():
                cl2db[c.strip()] = r["db_id"]

    def org_id(db, cl):
        db = db or cl2db.get(cl, "")
        return f"db:{db}" if db else (f"cl:{cl}" if cl else "")

    # ---- person→org edges per hub ----
    hub_orgs = defaultdict(list)
    for e in tsv(ZYL / "organizations" / "graph" / "person_org_edges.tsv"):
        h = e["person_hub_id"] or pid2hub.get(f"P-{mention_volume(e['host_file'])}-{e['host_xmlid']}", "")
        if not h:
            continue
        oid = org_id(e["db_id"], e["cluster_id"])
        if oid:
            hub_orgs[h].append((oid, e["canonical_yiddish"], e["rel_categories"], e["dates"],
                                e["settlements"], e["edge_source"]))

    # ---- vital places (QID-linked) per hub ----
    hub_places = defaultdict(lambda: defaultdict(set))
    for r in tsv(ZYL / "datasets" / "place_mentions_with_context.tsv"):
        if r["source_kind"] != "vital_place":
            continue
        h = pid2hub.get(r["entry_id"])
        if h and (r["wikidata_qid"] or r["place_as_written"]):
            st = r["link_status"]
            qid = r["wikidata_qid"] if st in ("linked", "needs_review") else ""  # never misresolved/unlinked
            name = r["name_en"] if qid else r["place_as_written"]
            label = f"{name}" + (f" ({qid}{'?' if st == 'needs_review' else ''})" if qid else "")
            hub_places[h][r["event_or_relation"]].add(label)

    # ---- D–G (11-22): the entry subject's own stations / years / performance facts ----
    import re as _re
    D = V2 / "defg"
    def base(eid):
        return eid if "~" not in eid else ""  # ~aN / ~gN are other subjects (split articles, glued entries)
    ent_places, ent_years, ent_works = defaultdict(list), defaultdict(set), defaultdict(list)
    ent_evid = defaultdict(list)
    if (D / "stations.tsv").exists():
        for r in tsv(D / "stations.tsv"):
            e = base(r["entry_id"])
            if not e or not r["subject_key"].endswith(":p0") or r["in_quote"]:
                continue
            y = (_re.match(r"1[6-9]\d\d", r["date_start"] or "") or [""])[0]
            if r["place"]:
                ent_places[e].append(f"{r['place']}{':' + y if y else ''}")
                ent_evid[(e, r["place"])].append(r["evidence"] or "")
            for d in (r["date_start"], r["date_end"]):
                m_ = _re.match(r"1[6-9]\d\d", d or "")
                if m_:
                    ent_years[e].add(int(m_.group(0)))
        for r in tsv(D / "performance_facts.tsv"):
            e = base(r["entry_id"])
            if not e or not r["person_key"].endswith(":p0") or r["in_quote"] or not r["work"]:
                continue
            ent_works[e].append(f"{r['function']}:{r['work']}" + (f" ({r['character']})" if r["character"] else ""))

    # ---- 11-24: link each D–G station place, context-checked (place_linker.py) ----
    sys.path.insert(0, str(HERE))
    from place_linker import Linker  # noqa: E402
    linker = Linker()
    link_how = Counter()

    def defg_of(entry_ids):
        es = [x for x in (entry_ids or "").split("|") if x]
        pl, ys, wk = [], set(), []
        for e in es:
            pl += ent_places.get(e, []); ys |= ent_years.get(e, set()); wk += ent_works.get(e, [])
        uniq = lambda xs: list(dict.fromkeys(xs))
        upl = uniq(pl)
        split = lambda x: (x.rpartition(":")[0], x.rpartition(":")[2]) if _re.search(r":1[6-9]\d\d$", x) else (x, "")
        # centroid of this person's unambiguous stations, for homonym choice
        pts = [c for c in (linker.coords(split(x)[0]) for x in upl) if c]
        cen = (sum(c[0] for c in pts) / len(pts), sum(c[1] for c in pts) / len(pts)) if pts else None
        def en_of(x):
            place, y = split(x)
            ev = " ".join(v for e in es for v in ent_evid.get((e, place), []))
            own = next((e for e in es if (e, place) in ent_evid), "")
            lab, how = linker.link(place, ev, cen, own)
            link_how[how] += 1
            return (lab or "?") + (f":{y}" if y else "")
        # truncate both lists at the same station so position i means the same station in each
        keep, n = [], 0
        for x in upl:
            n += len(x) + 1
            if n > 1500:
                break
            keep.append(x)
        return {"defg_places": "|".join(keep),
                "defg_places_en": "|".join(en_of(x) for x in keep),
                "defg_years": f"{min(ys)}-{max(ys)}" if ys else "",
                "defg_works": " ;; ".join(uniq(wk)[:60])}

    # ---- people_db ----
    pdb = {r["db_id"]: r for r in tsv(ZYL / "people" / "people_db.tsv")}

    # ================= candidate profiles =================
    prof_rows = []
    for h, r in hubs.items():
        dbs = [pdb[d] for d in r["db_ids"].split("|") if d in pdb]
        orgs = hub_orgs.get(h, [])
        oc = Counter(o[0] for o in orgs)
        wd = sorted({d["external_source_id"] for d in dbs
                     if "wikidata" in (d.get("external_source") or "").lower() and d.get("external_source_id")})
        prof_rows.append({
            "hub_id": h, "heading": r["canonical_heading"],
            "surname_keys": "|".join(sorted(hub_surnames.get(h, ()))),
            "name_surfaces": r.get("top_surfaces", ""),
            "gender": idx.hub_gender.get(h, ""),
            "birth_year": idx.hub_birth.get(h) or "", "death_year": idx.hub_death.get(h) or "",
            "birth_place": "|".join(sorted(hub_places[h].get("birth", ()))),
            "death_place": "|".join(sorted(hub_places[h].get("death", ()))),
            "burial_place": "|".join(sorted(hub_places[h].get("burial", ()))),
            "n_orgs": len(oc),
            "org_ids": "|".join(o for o, _ in oc.most_common()),
            "orgs_detail": " ;; ".join(f"{o[0]}={o[1]} [{o[2]}] {o[3]} @{o[4]}".strip() for o in orgs[:60]),
            "active_settlements": "|".join(sorted({s for o in orgs for s in o[4].split("|") if s})),
            "org_years": "|".join(sorted({y for o in orgs for y in re.findall(r"1[6-9]\d\d", o[3])})),
            "roles_db": "|".join(sorted({x for d in dbs for x in (d.get("professional_role_perf", "") + "|"
                                          + d.get("professional_role_org", "")).split("|") if x.strip()}))[:300],
            "wikidata": "|".join(wd),
            "lexicon_entries": r["entry_person_ids"],
            **defg_of(r["entry_person_ids"]),
        })
    pcols = list(prof_rows[0].keys())
    with open(HERE / "er_candidate_profiles.tsv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, pcols, delimiter="\t"); w.writeheader(); w.writerows(prof_rows)
    prof = {p["hub_id"]: p for p in prof_rows}
    print("place links:", dict(link_how.most_common()))
    print(f"er_candidate_profiles.tsv: {len(prof_rows)} hubs; with orgs {sum(1 for p in prof_rows if p['n_orgs'])}; "
          f"with birth place {sum(1 for p in prof_rows if p['birth_place'])}")

    # ================= mention context =================
    A = {r["mention_id"]: r for r in tsv(V2 / "a_full_form.tsv")}
    B = {r["mention_id"]: r for r in tsv(V2 / "b_recategorized.tsv")}
    C = {r["mention_id"]: r for r in tsv(V2 / "c_eponym_orgs.tsv")}
    RT = {r["mention_id"]: r for r in tsv(V2 / "r_retyped.tsv")} if (V2 / "r_retyped.tsv").exists() else {}
    # entries whose segmentation was repaired 11-22: a mention's host may be wrong for part of the text
    SEG = {}
    for f, kinds in (("segment_fixes.tsv", ("merged_article", "split_section")), ("glued_entry_fixes.tsv", ("glued_entry",))):
        pth = V2 / "qa" / f
        if pth.exists():
            for r in tsv(pth):
                if r["kind"] in kinds:
                    SEG[r["person_id"]] = r["kind"]
    import json
    def verdicts(name):
        f = V2 / f"verify_{name}.jsonl"
        out = {}
        if f.exists():
            for line in f.open(encoding="utf-8"):
                rec = json.loads(line)
                if "error" not in rec:
                    out[rec["id"]] = rec
        return out
    VC, VA, VB = verdicts("c_org"), verdicts("a_kin"), verdicts("b_src")
    RES = {r["mention_id"]: r for r in tsv(ZYL / "people" / "mention_surname_resolutions.tsv")}

    # Feb person→org relation rows per host, for mapping job-C organizations to core_db
    host_orgrows = defaultdict(list)
    for r in tsv(ZYL / "organizations" / "organizations_clustered.tsv"):
        x = r.get("_ - xml:id", "").strip()
        if not x:
            continue
        host_orgrows[(mention_volume(r["File"]), x.split("-", 1)[-1] if re.match(r"^\d+-", x) else x)].append(
            (toks(r["_ - organizations - _ - relations - _ - original_sentence"]),
             toks(r["_ - organizations - _ - title"] + " " + r["clustered organization"]),
             r["cluster_id"], r["canonical_yiddish"]))

    def map_c_org(m, c):
        rows = host_orgrows.get((m["host_volume"], m["host_xml_id_filled"]), [])
        st, ot = toks(m["relation_sentence"]), toks(c["org_surface"])
        best, bscore = None, 0.0
        for sent, title, cl, canon in rows:
            j = len(st & sent) / max(1, len(st | sent))
            t = len(ot & title) / max(1, len(ot)) if ot else 0
            score = max(j, t * 0.9)
            if score > bscore:
                best, bscore = (cl, canon), score
        if best and bscore >= 0.5:
            return org_id("", best[0]), best[1], round(bscore, 2)
        return "", "", round(bscore, 2)

    # curated rejections (people/mention_surface_rejections.tsv): a rejected surface must not
    # survive as gold / resolver answer for the hub it was wrongly aligned to
    rej = {}
    for rr in tsv(ZYL / "people" / "mention_surface_rejections.tsv"):
        heads = {h for h, hr in hubs.items() if normalize_person_name(hr["canonical_heading"]) == normalize_person_name(rr["as_heading"])}
        rej[normalize_person_name(rr["mention_surface"])] = (rr["action"], heads)

    out, n_cmap, n_rej = [], 0, 0
    for m in load_mentions_with_host():
        mid = m["mention_id"]
        if mid not in A:
            continue
        a, b, c, res = A[mid], B.get(mid), C.get(mid), RES.get(mid)
        host_hub = pid2hub.get(m["host_person_id"], "")
        hp = prof.get(host_hub, {})
        name = [t for t in m["mention_name"].split() if t not in HONORIFICS]
        token = res["surname_token"] if res else normalize_person_name(name[-1] if name else "")
        cands = ([x for x in res["candidate_hub_ids"].split("|") if x] if res
                 else [x for x in idx.candidate_hubs(token) if x in hubs])
        cands = [x for x in cands if x != host_hub] or cands
        c_oid = c_name = c_score = ""
        if c and c["org_mediated"] == "True":
            c_oid, c_name, c_score = map_c_org(m, c)
            if c_oid and VC.get(mid, {}).get("ok") is False:  # Gemini check: not the same organization
                c_oid, c_name, c_score = "", "", f"{c_score}|check_failed"
            n_cmap += bool(c_oid)
        # overlap features between mention context and each candidate (cheap baseline signals)
        host_orgs = set(hp.get("org_ids", "").split("|")) - {""}
        feats = []
        for h in cands[:25]:
            p = prof.get(h, {})
            po = set(p.get("org_ids", "").split("|")) - {""}
            shared = len(host_orgs & po)
            corg = int(bool(c_oid) and c_oid in po)
            feats.append(f"{h}:shared_orgs={shared},c_org={corg},born={p.get('birth_year','')}")
        out.append({
            "mention_id": mid, "volume": m["host_volume"], "mention_name": m["mention_name"],
            "surname_key": token, "extraction_issue": m["extraction_issue"],
            "gender": m["gender"], "person_description": m["person_description"][:200],
            "host_hub_id": host_hub, "host_heading": m["host_heading_filled"],
            "host_birth_year": hp.get("birth_year", ""), "host_birth_place": hp.get("birth_place", ""),
            "host_org_ids": hp.get("org_ids", ""), "host_active_settlements": hp.get("active_settlements", ""),
            "relation_category": (b["category"] if b else RT[mid]["category"] if mid in RT else m["relation_category"]),
            "relation_type": (b["type"] if b else RT[mid]["type"] if mid in RT else m["relation_specific"]),
            "relation_source": ("v2_job_B" + ({True: "|checked_ok", False: "|check_failed"}.get(VB.get(mid, {}).get("ok"), ""))) if b
                               else ("v2_8f_retyped" + ("|hand_read" if "[read" in RT[mid].get("schema_issue", "") else "")) if mid in RT else "feb_2026",
            "relation_old": f"{m['relation_category']}/{m['relation_specific']}" if (b or mid in RT) else "",
            "host_defg_places": hp.get("defg_places", ""), "host_defg_places_en": hp.get("defg_places_en", ""),
            "host_defg_years": hp.get("defg_years", ""),
            "relation_sentence": m["relation_sentence"][:400],
            "rel_settlement": m["rel_place_settlement"], "rel_country": m["rel_place_country"],
            "rel_venue": m["rel_place_venue"], "rel_date_start": m["rel_date_start"], "rel_date_end": m["rel_date_end"],
            "in_entry_full_form": "" if VA.get(mid, {}).get("ok") is False else a["full_form"],
            "in_entry_method": a["method"] + ("|check_failed" if VA.get(mid, {}).get("ok") is False else ""),
            # Sinai 11-21 (decision 6): a relative's full form built from the family's surname is an
            # INFERENCE (stage names, married names), never a stated name and never gold.
            "in_entry_status": ("" if not a.get("full_form") else
                                "rejected" if VA.get(mid, {}).get("ok") is False else
                                "inferred" if "kin_of_subject" in a.get("method", "") else "stated"),
            "c_org_surface": c["org_surface"] if c else "", "c_named_after_mention": c["named_after_mention"] if c else "",
            "c_mention_role": c["mention_role"] if c else "", "c_org_id": c_oid, "c_org_name": c_name,
            "c_org_match_score": c_score,
            "n_candidates": len(cands), "candidate_hub_ids": "|".join(cands),
            "candidate_features": " ;; ".join(feats),
            "resolver_verdict": res["verdict"] if res else "", "resolver_hub_id": res["resolved_hub_id"] if res else "",
            "gold_hub_id": res["gold_hub_id"] if res else "",
            "flag": ("host_segmentation:" + SEG[m["host_person_id"]]) if m.get("host_person_id") in SEG else "",
        })
        rj = rej.get(normalize_person_name(m["mention_name"]))
        if rj:
            o = out[-1]
            if rj[0] == "reject_not_a_person":
                o["flag"] = "not_a_person"; o["gold_hub_id"] = o["resolver_hub_id"] = ""; n_rej += 1
            else:
                for k in ("gold_hub_id", "resolver_hub_id"):
                    if o[k] in rj[1]:
                        o[k] = ""; o["flag"] = "rejected_alignment"; n_rej += 1
    # 2026-11-24 gold check: the old "gold_hub_id" is ONE research-assistant pick per bare surname
    # (surnames sheet `as_heading`), copied to every mention -- a surname prior, not a per-mention
    # answer. Rename it, add the sheet's listed alternative(s), and add per-mention answers only
    # where a mention was actually read in context.
    head2hub = defaultdict(set)
    for h, hr in hubs.items():
        head2hub[normalize_person_name(hr["canonical_heading"])].add(h)
    alts = defaultdict(set)
    for r in tsv(ZYL / "people" / "derived_mention_alignments.tsv"):
        if r["source_sheet"] == "surnames":
            for a in (r["alternative_heading"] or "").split("|"):
                alts[normalize_person_name(r["mention_surface"])] |= head2hub.get(normalize_person_name(a), set())
    OTHER = {"M-volume_1IIIorg-NOXID-לינעצקי-001": "HUB-D2311",       # Yitskhok-Yoel Linetski (with Goldfaden)
             "M-Volume_4IIIorg-NOXID-גראַדנער-002": "HUB-D900",        # Israel Grodner, 1880s troupe
             "M-volume_2IIIorg-NOXID-גראדנער-003": "HUB-D900"}
    checked = {}
    for r in tsv(ZYL / "people" / "er_gold_vs_defg_fullform_2026_11_24.tsv"):
        v = r["verdict"]
        h = {"defg_right": r["defg_points_to"], "gold_right": r["gold_hub_id"]}.get(v) or OTHER.get(r["mention_id"], "")
        if h:
            checked[r["mention_id"]] = (h, f"read_in_context_2026-11-24:{r['confidence']}")
    for o in out:
        prior = o.pop("gold_hub_id")
        flag = o.pop("flag")
        cset = set(o["candidate_hub_ids"].split("|"))
        o["surname_prior_hub_id"] = prior
        o["surname_prior_alt_hub_ids"] = "|".join(sorted((alts.get(o["surname_key"], set()) & cset) - {prior})) if prior else ""
        ch = checked.get(o["mention_id"])
        o["checked_hub_id"], o["checked_how"] = ch if ch else ("", "")
        o["flag"] = flag
    mcols = list(out[0].keys())
    with open(HERE / "er_mention_context.tsv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, mcols, delimiter="\t"); w.writeheader(); w.writerows(out)
    print(f"er_mention_context.tsv: {len(out)} mentions; host hub found {sum(1 for o in out if o['host_hub_id'])}; "
          f"with candidates {sum(1 for o in out if o['n_candidates'])}; surname prior {sum(1 for o in out if o['surname_prior_hub_id'])}; checked {sum(1 for o in out if o['checked_hub_id'])}; "
          f"C org → core_db/cluster {n_cmap}/{sum(1 for o in out if o['c_org_surface'])}; "
          f"rejection fixes {n_rej}; rel place {sum(1 for o in out if o['rel_settlement'])}; rel date {sum(1 for o in out if o['rel_date_start'])}")


if __name__ == "__main__":
    main()
