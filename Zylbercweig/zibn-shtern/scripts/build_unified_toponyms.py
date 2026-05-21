"""Build the unified toponym dataset around an attestation spine.

Architecture
------------
`toponyms_attestations.csv` is the source of truth: ONE row per attestation,
never deduplicated, for BOTH corpora, each carrying a back-pointer to its source
record so resolved links can later be enriched back into the people / org data:
  - person attestations  -> source_record_id = entry_id            (places_unified / raw extraction)
  - org attestations     -> source_record_id = cluster_id, + org_db_id (aligned_db_id -> core_db)

`toponyms_gazetteer.csv` (per QID) and `toponyms_unlinked.csv` (per unresolved
spelling) are DERIVED views — group-bys of the spine. The unlinked view keeps an
`attestation_ids` list so every entry maps straight back to its source rows.

Inputs
------
  person linked   : data/working/places_unified_corrected.csv   (resolved mention hub)
  person raw      : data/raw/Zylbercweig-Extraction2026-02-05-places.tsv  (for UNLINKED person spellings)
  person alt-QID  : ../ZylbercweigPlacesMaaty.tsv                (flagged, never overwrites)
  Kima IDs        : data/working/kimatch_matched_full.tsv        (1:1 by QID)
  Kima variants   : data/working/kima_variants_export.tsv
  org attestations: ../organizations/org_alignment_review.tsv    (extracted_* fields per cluster_id)
  org resolution  : ../organizations/settlement_variant_collapse_audit_2026-05-20.tsv  (cluster_id+variant -> qid)
  org coords      : ../organizations/settlement_coords.tsv       (lat/lon by QID)
  org punchlist   : ../organizations/unresolved_settlements_punchlist.tsv  (suggested_qid for unlinked)

The raw extraction is otherwise upstream of places_unified (its linked spellings
already live there); we read it only to recover UNLINKED person attestations with
their entry_id back-pointers.
"""
from __future__ import annotations

import csv
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORK = HERE.parent / "data" / "working"
ZYL = HERE.parent.parent  # Zylbercweig/
ORG = ZYL / "organizations"

UNIFIED = WORK / "places_unified_corrected.csv"
KIMATCH = WORK / "kimatch_matched_full.tsv"
KVAR = WORK / "kima_variants_export.tsv"
RAW = WORK.parent / "raw" / "Zylbercweig-Extraction2026-02-05-places.tsv"
MAATY = ZYL / "ZylbercweigPlacesMaaty.tsv"
COLLAPSE = ORG / "settlement_variant_collapse_audit_2026-05-20.tsv"
COORDS = ORG / "settlement_coords.tsv"
ORG_REVIEW = ORG / "org_alignment_review.tsv"
ORG_PUNCH = ORG / "unresolved_settlements_punchlist.tsv"
# Kima IDs confirmed by QID via the kimatch skill (supplements kimatch_matched_full).
KIMA_BACKFILL = WORK / "kima" / "kima_backfill_confirmed.tsv"
# Maaty alternate QIDs validated as places (Kima membership or Wikidata P31), used to
# relink attestations whose primary QID is a mis-resolved non-place.
MAATY_RELINK = WORK / "kima" / "maaty_relink_validated.tsv"

ATT_OUT = WORK / "toponyms_attestations.csv"
GAZ_OUT = WORK / "toponyms_gazetteer.csv"
UNLINKED_OUT = WORK / "toponyms_unlinked.csv"
MISRESOLVED_OUT = WORK / "toponyms_misresolved.csv"

_HEB = re.compile(r"[֐-׿יִ-ﭏ]")
_LAT = re.compile(r"[A-Za-z]")
_POINTS = re.compile(r"[֑-ׇֽֿׁׂׅׄ]")
_DESCRIPTOR_RE = re.compile(
    r"נעבן|ביים|דארף|שטעטל|פראווינץ|אויפן פראנט|דאָרף ביי|ווייט פון|קליין שטעטל"
)
_ORG_FIELDS = ("extracted_settlements", "extracted_venues",
               "extracted_addresses", "extracted_countries")


def script_of(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "empty"
    h, l = bool(_HEB.search(s)), bool(_LAT.search(s))
    if h and l:
        return "mixed"
    return "hebrew" if h else "latin" if l else "other"


def _strip_points(s: str) -> str:
    return _POINTS.sub("", unicodedata.normalize("NFKD", s or ""))


# Wikidata types that mean the QID is NOT a place — a mis-resolution.
# 'human settlement' is a place, so 'human' is matched only as a standalone word.
_NONPLACE_RE = re.compile(
    r"\b(taxon|genus|species|Wikimedia disambiguation|album|film|written work"
    r"|given name|family name|surname)\b", re.I)


def nonplace_kind(place_type: str, category: str) -> str:
    """Return the non-place kind for a QID's type strings, or '' if it looks like a place."""
    t = f"{place_type} | {category}".replace("human settlement", "").replace("human-readable", "")
    m = _NONPLACE_RE.search(t)
    if m:
        return m.group(1).lower()
    if re.search(r"\bhuman\b", t, re.I):
        return "human(person)"
    return ""


def is_descriptor(variant: str, note: str = "") -> bool:
    if "descriptor" in (note or "").lower():
        return True
    v = _strip_points(variant).strip()
    if _DESCRIPTOR_RE.search(v):
        return True
    return v.startswith("א ") or bool(re.match(r"^\d", v))


def is_qid(v: str) -> bool:
    v = (v or "").strip()
    return v.startswith("Q") and v[1:].isdigit()


def rd(path: Path, delim: str = "\t") -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter=delim))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def split_values(cell: str) -> list[str]:
    return [p.strip() for p in re.split(r"[|;]", cell or "") if p.strip()]


ATT_COLS = [
    "attestation_id", "source_corpus", "source_record_id", "org_db_id",
    "source_field", "context", "source_value", "source_value_script",
    "link_status", "qid", "label_en", "label_yi", "place_type", "category",
    "kima_id", "kima_rom", "kima_heb", "lat", "lon",
    "maaty_qid", "maaty_qid_conflict", "rejected_qid", "relink_source",
    "suggested_qid", "suggested_english", "is_descriptor", "review_flags",
]


def main() -> None:
    unified = rd(UNIFIED, ",")
    kimatch = rd(KIMATCH)
    kvar = rd(KVAR)
    raw = rd(RAW)
    maaty = rd(MAATY)
    collapse = rd(COLLAPSE)
    coords = rd(COORDS)
    org_review = rd(ORG_REVIEW)
    org_punch = rd(ORG_PUNCH)

    # ---- QID-keyed enrichment lookups ----
    kima_by_qid: dict[str, dict] = {}
    for r in kimatch:
        q = r["wikidata_qid"].strip()
        if is_qid(q) and r.get("kima_id", "").strip():
            kima_by_qid.setdefault(q, {"kima_id": r["kima_id"].strip(),
                                       "kima_rom": r.get("kima_rom", "").strip(),
                                       "kima_heb": r.get("kima_heb", "").strip()})
    # supplement with QID-confirmed backfill from the kimatch skill (does not override)
    if KIMA_BACKFILL.exists():
        for r in rd(KIMA_BACKFILL):
            q = r["qid"].strip()
            if is_qid(q) and r.get("kima_id", "").strip():
                kima_by_qid.setdefault(q, {"kima_id": r["kima_id"].strip(),
                                           "kima_rom": r.get("kima_rom", "").strip(),
                                           "kima_heb": r.get("kima_heb", "").strip()})
    coords_by_qid = {r["qid"].strip(): (r["lat"].strip(), r["lon"].strip())
                     for r in coords if is_qid(r["qid"].strip())}
    label_en: dict[str, str] = {}
    label_yi: dict[str, str] = {}
    ptype: dict[str, str] = {}
    category: dict[str, str] = {}
    for r in unified:
        q = r["qid"].strip()
        if not is_qid(q):
            continue
        label_en.setdefault(q, r.get("wikidata_label_en", "").strip())
        label_yi.setdefault(q, r.get("wikidata_label_yi", "").strip())
        ptype.setdefault(q, r.get("wikidata_type", "").strip())
        category.setdefault(q, r.get("resolved_category", "").strip())
    for r in kimatch:
        q = r["wikidata_qid"].strip()
        if is_qid(q):
            label_en.setdefault(q, r.get("english_name", "").strip())

    # Validated Maaty relink targets (places). Feed their labels/type/Kima into the
    # enrichment maps so relinked attestations resolve fully.
    relink_ok: set[str] = set()
    if MAATY_RELINK.exists():
        for r in rd(MAATY_RELINK):
            q = r["maaty_qid"].strip()
            if not is_qid(q):
                continue
            relink_ok.add(q)
            if r.get("label_en", "").strip():
                label_en.setdefault(q, r["label_en"].strip())
            if r.get("label_yi", "").strip():
                label_yi.setdefault(q, r["label_yi"].strip())
            if r.get("place_type", "").strip():
                ptype.setdefault(q, r["place_type"].strip())
            category.setdefault(q, "settlement")
            if r.get("kima_id", "").strip():
                kima_by_qid.setdefault(q, {"kima_id": r["kima_id"].strip(),
                                           "kima_rom": r.get("kima_rom", "").strip(),
                                           "kima_heb": r.get("kima_heb", "").strip()})

    # QIDs whose Wikidata type reveals a mis-resolution (person/taxon/disambiguation/…).
    nonplace_qids = {q: nonplace_kind(ptype.get(q, ""), category.get(q, ""))
                     for q in set(ptype) | set(category)}
    nonplace_qids = {q: k for q, k in nonplace_qids.items() if k}

    # Maaty alternate resolution by mention key
    maaty_by_key = {(r["entry_id"], r["source_role"], r["source_value"].strip()):
                    (r.get("WikidataQID", "").strip(), r.get("English Name", "").strip())
                    for r in maaty}

    # org resolution: (cluster_id, variant) -> (qid, english)
    org_res: dict[tuple, tuple] = {}
    for r in collapse:
        q = r["qid"].strip()
        if is_qid(q):
            org_res[(r["cluster_id"].strip(), r["original_variant"].strip())] = (q, r.get("english", "").strip())
            label_en.setdefault(q, r.get("english", "").strip())
    # org unlinked suggestions by spelling
    sugg_by_variant = {}
    for r in org_punch:
        v = r.get("yiddish", "").strip()
        if v and is_qid(r.get("suggested_qid", "")):
            sugg_by_variant[v] = (r["suggested_qid"].strip(), r.get("suggested_english", "").strip(),
                                  r.get("notes", ""))
        elif v and r.get("notes", "").strip():
            sugg_by_variant.setdefault(v, ("", "", r.get("notes", "")))

    def enrich(row: dict, q: str) -> None:
        """Attach QID-keyed resolution columns to an attestation row."""
        k = kima_by_qid.get(q, {})
        lat, lon = coords_by_qid.get(q, ("", ""))
        row.update(qid=q, label_en=label_en.get(q, ""), label_yi=label_yi.get(q, ""),
                   place_type=ptype.get(q, ""), category=category.get(q, ""),
                   kima_id=k.get("kima_id", ""), kima_rom=k.get("kima_rom", ""),
                   kima_heb=k.get("kima_heb", ""), lat=lat, lon=lon)

    att: list[dict] = []
    seq: dict[tuple, int] = defaultdict(int)

    def new_id(prefix: str, rec: str, field: str) -> str:
        seq[(rec, field)] += 1
        return f"{prefix}-{rec}#{field}#{seq[(rec, field)]}"

    # ---------------------------------------------------------------
    # PERSON attestations — linked (from unified)
    # ---------------------------------------------------------------
    linked_person_keys = set()  # (entry_id, source_value) already resolved
    for r in unified:
        q = r["qid"].strip()
        eid, role, val = r["entry_id"], r["source_role"], r["source_value"].strip()
        linked_person_keys.add((eid, val))
        mq, _ = maaty_by_key.get((eid, role, val), ("", ""))
        nr = str(r.get("needs_review", "")).strip().lower() == "true"
        row = {c: "" for c in ATT_COLS}
        row.update(attestation_id=new_id("P", eid, role), source_corpus="person",
                   source_record_id=eid, source_field=role, context=r.get("context", ""),
                   source_value=val, source_value_script=script_of(val),
                   maaty_qid=mq, maaty_qid_conflict="True" if (mq and is_qid(q) and mq != q) else "",
                   is_descriptor="True" if is_descriptor(val) else "",
                   review_flags=r.get("review_flags", ""))
        if is_qid(q) and q in nonplace_qids:
            if mq in relink_ok and mq not in nonplace_qids:
                # rescue via Maaty's validated place QID; keep the bad one for audit
                enrich(row, mq)
                row.update(link_status="linked", rejected_qid=q, relink_source="maaty")
            else:
                # mis-resolved, no valid alternate: keep the Yiddish toponym for re-linking
                row.update(link_status="misresolved", rejected_qid=q)
        elif is_qid(q):
            enrich(row, q)
            row["link_status"] = "needs_review" if nr else "linked"
        else:
            row["link_status"] = "unlinked"
        att.append(row)

    # ---------------------------------------------------------------
    # PERSON attestations — UNLINKED (from raw, with entry_id back-pointer)
    # ---------------------------------------------------------------
    for r in raw:
        eid = r.get("Column 1", "").strip()
        ctx = r.get("context", "").strip()
        if not eid:
            continue
        for field in ("place", "province", "country"):
            val = r.get(field, "").strip()
            if not val or (eid, val) in linked_person_keys:
                continue
            sq, se, _ = sugg_by_variant.get(val, ("", "", ""))
            row = {c: "" for c in ATT_COLS}
            row.update(attestation_id=new_id("P", eid, field), source_corpus="person",
                       source_record_id=eid, source_field=field, context=ctx,
                       source_value=val, source_value_script=script_of(val),
                       link_status="unlinked", suggested_qid=sq, suggested_english=se,
                       is_descriptor="True" if is_descriptor(val) else "")
            att.append(row)

    # ---------------------------------------------------------------
    # ORG attestations — all extracted_* place fields (with cluster_id back-pointer)
    # ---------------------------------------------------------------
    for r in org_review:
        cid = r.get("cluster_id", "").strip()
        dbid = r.get("aligned_db_id", "").strip()
        if not cid:
            continue
        for field in _ORG_FIELDS:
            fname = field.replace("extracted_", "")  # settlements/venues/addresses/countries
            for val in split_values(r.get(field, "")):
                resolved = org_res.get((cid, val))
                row = {c: "" for c in ATT_COLS}
                row.update(attestation_id=new_id("O", cid, fname), source_corpus="org",
                           source_record_id=cid, org_db_id=dbid, source_field=fname,
                           source_value=val, source_value_script=script_of(val),
                           is_descriptor="True" if is_descriptor(val) else "")
                if resolved and resolved[0] in nonplace_qids:
                    row.update(link_status="misresolved", rejected_qid=resolved[0])
                elif resolved:
                    enrich(row, resolved[0])
                    row["link_status"] = "linked"
                else:
                    sq, se, _ = sugg_by_variant.get(val, ("", "", ""))
                    row.update(link_status="unlinked", suggested_qid=sq, suggested_english=se)
                att.append(row)

    write_csv(ATT_OUT, ATT_COLS, att)

    # ---------------------------------------------------------------
    # DERIVED: gazetteer (per QID) — group linked attestations
    # ---------------------------------------------------------------
    g: dict[str, dict] = {}
    for a in att:
        q = a["qid"].strip()
        if not is_qid(q):
            continue
        e = g.setdefault(q, {"variants": set(), "fields": set(), "corpora": set(),
                             "n": 0, "n_person": 0, "n_org": 0, "n_flagged": 0})
        e["n"] += 1
        e["n_person"] += a["source_corpus"] == "person"
        e["n_org"] += a["source_corpus"] == "org"
        e["n_flagged"] += a["link_status"] == "needs_review"
        if a["source_value"].strip():
            e["variants"].add(a["source_value"].strip())
        if a["source_field"].strip():
            e["fields"].add(a["source_field"].strip())
        e["corpora"].add(a["source_corpus"])

    gaz_cols = ["qid", "label_en", "label_yi", "place_type", "category",
                "kima_id", "kima_rom", "kima_heb", "lat", "lon",
                "n_attestations", "n_person", "n_org", "n_flagged_mentions",
                "fields", "n_variants", "variants", "corpora", "external_sources"]
    gaz_rows = []
    for q in sorted(g):
        k = kima_by_qid.get(q, {})
        lat, lon = coords_by_qid.get(q, ("", ""))
        variants = sorted(g[q]["variants"])
        ext = ["wikidata"] + (["kima"] if k.get("kima_id") else [])
        gaz_rows.append({
            "qid": q, "label_en": label_en.get(q, ""), "label_yi": label_yi.get(q, ""),
            "place_type": ptype.get(q, ""), "category": category.get(q, ""),
            "kima_id": k.get("kima_id", ""), "kima_rom": k.get("kima_rom", ""),
            "kima_heb": k.get("kima_heb", ""), "lat": lat, "lon": lon,
            "n_attestations": g[q]["n"], "n_person": g[q]["n_person"], "n_org": g[q]["n_org"],
            "n_flagged_mentions": g[q]["n_flagged"], "fields": ";".join(sorted(g[q]["fields"])),
            "n_variants": len(variants), "variants": ";".join(variants),
            "corpora": ";".join(sorted(g[q]["corpora"])), "external_sources": ";".join(ext),
        })
    write_csv(GAZ_OUT, gaz_cols, gaz_rows)

    # ---------------------------------------------------------------
    # DERIVED: unlinked worklist (per spelling) — keeps attestation_ids back-map
    # ---------------------------------------------------------------
    u: dict[str, dict] = {}
    for a in att:
        if a["link_status"] != "unlinked":
            continue
        v = a["source_value"].strip()
        if not v:
            continue
        e = u.setdefault(v, {"occ": 0, "corpora": set(), "fields": set(), "contexts": set(),
                             "att_ids": [], "sugg_qid": "", "sugg_en": "",
                             "desc": a["is_descriptor"] == "True"})
        e["occ"] += 1
        e["corpora"].add(a["source_corpus"])
        if a["source_field"].strip():
            e["fields"].add(a["source_field"])
        if a["context"].strip():
            e["contexts"].add(a["context"])
        e["att_ids"].append(a["attestation_id"])
        if a["suggested_qid"].strip():
            e["sugg_qid"] = a["suggested_qid"].strip()
            e["sugg_en"] = a["suggested_english"].strip()
        e["desc"] = e["desc"] or a["is_descriptor"] == "True"

    unlinked_cols = ["variant", "script", "corpora", "occurrences", "fields", "contexts",
                     "suggested_qid", "suggested_english", "is_descriptor", "attestation_ids"]
    unlinked_rows = []
    for v, e in sorted(u.items(), key=lambda kv: (-kv[1]["occ"], kv[0])):
        unlinked_rows.append({
            "variant": v, "script": script_of(v), "corpora": ";".join(sorted(e["corpora"])),
            "occurrences": e["occ"], "fields": ";".join(sorted(e["fields"])),
            "contexts": ";".join(sorted(e["contexts"])), "suggested_qid": e["sugg_qid"],
            "suggested_english": e["sugg_en"], "is_descriptor": "True" if e["desc"] else "",
            "attestation_ids": ";".join(e["att_ids"]),
        })
    write_csv(UNLINKED_OUT, unlinked_cols, unlinked_rows)

    # ---------------------------------------------------------------
    # DERIVED: misresolved audit (per rejected QID) — relink worklist
    # ---------------------------------------------------------------
    m: dict[str, dict] = {}
    for a in att:
        if a["link_status"] != "misresolved":
            continue
        rq = a["rejected_qid"].strip()
        e = m.setdefault(rq, {"variants": set(), "att_ids": [], "corpora": set()})
        if a["source_value"].strip():
            e["variants"].add(a["source_value"].strip())
        e["att_ids"].append(a["attestation_id"])
        e["corpora"].add(a["source_corpus"])
    mis_cols = ["rejected_qid", "wrong_kind", "wrong_label_en", "wrong_type",
                "n_attestations", "corpora", "n_variants", "variants", "attestation_ids"]
    mis_rows = []
    for rq in sorted(m, key=lambda q: -len(m[q]["att_ids"])):
        variants = sorted(m[rq]["variants"])
        mis_rows.append({
            "rejected_qid": rq, "wrong_kind": nonplace_qids.get(rq, ""),
            "wrong_label_en": label_en.get(rq, ""), "wrong_type": ptype.get(rq, ""),
            "n_attestations": len(m[rq]["att_ids"]), "corpora": ";".join(sorted(m[rq]["corpora"])),
            "n_variants": len(variants), "variants": ";".join(variants),
            "attestation_ids": ";".join(m[rq]["att_ids"]),
        })
    write_csv(MISRESOLVED_OUT, mis_cols, mis_rows)

    # ---------------------------------------------------------------
    from collections import Counter
    print(f"attestations: {len(att)} rows -> {ATT_OUT.name}")
    print(f"  corpus: {dict(Counter(a['source_corpus'] for a in att))}")
    print(f"  status: {dict(Counter(a['link_status'] for a in att))}")
    print(f"gazetteer:    {len(gaz_rows)} places -> {GAZ_OUT.name}")
    print(f"  with kima_id: {sum(1 for r in gaz_rows if r['kima_id'])}"
          f" | with coords: {sum(1 for r in gaz_rows if r['lat'])}"
          f" | with org attestations: {sum(1 for r in gaz_rows if r['n_org'])}")
    print(f"unlinked:     {len(unlinked_rows)} spellings -> {UNLINKED_OUT.name}")
    print(f"  corpora: {dict(Counter(r['corpora'] for r in unlinked_rows))}")
    print(f"  descriptors: {sum(1 for r in unlinked_rows if r['is_descriptor'])}"
          f" | with suggested_qid: {sum(1 for r in unlinked_rows if r['suggested_qid'])}")
    print(f"misresolved:  {len(mis_rows)} rejected QIDs -> {MISRESOLVED_OUT.name}")
    print(f"  by kind: {dict(Counter(r['wrong_kind'] for r in mis_rows))}")


if __name__ == "__main__":
    main()
