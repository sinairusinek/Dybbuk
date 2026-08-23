"""KG bio layer — lexicon subjects + birth/death/burial places.

Step 0 (entry index) + Step 1 (biography layer) of the Colab-extraction ->
KG integration.  Called from build_kg.py after the plays layer so that every
layer shares one Graph (node ids, mint sequences, adjudication).

Sources
  people/people_extracted.tsv        one row per lexicon subject-entry
                                     (flatten_volumes.py over the Colab "III"
                                     JSONs): heading, entry_type, dates, places
  people/person_hub.tsv              entry_person_id -> people_db db_id
                                     (RA/human alignment, consolidated)
  zibn-shtern/.../toponyms_attestations.csv
                                     per-entry birth/death/burial place
                                     attestations with link_status + QID
                                     (build_unified_toponyms.py)

Outputs (via build_kg.py --execute)
  kg/entry_index.tsv                 the join table every later layer uses:
                                     entry_key, person_id, db_id, node_id ...
  person / place nodes + born_in / died_in / buried_in edges in kg/nodes.tsv
  and kg/edges.tsv, stamped source_layer=bio.

Node-id policy (same as the plays layer): a subject aligned to people_db is
person:<db_id>; an unaligned subject is person_entry:<person_id>.  Places are
place:<QID> when the attestation is linked/needs_review, else minted
place:UPL-nnnn keyed on the normalized surface (shared with the plays layer so
the same unresolved toponym collapses to one node).
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict

import plays_common as pc

PERSON_HUB_TSV = pc.PEOPLE_DIR / "person_hub.tsv"
PEOPLE_EXTRACTED_TSV = pc.PEOPLE_DIR / "people_extracted.tsv"
ATTESTATIONS_CSV = pc.ZIBN_WORKING / "toponyms_attestations.csv"
ENTRY_INDEX_TSV = pc.KG_DIR / "entry_index.tsv"

ENTRY_INDEX_FIELDS = ["entry_key", "person_id", "volume", "xml_id", "heading",
                      "entry_type", "hub_id", "db_id", "node_id", "align_evidence"]

CONTEXT_EDGE = {"birth": "born_in", "death": "died_in", "burial": "buried_in"}
LINK_CONF = {"linked": "high", "needs_review": "medium"}
EXTRACTION_MODEL = "colab_extraction_III"


# ---------------------------------------------------------------- step 0
def build_entry_index() -> dict[str, dict]:
    """person_id -> entry-index row. Alignment authority = person_hub.tsv."""
    hub_of: dict[str, tuple[str, str, str]] = {}
    for h in pc.read_tsv(PERSON_HUB_TSV):
        db_ids = [d for d in (h.get("db_ids") or "").split("|") if d]
        if len(db_ids) > 1:  # multi_db hubs are unresolved — don't pick one
            db_ids = []
        for pid in (h.get("entry_person_ids") or "").split("|"):
            if pid:
                hub_of[pid] = (h["hub_id"], db_ids[0] if db_ids else "",
                               h.get("evidence") or "")
    index: dict[str, dict] = {}
    for r in pc.read_tsv(PEOPLE_EXTRACTED_TSV):
        pid = r["person_id"]
        hub_id, db_id, evidence = hub_of.get(pid, ("", "", ""))
        index[pid] = {
            "entry_key": f"{r['volume']}-{r['xml_id']}",
            "person_id": pid, "volume": r["volume"], "xml_id": r["xml_id"],
            "heading": r["heading"], "entry_type": r.get("entry_type", ""),
            "hub_id": hub_id, "db_id": db_id,
            "node_id": f"person:{db_id}" if db_id else f"person_entry:{pid}",
            "align_evidence": evidence,
            # carried for the bio layer, not written to entry_index.tsv
            "_row": r,
        }
    return index


def entry_to_dbid_map(index: dict[str, dict]) -> dict[str, str]:
    return {pid: e["db_id"] for pid, e in index.items() if e["db_id"]}


# ---------------------------------------------------------------- helpers
_DATE_RE = re.compile(r"^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$")


def norm_date(raw: str) -> tuple[str, str, str]:
    """(iso_start, iso_end, precision) for the extraction's date strings.
    Handles YYYY, YYYY-MM, YYYY-MM-DD, 'YYYY~', 'YYYY/YYYY', '189X'."""
    s = (raw or "").strip()
    if not s:
        return "", "", ""
    m = _DATE_RE.match(s)
    if m:
        y, mo, d = m.groups()
        if d:
            return s, "", "day"
        if mo:
            return s, "", "month"
        return s, "", "year"
    if re.fullmatch(r"\d{4}~", s):
        return s[:4], "", "circa"
    m = re.fullmatch(r"(\d{4})/(\d{4})", s)
    if m:
        return m.group(1), m.group(2), "range"
    m = re.fullmatch(r"(\d{3})[xX.]", s) or re.fullmatch(r"(\d{3})", s)
    if m:
        return m.group(1) + "0", m.group(1) + "9", "decade"
    m = re.fullmatch(r"(\d{2})[xX.]{2}", s)
    if m:
        return m.group(1) + "00", m.group(1) + "99", "century"
    return "", "", "unparsed"


def _load_attestations() -> dict[tuple[str, str], dict]:
    """(entry_key, context) -> best place attestation (field=place, then
    settlement).  Only person-corpus rows."""
    best: dict[tuple[str, str], dict] = {}
    rank = {"place": 0, "settlement": 1}
    with open(ATTESTATIONS_CSV, encoding="utf-8-sig") as f:
        for a in csv.DictReader(f):
            if a["source_corpus"] != "person" or a["source_field"] not in rank:
                continue
            key = (a["source_record_id"], a["context"])
            cur = best.get(key)
            if cur is None or rank[a["source_field"]] < rank[cur["source_field"]]:
                best[key] = a
    return best


# ---------------------------------------------------------------- step 4
# family_background / education as node attributes (not nodes).  The family
# fields carry the extraction's controlled values (financial: modest|rich|poor,
# religion: religious|maskilic_secular|yichus|christian, theater_connection:
# connected|amateur_lover|opposed, structure: orphan); education is free text
# that gets a coarse class list on top.  Only vol 7 was RA-reviewed.
EXTRACTION_DIR = pc.HERE.parent / "Zylbercweig_extraction"

EDU_CLASSES = [  # (class, regex over pc.norm_yiddish text — un-finalized letters)
    ("kheyder", r"\bחדר|\bחדרימ|\bמלמד"),
    ("yeshiva", r"ישיבה|בית המדרש|\bתלמוד תורה"),
    ("gymnasium", r"גימנאז"),
    ("folkshul", r"פאלקס ?שול|פאלקשול"),
    ("public_school", r"פאבליק ?סקול|פובליק ?סקול|\bסקול\b"),
    ("commercial_school", r"קאמערצ|האנדלס"),
    ("conservatory", r"קאנסערוואטאר"),
    ("university", r"אוניווערסיטעט|אוניווערזיטעט|\bקאלעדזש|פאקולטעט|מעדיצינ"),
    ("drama_school", r"דראמ[^ ]* ?(?:שול|סטודיא|קורס)|טעאטער ?(?:שול|סטודיא)|סטודיא"),
    ("music_school", r"מוזיק ?שול|געזאנג"),
    ("extern", r"עקסטערנ"),
    ("self_taught", r"זעלבסט ?(?:בילדונג|געלערנט)|אויטאדידאקט"),
    ("private_tutors", r"פריוואט|מיט לערער"),
]
_EDU = [(c, re.compile(rx)) for c, rx in EDU_CLASSES]


def education_classes(text: str) -> list[str]:
    s = pc.norm_yiddish(text)
    return [c for c, rx in _EDU if rx.search(s)]


def _load_family_background() -> dict[str, dict]:
    """entry_key -> family attrs from the IIIorg JSONs."""
    import glob, json as _json, os
    out: dict[str, dict] = {}
    for path in sorted(glob.glob(str(EXTRACTION_DIR / "*IIIorg.json"))):
        m = re.search(r"(\d+)", os.path.basename(path))
        vol = m.group(1) if m else ""
        with open(path, encoding="utf-8") as f:
            for e in _json.load(f):
                fb = e.get("family_background") or []
                if not fb:
                    continue
                d: dict = {}
                for b in fb:
                    for k, ak in (("financial", "family_financial"),
                                  ("religion", "family_religion"),
                                  ("theater_connection", "family_theater"),
                                  ("structure", "family_structure")):
                        if b.get(k) and ak not in d:
                            d[ak] = b[k]
                    if b.get("description"):
                        d.setdefault("family_description", []).append(b["description"])
                if "family_description" in d:
                    d["family_description"] = " | ".join(d["family_description"])
                out[f"{vol}-{(e.get('xml:id') or '').strip()}"] = d
    return out


# ---------------------------------------------------------------- step 1
def add_bio_layer(g, labels, index: dict[str, dict]) -> dict:
    """Add every lexicon subject as a person node and its place edges."""
    from build_kg import _place_label_yi, _place_label_en  # avoid import cycle

    people, _orgs, _clusters, places = labels
    att = _load_attestations()
    family = _load_family_background()
    stats: Counter = Counter()

    for pid, e in index.items():
        r = e["_row"]
        attrs = {k: v for k, v in (
            ("entry_type", r.get("entry_type", "")),
            ("volume", r["volume"]), ("span", r.get("span", "")),
            ("birth_date", r.get("birth_date", "")),
            ("death_date", r.get("death_date", "")),
            ("credit", r.get("credit", "")),
            ("education", r.get("education", "")),
        ) if v}
        if r.get("education"):
            cls = education_classes(r["education"])
            if cls:
                attrs["education_classes"] = cls
            stats["education"] += 1
        fam = family.get(e["entry_key"])
        if fam:
            attrs.update(fam)
            stats["family_background"] += 1
        node_id = e["node_id"]
        if e["db_id"]:
            p = people.get(e["db_id"], {})
            g.add_node(node_id, node_type="person",
                       label_yiddish=p.get("hebname") or r["heading"],
                       label_english=p.get("english", ""),
                       ext_ref_type="people_db", ext_ref_id=e["db_id"],
                       secondary_ids=f"entry_person_id:{pid}",
                       match_status="matched", source_layer="bio",
                       attrs=json.dumps(attrs, ensure_ascii=False))
        else:
            g.add_node(node_id, node_type="person", label_yiddish=r["heading"],
                       ext_ref_type="entry_person_id", ext_ref_id=pid,
                       match_status="unmatched", source_layer="bio",
                       attrs=json.dumps(attrs, ensure_ascii=False))
        stats["subjects"] += 1
        stats["subjects_aligned" if e["db_id"] else "subjects_unaligned"] += 1

        for ctx, etype in CONTEXT_EDGE.items():
            surface = r.get(f"{ctx}_place_name", "")
            if not surface:
                continue
            prov = r.get(f"{ctx}_place_province", "")
            ctry = r.get(f"{ctx}_place_country", "")
            evidence = " / ".join(x for x in (surface, prov, ctry) if x)
            a = att.get((e["entry_key"], ctx))
            status = (a or {}).get("link_status", "")
            qid = (a or {}).get("qid", "")
            if a and qid and status in LINK_CONF:
                pl = places.get(qid, {})
                sec = json.dumps({k: pl.get(k, "") for k in ("kima_id", "lat", "lon")},
                                 ensure_ascii=False) if pl else ""
                target = f"place:{qid}"
                g.add_node(target, node_type="place",
                           label_yiddish=_place_label_yi(pl, surface),
                           label_english=_place_label_en(pl) or a.get("label_en", ""),
                           ext_ref_type="wikidata_qid", ext_ref_id=qid,
                           secondary_ids=sec,
                           match_status="matched" if status == "linked" else "candidate",
                           source_layer="bio")
                match = "matched" if status == "linked" else "candidate"
                conf = LINK_CONF[status]
            else:
                # unlinked / misresolved / no attestation -> minted node,
                # shared mint key with the plays layer
                notes = f"attestation:{status}" if status else "no_attestation"
                if status == "misresolved" and qid:
                    notes += f" rejected:{qid}"
                target = g.resolve_surface(
                    "place", surface, "bio", (a or {}).get("attestation_id", "") or pid,
                    evidence, node_type="place", label_yiddish=surface,
                    match_status="unmatched", notes=notes, source_layer="bio")
                if not target:
                    stats[f"{etype}:not_entity"] += 1
                    continue
                if target.startswith("place:UPL"):
                    match, conf = "unmatched", "low"
                else:
                    match, conf = "matched", "high"  # adjudicated ALIGN
            date_key = {"birth": "birth_date", "death": "death_date",
                        "burial": "death_date"}[ctx]
            d0, d1, prec = norm_date(r.get(date_key, ""))
            g.add_edge(source_id=node_id, target_id=target, edge_type=etype,
                       role_detail="", character="",
                       date_start=d0, date_end=d1, date_precision=prec,
                       event_id="", production_key="",
                       provenance_person_id=pid,
                       provenance_fact_ids=(a or {}).get("attestation_id", ""),
                       evidence_sentence=evidence,
                       extraction_model=EXTRACTION_MODEL,
                       confidence=conf, match_status=match,
                       review_status="auto", source_layer="bio")
            stats[f"{etype}:{match}"] += 1
    return dict(stats)


def write_entry_index(index: dict[str, dict]) -> None:
    pc.write_tsv(ENTRY_INDEX_TSV, list(index.values()), ENTRY_INDEX_FIELDS)
