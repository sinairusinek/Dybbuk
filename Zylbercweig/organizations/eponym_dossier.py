#!/usr/bin/env python3
"""Assemble the evidence for one eponym question, by id-joins only.

The point of this module: a decision about whether an organisation is named
after a person is made by READING, and the reading material already exists in
the corpus, linked by ids. No embeddings, no similarity search -- the joins
are exact and every quoted line keeps its provenance (who said it, where,
when), which is what makes a downstream verdict checkable.

  core_db.linked_cluster_ids  ->  organizations_clustered.tsv   (sentences)
  person_hub.entry_person_ids ->  people/entry_texts.tsv        (biographies)
  graph/person_org_edges.tsv                                    (who/role/date)

Pure: no model calls, no writes. `eponym_llm_judge.py` consumes it, and the
org-alignment work can import the same two builders.

Why the sentences matter, concretely -- db641 `לערמאַןס טרופּע` is attested as
`לערמאַנ'ס`, `לערמאַנקעס` and `לערמאַנקע` across three entries. No string
matcher reaches those from the catalogue spelling; a reader does.

  python3 eponym_dossier.py --show 641
  python3 eponym_dossier.py --show 1062 --span שאר
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import pathlib
import re
import sys
import unicodedata as ud

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
CORE_DB = BASE / "organizations" / "core_db.tsv"
CLUSTERED = BASE / "organizations" / "organizations_clustered.tsv"
EDGES = BASE / "organizations" / "graph" / "person_org_edges.tsv"
PERSON_HUB = BASE / "people" / "person_hub.tsv"
ENTRY_TEXTS = BASE / "people" / "entry_texts.tsv"

# organizations_clustered.tsv column names are flattened JSON paths.
C_SENT = "_ - organizations - _ - relations - _ - original_sentence"
C_HEAD = "_ - heading"
C_SUB = "_ - subheading"
C_ROLE = "_ - organizations - _ - relations - _ - role_title"
C_D1 = "_ - organizations - _ - relations - _ - date_start - date"
C_D2 = "_ - organizations - _ - relations - _ - date_end - date"
C_SETT = "_ - organizations - _ - locations - _ - settlement"
C_XMLID = "_ - xml:id"

HEB = re.compile(r"[֐-׿יִ-ﭏ]+")
FINAL_MAP = str.maketrans("ןםךףץ", "נמכפצ")

MAX_SENTENCES = 12
ENTRY_HEAD_CHARS = 1200
ENTRY_HIT_SENTENCES = 6

# host_file in the edge file -> volume number, for person_id reconstruction.
VOL_OF_FILE = {
    "volume_1IIIorg.json": "1", "volume_2IIIorg.json": "2",
    "Volume_3IIIorg.json": "3", "Volume_4IIIorg.json": "4",
    "Volume5IIIorg.json": "5", "volume6IIIorg.json": "6",
    "volume7IIIorg.json": "7",
}


def sm(s: str) -> str:
    """NFKD FIRST, then strip Hebrew points.

    The corpus stores precomposed ligatures (U+FB4E פֿ) where a typed string has
    pe + rafe. Stripping points without decomposing matches nothing and reads
    exactly like a clean absence -- this has cost real time more than once.
    """
    s = ud.normalize("NFKD", s or "")
    return "".join(c for c in s if not ("֑" <= c <= "ׇ"))


def key(s: str) -> str:
    """Comparison skeleton: points stripped and final letters folded.

    Folding is needed on BOTH sides: a possessive after a final form writes
    גאָלדפאַדענס, not גאָלדפאַדעןס.
    """
    return sm(s).translate(FINAL_MAP).strip()


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


# ── lazy singletons (each file is read at most once per process) ─────────────
_cache: dict[str, object] = {}


def _orgs() -> dict[str, dict]:
    if "orgs" not in _cache:
        _cache["orgs"] = {r["db_id"]: r for r in read_tsv(CORE_DB)
                          if (r.get("db_id") or "").strip()}
    return _cache["orgs"]  # type: ignore[return-value]


def _hubs() -> dict[str, dict]:
    if "hubs" not in _cache:
        _cache["hubs"] = {h["hub_id"]: h for h in read_tsv(PERSON_HUB)}
    return _cache["hubs"]  # type: ignore[return-value]


def _entry_texts() -> dict[str, dict]:
    if "texts" not in _cache:
        _cache["texts"] = {r["person_id"]: r for r in read_tsv(ENTRY_TEXTS)}
    return _cache["texts"]  # type: ignore[return-value]


def _clusters_by_id() -> dict[str, list[dict]]:
    """cluster_id -> mention rows. One streaming pass over a 34 MB file."""
    if "clusters" not in _cache:
        idx: dict[str, list[dict]] = collections.defaultdict(list)
        with CLUSTERED.open(encoding="utf-8") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                cid = (r.get("cluster_id") or "").strip()
                if cid:
                    idx[cid].append(r)
        _cache["clusters"] = idx
    return _cache["clusters"]  # type: ignore[return-value]


def _mention_edges_by_org() -> dict[str, list[dict]]:
    if "edges" not in _cache:
        idx: dict[str, list[dict]] = collections.defaultdict(list)
        for e in read_tsv(EDGES):
            if e.get("edge_source") == "mention":
                idx[e["db_id"]].append(e)
        _cache["edges"] = idx
    return _cache["edges"]  # type: ignore[return-value]


def entry_ids(hub: dict) -> list[str]:
    """`entry_person_ids` is PIPE-separated for the 168 people with entries in
    more than one volume ('P-1-…|P-5-…'). Splitting on comma alone leaves them
    joined and every text lookup misses -- it reads as "no entry"."""
    raw = hub.get("entry_person_ids") or ""
    return [p.strip() for p in re.split(r"[|,]", raw) if p.strip()]


def org_names(db_id: str) -> dict:
    """Catalogue names. db641's `name_yiddish` is EMPTY and its name lives in
    `name`; any reader of this file must take both."""
    r = _orgs().get(db_id, {})
    yid = (r.get("name_yiddish") or "").strip()
    latin = (r.get("name") or "").strip()
    return {
        "db_id": db_id,
        "name_yiddish": yid,
        "name": latin,
        "display": yid if HEB.search(yid) else latin,
        "org_type": (r.get("org_type") or "").strip(),
        "name_variants": (r.get("name_variants") or "").strip(),
        "address": (r.get("address") or "").strip(),
    }


def build_org_dossier(db_id: str, span: str = "") -> dict:
    """Catalogue row + the mention sentences that attest the organisation."""
    names = org_names(db_id)
    row = _orgs().get(db_id, {})
    cluster_ids = [c.strip() for c in (row.get("linked_cluster_ids") or "").split("|")
                   if c.strip()]

    idx = _clusters_by_id()
    seen: set[str] = set()
    mentions: list[dict] = []
    for cid in cluster_ids:
        for r in idx.get(cid, []):
            sent = (r.get(C_SENT) or "").strip()
            if not sent or sent in seen:
                continue
            seen.add(sent)
            mentions.append({
                "sentence": re.sub(r"\s+", " ", sent),
                "host_heading": (r.get(C_HEAD) or "").strip(),
                "host_subheading": (r.get(C_SUB) or "").strip(),
                "host_xml_id": (r.get(C_XMLID) or "").strip(),
                "role": (r.get(C_ROLE) or "").strip(),
                "date_start": (r.get(C_D1) or "").strip(),
                "date_end": (r.get(C_D2) or "").strip(),
                "settlement": (r.get(C_SETT) or "").strip(),
                "cluster_id": cid,
            })

    # Sentences naming the span come first -- those are the ones that identify
    # a person, and the cap must not drop them.
    k = key(span)
    if k:
        mentions.sort(key=lambda m: 0 if k in key(m["sentence"]) else 1)

    edges = _mention_edges_by_org().get(db_id, [])
    attestors = []
    for e in edges:
        h = (e.get("host_heading") or "").strip()
        if not h:
            continue
        attestors.append({
            "person": h,
            "relation": (e.get("rel_categories") or "").strip(),
            "role": (e.get("roles") or "").strip(),
            "dates": (e.get("dates") or "").strip(),
            "settlements": (e.get("settlements") or "").strip(),
            "person_id": f"P-{VOL_OF_FILE.get(e.get('host_file',''), '?')}-{e.get('host_xmlid','')}",
        })

    return {
        **names,
        "span": span,
        "n_mentions": len(mentions),
        "mentions": mentions[:MAX_SENTENCES],
        "n_attestors": len(attestors),
        "attestors": attestors[:MAX_SENTENCES],
    }


def _split_sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n+", text or "") if s.strip()]


def build_person_dossier(hub_id: str, org: dict | None = None) -> dict:
    """Hub identity + an entry excerpt steered at the organisation.

    The excerpt is the head of the biography PLUS any sentence naming the org
    or the span -- a flat head-truncation usually cuts before the troupe is
    ever mentioned, which is precisely the evidence that decides the case.
    """
    h = _hubs().get(hub_id)
    if not h:
        return {"hub_id": hub_id, "found": False}

    texts = _entry_texts()
    parts = [texts[p] for p in entry_ids(h) if p in texts]

    needles = []
    if org:
        for cand in (org.get("name_yiddish"), org.get("span"), org.get("display")):
            if cand and HEB.search(cand or ""):
                needles.append(key(cand))
    needles = [n for n in needles if n]

    excerpts, hit_sentences = [], []
    for g in parts:
        body = re.sub(r"\s+", " ", g.get("entry_text") or "").strip()
        vol = g.get("volume", "?")
        excerpts.append(f"[vol {vol}] {body[:ENTRY_HEAD_CHARS]}"
                        + (" …" if len(body) > ENTRY_HEAD_CHARS else ""))
        if needles:
            for s in _split_sentences(body):
                ks = key(s)
                if any(n in ks for n in needles):
                    hit_sentences.append(f"[vol {vol}] {s}")

    # The weak signal, kept as a feature and labelled as weak: measured on the
    # 148 gold pairs it fires on 6/127 CONFIRMs and 1/21 REJECTs -- 86%
    # precision but 5% recall. Useful as evidence, useless as a ranker.
    self_mentions = False
    if org:
        hosts = {re.sub(r"_\d+$", "", a["person_id"])
                 for a in (org.get("attestors") or [])}
        mine = {re.sub(r"_\d+$", "", p) for p in entry_ids(h)}
        self_mentions = bool(hosts & mine)

    return {
        "hub_id": hub_id,
        "found": True,
        "heading": (h.get("canonical_heading") or "").strip(),
        "person_db_id": (h.get("db_ids") or "").split(",")[0].strip(),
        "born": (h.get("birth_date") or "").strip(),
        "died": (h.get("death_date") or "").strip(),
        "name_surfaces": (h.get("top_surfaces") or "").strip(),
        "n_entries": (h.get("n_entries") or "").strip(),
        "has_entry": bool(parts),
        "entry_excerpts": excerpts,
        "entry_sentences_naming_org": hit_sentences[:ENTRY_HIT_SENTENCES],
        "own_entry_attests_org": self_mentions,
    }


def build_case(db_id: str, span: str, candidate_hub_ids: list[str]) -> dict:
    """The whole question: one org, one name in it, the people it might be."""
    org = build_org_dossier(db_id, span)
    return {
        "db_id": db_id,
        "span": span,
        "org": org,
        "candidates": [build_person_dossier(h, org) for h in candidate_hub_ids],
    }


def render_case(case: dict) -> str:
    """Flatten a case to the text the model reads.

    Also the grounding corpus: a verdict's quote must be a substring of this.
    """
    org = case["org"]
    L: list[str] = []
    L.append("=== ORGANISATION ===")
    L.append(f"catalogue id: db{org['db_id']}")
    L.append(f"name (Yiddish): {org['name_yiddish'] or '(none recorded)'}")
    L.append(f"name (Latin): {org['name'] or '(none recorded)'}")
    if org["name_variants"]:
        L.append(f"name variants: {org['name_variants']}")
    L.append(f"type: {org['org_type'] or '(none)'}")
    L.append(f"NAME IN QUESTION: {case['span']}")
    L.append("")
    L.append(f"--- attested in {org['n_mentions']} sentence(s); showing "
             f"{len(org['mentions'])} ---")
    for m in org["mentions"]:
        meta = " · ".join(x for x in (m["host_heading"], m["role"],
                                      m["date_start"], m["settlement"]) if x)
        L.append(f"[{meta}] {m['sentence']}")
    if org["attestors"]:
        L.append("")
        L.append("--- people whose entries name this organisation ---")
        for a in org["attestors"]:
            meta = " · ".join(x for x in (a["relation"], a["role"], a["dates"],
                                          a["settlements"]) if x)
            L.append(f"{a['person']}{(' (' + meta + ')') if meta else ''}")

    L.append("")
    L.append("=== CANDIDATE PEOPLE ===")
    if not case["candidates"]:
        L.append("(no candidate was found for this name)")
    for c in case["candidates"]:
        if not c.get("found"):
            L.append(f"- {c['hub_id']}: NOT FOUND")
            continue
        L.append("")
        L.append(f"- candidate {c['hub_id']} | {c['heading']} "
                 f"[person_db_id {c['person_db_id']}]")
        life = " – ".join(x for x in (c["born"], c["died"]) if x)
        if life:
            L.append(f"  lived: {life}")
        if c["name_surfaces"]:
            L.append(f"  name as written elsewhere: {c['name_surfaces']}")
        L.append(f"  own entry names this organisation: "
                 f"{'yes' if c['own_entry_attests_org'] else 'no'}")
        for s in c["entry_sentences_naming_org"]:
            L.append(f"  ! {s}")
        for e in c["entry_excerpts"]:
            L.append(f"  biography: {e}")
        if not c["has_entry"]:
            L.append("  biography: (no entry text in the corpus)")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", metavar="DB_ID", required=True)
    ap.add_argument("--span", default="")
    ap.add_argument("--hubs", default="", help="comma-separated hub_ids")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    hub_ids = [h.strip() for h in args.hubs.split(",") if h.strip()]
    if not hub_ids:
        # Default to the candidates the generator proposed for this org/span.
        cands = read_tsv(BASE / "organizations" / "eponym_candidates.tsv")
        rows = [c for c in cands if c["db_id"] == args.show
                and (not args.span or c["span"] == args.span) and c.get("hub_id")]
        hub_ids = list(dict.fromkeys(r["hub_id"] for r in rows))
        if not args.span and rows:
            args.span = rows[0]["span"]

    case = build_case(args.show, args.span, hub_ids)
    if args.json:
        json.dump(case, sys.stdout, ensure_ascii=False, indent=2)
        print()
    else:
        print(render_case(case))


if __name__ == "__main__":
    main()
