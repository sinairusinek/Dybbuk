"""One row per place mention, with the text it was found in and its external links.

The place spine (zibn-shtern/data/working/toponyms_attestations.csv) carries the
Wikidata/Kima links but no text: its `context` column is the event type
(birth/death/burial), and org attestations are deduplicated per cluster+spelling.
This export puts the sentence back on every place mention, from three sources:

  vital_place            birth/death/burial place of an entry's subject — spine
                         row + a window of the entry text around the spelling
  org_relation_place     a place tied to an organisation mention — spine row
                         expanded back to every mention row in
                         organizations_clustered.tsv, with its original sentence
  person_relation_place  a place tied to a person-to-person relation in
                         mentions_all.tsv. NOT in the spine; linked here by exact
                         spelling lookup against the spine's linked rows, only
                         when the spelling maps to ONE QID (link_status
                         `linked_by_spelling`, unreviewed)

Output: place_mentions_with_context.tsv (next to this script).
Run: python3.11 Zylbercweig/datasets/build_place_mentions_with_context.py
"""
from __future__ import annotations

import csv
import pathlib
import re
import sys
import unicodedata
from collections import Counter, defaultdict

HERE = pathlib.Path(__file__).resolve().parent
ZYL = HERE.parent
sys.path.insert(0, str(ZYL / "people"))

from people_common import load_mentions_with_host, mention_volume, read_tsv, write_tsv  # noqa: E402

WORK = ZYL / "zibn-shtern" / "data" / "working"
ATTESTATIONS = WORK / "toponyms_attestations.csv"
GAZETTEER = WORK / "toponyms_gazetteer.csv"
ENTRY_TEXTS = ZYL / "people" / "entry_texts.tsv"
ORG_CLUSTERED = ZYL / "organizations" / "organizations_clustered.tsv"
OUT = HERE / "place_mentions_with_context.tsv"

WINDOW = 150  # chars either side of a vital place inside the entry text

FIELDS = [
    "place_mention_id", "source_kind", "entry_id", "entry_heading",
    "event_or_relation", "related_name", "related_id", "place_field",
    "place_as_written", "context", "context_kind", "place_in_context", "link_status", "link_method",
    "wikidata_qid", "name_en", "place_type", "kima_id", "kima_name_latin",
    "kima_name_hebrew", "lat", "lon", "review_flags", "attestation_id",
]

ORG_LOC = "_ - organizations - _ - locations - _ - "
ORG_FIELD_COLS = {  # spine source_field -> organizations_clustered column
    "settlements": "settlement", "venues": "Venue", "countries": "country",
    "addresses": "address", "province": "province",
}
PERSON_REL_COLS = {
    "rel_place_settlement": "settlement", "rel_place_country": "country",
    "rel_place_province": "province", "rel_place_venue": "venue",
    "rel_place_address": "address",
}
FIELD_NAMES = {"settlements": "settlement", "venues": "venue",
               "countries": "country", "addresses": "address"}


def norm(s: str) -> str:
    # NFC folds the precomposed presentation forms (U+FB2E…) into base+point
    return unicodedata.normalize("NFC", s or "").replace("\xa0", " ").strip()


def flat(s: str) -> str:
    return " ".join((s or "").replace("⏎", " ").split())


def read_csv(path: pathlib.Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def text_window(text: str, needle: str) -> str:
    i = text.find(needle)
    if i < 0:
        return ""
    a, b = max(0, i - WINDOW), min(len(text), i + len(needle) + WINDOW)
    while a > 0 and not text[a - 1].isspace():
        a -= 1
    while b < len(text) and not text[b].isspace():
        b += 1
    return ("…" if a > 0 else "") + text[a:b].strip() + ("…" if b < len(text) else "")


def link_cols(a: dict, gaz: dict[str, dict]) -> dict:
    g = gaz.get(a.get("qid", ""), {})
    pick = lambda k: a.get(k) or g.get(k, "")  # noqa: E731
    return {
        "link_status": a.get("link_status", ""), "link_method": a.get("link_method", ""),
        "wikidata_qid": a.get("qid", ""), "name_en": pick("label_en"),
        "place_type": pick("place_type"), "kima_id": pick("kima_id"),
        "kima_name_latin": pick("kima_rom"), "kima_name_hebrew": pick("kima_heb"),
        "lat": pick("lat"), "lon": pick("lon"), "review_flags": a.get("review_flags", ""),
        "attestation_id": a.get("attestation_id", ""),
    }


def main() -> None:
    att = read_csv(ATTESTATIONS)
    gaz = {g["qid"]: g for g in read_csv(GAZETTEER)}
    texts = {r["person_id"]: r for r in read_tsv(ENTRY_TEXTS)}
    out: list[dict] = []

    # 1. vital places of the entry subject
    for a in att:
        if a["source_corpus"] != "person":
            continue
        pid = "P-" + a["source_record_id"]
        e = texts.get(pid, {})
        ctx = text_window(norm(e.get("entry_text", "")), norm(a["source_value"]))
        out.append({
            "source_kind": "vital_place", "entry_id": pid,
            "entry_heading": flat(e.get("heading", "")), "event_or_relation": a["context"],
            "place_field": a["source_field"], "place_as_written": a["source_value"],
            "context": flat(ctx), "context_kind": "entry_text_window" if ctx else "",
            **link_cols(a, gaz),
        })

    # 2. org places, expanded from cluster+spelling back to each mention row
    org_att = {(a["source_record_id"], a["source_field"], norm(a["source_value"])): a
               for a in att if a["source_corpus"] == "org"}
    with open(ORG_CLUSTERED, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            xml, vol = r["_ - xml:id"].strip(), mention_volume(r["File"])
            pid = f"P-{vol}-{xml}" if xml and vol else ""
            for field, col in ORG_FIELD_COLS.items():
                v = r[ORG_LOC + col].strip()
                if not v:
                    continue
                a = org_att.get((r["cluster_id"], field, norm(v)), {"link_status": "unlinked"})
                out.append({
                    "source_kind": "org_relation_place", "entry_id": pid,
                    "entry_heading": flat(r["_ - heading"]),
                    "event_or_relation": r["_ - organizations - _ - relations - _ - category"],
                    "related_name": r["_ - organizations - _ - title"],
                    "related_id": r["cluster_id"] + (f" / db {a['org_db_id']}" if a.get("org_db_id") else ""),
                    "place_field": FIELD_NAMES.get(field, field), "place_as_written": v,
                    "context": flat(r["_ - organizations - _ - relations - _ - original_sentence"]),
                    "context_kind": "extracted_sentence", **link_cols(a, gaz),
                })

    # 3. person-relation places: not in the spine, link by unambiguous spelling
    by_spelling: dict[str, dict[str, dict]] = defaultdict(dict)
    for a in att:
        if a["link_status"] == "linked" and a["qid"] and a.get("is_descriptor") != "True":
            by_spelling[norm(a["source_value"])].setdefault(a["qid"], a)
    for r in load_mentions_with_host():
        for col, field in PERSON_REL_COLS.items():
            v = (r.get(col) or "").strip()
            if not v:
                continue
            cands = by_spelling.get(norm(v), {})
            if len(cands) == 1:
                (a,) = cands.values()
                links = link_cols(a, gaz) | {
                    "link_status": "linked_by_spelling", "link_method": "spine_spelling_lookup",
                    "review_flags": "", "attestation_id": a["attestation_id"]}
            else:
                links = {"link_status": "ambiguous" if cands else "unlinked",
                         "review_flags": "candidates=" + "|".join(sorted(cands)) if cands else ""}
            out.append({
                "source_kind": "person_relation_place", "entry_id": r["host_person_id"],
                "entry_heading": flat(r["host_heading_filled"]),
                "event_or_relation": r.get("relation_category", ""),
                "related_name": r.get("mention_name", ""), "related_id": r.get("mention_id", ""),
                "place_field": field, "place_as_written": v,
                "context": flat(r.get("relation_sentence", "")),
                "context_kind": "extracted_sentence" if r.get("relation_sentence", "").strip() else "",
                **links,
            })

    for i, r in enumerate(out, 1):
        r["place_mention_id"] = f"PM{i:06d}"
        # extraction often takes the place from the wider entry, not the sentence
        if r["context"]:
            r["place_in_context"] = "yes" if norm(r["place_as_written"]) in norm(r["context"]) else "no"
    write_tsv(OUT, out, FIELDS)

    print(f"wrote {len(out)} rows -> {OUT.relative_to(ZYL.parent)}")
    for kind in ("vital_place", "org_relation_place", "person_relation_place"):
        rows = [r for r in out if r["source_kind"] == kind]
        print(f"  {kind:22s} {len(rows):6d}  with context {sum(1 for r in rows if r['context']):6d}  "
              f"place in it {sum(1 for r in rows if r.get('place_in_context') == 'yes'):6d}  "
              f"status {dict(Counter(r['link_status'] for r in rows).most_common())}")


if __name__ == "__main__":
    main()
