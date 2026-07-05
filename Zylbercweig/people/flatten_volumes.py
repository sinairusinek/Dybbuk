"""Flatten Zylbercweig volume JSONs into a unified people_extracted.tsv.

Source files live in Zylbercweig/Zylbercweig_extraction/*.json.
Two schemas coexist:
  - Rich schema (vols 1, 2, 4, 5, 6, 7): heading, names[], entry_type, dates, places, education
  - Mentions schema (vol 3): heading, entry text, people[] mentions inside the article body

This script emits one row per subject-entry across both schemas, keeping a stable
person_id of the form "P-{vol}-{xml_id}". Vol-3 mentions are written separately
to people_mentions_extracted.tsv (one row per in-text person mention) so the
matcher work for jobs 1+2 (subject-level) and job 3 (mention-level) can proceed
independently.
"""
from __future__ import annotations
import csv, json
from pathlib import Path

EXT = Path(__file__).parent.parent / "Zylbercweig_extraction"
OUT_DIR = Path(__file__).parent
OUT_PEOPLE = OUT_DIR / "people_extracted.tsv"
OUT_MENTIONS = OUT_DIR / "people_mentions_extracted.tsv"

VOLUMES = {
    1: ["volume_1AIII.json", "volume_1BIII.json"],
    2: ["volume_2III.json", "volume_2SplitsIII.json"],
    3: ["Volume_3III.json"],
    4: ["Volume_4III.json"],
    5: ["Volume5III.json", "Volume5HartIII.json"],
    6: ["volume6III.json", "volume6LiliFeinmanIII.json", "volume6EisenbergIII.json"],
    7: ["volume7III.json"],
}


def _date(v):
    if isinstance(v, dict):
        return v.get("date") or ""
    if isinstance(v, str):
        return v
    return ""


def _place(v):
    if not isinstance(v, dict):
        return "", "", ""
    return v.get("name", "") or "", v.get("province", "") or "", v.get("country", "") or ""


def _join(xs):
    if not xs:
        return ""
    return " | ".join(str(x).replace("\t", " ").replace("\n", " ").strip() for x in xs if x)


PERSON_COLS = [
    "person_id", "volume", "chunk_number", "xml_id", "heading",
    "span", "entry_type", "credit", "subheading",
    "birth_date", "death_date",
    "birth_place_name", "birth_place_province", "birth_place_country",
    "death_place_name", "death_place_province", "death_place_country",
    "burial_place_name",
    "names_variants",  # | separated
    "names_count",
    "education",  # | separated
    "endnotes_count",
    "has_entry_text",  # 1 if entry body present (vol3-style)
    "people_mentions_count",  # in-entry mentions (vol3-style)
]

MENTION_COLS = [
    "mention_id", "host_person_id", "volume", "host_xml_id", "host_heading",
    "name", "gender", "person_description", "relation",
]


def main():
    person_rows = []
    mention_rows = []
    for vol, fns in VOLUMES.items():
        for fn in fns:
            p = EXT / fn
            if not p.exists():
                print(f"WARN missing: {p}")
                continue
            data = json.load(open(p))
            for e in data:
                xml_id = e.get("xml:id", "")
                pid = f"P-{vol}-{xml_id}"
                names = e.get("names") or []
                bd = _date(e.get("birth_date"))
                dd = _date(e.get("death_date"))
                bp_n, bp_p, bp_c = _place(e.get("birth_place"))
                dp_n, dp_p, dp_c = _place(e.get("death_place"))
                bu_n, _, _ = _place(e.get("burial_place"))
                edu = e.get("education") or []
                endnotes = e.get("endnotes") or []
                entry_text = e.get("entry") or ""
                people_mentions = e.get("people") or []

                person_rows.append({
                    "person_id": pid,
                    "volume": vol,
                    "chunk_number": e.get("chunk_number", ""),
                    "xml_id": xml_id,
                    "heading": (e.get("heading") or "").strip(),
                    "span": e.get("span", ""),
                    "entry_type": e.get("entry_type", ""),
                    "credit": (e.get("credit") or "").replace("\t", " ").replace("\n", " ").strip(),
                    "subheading": (e.get("subheading") or "").replace("\t", " ").replace("\n", " ").strip(),
                    "birth_date": bd,
                    "death_date": dd,
                    "birth_place_name": bp_n,
                    "birth_place_province": bp_p,
                    "birth_place_country": bp_c,
                    "death_place_name": dp_n,
                    "death_place_province": dp_p,
                    "death_place_country": dp_c,
                    "burial_place_name": bu_n,
                    "names_variants": _join(names),
                    "names_count": len(names),
                    "education": _join(edu),
                    "endnotes_count": len(endnotes),
                    "has_entry_text": 1 if entry_text else 0,
                    "people_mentions_count": len(people_mentions),
                })

                for i, m in enumerate(people_mentions):
                    if not isinstance(m, dict):
                        continue
                    mention_rows.append({
                        "mention_id": f"M-{vol}-{xml_id}-{i:03d}",
                        "host_person_id": pid,
                        "volume": vol,
                        "host_xml_id": xml_id,
                        "host_heading": (e.get("heading") or "").strip(),
                        "name": (m.get("name") or "").strip(),
                        "gender": m.get("gender", ""),
                        "person_description": (m.get("person_description") or "").replace("\t", " ").replace("\n", " ").strip(),
                        "relation": (m.get("relation") or "").replace("\t", " ").replace("\n", " ").strip(),
                    })

    with open(OUT_PEOPLE, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=PERSON_COLS, delimiter="\t")
        w.writeheader()
        w.writerows(person_rows)
    with open(OUT_MENTIONS, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=MENTION_COLS, delimiter="\t")
        w.writeheader()
        w.writerows(mention_rows)
    print(f"wrote {OUT_PEOPLE} ({len(person_rows)} rows)")
    print(f"wrote {OUT_MENTIONS} ({len(mention_rows)} rows)")


if __name__ == "__main__":
    main()
