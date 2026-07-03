"""Materialize per-entry context for the Zalmen review views (Phase C).

mentions_all.tsv is 46 MB because every mention row repeats its host entry's
full text — too heavy for the Streamlit app to load per session. This script
deduplicates it into two slim TSVs:

  entry_texts.tsv          person_id → full entry text (one row per entry)
  entry_mentions_slim.tsv  host_person_id → mention_name/gender/relation
                           (sentence context, no entry-text payload)

Used by views/person_alignment.py (B2: full entry + mentions on both sides)
and views/surname_review.py (B3: candidate + host entry texts).

Run: python3.11 Zylbercweig/people/export_entry_context.py
"""
from __future__ import annotations

import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from people_common import load_mentions_with_host, write_tsv  # noqa: E402

TEXTS_TSV = HERE / "entry_texts.tsv"
SLIM_TSV = HERE / "entry_mentions_slim.tsv"


def main() -> None:
    texts: dict[str, dict] = {}
    slim: list[dict] = []
    for r in load_mentions_with_host():
        pid = r["host_person_id"]
        if pid and pid not in texts:
            text = (r.get("host_entry_text") or "").strip()
            if text:
                texts[pid] = {
                    "person_id": pid,
                    "volume": r["host_volume"],
                    "xml_id": r["host_xml_id_filled"],
                    "heading": r["host_heading_filled"],
                    "entry_text": text.replace("\t", " ").replace("\n", "⏎"),
                }
        slim.append({
            "host_person_id": pid,
            "mention_name": (r.get("mention_name") or "").strip(),
            "gender": (r.get("gender") or "").strip(),
            "relation_category": (r.get("relation_category") or "").strip(),
            "relation_sentence": (r.get("relation_sentence") or "").strip()[:400],
        })

    text_rows = sorted(texts.values(), key=lambda r: r["person_id"])
    slim.sort(key=lambda r: (r["host_person_id"], r["mention_name"]))
    write_tsv(TEXTS_TSV, text_rows,
              ["person_id", "volume", "xml_id", "heading", "entry_text"])
    write_tsv(SLIM_TSV, slim,
              ["host_person_id", "mention_name", "gender",
               "relation_category", "relation_sentence"])
    print(f"entry_texts.tsv: {len(text_rows)} entries "
          f"({TEXTS_TSV.stat().st_size / 1e6:.1f} MB)")
    print(f"entry_mentions_slim.tsv: {len(slim)} mentions "
          f"({SLIM_TSV.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
