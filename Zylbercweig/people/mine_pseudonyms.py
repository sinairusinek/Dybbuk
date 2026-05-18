"""Mine pseudonym/alias relations from heading + subheading + names_variants.

The Zylbercweig schema encodes aliases in three places:
  - heading bracketed suffix:  "אַדלער, יעקב פּ. [יעקב בן פּנחס]"
  - subheading bracketed text: "[ביילקע קאָלאַך]"
  - names_variants entries:    "בערטאַ קאַליך | בערטאָ קאַליש | ..."

This pass extracts every (primary_form, alias_form) pair and writes them to
people_aliases.tsv. The aliases TSV is then loadable by the alignment + dedup
passes to short-circuit pseudonym matches that pure similarity would miss
(e.g. שלום-עליכם ↔ שלום ראַבינאָוויטש).

Output columns: person_id, xml_id, heading, primary_form, alias_form, source.
"""
from __future__ import annotations
import csv, re, sys, pathlib

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
from people_similarity import heading_to_canonical  # type: ignore

PEOPLE_TSV = HERE / "people_extracted.tsv"
OUT_TSV = HERE / "people_aliases.tsv"

BRACKET = re.compile(r"[\[\(]([^\]\)]+)[\]\)]")
DATE_LIKE = re.compile(r"^[\d\s\-.–—:]+$")
DATE_WORDS = ("געב", "געשט", "געבוירן", "געשטאָרבן", "געב.", "געשט.")


def is_date_phrase(s: str) -> bool:
    s = s.strip()
    if DATE_LIKE.fullmatch(s):
        return True
    return any(w in s for w in DATE_WORDS)


def main():
    with open(PEOPLE_TSV) as f:
        people = list(csv.DictReader(f, delimiter="\t"))

    rows = []
    for p in people:
        primary = heading_to_canonical(p["heading"]) or p["heading"]
        seen_aliases = set()

        # bracketed aliases in heading
        for m in BRACKET.finditer(p["heading"] or ""):
            piece = m.group(1).strip()
            if not piece or is_date_phrase(piece):
                continue
            if piece in seen_aliases:
                continue
            seen_aliases.add(piece)
            rows.append({
                "person_id": p["person_id"],
                "xml_id": p["xml_id"],
                "heading": p["heading"],
                "primary_form": primary,
                "alias_form": piece,
                "source": "heading_bracket",
            })

        # bracketed text in subheading
        for m in BRACKET.finditer(p.get("subheading") or ""):
            piece = m.group(1).strip()
            if not piece or is_date_phrase(piece):
                continue
            if piece in seen_aliases:
                continue
            seen_aliases.add(piece)
            rows.append({
                "person_id": p["person_id"],
                "xml_id": p["xml_id"],
                "heading": p["heading"],
                "primary_form": primary,
                "alias_form": piece,
                "source": "subheading_bracket",
            })

        # names_variants: every variant beyond the heading itself
        primary_normish = primary.replace(",", "").strip()
        for v in (p.get("names_variants") or "").split("|"):
            v = v.strip()
            if not v:
                continue
            if v in seen_aliases:
                continue
            # skip near-duplicates of primary
            if v == p["heading"] or v == primary or v == primary_normish:
                continue
            # skip bare initials
            if len(v.replace(".", "").strip()) <= 1:
                continue
            seen_aliases.add(v)
            rows.append({
                "person_id": p["person_id"],
                "xml_id": p["xml_id"],
                "heading": p["heading"],
                "primary_form": primary,
                "alias_form": v,
                "source": "names_variant",
            })

    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=list(rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT_TSV} ({len(rows)} alias rows for "
          f"{len({r['person_id'] for r in rows})} distinct persons)")
    # quick breakdown
    from collections import Counter
    print("by source:", dict(Counter(r["source"] for r in rows)))


if __name__ == "__main__":
    main()
