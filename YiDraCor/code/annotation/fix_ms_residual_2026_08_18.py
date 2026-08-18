"""Clear the mechanical residue of the 2026-08-16 stage_untyped sheet.

Three classes, all following conventions that already exist; the two genuinely
unclear spans (BasKoyen p.29 `3 1 2`, DiTsveyTnoim p.48 `.II A`) are left for
the RA. Sinai 2026-08-18.

A. Repeat-with-a-count `(N)` -> stage{type:delivery}. Section M4 already types the
   repeat mark as `delivery` and does not record the count. BenHaDor p.33 writes
   `אלטע | קווא (4) (ביסס)` — the count and the repeat word side by side — which
   is what identifies the bare `(4)` as the same instruction.

B. A bare Roman numeral heading a stanza -> head. Khurbn p.21 numbers the five
   stanzas of `שלאף מיין קינד` I..V with a `Refrein` between; those are verse
   numbering, not act headings and not cues. Section M5 already sends the parallel
   `רעפריין` rubric to `head`. Requires the numeral to be alone on its line and
   followed within two lines by a line carrying `l`/`lg`.

   NB this corrects C7: Khurbn's bare numerals reach V because it is the fifth
   STANZA, not a fifth act. They are also not uniformly one thing — p.19 `II` is
   a music cue (an orchestra march follows), which is why C7b holds the play back
   from any blanket rule.

    python3.11 -m annotation.fix_ms_residual_2026_08_18 --dry-run
    python3.11 -m annotation.fix_ms_residual_2026_08_18 --write
"""
import argparse
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lxml import etree
from annotation.schema import parse_custom, serialize_custom, dedup_entries

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
COUNT = re.compile(r'\(\s*\d{1,2}\s*\)')
# C. Three music cues the corpus-wide patterns cannot reach, fixed by name
# rather than by widening a regex that would then over-fire elsewhere:
#   Emigration p.110 `RII4` — a bare digit glued to a cue; the number pattern
#     needs an N/No prefix.
#   Emigration p.86  `13`   — the `N(` sits on the PREVIOUS line, so nothing
#     on-line matches; a song follows.
#   Khurbn p.19      `II`   — a music cue, not a stanza number: the next lines
#     are `מוזיק שפיעלט` and `/ארקעסטער מארש/`. The same play numbers stanzas
#     with the same characters elsewhere, which is why no blanket rule is safe.
EXPLICIT = {
    ("MS_Emigration", 110, "4"):  ("musicCue", "number", "4"),
    ("MS_Emigration", 86, "3"):   ("musicCue", "number", "13"),
    ("MS_KhurbnYerusholaim", 19, "II"): ("musicCue", "out", None),
}
NUMERAL = re.compile(r'^(I{1,3}|IV|V)\.?$')


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--write", action="store_true")
    args = ap.parse_args()

    rep = Counter()
    for man in sorted(REPO.glob("data/*/_ms_pull_manifest.json")):
        play = man.parent.name
        for f in sorted((man.parent / "page_annotated").glob("*.xml")):
            tree = etree.parse(str(f))
            lines = list(tree.iter(NS + "TextLine"))
            texts = []
            for tl in lines:
                u = tl.find(f"./{NS}TextEquiv/{NS}Unicode")
                texts.append((u.text or "") if u is not None else "")
            page_touched = False
            for i, tl in enumerate(lines):
                text = texts[i]
                entries = parse_custom(tl.get("custom") or "")
                out, changed = [], False
                for tag, a in entries:
                    if tag not in ("stage", "heading"):
                        out.append((tag, a)); continue
                    try:
                        s = int(a["offset"]); e = s + int(a["length"])
                    except (KeyError, ValueError):
                        out.append((tag, a)); continue
                    span = text[s:e].strip()
                    # C. named one-off cues
                    key = (play, int(f.name.split("_")[0]), span)
                    if tag == "stage" and not a.get("type") and key in EXPLICIT:
                        fn, role, n = EXPLICIT[key]
                        b = {k: v for k, v in a.items() if k in ("offset", "length")}
                        b["function"] = fn; b["role"] = role
                        if n:
                            b["n"] = n
                        out.append(("metamark", b)); changed = True
                        rep[f"C named cue -> metamark ({play} p.{key[1]})"] += 1
                        continue
                    # A. repeat-with-a-count
                    if tag == "stage" and not a.get("type") and COUNT.fullmatch(span):
                        b = dict(a); b["type"] = "delivery"
                        out.append(("stage", b)); changed = True
                        rep[f"A repeat-count -> delivery ({play})"] += 1
                        continue
                    # B. stanza numeral
                    if NUMERAL.match(span) and span == text.strip():
                        nxt = " ".join(
                            (tl2.get("custom") or "") for tl2 in lines[i + 1:i + 3])
                        if re.search(r'\bl\s*\{|\blg\s*\{', nxt):
                            b = {k: v for k, v in a.items()
                                 if k in ("offset", "length")}
                            out.append(("head", b)); changed = True
                            rep[f"B stanza numeral -> head ({play})"] += 1
                            continue
                    out.append((tag, a))
                if changed:
                    tl.set("custom", serialize_custom(dedup_entries(out)))
                    page_touched = True
            if page_touched and args.write:
                tree.write(str(f), encoding="utf-8", xml_declaration=True)
                rep["pages written"] += 1
    for k in sorted(rep):
        print(f"  {k:48} {rep[k]}")
    print("\n" + ("WROTE" if args.write else "DRY RUN"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
