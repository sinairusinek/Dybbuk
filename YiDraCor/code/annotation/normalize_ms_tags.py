"""Normalise the manuscript-track tag defects the schema cannot simply accept.

Companion to the 2026-08-16 schema widening. That change accepted what was
legitimate but unrecognised — the TEI name elements, `foreign @lang`,
Transkribus' own `comment`/`topic`. What is left here is genuinely wrong in the
data and has to be rewritten:

  personName → persName   `personName` is not a TEI element; `persName` is.
                          Meshumed writes the wrong one 84 times and the right
                          one 29 times, in the same play.

  fw @type                `fw` requires a type and 368 spans had none. 360 are
                          bare digits — printed page numbers, so `pageNum`,
                          the same call the print corpus made. The other 8 are
                          NOT forme work at all (a speech line, character names,
                          a music cue `N = 1`); they are left untyped and
                          reported, because typing them would bake in a
                          mis-tagging rather than fix it.

  foreign @lang           Normalised through `schema.normalize_lang` to BCP-47
                          so `build_tei` can emit @xml:lang directly: LK → he
                          (loshn-koydesh), Rus → ru, Slav → sla, and so on.

  python3.11 -m annotation.normalize_ms_tags --dry-run
  python3.11 -m annotation.normalize_ms_tags --apply --report /tmp/fw.tsv
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (  # noqa: E402
    PAGE_NS, parse_custom, serialize_custom, normalize_lang,
)
from annotation.review_links import page_url  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"
# A printed page number: digits, optionally with surrounding punctuation.
_PAGENUM = re.compile(r"^[\s.\-—–\[\](){}]*\d+[\s.\-—–\[\](){}]*$")


def _line_text(tl) -> str:
    for te in tl.findall(f"{NS}TextEquiv"):
        u = te.find(f"{NS}Unicode")
        if u is not None:
            return u.text or ""
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report")
    args = ap.parse_args()

    data = REPO / "data"
    folders = [p for p in sorted(data.glob("MS_*")) if (p / "page_annotated").is_dir()]
    folders += [data / "Lateiner_Meshumed"]

    stats, odd = Counter(), []
    for folder in folders:
        pa = folder / "page_annotated"
        if not pa.is_dir():
            continue
        for xf in sorted(pa.glob("*.xml")):
            tree = etree.parse(str(xf))
            changed = False
            for tl in tree.getroot().iter(f"{NS}TextLine"):
                ents = parse_custom(tl.get("custom") or "")
                text = _line_text(tl)
                out, touched = [], False
                for tag, a in ents:
                    if tag == "personName":
                        tag = "persName"
                        stats["personName → persName"] += 1
                        touched = True
                    if tag == "foreign" and a.get("lang"):
                        norm = normalize_lang(a["lang"])
                        if norm != a["lang"]:
                            a = dict(a)
                            a["lang"] = norm
                            stats[f"lang {norm}"] += 1
                            touched = True
                    if tag == "fw" and not a.get("type"):
                        try:
                            off, ln = int(a["offset"]), int(a["length"])
                            span = text[off:off + ln]
                        except (KeyError, ValueError):
                            span = text
                        if _PAGENUM.match(span):
                            a = dict(a)
                            a["type"] = "pageNum"
                            stats["fw → type:pageNum"] += 1
                            touched = True
                        else:
                            stats["fw LEFT (not forme work)"] += 1
                            odd.append({
                                "play": folder.name, "page": xf.name,
                                "transkribus_url": page_url(folder.name, xf.name),
                                "span": span.strip()[:70],
                                "line": re.sub(r"\s+", " ", text).strip()[:80],
                                "note": "tagged `fw` but is not forme work — "
                                        "retag or drop",
                            })
                    out.append((tag, a))
                if touched:
                    tl.set("custom", serialize_custom(out))
                    changed = True
            if changed and args.apply:
                tree.write(str(xf), encoding="utf-8", xml_declaration=True)

    for k, v in sorted(stats.items(), key=lambda kv: -kv[1]):
        print(f"{v:5}  {k}")
    print("APPLIED" if args.apply else "DRY RUN — nothing written")

    if args.report and odd:
        cols = ["play", "page", "transkribus_url", "span", "line", "note"]
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
            w.writeheader()
            w.writerows(odd)
        print(f"mis-tagged fw spans → {args.report} ({len(odd)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
