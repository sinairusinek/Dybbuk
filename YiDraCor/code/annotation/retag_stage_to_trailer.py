"""Retag the manuscript `stage` spans that are not stage directions at all.

`stage_lexicon` already recognises these — it returns `trailer` for a line
opening `ענדע` and `epilog` for `עפילאג` — but it returns a value that is not a
member of the stage @type vocabulary, because they are not stage directions:
they are a different TEI element. `type_stage_spans` therefore, correctly,
declines to write them and leaves the spans bare. This finishes the job.

  `ענדע ערשטער אקט`, `ענדע 2 אקט`, `"ענדע"`  →  trailer
  `עפילאָג`                                   →  heading {type:epilog}

TEI-wise a <trailer> is the closing label of a division, which is exactly what
these are; the 2026-05-24 PI review already re-typed the printed corpus's
`ענדע` lines out of `stage` for the same reason, so this brings the manuscript
track into line with a decision the print track made over a year ago.

  python3.11 -m annotation.retag_stage_to_trailer --dry-run
  python3.11 -m annotation.retag_stage_to_trailer --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import PAGE_NS, parse_custom, serialize_custom  # noqa: E402
from annotation.auto_resolve_flags import stage_lexicon  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"


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
    args = ap.parse_args()

    data = REPO / "data"
    folders = [p for p in sorted(data.glob("MS_*")) if (p / "page_annotated").is_dir()]
    folders += [data / "Lateiner_Meshumed"]

    stats = Counter()
    for folder in folders:
        pa = folder / "page_annotated"
        if not pa.is_dir():
            continue
        for xf in sorted(pa.glob("*.xml")):
            tree = etree.parse(str(xf))
            changed = False
            for tl in tree.getroot().iter(f"{NS}TextLine"):
                ents = parse_custom(tl.get("custom") or "")
                if not any(t == "stage" and not a.get("type") for t, a in ents):
                    continue
                text = _line_text(tl)
                out, touched = [], False
                for tag, a in ents:
                    if tag == "stage" and not a.get("type"):
                        try:
                            off, ln = int(a["offset"]), int(a["length"])
                            span = text[off:off + ln]
                        except (KeyError, ValueError):
                            span = text
                        # Ask the lexicon about the SPAN, then the whole line —
                        # `ענדע` is often the span while the line carries the
                        # act number too.
                        # The MS plays delimit directions with their own
                        # marks — Yoysef with %, Khurbn with /, others with
                        # quotes or brackets. `stage_lexicon` strips only
                        # parentheses and punctuation, so `%ענדע ערשטער אקט%`
                        # and `"ענדע"` fail its `startswith` test. Strip the
                        # delimiters here rather than widening a lexicon the
                        # print track shares and depends on.
                        bare = span.strip("%/\"'”“[]{}|«»‟ \t")
                        verdict = (stage_lexicon(bare) or stage_lexicon(span)
                                   or stage_lexicon(text))
                        if verdict == "trailer":
                            out.append(("trailer", {k: v for k, v in a.items()
                                                    if k in ("offset", "length")}))
                            stats["stage → trailer"] += 1
                            touched = True
                            continue
                        if verdict == "epilog":
                            keep = {k: v for k, v in a.items()
                                    if k in ("offset", "length")}
                            keep["type"] = "epilog"
                            out.append(("heading", keep))
                            stats["stage → heading{type:epilog}"] += 1
                            touched = True
                            continue
                    out.append((tag, a))
                if touched:
                    tl.set("custom", serialize_custom(out))
                    changed = True
            if changed and args.apply:
                tree.write(str(xf), encoding="utf-8", xml_declaration=True)

    for k, v in sorted(stats.items()):
        print(f"{v:4}  {k}")
    print("APPLIED" if args.apply else "DRY RUN — nothing written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
