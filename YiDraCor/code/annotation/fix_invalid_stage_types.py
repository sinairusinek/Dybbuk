"""Repair the 21 manuscript `stage` spans whose @type is not in the vocabulary.

Found 2026-08-16 while auditing stage typing: these predate the typing pass —
they are the spans that already carried a type, and every one of them fails
`schema._validate_stage_type`, so lint would reject them and `build_tei` would
emit an invalid @type.

Three kinds, and only the first is mechanical:

1. SEPARATOR / SPELLING. Multi-token types written with commas or a pipe
   instead of spaces (`exit, delivery, business`, `settings|novelistic`), and
   the plural `settings`. Normalised by rule.

2. HEADING ATTRS ON A STAGE SPAN. Nine Yaakov-Esav spans and three Tissa-Essler
   ones carry `act n:1` / `scene 1` / `scene 2` — the RA applied the heading
   vocabulary to a stage span. Their content settles what they are: the long
   scenic descriptions that open a division (`א געווייסט צימער אין חאניע דעם
   קברנ׳ס וואהנונג`) are `setting`. The one exception is the literal
   `ערשטער אקט.`, which is genuinely an act heading wrongly tagged `stage`; it
   is left alone for the heading-notation question rather than being forced
   into the stage vocabulary.

3. A LEAKED lg ATTRIBUTE. One Yoysef span typed `cont`, over
   `אַ מעלאדראמע יוסף ערשייַנט` — a melodrama cue plus an entrance, so
   `delivery entrance`.

  python3.11 -m annotation.fix_invalid_stage_types --dry-run
  python3.11 -m annotation.fix_invalid_stage_types --apply
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (  # noqa: E402
    PAGE_NS, parse_custom, serialize_custom, _validate_stage_type,
)

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"

# Left untouched on purpose: an act heading mis-tagged `stage`. Retagging it
# `heading` needs the act/scene answer that is still out with the RAs.
_LEAVE_FOR_HEADING_QUESTION = "ערשטער אקט"


def repair(typ: str, span_text: str):
    """Return a valid @type, or None to leave the span alone."""
    t = (typ or "").strip()
    # 0. literal escape sequences. Transkribus stores some punctuation the RA
    # typed as the six characters ` ` rather than the character itself —
    # the same mojibake as the `con: yes` seen on lg spans. Decode before
    # anything else, or `business, delivery` merely becomes
    # `business  delivery`.
    t = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), t)
    # 1. separators and spelling
    t = t.replace("|", " ").replace(",", " ")
    t = re.sub(r"\bsettings\b", "setting", t)
    t = " ".join(t.split())

    # 2. heading vocabulary on a stage span
    if re.match(r"^(act|scene)\b", t):
        if _LEAVE_FOR_HEADING_QUESTION in (span_text or ""):
            return None
        return "setting"

    # 3. leaked lg attribute
    if t == "cont":
        return "delivery entrance"

    return t if _validate_stage_type(t) is None else None


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
                if not any(t == "stage" for t, _ in ents):
                    continue
                text = _line_text(tl)
                out, touched = [], False
                for tag, a in ents:
                    if tag == "stage" and a.get("type") and _validate_stage_type(a["type"]):
                        try:
                            off, ln = int(a["offset"]), int(a["length"])
                            span = text[off:off + ln]
                        except (KeyError, ValueError):
                            span = text
                        fixed = repair(a["type"], span)
                        if fixed:
                            stats[f"{a['type']} → {fixed}"] += 1
                            a = dict(a)
                            a["type"] = fixed
                            touched = True
                        else:
                            stats[f"LEFT {a['type']}"] += 1
                    out.append((tag, a))
                if touched:
                    tl.set("custom", serialize_custom(out))
                    changed = True
            if changed and args.apply:
                tree.write(str(xf), encoding="utf-8", xml_declaration=True)

    for k, v in sorted(stats.items(), key=lambda kv: -kv[1]):
        print(f"{v:4}  {k}")
    print(f"{sum(v for k, v in stats.items() if not k.startswith('LEFT')):4}  repaired")
    print("APPLIED" if args.apply else "DRY RUN — nothing written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
