"""Remove all `unclear {...}` entries from custom attributes of a play's XMLs.

Useful when the unclear marks (added during vocalization conflict-flagging)
should be cleared so the RA can redo the vocalization without noise.

Usage:
  python -m vocalization.strip_unclear --play DerManUnterTiff
  python -m vocalization.strip_unclear --play DerManUnterTiff --dirs page_final page_annotated
"""

import argparse
import re
import sys
from pathlib import Path

from lxml import etree

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
REPO_ROOT = Path(__file__).resolve().parents[2]

# Match `unclear {...}` entries (with any attributes) and the trailing whitespace.
UNCLEAR_RE = re.compile(r"\s*unclear\s*\{[^}]*\}")


def strip_unclear_from_custom(custom: str) -> str:
    return UNCLEAR_RE.sub("", custom or "").strip()


def process_file(path: Path) -> int:
    tree = etree.parse(str(path))
    changed = 0
    for el in tree.iter():
        c = el.get("custom")
        if c and "unclear" in c:
            new = strip_unclear_from_custom(c)
            if new != c:
                el.set("custom", new)
                changed += 1
    if changed:
        tree.write(str(path), encoding="UTF-8",
                   xml_declaration=True, standalone=True)
    return changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", required=True)
    ap.add_argument("--dirs", nargs="+", default=["page_final", "page_annotated"],
                    help="subdirs of data/{play}/ to process")
    args = ap.parse_args()

    play_dir = REPO_ROOT / "data" / args.play
    total = 0
    for d in args.dirs:
        sub = play_dir / d
        if not sub.is_dir():
            print(f"skip (no dir): {sub.relative_to(REPO_ROOT)}")
            continue
        files = sorted(sub.glob("*.xml"))
        sub_total = 0
        for f in files:
            n = process_file(f)
            if n:
                sub_total += n
        print(f"{sub.relative_to(REPO_ROOT)}: {sub_total} elements cleaned across {len(files)} files")
        total += sub_total
    print(f"total: {total} unclear entries removed")


if __name__ == "__main__":
    main()
