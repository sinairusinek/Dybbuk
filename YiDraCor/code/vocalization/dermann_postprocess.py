"""DerManUnterTiff-specific vocalization post-passes.

Applies three edition-specific corrections to `data/DerManUnterTiff/page_final/`:

  1. Matres lectionis shift: hiriq/segol moves from consonant to the following
     mater letter (yod for hiriq, ayin for segol). Edition-wide.
  2. Speaker canonical forms: in speaker-label lines, replace bare/over-vocalized
     character names with their canonical speaker forms.
  3. Stage-direction vowel strip: within stage directions, remove all hiriqs
     and segols (single-line paren blocks via regex; multi-line blocks via
     per-page override).

Run from code/ dir:
    python -m vocalization.dermann_postprocess
    python -m vocalization.dermann_postprocess --dry-run    # show diffs, no write
    python -m vocalization.dermann_postprocess --pages 5,11 # subset
"""

import argparse
import logging
import re
import sys
from pathlib import Path

from lxml import etree

from .speakers import normalize_speaker_label, strip_nikud

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

REPO_ROOT = Path(__file__).resolve().parents[2]
PLAY_DIR = REPO_ROOT / "data" / "DerManUnterTiff" / "page_final"

# Unicode points
HIRIQ  = "ִ"   # ִ
SEGOL  = "ֶ"   # ֶ
DAGESH = "ּ"   # ּ
YOD    = "י"   # י
AYIN   = "ע"   # ע

# Canonical speaker forms: skeleton (consonants only) → canonical form
SPEAKER_CANONICAL = {
    "סאבעלע":   "סאבּעלע",
    "טורניווער": "טוּרניווער",
}

# Multi-line stage blocks (inclusive ranges) — page_num: [(first, last), ...]
MULTILINE_STAGE = {
    5:  [(0, 5)],                  # opening set description
    7:  [(17, 18)],                # entrance of Krokover
    11: [(18, 20)],                # Yakhtshe hides under the table
    12: [(23, 24)],                # Turniver chase around table
    14: [(25, 27)],                # whispered aside (broken parens)
}

# Extra stage lines (no parens, still stage) — page_num: [line_idx, ...]
EXTRA_STAGE_LINES = {
    17: [9],  # "אויפֿטריט טוּרְנִיווער אוּן קראָקעֶוועֶר"
}

# Pages where speaker-label normalization should NOT apply (e.g. cast list,
# where a name-only line is a dramatis-personae entry, not a speech cue).
NO_SPEAKER_NORMALIZE_PAGES = {4}


def matres_shift(text: str) -> str:
    """Move hiriq/segol from a consonant to the following mater letter.

    When the source already has the vowel on BOTH the consonant and the mater
    (`Cִיִ` / `CֶעֶE`), the consonant's vowel is the redundant one — drop it,
    keeping the mater's. Otherwise, move the vowel from consonant to mater.
    """
    # Hiriq: drop redundant consonant-hiriq when mater also has hiriq
    text = re.sub(r"([א-ת][ּ]?)ִ(יִ)", r"\1\2", text)
    # Hiriq: shift consonant-hiriq onto following mater yod
    text = re.sub(r"([א-ת][ּ]?)ִ(י)", r"\1\2" + HIRIQ, text)
    # Segol: drop redundant consonant-segol when mater also has segol
    text = re.sub(r"([א-ת][ּ]?)ֶ(עֶ)", r"\1\2", text)
    # Segol: shift consonant-segol onto following mater ayin
    text = re.sub(r"([א-ת][ּ]?)ֶ(ע)", r"\1\2" + SEGOL, text)
    return text


def apply_speaker_canonical(text: str) -> str:
    """Thin DerMann wrapper around the general speaker-label normalizer."""
    return normalize_speaker_label(text, canonical=SPEAKER_CANONICAL)


def find_paren_spans(text: str) -> list[tuple[int, int]]:
    """Return list of (start, end) for balanced (...) within a single line."""
    spans = []
    depth = 0
    start = -1
    for i, c in enumerate(text):
        if c == "(":
            if depth == 0:
                start = i
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0 and start >= 0:
                spans.append((start, i + 1))
                start = -1
            elif depth < 0:
                depth = 0
    return spans


def strip_in_spans(text: str, spans: list[tuple[int, int]]) -> str:
    if not spans:
        return text
    out = []
    prev = 0
    for s, e in sorted(spans):
        out.append(text[prev:s])
        chunk = text[s:e].replace(HIRIQ, "").replace(SEGOL, "")
        out.append(chunk)
        prev = e
    out.append(text[prev:])
    return "".join(out)


def is_whole_line_stage(page_num: int, line_idx: int) -> bool:
    for first, last in MULTILINE_STAGE.get(page_num, []):
        if first <= line_idx <= last:
            return True
    return line_idx in EXTRA_STAGE_LINES.get(page_num, [])


def correct_line(page_num: int, line_idx: int, text: str) -> str:
    """Apply all DerMann corrections to one line."""
    # 1. Matres lectionis shift (edition-wide)
    text = matres_shift(text)
    # 2. Speaker canonical (skipped on cast-list-style pages)
    if page_num not in NO_SPEAKER_NORMALIZE_PAGES:
        text = apply_speaker_canonical(text)
    # 3. Stage direction vowel strip
    if is_whole_line_stage(page_num, line_idx):
        text = text.replace(HIRIQ, "").replace(SEGOL, "")
    else:
        spans = find_paren_spans(text)
        if spans:
            text = strip_in_spans(text, spans)
    return text


def page_num_from_filename(name: str) -> int:
    m = re.match(r"^(\d+)_", name)
    if not m:
        raise ValueError(f"bad filename: {name}")
    return int(m.group(1))


def process_file(path: Path, dry_run: bool = False) -> dict:
    page_num = page_num_from_filename(path.name)
    tree = etree.parse(str(path))
    changed = 0
    examples = []
    for line in tree.iter(LINE_TAG):
        line_idx_str = line.get("custom", "")
        # Extract sequential line index via document order
    # Re-iterate in document order with index
    lines = list(tree.iter(LINE_TAG))
    for idx, line in enumerate(lines):
        for u in line.iter(UNICODE_TAG):
            if u.getparent().getparent() is line and u.text:
                old = u.text
                new = correct_line(page_num, idx, old)
                if new != old:
                    changed += 1
                    if len(examples) < 3:
                        examples.append((idx, old, new))
                    u.text = new
                break
    if changed and not dry_run:
        tree.write(str(path), encoding="UTF-8",
                   xml_declaration=True, standalone=True)
    return {"page": page_num, "changed_lines": changed, "examples": examples}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pages", help='filter, e.g. "5,11" or "3-7"')
    args = ap.parse_args()

    page_filter = None
    if args.pages:
        page_filter = set()
        for part in args.pages.split(","):
            if "-" in part:
                a, b = part.split("-")
                page_filter.update(range(int(a), int(b) + 1))
            else:
                page_filter.add(int(part))

    files = sorted(PLAY_DIR.glob("*.xml"))
    log.info(f"{len(files)} pages in {PLAY_DIR.relative_to(REPO_ROOT)}")

    total_changed = 0
    for f in files:
        n = page_num_from_filename(f.name)
        if page_filter is not None and n not in page_filter:
            continue
        result = process_file(f, dry_run=args.dry_run)
        if result["changed_lines"]:
            log.info(f"[p{result['page']:>2}] {result['changed_lines']} lines changed")
            for idx, old, new in result["examples"]:
                log.info(f"   l{idx}: {old!r}")
                log.info(f"     →    {new!r}")
            total_changed += result["changed_lines"]
    log.info(f"total changed: {total_changed} lines ({'DRY RUN' if args.dry_run else 'WRITTEN'})")


if __name__ == "__main__":
    main()
