"""Re-anchor `custom` spans onto a corrected line text after a Transkribus
round-trip.

When the RA edits a line on Transkribus (adding niqqud, fixing an OCR error),
the line's `custom` attribute keeps its old offset/length values that point
into the OLD text — so each span now references the wrong characters in the
NEW text. This module recomputes offset/length by:

  1. Stripping Hebrew combining marks (niqqud, dagesh, etc.) from both old and
     new text to get a "bare consonant" view.
  2. Locating the span's bare substring in the new bare text.
  3. Mapping that bare range back to character indices in the full new text.

If the bare consonants changed (OCR-level correction), falls back to
substring search; if the old substring is not found in the new bare text, the
span is dropped and recorded in the report.

Public entry:
  reanchor_tree(old_tree, new_tree) -> (mutated new_tree, report dict)
  reanchor_file(old_path, new_path, out_path) -> report dict
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from lxml import etree

from annotation.schema import PAGE_NS, parse_custom, serialize_custom
from annotation.normalize import normalize_line

LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

# Hebrew combining marks (niqqud, dagesh, sin/shin dot, rafe, cantillation)
COMBINING = set(chr(c) for c in range(0x0591, 0x05C8))
# Punctuation that the RA may insert/remove without changing the underlying
# word identity (e.g. diminutive apostrophe in דבורה'לע / פריידא'לע).
SOFT_PUNCT = {"'", "׳"}  # ASCII apostrophe + Hebrew geresh


def _bare_with_map(text: str, *, strip_soft_punct: bool = False) -> tuple[str, list[int]]:
    """Return (bare_text, map) where map[i] = index in `text` of bare_text[i]."""
    drop = COMBINING | (SOFT_PUNCT if strip_soft_punct else set())
    bare_chars = []
    idx_map = []
    for i, c in enumerate(text):
        if c not in drop:
            bare_chars.append(c)
            idx_map.append(i)
    return "".join(bare_chars), idx_map


def _bare_before(text: str, pos: int, *, strip_soft_punct: bool = False) -> int:
    """Number of non-combining (and optionally non-soft-punct) chars in text[:pos]."""
    drop = COMBINING | (SOFT_PUNCT if strip_soft_punct else set())
    return sum(1 for c in text[:pos] if c not in drop)


def _line_text(line_el) -> tuple[etree._Element | None, str]:
    for u in line_el.iter(UNICODE_TAG):
        if u.getparent().getparent() is line_el:
            return u, (u.text or "")
    return None, ""


def reanchor_span(old_text: str, off: int, length: int,
                  new_text: str) -> tuple[int, int] | None:
    """Return (new_offset, new_length) for the same span content in new_text,
    or None if it can't be located."""
    if off < 0 or length <= 0 or off + length > len(old_text):
        return None

    old_bare, _ = _bare_with_map(old_text)
    new_bare, new_map = _bare_with_map(new_text)

    bare_start = _bare_before(old_text, off)
    bare_end = _bare_before(old_text, off + length)

    if old_bare == new_bare:
        # Fast path: bare consonants unchanged — index-map directly.
        if bare_start >= len(new_map):
            new_off = len(new_text)
        else:
            new_off = new_map[bare_start]
        if bare_end >= len(new_map):
            new_end = len(new_text)
        else:
            new_end = new_map[bare_end]
        return new_off, new_end - new_off

    # Slow path: bare text changed too. Try plain bare-substring search first;
    # then a stricter retry stripping soft punctuation (RA-inserted apostrophes).
    sub = old_bare[bare_start:bare_end]
    if sub:
        candidates = []
        start = 0
        while True:
            i = new_bare.find(sub, start)
            if i < 0:
                break
            candidates.append(i)
            start = i + 1
        if candidates:
            pick = min(candidates, key=lambda i: abs(i - bare_start))
            new_off = new_map[pick] if pick < len(new_map) else len(new_text)
            pick_end = pick + len(sub)
            new_end = new_map[pick_end] if pick_end < len(new_map) else len(new_text)
            return new_off, new_end - new_off

    # Soft-punct-stripped retry (handles RA inserting/removing diminutive ').
    old_core, _ = _bare_with_map(old_text, strip_soft_punct=True)
    new_core, new_core_map = _bare_with_map(new_text, strip_soft_punct=True)
    core_start = _bare_before(old_text, off, strip_soft_punct=True)
    core_end = _bare_before(old_text, off + length, strip_soft_punct=True)
    sub_core = old_core[core_start:core_end]
    if not sub_core:
        return None
    idx = new_core.find(sub_core)
    if idx < 0:
        return None
    new_off = new_core_map[idx] if idx < len(new_core_map) else len(new_text)
    idx_end = idx + len(sub_core)
    new_end = new_core_map[idx_end] if idx_end < len(new_core_map) else len(new_text)
    return new_off, new_end - new_off


def reanchor_tree(old_tree, new_tree) -> dict:
    """Mutate `new_tree`: rewrite each TextLine's custom attribute so spans
    point into the new line text. Returns a report:
      {'lines': N, 'spans_kept': K, 'spans_dropped': D, 'drops': [...] }
    """
    # Index old lines by id.
    old_by_id = {ln.get("id"): ln for ln in old_tree.iter(LINE_TAG)}

    report = {"lines": 0, "spans_kept": 0, "spans_dropped": 0, "drops": []}

    for new_line in new_tree.iter(LINE_TAG):
        report["lines"] += 1
        lid = new_line.get("id")
        old_line = old_by_id.get(lid)
        if old_line is None:
            continue
        _, old_text = _line_text(old_line)
        new_u, new_text_raw = _line_text(new_line)
        new_text = normalize_line(new_text_raw)
        if new_u is not None and new_text != new_text_raw:
            new_u.text = new_text

        # Source of truth for which spans existed: the OLD line's custom,
        # because Transkribus may strip our annotations when text is edited.
        old_custom = old_line.get("custom") or ""
        new_custom = new_line.get("custom") or ""
        old_entries = parse_custom(old_custom)
        new_entries = parse_custom(new_custom)

        # readingOrder & structure come from the new tree as-is.
        passthrough = [(t, a) for t, a in new_entries
                       if t in {"readingOrder", "structure"}]

        rewritten = list(passthrough)
        for tag, attrs in old_entries:
            if tag in {"readingOrder", "structure"}:
                continue
            try:
                off = int(attrs.get("offset"))
                ln = int(attrs.get("length"))
            except (TypeError, ValueError):
                report["spans_dropped"] += 1
                report["drops"].append({"line": lid, "tag": tag,
                                        "reason": "missing offset/length",
                                        "attrs": attrs})
                continue
            res = reanchor_span(old_text, off, ln, new_text)
            if res is None:
                report["spans_dropped"] += 1
                report["drops"].append({
                    "line": lid, "tag": tag,
                    "reason": "no match in new text",
                    "old_substring": old_text[off:off + ln],
                    "old_offset": off, "old_length": ln,
                })
                continue
            new_off, new_ln = res
            new_attrs = dict(attrs)
            new_attrs["offset"] = str(new_off)
            new_attrs["length"] = str(new_ln)
            rewritten.append((tag, new_attrs))
            report["spans_kept"] += 1

        new_line.set("custom", serialize_custom(rewritten))

    return report


def reanchor_file(old_path: str | Path, new_path: str | Path,
                  out_path: str | Path) -> dict:
    """Read old (pre-push) and new (pulled) PAGE-XML, re-anchor, write to out_path."""
    old_tree = etree.parse(str(old_path))
    new_tree = etree.parse(str(new_path))
    report = reanchor_tree(old_tree, new_tree)
    new_tree.write(str(out_path), encoding="UTF-8",
                   xml_declaration=True, standalone=True)
    return report


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--old", required=True, help="pre-push annotated PAGE-XML")
    ap.add_argument("--new", required=True, help="freshly-pulled PAGE-XML")
    ap.add_argument("--out", required=True, help="where to write the re-anchored file")
    args = ap.parse_args(argv)
    report = reanchor_file(args.old, args.new, args.out)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["spans_dropped"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
