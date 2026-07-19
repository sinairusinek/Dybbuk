"""Annotate vocalized PAGE-XML pages with span tags for Transkribus round-trip.

The LLM step happens in the Claude Code session itself — this module does NOT
call the Anthropic API. It provides:
  - `list_pages(play)`         — page list, prefers page_final/ over page/
  - `dump_lines(play, n)`      — emit line JSON for the LLM to annotate
  - `apply_annotation(...)`    — apply an annotation dict to a page and write
                                 to page_annotated/

Per-play state (act/scene running numbers, cast xml:ids) is loaded from /
saved to `page_annotated/_context.json` so calls compose across pages.

Usage:
  python -m annotation.annotate_pages dump --play DerManUnterTiff --page 5
  python -m annotation.annotate_pages apply --play DerManUnterTiff --page 5 --json /tmp/p5.json
  python -m annotation.annotate_pages list --play DerManUnterTiff
"""

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Optional

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[0].parent))
from annotation.schema import (
    PAGE_NS, PAGE_TYPES,
    append_custom, set_region_structure, sanitize_xmlid, validate_span,
)
from annotation.normalize import normalize_line

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

LINE_TAG = f"{{{PAGE_NS}}}TextLine"
REGION_TAG = f"{{{PAGE_NS}}}TextRegion"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

REPO_ROOT = Path(__file__).resolve().parents[2]

# Per-play scan pages to skip (duplicates, misbound, etc.) — never written to
# page_annotated/, never uploaded to Transkribus.
SKIP_PAGES: dict[str, set[int]] = {
    "DerManUnterTiff": {3},  # duplicate of p18 (the "−16−" song), inferior scan
}


def list_pages(play: str) -> list[tuple[int, Path]]:
    """Return [(page_num, source_path)] choosing page_final/ over page/ when available.
    Pages listed in SKIP_PAGES[play] are excluded."""
    play_dir = REPO_ROOT / "data" / play
    page_dir = play_dir / "page"
    final_dir = play_dir / "page_final"
    if not page_dir.is_dir():
        raise FileNotFoundError(f"No page/ dir for {play}: {page_dir}")
    skip = SKIP_PAGES.get(play, set())
    out = []
    for src in sorted(page_dir.glob("*.xml")):
        m = re.match(r"^(\d+)_", src.name)
        if not m:
            continue
        n = int(m.group(1))
        if n in skip:
            continue
        final_candidate = final_dir / src.name
        out.append((n, final_candidate if final_candidate.exists() else src))
    return out


def extract_lines(tree, *, normalize: bool = True) -> list[tuple[etree._Element, str]]:
    """Return list of (TextLine element, line text) in document order.

    When normalize=True (default), each line's Unicode text is run through
    annotation.normalize.normalize_line (collapses OCR-doubled diacritics) and
    the change is written back to the Unicode element so offsets line up.
    """
    out = []
    for line in tree.iter(LINE_TAG):
        text = ""
        u_el = None
        for u in line.iter(UNICODE_TAG):
            if u.getparent().getparent() is line and u.text:
                u_el = u
                text = u.text
                break
        if normalize and text:
            new_text = normalize_line(text)
            if new_text != text and u_el is not None:
                u_el.text = new_text
                text = new_text
        out.append((line, text))
    return out


def apply_annotations(tree, line_pairs, result: dict, context: dict) -> dict:
    """Mutate tree in place. Return stats + updated context."""
    page_type = result.get("page_type")
    if page_type not in PAGE_TYPES:
        raise ValueError(f"bad page_type: {page_type!r}")

    region = next(tree.iter(REGION_TAG), None)
    if region is not None:
        region.set("custom", set_region_structure(region.get("custom", ""), page_type))

    stats = {"page_type": page_type, "spans": 0, "errors": []}
    by_idx = {ln["line_idx"]: ln for ln in result.get("lines", [])}

    for idx, (line_el, text) in enumerate(line_pairs):
        spec = by_idx.get(idx)
        if not spec:
            continue
        # Sort spans by offset for stable order
        spans = sorted(spec.get("spans", []), key=lambda s: (s.get("offset", 0), s.get("length", 0)))
        # Overlap check per tag
        seen = {}
        for span in spans:
            err = validate_span(text, span)
            if err:
                stats["errors"].append(f"line {idx}: {err}; span={span}")
                continue
            tag = span["tag"]
            off, ln = span["offset"], span["length"]
            ranges = seen.setdefault(tag, [])
            if any(not (off + ln <= a or off >= a + b) for (a, b) in ranges):
                stats["errors"].append(f"line {idx}: {tag} overlaps existing {tag} span; span={span}")
                continue
            ranges.append((off, ln))

            attrs = dict(span.get("attrs") or {})
            # heading: bump rolling context
            if tag == "heading":
                t = attrs.get("type")
                if t == "act":
                    context["last_act"] = attrs.get("n", context.get("last_act"))
                    context["last_scene"] = None
                elif t == "scene":
                    context["last_scene"] = attrs.get("n", context.get("last_scene"))
            # role: sanitize xmlid and track for the play's cast_ids
            if tag == "role":
                raw = attrs.get("xmlid") or text[off:off + ln]
                xid = sanitize_xmlid(raw)
                existing = set(context.setdefault("cast_ids", []))
                base, k = xid, 2
                while xid in existing:
                    xid = f"{base}{k}"
                    k += 1
                attrs["xmlid"] = xid
                context["cast_ids"].append(xid)

            # speaker: track most recent
            if tag == "speaker":
                context["last_speaker"] = text[off:off + ln]

            custom_attrs = {"offset": str(off), "length": str(ln)}
            custom_attrs.update({k: v for k, v in attrs.items()})
            line_el.set("custom", append_custom(line_el.get("custom", ""), tag, custom_attrs))
            stats["spans"] += 1

    return stats


def initial_context() -> dict:
    return {"last_act": None, "last_scene": None, "last_speaker": None, "cast_ids": []}


def play_out_dir(play: str) -> Path:
    p = REPO_ROOT / "data" / play / "page_annotated"
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_context(play: str) -> dict:
    f = play_out_dir(play) / "_context.json"
    if f.exists():
        return json.loads(f.read_text(encoding="utf-8"))
    return initial_context()


def save_context(play: str, context: dict) -> None:
    f = play_out_dir(play) / "_context.json"
    f.write_text(json.dumps(context, ensure_ascii=False, indent=2),
                 encoding="utf-8")


def find_page(play: str, page_num: int) -> Path:
    for n, src in list_pages(play):
        if n == page_num:
            return src
    raise FileNotFoundError(f"page {page_num} not found for {play}")


class StaleSourceError(Exception):
    """Raised when page/ is older than the newest Transkribus pull, so
    regenerating page_annotated/ from it would bury human annotation."""


def _newest_pull(play: str, name: str) -> Optional[Path]:
    """Newest `page_transkribus_pulled_<date>*/` copy of `name`, if any.
    Directory names are date-stamped, so lexicographic max == newest."""
    root = REPO_ROOT / "data" / play
    cands = sorted(d for d in root.glob("page_transkribus_pulled_*") if d.is_dir())
    for d in reversed(cands):
        f = d / name
        if f.exists():
            return f
    return None


def check_source_fresh(play: str, src: Path) -> None:
    """Refuse to overwrite an existing page_annotated/ file when `src` is stale.

    2026-06-24: page/ still held the 06-18 pull while Noa's castList work sat in
    page_transkribus_pulled_2026-06-24post_castlist/. apply_annotation re-parsed
    page/ and wrote over page_annotated/, so the push buried her FINAL layer.
    """
    out_path = play_out_dir(play) / src.name
    if not out_path.exists():
        return  # bootstrap: nothing to lose
    pull = _newest_pull(play, src.name)
    if pull is None:
        return
    if pull.read_bytes() != src.read_bytes():
        raise StaleSourceError(
            f"{src.relative_to(REPO_ROOT)} differs from the newer pull "
            f"{pull.relative_to(REPO_ROOT)}; refusing to overwrite "
            f"{out_path.relative_to(REPO_ROOT)}. Resync page/ from the pull first "
            f"(or pass --allow-stale-source if you know page/ is authoritative)."
        )


def dump_lines(play: str, page_num: int) -> dict:
    """Return the per-page line payload the LLM should annotate."""
    src = find_page(play, page_num)
    tree = etree.parse(str(src))
    line_pairs = extract_lines(tree)
    return {
        "play": play,
        "page": page_num,
        "filename": src.name,
        "source": "page_final" if "page_final" in str(src) else "page",
        "context": load_context(play),
        "lines": [{"line_idx": i, "text": t} for i, (_el, t) in enumerate(line_pairs)],
    }


def apply_annotation(play: str, page_num: int, annotation: dict,
                     allow_stale_source: bool = False) -> dict:
    """Apply an annotation dict (page_type + lines+spans) to the page and write
    `page_annotated/<filename>.xml`. Updates and saves rolling context."""
    src = find_page(play, page_num)
    if not allow_stale_source:
        check_source_fresh(play, src)
    tree = etree.parse(str(src))
    line_pairs = extract_lines(tree)

    context = load_context(play)
    stats = apply_annotations(tree, line_pairs, annotation, context)

    out_dir = play_out_dir(play)
    out_path = out_dir / src.name
    tree.write(str(out_path), encoding="UTF-8",
               xml_declaration=True, standalone=True)
    save_context(play, context)

    log.info(f"[p{page_num}] page_type={stats['page_type']} spans={stats['spans']} "
             f"errors={len(stats['errors'])} → {out_path.relative_to(REPO_ROOT)}")
    for e in stats["errors"][:5]:
        log.warning(f"  • {e}")
    return {"page": page_num, **stats, "out": str(out_path.relative_to(REPO_ROOT))}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="list pages for a play")
    p_list.add_argument("--play", required=True)

    p_dump = sub.add_parser("dump", help="dump line payload for LLM")
    p_dump.add_argument("--play", required=True)
    p_dump.add_argument("--page", type=int, required=True)

    p_apply = sub.add_parser("apply", help="apply LLM annotation JSON")
    p_apply.add_argument("--play", required=True)
    p_apply.add_argument("--page", type=int, required=True)
    p_apply.add_argument("--json", required=True, help="path to annotation JSON")

    p_reset = sub.add_parser("reset-context", help="reset rolling context for a play")
    p_reset.add_argument("--play", required=True)

    args = ap.parse_args()

    if args.cmd == "list":
        for n, src in list_pages(args.play):
            print(f"{n}\t{src.relative_to(REPO_ROOT)}")
    elif args.cmd == "dump":
        print(json.dumps(dump_lines(args.play, args.page), ensure_ascii=False, indent=2))
    elif args.cmd == "apply":
        data = json.loads(Path(args.json).read_text(encoding="utf-8"))
        result = apply_annotation(args.play, args.page, data)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.cmd == "reset-context":
        save_context(args.play, initial_context())
        log.info(f"context reset for {args.play}")


if __name__ == "__main__":
    main()
