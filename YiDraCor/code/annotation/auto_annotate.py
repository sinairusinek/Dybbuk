"""Programmatic first-pass annotator for body pages.

Heuristics:
  - speaker  : line begins with a known cast name (any vocalized form) + ':'
  - heading  : line matches act-/scene-ordinal patterns
  - stage    : parenthesized fragment(s); handles multi-line open/close
  - page_type: castList if multiple lines match 'name <desc>' from cast_dict;
               titlePage if line count < 6 and no speakers detected; else body.

Usage:
  python -m annotation.auto_annotate --play <folder> [--start N] [--end N] [--dry-run]

Skips pages already in page_annotated/.
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_act_heading, parse_scene_heading
from annotation.annotate_pages import (list_pages, dump_lines, apply_annotation,
                                       StaleSourceError, REPO_ROOT)

NIKUD = re.compile(r"[֑-ֽֿ-ׇ]")


def strip_nikud(s: str) -> str:
    return NIKUD.sub("", s)


HE_ORDINAL_ACT = {
    "ערשטער": 1, "ראשון": 1, "ערשטע": 1,
    "צווייטער": 2, "צוייטער": 2, "צווייטע": 2, "שני": 2,
    "דריטער": 3, "דריטע": 3, "שלישי": 3,
    "פערטער": 4, "פערטע": 4, "רביעי": 4,
    "פינפטער": 5, "חמישי": 5,
    "זעקסטער": 6, "שישי": 6,
    "זיבעטער": 7, "שביעי": 7,
}

_N = NIKUD.pattern + "*"
STAGE_TYPE_HINTS = [
    ("entrance", re.compile(f"א{_N}ו?{_N}י?{_N}פֿ?{_N}ט{_N}ר{_N}י{_N}ט|ט{_N}ר{_N}ע{_N}ט\\s+א{_N}ר?{_N}ו{_N}י{_N}ס|ק{_N}ו{_N}מ{_N}ט\\s+א{_N}ר?{_N}י{_N}י{_N}ן")),
    ("exit",     re.compile(f"א{_N}ב{_N}\\s*\\)|ג{_N}י{_N}י{_N}ט\\s+א{_N}ר?{_N}ו{_N}י{_N}ס")),
    ("delivery", re.compile(f"ב{_N}ע{_N}ז|ז{_N}י{_N}נ{_N}ג{_N}ט|פֿ?{_N}א{_N}ר\\s+ז{_N}י{_N}ך|^\\(\\s*צ{_N}ו{_N}\\s+\\S")),
    ("location", re.compile(f"א{_N}י{_N}ן\\s+(פֿ?{_N}ע{_N}נ{_N}ס{_N}ט{_N}ע{_N}ר|ט{_N}ה{_N}י{_N}ר)")),
]


def stage_type(text: str) -> str:
    for kind, rx in STAGE_TYPE_HINTS:
        if rx.search(text):
            return kind
    return "business"


def build_name_matcher(cast_dict_path: Path):
    """Return list of (compiled regex matching name at line start, xmlid).

    Tolerates common Yiddish spelling variants in the OCR'd line:
      - apostrophe in the canonical name is optional
      - א and ע treated as interchangeable
      - any consonant may have nikud after it
    """
    if not cast_dict_path.exists():
        return []
    d = json.loads(cast_dict_path.read_text())
    out = []
    for xmlid, info in d.get("roles", {}).items():
        forms = [info["bare"]] + list(info.get("prefix_variants", []))
        for raw in forms:
            bare = strip_nikud(raw).replace("'", "")
            pieces = []
            for c in bare:
                if c in "אע":
                    pieces.append("[אע]" + NIKUD.pattern + "*")
                elif c == " ":
                    pieces.append(r"\s+")
                else:
                    pieces.append(re.escape(c) + NIKUD.pattern + "*" + r"'?")
            pat = "".join(pieces)
            # Allow an optional parenthesized stage cue around the name
            # (e.g. `שמואל (לויפט):` or `(שטעהט אויף) שמואל:`).
            # `find_paren_spans` on the full line emits the stage span; the
            # speaker span (group 1) stays scoped to the name itself.
            paren_opt = r"(?:\([^)]*\)\s*)*"
            rx = re.compile(r"^\s*" + paren_opt + r"(" + pat + r")\s*"
                            + paren_opt + r"[:׃]")
            out.append((rx, xmlid, raw))
    # Longer first so multi-word names match before short prefixes.
    out.sort(key=lambda t: -len(t[2]))
    return out


def find_speaker(line: str, matchers):
    """Return (offset, length, xmlid) or None."""
    for rx, xmlid, _ in matchers:
        m = rx.match(line)
        if m:
            return (m.start(1), len(m.group(1)), xmlid)
    return None


HEADING_ACT_RX = re.compile(
    r"^\s*((?:" + "|".join(re.escape(w) for w in HE_ORDINAL_ACT) + r")\s+אַ?קְ?ט)\s*\.?\s*$"
)
HEADING_SCENE_RX = re.compile(
    r"^\s*(סְ?צֶ?ענֶ?ע\s+(?:" + "|".join(re.escape(w) for w in HE_ORDINAL_ACT) + r"))\s*\.?\s*$"
)


def find_heading(line: str):
    # Delegates to annotation.schema, which also handles Roman numerals on
    # either side of `אקט` and a trailing parenthetical. See parse_act_heading.
    n = parse_act_heading(line)
    if n:
        return ("act", n, 0, len(line.rstrip()))
    n = parse_scene_heading(line)
    if n:
        return ("scene", n, 0, len(line.rstrip()))
    return None


def find_paren_spans(line: str, prev_open: bool):
    """Return (spans, new_open).
    spans: list of (offset, length, fragment_text).
    Handles continuation lines (prev_open=True) by tagging from start up to ')'
    (or whole line if no close)."""
    spans = []
    i = 0
    open_ = prev_open
    start = 0 if prev_open else None
    while i < len(line):
        ch = line[i]
        if ch == "(" and not open_:
            open_ = True
            start = i
        elif ch == ")" and open_:
            # include the close paren and trailing dot if present
            end = i + 1
            if end < len(line) and line[end] == ".":
                end += 1
            spans.append((start, end - start, line[start:end]))
            open_ = False
            start = None
            i = end
            continue
        i += 1
    if open_ and start is not None:
        # no close on this line — tag to end of line
        spans.append((start, len(line) - start, line[start:]))
    return spans, open_


def classify_page(lines, matchers, cast_dict_path):
    """Return 'titlePage' | 'castList' | 'body'."""
    nonempty = [l for l in lines if l.strip()]
    if not nonempty:
        return "titlePage"
    # If few short lines and no speakers/parens → titlePage.
    has_speaker = any(find_speaker(l, matchers) for l in nonempty)
    if len(nonempty) < 6 and not has_speaker:
        return "titlePage"
    # castList: many lines BEGIN with a cast name (without ":" — that's a speaker).
    # Song lyrics may *mention* cast names mid-line; that should not trigger.
    if cast_dict_path and cast_dict_path.exists():
        d = json.loads(cast_dict_path.read_text())
        bares = [info["bare"] for info in d.get("roles", {}).values()]
        hits = 0
        for l in nonempty[:20]:
            bare_l = strip_nikud(l).lstrip()
            if ":" in l[:20]:
                continue
            if any(b and bare_l.startswith(b) for b in bares):
                hits += 1
        if hits >= 3 and not has_speaker:
            return "castList"
    return "body"


def annotate_page_body(lines, matchers):
    """Return list of {line_idx, spans} for body pages."""
    out = []
    paren_open = False
    prev_was_heading = False
    for i, l in enumerate(lines):
        spans = []
        # Heading
        h = find_heading(l)
        if h:
            kind, n, off, ln = h
            spans.append({"tag": "heading", "offset": off, "length": ln,
                          "attrs": {"type": kind, "n": str(n)}})
            out.append({"line_idx": i, "spans": spans})
            prev_was_heading = True
            continue
        # B6 (Noa 2026-06-14): line immediately following an act/scene heading,
        # not parenthesized and not a speaker turn → whole-line stage{type:setting}
        # describing the locus of the scene. Guards against empty/whitespace lines.
        if prev_was_heading and not paren_open and l.strip():
            sp_check = find_speaker(l, matchers)
            has_paren = "(" in l
            if not sp_check and not has_paren:
                spans.append({"tag": "stage", "offset": 0,
                              "length": len(l.rstrip()),
                              "attrs": {"type": "setting"}})
                out.append({"line_idx": i, "spans": spans})
                prev_was_heading = False
                continue
        prev_was_heading = False
        # Speaker
        sp = find_speaker(l, matchers) if not paren_open else None
        if sp:
            off, ln, xmlid = sp
            spans.append({"tag": "speaker", "offset": off, "length": ln,
                          "attrs": {"xmlid": xmlid}})
        # Stage / parens
        paren_spans, paren_open = find_paren_spans(l, paren_open)
        for off, ln, frag in paren_spans:
            spans.append({"tag": "stage", "offset": off, "length": ln,
                          "attrs": {"type": stage_type(frag)}})
        out.append({"line_idx": i, "spans": spans})
    return out


def annotate_page(play, page_num):
    """Build annotation dict for one page."""
    cast = REPO_ROOT / "data" / play / "cast_dict.json"
    matchers = build_name_matcher(cast)
    payload = dump_lines(play, page_num)
    line_texts = [l["text"] for l in payload["lines"]]

    ptype = classify_page(line_texts, matchers, cast)
    if ptype == "body":
        lines_out = annotate_page_body(line_texts, matchers)
    else:
        # title/castList: leave empty spans (castList annotated manually; titles need no spans)
        lines_out = [{"line_idx": i, "spans": []} for i in range(len(line_texts))]

    return {"page_type": ptype, "lines": lines_out}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", required=True)
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=10**9)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="re-annotate even if already in page_annotated/")
    ap.add_argument("--no-songs", action="store_true",
                    help="skip the song (l/head/lg) post-pass")
    ap.add_argument("--allow-stale-source", action="store_true",
                    help="overwrite page_annotated/ even when page/ is older "
                         "than the newest Transkribus pull (dangerous)")
    ap.add_argument("--redo-manual", action="store_true",
                    help="also regenerate castList/title pages that already "
                         "exist in page_annotated/ (they are hand-annotated)")
    args = ap.parse_args()

    annotated_dir = REPO_ROOT / "data" / args.play / "page_annotated"
    already = set()
    for f in annotated_dir.glob("*.xml"):
        already.add(f.name)

    pages = [(n, src) for n, src in list_pages(args.play) if args.start <= n <= args.end]
    print(f"plan: {len(pages)} pages in range [{args.start},{args.end}]")
    totals = {"body": 0, "castList": 0, "titlePage": 0, "skipped_already": 0,
              "skipped_manual": 0, "skipped_stale": 0, "applied": 0}
    for n, src in pages:
        if not args.force and src.name in already:
            totals["skipped_already"] += 1
            continue
        try:
            ann = annotate_page(args.play, n)
        except Exception as e:
            print(f"  p.{n}: ERROR {e}")
            continue
        n_spans = sum(len(l["spans"]) for l in ann["lines"])
        totals[ann["page_type"]] += 1
        print(f"  p.{n:3d} type={ann['page_type']:10s} spans={n_spans}")
        # castList/title pages carry no generated spans — regenerating one only
        # replaces a hand-annotated file with a bare structure tag. This runs
        # even under --force, which is how Noa's 2026-06-24 castLists were lost.
        if ann["page_type"] != "body" and src.name in already and not args.redo_manual:
            totals["skipped_manual"] += 1
            print(f"        skip: non-body page already annotated by hand")
            continue
        if args.dry_run:
            continue
        try:
            result = apply_annotation(args.play, n, ann,
                                      allow_stale_source=args.allow_stale_source)
        except StaleSourceError as e:
            totals["skipped_stale"] += 1
            print(f"        STALE SOURCE, not written: {e}")
            continue
        totals["applied"] += 1
    print("totals:", totals)

    if args.dry_run or args.no_songs:
        return
    # Song post-pass: detect l/head/lg across all annotated pages of the play.
    from annotation.annotate_songs import process_play as _annotate_songs
    song_stats = _annotate_songs(args.play, dry_run=False)
    print(f"songs: {song_stats}")


if __name__ == "__main__":
    main()
