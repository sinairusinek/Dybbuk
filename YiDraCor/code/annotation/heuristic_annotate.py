"""Per-play heuristic annotator: read cast_dict.json, dump pages, emit JSON.

Generalization of the one-off MMK body annotator from 2026-05-31. Produces
PAGE-XML annotation JSON suitable for `annotate_pages apply` — same shape as
the LLM would, but driven by:

  - **speaker**: line begins with a cast name (any of `bare` / `form` /
    `prefix_variants` from cast_dict.json, nikud-stripped) + ':'
  - **stage**: parenthesized regions (with multi-line `(` … `)` continuation
    handling); type assigned by the same lexicon used by
    `auto_resolve_flags.stage_lexicon_span` — single emotion adverb →
    delivery, trailing `אב` → exit (modal-guarded), אויפטריט/קומט אריין →
    entrance, פערוואנדלונג/פאָרהאַנג → setting, צימער → setting; default `business`
  - **heading**: act ordinals (`ערשטער אקט`, …) → `heading{type:act,n:N}`;
    `סצענע X` → scene
  - **trailer**: line starts with `ענדע` → `<trailer>` (whole-line)
  - **folio-only**: digit-only lines → no spans

By design this annotator only writes annotations on lines where it is
confident. Unmatched lines (no speaker, no parens, no heading) get empty
spans — those will be picked up by the linter and the
`suggest_cast_variants` proposal step. This is the bulk-first-pass tool;
LLM in-session refinement or RA editing follows.

Use AFTER `auto_annotate.py` (titlePage/castList classification) or with an
already-prepared castList annotation, since this script ONLY does body
pages.

Usage:
  python -m annotation.heuristic_annotate --play <folder>
  python -m annotation.heuristic_annotate --play <folder> --pages 5-22
  python -m annotation.heuristic_annotate --play <folder> --pages 5-22 --dry-run
  python -m annotation.heuristic_annotate --play <folder> --overwrite  # ignore existing page_annotated/
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.lint_pages import REPO

NIKUD = re.compile(r"[֑-ׇ]")
_HEB_TOKEN = re.compile(r"[א-תװ-ײ']+")

# --- stage classifier (mirrors auto_resolve_flags.stage_lexicon_span) ---

EMOTION_ADVERBS = {
    "בייז", "שרייט", "ברוגז", "שפעטיש", "זיגנענד", "אפארט",
    "בעגייסטערט", "פערקלעהרט", "פערקלערט", "פערשעמט",
    "לאכענד", "ערנסט", "שטיל",
    # Sinai 2026-06-24 general correction #5
    "וויינט", "לאכט", "וויינענד",
}
_COMPOUND_ACTION = {"זעצט", "גיט", "נעמט", "קושט", "שאקעלט", "שטעלט",
                    "הויבט", "דרעהט", "ציהט", "פאלט"}
_MODAL_BEFORE_AB = {"וויל", "ויל", "וויעל",
                    "זאל", "זאָל",
                    "מוז", "מוּז",
                    "דארף", "דאַרף", "דארפט", "דאַרפט",
                    "קען", "קעהן",
                    "געהט", "גייט", "גיט"}


def strip_nikud(s: str) -> str:
    return NIKUD.sub("", s or "")


def classify_stage(span_text: str) -> str:
    sk = strip_nikud(span_text)
    tokens = _HEB_TOKEN.findall(sk)
    if not tokens:
        return "business"
    token_set = set(tokens)
    short = len(tokens) <= 6
    no_period = "." not in span_text
    if len(tokens) == 1 and tokens[0] in EMOTION_ADVERBS:
        return "delivery"
    has_action = bool(token_set & _COMPOUND_ACTION)
    has_ersheynt = bool(token_set & {"ערשיינט", "ערשײנט", "ערשיינען", "ערשײנען"})
    has_oyftrit = tokens[0] in {"אויפטריט", "אויפטרעטען", "אויפטרעטן"}
    has_arayn_kumt = "אריין" in token_set and bool(token_set & {"קומט", "קומען"})
    has_entrance_cue = has_ersheynt or has_oyftrit or has_arayn_kumt
    has_ab = "אב" in token_set or "אבּ" in token_set
    # Noa 2026-06-24 B9: entrance cue + exit cue in same direction → mixed.
    if short and no_period and has_ersheynt and has_ab:
        return "mixed"
    # TEI-principled multi-token typing (Sinai+Noa 2026-06-18 ratified):
    # compound directions emit space-separated `@type` per TEI P5 spec.
    if short and no_period and tokens[-1] in {"אב", "אבּ"}:
        prev = tokens[-2] if len(tokens) >= 2 else ""
        if prev in _MODAL_BEFORE_AB:
            return "business"
        # exit + extra action in same direction → "exit business".
        if len(tokens) > 2:
            return "exit business"
        return "exit"
    if short and no_period and has_ab and len(tokens) >= 2:
        # אב not at end but present + another token → "exit business" (e.g. "אב, שטורם")
        return "exit business"
    if len(tokens) <= 5:
        sk_norm = sk.replace("פֿ", "פ")
        if ("פערוואנדלונג" in sk_norm
                or "פערווענלונג" in sk_norm
                or "פערוואנדעלונג" in sk_norm
                or "פערענדלונג" in sk_norm  # Sinai 2026-06-24 general correction #2
                or "פארהאנג" in sk_norm
                or "פאָרהאַנג" in span_text):
            return "setting"
    if not (short and no_period):
        # ערשיינט inside a longer/punctuated span is still entrance / entrance+business.
        if has_ersheynt:
            return "entrance business" if has_action or len(tokens) > 3 else "entrance"
        return "business"
    if has_ersheynt and (has_action or len(tokens) >= 4):
        return "entrance business"
    if has_entrance_cue and has_action:
        return "entrance business"
    if has_entrance_cue:
        return "entrance"
    if "צימער" in token_set and len(tokens) <= 5:
        return "setting"
    return "business"


# --- speaker / heading / trailer detectors ---

ACT_HEAD = re.compile(
    r"^\s*(ערשטער|צווייטער|דריטער|פערטער|פינפטער|זעקסטער|זיבעטער)\s+אקט\.?\s*$"
)
ACT_ORD = {"ערשטער": "1", "צווייטער": "2", "דריטער": "3", "פערטער": "4",
           "פינפטער": "5", "זעקסטער": "6", "זיבעטער": "7"}
SCENE_HEAD = re.compile(r"^\s*סצענע\s+(\S+)\.?\s*$")
TRAILER_RE = re.compile(r"^\s*\(?\s*ענדע\b")
FOLIO_RE = re.compile(r"^\s*[\-–—\s]*\d{1,4}[\-–—\s]*$")


def load_speaker_lookup(play: str) -> dict[str, str]:
    """{nikud-stripped surface → xmlid} from cast_dict.json (bare + form + prefix_variants)."""
    f = REPO / "data" / play / "cast_dict.json"
    if not f.exists():
        print(f"WARNING: no cast_dict.json for {play} — speaker detection disabled.",
              file=sys.stderr)
        return {}
    d = json.loads(f.read_text(encoding="utf-8"))
    lookup: dict[str, str] = {}
    for xmlid, info in d.get("roles", {}).items():
        surfaces = [info.get("bare", ""), info.get("form", "")]
        surfaces += info.get("prefix_variants") or []
        for s in surfaces:
            k = strip_nikud(s).strip()
            if k and k not in lookup:
                lookup[k] = xmlid
    return lookup


def find_speaker(text: str, lookup: dict[str, str]):
    """Return (offset, length, xmlid) if line starts with `<cast>:`, else None.

    Tolerates a parenthesized stage cue around the name
    (e.g. `שמואל (לויפט): היי` or `(שטעהט אויף) שמואל: היי`):
    the returned offset/length cover only the name; the parens get picked up
    separately as `stage` spans by the caller's paren scan.
    """
    m = re.match(r"\s*([^:]{1,60}?)\s*:", text)
    if not m:
        return None
    prefix = m.group(1)
    label = strip_nikud(prefix).strip()
    if label in lookup:
        return (m.start(1), m.end(1) - m.start(1), lookup[label])
    # Fallback: walk the prefix, skipping `(...)` groups, and look for a
    # contiguous non-paren run whose nikud-stripped form is in `lookup`.
    i = 0; n = len(prefix); base = m.start(1)
    while i < n:
        if prefix[i] == "(":
            j = prefix.find(")", i + 1)
            i = (j + 1) if j >= 0 else n
            continue
        if prefix[i].isspace():
            i += 1; continue
        j = i
        while j < n and prefix[j] != "(":
            j += 1
        seg = prefix[i:j].rstrip()
        if seg:
            key = strip_nikud(seg).strip()
            if key in lookup:
                return (base + i, len(seg), lookup[key])
        i = j
    return None


def find_paren_spans(text: str, open_state: bool):
    """Return (spans, new_open_state). spans = list of (offset, length, type)."""
    spans = []
    i = 0; n = len(text)
    if open_state:
        close = text.find(")")
        if close >= 0:
            spans.append((0, close + 1, classify_stage(text[:close + 1])))
            open_state = False
            i = close + 1
        else:
            spans.append((0, n, classify_stage(text)))
            return spans, True
    while i < n:
        op = text.find("(", i)
        if op < 0:
            break
        cl = text.find(")", op + 1)
        if cl >= 0:
            spans.append((op, cl - op + 1, classify_stage(text[op:cl + 1])))
            i = cl + 1
        else:
            spans.append((op, n - op, classify_stage(text[op:])))
            open_state = True
            break
    return spans, open_state


def annotate_page(dump: dict, lookup: dict[str, str]) -> dict:
    out_lines = []
    open_state = False
    expect_setting_next = False  # Noa 2026-06-14: line after act/scene heading
                                 # establishing the scene location → setting
    for L in dump["lines"]:
        idx = L["line_idx"]; text = L["text"]
        spans: list[dict] = []
        sk = strip_nikud(text)
        # heading: act
        m = ACT_HEAD.match(sk)
        if m:
            spans.append({"tag": "heading", "offset": 0, "length": len(text.rstrip()),
                          "attrs": {"type": "act", "n": ACT_ORD[m.group(1)]}})
            expect_setting_next = True
            out_lines.append({"line_idx": idx, "spans": spans}); continue
        # heading: scene
        m = SCENE_HEAD.match(sk)
        if m:
            spans.append({"tag": "heading", "offset": 0, "length": len(text.rstrip()),
                          "attrs": {"type": "scene", "n": m.group(1)}})
            expect_setting_next = True
            out_lines.append({"line_idx": idx, "spans": spans}); continue
        # trailer
        if TRAILER_RE.match(sk):
            spans.append({"tag": "trailer", "offset": 0, "length": len(text.rstrip()),
                          "attrs": {}})
            expect_setting_next = False
            out_lines.append({"line_idx": idx, "spans": spans}); continue
        # folio-only — pass through without consuming the setting expectation
        if FOLIO_RE.fullmatch(text):
            out_lines.append({"line_idx": idx, "spans": []}); continue
        # post-act/scene-header setting: the next eligible line is the
        # scene-location description ("זאַל ווילניצקי", "איינע וואלדגעגענד…").
        # Eligible = not a speaker turn, not parenthesized, non-empty.
        if expect_setting_next and text.strip():
            sp = find_speaker(text, lookup)
            stripped = text.strip()
            is_paren = stripped.startswith("(")
            if not sp and not is_paren:
                spans.append({"tag": "stage", "offset": 0, "length": len(text.rstrip()),
                              "attrs": {"type": "setting"}})
                expect_setting_next = False
                out_lines.append({"line_idx": idx, "spans": spans}); continue
            # speaker turn or paren stage on this line — give up on the
            # post-header setting expectation rather than override.
            expect_setting_next = False
        # speaker + stages
        sp = find_speaker(text, lookup)
        if sp:
            spans.append({"tag": "speaker", "offset": sp[0], "length": sp[1],
                          "attrs": {"xmlid": sp[2]}})
            colon = text.find(":", sp[0] + sp[1])
            # Pre-colon parens (stage cue between speaker and `:`, or before
            # the name). Scope to text[:colon] so we don't double-count the
            # post-colon scan.
            pre_end = colon if colon >= 0 else (sp[0] + sp[1])
            pre_spans, _ = find_paren_spans(text[:pre_end], False)
            for off, ln, ty in pre_spans:
                if off + ln <= sp[0] or off >= sp[0] + sp[1]:
                    spans.append({"tag": "stage", "offset": off, "length": ln,
                                  "attrs": {"type": ty}})
            scan_from = colon + 1 if colon >= 0 else sp[0] + sp[1]
            paren_spans, open_state = find_paren_spans(text[scan_from:], open_state)
            for off, ln, ty in paren_spans:
                spans.append({"tag": "stage", "offset": scan_from + off,
                              "length": ln, "attrs": {"type": ty}})
        else:
            paren_spans, open_state = find_paren_spans(text, open_state)
            for off, ln, ty in paren_spans:
                spans.append({"tag": "stage", "offset": off, "length": ln,
                              "attrs": {"type": ty}})
        out_lines.append({"line_idx": idx, "spans": spans})
    return {"page_type": "body", "lines": out_lines}


def list_source_pages(play: str) -> list[int]:
    src = REPO / "data" / play / "page_final"
    if not src.is_dir():
        src = REPO / "data" / play / "page"
    if not src.is_dir():
        return []
    pages: list[int] = []
    for p in sorted(src.glob("*.xml")):
        m = re.match(r"(\d{4})_", p.name)
        if m:
            pages.append(int(m.group(1)))
    return sorted(set(pages))


def already_annotated(play: str, page: int) -> bool:
    ann = REPO / "data" / play / "page_annotated"
    if not ann.is_dir():
        return False
    return bool(list(ann.glob(f"{page:04d}_*.xml")))


def parse_pages(spec: str | None, all_pages: list[int]) -> list[int]:
    if not spec:
        return all_pages
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return sorted(p for p in out if p in set(all_pages))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--play", required=True, help="edition folder name")
    ap.add_argument("--pages", help="page range, e.g. '5-22' or '5,7,10-12' (default: all source pages)")
    ap.add_argument("--overwrite", action="store_true",
                    help="re-annotate pages that already have a page_annotated/ entry")
    ap.add_argument("--dry-run", action="store_true", help="don't write/apply, just preview span counts")
    args = ap.parse_args()

    lookup = load_speaker_lookup(args.play)
    if not lookup:
        print("(Speaker detection disabled — only stage/heading/trailer will be tagged.)")
    all_pages = list_source_pages(args.play)
    if not all_pages:
        print(f"ERROR: no source pages in data/{args.play}/page_final/ or page/", file=sys.stderr)
        return 2
    pages = parse_pages(args.pages, all_pages)

    n_skip = n_done = 0
    for page in pages:
        if not args.overwrite and already_annotated(args.play, page):
            n_skip += 1; continue
        # dump
        try:
            r = subprocess.run(
                [sys.executable, "-m", "annotation.annotate_pages", "dump",
                 "--play", args.play, "--page", str(page)],
                check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"  p{page}: dump FAILED: {e.stderr[:200]}", file=sys.stderr); continue
        dump = json.loads(r.stdout)
        ann = annotate_page(dump, lookup)
        n_spans = sum(len(L["spans"]) for L in ann["lines"])
        if args.dry_run:
            print(f"  p{page:>3}: {n_spans:>3} spans (dry-run)")
            n_done += 1; continue
        tmp = Path(f"/tmp/heuristic_{args.play}_p{page}.json")
        tmp.write_text(json.dumps(ann, ensure_ascii=False, indent=2))
        r = subprocess.run(
            [sys.executable, "-m", "annotation.annotate_pages", "apply",
             "--play", args.play, "--page", str(page), "--json", str(tmp)],
            capture_output=True, text=True,
        )
        ok = "errors=0" in r.stderr
        print(f"  p{page:>3}: {n_spans:>3} spans applied" + ("" if ok else "  ⚠ check errors"))
        n_done += 1

    print(f"\nDone: {n_done} page(s) annotated, {n_skip} skipped (already annotated; pass --overwrite to redo).")
    if not args.dry_run and n_done:
        print("Next: push to Transkribus, then run apply_collective_speakers, "
              "auto_resolve_flags, lint_pages, and suggest_cast_variants.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
