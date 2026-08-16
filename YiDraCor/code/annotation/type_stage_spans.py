"""Type the untyped `stage` spans on the manuscript track, by rule.

`schema.validate_span` requires `@type` on every stage span ("no bare stage"),
and the 2026-08-16 survey found ~2,070 of 2,114 manuscript stage spans untyped
— Emigration, Ben HaDor and Meshumed at literally zero.

This deliberately does NOT introduce a new typing model. The rules in
`auto_resolve_flags` are the calibrated ones — built against RA corrections
(2026-05-31), revised by Noa's multi-token decision (2026-06-18), the PI's
modal-before-אב guard, and the `exit entrance` ruling (2026-07-20) — and per
the standing decision that rule-based corrections override the LLM, they are
imported and applied here rather than reimplemented:

  stage_lexicon        whole-line scene-boundary cues (פערוואנדלונג, פארהאנג
                       → setting; ענדע → trailer; עפילאג → epilog)
  stage_lexicon_span   span-level cues (single-token emotion adverb → delivery;
                       ערשיינט → entrance; trailing אב → exit, with the modal
                       guard; compound forms → multi-token @type)
  apply_opening_setting  positional: the parenthesised direction opening an
                       act or scene is a `setting` regardless of vocabulary

Only spans with NO type are touched; an existing type is never overwritten.
Whatever the rules decline stays untyped and goes to the review sheet, where a
human (or a bulk LLM pass) decides — the rules are precision-first by design
and are not expected to cover parenthesised action directions.

Operates on the local `page_annotated/` mirror.

  python3.11 -m annotation.type_stage_spans --dry-run
  python3.11 -m annotation.type_stage_spans --apply --report /tmp/stage.tsv
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
    PAGE_NS, parse_custom, serialize_custom, _validate_stage_type,
    parse_act_heading, parse_scene_heading,
)
from annotation.auto_resolve_flags import (  # noqa: E402
    stage_lexicon, stage_lexicon_span, apply_opening_setting,
)
from annotation.review_links import page_url  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"

# Manuscript folders plus Meshumed (the one MS play with a legacy folder name).
MS_GLOB = "MS_*"
MS_EXTRA = ["Lateiner_Meshumed"]


# --- prompt-book musical apparatus (manuscript track only) ------------------
# The MS plays are souffleur's notebooks, and their Latin-script spans are the
# prompter's own production apparatus, historical and formulaic:
#   N / No / NO / N=1 / N 5 / No=9 / N-10   Nummer — which musical number
#   Return / Ret / Returnel / Returnelle    Ritornell (ritornello), not English
#                                           "return"; spelled a dozen ways
#   Terzett / Duett / Musik / Tanz          the musical form being cued
#   Bis / bis                               the repeat mark, Latin-script twin
#                                           of the printed (ביס)
#   Vorhang                                 curtain — German twin of פארהאנג,
#                                           already `setting` in stage_lexicon
#   Trompetenschall                         a sound effect, not music
# Musical performance instructions are `delivery` corpus-wide (Sinai
# 2026-07-21, when `repeat` was retired for exactly this reason), so the
# apparatus types as `delivery`; the curtain stays `setting` to match its
# Yiddish counterpart and the sound effect is `business`.
_MS_APPARATUS = [
    (re.compile(r'^\s*"?vorhang\b', re.I), "setting"),
    (re.compile(r'^\s*"?trompetenschall', re.I), "business"),
    (re.compile(r'^\s*"?bis\b', re.I), "delivery"),
    # Nummer: bare N / No / NO / Nr, and N=1, N 5, No=9, N-10, N3.
    (re.compile(r'^\s*"?n(?:[or]\b|\b|\s*[=.\-]?\s*\d)', re.I), "delivery"),
    # Ritornell, in every attested spelling: Ret, Retur, Return, Returned,
    # Returnel(:), Returnell, Returnelle, Retunel. A bare `ret` prefix is safe
    # here because this runs only on Latin-script spans in the MS plays.
    (re.compile(r'\bret[a-z]*', re.I), "delivery"),
    (re.compile(r'\b(terzett|duett|musik|tanz|arie|aro)\b', re.I), "delivery"),
]


def ms_apparatus_type(span_text: str):
    """Type a prompt-book musical/production cue, else None.

    Latin-script only: a Yiddish direction that merely contains a Latin
    substring must not be caught here, it belongs to the calibrated lexicon.
    """
    t = (span_text or "").strip()
    if not t or re.search(r"[א-ת]", t):
        return None
    for rx, typ in _MS_APPARATUS:
        if rx.search(t):
            return typ
    return None


def _line_text(tl) -> str:
    for te in tl.findall(f"{NS}TextEquiv"):
        u = te.find(f"{NS}Unicode")
        if u is not None:
            return u.text or ""
    return ""


def type_headings(root, stats: Counter) -> None:
    """Give the `heading` spans their @type.

    All 142 manuscript headings carry no type, which `validate_span` rejects
    (`heading.type must be one of {act, scene, epilog}`) and which leaves
    build_tei with no act/scene divisions at all. `schema.parse_act_heading`
    already handles every form in this corpus — word ordinals and Roman
    numerals on either side of אקט, trailing period, trailing parenthetical —
    so it decides here too. This must run BEFORE the stage pass:
    apply_opening_setting keys off `heading{type:act|scene}`, so with untyped
    headings the positional `setting` rule can never fire.
    """
    for tl in root.iter(f"{NS}TextLine"):
        ents = parse_custom(tl.get("custom") or "")
        if not any(t == "heading" for t, _ in ents):
            continue
        text = _line_text(tl)
        out, touched = [], False
        for tag, a in ents:
            if tag != "heading" or a.get("type"):
                out.append((tag, a))
                continue
            span = text
            try:
                off, ln = int(a["offset"]), int(a["length"])
                span = text[off:off + ln]
            except (KeyError, ValueError):
                pass
            n_act = parse_act_heading(span) or parse_act_heading(text)
            n_scene = parse_scene_heading(span) or parse_scene_heading(text)
            a = dict(a)
            if n_act:
                a["type"], a["n"] = "act", str(n_act)
                stats["heading_act"] += 1
            elif n_scene:
                a["type"], a["n"] = "scene", str(n_scene)
                stats["heading_scene"] += 1
            else:
                stats["heading_untyped"] += 1
                out.append((tag, a))
                continue
            touched = True
            out.append((tag, a))
        if touched:
            tl.set("custom", serialize_custom(out))


def process_page(path: Path, apply: bool):
    tree = etree.parse(str(path))
    root = tree.getroot()
    stats = Counter()
    rows = []

    type_headings(root, stats)

    # Positional rule: it retypes only untyped/`business` opening spans.
    for _ in apply_opening_setting(root):
        stats["opening_setting"] += 1

    for tl in root.iter(f"{NS}TextLine"):
        ents = parse_custom(tl.get("custom") or "")
        if not any(t == "stage" for t, _ in ents):
            continue
        text = _line_text(tl)
        out, touched = [], False
        for tag, a in ents:
            if tag != "stage":
                out.append((tag, a))
                continue
            if a.get("type"):
                stats["already_typed"] += 1
                out.append((tag, a))
                continue
            try:
                off, ln = int(a["offset"]), int(a["length"])
                span_text = text[off:off + ln]
            except (KeyError, ValueError):
                stats["bad_span"] += 1
                out.append((tag, a))
                continue

            t = (stage_lexicon(text) or stage_lexicon_span(span_text)
                 or ms_apparatus_type(span_text))
            # stage_lexicon can return `trailer`/`epilog`, which are not stage
            # types at all but a different element — retagging those is
            # auto_resolve_flags' job on the live transcript, not ours. Leave
            # them for the review sheet rather than writing an invalid @type.
            if t and _validate_stage_type(t) is None:
                a = dict(a)
                a["type"] = t
                touched = True
                stats[f"typed_{t.replace(' ', '+')}"] += 1
                stats["typed"] += 1
            else:
                stats["untyped"] += 1
                rows.append({
                    "page": path.name,
                    "line_id": tl.get("id") or "",
                    "span": span_text.strip()[:90],
                    "line": text.strip()[:90],
                    "rule_said": t or "",
                })
            out.append((tag, a))
        if touched:
            tl.set("custom", serialize_custom(out))

    if apply and (stats["typed"] or stats["opening_setting"]
                  or stats["heading_act"] or stats["heading_scene"]):
        tree.write(str(path), encoding="utf-8", xml_declaration=True)
    return stats, rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report")
    ap.add_argument("--all-plays", action="store_true",
                    help="not just the manuscript track")
    args = ap.parse_args()

    data = REPO / "data"
    if args.all_plays:
        folders = sorted(p for p in data.iterdir() if (p / "page_annotated").is_dir())
    else:
        folders = sorted(p for p in data.glob(MS_GLOB) if (p / "page_annotated").is_dir())
        folders += [data / n for n in MS_EXTRA if (data / n / "page_annotated").is_dir()]
    if args.only:
        folders = [f for f in folders if f.name in set(args.only)]

    all_rows, grand = [], Counter()
    print(f"{'play':30} {'typed':>7} {'opening':>8} {'left':>6} {'had type':>9} "
          f"{'head':>5} {'head?':>6}")
    for folder in sorted(set(folders)):
        tot, rows = Counter(), []
        for xf in sorted((folder / "page_annotated").glob("*.xml")):
            try:
                st, rs = process_page(xf, args.apply)
            except etree.XMLSyntaxError:
                tot["parse_error"] += 1
                continue
            tot.update(st)
            for r in rs:
                r["play"] = folder.name
            rows += rs
        if not tot:
            continue
        grand.update(tot)
        all_rows += rows
        print(f"{folder.name[:30]:30} {tot['typed']:7} {tot['opening_setting']:8} "
              f"{tot['untyped']:6} {tot['already_typed']:9} "
              f"{tot['heading_act'] + tot['heading_scene']:5} "
              f"{tot['heading_untyped']:6}")
    print(f"{'TOTAL':30} {grand['typed']:7} {grand['opening_setting']:8} "
          f"{grand['untyped']:6} {grand['already_typed']:9} "
          f"{grand['heading_act'] + grand['heading_scene']:5} "
          f"{grand['heading_untyped']:6}")
    print("\nby type:", {k.replace("typed_", ""): v for k, v in sorted(grand.items())
                         if k.startswith("typed_")})
    print("APPLIED — files rewritten" if args.apply else "DRY RUN — nothing written")

    if args.report and all_rows:
        cols = ["play", "page", "transkribus_url", "line_id", "span", "line",
                "rule_said"]
        for r in all_rows:
            r["transkribus_url"] = page_url(r["play"], r["page"])
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
            w.writeheader()
            for r in all_rows:
                w.writerow({c: r.get(c, "") for c in cols})
        print(f"untyped stage spans → {args.report} ({len(all_rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
