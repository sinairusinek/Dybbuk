"""Push locally-generated annotation onto the LIVE top transcript, additively.

Sinai 2026-07-20. `auto_annotate` regenerates `page_annotated/` from `page/`,
which carries no annotation — so the generated file contains ONLY what the
heuristics derived. Pushing it wholesale (push-dir) replaces the live
transcript and drops anything the heuristics do not reproduce. Concretely, on
Das Yudishe Kind that would have destroyed the `fw{type:pageNum}` spans from
the 2026-07-03 sweep: heuristic_annotate matches folio lines and deliberately
emits NO spans for them, so a wholesale push silently un-tags every page
number. This is the same failure that wiped Noa's 32 verse spans on BasSheva
p8 — see restore_lost_l_spans.

So: fetch the live top, ADD every generated span the live line does not
already carry, and push that. Never remove, never rewrite. A span is
"already carried" if a span with the same (tag, offset, length) is present —
so re-running is a no-op.

  python3.11 -m annotation.merge_push_annotation --play <folder> --start 13 --end 60 --dry-run
  python3.11 -m annotation.merge_push_annotation --play <folder> --start 13 --end 60 --push
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom, validate_span
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NS = f"{{{PAGE_NS}}}"


def line_text(el) -> str:
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def key(tag, a) -> tuple:
    return (tag, str(a.get("offset", "")), str(a.get("length", "")))


def merge_page(live_root, gen_root) -> list[str]:
    """Add spans present in gen but not in live. Returns human-readable changes."""
    gen = {}
    for el in gen_root.iter(f"{NS}TextLine"):
        gen[el.get("id")] = parse_custom(el.get("custom") or "")
    changes = []
    for el in live_root.iter(f"{NS}TextLine"):
        want = gen.get(el.get("id"))
        if not want:
            continue
        txt = line_text(el)
        entries = parse_custom(el.get("custom") or "")
        have = {key(t, a) for t, a in entries}
        added = False
        for tag, a in want:
            if tag == "readingOrder" or key(tag, a) in have:
                continue
            # Only one span of a given tag may cover a given range, and the
            # live line may already carry a DIFFERENT span of the same tag at
            # another offset (e.g. a speaker the sweeps added). Adding a second
            # overlapping one would be a defect, so skip on overlap.
            try:
                off, ln = int(a.get("offset", 0)), int(a.get("length", 0))
            except (TypeError, ValueError):
                continue
            if off + ln > len(txt):
                changes.append(f"  ! {el.get('id')}: {tag} span past end of live text — skipped")
                continue
            clash = False
            for t2, a2 in entries:
                if t2 != tag:
                    continue
                try:
                    o2, l2 = int(a2.get("offset", 0)), int(a2.get("length", 0))
                except (TypeError, ValueError):
                    continue
                if not (off + ln <= o2 or off >= o2 + l2):
                    clash = True
                    break
            if clash:
                changes.append(f"  ! {el.get('id')}: {tag} overlaps an existing {tag} — kept live")
                continue
            # validate_span takes offset/length as siblings of `attrs`, not
            # inside it — passing them in `attrs` makes every span fail as
            # "unknown attrs on stage: ['length', 'offset']".
            err = validate_span(txt, {
                "tag": tag, "offset": off, "length": ln,
                "attrs": {k: v for k, v in a.items()
                          if k not in ("offset", "length")}})
            if err:
                changes.append(f"  ! {el.get('id')}: {tag} invalid ({err}) — skipped")
                continue
            entries.append((tag, dict(a)))
            added = True
            changes.append(f"  + {tag}{{{a.get('type') or a.get('xmlid') or ''}}} "
                           f"{txt.strip()[:34]!r}")
        if added:
            el.set("custom", serialize_custom(entries))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", required=True)
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=10**9)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()

    doc = load_doc_ids().get(args.play)
    if doc is None:
        print(f"no doc id for {args.play}"); return 1
    src_dir = REPO / "data" / args.play / "page"
    gen_dir = REPO / "data" / args.play / "page_annotated"
    client = TrpClient.from_env(); client.login()
    n_span = n_page = 0

    for src in sorted(src_dir.glob("*.xml")):
        page = int(src.name.split("_")[0])
        if not (args.start <= page <= args.end):
            continue
        gen_path = gen_dir / src.name
        if not gen_path.exists():
            print(f"p{page}: no generated file {gen_path.name} — skip"); continue
        tsid, owner, xml = top_transcript(client, doc, page)
        if xml is None:
            print(f"p{page}: no server transcript — skip"); continue
        live = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        gen = etree.parse(str(gen_path)).getroot()
        changes = merge_page(live, gen)
        adds = [c for c in changes if c.startswith("  +")]
        if not adds:
            continue
        n_span += len(adds); n_page += 1
        print(f"\np{page} ({len(adds)} spans, top: {owner.split('@')[0]})")
        for c in changes[:6]:
            print(c)
        if len(changes) > 6:
            print(f"  … {len(changes) - 6} more")
        if args.push:
            client.push_transcript(
                COL, doc, page, etree.tostring(live, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note="YiDraCor annotate pass (additive merge onto live)",
                tool_name="YiDraCor-annotation-pipeline")
            print(f"  → pushed (parent {tsid})")

    print(f"\n{'PUSHED' if args.push else 'DRY RUN — nothing written'}: "
          f"{n_span} spans across {n_page} pages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
