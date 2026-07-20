"""Restore `l` (verse-line) spans that a stale-mirror push wiped off the live top.

Sinai 2026-07-20, BasSheva p8. Noa added 32 `l` spans directly in Transkribus on
2026-07-11. The local `page_annotated/` mirror was never refreshed, so it still
held the pre-Noa state (l=0). On 2026-07-19 a push built from that stale mirror
replaced the live transcript wholesale — text survived (it is the same OCR, bar
a qamatz on two `עָזריה` lines) but every `l` span was gone. The loss is
invisible to lint, which checks that spans are well-formed, never that spans
which USED to exist still do.

This walks each page's transcript history, finds the ancestor with the most `l`
spans, and re-adds to the live top any `l` span that is missing there.

Guards, because a re-add must never undo a DELIBERATE removal:
  * skips any line whose current custom carries a whole-line `stage` span —
    that is §2b of retag_musical_directions ("a whole-line stage direction is
    not a verse line"), which intentionally strips `l` from bare `ביס` and from
    `(אלע טאנצען אב)`-style lines. Without this guard the restore would fight
    the retag pass forever.
  * skips any span that no longer fits the current line text (offset+length
    past the end) — the text may have been corrected since.
  * only ADDS. Never removes or rewrites a span that is currently present.

  python3.11 -m annotation.restore_lost_l_spans --dry-run
  python3.11 -m annotation.restore_lost_l_spans --only BasSheva --push
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, COL
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NS = f"{{{PAGE_NS}}}"
MAX_HISTORY = 8          # layers back to consider; older than this predates annotation


def line_text(el) -> str:
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def l_spans(root) -> dict[str, list]:
    """{line_id: [l-span attr dicts]} for one transcript."""
    out = {}
    for el in root.iter(f"{NS}TextLine"):
        got = [a for t, a in parse_custom(el.get("custom") or "") if t == "l"]
        if got:
            out[el.get("id")] = got
    return out


def restore_page(top_root, ancestors: list) -> list[str]:
    """Re-add `l` spans present in any ancestor but missing on top. Returns changes."""
    donor: dict[str, list] = {}
    for anc in ancestors:                      # newest→oldest; first writer wins
        for lid, spans in l_spans(anc).items():
            donor.setdefault(lid, spans)
    changes = []
    for el in top_root.iter(f"{NS}TextLine"):
        lid = el.get("id")
        if lid not in donor:
            continue
        txt = line_text(el)
        entries = parse_custom(el.get("custom") or "")
        if any(t == "l" for t, _ in entries):
            continue                            # already has one — leave alone
        stripped = txt.strip()
        # §2b guard: a whole-line stage direction is not a verse line.
        if stripped and any(
                t == "stage" and int(a.get("length", 0)) >= len(stripped) - 1
                for t, a in entries):
            continue
        # fw guard: the 2026-07-03 sweep deliberately REPLACED bogus `l` spans on
        # page-number lines with `fw{type:pageNum}`. BasSheva p11 `— 7 —` still
        # has the stale `l` in its history; restoring it would reinstate a defect
        # the sweep fixed. A line is either a folio marker or a verse, never both.
        if any(t == "fw" for t, _ in entries):
            changes.append(f"{lid}: SKIP — line carries fw{{}}, not a verse line")
            continue
        for a in donor[lid]:
            try:
                off, ln = int(a.get("offset", 0)), int(a.get("length", 0))
            except (TypeError, ValueError):
                continue
            if off + ln > len(txt):
                # The stale push de-vocalized two `עָזריה` lines, so a few
                # whole-line spans now overhang by a character or two. These were
                # whole-line verse spans; clamping restores the intent exactly.
                # Anything overhanging by more than a couple of characters is a
                # real text change, not a nikud edit — leave that for a human.
                if a.get("continued") and off + ln - len(txt) <= 3 and off < len(txt):
                    ln = len(txt) - off
                    changes.append(f"clamped l({off},{ln}) on {stripped[:26]!r} "
                                   f"(text shortened since)")
                else:
                    changes.append(f"{lid}: SKIP l({off},{ln}) — past end of current "
                                   f"text (len {len(txt)})")
                    continue
            else:
                changes.append(f"restored l({off},{ln}) on {stripped[:30]!r}")
            entries.append(("l", dict(a, offset=str(off), length=str(ln))))
        el.set("custom", serialize_custom(entries))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    args = ap.parse_args()

    doc_ids = load_doc_ids()
    plays = [args.only] if args.only else sorted(doc_ids)
    client = TrpClient.from_env(); client.login()
    n_span = n_page = 0

    for play in plays:
        doc = doc_ids.get(play)
        if doc is None:
            continue
        fulldoc = client.fulldoc(COL, doc)
        hits = []
        for p in sorted(fulldoc["pageList"]["pages"], key=lambda x: x["pageNr"]):
            tr = p["tsList"]["transcripts"][:MAX_HISTORY]
            if len(tr) < 2:
                continue
            roots = []
            for t in tr:
                try:
                    roots.append(etree.fromstring(
                        client.fetch_transcript(t["url"]).encode("utf-8")))
                except Exception:
                    roots.append(None)
            if roots[0] is None:
                continue
            top, ancestors = roots[0], [r for r in roots[1:] if r is not None]
            n_top = sum(len(v) for v in l_spans(top).values())
            n_best = max([sum(len(v) for v in l_spans(r).values()) for r in ancestors]
                         or [0])
            if n_best <= n_top:
                continue
            changes = restore_page(top, ancestors)
            real = [c for c in changes if not c.startswith(("restored l", "")) or
                    c.startswith("restored l")]
            if not any(c.startswith(("restored l", "clamped l")) for c in changes):
                continue
            hits.append((p["pageNr"], tr[0], top, changes))
        if not hits:
            continue
        print(f"\n=== {play} (doc {doc}) ===")
        for page_nr, top_ts, root, changes in hits:
            added = sum(1 for c in changes if c.startswith(("restored l", "clamped l")))
            print(f"  p{page_nr}: {added} l span(s) to restore "
                  f"(top: {top_ts['userName'].split('@')[0]})")
            for c in changes:
                if c.startswith(("restored l", "clamped l")) and added <= 6:
                    print(f"      • {c}")
                elif not c.startswith(("restored l", "clamped l")):
                    print(f"      ! {c}")
            n_span += added; n_page += 1
            if args.push:
                blob = etree.tostring(root, encoding="UTF-8",
                                      xml_declaration=True, standalone=True)
                res = client.push_transcript(
                    COL, doc, page_nr, blob.decode("utf-8"),
                    parent_tsid=top_ts.get("tsId"), status=top_ts["status"],
                    note="restore l spans lost to a stale-mirror push",
                    tool_name="YiDraCor-annotation-pipeline")
                print(f"      → pushed tsId={res.get('tsId')}")
                out = REPO / "data" / play / "page_annotated"
                for f in sorted(out.glob(f"{page_nr:04d}_*.xml")):
                    f.write_bytes(blob)

    print(f"\n{'PUSHED' if args.push else 'DRY RUN — nothing written'}: "
          f"{n_span} l spans across {n_page} pages")


if __name__ == "__main__":
    main()
