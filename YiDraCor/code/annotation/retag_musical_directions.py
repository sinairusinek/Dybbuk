"""Retag the printed musical markers `(ביס)` and `רעפריין` corpus-wide.

Implements docs/proposal_musical_directions_2026-07-19.md:
  - `(ביס)`   → stage {type:repeat}   (was: business, or untagged)
  - `רעפריין` → head   (was: speaker / l / stage), keeping any lg_id

Operates on the CURRENT TOP Transkribus transcript for each page, never on the
local mirror, so no unrelated human decision is reverted. Every other span on
the line is preserved byte-for-byte.

  python3.11 -m annotation.retag_musical_directions --dry-run
  python3.11 -m annotation.retag_musical_directions --push
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

BIS_RX = re.compile(r"\(\s*ביס\s*\)")
# line-initial rubric, optional colon; capture just the word
REFRAIN_RX = re.compile(r"^(\s*)(רעפריין)\s*[:׃]?")


def line_text(el) -> str:
    u = el.find(f".//{UNICODE_TAG}")
    return (u.text or "") if u is not None else ""


def _covers(attrs: dict, lo: int, hi: int) -> bool:
    """True if the span overlaps [lo,hi)."""
    o = int(attrs.get("offset", 0)); l = int(attrs.get("length", 0))
    return o < hi and (o + l) > lo


def retag_line(el) -> list[str]:
    """Mutate one TextLine's custom attr. Returns list of human-readable changes."""
    txt = line_text(el)
    entries = parse_custom(el.get("custom", ""))
    changes = []

    # ---- 1. (ביס) → stage{type:repeat} -------------------------------------
    for m in BIS_RX.finditer(txt):
        lo, hi = m.start(), m.end()
        hit = None
        for i, (tag, attrs) in enumerate(entries):
            if tag == "stage" and _covers(attrs, lo, hi):
                hit = i
                break
        if hit is not None:
            old = entries[hit][1].get("type")
            if old == "repeat":
                continue
            entries[hit][1]["type"] = "repeat"
            changes.append(f"(ביס)@{lo}: stage type {old} → repeat")
        else:
            entries.append(("stage", {"offset": str(lo), "length": str(hi - lo),
                                      "type": "repeat"}))
            changes.append(f"(ביס)@{lo}: untagged → stage type:repeat")

    # ---- 2. רעפריין → head -------------------------------------------------
    m = REFRAIN_RX.match(txt)
    if m:
        lo, hi = m.start(2), m.end(2)          # the word only, no colon
        rest = m.end()                          # text after rubric+colon
        lg_id = None
        kept = []
        for tag, attrs in entries:
            if tag in ("speaker", "stage") and _covers(attrs, lo, hi):
                changes.append(f"רעפריין: dropped {tag} span on the rubric")
                continue
            if tag == "l" and _covers(attrs, lo, hi):
                lg_id = attrs.get("lg_id")
                o = int(attrs.get("offset", 0)); ln = int(attrs.get("length", 0))
                tail = o + ln - rest
                if tail > 0:
                    # sung text follows the rubric on the same line — keep it as
                    # `l`, but starting after the rubric.
                    attrs["offset"] = str(rest); attrs["length"] = str(tail)
                    kept.append((tag, attrs))
                    changes.append(f"רעפריין: l span shrunk to the sung tail @{rest}")
                else:
                    changes.append("רעפריין: dropped l span (rubric is not a verse line)")
                continue
            kept.append((tag, attrs))
        entries = kept
        if not any(t == "head" and _covers(a, lo, hi) for t, a in entries):
            head = {"offset": str(lo), "length": str(hi - lo)}
            if lg_id is not None:
                head["lg_id"] = lg_id
            entries.append(("head", head))
            changes.append(f"רעפריין: → head{' lg_id=' + lg_id if lg_id else ''}")

    if changes:
        el.set("custom", serialize_custom(entries))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    args = ap.parse_args()

    plays = json.loads((REPO / "data" / "_musical_targets.json").read_text())
    client = TrpClient.from_env(); client.login()
    total_pages = total_changes = 0

    for play, info in plays.items():
        if args.only and args.only != play:
            continue
        col, doc = info["col"], info["doc"]
        fulldoc = client.fulldoc(col, doc)
        by_nr = {p["pageNr"]: p for p in fulldoc["pageList"]["pages"]}
        for page_nr in info["pages"]:
            p = by_nr.get(page_nr)
            if p is None:
                print(f"  !! {play} p{page_nr}: not found"); continue
            top = p["tsList"]["transcripts"][0]
            xml = client.fetch_transcript(top["url"])
            tree = etree.fromstring(xml.encode("utf-8"))
            changes = []
            for el in tree.iter(LINE_TAG):
                changes += retag_line(el)
            if not changes:
                continue
            total_pages += 1; total_changes += len(changes)
            who = top["userName"].split("@")[0]
            print(f"\n{play} p{page_nr}  (top layer: {who}, {top['status']})")
            for c in changes:
                print(f"    • {c}")
            if args.push:
                blob = etree.tostring(tree, encoding="UTF-8",
                                      xml_declaration=True, standalone=True)
                res = client.push_transcript(
                    col, doc, page_nr, blob.decode("utf-8"),
                    parent_tsid=top.get("tsId"), status=top["status"],
                    note="retag (ביס)/רעפריין as musical directions")
                print(f"    → pushed tsId={res.get('tsId')} status={top['status']}")
                # Mirror locally. Di Seder carries two filename families for the
                # SAME pageId, so write every mirror of this page, not just one.
                out = REPO / "data" / play / "page_annotated"
                for f in sorted(out.glob(f"{page_nr:04d}_*.xml")):
                    f.write_bytes(blob)
    print(f"\n{'DRY RUN — nothing written' if not args.push else 'PUSHED'}: "
          f"{total_changes} changes across {total_pages} pages")


if __name__ == "__main__":
    main()
