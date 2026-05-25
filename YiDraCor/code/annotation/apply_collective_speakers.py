"""Tag untagged collective/chorus speech turns as `speaker` on Transkribus.

The PI ratified the convention (2026-05-24 review): collective labels — `אלע`,
`קאהר`/chorus, `שטימען`, `ביידע`, `מענער`, `מעדכען`, `דועט`, `איינער`, and the
song-supplement voice rubrics — are real speech turns but get a SHARED
collective xml:id (`alle`, `chor`, …) and NO individual cast entry. So there is
nothing for a human to review: this annotates them directly.

Targets are re-derived from `page_annotated/` by the same rule lint_pages uses
(turn-shaped line, collective label, no existing speaker span). For each, it
fetches the page's live top transcript, and — if the line still lacks a speaker
span — adds `speaker {offset:0; length:<label>; xmlid:<collective>}` (length
computed from the LIVE line text) and pushes a parent-layered IN_PROGRESS
transcript. Idempotent; safe to re-run.

Run:
  python -m annotation.apply_collective_speakers --dry-run
  python -m annotation.apply_collective_speakers --only Di_seyder_nakht_Emkroyt_1908
  python -m annotation.apply_collective_speakers
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import sys
from collections import defaultdict
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom, is_collective_label
from annotation.lint_pages import (
    NS, REPO, TURN_RE, COLLECTIVE_XMLID, skel, line_text, page_type, page_files,
)
from transkribus.client import TrpClient

COL = 18874

# Song-supplement boundary: pages strictly after this are the lyric appendix,
# where collective labels are voice RUBRICS (sopran/alt/…, and supplement
# alle/beyde) kept inline within <lg> — NOT speech turns (PI review 2026-05-24).
# Only Di Seder has a supplement; verified other plays' late pages are body.
SONG_SUPPLEMENT_FROM = {"Di_seyder_nakht_Emkroyt_1908": 54}


def load_doc_ids() -> dict[str, int]:
    """folder -> transkribus_doc_id (legacy collection 18874 shares doc ids)."""
    out = {}
    with open(REPO / "data" / "editions.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("folder") and r.get("transkribus_doc_id"):
                out[r["folder"]] = int(r["transkribus_doc_id"])
    return out


def collective_targets(play: str):
    """Yield (page, line_id, xmlid) for untagged collective turns in a play.

    Skips title/cast pages and the song supplement (lyric rubrics, kept inline).
    """
    supp_from = SONG_SUPPLEMENT_FROM.get(play)
    for page, path in page_files(play):
        if supp_from is not None and page > supp_from:
            continue
        tree = etree.parse(str(path))
        if page_type(tree) in ("titlePage", "castList"):
            continue
        for tl in tree.iter(NS + "TextLine"):
            spans = parse_custom(tl.get("custom") or "")
            if any(t == "speaker" for t, _ in spans):
                continue
            txt = line_text(tl)
            m = TURN_RE.match(txt)
            if not m or not is_collective_label(m.group(1)):
                continue
            xmlid = COLLECTIVE_XMLID.get(skel(m.group(1)), skel(m.group(1)))
            yield page, tl.get("id"), xmlid


def find_line(root, line_id):
    for tl in root.iter(NS + "TextLine"):
        if tl.get("id") == line_id:
            return tl
    return None


def tag_collective(root, line_id, xmlid):
    """Add speaker span if the live line is still an untagged collective turn."""
    tl = find_line(root, line_id)
    if tl is None:
        return False, f"line {line_id} not found on server"
    entries = parse_custom(tl.get("custom") or "")
    if any(t == "speaker" for t, _ in entries):
        return False, "already has speaker span"
    txt = line_text(tl)
    m = TURN_RE.match(txt)
    if not m or not is_collective_label(m.group(1)):
        return False, f"live text no longer a collective turn ({txt[:16]!r})"
    length = len(m.group(1))
    entries.append(("speaker", {"offset": "0", "length": str(length), "xmlid": xmlid}))
    tl.set("custom", serialize_custom(entries))
    return True, f"+speaker xmlid:{xmlid} (len {length})"


def top_transcript(client, doc, page):
    fd = client.fulldoc(COL, doc)
    for p in fd.get("pageList", {}).get("pages", []):
        if int(p.get("pageNr")) == page:
            tss = p.get("tsList", {}).get("transcripts") or []
            if not tss:
                return None, None, None
            top = tss[0]
            return top.get("tsId"), top.get("userName"), client.fetch_transcript(top["url"])
    return None, None, None


def discover_plays():
    return sorted(p.name for p in (REPO / "data").iterdir()
                  if (p / "page_annotated").is_dir())


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    args = ap.parse_args()

    doc_ids = load_doc_ids()
    plays = [args.only] if args.only else discover_plays()
    client = TrpClient.from_env()
    note = f"YiDraCor collective-speaker tagging {_dt.date.today().isoformat()}"

    n_change = n_push = n_skip = n_fail = 0
    used_xmlids: dict[str, set[str]] = defaultdict(set)
    for play in plays:
        doc = doc_ids.get(play)
        if doc is None:
            print(f"[{play}] no transkribus_doc_id — skip")
            continue
        per_page: dict[int, list] = defaultdict(list)
        for page, line_id, xmlid in collective_targets(play):
            per_page[page].append((line_id, xmlid))
            used_xmlids[play].add(xmlid)
        if not per_page:
            continue
        print(f"\n=== {play} (doc {doc}) — {sum(len(v) for v in per_page.values())} collective turns on {len(per_page)} pages ===")
        for page in sorted(per_page):
            tsid, owner, xml = top_transcript(client, doc, page)
            if xml is None:
                print(f"  p{page}: no server transcript — skip"); continue
            root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
            changed_here = False
            for line_id, xmlid in per_page[page]:
                ok, status = tag_collective(root, line_id, xmlid)
                if "not found" in status:
                    n_fail += 1; print(f"  p{page} {line_id}: ✗ {status}")
                elif ok:
                    n_change += 1; changed_here = True
                    print(f"  p{page} {line_id}: ✎ {status}")
                else:
                    print(f"  p{page} {line_id}: · {status}")
            if not changed_here:
                n_skip += 1; continue
            if args.dry_run:
                n_push += 1; print(f"  p{page}: [dry-run] would push (parent {tsid}, top by {owner})")
                continue
            client.push_transcript(COL, doc, page, etree.tostring(root, encoding="unicode"),
                                   parent_tsid=tsid, status="IN_PROGRESS", note=note,
                                   tool_name="YiDraCor-annotation-pipeline")
            n_push += 1; print(f"  p{page}: → pushed (parent {tsid}, top by {owner})")

    print(f"\n{'DRY-RUN ' if args.dry_run else ''}SUMMARY: {n_change} spans, "
          f"{n_push} pages {'to push' if args.dry_run else 'pushed'}, "
          f"{n_skip} pages already-tagged, {n_fail} failures")
    print("\nCollective xml:ids used (declare these as collective roles in cast_dict):")
    for play, ids in sorted(used_xmlids.items()):
        print(f"  {play}: {sorted(ids)}")
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
