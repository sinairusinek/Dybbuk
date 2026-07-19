"""Back-fill `xmlid` onto castList `role` spans from cast_dict.

The RAs tag role spans in Transkribus without an xmlid — their workflow doesn't
set one — and `extract_cast_dict` mints the id into cast_dict.json, recording
each role's exact `loc` (page, line_id, offset, length). So the mapping already
exists; it was simply never written back to the page. That leaves 64 spans
failing `role span requires xmlid attribute` and breaks @who resolution on the
castList side.

This matches by (line_id, offset) and VERIFIES the covered text against the
cast_dict form before writing, so a stale `loc` is skipped rather than
mis-assigned. Operates on the live top transcript; pushes with the parent
chained and status preserved.

  python3.11 -m annotation.backfill_role_xmlids [--dry-run] [--only PLAY]
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom, _NIKUD
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"
COL = 18874


def skel(s: str) -> str:
    """Nikud-stripped, punctuation-stripped comparison key."""
    return _NIKUD.sub("", s or "").strip().strip(".,:;־ ")


def line_text(el) -> str:
    u = el.find(f".//{UNICODE_TAG}")
    return (u.text or "") if u is not None else ""


def doc_ids() -> dict[str, int]:
    import csv
    out = {}
    with open(REPO / "data" / "editions.csv", newline="",
              encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("folder") and r.get("transkribus_doc_id"):
                out[r["folder"]] = int(r["transkribus_doc_id"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only")
    args = ap.parse_args()

    docs = doc_ids()
    client = TrpClient.from_env(); client.login()
    tot_set = tot_skip = 0

    for cd_path in sorted((REPO / "data").glob("*/cast_dict.json")):
        play = cd_path.parent.name
        if args.only and args.only != play:
            continue
        doc = docs.get(play)
        if doc is None:
            continue
        cast = json.loads(cd_path.read_text(encoding="utf-8"))
        # index the roles that carry a usable loc, by page
        by_page: dict[int, list] = {}
        for xmlid, info in cast.get("roles", {}).items():
            loc = info.get("loc")
            if not loc or loc.get("page") is None:
                continue
            by_page.setdefault(int(loc["page"]), []).append((xmlid, info, loc))
        if not by_page:
            continue

        fulldoc = client.fulldoc(COL, doc)
        by_nr = {p["pageNr"]: p for p in fulldoc["pageList"]["pages"]}

        for page, entries_for_page in sorted(by_page.items()):
            p = by_nr.get(page)
            if p is None:
                continue
            top = p["tsList"]["transcripts"][0]
            tree = etree.fromstring(client.fetch_transcript(top["url"]).encode("utf-8"))
            lines = {el.get("id"): el for el in tree.iter(LINE_TAG)}
            changes, skips = [], []

            for xmlid, info, loc in entries_for_page:
                el = lines.get(loc.get("line_id"))
                if el is None:
                    skips.append(f"{xmlid}: line {loc.get('line_id')} not on page"); continue
                txt = line_text(el)
                spans = parse_custom(el.get("custom") or "")
                off = int(loc.get("offset", 0)); ln = int(loc.get("length", 0))
                hit = None
                for i, (tag, a) in enumerate(spans):
                    if tag != "role":
                        continue
                    if int(a.get("offset", -1)) == off:
                        hit = i; break
                if hit is None:
                    skips.append(f"{xmlid}: no role span at offset {off}"); continue
                a = spans[hit][1]
                if a.get("xmlid"):
                    continue  # already resolved
                # VERIFY the covered text before writing
                covered = txt[off:off + int(a.get("length", ln))]
                want = info.get("form") or info.get("bare") or ""
                if skel(covered) != skel(want):
                    skips.append(f"{xmlid}: text mismatch — page {covered.strip()!r} "
                                 f"vs cast_dict {want.strip()!r}")
                    continue
                a["xmlid"] = xmlid
                el.set("custom", serialize_custom(spans))
                changes.append(f"{xmlid} ← {covered.strip()[:24]!r}")

            if changes:
                print(f"\n{play} p{page}  (top: {top['userName'].split('@')[0]}, {top['status']})")
                for c in changes:
                    print(f"    • role xmlid:{c}")
            for s in skips:
                print(f"    ! {play} p{page}: {s}")
            tot_set += len(changes); tot_skip += len(skips)

            if changes and not args.dry_run:
                blob = etree.tostring(tree, encoding="UTF-8",
                                      xml_declaration=True, standalone=True)
                client.push_transcript(COL, doc, page, blob.decode("utf-8"),
                                       parent_tsid=top.get("tsId"),
                                       status=top["status"],
                                       note="back-fill role xmlid from cast_dict")
                print(f"    → pushed")

    print(f"\n{'DRY RUN — nothing written' if args.dry_run else 'PUSHED'}: "
          f"{tot_set} xmlids set, {tot_skip} skipped")


if __name__ == "__main__":
    main()
