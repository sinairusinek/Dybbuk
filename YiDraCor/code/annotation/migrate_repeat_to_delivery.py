"""Retype every live `stage{type:repeat}` span as `stage{type:delivery}`.

Sinai 2026-07-21: `repeat` is retired; the printed `(ביס)` mark is now
`delivery` like every other musical performance instruction. This migrates the
134 spans already on Transkribus (13 of them ascribed with @who, which is
preserved). Idempotent — a page with no `repeat` span is a no-op. Finds pages
by scanning the live top, so it never depends on the local mirror.

  python3.11 -m annotation.migrate_repeat_to_delivery --dry-run
  python3.11 -m annotation.migrate_repeat_to_delivery --push
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
COL = 18874


def retype(root) -> list[str]:
    changes = []
    for el in root.iter(f"{NS}TextLine"):
        ents = parse_custom(el.get("custom") or "")
        hit = False
        out = []
        for tag, a in ents:
            if tag == "stage" and a.get("type") == "repeat":
                a = dict(a); a["type"] = "delivery"
                hit = True
                who = f" who=#{a['xmlid']}" if a.get("xmlid") else ""
                changes.append(f"repeat → delivery{who}")
            out.append((tag, a))
        if hit:
            el.set("custom", serialize_custom(out))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()

    client = TrpClient.from_env(); client.login()
    docs = [e["transkribus_doc_id"] for e in
            json.load((REPO / "data" / "editions.json").open())["editions"]]
    n = pages = 0
    for doc in docs:
        fd = client.fulldoc(COL, doc)
        for p in fd["pageList"]["pages"]:
            top = p["tsList"]["transcripts"][0]
            try:
                root = etree.fromstring(client.fetch_transcript(top["url"]).encode("utf-8"))
            except Exception:
                continue
            changes = retype(root)
            if not changes:
                continue
            n += len(changes); pages += 1
            print(f"doc {doc} p{p['pageNr']}: {len(changes)} span(s)  [{changes[0]}]")
            if args.push:
                client.push_transcript(
                    COL, doc, p["pageNr"], etree.tostring(root, encoding="unicode"),
                    parent_tsid=top.get("tsId"), status=top.get("status", "IN_PROGRESS"),
                    note="retire repeat: stage type repeat → delivery",
                    tool_name="YiDraCor-annotation-pipeline")
                print("  → pushed")
    print(f"\n{'PUSHED' if args.push else 'DRY RUN'}: {n} spans across {pages} pages")


if __name__ == "__main__":
    main()
