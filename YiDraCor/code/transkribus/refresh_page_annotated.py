"""Refresh data/<play>/page_annotated/ from live Transkribus top transcripts.

Fills the gap noted 2026-06-29: `pull_layers` only diff-snapshots into
`layer_diff_<DATE>/` and `sync pull` is one-doc-at-a-time, so nothing kept
`page_annotated/` (which lint and all scan phases read) current after pushes
or RA edits on Transkribus. This overwrites each local page file with the
live top transcript, corpus-wide.

Run (repo root, python3.11):
  python -m transkribus.refresh_page_annotated [--only FOLDER]
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
COL = 2372172


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", help="restrict to one play folder")
    args = ap.parse_args()

    client = TrpClient.from_env()
    n_fail = 0
    with open(REPO / "data" / "editions.csv", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        folder = (r.get("folder") or "").strip()
        doc_id = (r.get("transkribus_doc_id") or "").strip()
        pa = REPO / "data" / folder / "page_annotated"
        if not (folder and doc_id and pa.is_dir()):
            continue
        if args.only and folder != args.only:
            continue
        try:
            fd = client.fulldoc(COL, int(doc_id))
        except Exception as e:
            print(f"[{folder}] fulldoc FAILED: {e}", flush=True)
            n_fail += 1
            continue
        pages = fd.get("pageList", {}).get("pages", [])
        n = 0
        for p in pages:
            ts = p.get("tsList", {}).get("transcripts", [])
            if not ts:
                continue
            xml = None
            for attempt in range(3):
                try:
                    xml = client.fetch_transcript(ts[0]["url"])
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"[{folder}] p{p['pageNr']} fetch FAILED: {e}", flush=True)
                        n_fail += 1
                    else:
                        time.sleep(2)
            if xml is None:
                continue
            out = pa / f"{int(p['pageNr']):04d}_{p.get('pageId')}.xml"
            out.write_text(xml, encoding="utf-8")
            n += 1
        print(f"[{folder}] refreshed {n}/{len(pages)} pages", flush=True)
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
