"""Pull the manuscript track's live top transcripts into the local mirror.

`refresh_page_annotated` fills only `page_annotated/`. That is not enough to
pull *before a push*: `push_ms_track` decides there is something of ours to
send by diffing `page_annotated/` against `page/`, and its safety guard
compares the live top `tsId` against the one recorded in
`_ms_pull_manifest.json`. Refreshing one of the three leaves the other two
stale, so the next push either skips every page (manifest tsId no longer
matches live) or re-sends a layer it already sent (`page/` behind).

This writes all three together, so after a run:

    page/  ==  page_annotated/  ==  live top transcript
    manifest tsId  ==  live top tsId

i.e. nothing to push and the guard satisfied. Annotate `page_annotated/`
afterwards and `push_ms_track` sees exactly your changes.

    python3.11 -m transkribus.pull_ms_track --dry-run
    python3.11 -m transkribus.pull_ms_track --only MS_BasKoyen
    python3.11 -m transkribus.pull_ms_track

Sinai 2026-08-18.
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
COL = 2372172


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append", help="restrict to play folder(s)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change; write nothing")
    args = ap.parse_args()

    client = TrpClient.from_env()
    manifests = sorted(REPO.glob("data/*/_ms_pull_manifest.json"))
    if args.only:
        want = set(args.only)
        manifests = [m for m in manifests if m.parent.name in want]
    if not manifests:
        print("no manifests matched")
        return 1

    n_fail = 0
    for man in manifests:
        d = json.loads(man.read_text(encoding="utf-8"))
        play, doc_id = man.parent.name, d["doc_id"]
        pa, orig = man.parent / "page_annotated", man.parent / "page"
        try:
            fd = client.fulldoc(COL, int(doc_id))
        except Exception as e:
            print(f"[{play}] fulldoc FAILED: {e}", flush=True)
            n_fail += 1
            continue

        live = {}
        for p in fd.get("pageList", {}).get("pages", []):
            ts = p.get("tsList", {}).get("transcripts", [])
            if ts:
                live[p.get("pageId")] = (p["pageNr"], ts[0])

        moved = fetched = 0
        for rec in d["pages"]:
            hit = live.get(rec["pageId"])
            if hit is None:
                continue
            page_nr, top = hit
            drift = str(top.get("tsId")) != str(rec.get("tsId"))
            if drift:
                moved += 1
                print(f"  MOVED {play} p.{page_nr}: {rec.get('tsId')} -> "
                      f"{top.get('tsId')} ({top.get('userName')})", flush=True)
            if args.dry_run:
                continue
            xml = None
            for attempt in range(3):
                try:
                    xml = client.fetch_transcript(top["url"])
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"  [{play}] p{page_nr} fetch FAILED: {e}", flush=True)
                        n_fail += 1
                    else:
                        time.sleep(2)
            if xml is None:
                continue
            # All three in step, or the next push misreads the state.
            (pa / rec["file"]).write_text(xml, encoding="utf-8")
            (orig / rec["file"]).write_text(xml, encoding="utf-8")
            rec["tsId"] = top.get("tsId")
            if top.get("status"):
                rec["status"] = top["status"]
            fetched += 1

        if not args.dry_run:
            man.write_text(json.dumps(d, ensure_ascii=False, indent=2),
                           encoding="utf-8")
        verb = "would refresh" if args.dry_run else "refreshed"
        print(f"[{play}] {verb} {fetched}/{len(d['pages'])} pages, "
              f"{moved} moved since last pull", flush=True)

    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
