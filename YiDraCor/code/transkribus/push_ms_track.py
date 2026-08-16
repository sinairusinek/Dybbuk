"""Push the manuscript track's local annotation back to Transkribus.

`safe_push` cannot do this job: it is hardwired to collection 18874 and to a
fixed list of print plays. The guard it exists to enforce still applies, though
— never bury a human's current top layer — so this enforces the same thing by a
stricter and simpler test.

THE GUARD. `_ms_pull_manifest.json` records, per page, the exact `tsId` this
mirror was built from. A page is pushed only if the live top transcript is
*still that same tsId*. If it differs, somebody has edited the page since the
pull and our payload predates their work, so the page is SKIPPED and reported.
That is the whole check, and it is sufficient: we cannot mask an edit we can
prove has not happened.

`client.push_transcript` adds the wholesale span-loss guard underneath, which
catches the other failure mode — a payload built from a stale copy.

Each push is parent-chained to the tsId it was built on, so the layer history
stays intact and any edit remains recoverable. Page status is preserved rather
than reset: a GT page stays GT.

  python3.11 -m transkribus.push_ms_track --dry-run
  python3.11 -m transkribus.push_ms_track --only MS_BasKoyen --dry-run
  python3.11 -m transkribus.push_ms_track --push
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transkribus.client import TrpClient, SpanLossError  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
COL = 2372172
TOOL = "YiDraCor-ms-track-2026-08-16"
NOTE = ("MS track: lg/l stanzas, speaker+role xmlids, stage/heading types, "
        "tag normalisation")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append", help="restrict to play folder(s)")
    ap.add_argument("--push", action="store_true", help="actually upload")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, help="stop after N pages (for a first run)")
    args = ap.parse_args()

    client = TrpClient.from_env()
    manifests = sorted(REPO.glob("data/*/_ms_pull_manifest.json"))
    if args.only:
        want = set(args.only)
        manifests = [m for m in manifests if m.parent.name in want]

    n_push = n_skip_same = n_skip_moved = n_fail = 0
    pushed_pages = []
    for man in manifests:
        d = json.loads(man.read_text(encoding="utf-8"))
        play, doc_id = man.parent.name, d["doc_id"]
        pa = man.parent / "page_annotated"
        orig = man.parent / "page"

        try:
            fd = client.fulldoc(COL, doc_id)
        except Exception as e:
            print(f"{play}: cannot read doc {doc_id}: {e}")
            continue
        live = {}
        for p in fd.get("pageList", {}).get("pages", []):
            ts = p.get("tsList", {}).get("transcripts", [])
            if ts:
                live[p.get("pageId")] = (p["pageNr"], ts[0])

        changed = moved = same = 0
        for rec in d["pages"]:
            f = pa / rec["file"]
            if not f.is_file():
                continue
            payload = f.read_text(encoding="utf-8")
            before = (orig / rec["file"])
            if before.is_file() and before.read_text(encoding="utf-8") == payload:
                same += 1
                continue          # nothing of ours to add

            hit = live.get(rec["pageId"])
            if hit is None:
                continue
            page_nr, top = hit
            if str(top.get("tsId")) != str(rec.get("tsId")):
                moved += 1
                print(f"  SKIP {play} p.{page_nr}: live top is tsId "
                      f"{top.get('tsId')} ({top.get('userName')}), mirror built "
                      f"on {rec.get('tsId')} — would bury a newer edit")
                continue

            changed += 1
            if not args.push:
                continue
            try:
                resp = client.push_transcript(
                    COL, doc_id, page_nr, payload,
                    parent_tsid=int(rec["tsId"]),
                    status=rec.get("status") or "IN_PROGRESS",
                    note=NOTE, tool_name=TOOL,
                )
                new_ts = resp.get("tsId") or resp.get("key")
                pushed_pages.append((play, rec["file"], new_ts))
                if new_ts:
                    rec["tsId"] = new_ts
                # `page/` mirrors what is ON Transkribus, so bring it level with
                # what we just put there. Without this the payload still differs
                # from `page/` on the next run and the same layer is pushed
                # again, stacking duplicates.
                (orig / rec["file"]).write_text(payload, encoding="utf-8")
            except SpanLossError as e:
                n_fail += 1
                print(f"  SPAN-LOSS GUARD {play} p.{page_nr}: {e}")
            except Exception as e:
                n_fail += 1
                print(f"  FAIL {play} p.{page_nr}: {str(e)[:160]}")
            if args.limit and len(pushed_pages) >= args.limit:
                break

        n_push += changed
        n_skip_same += same
        n_skip_moved += moved
        print(f"{play:24} to push {changed:4}   unchanged {same:4}   "
              f"skipped(moved) {moved:2}")
        if args.push:
            man.write_text(json.dumps(d, ensure_ascii=False, indent=1),
                           encoding="utf-8")
        if args.limit and len(pushed_pages) >= args.limit:
            break

    print(f"\n{'PUSHED' if args.push else 'WOULD PUSH'} {n_push} pages; "
          f"{n_skip_same} unchanged; {n_skip_moved} skipped as moved; "
          f"{n_fail} failed")
    if not args.push:
        print("DRY RUN — nothing uploaded")
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
