"""Safe Phase-4 push to Transkribus collection 18874.

Pushes our locally-migrated page_annotated/ as a new IN_PROGRESS layer, but
NEVER masks human work. The masking failure mode (see [[feedback-pull-masks-gt]])
is the whole reason this is careful. Per page, two guards:

  GUARD 1 (top-owner): if the CURRENT top layer was made by a human other than
  us (Judith 357543, Noa 397914, or any non-pipeline human), SKIP — we don't
  bury someone's current top edit.

  GUARD 2 (density): even if the top is ours, if some HUMAN layer (any depth,
  any author except us-via-pipeline) has higher nikud density than our file,
  SKIP — that buried layer is likely better-vocalized gold we'd be masking.

Our own layers are userId 2180 with tool containing 'YiDraCor'. A 2180 layer
made in the GUI (tool App_*) is treated as human (the account owner's manual
edit) and protected by both guards.

Run:
  python -m transkribus.safe_push --dry-run         # analyse all, push nothing
  python -m transkribus.safe_push                    # push the SAFE set
  python -m transkribus.safe_push --only Yudale --dry-run
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys
import unicodedata
from pathlib import Path

from lxml import etree

from .client import TrpClient

REPO_ROOT = Path(__file__).resolve().parents[2]
COL = 18874
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NS = f"{{{PAGE_NS}}}"
NIKKUD = re.compile(r"[֑-ׇ]")
WORD = re.compile(r"[֐-׿]+")
US_ID = 2180  # account owner; pipeline pushes are this id + tool containing 'YiDraCor'
# Human collaborators whose vocalization could be gold we must not mask.
HUMAN_COLLAB = {357543: "Judith", 397914: "Noa", 66270: "rabeliovi"}

# (label, docId, page_annotated subdir, to-push page numbers)
JOBS = [
    ("Yudale", 828539, "Yudale_der_blinder,_Emkroyt1908", [1, 2, 3] + list(range(10, 71))),
    ("DiSeder", 828503, "Di_seyder_nakht_Emkroyt_1908", [1, 2, 3] + list(range(8, 55))),
    ("DasYudKind", 828424, "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete", [2] + list(range(4, 13))),
    ("KidushHashem", 820939, "KidushHashem", list(range(7, 81))),
    ("AlNaharot", 820975, "AlNaharotBavel-Amkreut&Freund1909", list(range(1, 5)) + list(range(12, 69))),
]


def density_of_text(line_texts) -> tuple[int, int]:
    voc = tot = 0
    for t in line_texts:
        for w in WORD.findall(unicodedata.normalize("NFC", t or "")):
            tot += 1
            if NIKKUD.search(w):
                voc += 1
    return voc, tot


def local_density(path: Path) -> float:
    tree = etree.parse(str(path))
    texts = []
    for tl in tree.iter(NS + "TextLine"):
        for u in tl.iter(NS + "Unicode"):
            if u.getparent().getparent() is tl:
                texts.append(u.text or ""); break
    voc, tot = density_of_text(texts)
    return voc / tot if tot else 0.0


def server_text_density(client: TrpClient, url: str) -> float:
    xml = client.fetch_transcript(url)
    root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
    texts = []
    for tl in root.iter(NS + "TextLine"):
        for u in tl.iter(NS + "Unicode"):
            if u.getparent().getparent() is tl:
                texts.append(u.text or ""); break
    voc, tot = density_of_text(texts)
    return voc / tot if tot else 0.0


def is_ours(ts: dict) -> bool:
    return ts.get("userId") == US_ID and "YiDraCor" in (ts.get("toolName") or "")


def find_local(subdir: str, n: int) -> Path | None:
    d = REPO_ROOT / "data" / subdir / "page_annotated"
    hits = sorted(d.glob(f"{n:04d}_*.xml"))
    return hits[0] if hits else None


def analyse_page(client, page, local_path) -> dict:
    transcripts = page.get("tsList", {}).get("transcripts") or []
    top = transcripts[0] if transcripts else {}
    our_d = local_density(local_path)
    # densest known-human-collaborator layer (Judith/Noa/rabeliovi)
    best_human = None
    for ts in transcripts:
        if ts.get("userId") not in HUMAN_COLLAB:
            continue
        if ts.get("url"):
            try:
                d = server_text_density(client, ts["url"])
            except Exception:
                d = None
            if d is not None and (best_human is None or d > best_human[0]):
                best_human = (d, ts)
    decision, reason = "PUSH", ""
    if top and not is_ours(top):
        decision = "SKIP"; reason = f"top layer is human ({top.get('userName')}, {top.get('status')})"
    elif best_human and best_human[0] > our_d + 0.03:
        decision = "SKIP"
        reason = (f"buried human layer denser ({best_human[0]:.0%} by "
                  f"{best_human[1].get('userName')}) than ours ({our_d:.0%})")
    return {
        "our_density": round(our_d, 3),
        "top_owner": top.get("userName"), "top_status": top.get("status"),
        "top_ours": is_ours(top), "top_tsid": top.get("tsId"),
        "best_human_density": round(best_human[0], 3) if best_human else None,
        "decision": decision, "reason": reason,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="restrict to one edition label")
    ap.add_argument("--report", default=str(REPO_ROOT / "data" / f"safe_push_plan_{_dt.date.today()}.json"))
    args = ap.parse_args()

    client = TrpClient.from_env()
    plan = []
    for label, doc, subdir, pages in JOBS:
        if args.only and label != args.only:
            continue
        fd = client.fulldoc(COL, doc)
        srv = {p.get("pageNr"): p for p in fd.get("pageList", {}).get("pages", [])}
        print(f"\n=== {label} (doc {doc}) — {len(pages)} candidate pages ===", file=sys.stderr)
        for n in pages:
            lp = find_local(subdir, n)
            if lp is None:
                print(f"  p{n}: no local file — skip", file=sys.stderr); continue
            page = srv.get(n)
            if page is None:
                print(f"  p{n}: no server page — skip", file=sys.stderr); continue
            a = analyse_page(client, page, lp)
            a.update({"edition": label, "doc": doc, "page": n, "file": lp.name})
            plan.append(a)
            mark = "PUSH" if a["decision"] == "PUSH" else "skip"
            print(f"  p{n:>3} {mark}: our={a['our_density']:.0%} "
                  f"top={'us' if a['top_ours'] else a['top_owner']} "
                  f"{a['reason']}", file=sys.stderr)

    npush = sum(1 for a in plan if a["decision"] == "PUSH")
    nskip = len(plan) - npush
    Path(args.report).write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nPLAN: {npush} to PUSH, {nskip} to SKIP. Report: {args.report}")

    if args.dry_run:
        print("[dry-run] nothing sent.")
        return 0

    note = f"YiDraCor Phase-4 annotation {_dt.date.today().isoformat()}"
    ok = err = 0
    for a in plan:
        if a["decision"] != "PUSH":
            continue
        lp = REPO_ROOT / "data" / next(s for l, d, s, p in JOBS if l == a["edition"]) / "page_annotated" / a["file"]
        xml = lp.read_text(encoding="utf-8")
        try:
            client.push_transcript(COL, a["doc"], a["page"], xml,
                                   parent_tsid=a["top_tsid"], status="IN_PROGRESS",
                                   note=note, tool_name="YiDraCor-annotation-pipeline")
            ok += 1; print(f"[ok] {a['edition']} p{a['page']}")
        except Exception as e:
            err += 1; print(f"[FAIL] {a['edition']} p{a['page']}: {e}")
    print(f"\nPushed ok={ok} err={err} (skipped {nskip})")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
