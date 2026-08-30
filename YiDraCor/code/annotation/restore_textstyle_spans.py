"""Restore `textStyle` scribal marks that the 2026-08-30 push stripped off live.

WHAT HAPPENED
-------------
`auto_resolve_flags` rule 1 dropped `textStyle` unconditionally as "legacy
cruft". That was written for the PRINT track, where the tag really was
leftover Transkribus styling; the print plays have none left. On the MANUSCRIPT
track the same tag carries the scribe's own marks:

    strikethrough:true   text the scribe struck out   (556 spans)
    underlined:true      text the scribe underlined   (174 spans)

Running the resolver over the nine notebooks on 2026-08-30 removed these from
the live transcripts of every play whose push completed. The client's
span-loss guard caught the pages where the loss was more than half a page's
spans (Emigration p68, and with it Tissa-Essler and Yoysef, which never
pushed), but pages losing a smaller share went through.

They belong in the edition: the full TEI carries them (`<del>` for a
strikethrough, `<hi>` for an underline) and the DraCor variant deletes the
struck text along with the tag (Sinai, 2026-08-30).

WHY GIT, NOT TRANSCRIPT HISTORY
-------------------------------
`restore_lost_l_spans` (the July precedent, same class of accident) walks each
page's Transkribus ancestry looking for the version with the most spans. Here
there is something better: `page_annotated/` was committed at 7be34ea6, BEFORE
the push, and holds every lost span with its exact offset and length. So the
baseline is read straight from git — no guessing which ancestor is right.

    git show 7be34ea6:YiDraCor/data/<play>/page_annotated/<file>

GUARDS
------
  * re-adds a span only if the line still exists and its text is UNCHANGED
    between baseline and live (offsets would otherwise be meaningless);
  * never duplicates a span that is already on the live line;
  * skips any span whose offset+length no longer fits the line;
  * reports every skip with a reason rather than silently dropping it.

    python3.11 -m annotation.restore_textstyle_spans --dry-run
    python3.11 -m annotation.restore_textstyle_spans --only Lateiner_Meshumed
    python3.11 -m annotation.restore_textstyle_spans
"""
from __future__ import annotations
import argparse, json, subprocess, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
COL = 2372172
BASELINE = "7be34ea6"          # last commit before the 2026-08-30 push
NOTE = "restore textStyle scribal marks (2026-08-30 regression)"

PLAYS = ["Lateiner_Meshumed", "MS_Emigration", "MS_TissaEssler",
         "MS_YoysefInEgipten", "MS_KhurbnYerusholaim", "MS_BenHaDor",
         "MS_DiTsveyTnoim", "MS_BasKoyen", "MS_YaakovEsav"]


def git_show(rel: str) -> str | None:
    r = subprocess.run(["git", "show", f"{BASELINE}:{rel}"],
                       cwd=REPO.parent, capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def line_text(el) -> str:
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def lines_of(xml: str) -> dict[str, tuple[str, str]]:
    """{line_id: (text, custom)}"""
    root = etree.fromstring(xml.encode("utf-8"))
    return {el.get("id"): (line_text(el), el.get("custom") or "")
            for el in root.iter(f"{NS}TextLine")}


def wanted(custom: str) -> list[tuple[str, dict]]:
    """The textStyle spans worth restoring from a baseline custom string."""
    return [(t, a) for t, a in parse_custom(custom)
            if t == "textStyle"
            and (a.get("strikethrough") == "true" or a.get("underlined") == "true")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()

    client = TrpClient.from_env()
    client.login()
    plays = [p for p in PLAYS if not a.only or p == a.only]
    grand_add = grand_skip = 0

    for play in plays:
        man_p = REPO / "data" / play / "_ms_pull_manifest.json"
        if not man_p.exists():
            print(f"{play}: no manifest, skipped"); continue
        man = json.loads(man_p.read_text(encoding="utf-8"))
        doc = man["doc_id"]
        n_add = n_skip = n_pages = 0
        print(f"\n=== {play} (doc {doc})")

        for pg in man["pages"]:
            rel = f"YiDraCor/data/{play}/page_annotated/{pg['file']}"
            base_xml = git_show(rel)
            if base_xml is None:
                continue
            base = lines_of(base_xml)
            if not any(wanted(c) for _, c in base.values()):
                continue

            page_nr = pg["pageNr"]
            tr = client.fulldoc(COL, doc)["pageList"]["pages"]
            tsl = [p for p in tr if p["pageNr"] == page_nr]
            if not tsl or not tsl[0].get("tsList", {}).get("transcripts"):
                print(f"  p{page_nr}: no transcript"); continue
            top = tsl[0]["tsList"]["transcripts"][0]
            live_xml = client.fetch_transcript(top["url"])
            root = etree.fromstring(live_xml.encode("utf-8"))

            changed, report = False, []
            for el in root.iter(f"{NS}TextLine"):
                lid = el.get("id")
                if lid not in base:
                    continue
                btxt, bcustom = base[lid]
                add = wanted(bcustom)
                if not add:
                    continue
                ltxt = line_text(el)
                if ltxt != btxt:
                    n_skip += len(add)
                    report.append(f"    {lid}: text changed since baseline "
                                  f"— {len(add)} span(s) NOT restored")
                    continue
                cur = parse_custom(el.get("custom") or "")
                have = {(t, aa.get("offset"), aa.get("length")) for t, aa in cur}
                new = []
                for t, aa in add:
                    key = (t, aa.get("offset"), aa.get("length"))
                    if key in have:
                        continue
                    off, ln = int(aa.get("offset", 0)), int(aa.get("length", 0))
                    if off + ln > len(ltxt):
                        n_skip += 1
                        report.append(f"    {lid}: span {off}+{ln} exceeds "
                                      f"line ({len(ltxt)}) — skipped")
                        continue
                    new.append((t, aa))
                if not new:
                    continue
                merged = sorted(cur + new,
                                key=lambda e: (e[0] != "readingOrder",
                                               int(e[1].get("offset", -1))))
                el.set("custom", serialize_custom(merged))
                changed = True; n_add += len(new)
                mark = ",".join("del" if x[1].get("strikethrough") == "true"
                                else "ul" for x in new)
                report.append(f"    {lid}: +{len(new)} ({mark}) "
                              f"{ltxt[:38]!r}")

            if not changed:
                continue
            n_pages += 1
            print(f"  p{page_nr}:")
            for r in report:
                print(r)
            if not a.dry_run:
                client.push_transcript(
                    COL, doc, page_nr, etree.tostring(root, encoding="unicode"),
                    parent_tsid=top["tsId"], status="IN_PROGRESS", note=NOTE,
                    tool_name="YiDraCor-annotation-pipeline")
                print(f"  p{page_nr}: → pushed")

        print(f"  {play}: +{n_add} spans on {n_pages} pages"
              + (f", {n_skip} skipped" if n_skip else ""))
        grand_add += n_add; grand_skip += n_skip

    print(f"\n{'DRY RUN — ' if a.dry_run else ''}restored {grand_add} textStyle "
          f"span(s){f', {grand_skip} skipped' if grand_skip else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
