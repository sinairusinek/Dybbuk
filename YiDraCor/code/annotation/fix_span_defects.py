"""Repair small mechanical span defects surfaced by lint_pages.

Three classes, all decided by rule — anything ambiguous is REPORTED, not fixed:
  1. stage @type typos      — case slips and misspellings (`Entrance`→`entrance`,
                              `drlivery`/`deivery`→`delivery`, `settins`→`setting`)
  2. stage{type:trailer}    — per ST12 `trailer` is a TAG, not a stage @type;
                              `(ענדע פון ערשטען אקט)` retags to <trailer>
  3. fw missing @type       — only when the covered text is a page number
                              (`— 53 —`, `9`); anything else is reported
  4. span out of range      — offset+length exceeds the line; clamped to the
                              line end, and reported so the boundary can be eyed

Operates on the live top transcript, parent chained, status preserved.
  python3.11 -m annotation.fix_span_defects [--dry-run]
"""
from __future__ import annotations
import argparse, csv, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (parse_custom, serialize_custom, STAGE_TYPES,
                               FW_TYPES, _validate_stage_type)
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"
COL = 18874

TYPE_FIX = {"entrance": "entrance", "delivery": "delivery", "setting": "setting",
            "drlivery": "delivery", "deivery": "delivery", "settins": "setting",
            "dilivery": "delivery", "buisness": "business", "busines": "business"}
PAGENUM_RE = re.compile(r"^[\s—–\- ]*\d+[\s—–\- .]*$")


def line_text(el) -> str:
    u = el.find(f".//{UNICODE_TAG}")
    return (u.text or "") if u is not None else ""


def doc_ids() -> dict[str, int]:
    out = {}
    with open(REPO / "data" / "editions.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("folder") and r.get("transkribus_doc_id"):
                out[r["folder"]] = int(r["transkribus_doc_id"])
    return out


def fix_line(el) -> tuple[list[str], list[str]]:
    txt = line_text(el)
    entries = parse_custom(el.get("custom") or "")
    fixed, reported = [], []
    out = []
    for tag, a in entries:
        # --- 4. out-of-range span → clamp
        try:
            off = int(a.get("offset")); ln = int(a.get("length"))
        except (TypeError, ValueError):
            off = ln = None
        if off is not None and ln is not None and off + ln > len(txt):
            new_ln = max(0, len(txt) - off)
            if new_ln == 0:
                reported.append(f"{tag} span starts past end of line (offset {off}, "
                                f"line len {len(txt)}) — DROPPED")
                continue
            a["length"] = str(new_ln)
            fixed.append(f"{tag} span clamped {ln}→{new_ln} (line len {len(txt)})")
            ln = new_ln

        if tag == "stage":
            t = (a.get("type") or "").strip()
            # --- 2. trailer is a tag, not a stage type
            if t == "trailer":
                out.append(("trailer", {k: v for k, v in a.items() if k != "type"}))
                fixed.append("stage{type:trailer} → <trailer> tag (ST12)")
                continue
            # --- 1. @type typo. Use the real validator: multi-token values
            # like `entrance business` are VALID under option C (ST3) and must
            # not be reported as unknown.
            if t and _validate_stage_type(t) is not None:
                low = t.lower()
                repl = TYPE_FIX.get(low) or (low if low in STAGE_TYPES else None)
                if repl:
                    a["type"] = repl
                    fixed.append(f"stage type {t!r} → {repl!r}")
                else:
                    reported.append(f"stage type {t!r} not in vocab and no known fix")

        if tag == "fw" and not a.get("type"):
            covered = txt[off:off + ln] if off is not None and ln is not None else txt
            if PAGENUM_RE.match(covered):
                a["type"] = "pageNum"
                fixed.append(f"fw type:pageNum ({covered.strip()!r})")
            else:
                reported.append(f"fw span with no type over non-pagenum text "
                                f"{covered.strip()[:34]!r} — needs a human")
        out.append((tag, a))

    if fixed:
        el.set("custom", serialize_custom(out))
    return fixed, reported


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = list(csv.DictReader(
        open(REPO / "data" / "review" / "lint_2026-07-19.csv", encoding="utf-8")))
    editions = {}
    with open(REPO / "data" / "editions.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("folder"):
                editions[r.get("title", r["folder"])] = r["folder"]

    targets: dict[str, set] = {}
    for r in rows:
        d = r["issue/detail"]
        if ("not in vocab" in d or "out of range" in d
                or "fw span requires a type" in d):
            play = editions.get(r["edition"])
            if play:
                targets.setdefault(play, set()).add(int(r["page(s)"]))

    docs = doc_ids()
    client = TrpClient.from_env(); client.login()
    n_fix = n_rep = 0

    for play, pages in sorted(targets.items()):
        doc = docs.get(play)
        if doc is None:
            continue
        fulldoc = client.fulldoc(COL, doc)
        by_nr = {p["pageNr"]: p for p in fulldoc["pageList"]["pages"]}
        for page in sorted(pages):
            p = by_nr.get(page)
            if p is None:
                continue
            top = p["tsList"]["transcripts"][0]
            tree = etree.fromstring(client.fetch_transcript(top["url"]).encode("utf-8"))
            allf, allr = [], []
            for el in tree.iter(LINE_TAG):
                f, r = fix_line(el)
                allf += f; allr += r
            if allf or allr:
                print(f"\n{play[:26]} p{page}  (top: {top['userName'].split('@')[0]})")
                for x in allf:
                    print(f"    • {x}")
                for x in allr:
                    print(f"    ! REPORT: {x}")
            n_fix += len(allf); n_rep += len(allr)
            if allf and not args.dry_run:
                blob = etree.tostring(tree, encoding="UTF-8",
                                      xml_declaration=True, standalone=True)
                client.push_transcript(COL, doc, page, blob.decode("utf-8"),
                                       parent_tsid=top.get("tsId"),
                                       status=top["status"],
                                       note="fix span defects (type typos / fw / range)")
                print("    → pushed")

    print(f"\n{'DRY RUN' if args.dry_run else 'PUSHED'}: {n_fix} fixed, "
          f"{n_rep} reported for a human")


if __name__ == "__main__":
    main()
