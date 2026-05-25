"""Apply the PI's 2026-05-24 annotation-flag decisions to Transkribus.

Source of truth: data/review/annotation_flags_decisions.tsv (the Zalmen-app
review log). This script translates each actionable decision into an edit on the
live PAGE-XML and pushes it as a new IN_PROGRESS layer (parent = current top),
so it never silently masks a newer human edit — it builds on top of it.

It is IDEMPOTENT: it fetches the CURRENT top transcript per page and only
changes lines whose edit is not already present. That doubles as the
"did Noa already introduce this in Transkribus?" check the PI asked for — a page
that already carries every decision shows up as "already applied, no push".

Decision → edit mapping (collective-speaker / acknowledge-only rows need no
Transkribus edit and are not listed here):

  untyped stage → setting      : add  type:setting  to the line's `stage` span
  untyped stage → trailer      : retag the `stage` span as `trailer`
  untyped stage → epilog        : retag the `stage` span as heading{type:epilog}
  legacy attr/tag → fix         : drop the legacy `head{unit-type}` / `Header`
                                  entry (the proper `heading{type:act}` already
                                  coexists on the line)
  legacy tag textStyle → delete : drop the `textStyle` entry
  OCR consonant (Di Seder p9)   : fix ולמן→זלמן in the Unicode text + add
                                  speaker{xmlid:zelmen_kahn}
  back_matter remove regions    : delete all TextRegions on the page

Run:
  python -m annotation.apply_pi_decisions --dry-run            # report only
  python -m annotation.apply_pi_decisions --only KidushHashem  # one edition
  python -m annotation.apply_pi_decisions                      # push the edits
"""
from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from transkribus.client import TrpClient

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NS = f"{{{PAGE_NS}}}"
COL = 18874  # legacy TrpServer collection (matches transkribus.safe_push)

# (label, docId)
DOCS = {
    "AlNaharot": 820975,
    "KidushHashem": 820939,
    "DiSeder": 828503,
    "Yudale": 828539,
}

# Per-edition line edits. Each is (page, line_id, op[, arg]).
#   op ∈ {set_stage_type, stage_to_trailer, stage_to_epilog,
#         drop_legacy_head, drop_textstyle, diseder_zlmn}
# Page-level ops live in PAGE_OPS.
LINE_EDITS = {
    "AlNaharot": [
        # untyped stage → setting (fervandlung / forhang)
        (12, "r1l8", "set_stage_type", "setting"),
        (18, "r1l6", "set_stage_type", "setting"),
        (25, "r1l9", "set_stage_type", "setting"),
        (31, "r1l3", "set_stage_type", "setting"),
        (37, "r1l20", "set_stage_type", "setting"),
        (43, "r1l4", "set_stage_type", "setting"),
        (43, "r1l15", "set_stage_type", "setting"),
        (50, "r1l25", "set_stage_type", "setting"),
        (50, "r1l33", "set_stage_type", "setting"),
        (59, "r1l8", "set_stage_type", "setting"),
        (63, "r1l15", "set_stage_type", "setting"),
        # ende … → trailer
        (63, "r1l25", "stage_to_trailer"),
        (37, "line_1649598655516_2903", "stage_to_trailer"),
        (25, "line_1649597840955_2176", "stage_to_trailer"),  # "ende fun'm ershten akt" (pattern, decision blank)
        # epilog → structural epilogue heading
        (60, "line_1649538258191_4864", "stage_to_epilog"),
    ],
    "KidushHashem": [
        # untyped stage → setting (fervandlung / forhang [falt])
        (75, "r1l23", "set_stage_type", "setting"),
        (74, "r1l4", "set_stage_type", "setting"),
        (65, "r1l22", "set_stage_type", "setting"),
        (58, "r1l2", "set_stage_type", "setting"),
        (57, "r1l22", "set_stage_type", "setting"),
        (51, "r1l22", "set_stage_type", "setting"),
        # ende der … akt / piesse → trailer
        (46, "r1l41", "stage_to_trailer"),
        (51, "r1l23", "stage_to_trailer"),
        (65, "r1l23", "stage_to_trailer"),
        (75, "r1l24", "stage_to_trailer"),
        # legacy attr/tag → drop legacy entry (proper heading already present)
        (52, "r1l2", "drop_legacy_head"),
        (47, "r2l1", "drop_legacy_head"),
        (26, "r1l5", "drop_legacy_head"),
        (66, "r1l2", "drop_legacy_head"),   # heading already type:act n:5
        (7, "r3l8", "drop_legacy_head"),    # legacy `Header` tag
        # legacy textStyle → delete (PI: "delete in transkribus")
        (42, "r1l12", "drop_textstyle"),
        (45, "r1l4", "drop_textstyle"),
        (8, "r1l9", "drop_textstyle"),
        (70, "r1l5", "drop_textstyle"),     # already removed upstream → idempotent skip
    ],
    "DiSeder": [
        (9, "r_3_1l3", "diseder_zlmn"),
    ],
    "Yudale": [],
}

# Page-level region deletions (PI: "remove all regions in the page").
PAGE_OPS = {
    "Yudale": [(68, "remove_all_regions"),
               (69, "remove_all_regions"),
               (70, "remove_all_regions")],
}


def line_unicode_el(tl):
    for u in tl.iter(NS + "Unicode"):
        if u.getparent().getparent() is tl:
            return u
    return None


def find_line(root, line_id):
    for tl in root.iter(NS + "TextLine"):
        if tl.get("id") == line_id:
            return tl
    return None


def apply_line_edit(root, page, line_id, op, arg=None):
    """Mutate `root` in place. Return (changed: bool, status: str)."""
    tl = find_line(root, line_id)
    if tl is None:
        return False, f"LINE NOT FOUND p{page} {line_id}"
    entries = parse_custom(tl.get("custom") or "")

    if op == "set_stage_type":
        stages = [(t, a) for (t, a) in entries if t == "stage"]
        if not stages:
            return False, "no stage span"
        if all(a.get("type") == arg for _, a in stages):
            return False, f"already type:{arg}"
        for t, a in entries:
            if t == "stage":
                a["type"] = arg
        tl.set("custom", serialize_custom(entries))
        return True, f"stage → type:{arg}"

    if op == "stage_to_trailer":
        if any(t == "trailer" for t, _ in entries) and not any(t == "stage" for t, _ in entries):
            return False, "already trailer"
        new = []
        did = False
        for t, a in entries:
            if t == "stage":
                new.append(("trailer", {k: v for k, v in a.items() if k != "type"}))
                did = True
            else:
                new.append((t, a))
        if not did:
            return False, "no stage span to retag"
        tl.set("custom", serialize_custom(new))
        return True, "stage → trailer"

    if op == "stage_to_epilog":
        if any(t == "heading" and a.get("type") == "epilog" for t, a in entries):
            return False, "already heading:epilog"
        new = []
        did = False
        for t, a in entries:
            if t == "stage":
                ha = {k: v for k, v in a.items() if k in ("offset", "length")}
                ha["type"] = "epilog"
                new.append(("heading", ha))
                did = True
            else:
                new.append((t, a))
        if not did:
            return False, "no stage span to retag"
        tl.set("custom", serialize_custom(new))
        return True, "stage → heading:epilog"

    if op == "drop_legacy_head":
        kept = [(t, a) for (t, a) in entries
                if not (t == "Header" or (t == "head" and "unit-type" in a))]
        if len(kept) == len(entries):
            return False, "no legacy head/Header entry"
        # sanity: the proper heading must survive
        if not any(t == "heading" for t, _ in kept):
            return False, "REFUSING: no surviving heading{type:act} on line"
        tl.set("custom", serialize_custom(kept))
        return True, "dropped legacy head/Header"

    if op == "drop_textstyle":
        kept = [(t, a) for (t, a) in entries if t != "textStyle"]
        if len(kept) == len(entries):
            return False, "no textStyle entry"
        tl.set("custom", serialize_custom(kept))
        return True, "dropped textStyle"

    if op == "diseder_zlmn":
        u = line_unicode_el(tl)
        txt = (u.text or "") if u is not None else ""
        changed = False
        msgs = []
        if txt.startswith("ולמן"):
            u.text = "זלמן" + txt[len("ולמן"):]
            changed = True
            msgs.append("text ולמן→זלמן")
        elif not txt.startswith("זלמן"):
            return False, f"unexpected line text {txt[:8]!r}"
        if not any(t == "speaker" for t, _ in entries):
            entries.append(("speaker", {"offset": "0", "length": "4", "xmlid": "zelmen_kahn"}))
            tl.set("custom", serialize_custom(entries))
            changed = True
            msgs.append("+speaker zelmen_kahn")
        if not changed:
            return False, "already fixed"
        return True, ", ".join(msgs)

    return False, f"unknown op {op}"


def apply_page_op(root, page, op):
    if op == "remove_all_regions":
        regions = [r for r in root.iter(NS + "TextRegion")]
        if not regions:
            return False, "no regions (already empty)"
        for r in regions:
            r.getparent().remove(r)
        return True, f"removed {len(regions)} region(s)"
    return False, f"unknown page op {op}"


def top_transcript(client, doc, page):
    """Return (tsId, page_xml_str) of the current top transcript for a page."""
    fd = client.fulldoc(COL, doc)
    for p in fd.get("pageList", {}).get("pages", []):
        if int(p.get("pageNr")) == page:
            tss = p.get("tsList", {}).get("transcripts") or []
            if not tss:
                return None, None, None
            top = tss[0]
            xml = client.fetch_transcript(top["url"])
            return top.get("tsId"), top.get("userName"), xml
    return None, None, None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="report only, push nothing")
    ap.add_argument("--only", help="restrict to one edition label")
    args = ap.parse_args()

    client = TrpClient.from_env()
    note = f"YiDraCor PI annotation-flag decisions {_dt.date.today().isoformat()}"

    # group edits per (label, page)
    per_page: dict[tuple[str, int], list] = {}
    for label, edits in LINE_EDITS.items():
        for page, line_id, op, *rest in edits:
            per_page.setdefault((label, page), []).append(("line", line_id, op, rest[0] if rest else None))
    for label, ops in PAGE_OPS.items():
        for page, op in ops:
            per_page.setdefault((label, page), []).append(("page", None, op, None))

    n_push = n_skip = n_change = n_fail = 0
    for (label, page) in sorted(per_page):
        if args.only and label != args.only:
            continue
        doc = DOCS[label]
        tsid, owner, xml = top_transcript(client, doc, page)
        if xml is None:
            print(f"[{label} p{page}] no transcript on server — SKIP")
            continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        page_changed = False
        print(f"[{label} p{page}] top by {owner} (ts {tsid}):")
        for kind, line_id, op, arg in per_page[(label, page)]:
            if kind == "line":
                changed, status = apply_line_edit(root, page, line_id, op, arg)
            else:
                changed, status = apply_page_op(root, page, op)
            tag = line_id or "PAGE"
            if status.startswith("REFUSING") or status.startswith("LINE NOT FOUND") or "unexpected" in status:
                n_fail += 1
                print(f"    ✗ {tag}: {status}")
            elif changed:
                n_change += 1
                print(f"    ✎ {tag}: {status}")
            else:
                print(f"    · {tag}: {status} (already applied)")
            page_changed = page_changed or changed
        if not page_changed:
            n_skip += 1
            continue
        if args.dry_run:
            n_push += 1
            print(f"    → [dry-run] would push new layer (parent {tsid})")
            continue
        out_xml = etree.tostring(root, encoding="unicode")
        client.push_transcript(COL, doc, page, out_xml,
                               parent_tsid=tsid, status="IN_PROGRESS",
                               note=note, tool_name="YiDraCor-annotation-pipeline")
        n_push += 1
        print(f"    → pushed new layer (parent {tsid})")

    print(f"\n{'DRY-RUN ' if args.dry_run else ''}SUMMARY: "
          f"{n_change} line/page edits, {n_push} pages "
          f"{'to push' if args.dry_run else 'pushed'}, "
          f"{n_skip} pages already-applied, {n_fail} failures")
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
