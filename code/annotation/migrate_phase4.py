"""Phase-4 annotation migration: carry existing page_annotated spans onto the
re-vocalized page_final gold, conform to the current schema, and add `fw`
page-number tags.

Per page (only pages given; RA-verified pages are excluded by the caller):
  1. reanchor existing spans (page_annotated) onto new text (page_final) —
     recomputes offsets across the nikud change (annotation.reanchor).
  2. copy the region page-type marker (structure {type:...}) from the OLD
     annotated tree (reanchor takes region customs from the new/page_final tree,
     which lacks the marker — this is the documented reanchor regression).
  3. add `fw {type:pageNum}` to printed page-number lines that lack one.
  4. strip any leftover `unclear {...}` entries.
  5. validate every span against schema; collect a conflict report
     (reanchor drops, schema violations, speaker xmlids absent from cast_dict).

Writes the migrated tree to page_annotated/ (back up first). Returns a report.

Usage:
  python -m annotation.migrate_phase4 --play <folder> --pages 1 2 3 10-70 \
      [--report data/annotation_conflicts_<edition>_<date>.json] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[0].parent))
from annotation.reanchor import reanchor_tree
from annotation.schema import (
    PAGE_NS, parse_custom, serialize_custom, set_region_structure,
    validate_span, PAGE_TYPES,
)

NS = f"{{{PAGE_NS}}}"
REPO_ROOT = Path(__file__).resolve().parents[2]
UNCLEAR_RE = re.compile(r"\s*unclear\s*\{[^}]*\}")
# A printed page-number line in this corpus is dash-framed ("— 23 —"); require
# >=1 leading dash so bare back-matter numbers (prices, "1000") aren't tagged.
PAGENUM_RE = re.compile("^\\s*[-—–]+\\s*\\d+\\s*[-—–]*\\s*$")


def line_text(tl) -> str:
    for u in tl.iter(NS + "Unicode"):
        if u.getparent().getparent() is tl:
            return u.text or ""
    return ""


def old_page_type(old_tree) -> tuple[str | None, str | None]:
    """Return (region_id, page_type) of the region carrying structure{type:..}."""
    for reg in old_tree.iter(NS + "TextRegion"):
        for tag, a in parse_custom(reg.get("custom") or ""):
            if tag == "structure" and a.get("type") in PAGE_TYPES:
                return reg.get("id"), a["type"]
    return None, None


def apply_page_type(new_tree, region_id: str | None, page_type: str) -> bool:
    """Set structure{type:page_type} on the new tree. Prefer the same region id
    as the old tree; else the lowest-readingOrder TextRegion that has TextLines."""
    if region_id is not None:
        for reg in new_tree.iter(NS + "TextRegion"):
            if reg.get("id") == region_id:
                reg.set("custom", set_region_structure(reg.get("custom", ""), page_type))
                return True
    # fallback: lowest readingOrder region with lines
    best, best_idx = None, None
    for reg in new_tree.iter(NS + "TextRegion"):
        if not list(reg.iter(NS + "TextLine")):
            continue
        idx = None
        for tag, a in parse_custom(reg.get("custom") or ""):
            if tag == "readingOrder" and a.get("index", "").isdigit():
                idx = int(a["index"])
        idx = idx if idx is not None else 1_000_000
        if best_idx is None or idx < best_idx:
            best, best_idx = reg, idx
    if best is not None:
        best.set("custom", set_region_structure(best.get("custom", ""), page_type))
        return True
    return False


def clamp_old_span_lengths(old_tree) -> int:
    """Clamp offset-bearing span lengths to the old line's text length, in place.

    Some old annotations carry stale lengths that overshoot the current text
    (e.g. a prior nikud-strip on vocalized cast-list role names shortened the
    text but left length unchanged). reanchor's bounds-check bails on
    offset+length > len(text), dropping an otherwise-recoverable span. Clamping
    to the line length lets the bare-consonant match succeed. Returns count."""
    clamped = 0
    for tl in old_tree.iter(NS + "TextLine"):
        n = len(line_text(tl))
        entries = parse_custom(tl.get("custom") or "")
        changed = False
        out = []
        for tag, attrs in entries:
            if "offset" in attrs and "length" in attrs:
                try:
                    off = int(attrs["offset"]); ln = int(attrs["length"])
                except ValueError:
                    out.append((tag, attrs)); continue
                if off < n and off + ln > n:
                    attrs = dict(attrs); attrs["length"] = str(n - off)
                    clamped += 1; changed = True
            out.append((tag, attrs))
        if changed:
            tl.set("custom", serialize_custom(out))
    return clamped


def carry_offsetless_markers(old_tree, new_tree) -> tuple[int, list[str]]:
    """Carry line-level group markers (lg group form, head) that have no
    offset/length from the old tree to the matching line id in the new tree.
    reanchor only rewrites offset-bearing spans and drops these. Normalizes
    lg.cont: keep only if 'yes', drop otherwise (new schema). Returns
    (markers_carried, line_ids_not_found_in_new)."""
    new_by_id = {tl.get("id"): tl for tl in new_tree.iter(NS + "TextLine")}
    carried = 0
    missing = []
    for old_line in old_tree.iter(NS + "TextLine"):
        lid = old_line.get("id")
        offsetless = []
        for tag, attrs in parse_custom(old_line.get("custom") or ""):
            if tag in ("readingOrder", "structure"):
                continue
            if "offset" in attrs:
                continue  # handled by reanchor
            if tag == "lg" and attrs.get("cont") and attrs["cont"] != "yes":
                attrs = {k: v for k, v in attrs.items() if k != "cont"}
            offsetless.append((tag, attrs))
        if not offsetless:
            continue
        nl = new_by_id.get(lid)
        if nl is None:
            missing.append(lid)
            continue
        entries = parse_custom(nl.get("custom") or "")
        entries.extend(offsetless)
        nl.set("custom", serialize_custom(entries))
        carried += len(offsetless)
    return carried, missing


def add_fw_pagenums(new_tree) -> list[str]:
    """Tag printed page-number lines with fw{type:pageNum}. Returns line ids tagged."""
    tagged = []
    for tl in new_tree.iter(NS + "TextLine"):
        txt = line_text(tl)
        if not PAGENUM_RE.match(txt):
            continue
        custom = tl.get("custom") or ""
        if "fw" in custom:
            continue
        # span over the trimmed visible content
        m = re.search(r"\S.*\S|\S", txt)
        if not m:
            continue
        off, end = m.start(), m.end()
        entries = parse_custom(custom)
        entries.append(("fw", {"offset": str(off), "length": str(end - off), "type": "pageNum"}))
        tl.set("custom", serialize_custom(entries))
        tagged.append(tl.get("id"))
    return tagged


def strip_unclear(new_tree) -> int:
    n = 0
    for el in new_tree.iter():
        c = el.get("custom")
        if c and "unclear" in c:
            new = UNCLEAR_RE.sub("", c).strip()
            if new != c:
                el.set("custom", new)
                n += 1
    return n


def collect_conflicts(new_tree, cast_ids: set[str], page: int) -> list[dict]:
    out = []
    for tl in new_tree.iter(NS + "TextLine"):
        txt = line_text(tl)
        for tag, attrs in parse_custom(tl.get("custom") or ""):
            if tag in ("readingOrder", "structure"):
                continue
            # rebuild a span dict for validation
            try:
                off = int(attrs.get("offset")); ln = int(attrs.get("length"))
            except (TypeError, ValueError):
                continue
            span = {"tag": tag, "offset": off, "length": ln,
                    "attrs": {k: v for k, v in attrs.items()
                              if k not in ("offset", "length")}}
            err = validate_span(txt, span)
            if err:
                out.append({"page": page, "line": tl.get("id"),
                            "kind": "schema", "detail": err,
                            "text": txt[off:off + ln]})
            if tag in ("speaker", "role"):
                xid = attrs.get("xmlid")
                if xid and xid not in cast_ids:
                    out.append({"page": page, "line": tl.get("id"),
                                "kind": "speaker_not_in_cast", "xmlid": xid,
                                "text": txt[off:off + ln]})
    return out


def parse_pages(spec: list[str]) -> list[int]:
    out = []
    for s in spec:
        if "-" in s:
            a, b = s.split("-"); out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(s))
    return sorted(set(out))


def find_pages(play: str) -> dict[int, tuple[Path, Path]]:
    """Map page_num -> (page_annotated_path, page_final_path) for pages present in both."""
    pdir = REPO_ROOT / "data" / play
    ann = {int(p.name[:4]): p for p in (pdir / "page_annotated").glob("[0-9]" * 4 + "_*.xml")}
    fin = {int(p.name[:4]): p for p in (pdir / "page_final").glob("[0-9]" * 4 + "_*.xml")}
    return {n: (ann[n], fin[n]) for n in ann if n in fin}


def load_cast_ids(play: str) -> set[str]:
    f = REPO_ROOT / "data" / play / "cast_dict.json"
    if not f.exists():
        return set()
    d = json.loads(f.read_text(encoding="utf-8"))
    roles = d.get("roles", {})
    return set(roles.keys()) if isinstance(roles, dict) else {r.get("xmlid") for r in roles}


def migrate_page(ann_path: Path, fin_path: Path, page: int, cast_ids: set[str],
                 dry_run: bool) -> dict:
    old = etree.parse(str(ann_path))
    new = etree.parse(str(fin_path))
    clamp_old_span_lengths(old)
    rep = reanchor_tree(old, new)
    carried, _ = carry_offsetless_markers(old, new)
    # Real drops exclude the offset-less group markers we deliberately carry.
    real_drops = [d for d in rep["drops"]
                  if not (d.get("reason") == "missing offset/length"
                          and d.get("tag") in ("lg", "head"))]
    rid, ptype = old_page_type(old)
    pt_ok = apply_page_type(new, rid, ptype) if ptype else False
    fw = add_fw_pagenums(new)
    stripped = strip_unclear(new)
    conflicts = collect_conflicts(new, cast_ids, page)
    # A page with text lines but no carried/kept spans was never annotated.
    n_text_lines = sum(1 for tl in new.iter(NS + "TextLine") if line_text(tl).strip())
    needs_annotation = n_text_lines > 0 and (rep["spans_kept"] + carried) == 0
    if not dry_run:
        new.write(str(ann_path), encoding="UTF-8", xml_declaration=True, standalone=True)
    return {
        "page": page, "page_type": ptype, "page_type_applied": pt_ok,
        "spans_kept": rep["spans_kept"], "markers_carried": carried,
        "spans_dropped": len(real_drops), "drops": real_drops,
        "fw_added": len(fw), "unclear_stripped": stripped,
        "needs_annotation": needs_annotation, "text_lines": n_text_lines,
        "conflicts": conflicts,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--play", required=True)
    ap.add_argument("--pages", nargs="+", required=True,
                    help="page numbers / ranges, e.g. 1 2 3 10-70")
    ap.add_argument("--report", help="write a JSON conflict report here")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    pages = parse_pages(args.pages)
    avail = find_pages(args.play)
    cast_ids = load_cast_ids(args.play)
    results = []
    needs = []
    total_drops = total_fw = total_conf = 0
    for n in pages:
        if n not in avail:
            print(f"p{n:02d}: SKIP (no annotated/final pair)")
            continue
        ann_p, fin_p = avail[n]
        r = migrate_page(ann_p, fin_p, n, cast_ids, args.dry_run)
        results.append(r)
        total_drops += r["spans_dropped"]; total_fw += r["fw_added"]
        total_conf += len(r["conflicts"])
        if r["needs_annotation"]:
            needs.append(n)
        flag = ""
        if r["spans_dropped"]: flag += f" DROPS={r['spans_dropped']}"
        if r["conflicts"]: flag += f" CONFLICTS={len(r['conflicts'])}"
        if r["needs_annotation"]: flag += f" NEEDS-ANNOTATION({r['text_lines']} lines)"
        if not r["page_type_applied"]: flag += " NO-PAGETYPE"
        print(f"p{n:02d}: type={r['page_type']} kept={r['spans_kept']:3} "
              f"carried={r['markers_carried']:2} fw+={r['fw_added']} "
              f"unclear-={r['unclear_stripped']}{flag}")

    print(f"\n{'DRY-RUN ' if args.dry_run else ''}TOTAL: {len(results)} pages  "
          f"drops={total_drops}  fw_added={total_fw}  conflicts={total_conf}")
    if needs:
        print(f"NEEDS FRESH ANNOTATION (old had no spans): {needs}")
    if args.report:
        rep = {"play": args.play, "pages": pages,
               "summary": {"pages": len(results), "drops": total_drops,
                           "fw_added": total_fw, "conflicts": total_conf},
               "details": results}
        Path(args.report).write_text(json.dumps(rep, ensure_ascii=False, indent=2),
                                     encoding="utf-8")
        print(f"Wrote report {args.report}")


if __name__ == "__main__":
    main()
