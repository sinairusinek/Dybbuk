#!/usr/bin/env python3
"""Extract every <stage> annotation from the page-annotated PAGE-XML into one CSV.

Stage directions are stored as Transkribus `custom` tags on each TextLine:
    stage {offset:N; length:M; type:T;}
where offset/length index (by code point) into that line's TextEquiv/Unicode text.

For each stage tag we emit: play title, the full line text (the line carrying the
tag; consecutive lines whose tags chain are merged), the direction substring alone,
its allocated @type (blank = untyped), the starting TextLine id, and a deep-link to
the Transkribus page.

Usage:
  python3 code/annotation/extract_stage_directions.py \
      --out data/review/stage_directions_2026-05-25.csv
"""
from __future__ import annotations

import argparse
import csv
import glob
import pathlib
import re
import xml.etree.ElementTree as ET

REPO = pathlib.Path(__file__).resolve().parents[2]          # .../YiDraCor
EDITIONS_CSV = REPO / "data" / "editions.csv"
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
TS_DEEPLINK = "https://app.transkribus.org/collection/{col}/doc/{doc}/detail/{page}"

_STAGE_RE = re.compile(r"stage \{([^}]*)\}")
_PAGE_PREFIX_RE = re.compile(r"^(\d+)_")
_NIKUD = re.compile(r"[֑-ׇ]")   # strip stray vowel points from @type values


def parse_stage_tag(body: str) -> dict:
    out = {}
    for part in body.split(";"):
        part = part.strip()
        if ":" in part:
            k, v = part.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def line_text(tl: ET.Element) -> str:
    te = tl.find(f"{{{PAGE_NS}}}TextEquiv")
    if te is None:
        return ""
    uni = te.find(f"{{{PAGE_NS}}}Unicode")
    return (uni.text or "") if uni is not None else ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/review/stage_directions.csv")
    args = ap.parse_args()

    editions = list(csv.DictReader(open(EDITIONS_CSV, encoding="utf-8-sig")))
    by_folder = {e["folder"]: e for e in editions}

    rows: list[dict] = []
    for ed in editions:
        folder = ed["folder"]
        pa = REPO / "data" / folder / "page_annotated"
        if not pa.is_dir():
            continue
        title = ed["title"]
        doc = ed["transkribus_doc_id"]
        col = ed["transkribus_collection_id"]

        # Collect every stage span across the whole edition in reading order
        # (document order across pages), then merge contiguous runs below.
        units: list[dict] = []
        for xml_path in sorted(pa.glob("*.xml")):
            m = _PAGE_PREFIX_RE.match(xml_path.name)
            page = int(m.group(1)) if m else None
            try:
                tree = ET.parse(xml_path)
            except ET.ParseError:
                continue
            for tl in tree.iter(f"{{{PAGE_NS}}}TextLine"):
                custom = tl.get("custom", "")
                stages = _STAGE_RE.findall(custom)
                if not stages:
                    continue
                txt = line_text(tl)
                line_id = tl.get("id", "")
                for body in stages:
                    tag = parse_stage_tag(body)
                    try:
                        off = int(tag.get("offset", "0"))
                        length = int(tag.get("length", "0"))
                    except ValueError:
                        off, length = 0, 0
                    units.append({
                        "page": page, "line_id": line_id, "text": txt,
                        "off": off, "length": length,
                        "type": _NIKUD.sub("", tag.get("type", "")),
                        "ends_line": (off + length) >= len(txt.rstrip()),
                    })

        # Merge a span into the previous one when it is a clean continuation:
        # same type, previous span ran to its line end, this one starts at col 0,
        # and it sits on a later line. Captures directions broken across lines.
        groups: list[list[dict]] = []
        for u in units:
            prev = groups[-1][-1] if groups else None
            cont = (prev is not None and u["type"] == prev["type"]
                    and prev["ends_line"] and u["off"] == 0
                    and u["line_id"] != prev["line_id"])
            if cont:
                groups[-1].append(u)
            else:
                groups.append([u])

        for g in groups:
            first = g[0]
            full_line = " ".join(u["text"] for u in g)
            direction = " ".join(
                u["text"][u["off"]:u["off"] + u["length"]] for u in g
            )
            page = first["page"]
            rows.append({
                "play": title,
                "full_line": full_line,
                "direction": direction,
                "type": first["type"],
                "line_id": first["line_id"],
                "page": page if page is not None else "",
                "page_url": TS_DEEPLINK.format(col=col, doc=doc, page=page) if page else "",
            })

    out_path = (REPO / args.out) if not pathlib.Path(args.out).is_absolute() else pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["play", "full_line", "direction", "type", "line_id", "page", "page_url"]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} stage directions -> {out_path.relative_to(REPO)}")
    by_type: dict[str, int] = {}
    for r in rows:
        by_type[r["type"] or "(untyped)"] = by_type.get(r["type"] or "(untyped)", 0) + 1
    for k, v in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
