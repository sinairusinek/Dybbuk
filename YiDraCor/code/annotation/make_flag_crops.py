#!/usr/bin/env python3
"""Generate per-line image crops + an enriched TSV for the RA annotation-flags
review (consumed by the Zalmen `yidracor_flags` view).

For each row in an annotation-flags CSV it:
  1. resolves the edition label -> folder + Transkribus docId (via editions.csv),
  2. downloads the flagged page image to a gitignored local cache (Transkribus),
  3. reads the flagged TextLine's polygon from the page-XML and crops a strip,
  4. writes an enriched TSV adding: crop_path, transkribus_url, translit.

Crops + the enriched TSV are committed (small); the page-image cache is not.

Usage:
  source ~/.zshrc   # Transkribus creds (TRANSKRIBUS_USER/PASS) for image download
  python3 code/annotation/make_flag_crops.py \
      --flags data/annotation_flags_for_RA_2026-05-21.csv

Without creds the TSV (with deep-links + translit) is still produced; only the
crop PNGs are skipped.
"""
from __future__ import annotations

import argparse
import csv
import glob
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

REPO = pathlib.Path(__file__).resolve().parents[2]          # .../YiDraCor
EDITIONS_CSV = REPO / "data" / "editions.csv"
OUT_DIR = REPO / "data" / "review"
CROP_DIR = OUT_DIR / "crops"
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
TS_COLLECTION = "2372172"                                    # Latayner-Hurwitz-Plays
TS_DEEPLINK = "https://app.transkribus.org/collection/{col}/doc/{doc}/detail/{page}"
CROP_PAD = 10                                                # px around the polygon

# ── Approximate YIVO-ish romanization (reading aid for the RA; not canonical) ──
_DIGRAPHS = [
    ("וו", "v"), ("וי", "oy"), ("ױ", "oy"), ("ײ", "ey"), ("יי", "ey"),
    ("אַ", "a"), ("אָ", "o"), ("בּ", "b"), ("בֿ", "v"), ("כּ", "k"),
    ("פּ", "p"), ("פֿ", "f"), ("שׂ", "s"), ("תּ", "t"), ("וּ", "u"), ("יִ", "i"),
]
_SINGLE = {
    "א": "", "ב": "b", "ג": "g", "ד": "d", "ה": "h", "ו": "u", "ז": "z",
    "ח": "kh", "ט": "t", "י": "i", "כ": "kh", "ך": "kh", "ל": "l", "מ": "m",
    "ם": "m", "נ": "n", "ן": "n", "ס": "s", "ע": "e", "פ": "f", "ף": "f",
    "צ": "ts", "ץ": "ts", "ק": "k", "ר": "r", "ש": "sh", "ת": "s",
}
_NIKUD = re.compile(r"[֑-ׇ]")          # cantillation + vowel points
_MAQAF = "־"


def translit(text: str) -> str:
    if not text:
        return ""
    s = text.replace(_MAQAF, "-")
    for src, dst in _DIGRAPHS:
        s = s.replace(src, dst)
    s = _NIKUD.sub("", s)
    out = []
    for ch in s:
        out.append(_SINGLE.get(ch, ch))
    return "".join(out).strip()


# ── Edition resolution ────────────────────────────────────────────────────────
def load_editions() -> list[dict]:
    with open(EDITIONS_CSV, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def resolve_edition(label: str, editions: list[dict]) -> dict | None:
    """flags label (e.g. 'Al Naharot') -> editions row (title startswith label)."""
    lab = label.strip().lower()
    cands = [e for e in editions if e["title"].strip().lower().startswith(lab)]
    if not cands:
        cands = [e for e in editions if lab in e["title"].strip().lower()]
    return cands[0] if cands else None


# ── Page / line parsing ───────────────────────────────────────────────────────
def first_page(pages_field: str) -> int | None:
    m = re.search(r"\d+", pages_field or "")
    return int(m.group()) if m else None


_LINE_ID_RE = re.compile(r"^(r\d|line_|line\d)", re.IGNORECASE)


def is_line_id(s: str) -> bool:
    return bool(_LINE_ID_RE.match((s or "").strip()))


def page_xml_path(folder: str, page: int) -> pathlib.Path | None:
    hits = glob.glob(str(REPO / "data" / folder / "page_annotated" / f"{page:04d}_*.xml"))
    return pathlib.Path(hits[0]) if hits else None


def line_bbox(xml_path: pathlib.Path, line_id: str) -> tuple[int, int, int, int] | None:
    """Return (left, top, right, bottom) of the TextLine's Coords polygon."""
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return None
    for tl in tree.iter(f"{{{PAGE_NS}}}TextLine"):
        if tl.get("id") == line_id:
            coords = tl.find(f"{{{PAGE_NS}}}Coords")
            if coords is None:
                return None
            pts = [tuple(map(int, p.split(","))) for p in coords.get("points").split()]
            xs, ys = [p[0] for p in pts], [p[1] for p in pts]
            return min(xs), min(ys), max(xs), max(ys)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--flags", required=True, help="annotation-flags CSV path")
    ap.add_argument("--no-images", action="store_true", help="skip image download/crop")
    args = ap.parse_args()

    flags_path = (REPO / args.flags) if not pathlib.Path(args.flags).is_absolute() else pathlib.Path(args.flags)
    editions = load_editions()
    CROP_DIR.mkdir(parents=True, exist_ok=True)

    with open(flags_path, newline="", encoding="utf-8-sig") as f:  # strip BOM
        rows = list(csv.DictReader(f))
    in_headers = list(rows[0].keys()) if rows else []

    # Transkribus client + per-doc page->image-url map, created lazily.
    client = None
    page_url_cache: dict[str, dict[int, str]] = {}
    if not args.no_images:
        sys.path.insert(0, str(REPO / "code"))
        try:
            from transkribus.client import TrpClient
            client = TrpClient.from_env()
        except Exception as e:  # noqa: BLE001
            print(f"[warn] no Transkribus client ({e}); skipping image download.", file=sys.stderr)
            client = None

    try:
        from PIL import Image
    except ImportError:
        Image = None
        print("[warn] Pillow not installed; skipping crops.", file=sys.stderr)

    n_crop, n_skip = 0, 0
    for r in rows:
        r["crop_path"] = ""
        r["transkribus_url"] = ""
        r["translit"] = translit(r.get("text", ""))

        ed = resolve_edition(r.get("edition", ""), editions)
        if not ed:
            n_skip += 1
            continue
        folder = ed["folder"]
        doc_id = ed["transkribus_doc_id"]
        page = first_page(r.get("page(s)", ""))
        if page is None:
            continue
        r["transkribus_url"] = TS_DEEPLINK.format(col=TS_COLLECTION, doc=doc_id, page=page)

        line_id = (r.get("line_id / count") or "").strip()
        if not is_line_id(line_id) or client is None or Image is None:
            continue

        xml_path = page_xml_path(folder, page)
        if not xml_path:
            n_skip += 1
            continue
        bbox = line_bbox(xml_path, line_id)
        if not bbox:
            n_skip += 1
            continue

        # Download page image to gitignored cache (once per page).
        cache_dir = REPO / "data" / folder / "page_images"
        cache_dir.mkdir(parents=True, exist_ok=True)
        img_path = cache_dir / f"p{page:04d}.jpg"
        if not img_path.exists():
            urls = page_url_cache.get(doc_id)
            if urls is None:
                urls = client.page_image_map(int(ed["transkribus_collection_id"]), int(doc_id))
                page_url_cache[doc_id] = {k: v["url"] for k, v in urls.items()}
                urls = page_url_cache[doc_id]
            url = urls.get(page)
            if not url:
                n_skip += 1
                continue
            img_path.write_bytes(client.fetch_image(url))

        left, top, right, bottom = bbox
        with Image.open(img_path) as im:
            W, H = im.size
            box = (max(0, left - CROP_PAD), max(0, top - CROP_PAD),
                   min(W, right + CROP_PAD), min(H, bottom + CROP_PAD))
            crop = im.crop(box)
            slug = re.sub(r"[^A-Za-z0-9]+", "-", ed["title"]).strip("-")
            out = CROP_DIR / f"{slug}_p{page:04d}_{line_id}.png"
            crop.save(out)
        r["crop_path"] = str(out.relative_to(REPO))
        n_crop += 1

    out_tsv = OUT_DIR / "annotation_flags_enriched.tsv"
    headers = in_headers + ["translit", "crop_path", "transkribus_url"]
    with open(out_tsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        w.writerows(rows)

    print(f"crops: {n_crop}  skipped-line-crops: {n_skip}  rows: {len(rows)}")
    print(f"wrote {out_tsv.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
