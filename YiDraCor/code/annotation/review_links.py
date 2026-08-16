"""Transkribus deep links for review sheets — one place that knows the mapping.

Every RA sheet needs the same thing: given a play folder and a page, produce a
link that opens that page in Transkribus. Three modules already had the URL
template inline (`extract_stage_directions`, `export_speaker_who_review`,
`make_flag_crops`); this adds the folder → (collection, doc) lookup they each
did their own way, so a sheet can be linked with one call.

The doc id comes from `data/editions.csv` for the print track and from the
`_ms_pull_manifest.json` written by the manuscript bootstrap for the MS track —
the MS plays are not in editions.csv yet, and the manifest is authoritative for
them because it records what was actually pulled.

  from annotation.review_links import page_url
  page_url("MS_BasKoyen", "0006_31089289.xml")   # or 6, or "6"
"""
from __future__ import annotations

import csv
import json
import re
from functools import lru_cache
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DEEPLINK = "https://app.transkribus.org/collection/{col}/doc/{doc}/detail/{page}"
DEFAULT_COL = 2372172


@lru_cache(maxsize=1)
def _doc_map() -> dict[str, tuple[int, int]]:
    """folder -> (collection_id, doc_id)."""
    out: dict[str, tuple[int, int]] = {}
    ed = REPO / "data" / "editions.csv"
    if ed.exists():
        with open(ed, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                folder = (r.get("folder") or "").strip()
                doc = (r.get("transkribus_doc_id") or "").strip()
                col = (r.get("transkribus_collection_id") or "").strip()
                if folder and doc.isdigit():
                    out[folder] = (int(col) if col.isdigit() else DEFAULT_COL, int(doc))
    # The manuscript manifests win: they record the collection and doc the
    # pages were actually pulled from. editions.csv had Meshumed under the old
    # collection 18874 for months (corrected 2026-06-18), and the MS plays now
    # live in 2372172 regardless of what any stale row says.
    for man in (REPO / "data").glob("*/_ms_pull_manifest.json"):
        try:
            d = json.loads(man.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        doc, col = d.get("doc_id"), d.get("collection", DEFAULT_COL)
        if doc:
            out[man.parent.name] = (int(col), int(doc))
    return out


def page_number(page) -> int | None:
    """Accept a page number, a numeric string, or a `0006_31089289.xml` filename."""
    if isinstance(page, int):
        return page
    s = str(page or "").strip()
    if s.isdigit():
        return int(s)
    m = re.match(r"^(\d+)", s)
    return int(m.group(1)) if m else None


def page_url(folder: str, page) -> str:
    """Deep link to one page, or "" when the play or page can't be resolved."""
    ref = _doc_map().get(folder)
    n = page_number(page)
    if not ref or n is None:
        return ""
    col, doc = ref
    return DEEPLINK.format(col=col, doc=doc, page=n)


def doc_url(folder: str) -> str:
    ref = _doc_map().get(folder)
    if not ref:
        return ""
    col, doc = ref
    return f"https://app.transkribus.org/collection/{col}/doc/{doc}"
