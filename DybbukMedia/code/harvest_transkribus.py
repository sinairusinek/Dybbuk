"""Catalogue the visual material held in Transkribus collection 18874.

Collection 18874 is the project's general Yiddish HTR collection (241 docs), not
a dedicated posters collection, so this enumerates it and keeps only the docs
that are visual material for this db:

  821034  Kaminska_ya-rg8-1-f4518  — 49 pages of THEATRE POSTERS. Its `desc`
          is an item-by-item inventory naming our plays with city, year,
          director and troupe, so each bullet is parsed into its own row.
  899977  ya-cc-rg289-d1           — the YIVO CARD CATALOGUE (1551 pp).
  898723  CardCatalogue_ya-cc-rg289-d3 — a second card-catalogue run (994 pp).
  817419  blimelePoster            — a single poster.
  ya-rg8-* docs                    — the manuscript plays, already editions.

Run with --refresh to re-query Transkribus; otherwise the cached doc list in
data/transkribus_18874_docs.json is used, so a re-run needs no credentials.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent
                       / "YiDraCor" / "code"))

from common import (ROOT, clean, load_entities, load_title_index, merge,
                    propose_title, read_manifest, write_manifest, year_of)

COLLECTION = 18874
CACHE = ROOT / "data" / "transkribus_18874_docs.json"

POSTER_DOCS = {821034}
CARD_DOCS = {899977, 898723}
SINGLE_POSTER_DOCS = {817419}


def fetch_docs(refresh: bool) -> list[dict]:
    """Doc list for the collection, from cache unless --refresh."""
    if not refresh and CACHE.exists():
        return json.loads(CACHE.read_text(encoding="utf-8"))
    from transkribus.client import TrpClient          # noqa: PLC0415
    c = TrpClient.from_env()
    docs = c.session.get(f"{c.base}/collections/{COLLECTION}/list",
                         timeout=60).json()
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(docs, ensure_ascii=False, indent=1),
                     encoding="utf-8")
    return docs


def parse_inventory(desc: str) -> list[dict]:
    """Split doc 821034's description into one entry per poster group.

    Each bullet looks like:
      - "Khinke-Pinke," 4 items. In Warsaw, 1914, troupe of Kompaneyets. In ...
    so the quoted title, the item count, and the per-item clauses are all
    recoverable.
    """
    out = []
    for raw in re.split(r"\n\s*-\s+", "\n" + (desc or "")):
        raw = clean(raw)
        if not raw:
            continue
        m = re.match(r'"([^"]+)[,."]*"?\s*(?:,\s*(\d+)\s+items?\.)?\s*(.*)',
                     raw)
        if not m:
            continue
        title, count, rest = m.group(1).strip(" ,."), m.group(2), m.group(3)
        if not title:
            continue
        out.append({"title": title, "count": int(count) if count else 1,
                    "detail": rest.strip()})
    return out


def main() -> None:
    refresh = "--refresh" in sys.argv
    ents = load_entities()
    idx = load_title_index(ents)
    try:
        docs = fetch_docs(refresh)
    except SystemExit as exc:          # missing credentials
        print(f"transkribus: {exc}; run with --refresh once credentials are set")
        return

    by_id = {int(d["docId"]): d for d in docs if d.get("docId")}
    rows = []

    # --- doc 821034: the poster inventory -------------------------------
    for doc_id in sorted(POSTER_DOCS):
        d = by_id.get(doc_id)
        if not d:
            continue
        url = f"https://app.transkribus.org/collection/{COLLECTION}/doc/{doc_id}"
        entries = parse_inventory(d.get("desc") or "")
        for j, e in enumerate(entries, start=1):
            ids, status = propose_title(ents, idx, e["title"])
            rows.append({
                "media_id": f"tk{doc_id}-{j:03d}",
                "source": "transkribus",
                "item_type": "poster",
                "title": e["title"],
                "date": year_of(e["detail"]),
                "date_note": "" if year_of(e["detail"]) else "undated",
                "place": clean(re.match(r"In ([^,.]+)", e["detail"]).group(1))
                         if re.match(r"In ([^,.]+)", e["detail"]) else "",
                "holding_institution": "YIVO",
                "collection": f"RG 8 ({clean(d.get('title'))})",
                "signature": "ya-rg8-1-f4518",
                "source_url": url,
                "fetch_hint": (f"transkribus col {COLLECTION} doc {doc_id} "
                               f"({d.get('nrOfPages')} pp); locate the "
                               f"{e['count']} poster(s) for {e['title']!r}"),
                "rights": "unknown",
                "entity_ids": ids,
                "play_key": e["title"],
                "link_status": status,
                "status": "PENDING",
                "notes": (f"{e['count']} item(s) in the folder. {e['detail']}"
                          .strip()),
            })

    # --- card catalogues and single posters -----------------------------
    for doc_id in sorted(CARD_DOCS | SINGLE_POSTER_DOCS):
        d = by_id.get(doc_id)
        if not d:
            continue
        is_card = doc_id in CARD_DOCS
        title = clean(d.get("title"))
        ids, status = (("", "GAP") if is_card
                       else propose_title(ents, idx, title))
        rows.append({
            "media_id": f"tk{doc_id}-000",
            "source": "transkribus",
            "item_type": "card_catalog" if is_card else "poster",
            "title": title,
            "holding_institution": "YIVO",
            "collection": ("RG 289 card catalogue" if is_card
                           else f"({title})"),
            "signature": title if title.startswith("ya-") else "",
            "source_url": (f"https://app.transkribus.org/collection/"
                           f"{COLLECTION}/doc/{doc_id}"),
            "fetch_hint": (f"transkribus col {COLLECTION} doc {doc_id} "
                           f"({d.get('nrOfPages')} pp)"),
            "rights": "unknown",
            "entity_ids": ids,
            "link_status": status,
            "status": "PENDING",
            "notes": ("whole-document row: a page-level pass is still needed. "
                      "The workbook sheets cardCatalogAffishenProgramen and "
                      "PerlmutterMSScards look like transcriptions of this "
                      "catalogue — verify the join before treating them as its "
                      "index." if is_card else ""),
        })

    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    print(f"transkribus: {len(by_id)} docs in collection {COLLECTION}, "
          f"{len(rows)} rows -> +{added} added, {updated} updated, "
          f"{protected} human fields kept")


if __name__ == "__main__":
    main()
