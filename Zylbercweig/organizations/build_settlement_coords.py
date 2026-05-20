"""Fill `settlement_coords.tsv` with (lat, lon) for every QID in the index.

Idempotent: reads existing cache, skips QIDs already present, queries Wikidata
SPARQL in batches for the rest, appends results. Safe to re-run.

Usage:
    python Zylbercweig/organizations/build_settlement_coords.py
"""
from __future__ import annotations

import csv
import datetime as _dt
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

_BASE = Path(__file__).resolve().parent
_COORDS = _BASE / "settlement_coords.tsv"
_HEADERS = ["qid", "lat", "lon", "source", "fetched_at"]
_API = "https://www.wikidata.org/w/api.php"
_UA = "Dybbuk-SettlementCoords/1.0 (https://github.com/sinairusinek/Dybbuk)"
_BATCH = 50  # wbgetentities allows up to 50 IDs per request


def _load_cache() -> dict[str, tuple[float, float]]:
    out: dict[str, tuple[float, float]] = {}
    if not _COORDS.exists():
        return out
    with _COORDS.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            try:
                out[row["qid"]] = (float(row["lat"]), float(row["lon"]))
            except (ValueError, KeyError):
                pass
    return out


def _append(rows: list[dict[str, str]]) -> None:
    is_new = not _COORDS.exists()
    with _COORDS.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_HEADERS, delimiter="\t")
        if is_new:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def _fetch_batch(qids: list[str]) -> dict[str, tuple[float, float]]:
    """Use the wbgetentities action API — independent of the WDQS endpoint,
    so it stays available during SPARQL outages. Returns {qid: (lat, lon)}.
    """
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "props": "claims",
        "format": "json",
    }
    url = _API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        import json
        data = json.load(resp)
    out: dict[str, tuple[float, float]] = {}
    for qid, ent in (data.get("entities") or {}).items():
        claims = (ent.get("claims") or {}).get("P625") or []
        for cl in claims:
            try:
                v = cl["mainsnak"]["datavalue"]["value"]
                out[qid] = (float(v["latitude"]), float(v["longitude"]))
                break
            except (KeyError, TypeError, ValueError):
                continue
    return out


def main() -> int:
    sys.path.insert(0, str(_BASE))
    from settlement_index import SettlementIndex

    ix = SettlementIndex()
    all_qids = [qid for qid, _en, _yi in ix.cities()]
    cached = _load_cache()
    todo = [q for q in all_qids if q not in cached and q.startswith("Q")]

    print(f"cities: {len(all_qids)} · cached: {len(cached)} · to fetch: {len(todo)}")
    if not todo:
        return 0

    now = _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    fetched = 0
    inter_batch_sleep = 0.5
    i = 0
    batch_idx = 0
    total_batches = (len(todo) - 1) // _BATCH + 1
    while i < len(todo):
        batch = todo[i : i + _BATCH]
        try:
            hits = _fetch_batch(batch)
        except Exception as exc:
            msg = str(exc)
            wait = 75 if "429" in msg else 10
            print(f"batch {batch_idx + 1}/{total_batches}: error {msg} — sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            inter_batch_sleep = max(inter_batch_sleep, 65.0)
            continue
        rows = [
            {"qid": q, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}", "source": "wikidata-P625", "fetched_at": now}
            for q, (lat, lon) in hits.items()
        ]
        if rows:
            _append(rows)
            fetched += len(rows)
        print(f"batch {batch_idx + 1}/{total_batches}: +{len(rows)} (miss {len(batch) - len(rows)})", flush=True)
        i += _BATCH
        batch_idx += 1
        if i < len(todo):
            time.sleep(inter_batch_sleep)

    print(f"done — fetched {fetched} new coords; total cached {len(cached) + fetched}/{len(all_qids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
