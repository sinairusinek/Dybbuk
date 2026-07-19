#!/usr/bin/env python3
"""Flag address-level pins that sit implausibly far from their own settlement.

Two-track geolocation policy (see TODO_settlement_vs_address_geocoding.md):
settlement centroids are automatic and derived from the QID, while address
lat/lon in `org_addresses_review.tsv` is manual and reviewer-confirmed, with
Nominatim only as a starting point. That doc warns auto-geocoded pins "drift to
the wrong block or city" because ghetto-era street names rarely match modern
OSM data.

This finds where that happened, by treating the settlement centroid as ground
truth and measuring how far the address pin fell from it. A theatre on Pitkin
Avenue whose pin is 60 km out in Suffolk County is not a judgement call.

Bands are deliberately generous — the point is to surface the unambiguous,
not to relitigate every block-level pin:

  >  50 km   almost certainly the wrong city
  >  15 km   suspicious for a pre-war urban address
  > 200 km   wrong country/region

Usage:
    python3.11 audit_address_geocode_outliers.py            # report
    python3.11 audit_address_geocode_outliers.py --csv out.csv
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from settlement_index import _resolve_db_settlements, coords_for  # noqa: E402
from tsv_io import read_tsv, write_tsv  # noqa: E402

_ADDRESSES = Path(__file__).resolve().parent / "org_addresses_review.tsv"

NEAR_KM = 15.0
FAR_KM = 50.0
ABSURD_KM = 200.0


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def band(km: float) -> str:
    if km > ABSURD_KM:
        return "ABSURD"
    if km > FAR_KM:
        return "WRONG-CITY?"
    if km > NEAR_KM:
        return "SUSPICIOUS"
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="write flagged rows to this path")
    ap.add_argument("--min-km", type=float, default=NEAR_KM)
    args = ap.parse_args()

    _headers, rows = read_tsv(_ADDRESSES)

    flagged: list[dict[str, str]] = []
    n_pinned = n_checked = n_nocoord = 0

    for row in rows:
        lat_s, lon_s = (row.get("lat") or "").strip(), (row.get("lon") or "").strip()
        if not lat_s or not lon_s:
            continue
        try:
            pin = (float(lat_s), float(lon_s))
        except ValueError:
            continue
        n_pinned += 1

        # Compare against every settlement the row resolves to, and keep the
        # NEAREST. A multi-location org is only an outlier if the pin is far
        # from all of its cities.
        resolutions = _resolve_db_settlements(row)
        centroids = [(r, coords_for(r.qid)) for r in resolutions]
        centroids = [(r, c) for r, c in centroids if c]
        if not centroids:
            n_nocoord += 1
            continue
        n_checked += 1

        best_r, best_km = None, float("inf")
        for r, c in centroids:
            km = haversine_km(pin, c)
            if km < best_km:
                best_r, best_km = r, km

        tag = band(best_km)
        if not tag or best_km < args.min_km:
            continue
        flagged.append({
            "db_id": row.get("db_id", ""),
            "name": row.get("canonical_yiddish", ""),
            "band": tag,
            "km_from_settlement": f"{best_km:.1f}",
            "nearest_settlement": (best_r.english or best_r.qid) if best_r else "",
            "qid": best_r.qid if best_r else "",
            "confirmed_settlement": row.get("confirmed_settlement", ""),
            "confirmed_address_romanized": row.get("confirmed_address_romanized", ""),
            "lat": lat_s,
            "lon": lon_s,
            "reviewer": row.get("reviewer", ""),
        })

    flagged.sort(key=lambda r: -float(r["km_from_settlement"]))

    print(f"rows with an address pin      : {n_pinned}")
    print(f"  checkable (settlement known): {n_checked}")
    print(f"  skipped (no centroid)       : {n_nocoord}")
    print(f"flagged (> {args.min_km:g} km)             : {len(flagged)}")
    by_band: dict[str, int] = {}
    for f in flagged:
        by_band[f["band"]] = by_band.get(f["band"], 0) + 1
    for b in ("ABSURD", "WRONG-CITY?", "SUSPICIOUS"):
        if by_band.get(b):
            print(f"    {b:12} {by_band[b]}")
    print()
    for f in flagged:
        print(
            f"  {f['band']:12} {f['km_from_settlement']:>8} km  "
            f"DB {f['db_id']:>5}  {f['name'][:30]:30}  "
            f"→ {f['nearest_settlement'][:18]:18}  {f['confirmed_address_romanized'][:44]}"
        )

    if args.csv and flagged:
        write_tsv(args.csv, list(flagged[0].keys()), flagged, terminator="\n")
        print(f"\nwrote {len(flagged)} rows → {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
