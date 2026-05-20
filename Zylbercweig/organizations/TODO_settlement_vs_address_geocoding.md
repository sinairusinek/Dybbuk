# TODO — two-track geolocation policy

The Settlement Audit map header surfaces this distinction; keep the two tracks
strictly separated.

## Settlement-level (city centroid)

- **Automatic.** Derived from the Wikidata QID via kimatch + `build_settlement_coords.py`.
- Idempotent — re-running `python Zylbercweig/organizations/build_settlement_coords.py`
  only fills gaps in `settlement_coords.tsv`.
- No human review needed. The QID is the canonical key; if the QID is correct,
  the centroid is correct enough for "is this city on the map" purposes.

## Address-level (street-precise)

- **Manual.** Lives in `org_addresses_review.tsv` (`lat`, `lon`,
  `confirmed_locations`).
- Set by the reviewer in the Organization Cards view.
- Nominatim is a *starting point* only — every `confirmed_address` lat/lon must
  be reviewer-confirmed. Reason: ghetto-era street names rarely match modern
  OSM data; auto-geocoded pins drift to the wrong block or city.

## Where each track is used

- Map header dots → **settlement-level** only.
- Per-org address pins (if/when added) → **address-level** only, sourced from
  `org_addresses_review.tsv:confirmed_locations`.

Do not mix: never substitute the settlement centroid for a missing address
lat/lon, and never treat a Nominatim hit as confirmed without reviewer sign-off.
