"""Find DB rows whose multiple `confirmed_locations` entries resolve to
the same place (via the kimatch resolver + the curated punchlist
overlay).

Read-only diagnostic. Prints, per affected row:
  - the canonical_yiddish + db_id
  - each location's settlement strings and their resolved QID
  - the same-QID groups that would collapse

Pass --include-resolver-misses to also surface rows where some entries
fail to resolve via overlay (so you know where punchlist gaps remain).
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_overlay import resolve_anyplace  # noqa: E402

csv.field_size_limit(10**9)
_ADDR = _HERE / "org_addresses_review.tsv"


def _strings_from_loc(loc: dict) -> list[str]:
    out = []
    for key in ("settlement", "settlement_yiddish"):
        s = (loc.get(key) or "").strip()
        if s:
            out.append(s)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--include-resolver-misses", action="store_true",
                    help="Also print rows that have unresolved entries.")
    args = ap.parse_args()

    hits = 0
    misses = 0

    with _ADDR.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            raw = (row.get("confirmed_locations") or "").strip()
            if not raw or raw == "[]":
                continue
            try:
                locs = json.loads(raw)
            except Exception:
                continue
            if not isinstance(locs, list) or len(locs) < 2:
                continue

            db_id = row.get("db_id", "?")
            name = (row.get("canonical_yiddish") or "").strip()

            # Per location, pick the first resolvable string.
            per_loc: list[tuple[int, str | None, str | None, dict]] = []
            for i, loc in enumerate(locs):
                if not isinstance(loc, dict):
                    continue
                resolved = None
                trigger = None
                for s in _strings_from_loc(loc):
                    hit = resolve_anyplace(s)
                    if hit:
                        resolved = hit
                        trigger = s
                        break
                per_loc.append((i, resolved.qid if resolved else None,
                                trigger, loc))

            groups: dict[str | None, list[tuple[int, str | None, dict]]] = defaultdict(list)
            for i, qid, trig, loc in per_loc:
                groups[qid].append((i, trig, loc))

            collisions = {q: g for q, g in groups.items()
                          if q is not None and len(g) >= 2}
            unresolved = groups.get(None, [])

            if collisions:
                hits += 1
                print(f"\n=== db_id {db_id} · {name} · {len(locs)} locations ===")
                for qid, group in collisions.items():
                    sample = next((resolve_anyplace(trig) for _, trig, _ in group if trig), None)
                    label = (sample.english or sample.yiddish) if sample else qid
                    src = sample.source if sample else "?"
                    print(f"  COLLAPSE → {qid} · {label} · [{src}]")
                    for i, trig, loc in group:
                        addr = (loc.get('address_romanized') or '').strip()
                        print(f"    #{i}: settlement={loc.get('settlement','')!r}"
                              f" yi={loc.get('settlement_yiddish','')!r}"
                              + (f" addr={addr!r}" if addr else ""))
                if unresolved and args.include_resolver_misses:
                    print(f"  (also unresolved entries: {len(unresolved)})")
            elif unresolved and args.include_resolver_misses:
                # Optional: surface rows with ≥1 unresolved entry so we
                # know where overlay gaps remain.
                if any(qid is None for _, qid, _, _ in per_loc) and len(per_loc) >= 2:
                    misses += 1
                    print(f"\n=== db_id {db_id} · {name} · {len(locs)} locations · unresolved ===")
                    for i, qid, trig, loc in per_loc:
                        mark = f"→ {qid}" if qid else "UNRESOLVED"
                        print(f"  #{i}: settlement={loc.get('settlement','')!r}"
                              f" yi={loc.get('settlement_yiddish','')!r}  {mark}")

    print("\n" + "─" * 70)
    print(f"DB rows with same-QID collapses : {hits}")
    if args.include_resolver_misses:
        print(f"DB rows with unresolved entries : {misses}")


if __name__ == "__main__":
    main()
