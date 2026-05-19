"""Diagnose entities whose multiple settlement entries collapse to one QID.

Scans both sides of the alignment:
  - DB rows: confirmed_locations[].settlement / settlement_yiddish
  - Clusters: extracted_settlements (pipe / JSON list)

For each entity with ≥2 distinct settlement strings, resolve every string
through the kimatch-backed settlement_resolver and group by QID. Print every
group where ≥2 strings collapse to the same QID — these are the merge
candidates. Address strings are NOT considered.

Read-only: makes no changes.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_resolver import get_resolver  # noqa: E402

csv.field_size_limit(10**9)

_ADDR = _HERE / "org_addresses_review.tsv"
_ALIGN = _HERE / "org_alignment_review.tsv"


def _looks_conflicted(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("rb") as f:
        head = f.read(8)
    return head.startswith(b"<<<<<<<")


def _extract_settlement_strings_from_loc(loc: dict) -> list[str]:
    out = []
    for key in ("settlement", "settlement_yiddish"):
        s = (loc.get(key) or "").strip()
        if s:
            out.append(s)
    return out


def _parse_extracted_settlements(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    # JSON list?
    if raw.startswith("["):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(s).strip() for s in data if str(s).strip()]
        except Exception:
            pass
    # pipe or semicolon separated
    parts = [p.strip() for p in raw.replace(";", "|").split("|")]
    return [p for p in parts if p]


def _report(label: str, ident: str, settlement_strs: list[str], R) -> bool:
    """Return True if a collision was printed for this entity."""
    distinct = list(dict.fromkeys(settlement_strs))  # dedupe but keep order
    if len(distinct) < 2:
        return False

    groups: dict[str | None, list[str]] = defaultdict(list)
    for s in distinct:
        hit = R.resolve(s)
        groups[hit.qid if hit else None].append(s)

    collisions = {q: g for q, g in groups.items() if q is not None and len(g) >= 2}
    if not collisions:
        return False

    print(f"\n{label} {ident}")
    for qid, group in collisions.items():
        sample = R.resolve(group[0])
        canon = sample.english or sample.yiddish if sample else qid
        print(f"  → {qid} ({canon})")
        for s in group:
            print(f"     · {s!r}")
    unresolved = groups.get(None, [])
    if unresolved:
        print(f"  unresolved: {unresolved}")
    return True


def main() -> None:
    if _looks_conflicted(_ADDR) or _looks_conflicted(_ALIGN):
        print("ERROR: TSV files have unresolved merge conflict markers. "
              "Resolve conflicts before running.", file=sys.stderr)
        sys.exit(1)

    R = get_resolver()
    db_hits = 0
    cl_hits = 0

    # ── DB side ────────────────────────────────────────────────────────
    print("=" * 70)
    print("DB rows with same-QID collisions across confirmed_locations")
    print("=" * 70)
    if _ADDR.exists():
        with _ADDR.open() as f:
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
                strs: list[str] = []
                for loc in locs:
                    if isinstance(loc, dict):
                        strs.extend(_extract_settlement_strings_from_loc(loc))
                name = (row.get("canonical_yiddish") or "").strip()
                ident = f"db_id={row.get('db_id','?')}  {name}"
                if _report("DB", ident, strs, R):
                    db_hits += 1

    # ── Cluster side ───────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("Clusters with same-QID collisions across extracted_settlements")
    print("=" * 70)
    if _ALIGN.exists():
        with _ALIGN.open() as f:
            for row in csv.DictReader(f, delimiter="\t"):
                strs = _parse_extracted_settlements(row.get("extracted_settlements", ""))
                if len(strs) < 2:
                    continue
                name = (row.get("canonical_yiddish") or "").strip()
                ident = f"cluster={row.get('cluster_id','?')}  {name}"
                if _report("CL", ident, strs, R):
                    cl_hits += 1

    print("\n" + "─" * 70)
    print(f"DB rows with collisions      : {db_hits}")
    print(f"Clusters with collisions     : {cl_hits}")


if __name__ == "__main__":
    main()
