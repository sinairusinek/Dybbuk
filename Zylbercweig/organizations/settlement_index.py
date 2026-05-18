"""Build a (settlement_qid, org_type) → {db_rows, clusters} index.

Powers the "same city + same type" lens used by:
  - the per-cluster Siblings expander in `Organizations matching` (in-flow)
  - the standalone Settlement audit view

Itinerant org types (`is_itinerant`) are excluded — a Traveling Company in
Lodz isn't a "Lodz organization" in the sense the lens is for.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Iterable

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))
from pre_explode_clusters import is_itinerant  # noqa: E402
from settlement_resolver import ResolvedSettlement, get_resolver  # noqa: E402

csv.field_size_limit(10**9)

_BASE = Path(__file__).resolve().parent
_CORE_DB = _BASE / "core_db.tsv"
_ADDRESSES = _BASE / "org_addresses_review.tsv"
_ALIGNMENT = _BASE / "org_alignment_review.tsv"

_CLUSTER_SETTLEMENT_COL = "_ - organizations - _ - locations - _ - settlement"


@dataclass(frozen=True)
class DbCard:
    db_id: str
    name: str
    name_yiddish: str
    org_type: str
    confirmed_settlement: str  # raw, for display
    extracted_settlements: tuple[str, ...]


@dataclass(frozen=True)
class ClusterCard:
    cluster_id: str
    canonical_yiddish: str
    org_type: str
    cluster_size: int
    decision: str
    aligned_db_id: str
    settlement_raw: str


@dataclass
class CityBucket:
    qid: str
    english: str
    yiddish: str
    org_type: str
    db_cards: list[DbCard] = field(default_factory=list)
    clusters: list[ClusterCard] = field(default_factory=list)


def _resolve_db_settlements(row: dict[str, str]) -> list[ResolvedSettlement]:
    """A DB row can sit in multiple cities (multi-location). Resolve all."""
    R = get_resolver()
    seen: dict[str, ResolvedSettlement] = {}

    def add(s: str) -> None:
        hit = R.resolve(s)
        if hit:
            seen.setdefault(hit.qid, hit)

    add(row.get("confirmed_settlement_yiddish", ""))
    add(row.get("confirmed_settlement", ""))
    raw = (row.get("extracted_settlements") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                for s in data:
                    add(str(s))
        except Exception:
            pass
    # confirmed_locations is a JSON list of full address objects for multi-loc rows
    locs = (row.get("confirmed_locations") or "").strip()
    if locs:
        try:
            data = json.loads(locs)
            if isinstance(data, list):
                for loc in data:
                    if isinstance(loc, dict):
                        add(loc.get("settlement_yiddish") or "")
                        add(loc.get("settlement") or "")
        except Exception:
            pass
    return list(seen.values())


def _resolve_cluster_settlements(raw: str) -> list[ResolvedSettlement]:
    R = get_resolver()
    seen: dict[str, ResolvedSettlement] = {}
    for sub in (raw or "").replace("|", ";").split(";"):
        sub = sub.strip()
        if not sub:
            continue
        hit = R.resolve(sub)
        if hit:
            seen.setdefault(hit.qid, hit)
    return list(seen.values())


class SettlementIndex:
    def __init__(self) -> None:
        # (qid, org_type) → CityBucket
        self._buckets: dict[tuple[str, str], CityBucket] = {}
        # cluster_id → list of (qid, org_type) keys it belongs to
        self._cluster_keys: dict[str, list[tuple[str, str]]] = defaultdict(list)
        # db_id → list of keys
        self._db_keys: dict[str, list[tuple[str, str]]] = defaultdict(list)
        # All distinct cities + types, for the audit-view dropdowns
        self._cities: dict[str, tuple[str, str]] = {}  # qid → (english, yiddish)
        self._build()

    def _get(self, qid: str, english: str, yiddish: str, org_type: str) -> CityBucket:
        key = (qid, org_type)
        b = self._buckets.get(key)
        if b is None:
            b = CityBucket(qid=qid, english=english, yiddish=yiddish, org_type=org_type)
            self._buckets[key] = b
            self._cities.setdefault(qid, (english, yiddish))
        return b

    def _build(self) -> None:
        # DB rows: join core_db + org_addresses_review by db_id
        addrs: dict[str, dict[str, str]] = {}
        if _ADDRESSES.exists():
            with _ADDRESSES.open() as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    addrs[row["db_id"]] = row

        if _CORE_DB.exists():
            with _CORE_DB.open() as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    org_type = (row.get("org_type") or "").strip()
                    if not org_type or is_itinerant(org_type):
                        continue
                    db_id = row["db_id"]
                    addr_row = addrs.get(db_id, {})
                    resolutions = _resolve_db_settlements(addr_row)
                    if not resolutions:
                        continue
                    card = DbCard(
                        db_id=db_id,
                        name=(row.get("name") or "").strip(),
                        name_yiddish=(row.get("name_yiddish") or "").strip(),
                        org_type=org_type,
                        confirmed_settlement=(addr_row.get("confirmed_settlement") or "").strip(),
                        extracted_settlements=tuple(
                            s.english or s.yiddish for s in resolutions
                        ),
                    )
                    for r in resolutions:
                        bucket = self._get(r.qid, r.english, r.yiddish, org_type)
                        bucket.db_cards.append(card)
                        self._db_keys[db_id].append((r.qid, org_type))

        # Clusters: from org_alignment_review (one row per cluster)
        if _ALIGNMENT.exists():
            with _ALIGNMENT.open() as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    org_type = (row.get("org_type") or "").strip()
                    if not org_type or is_itinerant(org_type):
                        continue
                    raw = (row.get("extracted_settlements") or "").strip()
                    settlement_strs: list[str] = []
                    if raw:
                        try:
                            data = json.loads(raw)
                            if isinstance(data, list):
                                settlement_strs = [str(s) for s in data]
                            elif data:
                                settlement_strs = [str(data)]
                        except Exception:
                            settlement_strs = [raw]
                    resolutions = _resolve_cluster_settlements(";".join(settlement_strs))
                    if not resolutions:
                        continue
                    cid = row["cluster_id"]
                    try:
                        size = int(row.get("cluster_size") or 0)
                    except ValueError:
                        size = 0
                    card = ClusterCard(
                        cluster_id=cid,
                        canonical_yiddish=(row.get("canonical_yiddish") or "").strip(),
                        org_type=org_type,
                        cluster_size=size,
                        decision=(row.get("decision") or "").strip(),
                        aligned_db_id=(row.get("aligned_db_id") or "").strip(),
                        settlement_raw=settlement_strs[0] if settlement_strs else "",
                    )
                    for r in resolutions:
                        bucket = self._get(r.qid, r.english, r.yiddish, org_type)
                        bucket.clusters.append(card)
                        self._cluster_keys[cid].append((r.qid, org_type))

    # ─── Lookup API ───────────────────────────────────────────────────────
    def siblings_for_cluster(self, cluster_id: str) -> list[CityBucket]:
        return [self._buckets[k] for k in self._cluster_keys.get(cluster_id, [])]

    def siblings_for_db(self, db_id: str) -> list[CityBucket]:
        return [self._buckets[k] for k in self._db_keys.get(db_id, [])]

    def bucket(self, qid: str, org_type: str) -> CityBucket | None:
        return self._buckets.get((qid, org_type))

    def cities(self) -> list[tuple[str, str, str]]:
        """All cities present in the index, sorted by english name.
        Returns (qid, english, yiddish)."""
        return sorted(
            ((qid, en, yi) for qid, (en, yi) in self._cities.items()),
            key=lambda t: (t[1] or t[2]).lower(),
        )

    def org_types_in_city(self, qid: str) -> list[str]:
        return sorted({t for (q, t) in self._buckets if q == qid})


@lru_cache(maxsize=1)
def get_index() -> SettlementIndex:
    return SettlementIndex()
