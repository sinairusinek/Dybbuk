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
_COORDS = _BASE / "settlement_coords.tsv"

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


def _cluster_settlement_strings(row: dict[str, str]) -> list[str]:
    """Extract the raw settlement strings from an alignment row's
    `extracted_settlements` (a JSON list, a JSON scalar, or a plain string)."""
    raw = (row.get("extracted_settlements") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return [raw]
    if isinstance(data, list):
        return [str(s) for s in data]
    if data:
        return [str(data)]
    return []


def _split_linked_ids(raw: str) -> list[str]:
    """linked_cluster_ids is pipe- or comma-separated."""
    out: list[str] = []
    for piece in (raw or "").replace(",", "|").split("|"):
        piece = piece.strip()
        if piece and piece not in out:
            out.append(piece)
    return out


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
        # Per-city precomputed aggregates (populated by _finalize after _build)
        # so that the audit view's selectbox format_func and sort comparator
        # don't re-walk every bucket on every render.
        self._mentions_by_qid: dict[str, int] = {}
        self._buckets_by_qid: dict[str, list[CityBucket]] = {}
        self._dominant_by_qid: dict[str, str] = {}
        self._types_by_qid: dict[str, list[str]] = {}
        self._build()
        self._finalize_aggregates()

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

        # cluster_id → its resolved settlements. Built first so a freshly-minted
        # DB row (which has no address yet) can fall back to the settlements of
        # the clusters it was minted from — otherwise it resolves to nothing and
        # is dropped from every bucket, making "Mint as new entity" look like a
        # no-op in the audit view.
        cluster_resolutions: dict[str, list[ResolvedSettlement]] = {}
        if _ALIGNMENT.exists():
            with _ALIGNMENT.open() as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    cid = (row.get("cluster_id") or "").strip()
                    if not cid:
                        continue
                    settlement_strs = _cluster_settlement_strings(row)
                    cluster_resolutions[cid] = _resolve_cluster_settlements(
                        ";".join(settlement_strs)
                    )

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
                        # Fall back to linked clusters' settlements (e.g. a row
                        # just minted from a cluster, before any address review).
                        seen: dict[str, ResolvedSettlement] = {}
                        for cid in _split_linked_ids(row.get("linked_cluster_ids", "")):
                            for hit in cluster_resolutions.get(cid, ()):
                                seen.setdefault(hit.qid, hit)
                        resolutions = list(seen.values())
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
                    cid = row["cluster_id"]
                    settlement_strs = _cluster_settlement_strings(row)
                    resolutions = cluster_resolutions.get(
                        cid
                    ) or _resolve_cluster_settlements(";".join(settlement_strs))
                    if not resolutions:
                        continue
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

    def _finalize_aggregates(self) -> None:
        """Single O(N_buckets) pass that fills the per-qid lookup tables used
        by the audit view, so subsequent calls are O(1).
        """
        by_qid: dict[str, list[CityBucket]] = {}
        for (q, _t), b in self._buckets.items():
            by_qid.setdefault(q, []).append(b)
        for q, bs in by_qid.items():
            bs.sort(key=lambda b: -(len(b.db_cards) + len(b.clusters)))
            self._buckets_by_qid[q] = bs
            self._types_by_qid[q] = sorted({b.org_type for b in bs})
            mentions = 0
            best_t = ""
            best_n = 0
            for b in bs:
                n = len(b.db_cards) + len(b.clusters)
                mentions += len(b.db_cards) + sum(max(c.cluster_size, 1) for c in b.clusters)
                if n > best_n:
                    best_n = n
                    best_t = b.org_type
            self._mentions_by_qid[q] = mentions
            self._dominant_by_qid[q] = best_t

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
        return self._types_by_qid.get(qid, [])

    def mentions_in_city(self, qid: str) -> int:
        return self._mentions_by_qid.get(qid, 0)

    def buckets_in_city(self, qid: str) -> list[CityBucket]:
        return self._buckets_by_qid.get(qid, [])

    def dominant_org_type(self, qid: str) -> str:
        return self._dominant_by_qid.get(qid, "")


@lru_cache(maxsize=1)
def load_coords() -> dict[str, tuple[float, float]]:
    out: dict[str, tuple[float, float]] = {}
    if not _COORDS.exists():
        return out
    with _COORDS.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            qid = (row.get("qid") or "").strip()
            try:
                lat = float(row.get("lat") or "")
                lon = float(row.get("lon") or "")
            except ValueError:
                continue
            if qid:
                out[qid] = (lat, lon)
    return out


def coords_for(qid: str) -> tuple[float, float] | None:
    return load_coords().get(qid)


@lru_cache(maxsize=1)
def get_index() -> SettlementIndex:
    return SettlementIndex()
