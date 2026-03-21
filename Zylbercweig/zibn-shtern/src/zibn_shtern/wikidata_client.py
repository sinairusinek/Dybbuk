from __future__ import annotations

import json
import time
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

_USER_AGENT = (
    "zibn-shtern/1.0 (Zylbercweig place-name reconciliation pipeline; "
    "https://github.com/Dybbuk/Zylbercweig; Python/urllib)"
)


def _http_get_json(url: str, timeout: int = 20) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": _USER_AGENT})
    # Wikimedia occasionally drops long-running connections; retry a few times.
    for attempt in range(3):
        try:
            with urlopen(req, timeout=timeout) as response:  # nosec B310 - controlled URL
                return json.loads(response.read().decode("utf-8"))
        except RemoteDisconnected:
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError("unreachable")


def normalize_qid(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.upper().startswith("Q") and text[1:].isdigit():
        return f"Q{int(text[1:])}"
    return None


def extract_qids(value: Any) -> list[str]:
    if value is None:
        return []
    text = str(value)
    if not text.strip():
        return []

    out: list[str] = []
    for token in text.replace("|", " ").replace(";", " ").replace(",", " ").split():
        token = token.strip()
        if token.upper().startswith("Q") and token[1:].isdigit():
            out.append(f"Q{int(token[1:])}")

    # preserve order and dedupe
    seen: set[str] = set()
    deduped: list[str] = []
    for qid in out:
        if qid not in seen:
            deduped.append(qid)
            seen.add(qid)
    return deduped


def _extract_entity_ids(entity: dict[str, Any], prop: str) -> list[str]:
    claims = entity.get("claims", {}).get(prop, [])
    out: list[str] = []
    for claim in claims:
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        value = datavalue.get("value", {})
        if isinstance(value, dict) and value.get("id", "").startswith("Q"):
            out.append(value["id"])
    return out


def _extract_labels(entity: dict[str, Any]) -> tuple[str | None, str | None]:
    labels = entity.get("labels", {})
    en = labels.get("en", {}).get("value")
    yi = labels.get("yi", {}).get("value")
    return en, yi


def fetch_entity_data(qid: str, timeout: int = 20) -> dict[str, Any] | None:
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    try:
        payload = _http_get_json(url, timeout=timeout)
    except (HTTPError, URLError, TimeoutError, RemoteDisconnected):
        return None

    entity = payload.get("entities", {}).get(qid)
    if not entity:
        return None

    label_en, label_yi = _extract_labels(entity)
    return {
        "qid": qid,
        "label_en": label_en,
        "label_yi": label_yi,
        "p31": _extract_entity_ids(entity, "P31"),
        "p17": _extract_entity_ids(entity, "P17"),
        "p131": _extract_entity_ids(entity, "P131"),
    }


def fetch_qid_labels(qids: list[str], timeout: int = 20) -> dict[str, dict[str, str | None]]:
    if not qids:
        return {}

    # Avoid oversized wbgetentities URLs by chunking IDs.
    chunk_size = 50
    out: dict[str, dict[str, str | None]] = {}

    for i in range(0, len(qids), chunk_size):
        batch = qids[i : i + chunk_size]
        ids = "|".join(batch)
        params = urlencode(
            {
                "action": "wbgetentities",
                "format": "json",
                "ids": ids,
                "props": "labels",
                "languages": "en|yi",
            }
        )
        url = f"https://www.wikidata.org/w/api.php?{params}"

        try:
            payload = _http_get_json(url, timeout=timeout)
        except (HTTPError, URLError, TimeoutError, RemoteDisconnected):
            continue

        entities = payload.get("entities", {})
        for qid, entity in entities.items():
            en = entity.get("labels", {}).get("en", {}).get("value")
            yi = entity.get("labels", {}).get("yi", {}).get("value")
            out[qid] = {"label_en": en, "label_yi": yi}

    return out


def search_settlement_by_label(label: str, limit: int = 10, timeout: int = 20) -> list[dict[str, str]]:
    """Search Wikidata for entities whose label matches *label*.

    Returns up to *limit* candidates as dicts with keys: qid, label,
    description.  The description (a short Wikidata summary string) can be
    used as a quick indicator of whether a candidate is a settlement before
    fetching its full entity data.
    """
    params = urlencode(
        {
            "action": "wbsearchentities",
            "format": "json",
            "search": label,
            "language": "en",
            "type": "item",
            "limit": limit,
        }
    )
    url = f"https://www.wikidata.org/w/api.php?{params}"
    try:
        payload = _http_get_json(url, timeout=timeout)
    except (HTTPError, URLError, TimeoutError, RemoteDisconnected):
        return []

    results: list[dict[str, str]] = []
    for item in payload.get("search", []):
        qid = item.get("id", "")
        if qid.startswith("Q"):
            results.append(
                {
                    "qid": qid,
                    "label": item.get("label", ""),
                    "description": item.get("description", ""),
                }
            )
    return results


def _fetch_into_cache(cache: dict[str, dict[str, Any]], qid: str, refresh: bool = False) -> None:
    if not qid:
        return
    if refresh or qid not in cache:
        detail = fetch_entity_data(qid)
        if detail is not None:
            cache[qid] = detail


def _collect_p131_ancestors(
    seed_qids: list[str],
    cache: dict[str, dict[str, Any]],
    refresh: bool,
    ancestor_depth: int,
) -> set[str]:
    if ancestor_depth <= 0:
        return set()

    frontier: set[str] = set(seed_qids)
    visited: set[str] = set()
    ancestors: set[str] = set()

    for _ in range(ancestor_depth):
        if not frontier:
            break

        next_frontier: set[str] = set()
        for qid in frontier:
            if qid in visited:
                continue
            visited.add(qid)

            _fetch_into_cache(cache, qid, refresh=refresh)
            detail = cache.get(qid)
            if not detail:
                continue

            for parent in detail.get("p131", []):
                ancestors.add(parent)
                if parent not in visited:
                    next_frontier.add(parent)

        frontier = next_frontier

    return ancestors


def _enrich_labels(cache: dict[str, dict[str, Any]], qids_to_enrich: set[str]) -> None:
    # label enrich for referenced types/locations
    ref_qids: set[str] = set()
    for qid in qids_to_enrich:
        detail = cache.get(qid)
        if not detail:
            continue
        ref_qids.update(detail.get("p31", []))
        ref_qids.update(detail.get("p17", []))
        ref_qids.update(detail.get("p131", []))

    label_map = fetch_qid_labels(sorted(ref_qids))

    for qid in qids_to_enrich:
        detail = cache.get(qid)
        if not detail:
            continue
        detail["p31_labels"] = [label_map.get(x, {}).get("label_en") or x for x in detail.get("p31", [])]
        detail["p17_labels"] = [label_map.get(x, {}).get("label_en") or x for x in detail.get("p17", [])]
        detail["p131_labels"] = [label_map.get(x, {}).get("label_en") or x for x in detail.get("p131", [])]


def load_cache(cache_path: str | Path) -> dict[str, Any]:
    path = Path(cache_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(cache_path: str | Path, payload: dict[str, Any]) -> None:
    path = Path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def get_qid_details(
    qids: list[str],
    cache_path: str | Path,
    refresh: bool = False,
    ancestor_depth: int = 0,
    checkpoint_every: int = 50,
) -> dict[str, dict[str, Any]]:
    cache: dict[str, dict[str, Any]] = load_cache(cache_path)

    requested = [q for q in qids if q]
    total = len(requested)
    for i, qid in enumerate(requested, 1):
        _fetch_into_cache(cache, qid, refresh=refresh)
        if i % checkpoint_every == 0:
            save_cache(cache_path, cache)
            print(f"  [{i}/{total}] fetched, cache saved", flush=True)

    ancestors = _collect_p131_ancestors(requested, cache, refresh=refresh, ancestor_depth=ancestor_depth)
    qids_to_return = set(requested) | ancestors

    # fetch any ancestor QIDs not yet in cache
    ancestor_only = ancestors - set(requested)
    total_anc = len(ancestor_only)
    for i, qid in enumerate(sorted(ancestor_only), 1):
        _fetch_into_cache(cache, qid, refresh=refresh)
        if i % checkpoint_every == 0:
            save_cache(cache_path, cache)
            print(f"  [ancestors {i}/{total_anc}] fetched, cache saved", flush=True)

    _enrich_labels(cache, qids_to_enrich=qids_to_return)

    save_cache(cache_path, cache)
    return {qid: cache[qid] for qid in qids_to_return if qid in cache}
