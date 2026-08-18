"""Settlement audit — self-sufficient settlement workbench.

A spatial + categorical lens over the dedup state:
- Map header: every geo-located settlement as a dot. Color by dominant org
  type or by address-resolution status. Click a dot = pick that city.
- Picker: cities sortable by mention count or alphabetically.
- Per-city: every org_type as its own list (DB rows + clusters interleaved).
- Per-row action menu: Mint as new entity · Merge here · Search more · Research.
- "Research" is gated to the Sinai reviewer and queues a cluster-research job.

Itinerant types are excluded by the index.
"""
from __future__ import annotations

import csv
import math
import pathlib
import re
import sys
from dataclasses import dataclass
from typing import Iterable

import streamlit as st

from zalmen.activity_log import log_action

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

from organizations.settlement_resolver import fold_for_search

from organizations.settlement_index import (
    CityBucket,
    ClusterCard,
    DbCard,
    coords_for,
    get_index,
    load_coords,
)

from views.org_alignment import (
    ALIGN_FILE,
    CORE_DB_FILE,
    CLUSTER_FILE,
    load_alignment,
    load_core_db,
    load_samples,
    save_alignment,
    save_core_db,
    get_entry_text,
    _split_pipe,
    _next_db_id,
    _mtime,
    _JSON_TO_XML,
)

from views.research_queue import (
    ADMIN_REVIEWER,
    RESEARCH_QUEUE,
    queue_research as _queue_research,
)
import mention_removals

# Show this many rows per (type, kind) section by default. Each row spawns
# ~12 widgets (popover body, radio, text input, buttons), so a busy city like
# Warsaw with hundreds of rows × multiple types would otherwise instantiate
# thousands of widgets per Streamlit rerun — enough to make every picker
# interaction feel frozen. A "Show all" toggle lifts the cap on demand.
_ROW_CAP = 20

# Bumped whenever a mutation invalidates the settlement index, so the cached
# folium map (see `_build_map_cached`) is rebuilt only when the underlying data
# actually changed — not on every rerun.
_INDEX_VERSION = 0


def _bump_index_version() -> None:
    global _INDEX_VERSION
    _INDEX_VERSION += 1


def _rerun_local() -> None:
    """Rerun the smallest scope that is legal here.

    Every app-scoped rerun re-mounts the folium iframe in the map header. The
    component reports its height only after it loads, so the page reflows from
    a collapsed header and the browser dumps you back at the top of the view —
    which is what "the audit keeps jumping" means in practice. Row actions
    (merge, mint, show-all, clear selection) only ever change what the
    workbench renders, so a fragment-scoped rerun is enough and leaves the map
    — and your scroll position — alone.

    `scope="fragment"` is rejected outside a fragment rerun, so fall back to a
    full rerun there. RerunException derives from BaseException, so it passes
    straight through this handler.
    """
    from streamlit.errors import StreamlitAPIException

    try:
        st.rerun(scope="fragment")
    except StreamlitAPIException:
        st.rerun()


# Color palette for the dominant-org-type map mode. The typology is ~27 types;
# we colour the most common ones and lump the long tail as grey.
_TYPE_COLORS: dict[str, str] = {
    "Theatre": "#e6194B",
    "Non-Yiddish Theatre": "#7c1b2b",
    "Traveling Company": "#f58231",
    "Company on Tour": "#ffe119",
    "Amateur": "#bfef45",
    "Kleinkunst": "#3cb44b",
    "Publisher": "#42d4f4",
    "Printer": "#4363d8",
    "Printer/Publisher": "#911eb4",
    "Journals/Newspapers": "#f032e6",
    "Media (Radio/Film/TV)": "#a9a9a9",
    "Library": "#9A6324",
    "Heritage Institution": "#800000",
    "Education": "#469990",
    "Musical organization": "#000075",
    "Theatre-related Society/Union": "#808000",
    "Religious institutions": "#fabed4",
    "Religious organizations": "#fabed4",
    "Welfare/Aid": "#dcbeff",
    "Business": "#aaffc3",
    "Labour": "#ffd8b1",
}
_FALLBACK_COLOR = "#888888"


# ─── pure-data helpers ────────────────────────────────────────────────────

def _merge_linked_ids(*values: str) -> str:
    seen: list[str] = []
    for v in values:
        for piece in (p.strip() for p in (v or "").replace(",", "|").split("|")):
            if piece and piece not in seen:
                seen.append(piece)
    return " | ".join(seen)


def _drop_linked_id(value: str, cluster_id: str) -> str:
    """Remove one cluster from a DB row's `linked_cluster_ids` list."""
    keep = [
        p.strip() for p in (value or "").replace(",", "|").split("|")
        if p.strip() and p.strip() != cluster_id
    ]
    return " | ".join(keep)


# Prefix used to mark a merge that changed nothing, so callers can skip the
# activity-log write and show an informational toast instead of a success one.
_ALREADY_PREFIX = "Already merged"


def _addr_status_for_qid(qid: str, ix, addr_by_dbid: dict[str, dict[str, str]]) -> str:
    """green = every DB row in this city has lat+lon, amber = some, red = none."""
    db_ids: set[str] = set()
    for b in ix.buckets_in_city(qid):
        for d in b.db_cards:
            db_ids.add(d.db_id)
    if not db_ids:
        return "gray"
    n_with = 0
    for dbid in db_ids:
        row = addr_by_dbid.get(dbid, {})
        if (row.get("lat") or "").strip() and (row.get("lon") or "").strip():
            n_with += 1
    if n_with == len(db_ids):
        return "green"
    if n_with == 0:
        return "red"
    return "orange"


_ADDR_FILE = BASE / "organizations" / "org_addresses_review.tsv"


@st.cache_data(show_spinner=False)
def _load_addresses_cached(mtime: float) -> dict[str, dict[str, str]]:
    """Cached on file mtime so we don't re-read 669 rows on every rerun."""
    out: dict[str, dict[str, str]] = {}
    if not _ADDR_FILE.exists():
        return out
    with _ADDR_FILE.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            dbid = (row.get("db_id") or "").strip()
            if dbid:
                out[dbid] = row
    return out


def _load_addresses() -> dict[str, dict[str, str]]:
    try:
        return _load_addresses_cached(_ADDR_FILE.stat().st_mtime if _ADDR_FILE.exists() else 0.0)
    except FileNotFoundError:
        return {}


# ─── core data mutations (used by both the top action-bar and per-row menus) ──

def _persist_and_clear(a_headers, a_rows, db_headers, db_rows) -> None:
    save_alignment(a_headers, a_rows)
    save_core_db(db_headers, db_rows)
    load_alignment.clear()
    load_core_db.clear()
    get_index.cache_clear()
    _bump_index_version()


def _align_clusters_to_db(
    target_db: str,
    cluster_ids: list[str],
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
) -> str:
    a_by_cid = {r["cluster_id"]: r for r in a_rows}
    db_by_id = {r.get("db_id", ""): r for r in db_rows}
    # A cluster can already point at a DIFFERENT DB row — a reviewer re-merging
    # something an earlier session already consumed. Overwriting aligned_db_id
    # without unlinking left the old row still claiming the cluster in its
    # linked_cluster_ids, so two DB rows owned one cluster. Unlink first.
    moved: list[str] = []
    touched_prev: set[str] = set()
    for cid in cluster_ids:
        row = a_by_cid.get(cid)
        if not row:
            continue
        prev = (row.get("aligned_db_id") or "").strip()
        if prev and prev != target_db:
            moved.append(cid)
            prev_row = db_by_id.get(prev)
            if prev_row is not None:
                prev_row["linked_cluster_ids"] = _drop_linked_id(
                    prev_row.get("linked_cluster_ids", ""), cid
                )
                touched_prev.add(prev)
        row["decision"] = "ALIGN"
        row["aligned_db_id"] = target_db
    target_row = db_by_id.get(target_db)
    if target_row is not None:
        target_row["linked_cluster_ids"] = _merge_linked_ids(
            target_row.get("linked_cluster_ids", ""), *cluster_ids
        )
    # A previous owner whose LAST cluster we just moved is now active with no
    # clusters — an orphan (this is how 200+ empty-link rows accumulated). We do
    # NOT auto-deprecate it: it may be a legitimate distinct entity the reviewer
    # means to keep, not a duplicate. Instead flag it in the result so the
    # orphaning is never silent; detect_data_defects.py Class C is the standing
    # catch-all, and repair_orphan_links.py handles the safe subset.
    emptied = [p for p in sorted(touched_prev, key=lambda x: (len(x), x))
               if db_by_id.get(p) is not None
               and not (db_by_id[p].get("linked_cluster_ids") or "").strip()]
    _persist_and_clear(a_headers, a_rows, db_headers, db_rows)
    msg = f"Aligned {len(cluster_ids)} cluster(s) → {target_db}"
    if moved:
        msg += f" · moved {len(moved)} off a previous row"
    if emptied:
        msg += (f" · ⚠ DB {', '.join(emptied)} now with no clusters — "
                f"review (DB Audit / detect_data_defects Class C)")
    return msg


def _mint_db_from_clusters(
    cluster_ids: list[str],
    bucket_org_type: str,
    seed_name_yiddish: str,
    seed_name_latin: str,
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
) -> tuple[str, str]:
    next_id = str(_next_db_id(db_rows))
    new_row = {h: "" for h in db_headers}
    new_row.update({
        "db_id": next_id,
        "name": (seed_name_latin or "").strip(),
        "name_yiddish": (seed_name_yiddish or "").strip(),
        "org_type": (bucket_org_type or "").title(),
        "address": "",
        "linked_cluster_ids": _merge_linked_ids(*cluster_ids),
    })
    db_rows.append(new_row)
    a_by_cid = {r["cluster_id"]: r for r in a_rows}
    for cid in cluster_ids:
        row = a_by_cid.get(cid)
        if not row:
            continue
        row["decision"] = "NEW"
        row["aligned_db_id"] = next_id
    _persist_and_clear(a_headers, a_rows, db_headers, db_rows)
    return next_id, f"Created DB {next_id} from {len(cluster_ids)} cluster(s)"


def _consolidate_clusters(
    cluster_ids: list[str],
    bucket_org_type: str,
    seed_name_yiddish: str,
    seed_name_latin: str,
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
) -> tuple[str, str]:
    """Combine cluster_ids into a single DB row, **reusing** any existing target
    row one of them already points to instead of minting a fresh one every time.

    This is the fix for the Gdansk-style corruption where five "Confirm merge"
    clicks on overlapping pairs produced five orphan DB rows. Behaviour:
      - If any input cluster is already aligned to a DB row, the lowest-id one
        wins; every other already-targeted DB row's clusters are re-pointed to
        the winner and the loser rows are deleted.
      - If no input cluster is aligned anywhere, mint one new DB row.
      - Clusters keep their existing decision when present (ALIGN stays ALIGN);
        previously-undecided clusters become NEW.
    """
    a_by_cid = {r["cluster_id"]: r for r in a_rows}

    existing_ids: list[str] = []
    for cid in cluster_ids:
        row = a_by_cid.get(cid)
        if not row:
            continue
        tid = (row.get("aligned_db_id") or "").strip()
        dec = (row.get("decision") or "").strip()
        if tid and dec in ("NEW", "ALIGN") and tid not in existing_ids:
            existing_ids.append(tid)

    if not existing_ids:
        return _mint_db_from_clusters(
            cluster_ids, bucket_org_type, seed_name_yiddish, seed_name_latin,
            a_rows, a_headers, db_rows, db_headers,
        )

    def _id_key(x: str) -> int:
        return int(x) if x.isdigit() else 10**9
    survivor = sorted(existing_ids, key=_id_key)[0]
    losers = [x for x in existing_ids if x != survivor]

    survivor_row = next((r for r in db_rows if r.get("db_id") == survivor), None)
    if survivor_row is None:
        # The dangling reference is the original bug; fall back to mint.
        return _mint_db_from_clusters(
            cluster_ids, bucket_org_type, seed_name_yiddish, seed_name_latin,
            a_rows, a_headers, db_rows, db_headers,
        )

    pieces = [survivor_row.get("linked_cluster_ids", "")]
    for r in db_rows:
        if r.get("db_id") in losers:
            pieces.append(r.get("linked_cluster_ids", ""))
    pieces.extend(cluster_ids)
    survivor_row["linked_cluster_ids"] = _merge_linked_ids(*pieces)

    in_set = set(cluster_ids)
    db_by_id = {r.get("db_id", ""): r for r in db_rows}
    for r in a_rows:
        cid = r.get("cluster_id", "")
        cur_target = (r.get("aligned_db_id") or "").strip()
        if cid in in_set or cur_target in losers:
            # Losers are deleted below, so only a surviving third row needs its
            # back-link cleaned — otherwise it keeps claiming a cluster that has
            # just moved to `survivor`.
            if cur_target and cur_target != survivor and cur_target not in losers:
                stale = db_by_id.get(cur_target)
                if stale is not None:
                    stale["linked_cluster_ids"] = _drop_linked_id(
                        stale.get("linked_cluster_ids", ""), cid
                    )
            r["aligned_db_id"] = survivor
            if not (r.get("decision") or "").strip():
                r["decision"] = "NEW"

    if losers:
        db_rows[:] = [r for r in db_rows if r.get("db_id") not in losers]

    _persist_and_clear(a_headers, a_rows, db_headers, db_rows)
    extra = f" (reclaimed {len(losers)} stray row{'s' if len(losers) != 1 else ''})" if losers else ""
    return survivor, f"Merged {len(cluster_ids)} cluster(s) into DB {survivor}{extra}"


def _merge_db_rows_op(
    primary: str,
    secondaries: list[str],
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
) -> str:
    primary_row = next((r for r in db_rows if r.get("db_id") == primary), None)
    if primary_row is None:
        return "Primary row not found"
    pieces = [primary_row.get("linked_cluster_ids", "")]
    for r in db_rows:
        if r.get("db_id") in secondaries:
            pieces.append(r.get("linked_cluster_ids", ""))
    primary_row["linked_cluster_ids"] = _merge_linked_ids(*pieces)
    for r in a_rows:
        if (r.get("aligned_db_id") or "").strip() in secondaries:
            r["aligned_db_id"] = primary
    db_rows[:] = [r for r in db_rows if r.get("db_id") not in secondaries]
    _persist_and_clear(a_headers, a_rows, db_headers, db_rows)
    return f"Merged {len(secondaries)} row(s) into {primary}"


_RECEIPT_KEY = "sa_last_receipt"


def _record_receipt(qid: str, msg: str, kind: str, ident: str, n_partners: int) -> None:
    """Remember the last mutation so the next render can show it in place.

    `st.toast` disappears after a few seconds and the Actions panel closes on
    the rerun that follows a merge, so nothing on screen said the merge landed —
    which is why reviewers came back and did it again. The receipt survives in
    session_state until the next mutation or a change of city.
    """
    db_id = ""
    m = re.search(r"DB (\d+)", msg or "")
    if m:
        db_id = m.group(1)
    elif kind == "db":
        db_id = ident
    st.session_state[_RECEIPT_KEY] = {
        "qid": qid, "msg": msg or "", "db_id": db_id, "n": n_partners,
    }


def _aligned_target(
    cluster_id: str,
    a_rows_by_cid: dict[str, dict[str, str]],
    db_by_id: dict[str, dict[str, str]],
) -> str:
    """DB row this cluster is already consumed by, or "" if it is still free.

    A pointer at a DB row that no longer exists is *not* consumed: those
    danglers are the residue of rows deleted by later merges, and hiding them
    as "resolved" would strand them.
    """
    row = a_rows_by_cid.get(cluster_id) or {}
    tid = (row.get("aligned_db_id") or "").strip()
    return tid if tid and tid in db_by_id else ""


def _merge_plan(
    self_kind: str,
    self_id: str,
    partners: list[tuple[str, str]],
    a_rows_by_cid: dict[str, dict[str, str]],
    db_by_id: dict[str, dict[str, str]],
) -> tuple[bool, list[str]]:
    """Predict what a proposed merge would do.

    Returns `(is_noop, warnings)`. `is_noop` is True when every cluster in the
    proposal already sits on the one DB row the merge would target — the
    signature of re-merging work a previous session already did. `warnings`
    names each cluster that would be *moved* off a different DB row and each DB
    row that would be absorbed, so the reviewer sees it before confirming
    rather than discovering it in the diff.
    """
    members = [(self_kind, self_id)] + list(partners)
    cluster_ids = [i for k, i in members if k == "cluster"]
    db_ids = [i for k, i in members if k == "db"]

    current = {cid: _aligned_target(cid, a_rows_by_cid, db_by_id) for cid in cluster_ids}

    if db_ids:
        # Routes that involve a DB row always keep a DB row: `self` when the menu
        # was opened on one, otherwise the first picked row.
        target = self_id if self_kind == "db" else db_ids[0]
    else:
        # Cluster-only: _consolidate_clusters keeps the lowest-numbered DB row
        # any member already points at, or mints a fresh one.
        existing = sorted(
            {t for t in current.values() if t},
            key=lambda x: int(x) if x.isdigit() else 10**9,
        )
        target = existing[0] if existing else ""

    if target and all(t == target for t in current.values()) and set(db_ids) <= {target}:
        return True, []

    warnings: list[str] = []
    for cid, cur in current.items():
        if cur and target and cur != target:
            name = (a_rows_by_cid.get(cid) or {}).get("canonical_yiddish", "") or cid
            warnings.append(f"`{cid}` {name[:34]} — currently in **DB {cur}**, would move")
    absorbed = [d for d in db_ids if d != target]
    for d in absorbed:
        nm = (db_by_id.get(d) or {}).get("name", "") or (db_by_id.get(d) or {}).get("name_yiddish", "")
        warnings.append(f"**DB {d}** {nm[:34]} — would be removed and folded in")
    return False, warnings


def _build_research_prompt(
    kind: str,
    target_id: str,
    qid: str,
    org_type: str,
    *,
    canonical_yiddish: str,
    self_label: str,
    samples: dict,
    a_rows_by_cid: dict[str, dict[str, str]],
    db_rows: list[dict[str, str]],
) -> str:
    """Compose a self-contained cluster-research prompt with names + attestations
    so Phase 1 of the skill can start without re-querying the DB.
    """
    lines = [
        f"Use the cluster-research skill on {kind} `{target_id}` "
        f"in city `{qid}` (org_type: {org_type}).",
        "",
    ]
    if kind == "cluster":
        row = a_rows_by_cid.get(target_id, {})
        canon = canonical_yiddish or (row.get("canonical_yiddish") or "").strip()
        variants = (row.get("name_variants") or "").strip()
        if canon:
            lines.append(f"- Canonical (Yiddish): {canon}")
        if variants:
            lines.append(f"- Variants: {variants}")
        for label, key in (
            ("Settlements", "extracted_settlements"),
            ("Addresses", "extracted_addresses"),
            ("Venues", "extracted_venues"),
        ):
            v = (row.get(key) or "").strip()
            if v:
                lines.append(f"- {label}: {v}")
        cluster_samples = (samples.get(target_id) or {}).get("samples") or []
        if cluster_samples:
            lines.append("")
            lines.append("Attestations (first 2):")
            for head, sent, _fle, _xid in cluster_samples[:2]:
                head_clean = (head or "(no heading)").strip()
                sent_clean = (sent or "").strip().replace("\n", " ")
                if sent_clean:
                    lines.append(f"- **{head_clean}** — {sent_clean}")
                else:
                    lines.append(f"- **{head_clean}**")
    else:  # db
        db_row = next((r for r in db_rows if r.get("db_id") == target_id), {})
        name = (db_row.get("name") or "").strip()
        name_yi = (db_row.get("name_yiddish") or "").strip()
        address = (db_row.get("address") or "").strip()
        if name:
            lines.append(f"- Name (Latin): {name}")
        if name_yi:
            lines.append(f"- Name (Yiddish): {name_yi}")
        if address:
            lines.append(f"- Address: {address}")
        linked = (db_row.get("linked_cluster_ids") or "").strip()
        if linked:
            lines.append(f"- Linked clusters: {linked}")
    return "\n".join(lines)


# ─── detail renderers (unchanged from prior version) ──────────────────────

def _render_cluster_details(c: ClusterCard, a_rows_by_cid, db_by_id, samples) -> None:
    row = a_rows_by_cid.get(c.cluster_id)
    if row is None:
        st.caption("No alignment row found.")
        return

    variants = _split_pipe(row.get("name_variants", ""))
    if variants:
        st.markdown("**Variants**")
        st.write(" | ".join(variants))

    for label, key in (
        ("Settlements", "extracted_settlements"),
        ("Addresses", "extracted_addresses"),
        ("Venues", "extracted_venues"),
        ("Countries", "extracted_countries"),
    ):
        val = (row.get(key) or "").strip()
        if val:
            st.markdown(f"**{label}**")
            st.write(val)

    s = samples.get(c.cluster_id, {})
    sample_list = s.get("samples", []) if s else []
    if sample_list:
        st.markdown("**Attestations**")
        for i, (head, sent, fle, xid) in enumerate(sample_list, start=1):
            st.markdown(f"{i}. **{head or '(no heading)'}**")
            if sent:
                st.caption(sent)
            if fle and xid:
                with st.expander(f"Full entry context ({xid})"):
                    full = get_entry_text(fle, xid)
                    if full:
                        st.markdown(
                            f"<div dir='rtl' style='font-size:0.9em; white-space:pre-wrap; "
                            f"line-height:1.6;'>{full}</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.caption(f"Entry not found in XML ({_JSON_TO_XML.get(fle, fle)}).")

    c_ids = _split_pipe(row.get("candidate_db_ids", ""))
    c_scores = _split_pipe(row.get("candidate_scores", ""))
    c_methods = _split_pipe(row.get("candidate_methods", ""))
    if c_ids:
        st.markdown("**DB candidates**")
        for i, dbid in enumerate(c_ids):
            db = db_by_id.get(dbid, {})
            score_txt = c_scores[i] if i < len(c_scores) else ""
            method_txt = c_methods[i] if i < len(c_methods) else ""
            icon = {"exact": "🎯", "phonetic": "🔊", "ipa_phonetic": "🔉", "fuzzy": "🔤", "person": "👤", "person_phonetic": "👤🔉"}.get(method_txt, "•")
            st.caption(
                f"{icon} {dbid} · {db.get('name','(missing)')} · "
                f"score {score_txt} · {method_txt}"
            )
    notes = (row.get("reviewer_notes") or "").strip()
    if notes:
        st.markdown(f"**Reviewer notes** · {notes}")


def _render_db_details(d: DbCard, a_rows, db_rows) -> None:
    db_full = next((r for r in db_rows if r.get("db_id") == d.db_id), {})
    if db_full.get("address"):
        st.markdown(f"**Address** · {db_full['address']}")
    if db_full.get("name"):
        st.markdown(f"**Name (Latin)** · {db_full['name']}")
    if db_full.get("name_yiddish"):
        st.markdown(f"**Name (Yiddish)** · {db_full['name_yiddish']}")
    if db_full.get("linked_cluster_ids"):
        st.markdown(f"**Linked clusters** · {db_full['linked_cluster_ids']}")
    aligned = [
        r for r in a_rows
        if (r.get("aligned_db_id") or "").strip() == d.db_id
    ]
    if aligned:
        st.markdown(f"**Currently aligned ({len(aligned)})**")
        for r in aligned[:10]:
            st.caption(
                f"{r.get('cluster_id','')} · {r.get('canonical_yiddish','')} "
                f"({r.get('decision','')})"
            )


# ─── map header ───────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False, max_entries=4)
def _build_map_cached(color_mode: str, version: int):
    """Build the folium map once per (color_mode, data-version) and reuse the
    object across reruns. Reconstructing ~150 CircleMarkers and re-serializing
    them on every rerun was the dominant cost; caching the figure lets
    st_folium skip the round-trip when nothing relevant changed. Recentering on
    a focus city is handled separately via st_folium's `center`/`zoom` props, so
    the cached figure is always built at the default viewport.
    """
    import folium

    ix = get_index()
    coords = load_coords()
    addr_by_dbid = _load_addresses()

    mappable = [(qid, en, yi) for (qid, en, yi) in ix.cities() if qid in coords]
    if not mappable:
        return None

    counts = {qid: ix.mentions_in_city(qid) for qid, _en, _yi in mappable}
    max_n = max(counts.values()) or 1

    m = folium.Map(location=[52.0, 19.0], zoom_start=4, tiles="OpenStreetMap")
    for qid, en, yi in mappable:
        lat, lon = coords[qid]
        n = counts[qid]
        radius = 4 + 10 * (math.log1p(n) / math.log1p(max_n))
        if color_mode == "Dominant org type":
            color = _TYPE_COLORS.get(ix.dominant_org_type(qid), _FALLBACK_COLOR)
        else:
            color = _addr_status_for_qid(qid, ix, addr_by_dbid)
        label = en or qid
        if yi and yi != en:
            label += f" · {yi}"
        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            weight=1,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            tooltip=f"{qid}|{label}|{n} mentions",
        ).add_to(m)
    return m


@st.fragment
def _render_map(
    ix,
    addr_by_dbid: dict[str, dict[str, str]],
    focus_qid: str | None,
) -> None:
    # Fragment: interacting with the map (colour toggle, click) reruns only this
    # block, and — crucially — picker/filter/row interactions elsewhere on the
    # page no longer rebuild or re-serialize the map.
    try:
        from streamlit_folium import st_folium
    except ImportError:
        st.info("Install `folium` + `streamlit-folium` to enable the map header.")
        return

    coords = load_coords()
    if not coords:
        st.info(
            "No settlement coords cached yet. Run "
            "`python Zylbercweig/organizations/build_settlement_coords.py` to populate."
        )
        return

    color_mode = st.radio(
        "Map colour",
        ("Dominant org type", "Address-resolution status"),
        horizontal=True,
        key="sa_map_color_mode",
    )

    m = _build_map_cached(color_mode, _INDEX_VERSION)
    if m is None:
        st.caption("No mappable cities in current index.")
        return

    # The focus city (picker selection or a previous map click) drives only the
    # st_folium viewport props — the cached figure itself is viewport-agnostic.
    if focus_qid and focus_qid in coords:
        flat, flon = coords[focus_qid]
        focus_center = (flat, flon, 11)
    else:
        focus_center = (52.0, 19.0, 4)

    out = st_folium(
        m,
        center=[focus_center[0], focus_center[1]],
        zoom=focus_center[2],
        width=None, height=420,
        returned_objects=["last_object_clicked_tooltip"],
        key="settlement_audit_map",
    )
    clicked = (out or {}).get("last_object_clicked_tooltip")
    # st_folium returns the most recent click's tooltip on EVERY rerun — even
    # reruns triggered by unrelated work (merges, mints, picker changes). We must
    # dedup on the *clicked QID*, not the raw tooltip string: the tooltip embeds
    # a live mention count (`qid|label|N mentions`), so any mutation that shifts
    # a count rebuilds the map and makes st_folium re-emit the same physical
    # click with a different N. Deduping on the full string let that slip through
    # and snapped focus back to the last-clicked marker (typically NY) mid-work.
    # Keying the guard on QID makes a replayed click a no-op regardless of count.
    clicked_qid = (clicked or "").split("|", 1)[0].strip()
    last_handled_qid = st.session_state.get("sa_last_handled_qid")
    if clicked_qid and clicked_qid != last_handled_qid:
        st.session_state["sa_last_handled_qid"] = clicked_qid
        if clicked_qid != focus_qid:
            st.session_state["audit_target_qid"] = clicked_qid
            st.rerun(scope="app")

    # Legend
    if color_mode == "Dominant org type":
        bits = " · ".join(
            f"<span style='color:{c}'>●</span> {t}"
            for t, c in list(_TYPE_COLORS.items())[:10]
        )
        st.caption(bits, unsafe_allow_html=True)
    else:
        st.caption(
            "<span style='color:green'>●</span> all DB rows geocoded · "
            "<span style='color:orange'>●</span> some · "
            "<span style='color:red'>●</span> none · "
            "<span style='color:gray'>●</span> no DB rows",
            unsafe_allow_html=True,
        )


# ─── per-row action menu ──────────────────────────────────────────────────

def _bucket_options(
    bucket: CityBucket,
    self_kind: str,
    self_id: str,
    a_rows_by_cid: dict[str, dict[str, str]],
    db_by_id: dict[str, dict[str, str]],
) -> list[tuple[str, str, str]]:
    """Return [(kind, id, label)] for every other item in the bucket.

    Clusters an earlier session already folded into a DB row keep rendering in
    the bucket, and this picker used to offer them with a label identical to an
    untouched cluster's. That is the mechanism behind the re-merge problem: 86
    of 86 clusters picked as a merge partner more than once were already
    consumed at the time they were re-picked, usually a day or more later, and
    often into a *different* target. So: drop the ones already sitting on this
    very row (picking them is a pure no-op), and push the rest to the bottom
    carrying the DB row that owns them.
    """
    free: list[tuple[str, str, str]] = []
    consumed: list[tuple[str, str, str]] = []
    for d in bucket.db_cards:
        if self_kind == "db" and d.db_id == self_id:
            continue
        label = f"DB {d.db_id} · {d.name or d.name_yiddish or '(unnamed)'}"
        free.append(("db", d.db_id, label))
    for c in bucket.clusters:
        if self_kind == "cluster" and c.cluster_id == self_id:
            continue
        target = _aligned_target(c.cluster_id, a_rows_by_cid, db_by_id)
        if target and self_kind == "db" and target == self_id:
            continue  # already on this row — nothing to merge
        name = c.canonical_yiddish or "(no canonical)"
        label = f"cluster {c.cluster_id} · {name} (n={c.cluster_size})"
        if target:
            consumed.append(("cluster", c.cluster_id, f"⛔ already in DB {target} — {label}"))
        else:
            free.append(("cluster", c.cluster_id, label))
    return free + consumed


def _row_action_menu(
    *,
    self_kind: str,
    self_id: str,
    self_label: str,
    bucket: CityBucket,
    qid: str,
    reviewer: str,
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
    a_rows_by_cid: dict[str, dict[str, str]],
    db_by_id: dict[str, dict[str, str]],
    samples: dict,
    canonical_yiddish: str = "",
) -> None:
    # Bucket-content signature scopes popover widget keys to the current state,
    # so a stale radio selection from a now-merged row can't linger.
    sig_parts = [d.db_id for d in bucket.db_cards] + [c.cluster_id for c in bucket.clusters]
    sig = str(abs(hash(tuple(sig_parts))))[:8]
    key = f"sa_act_{qid}_{bucket.org_type}_{self_kind}_{self_id}_{sig}"

    def _do_merge(partners: list[tuple[str, str]]) -> str | None:
        """partners = [(kind, id), ...] picked to merge into self. Routes by the
        4 self/partner combinations; uses _consolidate_clusters so re-running a
        merge reuses any already-minted target instead of producing orphans."""
        if not partners:
            return None
        is_noop, _warn = _merge_plan(
            self_kind, self_id, partners, a_rows_by_cid, db_by_id,
        )
        if is_noop:
            # Everything picked already sits on the row this merge would target.
            # Writing anyway rewrote both TSVs, pushed to GitHub and logged a
            # decision that recorded nothing — 22 of the 75 repeat actions in
            # the audit trail are exactly this.
            return f"{_ALREADY_PREFIX} — nothing to do"
        partner_clusters = [pid for pk, pid in partners if pk == "cluster"]
        partner_dbs = [pid for pk, pid in partners if pk == "db"]
        if self_kind == "cluster":
            cluster_ids = [self_id] + partner_clusters
            if partner_dbs:
                # Cluster + at least one DB → align all clusters to that DB.
                target = partner_dbs[0]
                msg = _align_clusters_to_db(
                    target, cluster_ids, a_rows, a_headers, db_rows, db_headers,
                )
                if len(partner_dbs) > 1:
                    msg += _merge_db_rows_op(
                        target, partner_dbs[1:], a_rows, a_headers, db_rows, db_headers,
                    )
                return msg
            _new, msg = _consolidate_clusters(
                cluster_ids, bucket.org_type, canonical_yiddish, "",
                a_rows, a_headers, db_rows, db_headers,
            )
            return msg
        # self_kind == "db"
        if partner_clusters:
            msg = _align_clusters_to_db(
                self_id, partner_clusters, a_rows, a_headers, db_rows, db_headers,
            )
        else:
            msg = ""
        if partner_dbs:
            msg2 = _merge_db_rows_op(
                self_id, partner_dbs, a_rows, a_headers, db_rows, db_headers,
            )
            msg = (msg + " · " + msg2) if msg else msg2
        return msg

    # Streamlit closes a popover on EVERY rerun, and st.tabs snaps back to its
    # first tab. So typing in Search reran the fragment, shut the popover and
    # reset to Merge -- which read as "only merge works". Drive the panel from
    # session_state instead, so it survives reruns and keeps its active tab.
    open_key = "sa_open_action"
    if st.button("Actions ⋯", key=f"{key}_open"):
        st.session_state[open_key] = (
            None if st.session_state.get(open_key) == key else key
        )

    if st.session_state.get(open_key) == key:
        with st.container(border=True):
            head_c, close_c = st.columns([5, 1])
            head_c.markdown(f"**Actions** · {self_label[:44]}")
            if close_c.button("✕", key=f"{key}_close", help="Close"):
                # The `if` that opened this container already evaluated above,
                # so the panel is on screen for this run regardless. Rerun the
                # fragment so the close takes effect immediately instead of on
                # the user's next interaction.
                st.session_state[open_key] = None
                _rerun_local()
            # radio, not st.tabs: a tab selection is not persisted across the
            # rerun that every widget in here triggers.
            mode = st.radio(
                "Action", ["Merge", "Search", "More"],
                key=f"{key}_mode", horizontal=True,
                label_visibility="collapsed",
            )

            if mode == "Merge":
                opts = _bucket_options(
                    bucket, self_kind, self_id, a_rows_by_cid, db_by_id,
                )
                if not opts:
                    st.caption("No other items in this bucket to combine with.")
                else:
                    st.caption(
                        "Tick every row that is the *same entity* as this one. "
                        "Confirm once — re-running this merge is safe and won't "
                        "create duplicate DB rows."
                    )
                    picked: list[tuple[str, str]] = []
                    for pk_kind, pk_id, pk_label in opts:
                        cb_key = f"{key}_mp_{pk_kind}_{pk_id}"
                        if st.checkbox(pk_label, key=cb_key):
                            picked.append((pk_kind, pk_id))

                    is_noop, warnings = (
                        _merge_plan(self_kind, self_id, picked, a_rows_by_cid, db_by_id)
                        if picked else (False, [])
                    )
                    move_ok = True
                    if is_noop:
                        st.info(
                            "Everything ticked is already merged into the same DB "
                            "row. Nothing to do."
                        )
                    elif warnings:
                        # Silently re-pointing a cluster that another DB row already
                        # owns is how one cluster ended up claimed by two rows. Name
                        # the consequence and make the reviewer opt in.
                        st.warning(
                            "This merge moves work that is already filed:\n\n"
                            + "\n".join(f"- {w}" for w in warnings)
                        )
                        move_ok = st.checkbox(
                            "Yes — re-file these into this entity",
                            key=f"{key}_confirm_move",
                        )
                    if st.button(
                        f"Confirm merge ({len(picked)} selected)",
                        key=f"{key}_confirm_multi",
                        type="primary",
                        disabled=not picked or is_noop or not move_ok,
                        use_container_width=True,
                    ):
                        msg = _do_merge(picked)
                        for pk_kind, pk_id, _ in opts:
                            st.session_state.pop(f"{key}_mp_{pk_kind}_{pk_id}", None)
                        st.session_state.pop(f"{key}_confirm_move", None)
                        landed = bool(msg) and not msg.startswith(_ALREADY_PREFIX)
                        if landed:
                            log_action(
                                "settlement_audit", "merge",
                                target_id=f"{self_kind}:{self_id}", decision="MERGE",
                                note=msg or "",
                                qid=qid, org_type=bucket.org_type,
                                partners=[f"{k}:{i}" for k, i in picked],
                            )
                            _record_receipt(qid, msg, self_kind, self_id, len(picked))
                        if msg:
                            st.toast(msg, icon="✅" if landed else "ℹ️")
                        _rerun_local()

            elif mode == "Search":
                st.caption("Search all DB rows + clusters across the whole corpus.")
                q = st.text_input(
                    "Search", key=f"{key}_q",
                    placeholder="name or fragment...",
                    label_visibility="collapsed",
                )
                if q and q.strip():
                    from types import SimpleNamespace
                    results = _search_corpus(
                        q.strip(), db_rows, a_rows, samples,
                        exclude=(self_kind, self_id),
                    )[:20]
                    if not results:
                        st.caption("No matches.")
                    for r_kind, r_id, r_label, _score in results:
                        cols = st.columns([3, 1, 1])
                        cols[0].caption(f"{r_kind} · {r_id} · {r_label}")
                        show_key = f"{key}_show_{r_kind}_{r_id}"
                        cols[1].toggle("Inspect", key=show_key, value=False,
                                       help="Show variants, settlements & attestations")
                        if cols[2].button("Merge", key=f"{key}_m_{r_kind}_{r_id}"):
                            msg = _do_merge([(r_kind, r_id)])
                            landed = bool(msg) and not msg.startswith(_ALREADY_PREFIX)
                            if landed:
                                log_action(
                                    "settlement_audit", "merge_via_search",
                                    target_id=f"{self_kind}:{self_id}", decision="MERGE",
                                    note=msg or "",
                                    qid=qid, org_type=bucket.org_type,
                                    partner=f"{r_kind}:{r_id}",
                                )
                                _record_receipt(qid, msg, self_kind, self_id, 1)
                            if msg:
                                st.toast(msg, icon="✅" if landed else "ℹ️")
                            _rerun_local()
                        if st.session_state.get(show_key):
                            with st.container(border=True):
                                if r_kind == "cluster":
                                    _render_cluster_details(
                                        SimpleNamespace(cluster_id=r_id),
                                        a_rows_by_cid, db_by_id, samples,
                                    )
                                else:
                                    _render_db_details(
                                        SimpleNamespace(db_id=r_id),
                                        a_rows, db_rows,
                                    )

            else:  # More
                if self_kind == "cluster":
                    st.markdown("**Mint solo**")
                    st.caption(
                        "Create a brand-new DB row for **only** this cluster — "
                        "use this when nothing else in the bucket should merge with it."
                    )
                    already = _aligned_target(self_id, a_rows_by_cid, db_by_id)
                    if already:
                        # Minting a cluster that is already on a row produced a
                        # duplicate entity and orphaned the old back-link.
                        st.info(f"This cluster is already **DB {already}**.")
                    if st.button("Mint as new entity (solo)", key=f"{key}_mint",
                                 disabled=bool(already),
                                 use_container_width=True):
                        _new, msg = _mint_db_from_clusters(
                            [self_id], bucket.org_type, canonical_yiddish, "",
                            a_rows, a_headers, db_rows, db_headers,
                        )
                        log_action(
                            "settlement_audit", "mint_solo",
                            target_id=self_id, decision="MINT",
                            note=msg or "",
                            qid=qid, org_type=bucket.org_type,
                            # _mint_db_from_clusters returns (db_id_str, msg), so
                            # getattr(..., "db_id") on a str always yielded "" and
                            # the log lost which row had just been minted.
                            new_db_id=_new or "",
                        )
                        _record_receipt(qid, msg, "db", _new or "", 1)
                        st.toast(msg, icon="✅")
                        _rerun_local()

                if reviewer == ADMIN_REVIEWER:
                    if self_kind == "cluster":
                        st.divider()
                    st.markdown("**Research**")
                    comment = st.text_area(
                        "Question / focus (optional)",
                        key=f"{key}_research_comment",
                        placeholder="e.g. is this the same theatre as Goldfaden's tour company?",
                        height=70,
                    )
                    if st.button("Queue for cluster-research", key=f"{key}_research",
                                 use_container_width=True):
                        _queue_research(
                            kind=self_kind, target_id=self_id, qid=qid,
                            org_type=bucket.org_type, label=self_label, reviewer=reviewer,
                            comment=(comment or "").strip(),
                        )
                        st.toast("Queued for cluster-research", icon="🔬")


def _search_corpus(
    query: str,
    db_rows: list[dict[str, str]],
    a_rows: list[dict[str, str]],
    samples: dict,
    exclude: tuple[str, str] = ("", ""),
) -> list[tuple[str, str, str, int]]:
    # Fold both sides: Yiddish is typed unvocalised but stored with niqqud, so
    # a raw substring match silently returns almost nothing. See fold_for_search.
    q = fold_for_search(query)
    if not q:
        return []
    results: list[tuple[str, str, str, int]] = []
    matched_clusters: set[str] = set()
    for r in db_rows:
        dbid = (r.get("db_id") or "").strip()
        if exclude == ("db", dbid):
            continue
        hay = fold_for_search(f"{r.get('name','')} {r.get('name_yiddish','')}")
        idx = hay.find(q)
        if idx >= 0:
            label = r.get("name") or r.get("name_yiddish") or "(unnamed)"
            results.append(("db", dbid, label, idx))
    for r in a_rows:
        cid = (r.get("cluster_id") or "").strip()
        if exclude == ("cluster", cid):
            continue
        hay = fold_for_search(f"{r.get('canonical_yiddish','')} {r.get('name_variants','')}")
        idx = hay.find(q)
        if idx >= 0:
            label = r.get("canonical_yiddish") or "(no canonical)"
            results.append(("cluster", cid, label, idx))
            matched_clusters.add(cid)
    # Also surface clusters whose attestation sentences contain the query —
    # the result is keyed by the parent cluster and the preview shows the
    # matching sentence so the reviewer can see why it hit.
    a_by_cid = {r.get("cluster_id", ""): r for r in a_rows}
    for cid, s in (samples or {}).items():
        if exclude == ("cluster", cid) or cid in matched_clusters:
            continue
        for _head, sent, _fle, _xid in (s.get("samples") or []):
            if not sent:
                continue
            idx = fold_for_search(sent).find(q)
            if idx >= 0:
                canon = (a_by_cid.get(cid, {}).get("canonical_yiddish") or "").strip()
                preview = sent[:80].replace("\n", " ")
                label = f"{canon or '(no canonical)'} — “{preview}…”"
                results.append(("cluster", cid, label, idx + 1000))
                matched_clusters.add(cid)
                break
    results.sort(key=lambda t: t[3])
    return results


# ─── multi-select action bar (top of view, when 2+ items checked) ─────────

def _action_bar(
    selected_db_ids: list[str],
    selected_cluster_ids: list[str],
    qid: str,
    db_by_id: dict[str, dict[str, str]],
    a_rows_by_cid: dict[str, dict[str, str]],
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
    sel_key_prefix: str,
) -> None:
    n_db = len(selected_db_ids)
    n_cl = len(selected_cluster_ids)
    st.markdown(f"**Selection** · {n_cl} cluster(s), {n_db} DB row(s)")

    # Same guards as the per-row Actions menu: a selection can just as easily be
    # made of clusters an earlier session already filed. Rendered before the
    # button row so the warning reads above the action it qualifies.
    align_noop, align_warn = (
        _merge_plan(
            "db", selected_db_ids[0],
            [("cluster", c) for c in selected_cluster_ids],
            a_rows_by_cid, db_by_id,
        )
        if (n_cl >= 1 and n_db == 1) else (False, [])
    )
    if align_noop:
        st.info("Every selected cluster is already on that DB row.")
    elif align_warn:
        st.warning(
            "Aligning moves work that is already filed:\n\n"
            + "\n".join(f"- {w}" for w in align_warn)
        )
    align_ok = (not align_warn) or st.checkbox(
        "Yes — re-file these into the selected DB row",
        key=f"{sel_key_prefix}_align_move_ok",
    )

    cols = st.columns([1, 1, 1, 1])

    # Determine org_type for mint — derive from first selected cluster.
    seed_type = ""
    seed_name = ""
    for cid in selected_cluster_ids:
        row = a_rows_by_cid.get(cid)
        if row:
            seed_type = (row.get("org_type") or "").strip()
            if not seed_name and (row.get("canonical_yiddish") or "").strip():
                seed_name = row["canonical_yiddish"].strip()
            if seed_type:
                break

    with cols[0]:
        if st.button(
            "Align selected → DB row",
            disabled=not (n_cl >= 1 and n_db == 1) or align_noop or not align_ok,
            type="primary", use_container_width=True,
            key=f"{sel_key_prefix}_btn_align",
        ):
            msg = _align_clusters_to_db(
                selected_db_ids[0], selected_cluster_ids,
                a_rows, a_headers, db_rows, db_headers,
            )
            for cid in selected_cluster_ids:
                st.session_state.pop(f"{sel_key_prefix}_cl_{cid}", None)
            st.session_state.pop(f"{sel_key_prefix}_db_{selected_db_ids[0]}", None)
            st.session_state.pop(f"{sel_key_prefix}_align_move_ok", None)
            log_action(
                "settlement_audit", "align_selected",
                target_id=f"db:{selected_db_ids[0]}", decision="MERGE",
                note=msg or "", qid=qid,
                partners=[f"cluster:{c}" for c in selected_cluster_ids],
            )
            _record_receipt(qid, msg, "db", selected_db_ids[0], n_cl)
            st.toast(msg, icon="✅")
            _rerun_local()

    with cols[1]:
        new_disabled = not (n_cl >= 1 and n_db == 0)
        if new_disabled:
            st.button("Create new DB entity", disabled=True,
                      use_container_width=True,
                      key=f"{sel_key_prefix}_btn_new_disabled")
        else:
            with st.popover("Create new DB entity", use_container_width=True):
                new_name = st.text_input(
                    "New organization name (Yiddish)", value=seed_name,
                    key=f"{sel_key_prefix}_new_name",
                )
                new_name_latin = st.text_input(
                    "Latin name (optional)", value="",
                    key=f"{sel_key_prefix}_new_latin",
                )
                if st.button("Confirm: create + align all selected",
                             key=f"{sel_key_prefix}_btn_new_confirm",
                             type="primary"):
                    _new, msg = _consolidate_clusters(
                        selected_cluster_ids, seed_type, new_name, new_name_latin,
                        a_rows, a_headers, db_rows, db_headers,
                    )
                    for cid in selected_cluster_ids:
                        st.session_state.pop(f"{sel_key_prefix}_cl_{cid}", None)
                    _record_receipt(qid, msg, "db", _new or "", n_cl)
                    st.toast(msg, icon="✅")
                    _rerun_local()

    with cols[2]:
        merge_disabled = n_db < 2
        if merge_disabled:
            st.button("Merge DB rows", disabled=True,
                      use_container_width=True,
                      key=f"{sel_key_prefix}_btn_merge_disabled")
        else:
            with st.popover(f"Merge {n_db} DB rows", use_container_width=True):
                st.caption("Pick the primary row to keep. Others will be removed "
                           "and their linked clusters re-pointed to the primary.")
                primary = st.radio(
                    "Primary",
                    options=selected_db_ids,
                    format_func=lambda d: f"{d} · {db_by_id.get(d, {}).get('name','')}",
                    key=f"{sel_key_prefix}_merge_primary",
                )
                confirm = st.checkbox(
                    "I understand this removes the other DB rows.",
                    key=f"{sel_key_prefix}_merge_confirm",
                )
                if st.button("Confirm merge", type="primary",
                             disabled=not confirm,
                             key=f"{sel_key_prefix}_btn_merge_confirm"):
                    secondaries = [d for d in selected_db_ids if d != primary]
                    msg = _merge_db_rows_op(
                        primary, secondaries, a_rows, a_headers, db_rows, db_headers,
                    )
                    for dbid in selected_db_ids:
                        st.session_state.pop(f"{sel_key_prefix}_db_{dbid}", None)
                    _record_receipt(qid, msg, "db", primary, len(secondaries))
                    st.toast(msg, icon="✅")
                    _rerun_local()

    with cols[3]:
        if st.button("Clear selection", use_container_width=True,
                     key=f"{sel_key_prefix}_btn_clear"):
            for k in list(st.session_state.keys()):
                if isinstance(k, str) and k.startswith(sel_key_prefix + "_cl_"):
                    st.session_state[k] = False
                if isinstance(k, str) and k.startswith(sel_key_prefix + "_db_"):
                    st.session_state[k] = False
            _rerun_local()


# ─── view entry point ─────────────────────────────────────────────────────

def render() -> None:
    st.header("Settlement audit")
    st.caption(
        "Spatial + categorical workbench. Click a city on the map (or pick from "
        "the list) to see every DB row and cluster grouped by org type. Each row "
        "has an Actions menu for Mint / Merge here / Search more / Research. "
        "Itinerant types are excluded."
    )

    try:
        ix = get_index()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not build settlement index: {exc}")
        return

    cities = ix.cities()
    if not cities:
        st.warning("No resolved settlements in index — check that kimatch outputs exist.")
        return

    reviewer = st.session_state.get("reviewer", "")
    addr_by_dbid = _load_addresses()

    # Resolve the focus city BEFORE the map renders, so the map's center matches
    # this turn's selection (whether the user came via the picker or via a
    # map-click in the previous turn). Streamlit updates widget session_state
    # before the script body runs, so settlement_audit_city is already current.
    target_qid = st.session_state.pop("audit_target_qid", None)
    # A reviewer save pushes a commit, which redeploys the app and drops every
    # session (see the query-param note in app.py). app.py restores the view from
    # the URL; carry the city the same way, so a redeploy mid-merge doesn't also
    # cost the RA their place in the picker. Applied once per distinct URL value
    # so it can't override a later pick.
    if not target_qid:
        qp_city = st.query_params.get("city")
        if (
            qp_city
            and qp_city != st.session_state.get("_sa_qp_city_applied")
            and "settlement_audit_city" not in st.session_state
        ):
            target_qid = qp_city
            st.session_state["_sa_qp_city_applied"] = qp_city
    if target_qid:
        match = next((c for c in cities if c[0] == target_qid), None)
        if match is not None:
            st.session_state["settlement_audit_city"] = match
    focus_city = st.session_state.get("settlement_audit_city")
    focus_qid = focus_city[0] if focus_city else None
    if focus_qid and st.query_params.get("city") != focus_qid:
        st.query_params["city"] = focus_qid
        st.session_state["_sa_qp_city_applied"] = focus_qid

    # --- Map header ---
    with st.container(border=True):
        _render_map(ix, addr_by_dbid, focus_qid)

    _unplaced_panel(ix)
    _workbench(reviewer)


def _unplaced_panel(ix) -> None:
    """Orgs that carry a location but land on no city.

    Without this they are simply absent — the map and picker only show what
    resolved, so a row recorded as "America" reads as if it had no location at
    all. Split in two because the halves need different work: country-level
    values are correctly unresolvable (the lens keys on cities) and want either
    RA re-coding or acceptance, whereas the rest are spelling variants, leaked
    street addresses and genuine gazetteer gaps.
    """
    country = ix.unplaced(country_level=True)
    other = ix.unplaced(country_level=False)
    if not country and not other:
        return

    n_c = sum(a + b for _v, a, b in country)
    n_o = sum(a + b for _v, a, b in other)
    with st.expander(
        f"⚠️ Not on the map — {n_c} country-level · {n_o} unrecognised", expanded=False
    ):
        col_a, col_b = st.columns(2)
        with col_a:
            st.caption(
                f"**Country / region only** — {len(country)} values, {n_c} orgs. "
                "Recorded location is a country, so there is no city to place "
                "them in. Re-code to a city where the source allows."
            )
            for value, n_db, n_cl in country[:40]:
                st.markdown(
                    f"<div dir='rtl'>{value} &nbsp;·&nbsp; "
                    f"{n_db} DB / {n_cl} clusters</div>",
                    unsafe_allow_html=True,
                )
        with col_b:
            st.caption(
                f"**Unrecognised** — {len(other)} values, {n_o} orgs. Spelling "
                "variants, street addresses leaked into a settlement field, and "
                "places missing from the gazetteer."
            )
            for value, n_db, n_cl in other[:40]:
                st.markdown(
                    f"<div dir='rtl'>{value[:60]} &nbsp;·&nbsp; "
                    f"{n_db} DB / {n_cl} clusters</div>",
                    unsafe_allow_html=True,
                )
            if len(other) > 40:
                st.caption(f"… and {len(other) - 40} more")


@st.fragment
def _workbench(reviewer) -> None:
    # Fragment: the picker, filters, selection action-bar and per-row Actions
    # menus all rerun in isolation. Ticking a checkbox, opening a popover or
    # committing a merge no longer triggers a full-page rerun (which would
    # re-mount the map iframe and throw your scroll position away). Only a
    # city change escalates to a full app rerun, so the map can recenter.
    #
    # The index is looked up HERE rather than passed in: Streamlit replays a
    # fragment with the arguments captured at the last full run, so a merge
    # that calls get_index.cache_clear() would otherwise keep re-rendering the
    # pre-merge buckets until something forced a full rerun.
    ix = get_index()
    cities = ix.cities()

    # --- Picker ---
    sort_col, picker_col = st.columns([1, 3])
    with sort_col:
        sort_mode = st.radio(
            "Sort cities by",
            ("Mentions", "Alphabetical"),
            key="sa_sort_mode",
        )

    if sort_mode == "Mentions":
        cities_sorted = sorted(cities, key=lambda t: -ix.mentions_in_city(t[0]))
    else:
        cities_sorted = sorted(cities, key=lambda t: (t[1] or t[2]).lower())

    def _city_label(t: tuple[str, str, str]) -> str:
        qid, en, yi = t
        head = en or qid
        if yi and yi != en:
            head += f" · {yi}"
        return f"{head}  ·  {ix.mentions_in_city(qid)} mentions"

    with picker_col:
        # index=None so the picker starts empty rather than snapping to the
        # first option (which, sorted by mentions, is New York). RAs must pick a
        # city before any results render. Once selected, the value persists in
        # session_state under this key across reruns and map clicks.
        city = st.selectbox(
            "Settlement", cities_sorted, format_func=_city_label,
            key="settlement_audit_city",
            index=None,
            placeholder="Select a city to begin…",
        )
    if not city:
        st.info("Pick a settlement above to see its DB rows and clusters.")
        return
    qid = city[0]

    # --- Sub-settlement containment (see settlement_parents.tsv) ---
    # Storage is flat: a Brooklyn org keeps its own QID and bucket. Containment
    # is a query-time rollup, so boroughs are reachable both as themselves and
    # under the parent city. Default ON — leaving it off is what let Brooklyn
    # orgs sit invisibly outside a "New York" review.
    children = ix.child_cities(qid)
    parent = ix.parent_city(qid)
    kind = ix.kind_of(qid)
    if kind and kind != "settlement":
        # A ghetto is not a city. It resolves and rolls up into its parent, but
        # must never read as an ordinary settlement in the UI.
        st.caption(f"🏷️ this is a **{kind}**, not a settlement")
    rollup = False
    if children:
        child_names = ", ".join(en for _q, en in children)
        rollup = st.checkbox(
            f"Include sub-settlements ({child_names})",
            value=True,
            key=f"sa_rollup_{qid}",
            help=(
                "These are contained in this settlement. Their rows are filed "
                "under their own QID, so they are hidden from this city unless "
                "rolled up. Sections below are labelled with their settlement."
            ),
        )
    if parent:
        p_qid, p_en = parent
        bc_col, bc_btn = st.columns([4, 1])
        bc_col.caption(f"⊂ part of **{p_en}**")
        if bc_btn.button("Go to parent", key=f"sa_toparent_{qid}"):
            st.session_state["audit_target_qid"] = p_qid
            st.rerun(scope="app")

    # Picking a new city from the dropdown reruns only this fragment, so the map
    # header (a separate fragment) wouldn't recenter. Escalate to a full app
    # rerun once on an actual change so the map follows the selection.
    prev_focus = st.session_state.get("_sa_focus_qid")
    st.session_state["_sa_focus_qid"] = qid
    if prev_focus is not None and prev_focus != qid:
        st.rerun(scope="app")

    # --- Load editable data ---
    a_headers, a_rows = load_alignment(_mtime(ALIGN_FILE))
    db_headers, db_rows = load_core_db(_mtime(CORE_DB_FILE))
    samples = load_samples(mention_removals.cluster_cache_key(CLUSTER_FILE)) if CLUSTER_FILE.exists() else {}
    db_by_id = {r.get("db_id", ""): r for r in db_rows}
    a_rows_by_cid = {r["cluster_id"]: r for r in a_rows}

    buckets = ix.buckets_in_city(qid, include_children=rollup)
    if not buckets:
        st.info("No entries in this city.")
        return

    sel_key_prefix = f"sa_sel_{qid}"

    # --- Receipt for the last mutation in this city ---
    receipt = st.session_state.get(_RECEIPT_KEY)
    receipt_db = ""
    if isinstance(receipt, dict) and receipt.get("qid") == qid and receipt.get("msg"):
        receipt_db = receipt.get("db_id", "")
        r_col, x_col = st.columns([8, 1])
        r_col.success(f"✅ {receipt['msg']}")
        if x_col.button("Dismiss", key=f"sa_receipt_x_{qid}"):
            st.session_state.pop(_RECEIPT_KEY, None)
            _rerun_local()

    # --- Global filters (apply across all types) ---
    with st.expander("Filters", expanded=False):
        f_col1, f_col2 = st.columns(2)
        with f_col1:
            decision_filter = st.multiselect(
                "Cluster decision",
                options=["(undecided)", "ALIGN", "NEW", "SPLIT", "DEFER", "DESCRIPTIVE", "DISCUSS"],
                default=["(undecided)", "ALIGN", "NEW"],
                key=f"sa_dec_{qid}",
            )
            min_size = st.number_input(
                "Min cluster size", min_value=1, value=1, step=1,
                key=f"sa_size_{qid}",
            )
        with f_col2:
            # dict.fromkeys: with rollup on, parent and child both contribute a
            # "Theatre" bucket, and a multiselect with duplicate options throws.
            type_opts = list(dict.fromkeys(b.org_type for b in buckets))
            type_filter = st.multiselect(
                "Org types to show",
                options=type_opts,
                default=type_opts,
                key=f"sa_types_{qid}",
            )

    # --- Selection state ---
    selected_db_ids: list[str] = []
    selected_cluster_ids: list[str] = []
    for b in buckets:
        for d in b.db_cards:
            if st.session_state.get(f"{sel_key_prefix}_db_{d.db_id}"):
                if d.db_id not in selected_db_ids:
                    selected_db_ids.append(d.db_id)
        for c in b.clusters:
            if st.session_state.get(f"{sel_key_prefix}_cl_{c.cluster_id}"):
                if c.cluster_id not in selected_cluster_ids:
                    selected_cluster_ids.append(c.cluster_id)

    if selected_db_ids or selected_cluster_ids:
        with st.container(border=True):
            _action_bar(
                selected_db_ids, selected_cluster_ids,
                qid, db_by_id, a_rows_by_cid,
                a_rows, a_headers, db_rows, db_headers,
                sel_key_prefix,
            )

    # --- Top metrics ---
    total_db = sum(len(b.db_cards) for b in buckets)
    total_cl = sum(len(b.clusters) for b in buckets)
    top = st.columns(4)
    top[0].metric("Types here", len(dict.fromkeys(b.org_type for b in buckets)))
    top[1].metric("DB rows", total_db)
    top[2].metric("Clusters", total_cl)
    top[3].metric(
        "Mentions",
        ix.mentions_in_city_rollup(qid) if rollup else ix.mentions_in_city(qid),
        delta=(
            f"+{ix.mentions_in_city_rollup(qid) - ix.mentions_in_city(qid)} from sub-settlements"
            if rollup and children
            else None
        ),
    )

    st.divider()

    # --- Per-type sections ---
    def _cl_passes(c: ClusterCard) -> bool:
        tag = c.decision or "(undecided)"
        if decision_filter and tag not in decision_filter:
            return False
        if c.cluster_size < min_size:
            return False
        return True

    # Dedupe ACROSS buckets, not just within one. A row attested in several
    # settlements ("ניו יאָרק | בראַנקס") lands in a bucket for each, and with
    # rollup on both are rendered in the same view — which duplicated the row
    # and collided its selection-checkbox key. Buckets are sorted largest-first,
    # so a row shows up in its most substantial section.
    claimed_db: set[str] = set()
    claimed_cl: set[str] = set()

    for bucket in buckets:
        if bucket.org_type not in type_filter:
            continue

        # Dedupe DB cards within this bucket
        seen: set[str] = set()
        unique_db: list[DbCard] = []
        for d in bucket.db_cards:
            if d.db_id in seen or d.db_id in claimed_db:
                continue
            seen.add(d.db_id)
            unique_db.append(d)
        unique_db.sort(key=lambda d: (d.name or d.name_yiddish or "").lower())

        clusters_all = [
            c for c in bucket.clusters
            if _cl_passes(c) and c.cluster_id not in claimed_cl
        ]
        clusters_all.sort(key=lambda c: (bool(c.decision), -c.cluster_size))

        # A cluster already folded into a DB row is finished work, but it kept
        # rendering as a full, selectable card indistinguishable from an open
        # one — so reviewers merged it again, days later, sometimes into a
        # different row. Split it out: open clusters stay as action cards,
        # resolved ones collapse into a read-only receipt list. Clusters whose
        # aligned_db_id points at a row that no longer exists are NOT resolved;
        # they stay in the open list flagged, because they need re-filing.
        clusters_visible = [
            c for c in clusters_all
            if not _aligned_target(c.cluster_id, a_rows_by_cid, db_by_id)
        ]
        clusters_resolved = [
            c for c in clusters_all
            if _aligned_target(c.cluster_id, a_rows_by_cid, db_by_id)
        ]

        claimed_db.update(d.db_id for d in unique_db)
        claimed_cl.update(c.cluster_id for c in clusters_all)

        if not unique_db and not clusters_all:
            continue

        # With rollup on, several buckets share an org_type — name the settlement
        # so a Brooklyn section is never mistaken for a Manhattan one.
        where = f" · {bucket.english or bucket.qid}" if bucket.qid != qid else ""
        head = (
            f"{bucket.org_type}{where} · "
            f"{len(unique_db)} DB / {len(clusters_visible)} open clusters"
        )
        if clusters_resolved:
            head += f" (+{len(clusters_resolved)} resolved)"
        st.subheader(head)

        align_counts: dict[str, int] = {}
        for c in bucket.clusters:
            if c.aligned_db_id:
                align_counts[c.aligned_db_id] = align_counts.get(c.aligned_db_id, 0) + 1

        cap_key = f"sa_capoff_{bucket.qid}_{bucket.org_type}"
        cap_off = st.session_state.get(cap_key, False)
        total_rows = len(unique_db) + len(clusters_visible)
        if total_rows > _ROW_CAP and not cap_off:
            if st.button(
                f"Show all {total_rows} rows in this section",
                key=f"{cap_key}_btn", use_container_width=False,
            ):
                st.session_state[cap_key] = True
                _rerun_local()
            db_to_show = unique_db[:_ROW_CAP]
            cluster_budget = max(0, _ROW_CAP - len(db_to_show))
            clusters_to_show = clusters_visible[:cluster_budget]
            st.caption(
                f"Showing {len(db_to_show)} DB · {len(clusters_to_show)} clusters "
                f"(of {len(unique_db)} / {len(clusters_visible)}). Capped for "
                "responsiveness — expand above to see all."
            )
        else:
            db_to_show = unique_db
            clusters_to_show = clusters_visible[:200]
            if cap_off and total_rows > _ROW_CAP:
                if st.button(
                    "Re-cap this section", key=f"{cap_key}_recap_btn",
                ):
                    st.session_state[cap_key] = False
                    _rerun_local()

        for d in db_to_show:
            with st.container(border=True):
                head_cols = st.columns([1, 9, 2])
                with head_cols[0]:
                    st.checkbox(
                        " ", key=f"{sel_key_prefix}_db_{d.db_id}",
                        label_visibility="collapsed",
                    )
                with head_cols[1]:
                    name = d.name or "(no Latin name)"
                    yi = d.name_yiddish or ""
                    head = f"**DB {d.db_id}** · {name}"
                    if yi:
                        head += f"  ·  {yi}"
                    if d.db_id and d.db_id == receipt_db:
                        head += "  ·  🆕 **just updated**"
                    st.markdown(head)
                    meta_bits = []
                    if d.confirmed_settlement:
                        meta_bits.append(f"📍 {d.confirmed_settlement}")
                    aligned_n = align_counts.get(d.db_id, 0)
                    if aligned_n:
                        meta_bits.append(f"🔗 {aligned_n} cluster{'s' if aligned_n != 1 else ''} aligned")
                    if meta_bits:
                        st.caption(" · ".join(meta_bits))
                with head_cols[2]:
                    _row_action_menu(
                        self_kind="db",
                        self_id=d.db_id,
                        self_label=name,
                        bucket=bucket,
                        qid=bucket.qid,
                        reviewer=reviewer,
                        a_rows=a_rows, a_headers=a_headers,
                        db_rows=db_rows, db_headers=db_headers,
                        a_rows_by_cid=a_rows_by_cid,
                        db_by_id=db_by_id,
                        samples=samples,
                    )
                with st.expander("Details"):
                    _render_db_details(d, a_rows, db_rows)

        for c in clusters_to_show:
            with st.container(border=True):
                head_cols = st.columns([1, 9, 2])
                with head_cols[0]:
                    st.checkbox(
                        " ", key=f"{sel_key_prefix}_cl_{c.cluster_id}",
                        label_visibility="collapsed",
                    )
                with head_cols[1]:
                    title = c.canonical_yiddish or "(no canonical)"
                    badge = f"**cluster {c.cluster_id}** · n={c.cluster_size}"
                    if c.decision:
                        badge += f" · {c.decision}"
                        if c.aligned_db_id:
                            # Reached only when the target row is gone: resolved
                            # clusters are rendered in the collapsed list below.
                            badge += f" · ⚠️ points at missing DB {c.aligned_db_id}"
                    else:
                        badge += " · _undecided_"
                    st.markdown(badge)
                    st.markdown(
                        f"<div dir='rtl' style='font-size:1.05rem'>{title}</div>",
                        unsafe_allow_html=True,
                    )
                with head_cols[2]:
                    _row_action_menu(
                        self_kind="cluster",
                        self_id=c.cluster_id,
                        self_label=title,
                        bucket=bucket,
                        qid=bucket.qid,
                        reviewer=reviewer,
                        a_rows=a_rows, a_headers=a_headers,
                        db_rows=db_rows, db_headers=db_headers,
                        a_rows_by_cid=a_rows_by_cid,
                        db_by_id=db_by_id,
                        samples=samples,
                        canonical_yiddish=c.canonical_yiddish,
                    )
                with st.expander("Details · mentions & candidates"):
                    _render_cluster_details(c, a_rows_by_cid, db_by_id, samples)

        omitted = len(clusters_visible) - len(clusters_to_show)
        if omitted > 0 and cap_off:
            st.caption(f"… and {omitted} more clusters (refine filters)")

        if clusters_resolved:
            # Read-only on purpose: no checkbox, no Actions menu. These are done,
            # and every widget rendered here is a widget the reviewer can use to
            # redo work. To undo one, act from the DB row that owns it.
            with st.expander(
                f"✅ {len(clusters_resolved)} resolved clusters "
                "(already folded into a DB row)"
            ):
                for c in clusters_resolved[:60]:
                    tgt = _aligned_target(c.cluster_id, a_rows_by_cid, db_by_id)
                    owner = db_by_id.get(tgt, {})
                    owner_name = (
                        owner.get("name") or owner.get("name_yiddish") or ""
                    )
                    st.caption(
                        f"`{c.cluster_id}` n={c.cluster_size} · "
                        f"{c.canonical_yiddish or '(no canonical)'} → "
                        f"**DB {tgt}** {owner_name}"
                    )
                if len(clusters_resolved) > 60:
                    st.caption(f"… and {len(clusters_resolved) - 60} more")
