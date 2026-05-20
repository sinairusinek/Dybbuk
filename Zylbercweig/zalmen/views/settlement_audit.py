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
import sys
from dataclasses import dataclass
from typing import Iterable

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

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

# Show this many rows per (type, kind) section by default. Each row spawns
# ~12 widgets (popover body, radio, text input, buttons), so a busy city like
# Warsaw with hundreds of rows × multiple types would otherwise instantiate
# thousands of widgets per Streamlit rerun — enough to make every picker
# interaction feel frozen. A "Show all" toggle lifts the cap on demand.
_ROW_CAP = 20

# Color palette for the dominant-org-type map mode. The typology is ~27 types;
# we colour the most common ones and lump the long tail as grey.
_TYPE_COLORS: dict[str, str] = {
    "Theatre": "#e6194B",
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


def _align_clusters_to_db(
    target_db: str,
    cluster_ids: list[str],
    a_rows: list[dict[str, str]],
    a_headers: list[str],
    db_rows: list[dict[str, str]],
    db_headers: list[str],
) -> str:
    a_by_cid = {r["cluster_id"]: r for r in a_rows}
    for cid in cluster_ids:
        row = a_by_cid.get(cid)
        if not row:
            continue
        row["decision"] = "ALIGN"
        row["aligned_db_id"] = target_db
    for r in db_rows:
        if r.get("db_id") == target_db:
            r["linked_cluster_ids"] = _merge_linked_ids(
                r.get("linked_cluster_ids", ""), *cluster_ids
            )
            break
    _persist_and_clear(a_headers, a_rows, db_headers, db_rows)
    return f"Aligned {len(cluster_ids)} cluster(s) → {target_db}"


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
            icon = {"exact": "🎯", "phonetic": "🔊", "fuzzy": "🔤"}.get(method_txt, "•")
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

def _render_map(
    ix,
    addr_by_dbid: dict[str, dict[str, str]],
    focus_qid: str | None,
) -> None:
    try:
        import folium  # noqa: F401
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

    cities = ix.cities()
    mappable = [(qid, en, yi) for (qid, en, yi) in cities if qid in coords]
    if not mappable:
        st.caption("No mappable cities in current index.")
        return

    counts = {qid: ix.mentions_in_city(qid) for qid, _en, _yi in mappable}
    max_n = max(counts.values()) or 1

    import folium

    # The focus city (set by either the picker or a previous map click) drives
    # both the initial map build AND the dynamic re-centering st_folium does
    # via its `center` / `zoom` props on each rerun.
    if focus_qid and focus_qid in coords:
        flat, flon = coords[focus_qid]
        focus_center = (flat, flon, 11)
    else:
        focus_center = (52.0, 19.0, 4)
    m = folium.Map(
        location=[focus_center[0], focus_center[1]],
        zoom_start=focus_center[2],
        tiles="OpenStreetMap",
    )

    for qid, en, yi in mappable:
        lat, lon = coords[qid]
        n = counts[qid]
        radius = 4 + 10 * (math.log1p(n) / math.log1p(max_n))
        if color_mode == "Dominant org type":
            dom = ix.dominant_org_type(qid)
            color = _TYPE_COLORS.get(dom, _FALLBACK_COLOR)
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

    out = st_folium(
        m,
        center=[focus_center[0], focus_center[1]],
        zoom=focus_center[2],
        width=None, height=420,
        returned_objects=["last_object_clicked_tooltip"],
        key="settlement_audit_map",
    )
    clicked = (out or {}).get("last_object_clicked_tooltip")
    if clicked:
        qid = clicked.split("|", 1)[0].strip()
        if qid and qid != focus_qid:
            # Route the click through audit_target_qid; render() picks it up at
            # the top of the next run and syncs settlement_audit_city, so the
            # picker and the map agree before the map is drawn again.
            st.session_state["audit_target_qid"] = qid
            st.rerun()

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
) -> list[tuple[str, str, str]]:
    """Return [(kind, id, label)] for every other item in the bucket."""
    out: list[tuple[str, str, str]] = []
    for d in bucket.db_cards:
        if self_kind == "db" and d.db_id == self_id:
            continue
        label = f"DB {d.db_id} · {d.name or d.name_yiddish or '(unnamed)'}"
        out.append(("db", d.db_id, label))
    for c in bucket.clusters:
        if self_kind == "cluster" and c.cluster_id == self_id:
            continue
        label = f"cluster {c.cluster_id} · {c.canonical_yiddish or '(no canonical)'} (n={c.cluster_size})"
        out.append(("cluster", c.cluster_id, label))
    return out


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

    with st.popover("Actions ⋯", use_container_width=False):
        # --- Mint as new entity (clusters only) ---
        if self_kind == "cluster":
            if st.button("Mint as new entity", key=f"{key}_mint", type="primary"):
                new_id, msg = _mint_db_from_clusters(
                    [self_id], bucket.org_type, canonical_yiddish, "",
                    a_rows, a_headers, db_rows, db_headers,
                )
                st.toast(msg, icon="✅")
                st.rerun()

        # --- Merge here ---
        st.markdown("**Merge here**")
        opts = _bucket_options(bucket, self_kind, self_id)
        if not opts:
            st.caption("No other items in this bucket.")
        else:
            pick = st.radio(
                "Merge with",
                options=opts,
                format_func=lambda t: t[2],
                key=f"{key}_pick",
                label_visibility="collapsed",
            )
            if st.button("Confirm merge", key=f"{key}_confirm", type="primary"):
                pk_kind, pk_id, _lbl = pick
                if self_kind == "cluster" and pk_kind == "db":
                    msg = _align_clusters_to_db(
                        pk_id, [self_id], a_rows, a_headers, db_rows, db_headers,
                    )
                elif self_kind == "cluster" and pk_kind == "cluster":
                    _new, msg = _mint_db_from_clusters(
                        [self_id, pk_id], bucket.org_type, canonical_yiddish, "",
                        a_rows, a_headers, db_rows, db_headers,
                    )
                elif self_kind == "db" and pk_kind == "cluster":
                    msg = _align_clusters_to_db(
                        self_id, [pk_id], a_rows, a_headers, db_rows, db_headers,
                    )
                else:  # db + db
                    msg = _merge_db_rows_op(
                        self_id, [pk_id], a_rows, a_headers, db_rows, db_headers,
                    )
                st.toast(msg, icon="✅")
                st.rerun()

        st.divider()

        # --- Search more ---
        st.markdown("**Search more**")
        st.caption("Search across all DB rows + clusters, not just this bucket.")
        q = st.text_input(
            "Search", key=f"{key}_q",
            placeholder="name or fragment...",
            label_visibility="collapsed",
        )
        if q and q.strip():
            results = _search_corpus(
                q.strip(), db_rows, a_rows, samples,
                exclude=(self_kind, self_id),
            )[:20]
            if not results:
                st.caption("No matches.")
            for r_kind, r_id, r_label, _score in results:
                cols = st.columns([4, 1])
                cols[0].caption(f"{r_kind} · {r_id} · {r_label}")
                if cols[1].button("Merge", key=f"{key}_m_{r_kind}_{r_id}"):
                    if self_kind == "cluster" and r_kind == "db":
                        msg = _align_clusters_to_db(
                            r_id, [self_id], a_rows, a_headers, db_rows, db_headers,
                        )
                    elif self_kind == "cluster" and r_kind == "cluster":
                        _new, msg = _mint_db_from_clusters(
                            [self_id, r_id], bucket.org_type, canonical_yiddish, "",
                            a_rows, a_headers, db_rows, db_headers,
                        )
                    elif self_kind == "db" and r_kind == "cluster":
                        msg = _align_clusters_to_db(
                            self_id, [r_id], a_rows, a_headers, db_rows, db_headers,
                        )
                    else:
                        msg = _merge_db_rows_op(
                            self_id, [r_id], a_rows, a_headers, db_rows, db_headers,
                        )
                    st.toast(msg, icon="✅")
                    st.rerun()

        # --- Research (Sinai only) ---
        if reviewer == ADMIN_REVIEWER:
            st.divider()
            st.markdown("**Research**")
            comment = st.text_area(
                "Question / focus (optional)",
                key=f"{key}_research_comment",
                placeholder="e.g. is this the same theatre as Goldfaden's tour company?",
                height=70,
            )
            if st.button("Queue for cluster-research", key=f"{key}_research"):
                _queue_research(
                    kind=self_kind, target_id=self_id, qid=qid,
                    org_type=bucket.org_type, label=self_label, reviewer=reviewer,
                    comment=(comment or "").strip(),
                )
                st.toast("Queued for cluster-research", icon="🔬")
            st.caption(
                "Switch to Claude and say: "
                f"`Use the cluster-research skill on {self_kind} {self_id} "
                f"in {qid} ({bucket.org_type}).`"
            )


def _search_corpus(
    query: str,
    db_rows: list[dict[str, str]],
    a_rows: list[dict[str, str]],
    samples: dict,
    exclude: tuple[str, str] = ("", ""),
) -> list[tuple[str, str, str, int]]:
    q = query.lower()
    results: list[tuple[str, str, str, int]] = []
    matched_clusters: set[str] = set()
    for r in db_rows:
        dbid = (r.get("db_id") or "").strip()
        if exclude == ("db", dbid):
            continue
        hay = f"{r.get('name','')} {r.get('name_yiddish','')}".lower()
        idx = hay.find(q)
        if idx >= 0:
            label = r.get("name") or r.get("name_yiddish") or "(unnamed)"
            results.append(("db", dbid, label, idx))
    for r in a_rows:
        cid = (r.get("cluster_id") or "").strip()
        if exclude == ("cluster", cid):
            continue
        hay = f"{r.get('canonical_yiddish','')} {r.get('name_variants','')}".lower()
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
            idx = sent.lower().find(q)
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
            disabled=not (n_cl >= 1 and n_db == 1),
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
            st.toast(msg, icon="✅")
            st.rerun()

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
                    _new, msg = _mint_db_from_clusters(
                        selected_cluster_ids, seed_type, new_name, new_name_latin,
                        a_rows, a_headers, db_rows, db_headers,
                    )
                    for cid in selected_cluster_ids:
                        st.session_state.pop(f"{sel_key_prefix}_cl_{cid}", None)
                    st.toast(msg, icon="✅")
                    st.rerun()

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
                    st.toast(msg, icon="✅")
                    st.rerun()

    with cols[3]:
        if st.button("Clear selection", use_container_width=True,
                     key=f"{sel_key_prefix}_btn_clear"):
            for k in list(st.session_state.keys()):
                if isinstance(k, str) and k.startswith(sel_key_prefix + "_cl_"):
                    st.session_state[k] = False
                if isinstance(k, str) and k.startswith(sel_key_prefix + "_db_"):
                    st.session_state[k] = False
            st.rerun()


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
    if target_qid:
        match = next((c for c in cities if c[0] == target_qid), None)
        if match is not None:
            st.session_state["settlement_audit_city"] = match
    focus_city = st.session_state.get("settlement_audit_city")
    focus_qid = focus_city[0] if focus_city else None

    # --- Map header ---
    with st.container(border=True):
        _render_map(ix, addr_by_dbid, focus_qid)

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
        city = st.selectbox(
            "Settlement", cities_sorted, format_func=_city_label,
            key="settlement_audit_city",
        )
    if not city:
        return
    qid = city[0]

    # --- Load editable data ---
    a_headers, a_rows = load_alignment(_mtime(ALIGN_FILE))
    db_headers, db_rows = load_core_db(_mtime(CORE_DB_FILE))
    samples = load_samples(_mtime(CLUSTER_FILE)) if CLUSTER_FILE.exists() else {}
    db_by_id = {r.get("db_id", ""): r for r in db_rows}
    a_rows_by_cid = {r["cluster_id"]: r for r in a_rows}

    buckets = ix.buckets_in_city(qid)
    if not buckets:
        st.info("No entries in this city.")
        return

    sel_key_prefix = f"sa_sel_{qid}"

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
            type_filter = st.multiselect(
                "Org types to show",
                options=[b.org_type for b in buckets],
                default=[b.org_type for b in buckets],
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
    top[0].metric("Types here", len(buckets))
    top[1].metric("DB rows", total_db)
    top[2].metric("Clusters", total_cl)
    top[3].metric("Mentions", ix.mentions_in_city(qid))

    st.divider()

    # --- Per-type sections ---
    def _cl_passes(c: ClusterCard) -> bool:
        tag = c.decision or "(undecided)"
        if decision_filter and tag not in decision_filter:
            return False
        if c.cluster_size < min_size:
            return False
        return True

    for bucket in buckets:
        if bucket.org_type not in type_filter:
            continue

        # Dedupe DB cards within this bucket
        seen: set[str] = set()
        unique_db: list[DbCard] = []
        for d in bucket.db_cards:
            if d.db_id in seen:
                continue
            seen.add(d.db_id)
            unique_db.append(d)
        unique_db.sort(key=lambda d: (d.name or d.name_yiddish or "").lower())

        clusters_visible = [c for c in bucket.clusters if _cl_passes(c)]
        clusters_visible.sort(key=lambda c: (bool(c.decision), -c.cluster_size))

        if not unique_db and not clusters_visible:
            continue

        st.subheader(
            f"{bucket.org_type} · {len(unique_db)} DB / {len(clusters_visible)} clusters"
        )

        align_counts: dict[str, int] = {}
        for c in bucket.clusters:
            if c.aligned_db_id:
                align_counts[c.aligned_db_id] = align_counts.get(c.aligned_db_id, 0) + 1

        cap_key = f"sa_capoff_{qid}_{bucket.org_type}"
        cap_off = st.session_state.get(cap_key, False)
        total_rows = len(unique_db) + len(clusters_visible)
        if total_rows > _ROW_CAP and not cap_off:
            if st.button(
                f"Show all {total_rows} rows in this section",
                key=f"{cap_key}_btn", use_container_width=False,
            ):
                st.session_state[cap_key] = True
                st.rerun()
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
                    st.rerun()

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
                        qid=qid,
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
                            badge += f" → {c.aligned_db_id}"
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
                        qid=qid,
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
