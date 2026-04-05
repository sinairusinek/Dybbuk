"""
A2-Addresses · Org Address Review view.

Features
--------
1. Two-column layout: list on left, detail panel opens in-place on right.
2. Tabs: Review (active clusters + sub-clusters) / Generic (confirmed generic labels).
   Exploded parent rows are hidden from both tabs.
3. Detail panel:
     a. Extracted settlement/address/venue chips — click to see sample texts
        with full-entry expander per sample.
    b. Linked cluster controls — open linked A1 clusters or unlink them.
    c. Mention manager — remove specific mentions from a linked cluster.
    d. "Explode by location" — auto-split by settlement.
    e. "Refined split" — manually assign individual mentions to named groups.
    f. Generic flag — marks a name as a label, not one entity.
    g. Confirm location + optional geocoding.

Files
-----
Reads:   ../organizations/org_addresses_review.tsv
         ../organizations/org_alignment_review.tsv
         ../organizations/organizations_clustered.tsv  (sample text lookup)
Writes:  ../organizations/org_addresses_review.tsv  (in-place)
         ../organizations/org_alignment_review.tsv  (cluster unlink)
         ../organizations/organizations_clustered.tsv  (mention removal)
"""

import csv, fcntl, pathlib, subprocess, sys, time, collections, re
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import streamlit as st

def _open_url(view: str, entity: str = "") -> str:
    """Build a deep-link URL for opening a specific view+entity in a new tab."""
    import urllib.parse
    params: dict[str, str] = {"view": view}
    if entity:
        params["entity"] = entity
    return "?" + urllib.parse.urlencode(params)

csv.field_size_limit(sys.maxsize)

ADDR_FILE      = pathlib.Path(__file__).parents[2] / "organizations" / "org_addresses_review.tsv"
CLUSTER_FILE   = pathlib.Path(__file__).parents[2] / "organizations" / "organizations_clustered.tsv"
CORE_DB_FILE   = pathlib.Path(__file__).parents[2] / "organizations" / "core_db.tsv"
ALIGN_FILE     = pathlib.Path(__file__).parents[2] / "organizations" / "org_alignment_review.tsv"
EXTRACT_SCRIPT = pathlib.Path(__file__).parents[2] / "organizations" / "extract_addresses.py"
LEXICON_DIR    = pathlib.Path(__file__).parents[2] / "The Lexicon"
PAGE_SIZE      = 50

# Optional map/geocode deps
try:
    import folium
    from streamlit_folium import st_folium
    HAS_MAP = True
except ImportError:
    HAS_MAP = False

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut
    HAS_GEOCODE = True
    _geocoder = Nominatim(user_agent="zalmen-zylbercweig")
except ImportError:
    HAS_GEOCODE = False

# Column names in organizations_clustered.tsv
_COL_CID      = "cluster_id"
_COL_SETTLE   = "_ - organizations - _ - locations - _ - settlement"
_COL_ADDR     = "_ - organizations - _ - locations - _ - address"
_COL_VENUE    = "_ - organizations - _ - locations - _ - Venue"
_COL_COUNTRY  = "_ - organizations - _ - locations - _ - country"
_COL_SENTENCE = "_ - organizations - _ - relations - _ - original_sentence"
_COL_HEADING  = "_ - heading"
_COL_FILE     = "File"
_COL_XMLID    = "_ - xml:id"

_MISSING = {"", "na", "n/a", "null", "none", "-", "--", "_"}
def _m(v): return v.strip().lower() in _MISSING
def _split(s): return [v.strip() for v in s.split("|") if v.strip()]


# ── XML entry lookup (reused from org_clusters.py) ───────────────────────────

_JSON_TO_XML = {
    "Volume5IIIorg.json":   "Structured_Volume5III.xml",
    "Volume_3IIIorg.json":  "Structured_Volume_3III.xml",
    "Volume_4IIIorg.json":  "Structured_Volume_4III.xml",
    "volume6IIIorg.json":   "Structured_volume6III.xml",
    "volume7IIIorg.json":   "Structured_volume7III.xml",
    "volume_1IIIorg.json":  "Structured_volume_1III.xml",
    "volume_2IIIorg.json":  "Structured_volume_2III.xml",
}

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_ID = "{http://www.w3.org/XML/1998/namespace}id"

@st.cache_resource(show_spinner=False)
def _load_all_xml() -> dict[str, ET.ElementTree]:
    trees: dict[str, ET.ElementTree] = {}
    for json_name, xml_name in _JSON_TO_XML.items():
        xml_path = LEXICON_DIR / xml_name
        if xml_path.exists():
            try:
                trees[json_name] = ET.parse(xml_path)
            except ET.ParseError:
                pass
    return trees

def get_entry_text(json_file: str, xml_id: str) -> str | None:
    if not json_file or not xml_id:
        return None
    tree = _load_all_xml().get(json_file)
    if tree is None:
        return None
    for el in tree.getroot().iter(f"{{{TEI_NS}}}div"):
        if el.get(XML_ID) == xml_id:
            text = " ".join(
                " ".join(e.itertext()).strip() for e in el.iter() if e.text or e.tail
            )
            return re.sub(r"\s+", " ", text).strip() or None
    return None


# ── I/O ───────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_orgs(mtime: float):
    with open(ADDR_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_alignment_rows(mtime: float):
    if not ALIGN_FILE.exists():
        return [], []
    with open(ALIGN_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames), list(r)


@st.cache_data(show_spinner=False)
def load_cluster_rows(mtime: float):
    with open(CLUSTER_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames), list(r)

@st.cache_data(show_spinner=False)
def load_samples(mtime: float) -> dict[str, dict[str, list[tuple[str,str,str,str]]]]:
    """
    Returns {cluster_id: {settlement: [(heading, sentence, file, xml_id), ...]}}
    Settlement key "" means no settlement recorded.
    Max 3 samples per (cluster, settlement).
    """
    idx: dict[str, dict[str, list]] = collections.defaultdict(lambda: collections.defaultdict(list))
    with open(CLUSTER_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            cid  = row.get(_COL_CID, "").strip()
            if not cid: continue
            s    = row.get(_COL_SETTLE, "").strip()
            sent = row.get(_COL_SENTENCE, "").strip()
            head = row.get(_COL_HEADING, "").strip()
            fle  = row.get(_COL_FILE, "").strip()
            xid  = row.get(_COL_XMLID, "").strip()
            bucket = idx[cid][s]
            if len(bucket) < 3 and (sent or head):
                bucket.append((head, sent, fle, xid))
    return {k: dict(v) for k, v in idx.items()}

def save_orgs(headers, rows):
    lock = ADDR_FILE.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(ADDR_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
                w.writeheader(); w.writerows(rows)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def save_alignment(headers, rows):
    lock = ALIGN_FILE.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(ALIGN_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
                w.writeheader()
                w.writerows(rows)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def save_cluster_rows(headers, rows):
    lock = CLUSTER_FILE.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(CLUSTER_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
                w.writeheader()
                w.writerows(rows)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)

def get_mtime(path=ADDR_FILE): return path.stat().st_mtime if path.exists() else 0.0


def _clear_data_caches():
    load_orgs.clear()
    load_alignment_rows.clear()
    load_cluster_rows.clear()
    load_samples.clear()


def _refresh_after_upstream_change():
    _clear_data_caches()
    _maybe_regenerate()


def _linked_source_rows(linked_cids: list[str], cluster_rows: list[dict[str, str]]):
    linked_set = set(linked_cids)
    return [
        (idx, row)
        for idx, row in enumerate(cluster_rows)
        if row.get(_COL_CID, "").strip() in linked_set
    ]


def _unlink_cluster(cluster_id: str):
    headers, rows = load_alignment_rows(get_mtime(ALIGN_FILE))
    if not rows:
        st.error("Alignment file is not available.")
        return

    for row in rows:
        if row.get("cluster_id", "").strip() == cluster_id:
            row["decision"] = ""
            row["aligned_db_id"] = ""
            save_alignment(headers, rows)
            _refresh_after_upstream_change()
            st.rerun()

    st.error(f"Could not find alignment row for {cluster_id}.")


def _remove_mention(cluster_row_idx: int):
    headers, rows = load_cluster_rows(get_mtime(CLUSTER_FILE))
    if cluster_row_idx < 0 or cluster_row_idx >= len(rows):
        st.error("The selected mention could not be found.")
        return

    rows[cluster_row_idx][_COL_CID] = ""
    save_cluster_rows(headers, rows)
    _refresh_after_upstream_change()
    st.rerun()

def _maybe_regenerate():
    """Re-run extract_addresses.py if upstream data (alignment/core DB) changed.
    Skipped on read-only deployments (e.g. Streamlit Cloud at /mount/src)."""
    if not EXTRACT_SCRIPT.exists() or not CORE_DB_FILE.exists():
        return
    # Skip if the file system is read-only (Streamlit Cloud mounts sources as read-only)
    try:
        ADDR_FILE.parent.stat()
        import os
        if not os.access(str(ADDR_FILE.parent), os.W_OK):
            return
    except Exception:
        return
    upstream_mtime = max(get_mtime(CORE_DB_FILE), get_mtime(ALIGN_FILE), get_mtime(CLUSTER_FILE))
    if ADDR_FILE.exists() and get_mtime(ADDR_FILE) >= upstream_mtime:
        return  # already up to date
    try:
        subprocess.run([sys.executable, str(EXTRACT_SCRIPT)], check=True)
        load_orgs.clear()  # bust cache so Streamlit picks up the new file
    except Exception:
        pass  # read-only filesystem — silently skip

# ── Geocoding ─────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=3600)
def geocode(query: str):
    if not HAS_GEOCODE or not query.strip():
        return None
    def _try(q):
        try:
            time.sleep(1)
            loc = _geocoder.geocode(q, timeout=10)
            if loc:
                return round(loc.latitude, 6), round(loc.longitude, 6)
        except GeocoderTimedOut:
            pass
        return None
    # Normalise junction patterns: "X and Y" / "X und Y" / "X corner Y" → "X & Y"
    normalized = re.sub(r'\b(?:and|und|corner(?:\s+of)?)\b', '&', query, flags=re.IGNORECASE)
    result = _try(normalized)
    if result:
        return result
    # Fallback: try just the first street + everything after the last comma (city)
    if '&' in normalized:
        first_street = normalized.split('&')[0].strip().rstrip(',')
        parts = normalized.rsplit(',', 1)
        city_part = parts[-1].strip() if len(parts) > 1 else ''
        if city_part and first_street:
            result = _try(f"{first_street}, {city_part}")
            if result:
                return result
    return None

# ── Helpers ───────────────────────────────────────────────────────────────────

def _geo_status(row):
    has_geolocated = bool(str(row.get("lat", "")).strip()) and bool(str(row.get("lon", "")).strip())
    has_address = bool(row.get("confirmed_address", "").strip())
    has_settlement = bool(row.get("confirmed_settlement", "").strip())
    has_country = bool(row.get("extracted_countries", "").strip())

    if has_geolocated:
        return "geolocated", "Geolocated", "#2563eb", "🌐"
    if has_address:
        return "has_address", "Has address", "#2f855a", "🟢"
    if has_settlement:
        return "has_settlement", "Has settlement", "#2b6cb0", "🔵"
    if has_country:
        return "has_country", "Has country", "#b7791f", "🟡"
    return "none", "No location", "#718096", "🔴"


def _status_badges(row):
    badges = []
    if row.get("is_exploded") == "TRUE":
        badges.append("💥 exploded")
    if row.get("is_generic") == "TRUE":
        badges.append("🔶 generic")
    return badges

def _new_subcluster_row(headers, parent, settlement, addresses, venues, countries, mentions, idx):
    """Build one sub-entry row for the explode/split operation."""
    empty = {h: "" for h in headers}
    empty.update({
        "db_id":                f"{parent['db_id']}_X{idx:02d}",
        "canonical_yiddish":    parent["canonical_yiddish"],
        "org_type":             parent["org_type"],
        "mentions":             str(mentions),
        "n_settlements":        "1",
        "extracted_settlements": settlement,
        "extracted_addresses":  " | ".join(sorted(addresses)),
        "extracted_venues":     " | ".join(sorted(venues)),
        "extracted_countries":  " | ".join(sorted(countries)),
        "is_generic":           "",
        "is_exploded":          "",
        "parent_db_id":         parent["db_id"],
        "confirmed_settlement": settlement,
        "confirmed_settlement_yiddish": settlement,
    })
    return empty

def _build_explode_candidates(parent_cid, all_source_rows):
    """
    From organizations_clustered.tsv rows for this cluster, group by settlement
    and return a list of (settlement, g) sorted by mentions descending.
    """
    groups: dict[str, dict] = {}
    for r in all_source_rows:
        s = r.get(_COL_SETTLE, "").strip() or r.get(_COL_COUNTRY, "").strip() or "(unknown)"
        if s not in groups:
            groups[s] = {"mentions": 0, "addresses": set(), "venues": set(), "countries": set()}
        g = groups[s]
        g["mentions"] += 1
        for col, key in ((_COL_ADDR,"addresses"),(_COL_VENUE,"venues"),(_COL_COUNTRY,"countries")):
            v = r.get(col,"").strip()
            if not _m(v): g[key].add(v)
    return sorted(groups.items(), key=lambda x: -x[1]["mentions"])


def _render_linked_clusters(db_id: str, linked_cids: list[str], align_rows_by_cid: dict[str, dict[str, str]]):
    if not linked_cids:
        return

    pending_key = f"pending_cluster_unlink_{db_id}"
    st.markdown("**Linked clusters**")
    for cluster_id in linked_cids:
        row = align_rows_by_cid.get(cluster_id, {})
        decision = row.get("decision", "").strip() or "Undecided"
        cluster_name = row.get("canonical_yiddish", "").strip() or cluster_id
        cluster_size = row.get("cluster_size", "").strip()

        info_col, open_col, unlink_col = st.columns([4.2, 1.3, 1.1])
        info_bits = [cluster_id, cluster_name, decision]
        if cluster_size:
            info_bits.append(f"{cluster_size} mentions")
        info_col.caption(" · ".join(info_bits))

        open_col.link_button("Open in Review ↗", _open_url("Entity Review", cluster_id),
                             use_container_width=True)

        if unlink_col.button("Unlink", key=f"unlink-{db_id}-{cluster_id}", use_container_width=True):
            st.session_state[pending_key] = cluster_id
            st.rerun()

        if st.session_state.get(pending_key) == cluster_id:
            st.warning(
                f"Unlink {cluster_id} from this entity? The cluster will stay in A1 with its candidates, but it will no longer feed this card."
            )
            confirm_col, cancel_col = st.columns([1.2, 1])
            if confirm_col.button("Confirm unlink", key=f"confirm-unlink-{db_id}-{cluster_id}", type="primary"):
                st.session_state.pop(pending_key, None)
                _unlink_cluster(cluster_id)
            if cancel_col.button("Cancel", key=f"cancel-unlink-{db_id}-{cluster_id}"):
                st.session_state.pop(pending_key, None)
                st.rerun()


def _render_mentions_manager(db_id: str, linked_sources: list[tuple[int, dict[str, str]]]):
    with st.expander(f"Manage mentions ({len(linked_sources)})", expanded=False):
        st.caption("Remove a mention to clear its cluster assignment. It will stop contributing to this card until it is reclustered.")

        if not linked_sources:
            st.caption("No linked mentions found.")
            return

        pending_key = f"pending_mention_remove_{db_id}"
        for cluster_row_idx, src_row in linked_sources:
            cluster_id = src_row.get(_COL_CID, "").strip()
            heading = src_row.get(_COL_HEADING, "").strip()
            settle = src_row.get(_COL_SETTLE, "").strip()
            sent = src_row.get(_COL_SENTENCE, "").strip()
            fle = src_row.get(_COL_FILE, "").strip()
            xid = src_row.get(_COL_XMLID, "").strip()

            text_col, action_col = st.columns([5, 1])
            with text_col:
                meta = [cluster_id or "(unclustered)"]
                if settle:
                    meta.append(settle)
                if fle or xid:
                    meta.append(" / ".join(v for v in (fle, xid) if v))
                st.caption(" · ".join(meta))
                if heading:
                    st.markdown(f"*{heading}*")
                if sent:
                    st.markdown(
                        f"<div dir='rtl' style='font-size:0.88em; border-left:3px solid #d1d5db; padding-left:8px; margin:4px 0'>{sent}</div>",
                        unsafe_allow_html=True,
                    )

            with action_col:
                if st.button("Remove", key=f"remove-mention-{db_id}-{cluster_row_idx}", use_container_width=True):
                    st.session_state[pending_key] = cluster_row_idx
                    st.rerun()

            if st.session_state.get(pending_key) == cluster_row_idx:
                st.warning("Remove this mention from its cluster? This clears its cluster_id in the clustered TSV.")
                confirm_col, cancel_col = st.columns([1.2, 1])
                if confirm_col.button("Confirm remove", key=f"confirm-remove-{db_id}-{cluster_row_idx}", type="primary"):
                    st.session_state.pop(pending_key, None)
                    _remove_mention(cluster_row_idx)
                if cancel_col.button("Cancel", key=f"cancel-remove-{db_id}-{cluster_row_idx}"):
                    st.session_state.pop(pending_key, None)
                    st.rerun()
            st.divider()


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.header("Entity Cards")

    if not CLUSTER_FILE.exists():
        st.error(f"`{CLUSTER_FILE}` not found — run `cluster_orgs.py` first.")
        return

    _maybe_regenerate()

    if not ADDR_FILE.exists():
        st.error(f"`{ADDR_FILE}` not found — run `extract_addresses.py` first.")
        return

    mtime_addr    = get_mtime(ADDR_FILE)
    mtime_cluster = get_mtime(CLUSTER_FILE)
    headers, rows = load_orgs(mtime_addr)
    samples       = load_samples(mtime_cluster)

    sel_cid = st.session_state.get("addr_selected")
    if not sel_cid:
        st.markdown("### Cards")
        _render_list(headers, rows, samples)
        return

    sel_row = next((r for r in rows if r.get("db_id") == sel_cid), None)
    if not sel_row:
        st.session_state.addr_selected = None
        st.rerun()

    visible_ids = st.session_state.get("addr_visible_ids_geo", [])
    try:
        sel_idx = visible_ids.index(sel_cid)
    except ValueError:
        sel_idx = -1

    nav_col1, nav_col2, nav_col3, nav_col4 = st.columns([1.2, 0.8, 3.2, 0.8])
    if nav_col1.button("← Back to cards", key="back_to_cards"):
        st.session_state.addr_selected = None
        st.rerun()
    if nav_col2.button("←", key="geo_prev_entity", disabled=sel_idx <= 0):
        prev_idx = sel_idx - 1
        st.session_state.addr_selected = visible_ids[prev_idx]
        st.session_state["addr_page_geo"] = prev_idx // PAGE_SIZE
        st.rerun()
    nav_col3.markdown("### Selected Entity")
    if nav_col4.button("→", key="geo_next_entity", disabled=sel_idx < 0 or sel_idx >= len(visible_ids) - 1):
        next_idx = sel_idx + 1
        st.session_state.addr_selected = visible_ids[next_idx]
        st.session_state["addr_page_geo"] = next_idx // PAGE_SIZE
        st.rerun()
    _render_detail(headers, rows, sel_row, samples)


# ── List panel ────────────────────────────────────────────────────────────────

def _render_list(headers, rows, samples):
    pool = [r for r in rows if r.get("is_exploded") != "TRUE"]
    _render_tab(headers, pool, tab_key="geo")


def _render_tab(headers, pool, tab_key: str, read_only_hint: bool = False):
    """Card-grid queue with geolocation color coding and filters."""
    if not pool:
        st.info("Nothing here yet.")
        return

    all_types = sorted({r["org_type"] for r in pool if r["org_type"].strip()})
    col_f1, col_f2, col_f3 = st.columns([2, 1.2, 1])
    with col_f1:
        name_q = st.text_input("Search name", key=f"name_q_{tab_key}")
    with col_f2:
        status_f = st.selectbox(
            "Geo status",
            ["All", "Geolocated", "Has address", "Has settlement", "Has country", "No location"],
            key=f"status_{tab_key}",
        )
    with col_f3:
        sort_m = st.selectbox("Sort", ["Mentions ↓", "Settlements ↓", "Alphabetical"], key=f"sort_{tab_key}")

    sel_types = st.multiselect("Org type", all_types, key=f"types_{tab_key}")
    tcol1, tcol2 = st.columns(2)
    show_generic = tcol1.toggle("Include generic", value=True, key=f"generic_{tab_key}")
    show_sub = tcol2.toggle("Include sub-clusters", value=True, key=f"sub_{tab_key}")

    visible = pool
    if sel_types:
        visible = [r for r in visible if r["org_type"] in sel_types]
    if name_q.strip():
        q = name_q.strip().lower()
        visible = [r for r in visible if q in (r.get("canonical_yiddish", "").lower())]
    if not show_generic:
        visible = [r for r in visible if r.get("is_generic") != "TRUE"]
    if not show_sub:
        visible = [r for r in visible if not r.get("parent_db_id")]

    status_map = {
        "Geolocated": "geolocated",
        "Has address": "has_address",
        "Has settlement": "has_settlement",
        "Has country": "has_country",
        "No location": "none",
    }
    if status_f in status_map:
        wanted = status_map[status_f]
        visible = [r for r in visible if _geo_status(r)[0] == wanted]

    if sort_m == "Settlements ↓":
        visible = sorted(visible, key=lambda r: -int(r.get("n_settlements", 0) or 0))
    elif sort_m == "Alphabetical":
        visible = sorted(visible, key=lambda r: r.get("canonical_yiddish", ""))
    else:
        visible = sorted(visible, key=lambda r: -int(r.get("mentions", 0) or 0))

    visible = [r for r in visible if r.get("db_id")]
    st.session_state[f"addr_visible_ids_{tab_key}"] = [r["db_id"] for r in visible]

    # ── Metrics ───────────────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Showing", len(visible))
    m2.metric("🌐 Geolocated", sum(1 for r in visible if _geo_status(r)[0] == "geolocated"))
    m3.metric("🟢 Has address", sum(1 for r in visible if _geo_status(r)[0] == "has_address"))
    reviewed = sum(1 for r in visible if _geo_status(r)[0] != "none")
    if visible:
        m4.progress(reviewed / len(visible), text=f"{reviewed}/{len(visible)}")

    if read_only_hint:
        st.caption("Click a row to open its detail and un-generic it if needed.")

    if not visible:
        st.info("No organizations match the current filter.")
        return

    # ── Pagination ────────────────────────────────────────────────────────────
    total_pages = max(1, (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE)
    fhash = f"{sel_types}|{status_f}|{sort_m}|{tab_key}"
    page_key   = f"addr_page_{tab_key}"
    hash_key   = f"addr_fhash_{tab_key}"
    if st.session_state.get(hash_key) != fhash:
        st.session_state[page_key] = 0
        st.session_state[hash_key] = fhash
        st.session_state.addr_selected = None

    page = st.session_state.get(page_key, 0)
    page_rows = visible[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    pc1, pc2, pc3 = st.columns([1, 4, 1])
    with pc1:
        if st.button("◀", disabled=page==0, key=f"prev_{tab_key}"):
            st.session_state[page_key] -= 1
            st.session_state.addr_selected = None
            st.rerun()
    with pc2:
        st.caption(f"Page {page+1}/{total_pages}  ({len(visible)} orgs)")
    with pc3:
        if st.button("▶", disabled=page>=total_pages-1, key=f"next_{tab_key}"):
            st.session_state[page_key] += 1
            st.session_state.addr_selected = None
            st.rerun()

    # ── Cards ─────────────────────────────────────────────────────────────────
    sel_cid = st.session_state.get("addr_selected")
    grid_cols = st.columns(3)
    col_idx = 0
    for row in page_rows:
        cid = row["db_id"]
        name = row.get("canonical_yiddish", "") or cid
        geo_key, geo_label, geo_color, geo_icon = _geo_status(row)
        badges = _status_badges(row)
        badge_txt = " · ".join(badges)
        sub_hint = f"sub of {row.get('parent_db_id')}" if row.get("parent_db_id") else ""

        with grid_cols[col_idx]:
            st.markdown(
                f"<div style='border-left:6px solid {geo_color}; padding:8px 10px; border-radius:6px; background:#f8fafc;'>"
                f"<div dir='rtl' style='font-size:1.05em; font-weight:700'>{name}</div>"
                f"<div style='font-size:0.85em; color:#475569'>{geo_icon} {geo_label}</div>"
                f"<div style='font-size:0.82em; color:#64748b'>{row.get('org_type','')} · {row.get('mentions','?')} mentions · {row.get('n_settlements','?')} settlements</div>"
                f"<div style='font-size:0.8em; color:#64748b'>{badge_txt or sub_hint}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            if st.button("Open card", key=f"sel_{tab_key}_{cid}", use_container_width=True,
                         type="primary" if sel_cid == cid else "secondary"):
                st.session_state.addr_selected = None if sel_cid == cid else cid
                st.rerun()

        col_idx = (col_idx + 1) % 3


# ── Detail panel ─────────────────────────────────────────────────────────────

def _render_detail(headers, rows, row, samples):
    cid      = row["db_id"]
    row_idx  = next((i for i,r in enumerate(rows) if r.get("db_id")==cid), None)
    canonical = row.get("canonical_yiddish","") or cid
    _, align_rows = load_alignment_rows(get_mtime(ALIGN_FILE))
    align_rows_by_cid = {
        r.get("cluster_id", "").strip(): r
        for r in align_rows
        if r.get("cluster_id", "").strip()
    }
    linked_cids = _split(row.get("linked_cluster_ids", ""))
    _, cluster_rows = load_cluster_rows(get_mtime(CLUSTER_FILE))
    linked_sources = _linked_source_rows(linked_cids, cluster_rows)

    new_name = st.text_input(
        "Entity name",
        value=canonical,
        key=f"entity_name_{cid}",
        placeholder="Edit the organization/entity name",
    ).strip()

    settlements = _split(row.get("extracted_settlements",""))
    addresses   = _split(row.get("extracted_addresses",""))
    venues      = _split(row.get("extracted_venues",""))
    countries   = _split(row.get("extracted_countries",""))

    # Merge samples from all linked clusters for this DB entity
    cluster_samples: dict[str, list] = {}
    for _lcid in linked_cids:
        for _settle, _slist in samples.get(_lcid, {}).items():
            cluster_samples.setdefault(_settle, []).extend(_slist)

    all_samples = []
    for settle_key, sample_list in cluster_samples.items():
        for sample in sample_list:
            all_samples.append((settle_key, sample))

    show_samples_key = f"show_entity_samples_{cid}"
    title_col, sample_col = st.columns([4, 1.4])
    with title_col:
        st.subheader(new_name or canonical)
        st.caption(
            f"{row.get('org_type','')} · {row.get('mentions','?')} mentions · "
            f"{row.get('n_settlements','?')} distinct settlements"
            + (f" · sub-entry of `{row['parent_db_id']}`" if row.get("parent_db_id") else "")
        )
    with sample_col:
        show_samples = st.session_state.get(show_samples_key, False)
        sample_label = "Hide sample texts" if show_samples else "Click to see sample texts"
        if st.button(sample_label, key=f"toggle_samples_{cid}", disabled=not all_samples, use_container_width=True):
            st.session_state[show_samples_key] = not show_samples
            st.rerun()

    if st.session_state.get(show_samples_key, False) and all_samples:
        _render_all_samples(all_samples, show_title=True)

    if linked_cids:
        _render_linked_clusters(cid, linked_cids, align_rows_by_cid)
        st.divider()

    is_exploded = row.get("is_exploded") == "TRUE"
    if is_exploded:
        st.info("💥 This cluster has been exploded into sub-clusters. Edit the sub-clusters individually.")
        if st.button("↩ Un-explode (collapse back)", key=f"unexplode_{cid}"):
            _do_unexplode(headers, rows, cid, row_idx)
        return

    # ── Generic flag ──────────────────────────────────────────────────────────
    is_generic = row.get("is_generic") == "TRUE"
    new_generic = st.checkbox(
        "🔶 Generic label — not a single organization",
        value=is_generic, key=f"gen_{cid}",
        help="Check if this name labels many different organizations across cities. "
             "Use 'Explode' to split into per-city candidates."
    )

    if settlements or addresses or venues:
        if settlements:
            st.markdown("**Extracted location data** — click a settlement to see sample texts:")
        else:
            st.markdown("**Extracted location data**")

        if settlements:
            chip_cols = st.columns(min(len(settlements), 6))
            for i, s in enumerate(settlements[:18]):
                count = len(cluster_samples.get(s, []))
                label = f"{'📍 ' if count else ''}{s}"
                with chip_cols[i % len(chip_cols)]:
                    key = f"chip_{cid}_{i}"
                    active = st.session_state.get(f"chip_active_{cid}") == s
                    if st.button(label, key=key, type="primary" if active else "secondary",
                                 use_container_width=True):
                        st.session_state[f"chip_active_{cid}"] = None if active else s
                        st.rerun()
            if len(settlements) > 18:
                st.caption(f"… and {len(settlements)-18} more settlements")

        active_s = st.session_state.get(f"chip_active_{cid}")
        if active_s:
            _render_samples(active_s, cluster_samples.get(active_s, []),
                            cluster_samples.get("", []))
        elif all_samples:
            st.caption("No settlement selected — showing mixed entry samples.")
            _render_all_samples(all_samples, show_title=False)

        if addresses:
            st.markdown("*Addresses:* " + "  ·  ".join(addresses[:8]))
        if venues and set(venues) - set(addresses):
            st.markdown("*Venues:* " + "  ·  ".join(v for v in venues[:6] if v not in addresses))
    elif all_samples:
        st.markdown("**Entry context samples**")
        _render_all_samples(all_samples, show_title=False)

    st.divider()

    _render_mentions_manager(cid, linked_sources)

    st.divider()

    # ── Explode / Split by location ───────────────────────────────────────────
    can_explode = len(settlements) > 1 or len(countries) > 1
    if can_explode:
        explode_label = "💥 Explode by location" if new_generic else "✂️ Split by location"
        explode_help  = (
            "Split into one sub-cluster per city/country." if new_generic
            else "This organization appears in multiple cities. Split into per-city sub-clusters."
        )
        with st.expander(f"{explode_label}  ({len(settlements) or len(countries)} locations)", expanded=False):
            _render_explode_panel(headers, rows, row, row_idx, settlements, countries, new_generic)

    st.divider()

    # ── Confirmed location (only for non-generic, non-exploded) ───────────────
    if not new_generic:
        st.markdown("""<style>
input[aria-label="Settlement"],
input[aria-label="Address (original script)"] {
    text-align: center !important;
    direction: rtl !important;
    background-color: #f5f2ff !important;
    border-color: #c8b8f0 !important;
}
</style>""", unsafe_allow_html=True)
        st.markdown("**Confirmed location:**")
        lc1, lc2 = st.columns(2)
        with lc1:
            st.markdown(
                "<p style='text-align:center;font-weight:600;margin-bottom:0.2rem'>Settlement</p>",
                unsafe_allow_html=True,
            )
            new_settle = st.text_input(
                "Settlement", value=row.get("confirmed_settlement", ""),
                key=f"settle_{cid}", label_visibility="collapsed",
            )
        with lc2:
            st.markdown(
                "<p style='text-align:center;font-weight:600;margin-bottom:0.2rem'>Address (original script)</p>",
                unsafe_allow_html=True,
            )
            new_addr = st.text_input(
                "Address (original script)", value=row.get("confirmed_address", ""),
                key=f"addr_{cid}", label_visibility="collapsed",
            )

        y1, y2 = st.columns(2)
        with y1:
            st.markdown(
                "<p style='text-align:center;font-weight:600;margin-bottom:0.2rem'>Settlement (Yiddish)</p>",
                unsafe_allow_html=True,
            )
            new_settle_yid = st.text_input(
                "Settlement (Yiddish)",
                value=row.get("confirmed_settlement_yiddish", row.get("confirmed_settlement", "")),
                key=f"settle_yid_{cid}",
                label_visibility="collapsed",
            )
        with y2:
            st.caption("Romanized settlement in 'Settlement', Yiddish form here.")

        gc1, gc2 = st.columns([3,1])
        new_roman = gc1.text_input("Romanized address for geocoding",
                                    value=row.get("confirmed_address_romanized",""),
                                    key=f"roman_{cid}", placeholder="Street name and house number, City")

        _lat_key, _lon_key = f"_gc_lat_{cid}", f"_gc_lon_{cid}"
        new_lat = st.session_state.get(_lat_key) or row.get("lat", "")
        new_lon = st.session_state.get(_lon_key) or row.get("lon", "")

        with gc2:
            st.markdown("&nbsp;", unsafe_allow_html=True)
            if HAS_GEOCODE and HAS_MAP:
                q = new_roman.strip() or f"{new_addr.strip()}, {new_settle.strip()}"
                if st.button("🌍 Geocode", key=f"gc_{cid}", disabled=not q):
                    with st.spinner("Geocoding…"):
                        res = geocode(q)
                    if res:
                        new_lat = st.session_state[_lat_key] = str(res[0])
                        new_lon = st.session_state[_lon_key] = str(res[1])
                        st.success(f"{res[0]}, {res[1]}")
                    else:
                        st.warning("Not found — try a more specific romanized address.")

        if new_lat and new_lon and HAS_MAP:
            dragged = _render_map(cid, canonical, new_lat, new_lon)
            if dragged:
                new_lat = st.session_state[_lat_key] = str(dragged[0])
                new_lon = st.session_state[_lon_key] = str(dragged[1])
            lc3, lc4 = st.columns(2)
            new_lat = lc3.text_input("Lat", value=new_lat, key=f"lat_{cid}")
            new_lon = lc4.text_input("Lon", value=new_lon, key=f"lon_{cid}")
    else:
        new_settle = new_settle_yid = new_addr = new_roman = new_lat = new_lon = ""

    new_note = st.text_input("Notes", value=row.get("reviewer_notes",""),
                              key=f"note_{cid}", placeholder="Notes (optional)")

    # ── Save ──────────────────────────────────────────────────────────────────
    if st.button("💾 Save", key=f"save_{cid}", type="primary") and row_idx is not None:
        # Ensure audit columns exist
        for _col in ("reviewer", "reviewed_at"):
            if _col not in headers:
                headers.append(_col)
                for _r in rows:
                    _r.setdefault(_col, "")
        rows[row_idx]["canonical_yiddish"]            = new_name or canonical
        rows[row_idx]["is_generic"]                  = "TRUE" if new_generic else ""
        rows[row_idx]["confirmed_settlement"]         = "" if new_generic else new_settle
        if "confirmed_settlement_yiddish" in headers:
            rows[row_idx]["confirmed_settlement_yiddish"] = "" if new_generic else new_settle_yid
        rows[row_idx]["confirmed_address"]            = "" if new_generic else new_addr
        rows[row_idx]["confirmed_address_romanized"]  = "" if new_generic else new_roman
        rows[row_idx]["lat"]                          = "" if new_generic else new_lat
        rows[row_idx]["lon"]                          = "" if new_generic else new_lon
        rows[row_idx]["reviewer_notes"]               = new_note
        rows[row_idx]["reviewer"]                     = st.session_state.get("reviewer", "")
        rows[row_idx]["reviewed_at"]                  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        save_orgs(headers, rows)
        load_orgs.clear()
        st.session_state.pop(f"_gc_lat_{cid}", None)
        st.session_state.pop(f"_gc_lon_{cid}", None)
        st.rerun()


# ── Sample text panel ─────────────────────────────────────────────────────────

def _render_samples(settlement: str, samples: list, no_settle_samples: list):
    """Show up to 3 sample sentences for a given settlement, each with a full-entry expander."""
    all_samples = samples or no_settle_samples
    if not all_samples:
        st.caption(f"No sample texts found for '{settlement}'.")
        return
    st.markdown(f"**Sample texts for `{settlement}`:**")
    for heading, sent, fle, xid in all_samples[:3]:
        if heading:
            st.markdown(f"*{heading}*")
        if sent:
            st.markdown(
                f"<div dir='rtl' style='font-size:0.9em; border-left:3px solid #93c5fd; "
                f"padding-left:8px; margin:4px 0'>{sent}</div>",
                unsafe_allow_html=True,
            )
        if xid and fle:
            with st.expander(f"📄 Full entry ({xid})", expanded=False):
                entry_text = get_entry_text(fle, xid)
                if entry_text:
                    st.markdown(
                        f"<div dir='rtl' style='font-size:0.88em; white-space:pre-wrap; "
                        f"line-height:1.6;'>{entry_text}</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption(f"Entry not found ({_JSON_TO_XML.get(fle, fle)}).")


def _render_all_samples(
    all_samples: list[tuple[str, tuple[str, str, str, str]]],
    max_items: int = 4,
    show_title: bool = True,
):
    """Show mixed samples across all settlements when no specific settlement is selected."""
    if not all_samples:
        return
    if show_title:
        st.markdown("**Sample texts (all locations):**")
    for settle_key, (heading, sent, fle, xid) in all_samples[:max_items]:
        if heading:
            st.markdown(f"*{heading}*")
        if settle_key:
            st.caption(f"Settlement: {settle_key}")
        elif not settle_key:
            st.caption("Settlement: (not extracted)")
        if sent:
            st.markdown(
                f"<div dir='rtl' style='font-size:0.9em; border-left:3px solid #93c5fd; "
                f"padding-left:8px; margin:4px 0'>{sent}</div>",
                unsafe_allow_html=True,
            )
        if xid and fle:
            with st.expander(f"📄 Full entry ({xid})", expanded=False):
                entry_text = get_entry_text(fle, xid)
                if entry_text:
                    st.markdown(
                        f"<div dir='rtl' style='font-size:0.88em; white-space:pre-wrap; "
                        f"line-height:1.6;'>{entry_text}</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption(f"Entry not found ({_JSON_TO_XML.get(fle, fle)}).")


# ── Explode panel ─────────────────────────────────────────────────────────────

def _render_explode_panel(headers, rows, parent_row, parent_idx, settlements, countries, is_generic):
    """Preview and confirm explode/split into per-settlement sub-clusters, with optional
    refined split that lets the reviewer assign individual mentions to named groups."""
    did = parent_row["db_id"]
    linked_cids = _split(parent_row.get("linked_cluster_ids", ""))
    _, cluster_rows = load_cluster_rows(get_mtime(CLUSTER_FILE))
    source = [row for _, row in _linked_source_rows(linked_cids, cluster_rows)]

    # Toggle between auto-explode and refined split
    refined = st.toggle("🔬 Refined split — assign individual mentions to groups",
                        key=f"refined_toggle_{did}", value=False)

    if refined:
        _render_refined_split(headers, rows, parent_row, parent_idx, source)
    else:
        # ── Auto-explode by settlement ────────────────────────────────────────
        candidates = _build_explode_candidates(did, source)
        st.markdown(f"**{len(candidates)} proposed sub-clusters** — review and confirm names:")
        st.caption("Each will become an independent entry. Sub-clusters with unknown location are grouped as '(unknown)'.")

        sub_names = {}
        for i, (settle, g) in enumerate(candidates):
            col_a, col_b = st.columns([2,1])
            default_name = f"{parent_row.get('canonical_yiddish','')} ({settle})" if settle != "(unknown)" else parent_row.get("canonical_yiddish","")
            sub_names[settle] = col_a.text_input(
                f"Name for '{settle}'", value=default_name, key=f"subname_{did}_{i}"
            )
            col_b.caption(f"{g['mentions']}× · {', '.join(list(g['addresses'])[:2]) or '—'}")

        st.warning("This will mark the current entry as exploded and create sub-entries. This can be undone.")
        if st.button("✅ Confirm explode", key=f"explode_{did}", type="primary"):
            _do_explode(headers, rows, parent_row, parent_idx, candidates, sub_names)


def _render_refined_split(headers, rows, parent_row, parent_idx, source):
    """Per-mention assignment UI for refined splitting into reviewer-named groups."""
    cid = parent_row["db_id"]
    groups_key  = f"refined_groups_{cid}"
    assign_key  = f"refined_assign_{cid}"

    # Initialise group names in session state
    if groups_key not in st.session_state:
        st.session_state[groups_key] = [
            f"{parent_row.get('canonical_yiddish', 'Group')} 1",
            f"{parent_row.get('canonical_yiddish', 'Group')} 2",
        ]
    if assign_key not in st.session_state:
        st.session_state[assign_key] = {}

    group_names: list[str] = st.session_state[groups_key]

    # ── Group name editors ────────────────────────────────────────────────────
    st.markdown("**Groups:**")
    new_names = []
    for gi, gname in enumerate(group_names):
        new_names.append(
            st.text_input(f"Group {gi+1} name", value=gname, key=f"gname_{cid}_{gi}")
        )
    st.session_state[groups_key] = new_names

    if st.button("➕ Add group", key=f"addgrp_{cid}"):
        st.session_state[groups_key].append(f"Group {len(group_names)+1}")
        st.rerun()

    st.divider()

    # ── Per-mention assignment ────────────────────────────────────────────────
    st.markdown(f"**Assign {len(source)} mentions:**")
    options = new_names + ["— exclude —"]
    assignments: dict[str, str] = st.session_state[assign_key]

    for idx, src_row in enumerate(source):
        xid     = src_row.get(_COL_XMLID, "").strip() or str(idx)
        heading = src_row.get(_COL_HEADING, "").strip()
        settle  = src_row.get(_COL_SETTLE, "").strip()
        sent    = src_row.get(_COL_SENTENCE, "").strip()

        col_info, col_sel = st.columns([3, 1])
        with col_info:
            label_parts = []
            if heading: label_parts.append(f"*{heading}*")
            if settle:  label_parts.append(f"📍 {settle}")
            st.markdown("  ·  ".join(label_parts) or f"Mention {idx+1}")
            if sent:
                st.markdown(
                    f"<div dir='rtl' style='font-size:0.85em; color:#555; "
                    f"border-left:3px solid #ccc; padding-left:6px; margin:2px 0'>{sent}</div>",
                    unsafe_allow_html=True,
                )
        with col_sel:
            current_assign = assignments.get(xid, new_names[0] if new_names else options[0])
            # Ensure current assignment is still a valid option
            if current_assign not in options:
                current_assign = options[0]
            chosen = st.selectbox(
                "Assign to", options, index=options.index(current_assign),
                key=f"assign_{cid}_{idx}", label_visibility="collapsed"
            )
            assignments[xid] = chosen

    st.session_state[assign_key] = assignments

    st.divider()
    st.warning("This will replace the current cluster with the groups below. This can be undone.")

    assigned_counts = collections.Counter(v for v in assignments.values() if v != "— exclude —")
    for gname in new_names:
        st.caption(f"**{gname}**: {assigned_counts.get(gname, 0)} mentions")

    if st.button("✅ Confirm refined split", key=f"refined_confirm_{cid}", type="primary"):
        _do_refined_split(headers, rows, parent_row, parent_idx, source, assignments, new_names)


def _do_refined_split(headers, rows, parent_row, parent_idx, source, assignments, group_names):
    """Build sub-entry rows from reviewer-assigned mention groups."""
    cid = parent_row["db_id"]

    # Aggregate data per group
    group_data: dict[str, dict] = {
        gname: {"mentions": 0, "settlements": set(), "addresses": set(),
                "venues": set(), "countries": set()}
        for gname in group_names
    }

    for idx, src_row in enumerate(source):
        xid   = src_row.get(_COL_XMLID, "").strip() or str(idx)
        gname = assignments.get(xid, "— exclude —")
        if gname == "— exclude —" or gname not in group_data:
            continue
        g = group_data[gname]
        g["mentions"] += 1
        for col, key in (
            (_COL_SETTLE, "settlements"), (_COL_ADDR, "addresses"),
            (_COL_VENUE, "venues"),       (_COL_COUNTRY, "countries"),
        ):
            v = src_row.get(col, "").strip()
            if not _m(v): g[key].add(v)

    new_rows = []
    for i, gname in enumerate(group_names):
        g = group_data[gname]
        if g["mentions"] == 0:
            continue  # skip empty groups
        sub = _new_subcluster_row(
            headers, parent_row,
            " | ".join(sorted(g["settlements"])) or "(unknown)",
            g["addresses"], g["venues"], g["countries"],
            g["mentions"], i,
        )
        sub["canonical_yiddish"] = gname
        sub["n_settlements"] = str(len(g["settlements"]))
        new_rows.append(sub)

    rows[parent_idx]["is_exploded"] = "TRUE"
    rows[parent_idx]["is_generic"]  = ""

    insert_at = parent_idx + 1
    for sub in reversed(new_rows):
        rows.insert(insert_at, sub)

    save_orgs(headers, rows)
    load_orgs.clear()

    # Clear refined-split session state for this cluster
    st.session_state.pop(f"refined_groups_{cid}", None)
    st.session_state.pop(f"refined_assign_{cid}", None)

    st.success(f"Created {len(new_rows)} sub-clusters.")
    st.rerun()


def _do_explode(headers, rows, parent_row, parent_idx, candidates, sub_names):
    """Write sub-cluster rows and mark parent as exploded."""
    new_rows = []
    for i, (settle, g) in enumerate(candidates):
        sub = _new_subcluster_row(
            headers, parent_row, settle,
            g["addresses"], g["venues"], g["countries"], g["mentions"], i
        )
        sub["canonical_yiddish"] = sub_names.get(settle, sub["canonical_yiddish"])
        new_rows.append(sub)

    rows[parent_idx]["is_exploded"] = "TRUE"
    rows[parent_idx]["is_generic"]  = ""

    insert_at = parent_idx + 1
    for sub in reversed(new_rows):
        rows.insert(insert_at, sub)

    save_orgs(headers, rows)
    load_orgs.clear()
    st.success(f"Created {len(new_rows)} sub-clusters.")
    st.rerun()


def _do_unexplode(headers, rows, parent_cid, parent_idx):
    """Remove sub-entries and clear exploded flag on parent."""
    rows_out = [r for r in rows if r.get("parent_db_id") != parent_cid]
    for r in rows_out:
        if r.get("db_id") == parent_cid:
            r["is_exploded"] = ""
    save_orgs(headers, rows_out)
    load_orgs.clear()
    st.rerun()


# ── Map helper ────────────────────────────────────────────────────────────────

def _render_map(cid, label, lat_str, lon_str):
    """Render a folium map with a draggable pin. Returns (lat, lon) if pin was dragged, else None."""
    if not HAS_MAP:
        return None
    try:
        lat, lon = float(lat_str), float(lon_str)
        m = folium.Map(location=[lat, lon], zoom_start=15, tiles="OpenStreetMap")
        folium.Marker(
            [lat, lon], popup=label, draggable=True,
            icon=folium.Icon(color="blue", icon="star"),
        ).add_to(m)
        map_data = st_folium(
            m, width=None, height=300, key=f"map_{cid}",
            returned_objects=["last_object_clicked"],
        )
        if map_data and map_data.get("last_object_clicked"):
            clicked = map_data["last_object_clicked"]
            return round(clicked["lat"], 6), round(clicked["lng"], 6)
    except (ValueError, TypeError):
        pass
    return None
