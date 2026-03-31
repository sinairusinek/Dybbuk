"""
A2-Addresses · Org Address Review view.

Features
--------
1. Paginated table (50 rows/page) — no per-row expanders.
2. Detail panel for selected org:
     a. Extracted settlement/address/venue chips — click to see sample texts.
     b. "Explode by location" — splits a cluster into per-settlement sub-clusters.
     c. "Split by location" — same for confirmed non-generic orgs with >1 settlement.
     d. Generic flag — marks a name as a label, not one entity.
     e. Confirm location + optional geocoding.
3. Exploded sub-clusters appear in the table beneath their parent.

Files
-----
Reads:   ../organizations/org_addresses_review.tsv
         ../organizations/organizations_clustered.tsv  (sample text lookup)
Writes:  ../organizations/org_addresses_review.tsv  (in-place)
"""

import csv, fcntl, pathlib, sys, time, collections
import streamlit as st

csv.field_size_limit(sys.maxsize)

ADDR_FILE     = pathlib.Path(__file__).parents[2] / "organizations" / "org_addresses_review.tsv"
CLUSTER_FILE  = pathlib.Path(__file__).parents[2] / "organizations" / "organizations_clustered.tsv"
PAGE_SIZE     = 50

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


# ── I/O ───────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_orgs(mtime: float):
    with open(ADDR_FILE, newline="", encoding="utf-8") as f:
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

def get_mtime(path=ADDR_FILE): return path.stat().st_mtime if path.exists() else 0.0

# ── Geocoding ─────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=3600)
def geocode(query: str):
    if not HAS_GEOCODE or not query.strip(): return None
    try:
        time.sleep(1)
        loc = _geocoder.geocode(query, timeout=10)
        if loc: return round(loc.latitude, 6), round(loc.longitude, 6)
    except GeocoderTimedOut: pass
    return None

# ── Helpers ───────────────────────────────────────────────────────────────────

def _status(row):
    if row.get("is_exploded") == "TRUE":   return "💥 exploded"
    if row.get("is_generic")  == "TRUE":   return "🔶 generic"
    if row.get("lat") and row.get("lon"):  return "📍 geocoded"
    if row.get("confirmed_settlement") or row.get("confirmed_address"): return "✏️ confirmed"
    return ""

def _new_subcluster_row(headers, parent, settlement, addresses, venues, countries, mentions, idx):
    """Build one sub-cluster row for the explode/split operation."""
    empty = {h: "" for h in headers}
    empty.update({
        "cluster_id":           f"{parent['cluster_id']}_X{idx:02d}",
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
        "parent_cluster_id":    parent["cluster_id"],
        "confirmed_settlement": settlement,
    })
    return empty

def _build_explode_candidates(parent_cid, all_source_rows):
    """
    From organizations_clustered.tsv rows for this cluster, group by settlement
    and return a list of dicts ready for _new_subcluster_row.
    Returns list of (settlement, mentions, addresses, venues, countries).
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

# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.header("A2 · Org Address Review")

    if not ADDR_FILE.exists():
        st.error(f"`{ADDR_FILE}` not found — run `extract_addresses.py` first.")
        return
    if not CLUSTER_FILE.exists():
        st.error(f"`{CLUSTER_FILE}` not found — run `cluster_orgs.py` first.")
        return

    mtime_addr    = get_mtime(ADDR_FILE)
    mtime_cluster = get_mtime(CLUSTER_FILE)
    headers, rows = load_orgs(mtime_addr)
    samples       = load_samples(mtime_cluster)

    # pre-build cluster source rows index (from clustered TSV, via samples keys)
    # We need per-settlement mention counts for explode — load lazily per cluster
    # (done inside _render_detail to avoid loading 16k rows upfront in cache)

    # ── Filters ───────────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([2, 2, 2])
    with fc1:
        all_types = sorted({r["org_type"] for r in rows if r["org_type"].strip()})
        default_t = [t for t in all_types if "theatre" in t.lower() or "טעאַטער" in t]
        sel_types = st.multiselect("Org type", all_types, default=default_t)
    with fc2:
        status_f = st.segmented_control(
            "Status", ["All", "Unreviewed", "Confirmed", "Geocoded", "Generic", "Exploded"],
            default="All",
        )
    with fc3:
        sort_m = st.selectbox("Sort", ["Mentions ↓", "Settlements ↓", "Alphabetical"])

    show_sub = st.checkbox("Show sub-clusters (from exploded parents)", value=False)

    visible = rows
    if sel_types:
        visible = [r for r in visible if r["org_type"] in sel_types]
    if not show_sub:
        visible = [r for r in visible if not r.get("parent_cluster_id")]

    STATUS_MAP = {
        "Unreviewed": lambda r: not _status(r),
        "Confirmed":  lambda r: _status(r) in ("✏️ confirmed", "📍 geocoded"),
        "Geocoded":   lambda r: bool(r.get("lat") and r.get("lon")),
        "Generic":    lambda r: r.get("is_generic") == "TRUE",
        "Exploded":   lambda r: r.get("is_exploded") == "TRUE",
    }
    if status_f in STATUS_MAP:
        visible = [r for r in visible if STATUS_MAP[status_f](r)]

    if sort_m == "Settlements ↓":
        visible = sorted(visible, key=lambda r: -int(r.get("n_settlements", 0) or 0))
    elif sort_m == "Alphabetical":
        visible = sorted(visible, key=lambda r: r.get("canonical_yiddish", ""))
    else:
        visible = sorted(visible, key=lambda r: -int(r.get("mentions", 0) or 0))

    # ── Metrics ───────────────────────────────────────────────────────────────
    m1,m2,m3,m4,m5 = st.columns(5)
    m1.metric("Showing", len(visible))
    m2.metric("🔶 Generic",  sum(1 for r in visible if r.get("is_generic")  == "TRUE"))
    m3.metric("💥 Exploded", sum(1 for r in visible if r.get("is_exploded") == "TRUE"))
    m4.metric("📍 Geocoded", sum(1 for r in visible if r.get("lat") and r.get("lon")))
    reviewed = sum(1 for r in visible if _status(r) and _status(r) != "💥 exploded")
    if visible: m5.progress(reviewed / len(visible), text=f"{reviewed}/{len(visible)}")

    st.divider()
    if not visible:
        st.info("No organizations match the current filter.")
        return

    # ── Pagination ────────────────────────────────────────────────────────────
    total_pages = max(1, (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE)
    fhash = f"{sel_types}|{status_f}|{sort_m}|{show_sub}"
    if st.session_state.get("addr_filter_hash") != fhash:
        st.session_state.addr_page = 0
        st.session_state.addr_filter_hash = fhash
        st.session_state.addr_selected = None

    page = st.session_state.get("addr_page", 0)
    page_rows = visible[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    pc1, pc2, pc3 = st.columns([1, 4, 1])
    with pc1:
        if st.button("◀", disabled=page==0, key="addr_prev"):
            st.session_state.addr_page -= 1
            st.session_state.addr_selected = None
            st.rerun()
    with pc2:
        st.caption(f"Page {page+1}/{total_pages}  ({len(visible)} orgs)")
    with pc3:
        if st.button("▶", disabled=page>=total_pages-1, key="addr_next"):
            st.session_state.addr_page += 1
            st.session_state.addr_selected = None
            st.rerun()

    # ── Table ─────────────────────────────────────────────────────────────────
    sel_cid = st.session_state.get("addr_selected")
    for row in page_rows:
        cid    = row["cluster_id"]
        status = _status(row)
        indent = "　" if row.get("parent_cluster_id") else ""  # indent sub-clusters
        label  = (
            f"{indent}{status+'  ' if status else ''}"
            f"**{row.get('canonical_yiddish','') or cid}**  "
            f"`{row.get('org_type','')}` "
            f"{row.get('mentions','?')}×  "
            f"{row.get('n_settlements','?')} cities"
        )
        if st.button(label, key=f"sel_{cid}", use_container_width=True,
                     type="primary" if sel_cid==cid else "secondary"):
            st.session_state.addr_selected = None if sel_cid==cid else cid
            st.rerun()

    # ── Detail panel ──────────────────────────────────────────────────────────
    if st.session_state.get("addr_selected"):
        sel_row = next((r for r in rows if r["cluster_id"]==st.session_state.addr_selected), None)
        if sel_row:
            st.divider()
            _render_detail(headers, rows, sel_row, samples)


# ── Detail panel ─────────────────────────────────────────────────────────────

def _render_detail(headers, rows, row, samples):
    cid      = row["cluster_id"]
    row_idx  = next((i for i,r in enumerate(rows) if r["cluster_id"]==cid), None)
    canonical = row.get("canonical_yiddish","") or cid

    st.subheader(canonical)
    st.caption(
        f"{row.get('org_type','')} · {row.get('mentions','?')} mentions · "
        f"{row.get('n_settlements','?')} distinct settlements"
        + (f" · sub-cluster of `{row['parent_cluster_id']}`" if row.get("parent_cluster_id") else "")
    )

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

    # ── Extracted location chips with sample-text toggle ─────────────────────
    settlements = _split(row.get("extracted_settlements",""))
    addresses   = _split(row.get("extracted_addresses",""))
    venues      = _split(row.get("extracted_venues",""))
    countries   = _split(row.get("extracted_countries",""))

    cluster_samples = samples.get(cid, {})

    if settlements or addresses or venues:
        st.markdown("**Extracted location data** — click a settlement to see sample texts:")

        # Settlement chips
        if settlements:
            chip_cols = st.columns(min(len(settlements), 6))
            for i, s in enumerate(settlements[:18]):  # cap display at 18
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

        # Active chip → sample texts
        active_s = st.session_state.get(f"chip_active_{cid}")
        if active_s:
            _render_samples(active_s, cluster_samples.get(active_s, []),
                            cluster_samples.get("", []))  # "" = no settlement recorded

        # Addresses / venues summary
        if addresses:
            st.markdown("*Addresses:* " + "  ·  ".join(addresses[:8]))
        if venues and set(venues) - set(addresses):
            st.markdown("*Venues:* " + "  ·  ".join(v for v in venues[:6] if v not in addresses))

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
        # Center-aligned labels + RTL-friendly inputs with a subtle tint.
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

        gc1, gc2 = st.columns([3,1])
        new_roman = gc1.text_input("Romanized address for geocoding",
                                    value=row.get("confirmed_address_romanized",""),
                                    key=f"roman_{cid}", placeholder="e.g. Nalewki 3, Warsaw")

        # Seed lat/lon from saved row, then allow session_state to override
        # (session_state holds freshly geocoded values that haven't been saved yet).
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
            _render_map(cid, canonical, new_lat, new_lon)
            lc3, lc4 = st.columns(2)
            new_lat = lc3.text_input("Lat", value=new_lat, key=f"lat_{cid}")
            new_lon = lc4.text_input("Lon", value=new_lon, key=f"lon_{cid}")
    else:
        new_settle = new_addr = new_roman = new_lat = new_lon = ""

    new_note = st.text_input("Notes", value=row.get("reviewer_notes",""),
                              key=f"note_{cid}", placeholder="Notes (optional)")

    # ── Save ──────────────────────────────────────────────────────────────────
    if st.button("💾 Save", key=f"save_{cid}", type="primary") and row_idx is not None:
        rows[row_idx]["is_generic"]                  = "TRUE" if new_generic else ""
        rows[row_idx]["confirmed_settlement"]         = "" if new_generic else new_settle
        rows[row_idx]["confirmed_address"]            = "" if new_generic else new_addr
        rows[row_idx]["confirmed_address_romanized"]  = "" if new_generic else new_roman
        rows[row_idx]["lat"]                          = "" if new_generic else new_lat
        rows[row_idx]["lon"]                          = "" if new_generic else new_lon
        rows[row_idx]["reviewer_notes"]               = new_note
        save_orgs(headers, rows)
        load_orgs.clear()
        # Clear pending geocode values now that they're saved.
        st.session_state.pop(f"_gc_lat_{cid}", None)
        st.session_state.pop(f"_gc_lon_{cid}", None)
        st.rerun()


# ── Sample text panel ─────────────────────────────────────────────────────────

def _render_samples(settlement: str, samples: list, no_settle_samples: list):
    """Show up to 3 sample sentences for a given settlement."""
    all_samples = samples or no_settle_samples
    if not all_samples:
        st.caption(f"No sample texts found for '{settlement}'.")
        return
    st.markdown(f"**Sample texts for `{settlement}`:**")
    for heading, sent, fle, xid in all_samples[:3]:
        parts = []
        if heading:
            parts.append(f"*{heading}*")
        if sent:
            parts.append(f"<div dir='rtl' style='font-size:0.9em; border-left:3px solid #93c5fd; padding-left:8px; margin:4px 0'>{sent}</div>")
        if parts:
            st.markdown("\n".join(parts), unsafe_allow_html=True)


# ── Explode panel ─────────────────────────────────────────────────────────────

def _render_explode_panel(headers, rows, parent_row, parent_idx, settlements, countries, is_generic):
    """Preview and confirm explode/split into per-settlement sub-clusters."""
    cid = parent_row["cluster_id"]

    # Load source rows for this cluster from clustered TSV to get per-settlement counts
    @st.cache_data(show_spinner=False)
    def _source_rows(cluster_id, mtime):
        result = []
        with open(CLUSTER_FILE, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                if r.get(_COL_CID,"").strip() == cluster_id:
                    result.append(r)
        return result

    source = _source_rows(cid, get_mtime(CLUSTER_FILE))
    candidates = _build_explode_candidates(cid, source)

    st.markdown(f"**{len(candidates)} proposed sub-clusters** — review and confirm names:")
    st.caption("Each will become an independent entry. Sub-clusters with unknown location are grouped as '(unknown)'.")

    # Editable name per candidate
    sub_names = {}
    for i, (settle, g) in enumerate(candidates):
        col_a, col_b = st.columns([2,1])
        default_name = f"{parent_row.get('canonical_yiddish','')} ({settle})" if settle != "(unknown)" else parent_row.get("canonical_yiddish","")
        sub_names[settle] = col_a.text_input(
            f"Name for '{settle}'", value=default_name, key=f"subname_{cid}_{i}"
        )
        col_b.caption(f"{g['mentions']}× · {', '.join(list(g['addresses'])[:2]) or '—'}")

    st.warning("This will mark the current cluster as exploded and create sub-clusters. This can be undone.")
    if st.button("✅ Confirm explode", key=f"explode_{cid}", type="primary"):
        _do_explode(headers, rows, parent_row, parent_idx, candidates, sub_names)


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

    # Insert sub-clusters right after parent
    insert_at = parent_idx + 1
    for sub in reversed(new_rows):
        rows.insert(insert_at, sub)

    save_orgs(headers, rows)
    load_orgs.clear()
    st.success(f"Created {len(new_rows)} sub-clusters.")
    st.rerun()


def _do_unexplode(headers, rows, parent_cid, parent_idx):
    """Remove sub-clusters and clear exploded flag on parent."""
    rows_out = [r for r in rows if r.get("parent_cluster_id") != parent_cid]
    for r in rows_out:
        if r["cluster_id"] == parent_cid:
            r["is_exploded"] = ""
    save_orgs(headers, rows_out)
    load_orgs.clear()
    st.rerun()


# ── Map helper ────────────────────────────────────────────────────────────────

def _render_map(cid, label, lat_str, lon_str):
    if not HAS_MAP: return
    try:
        lat, lon = float(lat_str), float(lon_str)
        m = folium.Map(location=[lat,lon], zoom_start=15, tiles="CartoDB positron")
        folium.Marker([lat,lon], popup=label,
                      icon=folium.Icon(color="blue", icon="star")).add_to(m)
        st_folium(m, width=None, height=240, key=f"map_{cid}", returned_objects=[])
    except (ValueError, TypeError):
        pass
