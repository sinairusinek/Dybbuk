"""
Zalmen — Zylbercweig review app.

Covers all human review for the Zylbercweig Lexicon pipeline:
  A1  Org → DB align    — align to existing DB / add new / generic / uncluster
  A2  Org clusters      — confirm/reject fuzzy cluster merges
  A2  Org addresses     — confirm addresses/geocoding
  B1  Person dedup      — resolve persons across volumes (not yet built)
  B2  Person → external — Wikidata/YIVO alignment (not yet built)

Run:
  streamlit run app.py                   # full app (all views)
  streamlit run app.py -- --view a2      # jump directly to A2 (for RA)
  streamlit run app.py -- --view b1      # jump directly to B1 (for RA)
"""

import sys
import streamlit as st

# ── Reviewer list ─────────────────────────────────────────────────────────────
try:
    _REVIEWERS: list[str] = list(st.secrets["reviewers"])
except Exception:
    _REVIEWERS = ["Sinai", "Maaty", "Bella", "Noa", "Judith", "Ruthie"]

# ── Optional --view argument: pin the app to one view for a specific RA ───────
# Usage: streamlit run app.py -- --view a2
_VIEW_ARG: str | None = None
_args = sys.argv[1:]
if "--view" in _args:
    idx = _args.index("--view")
    if idx + 1 < len(_args):
        _VIEW_ARG = _args[idx + 1].lower()

# ── View registry ─────────────────────────────────────────────────────────────
VIEWS = {
    "Entity Review":           ("org_review",    "review"),
    "Entity Cards":            ("org_addresses", "geo"),
    "B1 · Person Dedup":       (None,             "b1"),
    "B2 · Person → External":  (None,             "b2"),
}

VIEW_STATUS = {
    "Entity Review":           "✅ Ready",
    "Entity Cards":            "✅ Ready",
    "B1 · Person Dedup":       "✅ Ready",
    "B2 · Person → External":  "⏳ Blocked on B1",
}

# ── Page config ───────────────────────────────────────────────────────────────
# If pinned to a single view, use a more specific title
_pinned_label = next(
    (label for label, (_, key) in VIEWS.items() if key == _VIEW_ARG), None
)
_page_title = f"Zalmen · {_pinned_label}" if _pinned_label else "Zalmen"

st.set_page_config(
    page_title=_page_title,
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="collapsed" if _pinned_label else "expanded",
)

# ── Login gate ────────────────────────────────────────────────────────────────
if "reviewer" not in st.session_state:
    st.title("Zalmen · Who are you?")
    choice = st.selectbox("Select your name to continue:", ["— pick one —"] + _REVIEWERS)
    if st.button("Continue", type="primary", disabled=(choice == "— pick one —")):
        st.session_state["reviewer"] = choice
        st.rerun()
    st.stop()

# ── Query-param deep links (consumed before sidebar renders) ─────────────────
_qp = st.query_params
_qp_view = _qp.get("view", None)
_qp_entity = _qp.get("entity", None)
if _qp_view and _qp_view in VIEWS:
    st.session_state["main_view"] = _qp_view
    if _qp_entity:
        if _qp_view == "Entity Review":
            st.session_state["review_selected_cid"] = _qp_entity
        elif _qp_view == "Entity Cards":
            st.session_state["addr_selected"] = _qp_entity
    st.query_params.clear()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Zalmen")
    st.caption("Zylbercweig Lexicon review")
    st.divider()

    if _pinned_label:
        # Pinned mode: show which view this instance is for, no navigation
        st.markdown(f"**This instance:** {_pinned_label}")
        st.caption("Pinned view — navigation disabled.")
        selected = _pinned_label
    else:
        nav_target = st.session_state.pop("nav_view_target", None)
        if nav_target in VIEWS:
            st.session_state["main_view"] = nav_target
        elif "main_view" not in st.session_state:
            st.session_state["main_view"] = list(VIEWS.keys())[0]

        selected = st.radio(
            "View",
            list(VIEWS.keys()),
            key="main_view",
            format_func=lambda v: f"{v}  {VIEW_STATUS[v]}",
        )

    st.divider()
    st.caption(f"Logged in as **{st.session_state['reviewer']}**")
    if st.button("Switch user", use_container_width=True):
        del st.session_state["reviewer"]
        st.rerun()
    st.divider()
    st.caption("Zylbercweig Lexicon — Yiddish biographical encyclopedia")
    if not _pinned_label:
        st.caption(
            "**Parallel RA deployment:**\n"
            "```\nstreamlit run app.py -- --view a2\nstreamlit run app.py -- --view b1\n```"
        )

# ── Route to view ─────────────────────────────────────────────────────────────
view_module, _ = VIEWS[selected]

if view_module == "org_review":
    from views.org_review import render
    render()
elif view_module == "org_addresses":
    from views.org_addresses import render
    render()
else:
    st.info(f"**{selected}** is not yet implemented.")
    st.write("Work order: A1 → A2 (clusters/addresses) · B1 → B2")
