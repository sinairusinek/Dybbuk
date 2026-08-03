"""Plays KG explorer — interactive local-neighborhood view.

Pick a starting play / person / org / venue / place and see its neighborhood
up to N hops out. Uses pyvis (vis.js) rendered inline via Streamlit's HTML
component. Solves the "whole-graph-too-big" problem by only ever rendering
one entity's local subgraph.
"""
from __future__ import annotations
import csv, html
from collections import defaultdict, deque
from functools import lru_cache
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

try:
    from pyvis.network import Network
    _HAS_PYVIS = True
except ImportError:
    _HAS_PYVIS = False

REPO_ROOT = Path(__file__).resolve().parents[3]
KG_DIR = REPO_ROOT / "Zylbercweig" / "plays" / "kg"

TYPE_COLOR = {
    "play":             "#e6194B",
    "person":           "#4363d8",
    "production_event": "#3cb44b",
    "org":              "#f58231",
    "place":            "#911eb4",
    "venue":            "#42d4f4",
    "edition":          "#808000",
}
TYPE_SHAPE = {
    "play":             "dot",
    "person":           "dot",
    "production_event": "triangle",
    "org":              "square",
    "place":            "diamond",
    "venue":            "star",
    "edition":          "box",
}


@lru_cache(maxsize=1)
def load_kg():
    with (KG_DIR / "nodes.tsv").open(encoding="utf-8") as f:
        nodes = list(csv.DictReader(f, delimiter="\t"))
    with (KG_DIR / "edges.tsv").open(encoding="utf-8") as f:
        edges = list(csv.DictReader(f, delimiter="\t"))
    by_id = {n["node_id"]: n for n in nodes}
    adj_out = defaultdict(list)
    adj_in  = defaultdict(list)
    for e in edges:
        adj_out[e["source_id"]].append(e)
        adj_in[e["target_id"]].append(e)
    return nodes, edges, by_id, adj_out, adj_in


def neighborhood(seed_id: str, adj_out, adj_in, hops: int, allowed_types: set[str] | None):
    """BFS up to `hops` from seed_id, restricted to `allowed_types` if given."""
    visited = {seed_id}
    edges = []
    frontier = deque([(seed_id, 0)])
    while frontier:
        nid, d = frontier.popleft()
        if d >= hops:
            continue
        for e in adj_out.get(nid, []) + adj_in.get(nid, []):
            nxt = e["target_id"] if e["source_id"] == nid else e["source_id"]
            edges.append(e)
            if nxt in visited:
                continue
            visited.add(nxt)
            frontier.append((nxt, d + 1))
    if allowed_types is not None:
        visited = {v for v in visited if v == seed_id or _type_of(v) in allowed_types}
        edges = [e for e in edges if e["source_id"] in visited and e["target_id"] in visited]
    # dedup edges by edge_id
    seen = set(); out_edges = []
    for e in edges:
        if e["edge_id"] in seen: continue
        seen.add(e["edge_id"]); out_edges.append(e)
    return visited, out_edges


_TYPE_CACHE = {}
def _type_of(nid: str) -> str:
    if not _TYPE_CACHE:
        _nodes, _edges, by_id, _, _ = load_kg()
        for k, v in by_id.items():
            _TYPE_CACHE[k] = v["node_type"]
    return _TYPE_CACHE.get(nid, "")


def build_pyvis(seed_id, nodes_by_id, node_ids, edges):
    net = Network(height="700px", width="100%", bgcolor="#111", font_color="#eee",
                  directed=True, cdn_resources="in_line", notebook=False)
    net.force_atlas_2based(gravity=-60, central_gravity=0.012,
                           spring_length=110, spring_strength=0.08,
                           damping=0.9, overlap=0.2)
    deg = defaultdict(int)
    for e in edges:
        deg[e["source_id"]] += 1
        deg[e["target_id"]] += 1

    for nid in node_ids:
        n = nodes_by_id.get(nid, {"node_type": "", "label_yiddish": nid, "label_english": ""})
        ntype = n.get("node_type","")
        yid = n.get("label_yiddish","")
        eng = n.get("label_english","")
        label = yid or eng or nid
        disp = label if len(label) <= 32 else label[:29] + "…"
        tip = f"<b>{html.escape(label)}</b><br>type: {ntype}<br>id: {nid}<br>degree in view: {deg[nid]}"
        if yid and eng: tip += f"<br>en: {html.escape(eng)}"
        if n.get("ext_ref_type"): tip += f"<br>ref: {n['ext_ref_type']}:{n.get('ext_ref_id','')}"
        color = TYPE_COLOR.get(ntype, "#999")
        # Highlight seed
        border = "#fff" if nid == seed_id else color
        border_width = 4 if nid == seed_id else 1
        size = max(14, min(45, 10 + deg[nid] ** 0.6 * 3))
        net.add_node(nid, label=disp, color={"background": color, "border": border},
                     shape=TYPE_SHAPE.get(ntype,"dot"), size=size, title=tip,
                     borderWidth=border_width, group=ntype)
    for e in edges:
        et = e.get("edge_type","")
        tip = f"<b>{html.escape(et)}</b>"
        if e.get("role_detail"): tip += f"<br>{html.escape(e['role_detail'])}"
        if e.get("character"):   tip += f"<br>character: {html.escape(e['character'])}"
        if e.get("date_start"):  tip += f"<br>{e['date_start']}–{e.get('date_end','')}"
        if e.get("evidence_sentence"):
            tip += f"<br><i>{html.escape(e['evidence_sentence'])[:180]}</i>"
        net.add_edge(e["source_id"], e["target_id"], title=tip, label=et, arrows="to",
                     color={"color":"#555","opacity":0.5}, width=0.8,
                     font={"color":"#888","size":9})
    net.set_options("""
    {
      "interaction": {"hover": true, "navigationButtons": true, "keyboard": true, "tooltipDelay": 100},
      "edges": {"smooth": false, "arrows": {"to": {"scaleFactor": 0.4}}},
      "physics": {
        "solver": "forceAtlas2Based",
        "stabilization": {"enabled": true, "iterations": 1500, "updateInterval": 50, "fit": true},
        "timestep": 0.35,
        "minVelocity": 0.75,
        "maxVelocity": 40,
        "adaptiveTimestep": true
      }
    }
    """)
    html_src = net.generate_html(notebook=False)
    # Freeze physics once stabilization completes, so the layout stops drifting
    freeze_js = """
<script>
window.addEventListener('load', () => {
  const iv = setInterval(() => {
    if (typeof network !== 'undefined') {
      network.once('stabilizationIterationsDone', () => {
        network.setOptions({physics: {enabled: false}});
      });
      clearInterval(iv);
    }
  }, 100);
});
</script>
"""
    return html_src.replace("</body>", freeze_js + "</body>", 1)


def render():
    st.header("📊 Plays KG · Neighborhood explorer")
    if not _HAS_PYVIS:
        st.error("`pyvis` not installed. Run: `python3.11 -m pip install pyvis`")
        return
    if not (KG_DIR / "nodes.tsv").exists():
        st.error(f"KG files not found at {KG_DIR}. Run `python3.11 Zylbercweig/plays/build_kg.py --execute` first.")
        return

    nodes, edges, by_id, adj_out, adj_in = load_kg()

    st.caption(f"Full KG: {len(nodes)} nodes / {len(edges)} edges · "
               f"types: play · person · production_event · org · place · venue · edition")

    # --- Controls ---
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        q = st.text_input(
            "Search a play / person / org / venue / place",
            value=st.session_state.get("plays_kg_q", ""),
            placeholder="e.g. Goldfaden, Bar Kokhba, וואַרשע, Lateiner",
        ).strip()
    with col2:
        hops = st.slider("Hops", 1, 3, 1)
    with col3:
        max_nodes = st.number_input("Max nodes", 20, 800, 200, step=20)

    all_types = sorted({n["node_type"] for n in nodes})
    types_sel = st.multiselect("Include types (neighborhood is filtered to these)",
                               all_types, default=all_types)

    if not q:
        st.info("Type a name in the box above. Matches are searched in Yiddish + English labels.")
        # Show quick-start suggestions: top-degree entities of each type
        deg = defaultdict(int)
        for e in edges:
            deg[e["source_id"]] += 1; deg[e["target_id"]] += 1
        st.markdown("**Quick-start (highest-connectivity entities per type):**")
        by_type = defaultdict(list)
        for n in nodes:
            by_type[n["node_type"]].append(n)
        for t in all_types:
            top = sorted(by_type[t], key=lambda x: -deg[x["node_id"]])[:5]
            btn_cols = st.columns(len(top) + 1)
            btn_cols[0].markdown(f"**{t}**")
            for i, n in enumerate(top, 1):
                lbl = n.get("label_yiddish") or n.get("label_english") or n["node_id"]
                short = lbl if len(lbl) <= 22 else lbl[:20] + "…"
                if btn_cols[i].button(f"{short}  ·{deg[n['node_id']]}", key=f"qs_{n['node_id']}"):
                    st.session_state["plays_kg_q"] = lbl
                    st.rerun()
        return

    # --- Search matching nodes ---
    ql = q.lower()
    matches = [n for n in nodes
               if ql in (n.get("label_yiddish","") + " " + n.get("label_english","") + " " + n["node_id"]).lower()]
    if not matches:
        st.warning(f"No node matched '{q}'.")
        return
    if len(matches) > 1:
        st.write(f"**{len(matches)} matches** — pick one:")
        opts = {f"{m['node_type']:>18s}  ·  {m.get('label_yiddish') or m.get('label_english') or m['node_id']}  ({m['node_id']})": m["node_id"]
                for m in matches[:40]}
        pick = st.radio("Match", list(opts.keys()), label_visibility="collapsed")
        seed = opts[pick]
    else:
        seed = matches[0]["node_id"]

    seed_node = by_id[seed]
    st.markdown(f"### Seed: **{seed_node.get('label_yiddish') or seed_node.get('label_english') or seed}** "
                f"·  _{seed_node['node_type']}_  ·  `{seed}`")

    # --- Compute neighborhood ---
    node_ids, sub_edges = neighborhood(seed, adj_out, adj_in, hops, set(types_sel))
    if len(node_ids) > max_nodes:
        # Truncate: keep seed + highest-degree neighbors
        deg = defaultdict(int)
        for e in sub_edges:
            deg[e["source_id"]] += 1; deg[e["target_id"]] += 1
        keep = {seed} | set(sorted(node_ids - {seed}, key=lambda x: -deg[x])[:max_nodes-1])
        node_ids = keep
        sub_edges = [e for e in sub_edges if e["source_id"] in keep and e["target_id"] in keep]
        st.caption(f"⚠️ Neighborhood had more than {max_nodes} nodes — truncated to top-{max_nodes} by degree.")

    # --- Neighborhood stats ---
    from collections import Counter
    type_ct = Counter(_type_of(nid) for nid in node_ids)
    edge_ct = Counter(e["edge_type"] for e in sub_edges)
    stats_col1, stats_col2 = st.columns(2)
    with stats_col1:
        st.markdown("**Nodes by type**")
        for t, c in type_ct.most_common():
            st.markdown(f"<span style='color:{TYPE_COLOR.get(t,'#999')}'>●</span> {t}: **{c}**", unsafe_allow_html=True)
    with stats_col2:
        st.markdown("**Edges by relation**")
        for et, c in edge_ct.most_common(10):
            st.markdown(f"- `{et}`: {c}")

    # --- Render ---
    html_src = build_pyvis(seed, by_id, node_ids, sub_edges)
    components.html(html_src, height=720, scrolling=False)

    with st.expander(f"Raw edges in this view ({len(sub_edges)})"):
        if not sub_edges:
            st.caption("No edges in this neighborhood.")
        else:
            import pandas as pd
            wanted = ["source_id","edge_type","target_id","role_detail","character","date_start","evidence_sentence"]
            df = pd.DataFrame(sub_edges)
            cols = [c for c in wanted if c in df.columns]
            st.dataframe(df[cols], use_container_width=True, height=300)
