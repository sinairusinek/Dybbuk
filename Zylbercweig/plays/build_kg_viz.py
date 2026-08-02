"""Static HTML visualization of the plays knowledge graph.

Outputs Zylbercweig/plays/kg/graph.html — a self-contained interactive page
(pyvis / vis.js) with node-type filtering, degree-based sizing, and hover info.

The full graph is too large to render all at once (2.5k nodes / 4.9k edges),
so by default we show the top-N nodes by degree, mixed across types. Pass
--all to render everything (slow in browser).

Usage:
    python3.11 build_kg_viz.py             # top-500 by degree (default)
    python3.11 build_kg_viz.py --top 1000  # top-1000
    python3.11 build_kg_viz.py --all       # everything
    python3.11 build_kg_viz.py --types play,person,production_event
"""
from __future__ import annotations
import argparse, csv, html
from collections import defaultdict
from pathlib import Path
from pyvis.network import Network

HERE = Path(__file__).parent
KG = HERE / "kg"
NODES_TSV = KG / "nodes.tsv"
EDGES_TSV = KG / "edges.tsv"
OUT_HTML = KG / "graph.html"

# Distinct colors per node type
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

def load_nodes():
    with NODES_TSV.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def load_edges():
    with EDGES_TSV.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=500,
                    help="Show top-N nodes by degree (default 500). Ignored with --all.")
    ap.add_argument("--all", action="store_true", help="Show every node (slow).")
    ap.add_argument("--types", default="",
                    help="Comma-separated node types to include (default: all).")
    ap.add_argument("--out", default=str(OUT_HTML), help="Output HTML path.")
    args = ap.parse_args()

    all_nodes = load_nodes()
    all_edges = load_edges()
    print(f"loaded {len(all_nodes)} nodes, {len(all_edges)} edges")

    type_filter = {t.strip() for t in args.types.split(",") if t.strip()}
    if type_filter:
        all_nodes = [n for n in all_nodes if n["node_type"] in type_filter]

    node_ids = {n["node_id"] for n in all_nodes}
    edges = [e for e in all_edges
             if e["source_id"] in node_ids and e["target_id"] in node_ids]

    # Compute degree
    deg = defaultdict(int)
    for e in edges:
        deg[e["source_id"]] += 1
        deg[e["target_id"]] += 1

    # Select subset if not --all
    if not args.all and len(all_nodes) > args.top:
        chosen_ids = {n["node_id"] for n in
                      sorted(all_nodes, key=lambda x: -deg[x["node_id"]])[:args.top]}
        all_nodes = [n for n in all_nodes if n["node_id"] in chosen_ids]
        edges = [e for e in edges
                 if e["source_id"] in chosen_ids and e["target_id"] in chosen_ids]

    print(f"rendering {len(all_nodes)} nodes, {len(edges)} edges")

    net = Network(height="90vh", width="100%", bgcolor="#111", font_color="#eee",
                  directed=True, cdn_resources="in_line")
    net.barnes_hut(gravity=-4000, central_gravity=0.15, spring_length=120,
                   spring_strength=0.01, damping=0.35, overlap=0)

    for n in all_nodes:
        nid = n["node_id"]
        ntype = n["node_type"]
        yid = n.get("label_yiddish","")
        eng = n.get("label_english","")
        label = yid or eng or nid
        # Truncate for canvas
        disp = label if len(label) <= 32 else label[:29] + "…"
        # Rich tooltip (HTML)
        tip_parts = [f"<b>{html.escape(label)}</b>",
                     f"type: {ntype}", f"id: {nid}",
                     f"degree: {deg[nid]}"]
        if yid and eng: tip_parts.append(f"en: {html.escape(eng)}")
        if n.get("ext_ref_type"): tip_parts.append(f"ref: {n['ext_ref_type']}:{n.get('ext_ref_id','')}")
        if n.get("notes"): tip_parts.append(html.escape(n["notes"])[:200])
        size = max(8, min(40, 6 + deg[nid] ** 0.6 * 2))
        net.add_node(nid, label=disp,
                     color=TYPE_COLOR.get(ntype, "#999"),
                     shape=TYPE_SHAPE.get(ntype, "dot"),
                     size=size, title="<br>".join(tip_parts),
                     group=ntype)

    for e in edges:
        et = e.get("edge_type","")
        tip = f"<b>{html.escape(et)}</b>"
        if e.get("role_detail"): tip += f"<br>{html.escape(e['role_detail'])}"
        if e.get("character"):   tip += f"<br>character: {html.escape(e['character'])}"
        if e.get("date_start"):  tip += f"<br>{e['date_start']}–{e.get('date_end','')}"
        if e.get("evidence_sentence"):
            tip += f"<br><i>{html.escape(e['evidence_sentence'])[:180]}</i>"
        net.add_edge(e["source_id"], e["target_id"],
                     title=tip, label=et, arrows="to",
                     color={"color":"#555","opacity":0.5}, width=0.8,
                     font={"color":"#888","size":8})

    net.set_options("""
    {
      "interaction": {"hover": true, "navigationButtons": true, "keyboard": true, "tooltipDelay": 100},
      "edges": {"smooth": {"enabled": true, "type": "continuous"}, "arrows": {"to": {"scaleFactor": 0.4}}},
      "physics": {"stabilization": {"iterations": 200}}
    }
    """)

    out = Path(args.out)
    net.write_html(str(out), notebook=False, open_browser=False)

    # Inject a small header + legend + filter buttons into the produced HTML
    txt = out.read_text(encoding="utf-8")
    legend_items = "".join(
        f'<span class="chip" data-type="{t}" style="background:{TYPE_COLOR[t]}">'
        f'<input type="checkbox" checked onchange="toggleType(\'{t}\',this.checked)"> {t}'
        f'</span>' for t in TYPE_COLOR
    )
    header = f"""
<style>
  body {{ margin:0; font-family: -apple-system, sans-serif; background:#111; color:#eee; }}
  #hdr {{ position:fixed; top:0; left:0; right:0; padding:8px 14px;
          background:rgba(20,20,20,0.92); border-bottom:1px solid #333; z-index:1000;
          display:flex; align-items:center; gap:12px; flex-wrap:wrap; }}
  #hdr h1 {{ margin:0; font-size:15px; font-weight:600; }}
  #hdr .meta {{ color:#888; font-size:12px; }}
  .chip {{ display:inline-flex; align-items:center; gap:4px; padding:2px 8px;
           border-radius:12px; font-size:11px; color:#fff; opacity:0.9; }}
  .chip input {{ margin:0; }}
  #mynetwork {{ margin-top:44px; }}
</style>
<div id="hdr">
  <h1>Plays Knowledge Graph</h1>
  <span class="meta">{len(all_nodes)} nodes · {len(edges)} edges · rendering {'all' if args.all else f'top {args.top} by degree'}</span>
  {legend_items}
</div>
<script>
  function toggleType(t, on) {{
    if (!window.network || !window.nodes) return;
    const upd = [];
    window.nodes.get().forEach(n => {{
      if (n.group === t) upd.push({{id: n.id, hidden: !on}});
    }});
    window.nodes.update(upd);
  }}
  // expose nodes/network globally (pyvis names them locally)
  window.addEventListener('load', () => {{
    setTimeout(() => {{
      if (typeof nodes !== 'undefined') window.nodes = nodes;
      if (typeof network !== 'undefined') window.network = network;
    }}, 500);
  }});
</script>
"""
    txt = txt.replace("<body>", "<body>\n" + header, 1)
    out.write_text(txt, encoding="utf-8")

    print(f"wrote {out}  ({out.stat().st_size // 1024} KB)")
    print(f"open with: open {out}")

if __name__ == "__main__":
    main()
