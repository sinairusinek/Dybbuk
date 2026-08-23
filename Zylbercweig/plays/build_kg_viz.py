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
import argparse, csv, html, json, math
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
    ap.add_argument("--layers", default="",
                    help="Comma-separated source_layer values to include "
                         "(plays, bio; default: all).")
    args = ap.parse_args()

    all_nodes = load_nodes()
    all_edges = load_edges()
    print(f"loaded {len(all_nodes)} nodes, {len(all_edges)} edges")

    type_filter = {t.strip() for t in args.types.split(",") if t.strip()}
    if type_filter:
        all_nodes = [n for n in all_nodes if n["node_type"] in type_filter]

    layer_filter = {t.strip() for t in args.layers.split(",") if t.strip()}
    if layer_filter:
        all_edges = [e for e in all_edges
                     if (e.get("source_layer") or "plays") in layer_filter]
        touched = {e["source_id"] for e in all_edges} | {e["target_id"] for e in all_edges}
        all_nodes = [n for n in all_nodes if n["node_id"] in touched]

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
    net.force_atlas_2based(gravity=-60, central_gravity=0.012,
                           spring_length=110, spring_strength=0.08,
                           damping=0.9, overlap=0.2)

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
        ecol = "#7a5c2e" if (e.get("source_layer") or "plays") == "bio" else "#555"
        net.add_edge(e["source_id"], e["target_id"],
                     title=tip, label=et, arrows="to",
                     color={"color":ecol,"opacity":0.5}, width=0.8,
                     font={"color":"#888","size":8})

    # vis.js colors grouped nodes from its OWN default palette and drops the
    # per-node color when a group is set — so the group styling must be declared
    # here from TYPE_COLOR/TYPE_SHAPE, or the canvas won't match the legend chips.
    groups_opt = {t: {"color": {"background": TYPE_COLOR[t], "border": TYPE_COLOR[t]},
                      "shape": TYPE_SHAPE[t]} for t in TYPE_COLOR}
    net.set_options("""
    {
      "groups": %s,
      "interaction": {"hover": true, "navigationButtons": true, "keyboard": true, "tooltipDelay": 100},
      "edges": {"smooth": false, "arrows": {"to": {"scaleFactor": 0.4}}},
      "physics": {""" % json.dumps(groups_opt) + """
        "solver": "forceAtlas2Based",
        "stabilization": {"enabled": true, "iterations": 1500, "updateInterval": 50, "fit": true},
        "timestep": 0.35,
        "minVelocity": 0.75,
        "maxVelocity": 40,
        "adaptiveTimestep": true
      }
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
    # --- per-node year range (from dated edges) for the timeline slider ---
    def _yr(s):
        s = (s or "").strip()
        return int(s[:4]) if len(s) >= 4 and s[:4].isdigit() else None
    node_years = {}
    for e in edges:
        y = _yr(e.get("date_start"))
        if y is None:
            continue
        for k in (e["source_id"], e["target_id"]):
            lo, hi = node_years.get(k, (y, y))
            node_years[k] = (min(lo, y), max(hi, y))
    yrs = [y for pair in node_years.values() for y in pair]
    YMIN, YMAX = (min(yrs), max(yrs)) if yrs else (1870, 1950)
    node_years_js = json.dumps({k: [v[0], v[1]] for k, v in node_years.items()})

    # --- projected lat/lon for place nodes -> geonetwork anchors ---
    pts = []
    for n in all_nodes:
        if n["node_type"] != "place":
            continue
        try:
            sec = json.loads(n["secondary_ids"]) if n.get("secondary_ids") else {}
        except Exception:
            sec = {}
        lat, lon = sec.get("lat"), sec.get("lon")
        if lat and lon:
            try:
                pts.append((n["node_id"], float(lat), float(lon)))
            except ValueError:
                pass
    place_geo = {}
    if pts:
        mlat = sum(p[1] for p in pts) / len(pts)
        mlon = sum(p[2] for p in pts) / len(pts)
        K = 150.0                       # px per degree of latitude
        cosl = math.cos(math.radians(mlat)) or 1.0
        for nid, lat, lon in pts:
            place_geo[nid] = [round((lon - mlon) * cosl * K, 1),
                              round(-(lat - mlat) * K, 1)]
    place_geo_js = json.dumps(place_geo)

    header = f"""
<style>
  body {{ margin:0; font-family: -apple-system, sans-serif; background:#111; color:#eee; }}
  #hdr {{ position:fixed; top:0; left:0; right:0; padding:8px 14px;
          background:rgba(20,20,20,0.92); border-bottom:1px solid #333; z-index:1000;
          display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
  #hdr h1 {{ margin:0; font-size:15px; font-weight:600; }}
  #hdr .meta {{ color:#888; font-size:12px; }}
  #hdr button {{ background:#333; color:#eee; border:1px solid #555; border-radius:4px;
                 padding:2px 8px; font-size:11px; cursor:pointer; }}
  #hdr button.on {{ background:#2b6cb0; border-color:#4a90d9; }}
  .chip {{ display:inline-flex; align-items:center; gap:4px; padding:2px 8px;
           border-radius:12px; font-size:11px; color:#fff; opacity:0.9; }}
  .chip input {{ margin:0; }}
  #tl {{ display:inline-flex; align-items:center; gap:6px; font-size:11px; color:#ddd;
         background:#1c1c1c; border:1px solid #333; border-radius:6px; padding:3px 8px; }}
  #tl input[type=range] {{ width:120px; accent-color:#4a90d9; }}
  #tl b {{ color:#8fd0ff; min-width:78px; text-align:center; }}
  #tl label {{ color:#999; }}
  #mynetwork {{ margin-top:44px; }}
</style>
<div id="hdr">
  <h1>Plays Knowledge Graph</h1>
  <span class="meta">{len(all_nodes)} nodes · {len(edges)} edges · rendering {'all' if args.all else f'top {args.top} by degree'}</span>
  <button id="physBtn" onclick="togglePhysics()">⏸ Freeze layout</button>
  <button id="geoBtn" onclick="toggleGeo()" title="Pin place nodes at their real coordinates">🌍 Geo layout</button>
  <span id="tl">🕓
    <input id="yFrom" type="range" min="{YMIN}" max="{YMAX}" value="{YMIN}" step="1" oninput="onYear()">
    <input id="yTo"   type="range" min="{YMIN}" max="{YMAX}" value="{YMAX}" step="1" oninput="onYear()">
    <b id="yLbl">{YMIN}–{YMAX}</b>
    <label><input id="yUnd" type="checkbox" checked onchange="onYear()"> undated</label>
  </span>
  {legend_items}
</div>
<script>
  window._nodeYears = {node_years_js};
  window._placeGeo  = {place_geo_js};
  window._typeOn = {{}};
  window._geoOn = false;

  function recompute() {{
    if (!window.nodes) return;
    const yF = +document.getElementById('yFrom').value;
    const yT = +document.getElementById('yTo').value;
    const lo = Math.min(yF, yT), hi = Math.max(yF, yT);
    const und = document.getElementById('yUnd').checked;
    const upd = [];
    window.nodes.get().forEach(n => {{
      const typeVis = window._typeOn[n.group] !== false;
      const yr = window._nodeYears[n.id];
      const yearVis = yr ? (yr[0] <= hi && yr[1] >= lo) : und;
      upd.push({{id: n.id, hidden: !(typeVis && yearVis)}});
    }});
    window.nodes.update(upd);
  }}
  function onYear() {{
    const yF = +document.getElementById('yFrom').value;
    const yT = +document.getElementById('yTo').value;
    document.getElementById('yLbl').textContent = Math.min(yF, yT) + '–' + Math.max(yF, yT);
    recompute();
  }}
  function toggleType(t, on) {{
    window._typeOn[t] = on;
    recompute();
  }}
  function togglePhysics() {{
    if (!window.network) return;
    window._physicsOn = !window._physicsOn;
    window.network.setOptions({{physics: {{enabled: window._physicsOn}}}});
    document.getElementById('physBtn').textContent = window._physicsOn ? '⏸ Freeze layout' : '▶ Resume physics';
  }}
  function toggleGeo() {{
    if (!window.network || !window.nodes) return;
    window._geoOn = !window._geoOn;
    const btn = document.getElementById('geoBtn');
    const upd = [];
    if (window._geoOn) {{
      // Pin every place with coordinates at its projected position; physics then
      // pulls the plays/people/orgs connected to it toward that geographic anchor.
      for (const id in window._placeGeo) {{
        const p = window._placeGeo[id];
        upd.push({{id: id, x: p[0], y: p[1], fixed: {{x: true, y: true}}}});
      }}
      window.nodes.update(upd);
      window.network.setOptions({{physics: {{enabled: true}}}});
      window._physicsOn = true;
      document.getElementById('physBtn').textContent = '⏸ Freeze layout';
      btn.classList.add('on');
      btn.textContent = '🌍 Geo: ON';
      setTimeout(() => window.network.fit({{animation: true}}), 1400);
    }} else {{
      for (const id in window._placeGeo) upd.push({{id: id, fixed: false}});
      window.nodes.update(upd);
      btn.classList.remove('on');
      btn.textContent = '🌍 Geo layout';
    }}
  }}
  // expose nodes/network globally (pyvis names them locally)
  window.addEventListener('load', () => {{
    const iv = setInterval(() => {{
      if (typeof nodes !== 'undefined' && typeof network !== 'undefined') {{
        window.nodes = nodes; window.network = network; window._physicsOn = true;
        // Once stabilized, freeze physics so the layout stops drifting
        network.once('stabilizationIterationsDone', () => {{
          if (window._geoOn) return;   // geo mode manages its own physics
          network.setOptions({{physics: {{enabled: false}}}});
          window._physicsOn = false;
          const b = document.getElementById('physBtn');
          if (b) b.textContent = '▶ Resume physics';
        }});
        clearInterval(iv);
      }}
    }}, 100);
  }});
</script>
"""
    txt = txt.replace("<body>", "<body>\n" + header, 1)
    out.write_text(txt, encoding="utf-8")

    print(f"wrote {out}  ({out.stat().st_size // 1024} KB)")
    print(f"open with: open {out}")

if __name__ == "__main__":
    main()
