"""Build one self-contained HTML page with a DraCor-style character-network
graph for each of the 15 print YiDraCor plays.

For each play:
- Nodes = characters (from <particDesc>/<listPerson>/<person>).
- Edges = act co-appearances (weighted by number of acts shared).
- Node size ∝ number of <sp> lines that character speaks.
- Edge width ∝ shared-act count.

Output: YiDraCor/tei/character_networks.html — one page, sidebar of plays
on the left, network canvas on the right, inline vis-network 9.1.2 (no CDN).

Usage:
    python3.11 YiDraCor/code/build_character_networks.py
"""
from __future__ import annotations
import html
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]  # YiDraCor/
TEI_DIR = ROOT / "tei" / "dracor"
FALLBACK_DIR = ROOT / "tei"                 # for castList when dracor variant lacks it
OUT_HTML = ROOT / "tei" / "character_networks.html"
VIS_JS = Path("/opt/anaconda3/lib/python3.11/site-packages/pyvis/lib/vis-9.1.2/vis-network.min.js")

NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def _first_text(el, xpath):
    n = el.find(xpath, NS)
    if n is None:
        return ""
    return "".join(n.itertext()).strip()


def load_persons(root):
    """Return dict xml:id -> {label, sex, role, description}."""
    persons = {}
    # Preferred location for DraCor-compat TEI: particDesc/listPerson/person
    for pn in root.findall(".//tei:particDesc//tei:person", NS):
        xid = pn.get("{http://www.w3.org/XML/1998/namespace}id") or ""
        if not xid:
            continue
        label = _first_text(pn, "tei:persName") or _first_text(pn, "tei:name")
        sex = pn.get("sex", "")
        role = pn.get("role", "")
        desc = _first_text(pn, "tei:note")
        persons[xid] = {"label": label or xid, "sex": sex, "role": role, "desc": desc}
    # Also merge in <role> entries from castList (they carry Yiddish names too)
    for rl in root.findall(".//tei:castList//tei:role", NS):
        xid = rl.get("{http://www.w3.org/XML/1998/namespace}id") or ""
        if not xid or xid in persons:
            continue
        label = _first_text(rl, "tei:roleName") or "".join(rl.itertext()).strip()
        persons[xid] = {"label": label or xid, "sex": rl.get("sex",""),
                        "role": "", "desc": ""}
    return persons


def load_acts(root):
    """Return list of {label, speakers_counter} for each act (or leaf div).
    Falls back to a single 'whole play' scene when no divs exist."""
    all_divs = root.findall(".//tei:div", NS)
    # Prefer act-level divs at the top of the body; if scenes exist inside acts,
    # use scenes (finer granularity, better co-appearance signal).
    scenes = [d for d in all_divs if d.get("type") == "scene"]
    if scenes:
        divs = scenes
    else:
        divs = all_divs
    if not divs:
        # single virtual "scene" spanning the whole body
        body = root.find(".//tei:body", NS)
        divs = [body] if body is not None else [root]
    acts = []
    for i, d in enumerate(divs, 1):
        head = _first_text(d, "tei:head") or ""
        dtype = d.get("type", "")
        label = head or (f"{dtype.capitalize() or 'Section'} {i}")
        # Collect all @who attributes on <sp> elements inside this div
        speakers = Counter()
        for sp in d.findall(".//tei:sp", NS):
            who = (sp.get("who") or "").strip()
            for tok in who.split():
                tok = tok.lstrip("#")
                if tok:
                    speakers[tok] += 1
        if speakers:
            acts.append({"label": label, "speakers": speakers})
    return acts


def build_network(persons, acts, min_edge=1):
    """Return (nodes, edges, stats).

    Node size = total sp count for that character across the play.
    Edge weight = number of acts both characters appear in.
    """
    total_sp = Counter()
    for a in acts:
        for xid, n in a["speakers"].items():
            total_sp[xid] += n
    # Determine node set: any character with ≥1 speech in the play (even if not
    # in particDesc — surfaces stray xml:ids for review).
    xids_in_play = set(total_sp) | set(persons)
    xids_in_play = {x for x in xids_in_play if total_sp.get(x, 0) > 0} | \
                   {x for x in persons if any(x in a["speakers"] for a in acts)}
    # If no one speaks, still show the castList to at least populate the graph.
    if not xids_in_play:
        xids_in_play = set(persons)
    # Build edges + track which acts contributed to each edge
    pair_weight = Counter()
    pair_acts = defaultdict(list)
    node_acts = defaultdict(list)
    for act_i, a in enumerate(acts):
        speaking = [x for x in a["speakers"] if x in xids_in_play]
        for x in speaking:
            node_acts[x].append(act_i)
        for i in range(len(speaking)):
            for j in range(i + 1, len(speaking)):
                pair = tuple(sorted([speaking[i], speaking[j]]))
                pair_weight[pair] += 1
                pair_acts[pair].append(act_i)
    nodes = []
    max_sp = max(total_sp.values()) if total_sp else 1
    for xid in sorted(xids_in_play):
        meta = persons.get(xid, {"label": xid, "sex": "", "role": "", "desc": ""})
        lbl = meta["label"] or xid
        sp = total_sp.get(xid, 0)
        color = {"MALE": "#4363d8", "FEMALE": "#e6194B"}.get(meta["sex"].upper(), "#3cb44b")
        # size 12..46
        size = 12 + (sp / max_sp) * 34 if max_sp else 12
        tip_parts = [f"<b>{html.escape(lbl)}</b>",
                     f"speeches: {sp}",
                     f"acts spoken in: {sum(1 for a in acts if xid in a['speakers'])}/{len(acts)}"]
        if meta["sex"]: tip_parts.append(f"sex: {meta['sex']}")
        if meta["role"]: tip_parts.append(f"role: {meta['role']}")
        if meta["desc"]: tip_parts.append(html.escape(meta["desc"])[:200])
        nodes.append({
            "id": xid, "label": lbl, "color": color, "size": size,
            "title": "<br>".join(tip_parts),
            "acts": sorted(node_acts.get(xid, [])),   # act indices this char speaks in
            "baseColor": color,                        # kept so JS can restore after highlight
        })
    edges = []
    for (a, b), w in pair_weight.items():
        if w < min_edge:
            continue
        edges.append({"from": a, "to": b, "width": max(1, w),
                      "title": f"co-appear in {w} act{'s' if w != 1 else ''}",
                      "color": {"color": "#888", "opacity": min(1.0, 0.35 + 0.15 * w)},
                      "acts": sorted(set(pair_acts[(a, b)]))})
    stats = {
        "chars": len(nodes),
        "speaking_chars": sum(1 for n in nodes if total_sp.get(n["id"], 0) > 0),
        "acts": len(acts),
        "total_sp": sum(total_sp.values()),
        "top_speakers": [(persons.get(x, {}).get("label", x), c)
                         for x, c in total_sp.most_common(5)],
    }
    return nodes, edges, stats


def get_title(root, fallback):
    """Prefer the Yiddish title; fall back to file stem."""
    for xp in [".//tei:titleStmt/tei:title[@type='main']",
               ".//tei:titleStmt/tei:title",
               ".//tei:title"]:
        n = root.find(xp, NS)
        if n is not None and (n.text or "").strip():
            return n.text.strip()
    return fallback


def process_play(path: Path):
    root = ET.parse(path).getroot()
    persons = load_persons(root)
    # Fallback to non-dracor TEI if this one lacks castList
    if not persons:
        alt = FALLBACK_DIR / path.name
        if alt.exists() and alt != path:
            alt_root = ET.parse(alt).getroot()
            persons = load_persons(alt_root)
    acts = load_acts(root)
    nodes, edges, stats = build_network(persons, acts)
    return {
        "id": path.stem,
        "title": get_title(root, path.stem),
        "file": path.name,
        "nodes": nodes,
        "edges": edges,
        "stats": stats,
        "acts": [{"label": a["label"], "n_speakers": len(a["speakers"]),
                  "top": [(persons.get(x, {}).get("label", x), c)
                          for x, c in a["speakers"].most_common(5)]}
                 for a in acts],
    }


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<title>YiDraCor · Character Networks</title>
<style>
  :root {{ color-scheme: light dark; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background:#111; color:#eee; direction: ltr; }}
  #layout {{ display: grid; grid-template-columns: 280px 1fr; height: 100vh; }}
  #sidebar {{ background:#161616; border-right:1px solid #2a2a2a; overflow-y:auto; padding:12px 0; }}
  #sidebar h1 {{ font-size:14px; margin:6px 14px 12px; color:#ccc; font-weight:600; letter-spacing:0.4px; text-transform:uppercase; }}
  .play-item {{ padding:10px 14px; cursor:pointer; border-left:3px solid transparent;
                display:flex; flex-direction:column; gap:2px; font-size:13px; color:#ddd; }}
  .play-item:hover {{ background:#1e1e1e; }}
  .play-item.active {{ background:#252525; border-left-color:#3cb44b; }}
  .play-item .yi {{ direction:rtl; font-size:15px; color:#eee; }}
  .play-item .en {{ font-size:11px; color:#888; }}
  .play-item .meta {{ font-size:10px; color:#666; margin-top:2px; }}
  #main {{ position:relative; overflow:hidden; }}
  #header {{ padding:10px 20px; background:#181818; border-bottom:1px solid #2a2a2a;
             display:flex; align-items:baseline; gap:20px; flex-wrap:wrap; }}
  #header h2 {{ margin:0; font-size:18px; direction:rtl; }}
  #header .stats {{ font-size:12px; color:#999; }}
  #header .stats b {{ color:#ccc; }}
  #graph {{ position:absolute; top:44px; bottom:0; left:0; right:0; background:#111; }}
  #graph canvas {{ background:#111; }}
  #actlist {{ position:absolute; top:54px; right:12px; width:260px; max-height:60vh; overflow-y:auto;
              background:rgba(20,20,20,0.9); border:1px solid #333; border-radius:6px;
              padding:8px 12px; font-size:11px; direction:rtl; z-index:10; }}
  #actlist h3 {{ font-size:11px; margin:2px 0 6px; color:#888; text-transform:uppercase; letter-spacing:0.4px; direction:ltr; }}
  .act-row {{ margin-bottom:6px; padding:4px 6px; border-radius:4px; cursor:pointer; border:1px solid transparent; }}
  .act-row:hover {{ background:#1e1e1e; }}
  .act-row.active {{ background:#252525; border-color:#3cb44b; }}
  .act-row .name {{ color:#ccc; font-weight:500; }}
  .act-row .top {{ font-size:10px; color:#888; }}
  .act-row.all-btn {{ background:#1a1a1a; color:#aaa; text-align:center; }}
  .act-row.all-btn.active {{ background:#252525; border-color:#3cb44b; }}
  .legend {{ position:absolute; bottom:12px; left:12px; background:rgba(20,20,20,0.85);
             padding:6px 10px; border-radius:4px; font-size:11px; color:#aaa;
             display:flex; gap:12px; direction:ltr; z-index:10; }}
  .legend .chip {{ display:inline-flex; align-items:center; gap:4px; }}
  .legend .dot {{ display:inline-block; width:10px; height:10px; border-radius:50%; }}
</style>
<script>
{vis_js}
</script>
</head>
<body>
<div id="layout">
  <nav id="sidebar">
    <h1>YiDraCor · Plays</h1>
    <div id="playlist"></div>
  </nav>
  <main id="main">
    <div id="header">
      <h2 id="playtitle"></h2>
      <div class="stats" id="stats"></div>
    </div>
    <div id="graph"></div>
    <div id="actlist"></div>
    <div class="legend">
      <span class="chip"><span class="dot" style="background:#4363d8"></span> male</span>
      <span class="chip"><span class="dot" style="background:#e6194B"></span> female</span>
      <span class="chip"><span class="dot" style="background:#3cb44b"></span> unknown</span>
      <span class="chip">node size = # speeches</span>
      <span class="chip">edge width = # shared acts</span>
      <span class="chip" style="color:#3cb44b">▸ click a node = neighbors · click an act = ensemble · click empty = reset</span>
    </div>
  </main>
</div>
<script>
const PLAYS = {plays_json};
let currentNet = null;
let currentPlay = null;      // reference to the loaded play data
let currentData = null;      // {{ nodes: DataSet, edges: DataSet }}
let selectionMode = null;    // {{ type: 'act'|'node', actIdx?, nodeId? }} or null

const DIM_NODE  = {{ background: '#2a2a2a', border: '#333' }};
const DIM_EDGE  = {{ color: '#1e1e1e', opacity: 0.1 }};
const HL_BORDER = '#fff';

function nodeBaseColor(n) {{ return n.baseColor || n.color || '#3cb44b'; }}
function edgeBaseColor(e, w) {{ return {{ color: '#888', opacity: Math.min(1.0, 0.35 + 0.15 * (w||1)) }}; }}

function resetHighlight() {{
  selectionMode = null;
  const nodeUpdates = currentPlay.nodes.map(n => ({{
    id: n.id, color: {{ background: nodeBaseColor(n), border: '#333' }},
    borderWidth: 1, font: {{ color: '#eee' }},
  }}));
  const edgeUpdates = currentPlay.edges.map(e => ({{
    id: e.from + '__' + e.to, color: edgeBaseColor(e, e.width),
  }}));
  currentData.nodes.update(nodeUpdates);
  currentData.edges.update(edgeUpdates);
  document.querySelectorAll('.act-row').forEach(r => r.classList.remove('active'));
  document.querySelector('.act-row.all-btn')?.classList.add('active');
}}

function highlightAct(actIdx) {{
  selectionMode = {{ type: 'act', actIdx }};
  const involved = new Set();
  currentPlay.nodes.forEach(n => {{ if ((n.acts||[]).includes(actIdx)) involved.add(n.id); }});
  const nodeUpdates = currentPlay.nodes.map(n => {{
    const on = involved.has(n.id);
    return {{
      id: n.id,
      color: on ? {{ background: nodeBaseColor(n), border: HL_BORDER }} : DIM_NODE,
      borderWidth: on ? 3 : 1,
      font: {{ color: on ? '#eee' : '#555' }},
    }};
  }});
  const edgeUpdates = currentPlay.edges.map(e => {{
    const on = (e.acts||[]).includes(actIdx) && involved.has(e.from) && involved.has(e.to);
    return {{
      id: e.from + '__' + e.to,
      color: on ? {{ color: '#fff', opacity: 0.85 }} : DIM_EDGE,
    }};
  }});
  currentData.nodes.update(nodeUpdates);
  currentData.edges.update(edgeUpdates);
  // camera fits to the highlighted subgraph
  if (involved.size) {{
    currentNet.fit({{ nodes: [...involved], animation: {{ duration: 500, easingFunction: 'easeOutQuad' }} }});
  }}
  document.querySelectorAll('.act-row').forEach((r, i) => {{
    r.classList.toggle('active', r.dataset.actIdx == actIdx);
  }});
}}

function highlightNode(nodeId) {{
  selectionMode = {{ type: 'node', nodeId }};
  const neighbors = new Set([nodeId]);
  currentPlay.edges.forEach(e => {{
    if (e.from === nodeId) neighbors.add(e.to);
    if (e.to === nodeId)   neighbors.add(e.from);
  }});
  const nodeUpdates = currentPlay.nodes.map(n => {{
    const isSeed = n.id === nodeId;
    const isNeighbor = neighbors.has(n.id);
    return {{
      id: n.id,
      color: isNeighbor ? {{ background: nodeBaseColor(n),
                              border: isSeed ? HL_BORDER : nodeBaseColor(n) }}
                        : DIM_NODE,
      borderWidth: isSeed ? 4 : (isNeighbor ? 2 : 1),
      font: {{ color: isNeighbor ? '#eee' : '#555' }},
    }};
  }});
  const edgeUpdates = currentPlay.edges.map(e => {{
    const on = e.from === nodeId || e.to === nodeId;
    return {{
      id: e.from + '__' + e.to,
      color: on ? {{ color: '#fff', opacity: 0.85 }} : DIM_EDGE,
    }};
  }});
  currentData.nodes.update(nodeUpdates);
  currentData.edges.update(edgeUpdates);
  document.querySelectorAll('.act-row').forEach(r => r.classList.remove('active'));
}}

function renderPlay(idx) {{
  const p = PLAYS[idx];
  currentPlay = p;
  document.querySelectorAll('.play-item').forEach((el, i) => {{
    el.classList.toggle('active', i === idx);
  }});
  document.getElementById('playtitle').textContent = p.title;
  const s = p.stats;
  const top = s.top_speakers.map(([n, c]) => `${{n}} <span style="color:#666">·${{c}}</span>`).join(' ');
  document.getElementById('stats').innerHTML =
    `<b>${{s.speaking_chars}}</b>/${{s.chars}} speaking characters &middot; ` +
    `<b>${{s.acts}}</b> acts &middot; <b>${{s.total_sp}}</b> speeches &middot; ` +
    `top: ${{top}}`;

  // Act list — with a top "All" pseudo-row and clickable act rows
  const alDiv = document.getElementById('actlist');
  const rows = [`<h3>Acts · click to isolate</h3>`,
    `<div class="act-row all-btn active" data-act-idx="-1">All acts (reset)</div>`,
  ].concat(p.acts.map((a, i) => {{
    const top = a.top.map(([n,c]) => `${{n}} ·${{c}}`).join('  ');
    return `<div class="act-row" data-act-idx="${{i}}"><div class="name">${{a.label}} (${{a.n_speakers}})</div><div class="top">${{top}}</div></div>`;
  }}));
  alDiv.innerHTML = rows.join('');
  alDiv.querySelectorAll('.act-row').forEach(r => {{
    r.addEventListener('click', () => {{
      const idx = parseInt(r.dataset.actIdx, 10);
      if (idx < 0) resetHighlight();
      else highlightAct(idx);
    }});
  }});

  // Build vis network — give every edge a stable id so we can update it later
  if (currentNet) {{ currentNet.destroy(); currentNet = null; }}
  const container = document.getElementById('graph');
  const nodeCopies = p.nodes.map(n => ({{ ...n }}));
  const edgeCopies = p.edges.map(e => ({{ ...e, id: e.from + '__' + e.to }}));
  currentData = {{
    nodes: new vis.DataSet(nodeCopies),
    edges: new vis.DataSet(edgeCopies),
  }};
  const opts = {{
    nodes: {{ shape: 'dot', font: {{ color: '#eee', size: 14, face: 'sans-serif' }},
              borderWidth: 1, color: {{ border: '#333' }} }},
    edges: {{ smooth: false, color: {{ color: '#555', opacity: 0.5 }} }},
    interaction: {{ hover: true, tooltipDelay: 100, navigationButtons: true, keyboard: true, multiselect: false }},
    physics: {{
      solver: 'forceAtlas2Based',
      forceAtlas2Based: {{ gravitationalConstant: -80, centralGravity: 0.02,
                           springLength: 100, springConstant: 0.08, damping: 0.9, avoidOverlap: 0.4 }},
      stabilization: {{ enabled: true, iterations: 800, updateInterval: 50, fit: true }},
      timestep: 0.35, minVelocity: 0.75, maxVelocity: 40, adaptiveTimestep: true,
    }},
  }};
  currentNet = new vis.Network(container, currentData, opts);
  currentNet.once('stabilizationIterationsDone', () => {{
    currentNet.setOptions({{ physics: {{ enabled: false }} }});
  }});
  // Interaction handlers
  currentNet.on('click', (params) => {{
    if (params.nodes && params.nodes.length) {{
      highlightNode(params.nodes[0]);
    }} else if (params.edges && params.edges.length === 0) {{
      // click on empty canvas — reset
      resetHighlight();
    }}
  }});
}}

// Build sidebar
const list = document.getElementById('playlist');
PLAYS.forEach((p, i) => {{
  const el = document.createElement('div');
  el.className = 'play-item';
  el.innerHTML = `<span class="yi">${{p.title}}</span>` +
                 `<span class="en">${{p.id.replace(/-/g,' ')}}</span>` +
                 `<span class="meta">${{p.stats.speaking_chars}} chars · ${{p.stats.acts}} acts · ${{p.stats.total_sp}} speeches</span>`;
  el.onclick = () => renderPlay(i);
  list.appendChild(el);
}});
// Auto-load first
renderPlay(0);
</script>
</body>
</html>
"""


def main():
    plays = []
    for path in sorted(TEI_DIR.glob("*.xml")):
        try:
            plays.append(process_play(path))
        except Exception as e:
            print(f"  ✗ {path.name}: {e}")
    print(f"processed {len(plays)} plays")

    vis_js = VIS_JS.read_text(encoding="utf-8")
    html_out = HTML_TEMPLATE.format(
        vis_js=vis_js,
        plays_json=json.dumps(plays, ensure_ascii=False),
    )
    OUT_HTML.write_text(html_out, encoding="utf-8")
    kb = OUT_HTML.stat().st_size // 1024
    print(f"wrote {OUT_HTML} ({kb} KB)")

    print("\nPer-play summary:")
    for p in plays:
        s = p["stats"]
        print(f"  {p['id']:25s} {s['speaking_chars']:3d}/{s['chars']:3d} chars, "
              f"{s['acts']:2d} acts, {s['total_sp']:4d} speeches, "
              f"{len(p['edges']):3d} edges")


if __name__ == "__main__":
    main()
