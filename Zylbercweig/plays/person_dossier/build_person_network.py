#!/usr/bin/env python3.11
"""Build the ego-network page for a person dossier (docs/<slug>_network.html).

Reads kg/nodes.tsv + kg/edges.tsv and the dossier config. Emits a lean page:
vis-network from CDN + an inline __PAYLOAD__ (not a 3 MB pyvis dump).
build_kg_viz.py is a palette/behaviour reference only.

Display-level only: the config's "merge" variant nodes have their edges
re-attributed to the ego and flagged "via name variant"; nothing is written
back to the KG TSVs.

Usage: python3.11 build_person_network.py --config rumshinsky.json
"""
import argparse, csv, json, re, unicodedata
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
KG = HERE.parent / "kg"
DOCS = HERE.parent.parent.parent / "docs"

csv.field_size_limit(10**8)
POINTS = re.compile(r"[֑-ׇ]")


def sp(s):
    return POINTS.sub("", unicodedata.normalize("NFC", s or ""))


def load_tsv(p):
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = json.loads((HERE / args.config).read_text(encoding="utf-8"))
    ego = cfg["node_id"]

    nodes = {n["node_id"]: n for n in load_tsv(KG / "nodes.tsv")}
    edges = load_tsv(KG / "edges.tsv")

    merge_ids = {v["node_id"] for v in cfg["variant_nodes"] if v["verdict"] == "merge"}
    duo = {v["node_id"]: v for v in cfg["variant_nodes"] if v["verdict"] == "duo"}
    variant_ids = merge_ids | set(duo)

    # edge_type -> class
    cls_of, CLASSES = {}, cfg["edge_classes"]
    for cname, spec in CLASSES.items():
        for t in spec["types"]:
            cls_of[t] = cname

    # org -> located_in place label (for tooltips)
    org_place = {}
    for e in edges:
        if e["edge_type"] == "located_in" and e["target_id"].startswith("place:"):
            tgt = nodes.get(e["target_id"], {})
            org_place.setdefault(
                e["source_id"],
                sp(tgt.get("label_english") or tgt.get("label_yiddish") or ""))

    # ---- collect ego edges (direct + re-attributed from variant nodes) ----
    out_edges, unclassified = [], Counter()
    for e in edges:
        s, t = e["source_id"], e["target_id"]
        via = ""
        if s == ego or t == ego:
            pass
        elif s in variant_ids or t in variant_ids:
            via = s if s in variant_ids else t
            # re-attribute: the variant node stands in for the ego
            s = ego if s == via else s
            t = ego if t == via else t
        else:
            continue
        if s == t:
            continue
        cname = cls_of.get(e["edge_type"])
        if not cname:
            unclassified[e["edge_type"]] += 1
            cname = "discourse"
        other = t if s == ego else s
        rec = {
            "id": e["edge_id"], "from": s, "to": t, "other": other,
            "type": e["edge_type"], "cls": cname,
            "role": sp(e.get("role_detail", "")),
            "d0": e.get("date_start", ""), "d1": e.get("date_end", ""),
            "ev": sp(e.get("evidence_sentence", ""))[:400],
            "via": via, "viaLabel": sp(nodes.get(via, {}).get("label_yiddish", "")) if via else "",
        }
        if via in duo:
            rec["label"] = duo[via]["edge_label"]
            rec["duo"] = True
            rec["duoWith"] = duo[via].get("duo_with", "")
        out_edges.append(rec)

    # ---- node payload ----
    keep = {ego} | {e["other"] for e in out_edges}
    # the duo partner is worth showing even if no direct edge survived
    for v in duo.values():
        if v.get("duo_with") in nodes:
            keep.add(v["duo_with"])

    deg = Counter()
    for e in out_edges:
        deg[e["other"]] += 1

    out_nodes = []
    for nid in sorted(keep):
        n = nodes.get(nid)
        if not n:
            continue
        yi = sp(n.get("label_yiddish", ""))
        en = sp(n.get("label_english", ""))
        ntype = n["node_type"]
        out_nodes.append({
            "id": nid, "yi": yi, "en": en, "type": ntype,
            "unmatched": (n.get("match_status") == "unmatched"),
            "deg": deg.get(nid, 0), "ego": nid == ego,
            "place": org_place.get(nid, ""),
            "ref": (f"{n.get('ext_ref_type','')}:{n.get('ext_ref_id','')}"
                    if n.get("ext_ref_id") else ""),
        })

    payload = {
        "ego": ego, "name": cfg["display_name"], "nameYi": cfg["display_name_yi"],
        "nodes": out_nodes, "edges": out_edges,
        "classes": {k: {"color": v["color"], "dashes": v["dashes"], "width": v["width"]}
                    for k, v in CLASSES.items()},
        "variants": cfg["variant_nodes"],
        "entryPid": cfg["entry_person_id"],
        "slug": cfg["slug"],
    }

    by_cls = Counter(e["cls"] for e in out_edges)
    direct = sum(1 for e in out_edges if not e["via"])
    print(f"nodes: {len(out_nodes)}  edges: {len(out_edges)} "
          f"({direct} direct + {len(out_edges)-direct} re-attributed)")
    print("by class:", dict(by_cls))
    if unclassified:
        print("WARNING unclassified edge types (bucketed as discourse):", dict(unclassified))

    html = PAGE.replace("__SLUG__", cfg["slug"]) \
               .replace("__NAME__", cfg["display_name"]) \
               .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False))
    out = DOCS / f"{cfg['slug']}_network.html"
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out} — {out.stat().st_size/1024:.0f} KB")


FONTS = ('<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
         'family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&'
         'family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500'
         '&display=swap">')

PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__NAME__ · Network</title>""" + FONTS + """
<script src="https://unpkg.com/vis-network@9.1.2/standalone/umd/vis-network.min.js"></script>
<style>
:root{--paper:#faf7f0;--ink:#272219;--soft:#5c554a;--line:#e2dcce;--card:#f2eddf;
--teal:#0e6f8a;--teal2:#7fb8c9;--brick:#a1462e;--amber:#b07d2b;--grey:#a8a08e;
--green:#2f8f7f;--violet:#6b5b95}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);
font-family:"Spectral",Georgia,serif;font-size:15px}
header{padding:1.4rem 1.6rem .4rem;max-width:78rem;margin:0 auto}
h1{font-family:"Fraunces",serif;font-weight:600;font-size:1.9rem;margin:.1rem 0 .3rem}
.eyebrow{font-family:"IBM Plex Mono",monospace;font-size:.68rem;letter-spacing:.13em;
text-transform:uppercase;color:var(--teal);margin:0}
.lede{color:var(--soft);margin:.2rem 0 .5rem;max-width:48rem}
.nav{font-size:.85rem}.nav a{color:var(--teal);margin-right:1rem}
.yi{font-family:"Frank Ruhl Libre",serif;unicode-bidi:isolate}
.controls{display:flex;flex-wrap:wrap;gap:.5rem 1.1rem;align-items:center;
padding:.4rem 1.6rem .6rem;max-width:78rem;margin:0 auto;font-size:.84rem}
.controls label{display:flex;gap:.35rem;align-items:center;cursor:pointer}
.sw{display:inline-block;width:1.5em;height:0;border-top-width:3px;
border-top-style:solid;vertical-align:middle}
.dot{display:inline-block;width:.75em;height:.75em;border-radius:50%;vertical-align:-1px}
button{font-family:"IBM Plex Mono",monospace;font-size:.72rem;letter-spacing:.05em;
border:1px solid var(--line);background:var(--card);color:var(--ink);
border-radius:4px;padding:.3rem .6rem;cursor:pointer}
button:hover{border-color:var(--teal)}
#net{height:calc(100vh - 235px);min-height:420px;margin:0 1.6rem;
border:1px solid var(--line);border-radius:4px;background:#fdfbf6}
#tip{position:fixed;display:none;background:var(--card);border:1px solid var(--line);
border-radius:4px;padding:.55rem .75rem;font-size:.82rem;max-width:29rem;
pointer-events:none;box-shadow:0 2px 12px rgba(0,0,0,.14);z-index:20;line-height:1.5}
#tip .yi{direction:rtl;display:block;text-align:right;font-size:1rem}
#tip .ev{color:var(--soft);font-style:italic;direction:rtl;text-align:right;
display:block;margin-top:.3rem;font-family:"Frank Ruhl Libre",serif}
#tip .kv{font-family:"IBM Plex Mono",monospace;font-size:.68rem;color:var(--soft)}
.badge{display:inline-block;background:#a1462e22;color:var(--brick);border-radius:3px;
padding:0 .35em;font-family:"IBM Plex Mono",monospace;font-size:.66rem}
#count{color:var(--soft);font-size:.8rem}
</style></head><body>
<header>
<p class="eyebrow">Dybbuk · Person dossier · professional network</p>
<h1>__NAME__ — ego network</h1>
<p class="lede">Every KG edge on the ego node, one hop out. Edge <b>class</b> is the
structure: professional ties, family ties (brick, dashed), education (green, dotted),
and the diffuse <b>discourse</b> layer of <span class="mono">wrote_about</span> /
<span class="mono">associated_with</span> mentions — switch discourse off to see the
career skeleton. Dashed node borders = unmatched (<span class="mono">UP-</span>) nodes.
A halo marks an edge re-attributed here from a name-variant node.</p>
<p class="nav"><a href="__SLUG___dossier.html">← dossier</a>
<a href="__SLUG___timeline.html">timeline →</a>
<a href="__SLUG___map.html">map →</a></p>
</header>
<div class="controls" id="legend"></div>
<div class="controls" style="padding-top:0">
  <label><input type="checkbox" id="variants" checked> include name-variant edges</label>
  <label><input type="checkbox" id="labels" checked> node labels</label>
  <button id="freeze">freeze physics</button>
  <button id="fit">fit</button>
  <span id="count"></span>
</div>
<div id="net"></div>
<div id="tip"></div>
<script>
const P = __PAYLOAD__;
const TYPE_COLOR={person:'#0e6f8a',play:'#a1462e',org:'#b07d2b',org_cluster:'#b07d2b',
  place:'#6b5b95',production_event:'#2f8f7f',venue:'#42a4b4',edition:'#8a8577'};
const TYPE_SHAPE={person:'dot',play:'diamond',org:'square',org_cluster:'square',
  place:'triangle',production_event:'star',venue:'star',edition:'box'};
const CLS=P.classes, CLSNAMES=Object.keys(CLS);
const clsCount={};
P.edges.forEach(e=>clsCount[e.cls]=(clsCount[e.cls]||0)+1);

// ---- legend with per-class toggles ----
document.getElementById('legend').innerHTML =
  CLSNAMES.map(c=>{
    const s=CLS[c], d=Array.isArray(s.dashes)?'dotted':(s.dashes?'dashed':'solid');
    return `<label><input type="checkbox" class="clsx" data-c="${c}" checked>`+
      `<span class="sw" style="border-top-color:${s.color};border-top-style:${d}"></span>`+
      `${c} <span style="color:var(--soft)">${clsCount[c]||0}</span></label>`;
  }).join('') +
  `<span style="color:var(--soft);margin-left:.4rem">nodes:</span>` +
  Object.entries({person:'person',play:'play',org:'org / org-cluster',place:'place'})
    .map(([k,l])=>`<span><span class="dot" style="background:${TYPE_COLOR[k]}"></span> ${l}</span>`)
    .join('');

const nodeById={};P.nodes.forEach(n=>nodeById[n.id]=n);
const shortLabel=n=>{const t=n.yi||n.en||n.id;return t.length>26?t.slice(0,24)+'…':t;};

function nodeObj(n,withLabels){
  const col=TYPE_COLOR[n.type]||'#8a8577';
  const size=n.ego?34:Math.min(9+n.deg*1.7,24);
  return {id:n.id,label:withLabels?shortLabel(n):' ',
    shape:n.ego?'dot':(TYPE_SHAPE[n.type]||'dot'),size:size,
    borderWidth:n.ego?4:(n.unmatched?2:1),
    shapeProperties:{borderDashes:n.unmatched?[4,3]:false},
    color:{background:n.ego?'#0b5a70':col,border:n.ego?'#062f3c':(n.unmatched?'#8a8577':col),
      highlight:{background:col,border:'#272219'}},
    opacity:n.unmatched?0.72:1,
    font:{face:'Frank Ruhl Libre, Spectral, serif',size:n.ego?17:13,
      color:'#272219',strokeWidth:4,strokeColor:'#faf7f0'}};
}

function edgeObj(e){
  const s=CLS[e.cls];
  const o={id:e.id,from:e.from,to:e.to,
    color:{color:s.color,opacity:e.cls==='discourse'?0.34:0.8,highlight:'#272219'},
    dashes:s.dashes,width:s.width,
    smooth:{type:'continuous',roundness:.18},
    arrows:{to:{enabled:e.cls!=='family'&&e.cls!=='discourse',scaleFactor:.42}}};
  if(e.cls==='family'&&e.role)
    o.label=e.role, o.font={size:10,color:'#a1462e',strokeWidth:4,strokeColor:'#faf7f0',align:'top'};
  if(e.label)
    o.label=e.label, o.font={size:10,color:'#0b5a70',strokeWidth:4,strokeColor:'#faf7f0',align:'top'};
  if(e.via) o.shadow={enabled:true,color:'rgba(176,125,43,.65)',size:9,x:0,y:0};
  return o;
}

const nodesDS=new vis.DataSet([]), edgesDS=new vis.DataSet([]);
const net=new vis.Network(document.getElementById('net'),
  {nodes:nodesDS,edges:edgesDS},{
  physics:{solver:'forceAtlas2Based',stabilization:{iterations:260},
    forceAtlas2Based:{gravitationalConstant:-72,centralGravity:0.012,
      springLength:135,springConstant:0.075,damping:0.85,avoidOverlap:0.3}},
  interaction:{hover:true,tooltipDelay:100000,navigationButtons:false,zoomView:true},
  // NB: colors are set per-node/per-edge above; vis groups would override them.
  nodes:{borderWidthSelected:4},edges:{selectionWidth:2}});

function render(){
  const on={};document.querySelectorAll('.clsx').forEach(c=>on[c.dataset.c]=c.checked);
  const withVar=document.getElementById('variants').checked;
  const withLabels=document.getElementById('labels').checked;
  const es=P.edges.filter(e=>on[e.cls]&&(withVar||!e.via));
  const keep=new Set([P.ego]);es.forEach(e=>{keep.add(e.from);keep.add(e.to);});
  nodesDS.clear();edgesDS.clear();
  nodesDS.add(P.nodes.filter(n=>keep.has(n.id)).map(n=>nodeObj(n,withLabels)));
  edgesDS.add(es.map(edgeObj));
  document.getElementById('count').textContent=
    `${keep.size} nodes · ${es.length} edges shown`;
}

// ---- hover tooltip (vis titles are plain text; we want RTL + HTML) ----
const tip=document.getElementById('tip');
const esc=s=>(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;');
function show(html,ev){tip.innerHTML=html;tip.style.display='block';
  tip.style.left=Math.min(ev.clientX+15,innerWidth-470)+'px';
  tip.style.top=Math.min(ev.clientY+14,innerHeight-40)+'px';}
const hide=()=>tip.style.display='none';
net.on('hoverNode',p=>{
  const n=nodeById[p.node];if(!n)return;
  show(`<span class="yi">${esc(n.yi)||'&nbsp;'}</span><b>${esc(n.en)||''}</b>`
    +`<span class="kv">${n.type} · ${n.id}${n.ref?' · '+esc(n.ref):''}`
    +`${n.unmatched?' · unmatched':''} · ${n.deg} edge${n.deg===1?'':'s'} here</span>`
    +(n.place?`<br>located in <b>${esc(n.place)}</b>`:''),p.event);
});
net.on('hoverEdge',p=>{
  const e=P.edges.find(x=>x.id===p.edge);if(!e)return;
  const o=nodeById[e.other]||{};
  const dates=[e.d0,e.d1].filter(Boolean).join('–');
  show(`<b>${esc(e.type)}</b>${e.role?' · '+esc(e.role):''}`
    +`${dates?' <span class="kv">'+esc(dates)+'</span>':''}<br>`
    +`→ <span class="yi">${esc(o.yi||o.en||e.other)}</span>`
    +(e.via?`<br><span class="badge">via name variant</span> `
      +`<span class="yi" style="display:inline">${esc(e.viaLabel)}</span> `
      +`<span class="kv">${esc(e.via)}</span>`:'')
    +(e.duo?`<br><span class="badge">management duo</span> — not a spelling variant`:'')
    +(e.ev?`<span class="ev">${esc(e.ev)}</span>`:''),p.event);
});
net.on('blurNode',hide);net.on('blurEdge',hide);net.on('dragStart',hide);
net.on('click',p=>{if(!p.nodes.length&&!p.edges.length)hide();});

document.querySelectorAll('.clsx').forEach(c=>c.onchange=render);
document.getElementById('variants').onchange=render;
document.getElementById('labels').onchange=render;
let frozen=false;
document.getElementById('freeze').onclick=e=>{
  frozen=!frozen;net.setOptions({physics:{enabled:!frozen}});
  e.target.textContent=frozen?'unfreeze physics':'freeze physics';};
document.getElementById('fit').onclick=()=>net.fit({animation:true});
net.once('stabilizationIterationsDone',()=>{
  net.setOptions({physics:{enabled:false}});frozen=true;
  document.getElementById('freeze').textContent='unfreeze physics';
  net.fit();});
render();
</script>
</body></html>"""

if __name__ == "__main__":
    main()
