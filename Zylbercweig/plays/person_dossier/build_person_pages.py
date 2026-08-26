#!/usr/bin/env python3.11
"""Build docs/<slug>_course.html — the combined life & career course page.

Map and timeline are ONE view over the same stations, not two pages: hovering
either highlights the other, clicking a timeline band opens that station on the
map, and dragging the year axis scrubs a period so the map redraws to just that
window. That coupling is the point — a route map alone collapses forty years
into one tangle of lines.

Adapted from itinerary/build_istanbul_pages.py: the inline-SVG timeline engine,
COMMON_CSS palette and ENTRY_PANEL are carried over; the Istanbul-specific parts
(IST_RE, the wave colouring, DUP_GROUPS, istMarker) are dropped. The timeline is
re-laned by PLACE rather than by person, since a person dossier has one career,
and its year scale is computed from the container width so it never needs
horizontal scrolling.

Usage: python3.11 build_person_pages.py --config rumshinsky.json
"""
import argparse, json, re, unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
DOCS = HERE.parent.parent.parent / "docs"
POINTS = re.compile(r"[֑-ׇ]")


def sp(s):
    return POINTS.sub("", unicodedata.normalize("NFC", s or ""))


def build_payload(data):
    it = next((i for i in data["itineraries"] if i["is_entry_subject"]), None)
    if it is None:
        raise SystemExit("no entry-subject itinerary in the data")
    others = [i for i in data["itineraries"] if not i["is_entry_subject"]]

    def stop(s):
        return {
            "seq": s["seq"], "place": sp(s["place"]), "en": s["label_en"],
            "verb": s["verb"], "org": sp(s.get("org", "")), "role": s.get("role", ""),
            "ds": s["date_start"], "de": s["date_end"], "cert": s["certainty"],
            "t0": s["t_start"], "t1": s["t_end"], "inf": bool(s["t_inferred"]),
            "res": s["res_status"], "qid": s["qid"], "lat": s["lat"], "lon": s["lon"],
            "kg": bool(s.get("kg_backed")),
            "evid": sp(s.get("evidence", ""))[:300],
            "ev": [{"t": e.get("event_type", ""), "play": sp(e.get("play") or ""),
                    "venue": sp(e.get("venue") or ""), "date": e.get("date", ""),
                    "d": e.get("description", "")} for e in s["events"]],
        }

    stations = [stop(s) for s in it["stations"]]

    # lane order = first appearance, one lane per resolved place
    lanes, seen = [], {}
    for s in stations:
        k = s["qid"] or s["place"]
        if k not in seen:
            seen[k] = len(lanes)
            lanes.append({"key": k, "en": s["en"], "yi": s["place"],
                          "region": s["res"].startswith("region")})
        s["lane"] = seen[k]

    anchors = []
    for a in data["kg_anchors"]:
        anchors.append({
            "id": a["edge_id"], "type": a["type"], "role": a["role"],
            "yi": a["other_yi"], "en": a["other_en"], "otype": a["other_type"],
            "placeYi": a["place_yi"], "placeEn": a["place_en"], "qid": a["qid"],
            "lat": a["lat"], "lon": a["lon"], "y0": a["y0"], "y1": a["y1"],
            "verdict": a["verdict"], "seqs": a.get("matched_seq", []),
            "ev": a["ev"],
            "lane": seen.get(a["qid"], seen.get(a["place_yi"], -1)),
        })
    # A fact about a PLAY (composed_music) carries no place of its own, so it has
    # no lane — yet it is exactly the kind of fact the agreement table argues
    # about. Put it on the lane of the station it was matched to: the KG says he
    # composed X in 1924, and the timeline shows where he was in 1924.
    lane_of_seq = {s["seq"]: s["lane"] for s in stations}
    for a in anchors:
        if a["lane"] < 0 and a["seqs"]:
            a["lane"] = lane_of_seq.get(a["seqs"][0], -1)
            a["viaStation"] = a["seqs"][0]

    return {
        "name": data["name"], "nameYi": data["name_yi"], "slug": data["slug"],
        "pid": data["person_id"], "life": data["life"],
        "tl": data["timeline"], "mapCfg": data["map"],
        "stations": stations, "lanes": lanes, "anchors": anchors,
        "extractionOnly": data["extraction_only_seq"],
        "others": [{"subject": o["subject"], "stations": [stop(s) for s in o["stations"]]}
                   for o in others],
    }


FONTS = ('<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
         'family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&'
         'family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500'
         '&display=swap">')

COMMON_CSS = """
:root{--paper:#faf7f0;--ink:#272219;--soft:#5c554a;--line:#e2dcce;--card:#f2eddf;
--teal:#0e6f8a;--teal2:#7fb8c9;--brick:#a1462e;--amber:#b07d2b;--grey:#c9c2b2;
--green:#2f8f7f;--violet:#6b5b95}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);
font-family:"Spectral",Georgia,serif;font-size:15px}
header{padding:1.4rem 1.6rem .5rem;max-width:82rem;margin:0 auto}
h1{font-family:"Fraunces",serif;font-weight:600;font-size:1.9rem;margin:.1rem 0 .3rem}
.eyebrow{font-family:"IBM Plex Mono",monospace;font-size:.68rem;letter-spacing:.13em;
text-transform:uppercase;color:var(--teal);margin:0}
.lede{color:var(--soft);margin:.2rem 0 .6rem;max-width:52rem}
.nav{font-size:.85rem}.nav a{color:var(--teal);margin-right:1rem}
.yi{font-family:"Frank Ruhl Libre",serif;unicode-bidi:isolate}
.controls{display:flex;flex-wrap:wrap;gap:.5rem 1.1rem;align-items:center;
padding:.4rem 1.6rem .6rem;max-width:82rem;margin:0 auto;font-size:.84rem}
.controls label{display:flex;gap:.35rem;align-items:center;cursor:pointer}
.legend{display:inline-flex;gap:.9rem;flex-wrap:wrap;font-size:.8rem;color:var(--soft)}
.sw{display:inline-block;width:.75em;height:.75em;border-radius:50%;vertical-align:-1px}
button{font-family:"IBM Plex Mono",monospace;font-size:.72rem;letter-spacing:.05em;
border:1px solid var(--line);background:var(--card);color:var(--ink);
border-radius:4px;padding:.3rem .6rem;cursor:pointer}
button:hover{border-color:var(--teal)}
#tip{position:fixed;display:none;background:var(--card);border:1px solid var(--line);
border-radius:4px;padding:.55rem .75rem;font-size:.82rem;max-width:29rem;
pointer-events:none;box-shadow:0 2px 12px rgba(0,0,0,.14);z-index:1000;line-height:1.5}
#tip .yi{direction:rtl;display:block;text-align:right;font-size:1rem}
#tip .ev{color:var(--soft);font-style:italic;direction:rtl;text-align:right;
display:block;margin-top:.3rem;font-family:"Frank Ruhl Libre",serif}
#tip .kv{font-family:"IBM Plex Mono",monospace;font-size:.68rem;color:var(--soft)}
"""

ENTRY_PANEL = """
<style>
#epanel{display:none;position:fixed;top:0;right:0;bottom:0;width:min(600px,94vw);
z-index:1200;background:var(--paper);border-left:1px solid var(--line);
box-shadow:-8px 0 28px rgba(0,0,0,.18);flex-direction:column}
#ep-head{padding:.9rem 1.1rem .6rem;border-bottom:1px solid var(--line);
display:flex;align-items:flex-start;gap:.8rem}
#ep-title{font-family:"Frank Ruhl Libre",serif;font-size:1.25rem;font-weight:700;
direction:rtl;text-align:right;flex:1;line-height:1.3}
#ep-meta{font-family:"IBM Plex Mono",monospace;font-size:.68rem;color:var(--soft,#5c554a);
padding:0 1.1rem .5rem;border-bottom:1px solid var(--line)}
#ep-close{border:1px solid var(--line);background:var(--card);color:var(--ink);
border-radius:4px;font-size:1rem;line-height:1;padding:.25rem .55rem;cursor:pointer}
#ep-text{overflow-y:auto;padding:1rem 1.2rem 2rem;direction:rtl;text-align:right;
font-family:"Frank Ruhl Libre",serif;font-size:1.04rem;line-height:1.9;
white-space:pre-wrap;flex:1}
#ep-text mark{background:#0e6f8a33;color:inherit;padding:0 .1em;border-radius:2px}
.entrylink{cursor:pointer}
.entrylink:hover{text-decoration:underline;text-decoration-color:var(--teal,#0e6f8a)}
</style>
<div id="epanel" role="dialog" aria-label="Leksikon entry">
  <div id="ep-head"><div id="ep-title"></div>
  <button id="ep-close" aria-label="close">✕</button></div>
  <div id="ep-meta"></div>
  <div id="ep-text"></div>
</div>
<script>
(function(){
let ENTRIES=null;
const nik='[\\\\u0591-\\\\u05C7]*';
const mk=b=>b.split('').join(nik);
const NAME=new RegExp('('+__NAMEBASES__.map(mk).join('|')+')(?:'+nik+'[א-ת])*','g');
window.openEntry=async function(pid){
  const p=document.getElementById('epanel');
  p.style.display='flex';
  document.getElementById('ep-title').textContent='…';
  document.getElementById('ep-meta').textContent=pid;
  document.getElementById('ep-text').textContent='';
  if(!ENTRIES){
    try{ENTRIES=await (await fetch('__SLUG___entry.json')).json();}
    catch(e){document.getElementById('ep-text').textContent=
      'Entry texts could not be loaded (__SLUG___entry.json).';return;}
  }
  const e=ENTRIES[pid];
  if(!e){document.getElementById('ep-text').textContent='Entry not found: '+pid;return;}
  document.getElementById('ep-title').textContent=e.h;
  document.getElementById('ep-meta').textContent='Leksikon vol. '+e.v+' · '+pid
    +(e.x?' · excerpts around each mention':' · full entry');
  const esc=e.t.replace(/&/g,'&amp;').replace(/</g,'&lt;');
  document.getElementById('ep-text').innerHTML=esc.replace(NAME,'<mark>$1</mark>');
};
const close=()=>document.getElementById('epanel').style.display='none';
document.getElementById('ep-close').onclick=close;
document.addEventListener('keydown',e=>{if(e.key==='Escape')close();});
})();
</script>"""

# ---------------- COMBINED COURSE PAGE (map + timeline, linked) ----------------
COURSE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__NAME__ · Life &amp; Career Course</title>""" + FONTS + """
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>__CSS__
#stage{max-width:82rem;margin:0 auto;padding:0 1.6rem 2.5rem}
#map{height:44vh;min-height:330px;border:1px solid var(--line);border-radius:4px}
#tlwrap{margin-top:.9rem;border:1px solid var(--line);border-radius:4px;
background:#fdfbf6;padding:.3rem 0 .2rem;overflow-x:auto}
svg text{font-family:"Spectral",serif}
.lane-label{font-size:12px;fill:var(--ink)}
.lane-label.region{fill:var(--soft);font-style:italic}
.lane-label.on{font-weight:700;fill:var(--teal)}
.band{opacity:.9;cursor:pointer}
.band.inf{opacity:.34}
.band.out{opacity:.10}
.band.on{stroke:var(--ink);stroke-width:1.6}
.evdot{stroke:var(--paper);stroke-width:1;cursor:pointer}
.evdot.out{opacity:.12}
.anch{cursor:pointer}
.gridline{stroke:var(--line);stroke-width:1}
.lanerule{stroke:var(--line);stroke-width:1;stroke-dasharray:2 4;opacity:.7}
.decade{font-size:10.5px;fill:var(--soft);font-family:"IBM Plex Mono",monospace}
.lifeline{stroke:var(--brick);stroke-width:1;stroke-dasharray:3 3;opacity:.6}
.lifelabel{font-size:10px;fill:var(--brick);font-family:"IBM Plex Mono",monospace}
#brushstrip{cursor:ew-resize}
.brushsel{fill:var(--teal);opacity:.10}
.brushedge{stroke:var(--teal);stroke-width:1.5}
.brushhint{font-size:10px;fill:var(--soft);font-family:"IBM Plex Mono",monospace}
.numicon{background:var(--teal);color:#fff;border-radius:50%;text-align:center;
font-family:"IBM Plex Mono",monospace;font-size:11px;line-height:20px;
border:2px solid var(--paper);box-shadow:0 1px 4px rgba(0,0,0,.3);
transition:transform .12s ease}
.numicon.inf{background:#9db8c2}
.numicon.kg{box-shadow:0 0 0 2px var(--amber)}
.numicon.on{transform:scale(1.5);z-index:900}
.leaflet-popup-content{font-family:"Spectral",serif;font-size:.86rem;
max-height:280px;overflow-y:auto}
.leaflet-popup-content .yi{direction:rtl;font-family:"Frank Ruhl Libre",serif}
.leaflet-popup-content .kv{font-family:"IBM Plex Mono",monospace;font-size:.7rem;color:#5c554a}
.leaflet-popup-content .ev{direction:rtl;text-align:right;font-style:italic;
color:#5c554a;display:block;margin-top:.35rem;font-family:"Frank Ruhl Libre",serif}
#range{font-family:"IBM Plex Mono",monospace;font-size:.74rem;color:var(--teal);
background:var(--card);border:1px solid var(--line);border-radius:3px;padding:.15rem .5rem}
</style></head><body>
<header>
<p class="eyebrow">Dybbuk · Person dossier · life &amp; career course</p>
<h1>__NAME__ — Life &amp; Career Course</h1>
<p class="lede">Where he was and when, in one view. The map and the timeline are the
same twenty stations: hover either and the other lights up. <b>Drag across the year
axis</b> to scrub a period — the map redraws to just that window, so the route becomes
temporal rather than a single tangle. Faded bars have dates inferred from narrative
order; diamonds are the KG's own dated facts, and amber rings mark stations the KG
independently corroborates.</p>
<p class="nav"><a href="__SLUG___dossier.html">← dossier</a>
<a href="__SLUG___network.html">network →</a></p>
</header>
<div class="controls">
  <label><input type="checkbox" id="inf" checked> date-inferred stations</label>
  <label><input type="checkbox" id="anch" checked> KG anchor facts</label>
  <label><input type="checkbox" id="evs" checked> events</label>
  <label><input type="checkbox" id="regions"> region/country centroids on the map</label>
  <span id="range">all years</span>
  <button id="clear">clear the year filter</button>
  <button onclick="openEntry('__PID__')">read the Leksikon entry</button>
  <span class="legend">
    <span><span class="sw" style="background:var(--teal)"></span> station</span>
    <span><span class="sw" style="background:var(--green)"></span> performance</span>
    <span><span class="sw" style="background:var(--amber)"></span> founding/business</span>
    <span><span class="sw" style="background:var(--brick)"></span> life event</span>
  </span>
  <span id="count" style="color:var(--soft)"></span>
</div>
<div id="stage">
  <div id="map"></div>
  <div id="tlwrap"><svg id="tl"></svg></div>
</div>
<div id="tip"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const P=__PAYLOAD__;
const Y0=P.tl.y0,Y1=P.tl.y1,LANE=25,LEFT=230,PAD=40;
const esc=s=>(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;');
const evColor=t=>["performance","premiere","season","debut"].includes(t)?"var(--green)"
  :["marriage","death","burial","born"].includes(t)?"var(--brick)"
  :["founding","business"].includes(t)?"var(--amber)":"#8a8577";

// shared state
let PXY=13, W=0, brush=null, focus=null;
const inRange=s=>!brush||((s.t1||s.t0)>=brush[0]&&s.t0<=brush[1]);
const x=y=>LEFT+(y-Y0)*PXY;
const xInv=px=>Y0+(px-LEFT)/PXY;

// ---- tooltip ----
const tip=document.getElementById('tip');
function showTip(html,e){tip.innerHTML=html;tip.style.display='block';
  tip.style.left=Math.min(e.clientX+14,innerWidth-470)+'px';
  tip.style.top=Math.min(e.clientY+12,innerHeight-40)+'px';}
const hideTip=()=>tip.style.display='none';
function stationTip(s){
  return `<b>${esc(s.en||'')}</b> <span class="yi">${esc(s.place)}</span>`
    +`<span class="kv">station ${s.seq} · ${esc(s.verb)}`
    +`${s.role?' · '+esc(s.role):''} · ${esc(s.ds)||'?'}–${esc(s.de)||'?'}`
    +`${s.inf?' (inferred '+s.t0+'–'+s.t1+')':''}${s.kg?' · KG-backed':''}</span>`
    +(s.org?`<br>with <span class="yi" style="display:inline">${esc(s.org)}</span>`:'')
    +(s.evid?`<span class="ev">${esc(s.evid)}</span>`:'');
}

// ---- map ----
const map=L.map('map').setView(P.mapCfg.center,P.mapCfg.zoom);
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
 {attribution:'© OpenStreetMap © CARTO',maxZoom:12}).addTo(map);
let layers=[], markerBySeq={}, fitted=false;

function drawMap(){
  layers.forEach(l=>map.removeLayer(l));layers=[];markerBySeq={};
  const useReg=document.getElementById('regions').checked;
  const useInf=document.getElementById('inf').checked;
  const useAnch=document.getElementById('anch').checked;
  const pts=P.stations.filter(s=>s.lat&&(useReg||s.res.startsWith('settlement'))
    &&(useInf||!s.inf)&&inRange(s));
  if(pts.length>1){
    const line=L.polyline(pts.map(s=>[s.lat,s.lon]),
      {color:'#0e6f8a',weight:2,opacity:.5,dashArray:'6 5'});
    line.addTo(map);layers.push(line);
  }
  pts.forEach(s=>{
    const cls='numicon'+(s.inf?' inf':'')+(s.kg?' kg':'');
    const m=L.marker([s.lat,s.lon],{icon:L.divIcon({className:'',
      html:`<div class="${cls}" data-seq="${s.seq}" style="width:20px;height:20px">${s.seq}</div>`,
      iconSize:[20,20],iconAnchor:[10,10]})});
    const evs=s.ev.map(e=>`<li><b>${esc(e.t)}</b> ${esc(e.date)||''} `
      +(e.play?`<span class="yi">${esc(e.play)}</span> `:'')
      +(e.venue?`<span class="yi">@${esc(e.venue)}</span> `:'')
      +`<br>${esc(e.d)}</li>`).join('');
    m.bindPopup(`<b>${s.seq}. ${esc(s.en||'?')}</b> <span class="yi">${esc(s.place)}</span>`
      +`<br><span class="kv">${esc(s.verb)}${s.role?' · '+esc(s.role):''} · `
      +`${esc(s.ds)||'?'}–${esc(s.de)||'?'}${s.inf?' (inferred '+s.t0+'–'+s.t1+')':''}`
      +`${s.kg?' · corroborated by the KG':''}</span>`
      +(s.org?`<br>with <span class="yi">${esc(s.org)}</span>`:'')
      +(evs?`<ol style="margin:.3rem 0 0;padding-left:1.1rem">${evs}</ol>`:'')
      +(s.evid?`<span class="ev">${esc(s.evid)}</span>`:''));
    m.on('mouseover',()=>setFocus(s.seq,false));
    m.on('mouseout',()=>setFocus(null,false));
    m.addTo(map);layers.push(m);markerBySeq[s.seq]=m;
  });
  let na=0;
  if(useAnch) P.anchors.forEach(a=>{
    if(!a.lat)return;
    if(brush&&a.y0&&!(a.y0<=brush[1]&&(a.y1||a.y0)>=brush[0]))return;
    na++;
    const col=a.verdict==='match'?'#7fb8c9':a.verdict==='conflict'?'#a1462e':'#b07d2b';
    const c=L.circleMarker([a.lat,a.lon],{radius:11,color:col,weight:2,
      fillOpacity:0,dashArray:'3 3'});
    c.bindPopup(`<b>KG fact — ${esc(a.verdict)}</b> <span class="kv">${esc(a.id)}</span>`
      +`<br>${esc(a.type)}${a.role?' · '+esc(a.role):''} `
      +`<span class="yi">${esc(a.yi||a.en)}</span>`
      +`<br><span class="kv">${esc(a.placeEn||a.placeYi)}`
      +`${a.y0?' · '+a.y0+(a.y1&&a.y1!=a.y0?'–'+a.y1:''):' · undated'}</span>`
      +(a.ev?`<span class="ev">${esc(a.ev)}</span>`:''));
    c.addTo(map);layers.push(c);
  });
  document.getElementById('count').textContent=
    `${pts.length} stations plotted${useAnch?' · '+na+' KG anchor places':''}`;
  if(pts.length){
    const b=L.latLngBounds(pts.map(s=>[s.lat,s.lon])).pad(.15);
    if(!fitted){map.fitBounds(b);fitted=true;}
    else if(brush)map.flyToBounds(b,{duration:.5});
  }
}

// ---- linked focus ----
function setFocus(seq,fromMap){
  focus=seq;
  document.querySelectorAll('#tl .band').forEach(el=>
    el.classList.toggle('on',+el.dataset.s===seq));
  document.querySelectorAll('#tl .lane-label').forEach(el=>
    el.classList.remove('on'));
  document.querySelectorAll('.numicon').forEach(el=>
    el.classList.toggle('on',+el.dataset.seq===seq));
  if(seq!=null){
    const s=P.stations.find(z=>z.seq===seq);
    if(s){const lab=document.querySelector(`#tl .lane-label[data-lane="${s.lane}"]`);
      if(lab)lab.classList.add('on');}
  }
}

// ---- timeline ----
function drawTimeline(){
  const wrap=document.getElementById('tlwrap');
  const avail=wrap.clientWidth-4;
  PXY=Math.max(8,(avail-LEFT-PAD)/(Y1-Y0));   // fit the century to the container
  W=LEFT+(Y1-Y0)*PXY+PAD;
  const showInf=document.getElementById('inf').checked;
  const showAnch=document.getElementById('anch').checked;
  const showEv=document.getElementById('evs').checked;
  const sts=P.stations.filter(s=>s.t0&&(showInf||!s.inf));
  const H=P.lanes.length*LANE+78;
  const svg=document.getElementById('tl');
  svg.setAttribute('width',W);svg.setAttribute('height',H);
  let g='';
  for(let y=Math.ceil(Y0/10)*10;y<=Y1;y+=10){
    g+=`<line class="gridline" x1="${x(y)}" y1="38" x2="${x(y)}" y2="${H-24}"/>`
      +`<text class="decade" x="${x(y)-14}" y="30">${y}</text>`;}
  [[P.life.date_born,'b. '+(P.life.date_born||'')],
   [P.life.date_died,'d. '+(P.life.date_died||'')]].forEach(([d,lab])=>{
    const yy=parseInt((d||'').slice(0,4));if(!yy)return;
    g+=`<line class="lifeline" x1="${x(yy)}" y1="38" x2="${x(yy)}" y2="${H-24}"/>`
      +`<text class="lifelabel" x="${x(yy)+3}" y="${H-12}">${lab}</text>`;});
  // brush strip along the axis
  g+=`<rect id="brushstrip" x="${LEFT}" y="8" width="${(Y1-Y0)*PXY}" height="26"
       fill="transparent"/>`;
  if(!brush) g+=`<text class="brushhint" x="${LEFT+6}" y="20">drag here to scrub a period</text>`;
  if(brush){
    const bx0=x(brush[0]),bx1=x(brush[1]);
    g+=`<rect class="brushsel" x="${bx0}" y="38" width="${bx1-bx0}" height="${H-62}"/>`
      +`<line class="brushedge" x1="${bx0}" y1="8" x2="${bx0}" y2="${H-24}"/>`
      +`<line class="brushedge" x1="${bx1}" y1="8" x2="${bx1}" y2="${H-24}"/>`;
  }
  P.lanes.forEach((l,i)=>{
    const yy=56+i*LANE;
    const cls=l.region?'lane-label region':'lane-label';
    const nm=(l.en||l.yi||'?')+(l.region?' (region)':'');
    g+=`<text class="${cls}" data-lane="${i}" x="${LEFT-10}" y="${yy+4}" text-anchor="end">${esc(nm).slice(0,32)}</text>`
      +`<text class="lane-label yi" x="${LEFT-10}" y="${yy+16}" text-anchor="end"
         style="font-size:10px;opacity:.65" direction="rtl">${esc(l.yi)}</text>`
      +`<line class="lanerule" x1="${LEFT}" y1="${yy}" x2="${W-PAD+10}" y2="${yy}"/>`;});
  sts.forEach(s=>{
    const yy=56+s.lane*LANE;
    const x0=x(s.t0),x1=Math.max(x(s.t1||s.t0),x0+5);
    const col=s.res.startsWith('region')?'#ddd6c4':'var(--teal)';
    const out=inRange(s)?'':' out';
    g+=`<rect class="band${s.inf?' inf':''}${out}" x="${x0}" y="${yy-7}" width="${x1-x0}"
      height="13" rx="3" fill="${col}" data-s="${s.seq}"/>`;
    if(showEv) s.ev.forEach((e,j)=>{
      const ex=e.date&&/^\\d{4}/.test(e.date)?x(+e.date.slice(0,4)):x0+10+j*9;
      g+=`<circle class="evdot${out}" cx="${ex}" cy="${yy}" r="4" fill="${evColor(e.t)}"
        data-s="${s.seq}" data-e="${j}"/>`;});
  });
  if(showAnch) P.anchors.forEach((a,i)=>{
    if(a.lane<0||!a.y0)return;
    const yy=56+a.lane*LANE, ax=x(a.y0);
    const fill=a.verdict==='match'?'var(--teal2)'
      :a.verdict==='conflict'?'var(--brick)':'var(--amber)';
    g+=`<rect class="anch" x="${ax-5}" y="${yy-16}" width="10" height="10"
      transform="rotate(45 ${ax} ${yy-11})" fill="${fill}" stroke="var(--paper)"
      stroke-width="1.2" data-a="${i}"/>`;});
  svg.innerHTML=g;
  wireTimeline(svg);
  if(focus!=null)setFocus(focus,false);
}

function wireTimeline(svg){
  svg.querySelectorAll('[data-s]').forEach(el=>{
    const s=P.stations.find(z=>z.seq==el.dataset.s);
    const html=el.dataset.e!==undefined?(()=>{const e=s.ev[+el.dataset.e];
      return `<b>${esc(e.t)}</b> ${esc(e.date)||''}`
        +(e.play?`<span class="yi">${esc(e.play)}</span>`:'')
        +(e.venue?`venue: <span class="yi">${esc(e.venue)}</span>`:'')
        +`<br>${esc(e.d)}<br><span class="kv">station ${s.seq} · ${esc(s.en||s.place)}</span>`;})()
      :stationTip(s);
    el.addEventListener('mousemove',e=>showTip(html,e));
    el.addEventListener('mouseenter',()=>setFocus(s.seq,true));
    el.addEventListener('mouseleave',()=>{hideTip();setFocus(null,true);});
    el.addEventListener('click',()=>{           // timeline -> map
      const m=markerBySeq[s.seq];
      if(m){map.panTo(m.getLatLng(),{animate:true});m.openPopup();}
    });
  });
  svg.querySelectorAll('[data-a]').forEach(el=>{
    const a=P.anchors[+el.dataset.a];
    const html=`<b>KG fact — ${esc(a.verdict)}</b> <span class="kv">${esc(a.id)}</span><br>`
      +`${esc(a.type)}${a.role?' · '+esc(a.role):''} `
      +`<span class="yi" style="display:inline">${esc(a.yi||a.en)}</span><br>`
      +`<span class="kv">${esc(a.placeEn||a.placeYi||'no place of its own')} · ${a.y0}${a.y1&&a.y1!=a.y0?'–'+a.y1:''}`
      +`${a.seqs.length?' · station '+a.seqs.join(', '):' · no matching station'}`
      +`${a.viaStation?' · shown on station '+a.viaStation+"'s lane":''}</span>`
      +(a.ev?`<span class="ev">${esc(a.ev)}</span>`:'');
    el.addEventListener('mousemove',e=>showTip(html,e));
    el.addEventListener('mouseleave',hideTip);});
  // brush drag
  const strip=svg.querySelector('#brushstrip');
  if(!strip)return;
  let anchor=null;
  const yearAt=ev=>{
    const r=svg.getBoundingClientRect();
    return Math.round(Math.min(Y1,Math.max(Y0,xInv(ev.clientX-r.left))));};
  strip.addEventListener('mousedown',ev=>{anchor=yearAt(ev);ev.preventDefault();});
  svg.addEventListener('mousemove',ev=>{
    if(anchor==null)return;
    const b=yearAt(ev);
    brush=[Math.min(anchor,b),Math.max(anchor,b)];
    if(brush[1]-brush[0]<1)return;
    applyRange();drawTimeline();});
  window.addEventListener('mouseup',()=>{
    if(anchor==null)return;
    anchor=null;
    if(brush&&brush[1]-brush[0]<2)brush=null;
    applyRange();drawTimeline();drawMap();});
}

function applyRange(){
  document.getElementById('range').textContent=
    brush?`${brush[0]}–${brush[1]}`:'all years';
}

function redrawAll(){applyRange();drawTimeline();drawMap();}
['inf','anch','evs','regions'].forEach(id=>
  document.getElementById(id).onchange=redrawAll);
document.getElementById('clear').onclick=()=>{brush=null;redrawAll();};
let rt;addEventListener('resize',()=>{clearTimeout(rt);rt=setTimeout(drawTimeline,180);});
redrawAll();
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = json.loads((HERE / args.config).read_text(encoding="utf-8"))
    slug = cfg["slug"]
    data = json.loads((HERE / f"{slug}_itinerary.json").read_text(encoding="utf-8"))
    P = build_payload(data)
    payload = json.dumps(P, ensure_ascii=False)
    panel = (ENTRY_PANEL
             .replace("__NAMEBASES__", json.dumps(cfg["name_regex_base"], ensure_ascii=False))
             .replace("__SLUG__", slug))

    html = (COURSE.replace("__CSS__", COMMON_CSS)
                  .replace("__PAYLOAD__", payload)
                  .replace("__NAME__", cfg["display_name"])
                  .replace("__PID__", cfg["entry_person_id"])
                  .replace("__SLUG__", slug)
                  .replace("</body></html>", panel + "\n</body></html>"))
    out = DOCS / f"{slug}_course.html"
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out.name} — {out.stat().st_size/1024:.0f} KB")

    # the combined page replaces the two separate ones
    for old in (f"{slug}_timeline.html", f"{slug}_map.html"):
        p = DOCS / old
        if p.exists():
            p.unlink()
            print(f"removed superseded {old}")

    print(f"stations: {len(P['stations'])}  lanes(places): {len(P['lanes'])}  "
          f"anchors: {len(P['anchors'])}  third-person itineraries: {len(P['others'])}")


if __name__ == "__main__":
    main()
