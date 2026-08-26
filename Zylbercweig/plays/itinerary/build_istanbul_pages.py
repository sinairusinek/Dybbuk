#!/usr/bin/env python3.11
"""Build docs/istanbul_timeline.html and docs/istanbul_map.html from
istanbul_itineraries.json (embeds the data; no server needed)."""
import json, re
from pathlib import Path

HERE = Path(__file__).resolve().parent
DOCS = HERE.parent.parent.parent / "docs"
DATA = json.load(open(HERE / "istanbul_itineraries.json", encoding="utf-8"))

IST_RE = re.compile(r"(קאנסטאנטינאפ|סטאמבול|סטאמבל|איסטאמבול|סטומבול)")
POINTS = re.compile(r"[֑-ׇ]")


def is_ist(st):
    return st.get("qid") == "Q406" or bool(IST_RE.search(POINTS.sub("", st.get("place", ""))))


# the Leksikon repeats these people across volumes; keep the richer extraction
DUP_GROUPS = [
    {"P-1-facs_35_tr_1740520972", "P-5-facs_556_tr_1739222579"},      # Katia Adler
    {"P-1-facs_64_tr_1740520942", "P-5-facs_126_tr_1733832830"},      # Ch.-D. Ariel
    {"P-1-facs_64_tr_1740520959", "P-5-facs_560_tr_1739220843"},      # Rokhl Ariel
    {"P-2-facs_194_tr_1744279147", "P-5-facs_430_r"},                 # Leresko
    {"P-2-facs_292_tr_1744280734", "P-5-facs_568_tr_1739221639"},     # Mendelevitsh
    {"P-1-facs_335_r_6", "P-5-facs_250_tr_1739221688"},               # Hershele
    {"P-1-facs_50_tr_1740521040", "P-5-facs_568_r_2"},                # Sonia Amatin
]

# ---- prepare compact viz payload ----
itins = []
for it in DATA:
    sts = it["stations"]
    ist_years = [s["t_start"] for s in sts if is_ist(s) and s.get("t_start")]
    itins.append({
        "pid": it["person_id"], "name": POINTS.sub("", it["heading"]),
        "vol": it["volume"], "subject": it["subject"],
        "entrySubject": it["is_entry_subject"],
        "istYear": min(ist_years) if ist_years else None,
        "stations": [{
            "seq": s["seq"], "place": POINTS.sub("", s["place"]), "verb": s["verb"],
            "org": POINTS.sub("", s.get("org", "")), "role": s.get("role", ""),
            "ds": s["date_start"], "de": s["date_end"], "cert": s["certainty"],
            "t0": s["t_start"], "t1": s["t_end"], "inf": bool(s["t_inferred"]),
            "res": s["res_status"], "en": s["label_en"], "lat": s["lat"], "lon": s["lon"],
            "ist": is_ist(s),
            "ev": [{"t": e.get("event_type", ""), "play": POINTS.sub("", e.get("play", "") or ""),
                    "venue": POINTS.sub("", e.get("venue", "") or ""), "date": e.get("date", ""),
                    "d": e.get("description", "")} for e in s["events"]],
        } for s in sts],
    })

# collapse cross-volume duplicate entries: keep the richer entry-subject itinerary
for group in DUP_GROUPS:
    cand = [i for i in itins if i["pid"] in group and i["subject"] == "entry"]
    if len(cand) > 1:
        cand.sort(key=lambda i: -len(i["stations"]))
        keep, drop = cand[0], cand[1:]
        keep["name"] += f" (vols {'+'.join(sorted(c['vol'] for c in cand))})"
        for d in drop:
            itins.remove(d)

for i in itins:
    i["hasIst"] = any(s["ist"] for s in i["stations"])

payload = json.dumps(itins, ensure_ascii=False)

COMMON_CSS = """
:root{--paper:#faf7f0;--ink:#272219;--soft:#5c554a;--line:#e2dcce;--card:#f2eddf;
--teal:#0e6f8a;--teal2:#7fb8c9;--brick:#a1462e;--amber:#b07d2b;--grey:#c9c2b2}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);
font-family:"Spectral",Georgia,serif;font-size:15px}
header{padding:1.6rem 1.6rem .6rem;max-width:75rem;margin:0 auto}
h1{font-family:"Fraunces",serif;font-weight:600;font-size:1.9rem;margin:.1rem 0 .3rem}
.eyebrow{font-family:"IBM Plex Mono",monospace;font-size:.68rem;letter-spacing:.13em;
text-transform:uppercase;color:var(--teal);margin:0}
.lede{color:var(--soft);margin:.2rem 0 .6rem;max-width:46rem}
.nav{font-size:.85rem}.nav a{color:var(--teal);margin-right:1rem}
.yi{font-family:"Frank Ruhl Libre",serif;unicode-bidi:isolate}
.controls{display:flex;flex-wrap:wrap;gap:.9rem;align-items:center;padding:.5rem 1.6rem;
max-width:75rem;margin:0 auto;font-size:.85rem}
.controls label{display:flex;gap:.3rem;align-items:center;cursor:pointer}
.legend{display:inline-flex;gap:.9rem;flex-wrap:wrap;font-size:.8rem;color:var(--soft)}
.sw{display:inline-block;width:.75em;height:.75em;border-radius:50%;vertical-align:-1px}
"""

FONTS = ('<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
         'family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&'
         'family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500'
         '&display=swap">')

# shared entry-reader side panel (fetches docs/istanbul_entries.json lazily)
ENTRY_PANEL = """
<style>
#epanel{display:none;position:fixed;top:0;right:0;bottom:0;width:min(580px,94vw);
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
const IST=new RegExp('('+['קאנסטאנטינאפ','סטאמבול','סטאמבל','איסטאמבול','סטומבול']
  .map(mk).join('|')+')(?:'+nik+'[א-ת])*','g');
window.openEntry=async function(pid){
  const p=document.getElementById('epanel');
  p.style.display='flex';
  document.getElementById('ep-title').textContent='…';
  document.getElementById('ep-meta').textContent=pid;
  document.getElementById('ep-text').textContent='';
  if(!ENTRIES){
    try{ENTRIES=await (await fetch('istanbul_entries.json')).json();}
    catch(e){document.getElementById('ep-text').textContent=
      'Entry texts could not be loaded (istanbul_entries.json).';return;}
  }
  let e=ENTRIES[pid];
  if(!e && pid.includes('…')){
    const rx=new RegExp('^'+pid.split('…').map(s=>s.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\\\$&')).join('.*')+'$');
    const k=Object.keys(ENTRIES).find(k=>rx.test(k));
    if(k)e=ENTRIES[k],pid=k;
  }
  if(!e){document.getElementById('ep-text').textContent='Entry not found: '+pid;return;}
  document.getElementById('ep-title').textContent=e.h;
  document.getElementById('ep-meta').textContent='Leksikon vol. '+e.v+' · '+pid;
  const esc=e.t.replace(/&/g,'&amp;').replace(/</g,'&lt;');
  document.getElementById('ep-text').innerHTML=esc.replace(IST,'<mark>$1</mark>');
};
const close=()=>document.getElementById('epanel').style.display='none';
document.getElementById('ep-close').onclick=close;
document.addEventListener('keydown',e=>{if(e.key==='Escape')close();});
})();
</script>"""

# ---------------- TIMELINE ----------------
timeline = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Istanbul Timeline · Yiddish Theater</title>""" + FONTS + """
<style>""" + COMMON_CSS + """
#wrap{max-width:75rem;margin:0 auto;padding:0 1.6rem 3rem;overflow-x:auto}
svg text{font-family:"Spectral",serif}
.lane-label{font-size:11.5px;fill:var(--ink)}
.lane-label.third{fill:var(--soft);font-style:italic}
.band{opacity:.85}.band.inf{opacity:.38}
.evdot{stroke:var(--paper);stroke-width:1;cursor:pointer}
.gridline{stroke:var(--line);stroke-width:1}
.decade{font-size:10.5px;fill:var(--soft);font-family:"IBM Plex Mono",monospace}
#tip{position:fixed;display:none;background:var(--card);border:1px solid var(--line);
border-radius:4px;padding:.5rem .7rem;font-size:.8rem;max-width:26rem;pointer-events:none;
box-shadow:0 2px 10px rgba(0,0,0,.12);z-index:10}
#tip .yi{direction:rtl;display:block;text-align:right;font-size:.95rem}
</style></head><body>
<header>
<p class="eyebrow">Dybbuk · Istanbul itinerary pilot</p>
<h1>Istanbul Timeline</h1>
<p class="lede">Every extracted station and event for the 79 Leksikon entries that mention
Istanbul. Teal bands = time in Istanbul; grey bands = elsewhere; faded = date inferred
from narrative order. Dots are events.</p>
<p class="nav"><a href="istanbul_dossier.html">← dossier</a>
<a href="istanbul_map.html">map →</a></p>
</header>
<div class="controls">
  <label><input type="checkbox" id="istOnly"> Istanbul stations &amp; events only</label>
  <label><input type="checkbox" id="thirdP" checked> include third-person testimony lanes</label>
  <label><input type="checkbox" id="mentOnly"> include mention-only careers (no Istanbul station)</label>
  <span class="legend">
    <span><span class="sw" style="background:var(--teal)"></span> performance/season</span>
    <span><span class="sw" style="background:var(--brick)"></span> life event</span>
    <span><span class="sw" style="background:var(--amber)"></span> founding/business</span>
    <span><span class="sw" style="background:#8a8577"></span> other</span>
  </span>
</div>
<div id="wrap"><svg id="tl"></svg></div>
<div id="tip"></div>
<script>
const DATA = __PAYLOAD__;
const Y0=1845,Y1=1945,PXY=10.4,LANE=17,LEFT=215,W=LEFT+(Y1-Y0)*PXY+30;
const evColor = t => ["performance","premiere","season","debut"].includes(t) ? "var(--teal)"
  : ["marriage","death","burial","born"].includes(t) ? "var(--brick)"
  : ["founding","business"].includes(t) ? "var(--amber)" : "#8a8577";
const x = y => LEFT+(y-Y0)*PXY;
const tip=document.getElementById('tip');
function showTip(html,e){tip.innerHTML=html;tip.style.display='block';
  tip.style.left=Math.min(e.clientX+14,innerWidth-420)+'px';tip.style.top=(e.clientY+12)+'px';}
function render(){
  const istOnly=document.getElementById('istOnly').checked;
  const thirdP=document.getElementById('thirdP').checked;
  const mentOnly=document.getElementById('mentOnly').checked;
  let lanes=DATA.filter(d=>d.entrySubject||thirdP)
    .filter(d=>d.hasIst||mentOnly)
    .filter(d=>d.stations.some(s=>s.t0&&(!istOnly||s.ist)))
    .sort((a,b)=>(a.istYear||9999)-(b.istYear||9999));
  const H=lanes.length*LANE+60;
  const svg=document.getElementById('tl');
  svg.setAttribute('width',W);svg.setAttribute('height',H);
  let g='';
  for(let y=Math.ceil(Y0/10)*10;y<=Y1;y+=10){
    g+=`<line class="gridline" x1="${x(y)}" y1="28" x2="${x(y)}" y2="${H-20}"/>`+
       `<text class="decade" x="${x(y)-14}" y="20">${y}</text>`;}
  lanes.forEach((d,i)=>{
    const yy=40+i*LANE;
    const cls=d.entrySubject?'lane-label':'lane-label third';
    const nm=d.entrySubject?d.name:(d.subject+' ⟨per '+d.name.split(',')[0]+'⟩');
    g+=`<text class="${cls} entrylink" data-pid="${d.pid}" x="${LEFT-8}" y="${yy+4}" text-anchor="end">${nm.slice(0,34)}</text>`;
    d.stations.filter(s=>s.t0&&(!istOnly||s.ist)).forEach(s=>{
      const x0=x(s.t0),x1=Math.max(x(s.t1||s.t0),x0+3);
      const col=s.ist?'var(--teal)':(s.res.startsWith('region')?'#ddd6c4':'var(--grey)');
      g+=`<rect class="band${s.inf?' inf':''}" x="${x0}" y="${yy-5}" width="${x1-x0}" height="9" rx="2" fill="${col}"
        data-t="<b>${s.en||''}</b> <span class='yi'>${s.place}</span><br>${s.verb}${s.org?' · <span class=yi>'+s.org+'</span>':''}
        <br>${s.ds||'?'}–${s.de||'?'} ${s.inf?'(inferred '+s.t0+'–'+s.t1+')':''}"/>`;
      s.ev.forEach(e=>{
        if(istOnly&&!s.ist)return;
        const ex=e.date&&/^\\d{4}/.test(e.date)?x(+e.date.slice(0,4)):(x0+x1)/2;
        g+=`<circle class="evdot" cx="${ex}" cy="${yy}" r="3.4" fill="${evColor(e.t)}"
          data-t="<b>${e.t}</b> ${e.date||''}<br>${e.play?'<span class=yi>'+e.play+'</span><br>':''}${e.venue?'venue: <span class=yi>'+e.venue+'</span><br>':''}${e.d||''}<br><i>${s.en||s.place} ${s.t0||''}</i>"/>`;
      });
    });
  });
  svg.innerHTML=g;
  svg.querySelectorAll('[data-t]').forEach(el=>{
    el.addEventListener('mousemove',e=>showTip(el.dataset.t,e));
    el.addEventListener('mouseleave',()=>tip.style.display='none');});
  svg.querySelectorAll('text[data-pid]').forEach(el=>{
    el.addEventListener('click',()=>openEntry(el.dataset.pid));});
}
document.getElementById('istOnly').onchange=render;
document.getElementById('thirdP').onchange=render;
document.getElementById('mentOnly').onchange=render;
render();
</script></body></html>"""

# ---------------- MAP ----------------
mapp = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Istanbul Routes · Yiddish Theater</title>""" + FONTS + """
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>""" + COMMON_CSS + """
#map{height:calc(100vh - 175px);min-height:430px}
.leaflet-popup-content{font-family:"Spectral",serif;font-size:.85rem;max-height:260px;overflow-y:auto}
.leaflet-popup-content .yi{direction:rtl}
.stop-list{margin:.3rem 0 0;padding-left:1rem}
.stop-list li.ist-stop{background:#0e6f8a1f;border-left:3px solid var(--teal);
margin-left:-.5rem;padding:.1rem .25rem .1rem .4rem;border-radius:0 3px 3px 0;font-weight:600}
.leaflet-tooltip.tip-ist{border-color:var(--teal);border-width:1.5px;font-weight:600}
.ist-tip{color:var(--teal)}
.ist-pulse{animation:istpulse 1.1s ease-out 2}
@keyframes istpulse{0%{r:6;stroke-width:2}50%{r:13;stroke-width:5}100%{r:6;stroke-width:2}}
@media (prefers-reduced-motion:reduce){.ist-pulse{animation:none}}
</style></head><body>
<header style="padding-bottom:.3rem">
<p class="eyebrow">Dybbuk · Istanbul itinerary pilot</p>
<h1>Istanbul Routes</h1>
<p class="nav"><a href="istanbul_dossier.html">← dossier</a>
<a href="istanbul_timeline.html">timeline →</a>
&nbsp;·&nbsp; routes of the 79 Istanbul-linked careers; click a line for its stations</p>
</header>
<div class="controls">
  <label><input type="checkbox" id="w1" checked><span class="sw" style="background:#b07d2b"></span> first in Istanbul before 1890</label>
  <label><input type="checkbox" id="w2" checked><span class="sw" style="background:#0e6f8a"></span> 1890–1913</label>
  <label><input type="checkbox" id="w3" checked><span class="sw" style="background:#a1462e"></span> 1914+</label>
  <label><input type="checkbox" id="w0" checked><span class="sw" style="background:#8a8577"></span> undated</label>
  <label><input type="checkbox" id="regions"> include region/country centroids in routes</label>
  <span id="count" style="color:var(--soft)"></span>
</div>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const DATA = __PAYLOAD__;
const map=L.map('map').setView([44,24],4);
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
 {attribution:'© OpenStreetMap © CARTO',maxZoom:12}).addTo(map);
const IST=[41.010,28.960];
const istMarker=L.circleMarker(IST,{radius:9,color:'#0e6f8a',weight:3,
  fillColor:'#0e6f8a',fillOpacity:.55}).addTo(map);
istMarker.bindTooltip('Istanbul / קאָנסטאַנטינאָפּאָל',
  {permanent:true,direction:'right',offset:[10,0],className:'tip-ist'});
// emphasize Istanbul whenever the reader hovers a station or route
let istTimer=null;
function flashIst(){
  const el=istMarker.getElement(); if(!el)return;
  istMarker.setStyle({radius:12,weight:5,fillOpacity:.75});
  el.classList.add('ist-pulse');
  clearTimeout(istTimer);
  istTimer=setTimeout(()=>{
    istMarker.setStyle({radius:9,weight:3,fillOpacity:.55});
    el.classList.remove('ist-pulse');},1400);
}
const wave=d=>!d.istYear?'w0':d.istYear<1890?'w1':d.istYear<1914?'w2':'w3';
const wcol={w1:'#b07d2b',w2:'#0e6f8a',w3:'#a1462e',w0:'#8a8577'};
let layers=[];
function render(){
  layers.forEach(l=>map.removeLayer(l));layers=[];
  const useReg=document.getElementById('regions').checked;
  const on=w=>document.getElementById(w).checked;
  let n=0;
  DATA.filter(d=>d.entrySubject&&d.hasIst).forEach(d=>{
    const w=wave(d);if(!on(w))return;
    const pts=d.stations.filter(s=>s.lat&&(useReg||s.res.startsWith('settlement')));
    if(pts.length<2)return;n++;
    const latlngs=pts.map(s=>[s.lat,s.lon]);
    const line=L.polyline(latlngs,{color:wcol[w],weight:1.8,opacity:.55});
    const stops=d.stations.map(s=>`<li${s.ist?' class="ist-stop"':''}>${s.en||'?'} <span class="yi">${s.place}</span>`
      +` <small>${s.verb}${s.t0?' '+s.t0+(s.t1&&s.t1!==s.t0?'–'+s.t1:''):''}${s.inf?'~':''}</small></li>`).join('');
    line.bindPopup(`<b class="entrylink" style="color:var(--teal)" onclick="openEntry('${d.pid}')">${d.name}</b> <small>vol ${d.vol} · click name for the entry</small><ol class="stop-list">${stops}</ol>`);
    line.on('mouseover',e=>{e.target.setStyle({weight:4,opacity:.95});flashIst();});
    line.on('mouseout',e=>e.target.setStyle({weight:1.8,opacity:.55}));
    line.addTo(map);layers.push(line);
    pts.forEach(s=>{
      const m=L.circleMarker([s.lat,s.lon],{radius:s.ist?5:(s.verb==='pass_through'?2:3),
        color:wcol[w],weight:1,fillColor:wcol[w],
        fillOpacity:s.verb==='pass_through'?0.15:0.7});
      m.bindTooltip(`${s.ist?'<b class="ist-tip">★ Istanbul</b> — ':''}`
        +`${s.ist?'':(s.en||s.place)+' — '}${d.name.split(',')[0]} <i>${s.verb}</i> ${s.t0||''}`,
        {className:s.ist?'tip-ist':''});
      if(s.ist)m.bringToFront();
      m.on('mouseover',()=>flashIst());
      m.addTo(map);layers.push(m);
    });
  });
  document.getElementById('count').textContent=n+' routes drawn';
}
['w0','w1','w2','w3','regions'].forEach(id=>document.getElementById(id).onchange=render);
render();
</script></body></html>"""

timeline = timeline.replace("__PAYLOAD__", payload).replace(
    "</body></html>", ENTRY_PANEL + "\n</body></html>")
mapp = mapp.replace("__PAYLOAD__", payload).replace(
    "</body></html>", ENTRY_PANEL + "\n</body></html>")
(DOCS / "istanbul_timeline.html").write_text(timeline, encoding="utf-8")
(DOCS / "istanbul_map.html").write_text(mapp, encoding="utf-8")
print("wrote", DOCS / "istanbul_timeline.html", "and istanbul_map.html")
print("itineraries in payload:", len(itins),
      " with coords:", sum(1 for i in itins if any(s["lat"] for s in i["stations"])))
