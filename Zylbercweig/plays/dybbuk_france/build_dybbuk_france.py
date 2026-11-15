#!/usr/bin/env python3.11
"""Build docs/Visualizations/dybbuk_france.html (+ dybbuk_france_entries.json) from curated.py,
and dybbuk_france_facts.tsv (review table).

    python3.11 build_dybbuk_france.py
"""
import csv, json, re, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from lexicon_ref import ref  # noqa: E402
from curated import FACTS, PEOPLE, TROUPES, PLACES, VENUES, ROUTES, CITY  # noqa: E402
from anchors import load_texts, resolve, norm  # noqa: E402

DOCS = HERE.parents[2] / "docs" / "Visualizations"
POINTS = re.compile(r"[֑-ׇ]")
STEM = re.compile(r"דיבוק|דבוק|דיבעק|דבעק|צווישן צוויי וועלטן")
FR = re.compile(r"פאריז|פראנקרייך|(?<!\S)מעץ|עלזאס|לאטרינגען|שטראסבורג|(?<![א-ת])נאנסי|פראנצויז|פראנצעז")
ROW = {"yat_1924": "t:yat", "yat_dybbuk": "t:yat", "schwartz_spanish": "t:yat", "vilna_tour": "t:vilna",
       "vilna_premiere": "t:vilna", "habima_1926": "t:habima", "altman_sets": "altman", "madiokat_1929": "t:madiokat",
       "kadish_dybbuk": "kadish", "kadish_france": "kadish", "kompaneets_krakow": "kompaneets",
       "kompaneets_paris": "kompaneets", "lampe_paris": "lampe", "three_dybbuks": "lampe",
       "friedman_belgium": "friedman", "zucker_paris": "zucker", "koerner": "koerner", "blumenfeld": "blumenfeld"}
WORKNODE = {"madiokat_1929": "w:parody_maud", "three_dybbuks": "w:parody_three", "lampe_paris": "w:parody_three",
            "koerner": "w:french", "blumenfeld": "w:french", "halpern_film": "w:film", "halpern_paris": "w:film",
            "weintraub_opera": "w:opera", "weintraub_paris": "w:opera"}


def main():
    ids = {p for f in FACTS for p, _ in f["src"]}
    T = load_texts(ids)
    facts = []
    for f in FACTS:
        src = []
        for pid, anc in f["src"]:
            segs = resolve(T[pid]["entry_text"], anc)
            r = ref(pid, segs[0])
            src.append({"pid": pid, "segs": segs, "vol": r["vol"], "cols": r["cols"], "url": r.get("url", "")})
        g = dict(f, src=src, people=[{"k": k, "role": ro, "ch": ch} for k, ro, ch in f["people"]])
        g["row"] = ROW.get(f["id"], f["people"][0][0])
        g["wnode"] = WORKNODE.get(f["id"], "w:" + f["work"])
        g.setdefault("venue", None)
        g.setdefault("note", "")
        facts.append(g)

    # entry panel: windows around Dybbuk/French mentions (full text if short)
    panel = {}
    for pid in sorted(ids):
        t = T[pid]["entry_text"]
        u = POINTS.sub("", t)
        h = POINTS.sub("", T[pid]["heading"])
        if len(t) <= 5000:
            panel[pid] = {"h": h, "v": T[pid]["volume"], "t": t}
            continue
        # windows on the pointed text via the unpointed index (lengths differ: rebuild map)
        mp = [i for i, c in enumerate(t) if not POINTS.match(c)]
        spans = []
        for m in list(STEM.finditer(u)) + list(FR.finditer(u)):
            a, b = mp[max(0, m.start() - 700)], mp[min(len(u) - 1, m.end() + 700)]
            spans.append([a, b])
        spans.sort()
        merged = []
        for a, b in spans:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(b, merged[-1][1])
            else:
                merged.append([a, b])
        panel[pid] = {"h": h, "v": T[pid]["volume"], "x": 1,
                      "t": "\n\n· · ·\n\n".join(t[a:b + 1] for a, b in merged)}
    (DOCS / "dybbuk_france_entries.json").write_text(json.dumps(panel, ensure_ascii=False), encoding="utf-8")

    heads = {pid: POINTS.sub("", T[pid]["heading"]) for pid in ids}
    data = {
        "facts": facts, "heads": heads,
        "people": {k: {"en": a, "yi": b, "pid": c} for k, (a, b, c) in PEOPLE.items()},
        "troupes": {k: {"en": a, "yi": b, "pid": c} for k, (a, b, c) in TROUPES.items()},
        "places": {k: {"en": a, "lat": b, "lon": c} for k, (a, b, c) in PLACES.items()},
        "venues": {k: {"en": a, "lat": b, "lon": c} for k, (a, b, c) in VENUES.items()},
        "routes": [{"k": k, "label": lab, "fact": fid, "note": note,
                    "pts": [[c, *CITY[c]] for c in cities]} for k, lab, fid, cities, note in ROUTES],
    }
    n_played = sum(1 for f in facts if f["tier"] == "played")
    n_tour = sum(1 for f in facts if f["tier"] == "tour")
    n_people = len({p["k"] for f in facts for p in f["people"]})
    towns = {p for f in facts if f["tier"] in ("played", "tour", "french") for p in f["places"] if p != "france"}
    stats = {"entries": len(ids), "facts": len(facts), "played": n_played, "tour": n_tour,
             "people": n_people, "towns": len(towns)}
    html = TEMPLATE.replace("%%DATA%%", json.dumps(data, ensure_ascii=False).replace("</", "<\\/"))
    for k, v in stats.items():
        html = html.replace("%%" + k.upper() + "%%", str(v))
    (DOCS / "dybbuk_france.html").write_text(html, encoding="utf-8")

    with open(HERE / "dybbuk_france_facts.tsv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["id", "tier", "work", "label", "date", "places", "troupe", "people", "person_id", "vol", "cols", "scan", "quote"])
        for f in facts:
            for s in f["src"]:
                w.writerow([f["id"], f["tier"], f["work"], f["label"], f["date"], ",".join(f["places"]), f["troupe"] or "",
                            "; ".join(f"{p['k']}:{p['role']}" + (f"({p['ch']})" if p["ch"] else "") for p in f["people"]),
                            s["pid"], s["vol"], s["cols"], s["url"], " … ".join(s["segs"])])
    print(stats, f"html {(DOCS / 'dybbuk_france.html').stat().st_size:,} B,",
          f"entries {(DOCS / 'dybbuk_france_entries.json').stat().st_size:,} B")


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>The Dybbuk in France</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&family=Spectral:wght@400;600&family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
:root{
  --paper:#faf7f0; --ink:#272219; --ink-soft:#5c554a; --line:#e2dcce; --card:#f2eddf;
  --accent:#0e6f8a; --accent-ink:#0b5a70; --mono-bg:#ece7d8; --chip:#e4ecec;
  --played:#a1462e; --tour:#b7791f; --french:#6b4fa3; --stay:#0e6f8a; --elsewhere:#8a8274;
  --r-vilna:#a1462e; --r-habima:#2f7d4f; --r-madiokat:#b7791f; --r-vilna2:#c0587e; --r-yat:#8a8274;
  --work:#272219; --troupe:#b7791f; --place:#3d6fb4;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --paper:#161a1e; --ink:#e7e1d3; --ink-soft:#a49c8c; --line:#2c333a; --card:#1d2329;
    --accent:#5cb6cd; --accent-ink:#7fc6d8; --mono-bg:#20272d; --chip:#223034;
    --played:#e08a6a; --tour:#e2b04a; --french:#a98ee0; --stay:#5cb6cd; --elsewhere:#8f897d;
    --r-vilna:#e08a6a; --r-habima:#6cc08f; --r-madiokat:#e2b04a; --r-vilna2:#e58bb0; --r-yat:#8f897d;
    --work:#e7e1d3; --troupe:#e2b04a; --place:#7fa6e6;
  }
}
:root[data-theme="dark"]{
    --paper:#161a1e; --ink:#e7e1d3; --ink-soft:#a49c8c; --line:#2c333a; --card:#1d2329;
    --accent:#5cb6cd; --accent-ink:#7fc6d8; --mono-bg:#20272d; --chip:#223034;
    --played:#e08a6a; --tour:#e2b04a; --french:#a98ee0; --stay:#5cb6cd; --elsewhere:#8f897d;
    --r-vilna:#e08a6a; --r-habima:#6cc08f; --r-madiokat:#e2b04a; --r-vilna2:#e58bb0; --r-yat:#8f897d;
    --work:#e7e1d3; --troupe:#e2b04a; --place:#7fa6e6;
}
*{box-sizing:border-box}
body{background:var(--paper); color:var(--ink); font-family:"Spectral",Georgia,serif; font-size:1.02rem;
  line-height:1.6; margin:0; padding:2.6rem 16px 5rem}
main{max-width:68rem; margin:0 auto}
h1,h2,h3{font-family:"Fraunces","Spectral",Georgia,serif; line-height:1.15; font-weight:600; text-wrap:balance}
h1{font-size:clamp(2rem,6vw,2.8rem); margin:.3rem 0 .5rem}
h2{font-size:1.4rem; margin:0 0 .3rem}
p{max-width:44rem}
a{color:var(--accent-ink)}
.eyebrow{font-family:"IBM Plex Mono",monospace; font-size:.72rem; letter-spacing:.14em; text-transform:uppercase; color:var(--accent-ink); margin:0}
.lede{font-size:1.1rem; color:var(--ink-soft); max-width:44rem; margin-top:0}
.yi{font-family:"Frank Ruhl Libre","Spectral",serif; unicode-bidi:isolate}
section{border-top:1px solid var(--line); padding-top:1.5rem; margin-top:2.2rem}
.sub{color:var(--ink-soft); font-size:.92rem; margin:.1rem 0 1rem}
.statrow{display:flex; flex-wrap:wrap; gap:.7rem; margin:1.4rem 0 .6rem}
.stat{background:var(--card); border:1px solid var(--line); border-radius:4px; padding:.5rem .85rem; min-width:6.6rem}
.stat b{display:block; font-family:"Fraunces",serif; font-size:1.45rem; font-weight:600; color:var(--accent-ink); font-variant-numeric:tabular-nums}
.stat span{font-size:.76rem; color:var(--ink-soft)}
.legend{display:flex; flex-wrap:wrap; gap:.4rem 1rem; font-family:"IBM Plex Mono",monospace; font-size:.72rem; color:var(--ink-soft); margin:.4rem 0 .8rem}
.legend i{display:inline-block; width:.8rem; height:.8rem; border-radius:2px; vertical-align:-.1rem; margin-right:.35rem}
.legend i.hollow{background:transparent!important; border:2px solid var(--elsewhere)}
.panel{background:var(--card); border:1px solid var(--line); border-radius:6px}
#tl{overflow-x:auto; -webkit-overflow-scrolling:touch}
#tl svg{display:block}
#tl text{font-family:"Spectral",serif; fill:var(--ink)}
#tl .ax{font-family:"IBM Plex Mono",monospace; font-size:10px; fill:var(--ink-soft)}
#tl .mk{cursor:pointer}
#tl .rowlab{cursor:pointer}
#tl .rowlab:hover{fill:var(--accent-ink)}
#map{height:min(70vh,560px); border-radius:6px; border:1px solid var(--line)}
.leaflet-container{background:var(--card); font-family:"Spectral",serif}
.leaflet-popup-content{font-size:.9rem; line-height:1.4}
.maptools{display:flex; flex-wrap:wrap; gap:.5rem; margin:.6rem 0}
button,.btn{font-family:"IBM Plex Mono",monospace; font-size:.74rem; background:var(--card); color:var(--ink);
  border:1px solid var(--line); border-radius:3px; padding:.3rem .6rem; cursor:pointer}
button:hover{border-color:var(--accent)}
button[aria-pressed="false"]{opacity:.5}
#net{height:min(78vh,640px)}
#tip{position:fixed; pointer-events:none; background:var(--paper); color:var(--ink); border:1px solid var(--line);
  border-radius:4px; padding:.35rem .55rem; font-size:.82rem; max-width:20rem; box-shadow:0 2px 10px rgba(0,0,0,.15); display:none; z-index:2000}
/* drawer */
#drawer{position:fixed; top:0; right:0; height:100%; width:min(30rem,100%); background:var(--paper); border-left:1px solid var(--line);
  box-shadow:-6px 0 24px rgba(0,0,0,.18); transform:translateX(105%); transition:transform .22s ease; z-index:3000; display:flex; flex-direction:column}
#drawer.open{transform:none}
#drawer header{display:flex; justify-content:space-between; align-items:flex-start; gap:.6rem; padding:1rem 1rem .6rem; border-bottom:1px solid var(--line)}
#drawer header h3{margin:0; font-size:1.15rem}
#dbody{overflow-y:auto; padding:.4rem 1rem 2rem}
.card{border:1px solid var(--line); border-radius:5px; background:var(--card); padding:.7rem .85rem; margin:.8rem 0}
.card .top{display:flex; flex-wrap:wrap; gap:.3rem .6rem; align-items:baseline; font-family:"IBM Plex Mono",monospace; font-size:.72rem; color:var(--ink-soft)}
.card h4{font-family:"Fraunces",serif; font-weight:500; font-size:1rem; margin:.25rem 0 .35rem}
.chip{display:inline-block; border-radius:3px; padding:.02em .45em; font-family:"IBM Plex Mono",monospace; font-size:.68rem; color:#fff; letter-spacing:.03em}
.who{font-size:.85rem; color:var(--ink-soft); margin:.2rem 0}
blockquote{border-inline-start:3px solid var(--accent); margin:.5rem 0; padding:.45rem .7rem; direction:rtl; text-align:right;
  font-family:"Frank Ruhl Libre",serif; font-size:1rem; line-height:1.75; background:var(--paper); border-radius:3px 0 0 3px}
.src{font-family:"IBM Plex Mono",monospace; font-size:.72rem}
.note{font-size:.84rem; color:var(--ink-soft); font-style:italic; margin:.2rem 0}
.entrytext{direction:rtl; text-align:right; font-family:"Frank Ruhl Libre",serif; line-height:1.85; white-space:pre-wrap; font-size:1.02rem}
mark{background:color-mix(in srgb, var(--tour) 30%, transparent); color:inherit; padding:0 .1em}
.record{columns:2 22rem; column-gap:1.2rem}
.record .card{break-inside:avoid; margin-top:0}
.tiergroup h3{font-size:1.05rem; margin:1.2rem 0 .5rem}
.method{font-size:.88rem; color:var(--ink-soft)}
.method li{margin:.25rem 0}
.mono{font-family:"IBM Plex Mono",monospace; font-size:.82em; background:var(--mono-bg); padding:.05em .35em; border-radius:3px}
@media (max-width:560px){ body{padding-top:1.6rem} .record{columns:1} }
</style>
</head>
<body>
<main>
<p class="eyebrow">Play × place dossier · Leksikon fun yidishn teater</p>
<h1>The Dybbuk in France</h1>
<p class="lede">An-ski's play and its offshoots in France, as the Leksikon records them: who brought them there, where they played,
how the play reached French, and the Dybbuk people whose lives passed through Paris.
Click any mark, line or node to see the source passage.</p>

<div class="statrow">
  <div class="stat"><b>%%PLAYED%%</b><span>stagings in France</span></div>
  <div class="stat"><b>%%TOUR%%</b><span>tours carrying it</span></div>
  <div class="stat"><b>%%TOWNS%%</b><span>French towns</span></div>
  <div class="stat"><b>%%PEOPLE%%</b><span>people</span></div>
  <div class="stat"><b>%%FACTS%%</b><span>facts, each quoted</span></div>
  <div class="stat"><b>%%ENTRIES%%</b><span>Leksikon entries</span></div>
</div>
<div class="legend" id="legend"></div>

<section id="s-time">
<h2>Lifelines</h2>
<p class="sub">One row per person or company. Bars show time in France; marks show Dybbuk events in France and, in grey, the same people's Dybbuk work elsewhere.
Dashed bars are approximate. Undated facts sit in the right-hand column.</p>
<div id="tl" class="panel"></div>
</section>

<section id="s-map">
<h2>Map</h2>
<p class="sub">The routes that brought the Dybbuk to France, as the entries list their stops, and where it played there.
The grey dashed route is the Yiddish Art Theatre's 1924 tour, which came to Paris without it.</p>
<div class="maptools" id="maptools"></div>
<div id="map"></div>
</section>

<section id="s-net">
<h2>Network</h2>
<p class="sub">People (circles), companies (boxes), works (diamonds) and French places (triangles). A line joins two of them when one fact names both.
Red people played the Dybbuk in France; blue ones were in France, but the Leksikon does not say they played it there.</p>
<div class="maptools"><button id="netfit">fit</button><button id="netphys" aria-pressed="true">physics</button></div>
<div id="net" class="panel"></div>
</section>

<section id="s-rec">
<h2>The record</h2>
<p class="sub">Every fact behind the charts, with the passage it rests on.</p>
<div id="record"></div>
</section>

<section>
<h2>How this was made</h2>
<ul class="method">
<li><b>Corpus.</b> The 68 entries of the Leksikon that mention a Dybbuk and a French place or the French language.</li>
<li><b>Extraction.</b> Gemini 3 Flash read each entry for Dybbuk–France facts. Its draft was the starting point only: every fact here was checked against the entry by hand, and about two thirds of the drafted rows were dropped.</li>
<li><b>Evidence.</b> Each fact quotes the entry verbatim and links to its volume, columns and the archive.org scan (vols. 1–6).</li>
<li><b>Tiers.</b> “Played in France” requires the text to place a Dybbuk performance in France. “Tour” means a company carried the Dybbuk in its repertoire on a tour that reached France. “In French” covers translations, reported French stagings and French reviews. “In France” means a Dybbuk person or company was in France without, as far as the Leksikon says, playing it there.</li>
<li>See also: <a href="dybbuk_dossier.html">The Dybbuks</a> (every work of that name) and the <a href="france_dossier.html">France dossier</a>.</li>
</ul>
</section>
</main>

<aside id="drawer" aria-hidden="true"><header><div><p class="eyebrow" id="dkind"></p><h3 id="dtitle"></h3></div>
<button id="dclose" aria-label="close">✕</button></header><div id="dbody"></div></aside>
<div id="tip"></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/vis-network@9.1.2/standalone/umd/vis-network.min.js"></script>
<script>
const D = %%DATA%%;
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const TIERS = {played:"Played in France", tour:"Tour carrying it", french:"In French", stay:"In France", elsewhere:"Dybbuk elsewhere"};
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const nameOf = k => k.startsWith("t:") ? D.troupes[k.slice(2)].en : (D.people[k] ? D.people[k].en : k);
const WORKS = {"w:ansky":"An-ski's Dybbuk","w:parody_maud":"Maud's puppet parody","w:parody_three":"The Three Dybbuks (parody)",
  "w:film":"The Dybbuk film (1937)","w:opera":"Rocca's Dybbuk opera","w:french":"Le Dibbouk (French)"};

// ---------- legend ----------
document.getElementById("legend").innerHTML = Object.entries(TIERS).map(([k,v]) =>
  `<span><i style="background:var(--${k})" class="${k==="elsewhere"?"hollow":""}"></i>${v}</span>`).join("");

// ---------- drawer ----------
const drawer = document.getElementById("drawer");
document.getElementById("dclose").onclick = () => { drawer.classList.remove("open"); drawer.setAttribute("aria-hidden","true"); };
document.addEventListener("keydown", e => { if (e.key === "Escape") document.getElementById("dclose").click(); });
function cardHTML(f){
  const who = f.people.map(p => `${esc(nameOf(p.k))}${p.role?` <span class="src">${esc(p.role.replace("_"," "))}</span>`:""}${p.ch?` as ${esc(p.ch)}`:""}`).join(" · ");
  const where = f.places.map(p => D.places[p].en).join(", ");
  const src = f.src.map(s => `<blockquote>${s.segs.map(esc).join(" … ")}</blockquote>
    <div class="src"><bdi class="yi" dir="rtl">${esc(D.heads[s.pid])}</bdi> · ${s.url?`<a href="${s.url}" target="_blank" rel="noopener">vol. ${s.vol}, ${esc(s.cols)}</a>`:`vol. ${s.vol}, ${esc(s.cols)}`}
    · <a href="#" data-entry="${s.pid}">read the entry</a></div>`).join("");
  return `<div class="card"><div class="top"><span class="chip" style="background:var(--${f.tier})">${TIERS[f.tier]}</span>
    <span>${esc(f.date)}</span>${where?`<span>${esc(where)}</span>`:""}</div>
    <h4>${esc(f.label)}</h4><div class="who">${who}</div>${f.note?`<p class="note">${esc(f.note)}</p>`:""}${src}</div>`;
}
function openDrawer(kind, title, facts){
  document.getElementById("dkind").textContent = kind;
  document.getElementById("dtitle").textContent = title;
  document.getElementById("dbody").innerHTML = facts.map(cardHTML).join("") || "<p>No facts.</p>";
  drawer.classList.add("open"); drawer.setAttribute("aria-hidden","false");
  document.getElementById("dbody").scrollTop = 0;
}
let ENT = null;
async function openEntry(pid){
  if (!ENT) { try { ENT = await (await fetch("dybbuk_france_entries.json")).json(); } catch(e) { ENT = {}; } }
  const e = ENT[pid]; if (!e) return;
  const rx = /דיבוק|דבוק|פאריז|פראנקרייך|פראנצויז|מעץ|שטראסבורג/;
  const strip = w => w.replace(/[\u0591-\u05C7]/g, "").normalize("NFD").replace(/[\u0591-\u05C7]/g, "");
  const html = esc(e.t).replace(/[^\s]+/g, w => rx.test(strip(w)) ? `<mark>${w}</mark>` : w);
  document.getElementById("dkind").textContent = `Leksikon vol. ${e.v}` + (e.x ? " · excerpts" : "");
  document.getElementById("dtitle").innerHTML = `<span class="yi" dir="rtl">${esc(e.h)}</span>`;
  document.getElementById("dbody").innerHTML = `<p class="src">${e.x ? "Passages around each Dybbuk or French mention." : "Full entry."}</p><div class="entrytext">${html}</div>`;
  drawer.classList.add("open");
}
document.addEventListener("click", ev => {
  const a = ev.target.closest("[data-entry]"); if (!a) return;
  ev.preventDefault(); openEntry(a.dataset.entry);
});
const tip = document.getElementById("tip");
function showTip(ev, html){ tip.innerHTML = html; tip.style.display = "block";
  const x = Math.min(ev.clientX + 14, innerWidth - tip.offsetWidth - 8), y = Math.min(ev.clientY + 14, innerHeight - tip.offsetHeight - 8);
  tip.style.left = x + "px"; tip.style.top = y + "px"; }
const hideTip = () => tip.style.display = "none";

// ---------- timeline ----------
(function(){
  const rows = {};
  for (const f of D.facts) (rows[f.row] ??= []).push(f);
  const frYear = fs => Math.min(...fs.filter(f => f.tier !== "elsewhere" && f.y0).map(f => f.y0), 9999);
  const keys = Object.keys(rows).sort((a,b) => frYear(rows[a]) - frYear(rows[b]) || nameOf(a).localeCompare(nameOf(b)));
  const Y0 = 1890, Y1 = 1970, LW = 215, UW = 110, RH = 26, TOP = 28;
  const W = Math.max(760, Math.floor(document.getElementById("tl").getBoundingClientRect().width) - 4);
  const PW = W - LW - UW - 20;
  const x = y => LW + (y - Y0) / (Y1 - Y0) * PW;
  const H = TOP + keys.length * RH + 16;
  let s = `<svg style="width:100%;min-width:760px;height:auto" viewBox="0 0 ${W} ${H}" role="img" aria-label="Lifelines timeline">`;
  for (let y = Y0; y <= Y1; y += 10) s += `<line x1="${x(y)}" x2="${x(y)}" y1="${TOP-6}" y2="${H-10}" stroke="var(--line)"/><text class="ax" x="${x(y)}" y="${TOP-12}" text-anchor="middle">${y}</text>`;
  const ux = LW + PW + 20;
  s += `<line x1="${ux-10}" x2="${ux-10}" y1="${TOP-6}" y2="${H-10}" stroke="var(--line)"/><text class="ax" x="${ux + UW/2 - 10}" y="${TOP-12}" text-anchor="middle">undated</text>`;
  keys.forEach((k, i) => {
    const cy = TOP + i * RH + RH/2;
    if (i % 2 === 0) s += `<rect x="0" y="${cy-RH/2}" width="${W}" height="${RH}" fill="var(--card)" opacity=".55"/>`;
    s += `<text class="rowlab" data-row="${k}" x="${LW-10}" y="${cy+4}" text-anchor="end" font-size="12.5">${esc(nameOf(k))}</text>`;
    let ui = 0; const used = [];
    const fs = rows[k].slice().sort((a,b) => (a.tier==="stay"?0:1) - (b.tier==="stay"?0:1));
    for (const f of fs) {
      const col = `var(--${f.tier})`;
      let cx;
      if (!f.y0) { cx = ux + 8 + (ui++) * 16; }
      if (f.tier === "stay" && f.y0) {
        const a = x(f.y0), b = Math.max(x(f.y1 + 1), a + 6);
        s += `<rect class="mk" data-f="${f.id}" x="${a}" y="${cy-5}" width="${b-a}" height="10" rx="3" fill="${col}" opacity=".85" ${f.approx?'stroke-dasharray="3 2" stroke="var(--ink-soft)" fill-opacity=".45"':''}/>`;
        continue;
      }
      if (f.y0) { cx = x((f.y0 + (f.y1 || f.y0) + 1) / 2); while (used.some(u => Math.abs(u - cx) < 13)) cx += 13; used.push(cx); }
      const t = f.tier;
      if (t === "stay") s += `<rect class="mk" data-f="${f.id}" x="${cx-5}" y="${cy-5}" width="10" height="10" rx="3" fill="${col}"/>`;
      else if (t === "played") s += `<path class="mk" data-f="${f.id}" d="M${cx} ${cy-8}L${cx+8} ${cy}L${cx} ${cy+8}L${cx-8} ${cy}Z" fill="${col}" stroke="var(--paper)" stroke-width="1.5"/>`;
      else if (t === "tour") s += `<circle class="mk" data-f="${f.id}" cx="${cx}" cy="${cy}" r="7" fill="${col}" stroke="var(--paper)" stroke-width="1.5"/>`;
      else if (t === "french") s += `<rect class="mk" data-f="${f.id}" x="${cx-6}" y="${cy-6}" width="12" height="12" fill="${col}" stroke="var(--paper)" stroke-width="1.5"/>`;
      else s += `<path class="mk" data-f="${f.id}" d="M${cx} ${cy-6}L${cx+6} ${cy}L${cx} ${cy+6}L${cx-6} ${cy}Z" fill="var(--paper)" stroke="${col}" stroke-width="2"/>`;
    }
  });
  s += "</svg>";
  const el = document.getElementById("tl"); el.innerHTML = s;
  const byId = Object.fromEntries(D.facts.map(f => [f.id, f]));
  el.querySelectorAll(".mk").forEach(m => {
    const f = byId[m.dataset.f];
    m.addEventListener("mousemove", ev => showTip(ev, `<b>${esc(f.label)}</b><br>${esc(f.date)} · ${TIERS[f.tier]}`));
    m.addEventListener("mouseleave", hideTip);
    m.addEventListener("click", () => { hideTip(); openDrawer(TIERS[f.tier], nameOf(f.row), [f]); });
  });
  el.querySelectorAll(".rowlab").forEach(t => t.addEventListener("click", () =>
    openDrawer("Lifeline", nameOf(t.dataset.row), rows[t.dataset.row].slice().sort((a,b) => (a.y0||9999) - (b.y0||9999)))));
})();

// ---------- map ----------
(function(){
  const map = L.map("map", {scrollWheelZoom:false}).fitBounds([[43,-3],[57,26]]);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {maxZoom: 17,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'}).addTo(map);
  const byId = Object.fromEntries(D.facts.map(f => [f.id, f]));
  const groups = {};
  const tools = document.getElementById("maptools");
  function toggle(name, layer, color, dashed){
    layer.addTo(map); groups[name] = layer;
    const b = document.createElement("button"); b.setAttribute("aria-pressed","true");
    b.innerHTML = `<span style="display:inline-block;width:1.2rem;border-top:3px ${dashed?"dashed":"solid"} ${color};vertical-align:middle;margin-right:.35rem"></span>${esc(name)}`;
    b.onclick = () => { const on = b.getAttribute("aria-pressed") === "true"; b.setAttribute("aria-pressed", String(!on)); on ? map.removeLayer(layer) : layer.addTo(map); };
    tools.appendChild(b);
  }
  for (const r of D.routes) {
    const color = css("--r-" + r.k), dashed = r.k === "yat";
    const g = L.layerGroup();
    const ll = r.pts.map(p => [p[1], p[2]]);
    L.polyline(ll, {color, weight: dashed ? 2.5 : 3.5, opacity: .85, dashArray: dashed ? "6 6" : null})
      .bindTooltip(r.label, {sticky:true}).on("click", () => openDrawer("Route", r.label, [byId[r.fact]])).addTo(g);
    r.pts.forEach(p => L.circleMarker([p[1], p[2]], {radius:3.5, color, weight:1, fillColor:color, fillOpacity:1})
      .bindTooltip(`${p[0]} — ${r.label}`).addTo(g));
    toggle(r.label, g, color, dashed);
  }
  // French places
  const fr = L.layerGroup();
  const at = {};
  for (const f of D.facts) for (const p of f.places) (at[p] ??= []).push(f);
  const rank = {played:0, tour:1, french:2, stay:3, elsewhere:4};
  for (const [k, fs] of Object.entries(at)) {
    const pl = D.places[k], top = fs.slice().sort((a,b) => rank[a.tier]-rank[b.tier])[0].tier;
    const n = fs.length;
    L.circleMarker([pl.lat, pl.lon], {radius: 6 + Math.sqrt(n) * 4, color: css("--paper"), weight: 2,
      fillColor: css("--" + top), fillOpacity: .9, dashArray: k === "france" ? "4 3" : null})
      .bindTooltip(`<b>${esc(pl.en)}</b><br>${n} fact${n>1?"s":""}`)
      .on("click", () => openDrawer("Place", pl.en, fs.slice().sort((a,b) => rank[a.tier]-rank[b.tier] || (a.y0||9999)-(b.y0||9999))))
      .addTo(fr);
  }
  toggle("French places", fr, css("--played"), false);
  const vg = L.layerGroup();
  for (const f of D.facts) if (f.venue) {
    const v = D.venues[f.venue];
    L.circleMarker([v.lat, v.lon], {radius:5, color: css("--stay"), weight:2, fillColor: css("--paper"), fillOpacity:1})
      .bindTooltip(`${esc(v.en)} — ${esc(f.date)}`).on("click", () => openDrawer("Paris venue", v.en, [f])).addTo(vg);
  }
  toggle("Paris venues (zoom in)", vg, css("--stay"), false);
  const b = document.createElement("button"); b.textContent = "zoom to Paris";
  b.onclick = () => map.setView([48.862, 2.35], 13); tools.appendChild(b);
  const b2 = document.createElement("button"); b2.textContent = "Europe";
  b2.onclick = () => map.fitBounds([[43,-3],[57,26]]); tools.appendChild(b2);
})();

// ---------- network ----------
(function(){
  const nodes = new Map(), edges = new Map(), touch = {};
  const add = (id, o) => { if (!nodes.has(id)) nodes.set(id, o); };
  const link = (a, b, label, color, f) => {
    const k = [a,b].sort().join("|");
    if (!edges.has(k)) edges.set(k, {from:a, to:b, labels:new Set(), color, facts:new Set()});
    const e = edges.get(k); if (label) e.labels.add(label); e.facts.add(f.id);
    if (color === css("--played")) e.color = color;
  };
  const note = (id, f) => (touch[id] ??= new Set()).add(f.id);
  const playedIn = new Set(D.facts.filter(f => f.tier === "played").flatMap(f => f.people.map(p => p.k)));
  for (const f of D.facts) {
    const w = f.wnode;
    if (f.tier !== "stay") { add(w, {label: WORKS[w] || w, kind:"work"}); note(w, f); }
    if (w === "w:french") { add("w:ansky", {label: WORKS["w:ansky"], kind:"work"}); link("w:french","w:ansky","translation of", css("--french"), f); }
    if (f.troupe) { const t = "t:" + f.troupe; add(t, {label: D.troupes[f.troupe].en, kind:"troupe"}); note(t, f);
      if (f.tier !== "stay") link(t, w, "", css("--elsewhere"), f);
      for (const p of f.places) { add("p:"+p, {label: D.places[p].en, kind:"place"}); note("p:"+p, f); link(t, "p:"+p, f.date, css("--"+f.tier), f); } }
    for (const p of f.people) {
      const id = p.k; add(id, {label: D.people[id].en, kind:"person"}); note(id, f);
      if (f.tier !== "stay") link(id, w, p.ch || p.role.replace("_"," "), css(f.tier==="elsewhere"?"--elsewhere":"--"+f.tier), f);
      if (f.troupe) link(id, "t:" + f.troupe, "", css("--elsewhere"), f);
      if (!f.troupe || f.tier === "played") for (const pl of f.places) {
        add("p:"+pl, {label: D.places[pl].en, kind:"place"}); note("p:"+pl, f);
        link(id, "p:"+pl, f.y0 ? String(f.y0) + (f.y1 && f.y1 !== f.y0 ? "–" + String(f.y1).slice(2) : "") : "", css("--"+f.tier), f);
      }
    }
  }
  const ink = css("--ink"), paper = css("--paper");
  const V = [...nodes].map(([id, o]) => {
    const n = (touch[id] || new Set()).size;
    const base = {id, label:o.label, font:{color:ink, face:"Spectral", size:16, strokeWidth:3, strokeColor:paper}};
    if (o.kind === "work") return {...base, shape:"diamond", size: id === "w:ansky" ? 30 : 16, color:{background:css("--work"), border:css("--work")}, font:{...base.font, size: id==="w:ansky"?17:14, bold:true}};
    if (o.kind === "troupe") return {...base, shape:"box", color:{background:css("--troupe"), border:css("--troupe")}, font:{...base.font, color:"#1d1a14", strokeWidth:0}, margin:6};
    if (o.kind === "place") return {...base, shape:"triangle", size: 14 + n * 1.2, color:{background:css("--place"), border:css("--place")}};
    const c = playedIn.has(id) ? css("--played") : css("--stay");
    return {...base, shape:"dot", size: 8 + n * 2.2, color:{background:c, border:c}};
  });
  const E = [...edges.values()].map((e, i) => ({id:i, from:e.from, to:e.to, color:{color:e.color, opacity:.75},
    width: 1 + e.facts.size, title: [...e.labels].join(", "), facts:[...e.facts]}));
  const net = new vis.Network(document.getElementById("net"), {nodes: new vis.DataSet(V), edges: new vis.DataSet(E)}, {
    physics:{solver:"forceAtlas2Based", forceAtlas2Based:{gravitationalConstant:-110, springLength:130, avoidOverlap:.7}, stabilization:{iterations:400}},
    interaction:{hover:true, tooltipDelay:120, navigationButtons:false}, edges:{smooth:{type:"continuous"}}});
  net.once("stabilizationIterationsDone", () => { net.setOptions({physics:false}); document.getElementById("netphys").setAttribute("aria-pressed","false"); net.fit(); });
  const byId = Object.fromEntries(D.facts.map(f => [f.id, f]));
  const kindName = {work:"Work", troupe:"Company", place:"Place", person:"Person"};
  net.on("click", p => {
    if (p.nodes.length) { const id = p.nodes[0], o = nodes.get(id);
      openDrawer(kindName[o.kind], o.label, [...(touch[id]||[])].map(i => byId[i]).sort((a,b) => (a.y0||9999)-(b.y0||9999))); }
    else if (p.edges.length) { const e = E[p.edges[0]];
      openDrawer("Link", `${nodes.get(e.from).label} — ${nodes.get(e.to).label}`, e.facts.map(i => byId[i])); }
  });
  document.getElementById("netfit").onclick = () => net.fit({animation:true});
  const ph = document.getElementById("netphys");
  ph.onclick = () => { const on = ph.getAttribute("aria-pressed") === "true"; ph.setAttribute("aria-pressed", String(!on)); net.setOptions({physics: !on}); };
})();

// ---------- record ----------
(function(){
  const el = document.getElementById("record");
  el.innerHTML = Object.keys(TIERS).map(t => {
    const fs = D.facts.filter(f => f.tier === t).sort((a,b) => (a.y0||9999)-(b.y0||9999));
    return `<details class="tiergroup" ${t==="played"||t==="tour"||t==="french"?"open":""}><summary><h3 style="display:inline">${TIERS[t]} <span class="src">(${fs.length})</span></h3></summary>
      <div class="record">${fs.map(cardHTML).join("")}</div></details>`;
  }).join("");
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
