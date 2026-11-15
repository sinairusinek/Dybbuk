#!/usr/bin/env python3.11
"""Build docs/Visualizations/france_dossier.html (+ france_entries.json for the
timeline/map entry panel) from the France itineraries and the KG slice.

Pipeline (see README in this folder):
  run_extract.sh -> merge_shards.py -> ../itinerary/build_itinerary_data.py
  -> resolve_france.py -> ../itinerary/build_place_pages.py --place france
  -> kg_slice.py -> build_france_dossier.py
"""
import csv, html, json, re, sys, unicodedata
from collections import Counter, defaultdict
from pathlib import Path

H = Path(__file__).resolve().parent
PLAYS = H.parent
DOCS = PLAYS.parent.parent / "docs" / "Visualizations"
sys.path.insert(0, str(PLAYS))
from lexicon_ref import ref  # noqa: E402

csv.field_size_limit(10**9)
POINTS = re.compile(r"[֑-ׇ]")
esc = html.escape
FQ = {"Q90", "Q142", "Q201348", "Q22690", "Q40898", "Q456", "Q23482", "Q33959", "Q6602",
      "Q1479", "Q7880", "Q648", "Q45656"}
FR_RX = re.compile(r"פאריז|פראנקרייך|(?<!\S)מעץ|עלזאס|לאטרינגען|דראנסי|ווישי|פיטיוויע|גורס(?!\S)|"
                   r"(?<!\S)ניצא(?!\S)|מארסעל(?!א|\S)|מארסיי|ליאן|ראלאנד|וויטעל|באניע")
FALSE_FR = ["Q1022906", "Q1023020", "Q1048138", "Q198480", "Q218036", "Q4107532", "Q4209729",
            "Q81137", "Q856018", "Q869811"]
TOURING = {"Company on Tour", "Traveling Company", "troupe"}


def sp(s):
    return POINTS.sub("", unicodedata.normalize("NFD", s or "")).replace("פֿ", "פ")


def isf(s):
    return s.get("country") == "France" or s.get("qid") in FQ


def main():
    D = json.load(open(PLAYS / "itinerary" / "france_itineraries.json", encoding="utf-8"))
    K = json.load(open(H / "kg_slice.json", encoding="utf-8"))
    N = {r["node_id"]: r for r in csv.DictReader(open(PLAYS / "kg/nodes.tsv"), delimiter="\t")}
    E = list(csv.DictReader(open(PLAYS / "kg/edges.tsv"), delimiter="\t"))
    lab = lambda i: N.get(i, {}).get("label_english") or sp(N.get(i, {}).get("label_yiddish", i))
    ids = [l.strip() for l in open(H / "france_ids.txt") if l.strip()]
    texts = {}
    with open(PLAYS.parent / "people" / "entry_texts.tsv", encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["person_id"] in set(ids):
                texts[r["person_id"]] = r
    drafts = {json.loads(l)["person_id"]: json.loads(l) for l in open(H / "france_itinerary_drafts.jsonl")}
    failed = [p for p, r in drafts.items() if "error" in r]

    # ---- France stations ----
    S = []
    for it in D:
        for s in it["stations"]:
            if isf(s):
                S.append((it, s))
    ents = {it["person_id"] for it, _ in S}

    def cite(pid, quote):
        try:
            r = ref(pid, quote) if quote else ref(pid, "")
        except Exception:
            return f'<span class="src">{esc(pid)}</span>'
        if r.get("url"):
            return f'<a class="src" href="{esc(r["url"])}" title="{esc(pid)}">vol. {r["vol"]}, {esc(r["cols"])}</a>'
        return f'<span class="src" title="{esc(pid)}">vol. {r["vol"]}, {esc(r["cols"])}</span>'

    def who(it):
        h = sp(it["heading"])
        return f'<span class="yi">{esc(h)}</span>' + (
            f' <span class="small">(about <span class="yi">{esc(sp(it["subject"]))}</span>)</span>'
            if it["subject"] != "entry" else "")

    def when(s):
        if s.get("date_start"):
            return esc(s["date_start"] + (f"–{s['date_end']}" if s.get("date_end") else ""))
        if s.get("t_start"):
            return f'<span class="small" title="interpolated from narrative order">~{s["t_start"]}</span>'
        return "—"

    # ---- entry panel JSON (windows around French mentions; full text if short) ----
    panel = {}
    for pid, r in texts.items():
        t = sp(r["entry_text"])
        if len(t) <= 4000:
            panel[pid] = {"h": sp(r["heading"]), "v": r["volume"], "t": t}
            continue
        spans = []
        for m in FR_RX.finditer(t):
            a, b = max(0, m.start() - 900), min(len(t), m.end() + 900)
            if spans and a <= spans[-1][1]:
                spans[-1][1] = b
            else:
                spans.append([a, b])
        if not spans:
            spans = [[0, 1800]]
        panel[pid] = {"h": sp(r["heading"]), "v": r["volume"], "x": 1,
                      "t": "\n\n   · · ·\n\n".join(t[a:b] for a, b in spans)}
    (DOCS / "france_entries.json").write_text(json.dumps(panel, ensure_ascii=False), encoding="utf-8")

    # ---- KG facts ----
    places = K["places"]
    bio = [e for e in K["edges"] if e["edge_type"] in ("born_in", "died_in", "buried_in")]
    located = [e for e in K["edges"] if e["edge_type"] in ("located_in", "toured_in")]
    org_people = Counter(e["org"] for e in K["org_edges"])
    touring = sorted({e["source_id"] for e in K["edges"] if e["edge_type"] == "toured_in"})
    deaths_x = [(it, s) for it, s in S if s["verb"] == "death" or any(
        e.get("event_type") in ("death", "burial") for e in s["events"])]

    perf = []
    for it, s in S:
        for e in s["events"]:
            if e.get("play") and e.get("play") != "unknown":
                perf.append((it, s, e))
    plays = Counter(sp(e["play"]).strip("„“\"' ") for _, _, e in perf)

    def period(s):
        y = s.get("t_start")
        if not y:
            return None
        y = int(y)
        return 0 if y < 1914 else 1 if y < 1939 else 2 if y <= 1945 else 3
    PER = ["Before 1914", "1914–1938", "1939–1945", "After 1945"]
    per_st = defaultdict(list)
    for it, s in S:
        p = period(s)
        if p is not None and int(s["t_start"]) >= 1800:
            per_st[p].append((it, s))
    undated = sum(1 for it, s in S if period(s) is None)

    places_fr = Counter(s["label_en"] or "(unresolved)" for _, s in S)
    venues = Counter(sp(s["org"]) for _, s in S if s["org"])

    out = []
    a = out.append
    head = open(PLAYS / "dybbuk_dossier" / "page_head.html", encoding="utf-8").read()
    a(head.replace("<title>The Dybbuks</title>", "<title>France Dossier</title>"))
    a(f"""<main>
<p class="eyebrow">Dybbuk · Plays KG × Zylbercweig Leksikon · 2026-11-15</p>
<h1>France Dossier</h1>
<p class="lede">Everything the knowledge graph and the Leksikon fun yidishn teater hold about France: Paris above all,
and also Metz and Alsace-Lorraine, Nancy, Lyon, Vichy, Vittel, the camps at Drancy and Beaune-la-Rolande —
<span class="yi" dir="rtl">פֿראַנקרייך · פּאַריז</span>. The pattern is the Istanbul and Danzig dossiers, scaled from a
city to a country.</p>
<p style="font-size:.9rem"><b>Companion pages (itineraries):</b>
<a href="france_timeline.html">timeline</a> · <a href="france_map.html">route map</a></p>
<p style="font-size:.9rem"><b>Companion dossiers:</b> <a href="istanbul_dossier.html">Istanbul</a> ·
<a href="danzig_dossier.html">Danzig</a> · <a href="dybbuk_dossier.html">The Dybbuks</a> ·
<a href="rumshinsky_dossier.html">Joseph Rumshinsky</a>.</p>
<div class="statrow">
  <div class="stat"><b>{len(places)}</b><span>French place nodes</span></div>
  <div class="stat"><b>{len(K['edges'])}</b><span>KG edges</span></div>
  <div class="stat"><b>{len(K['events'])}</b><span>KG production events</span></div>
  <div class="stat"><b>{len(K['orgs'])}</b><span>orgs located here</span></div>
  <div class="stat"><b>{len(ents)}</b><span>lexicon entries</span></div>
  <div class="stat"><b>{len(S)}</b><span>stations in France</span></div>
  <div class="stat"><b>{len(perf)}</b><span>named plays performed</span></div>
</div>

<section>
<h2>Data problems found — and fixed</h2>
<p>Five problems surfaced while building this dossier. Four are fixed (2026-11-15). The fifth is a coverage gap, not an error.</p>
<ol>
<li><span class="flag">Ten Eastern European towns sat on French communes.</span> Grodzisk, Nowo-Minsk, Terespol, Komarno,
Szczerzec, Novyi Buh and Rava-Ruska were resolved to Wikidata items for French villages (Gugney-aux-Aulx, Saint-Carné,
Sombrin…). Olomouc, Slavuta and two Galician villages had the right QID but a French village's label. Together that put
23 births in Lorraine, Picardy and Brittany. <b>Fixed</b> through a new hand-override table that the gazetteer build applies
first (<span class="mono">zibn-shtern/data/reference/place_qid_overrides.tsv</span>). The gazetteer and the KG were rebuilt (§1).</li>
<li><span class="flag">Left Poale Zion was "in" a French canton.</span> <span class="yi">א'י</span>, the abbreviation for
Eretz-Israel, had been matched to the canton of Ay. <b>Fixed:</b> <span class="mono">org:2608</span> is now
<span class="mono">located_in</span> Land of Israel (<span class="mono">Q155321</span>).</li>
<li><span class="flag">Sammy Gronemann was "born in Strasbourg".</span> The entry says <span class="yi">שטראָסבורג / מערב-פּרייסן</span>,
Strasburg in West Prussia. <b>Fixed:</b> his birth now points to Brodnica (<span class="mono">Q690030</span>). The override is
scoped to his record only, since the same spelling elsewhere is Strasbourg.</li>
<li><span class="flag">Touring companies were recorded as resident.</span> <b>Fixed for the whole KG:</b> orgs typed
Traveling Company or Company on Tour now get <span class="mono">toured_in</span> instead of <span class="mono">located_in</span>
(733 edges). Orgs typed "Not an organization", such as Drancy camp and the Vladek children's home, no longer become org
nodes (34 relations dropped) (§4).</li>
<li><span class="flag">The war is nearly absent from the KG.</span> The bio layer records "died in X" only. Deportation,
internment and flight never become edges, while the itineraries hold {len(per_st[2])} French stations for 1939–45 (§7).
This is a gap in what the KG extracts, not a wrong fact. Closing it means adding itinerary edges
(<span class="mono">stayed_in / toured_in / deported_from</span>) as a KG layer, which three place dossiers now support.</li>
</ol>
</section>
""")
    # §1 place nodes
    a(f'<section><h2>1 · The place nodes</h2><p>{len(places)} KG place nodes are French:</p>')
    a('<div class="tablewrap"><table><tr><th>node</th><th>Yiddish</th><th>English</th><th>edges</th><th>note</th></tr>')
    notes = {"place:Q40898": "no edges yet"}
    cnt = Counter(e["target_id"] for e in K["edges"])
    for k, v in places.items():
        a(f'<tr><td><span class="mono">{esc(k)}</span></td><td class="rtlcell">{esc(sp(v["yi"]))}</td>'
          f'<td>{esc(v["en"])}</td><td class="num">{cnt.get(k, 0)}</td><td>{esc(notes.get(k, ""))}</td></tr>')
    a('</table></div>')
    a('<h3>France in the KG that was not France (fixed)</h3><p>The hand overrides now applied by the gazetteer build. '
      '"edges now" counts KG edges on the corrected QID after the rebuild.</p>')
    a('<div class="tablewrap"><table><tr><th>spelling</th><th>scope</th><th>now</th><th class="num">edges now</th><th>was</th></tr>')
    ov = list(csv.DictReader(open(PLAYS.parent / "zibn-shtern/data/reference/place_qid_overrides.tsv"), delimiter="\t"))
    tcount = Counter(e["target_id"] for e in E)
    for r in ov:
        sv = r["source_value"] or "(label fix)"
        a(f'<tr><td class="rtlcell">{esc(sv)}</td><td>{esc(r["corpus"])}{"" if r["record_id"] == "*" else " · " + esc(r["record_id"])}</td>'
          f'<td>{esc(r["label_en"])} <span class="mono">{esc(r["qid"])}</span></td><td class="num">{tcount.get("place:" + r["qid"], 0)}</td>'
          f'<td class="small">{esc(r["note"])}</td></tr>')
    a('</table></div></section>')

    # §2 bio
    a(f'<section><h2>2 · Life-event edges (bio layer)</h2><p>The bio layer holds {len(bio)} person↔France facts:</p>')
    a('<div class="tablewrap"><table><tr><th>edge</th><th>person</th><th>relation</th><th>place</th><th>date</th></tr>')
    for e in sorted(bio, key=lambda e: e["date_start"]):
        a(f'<tr><td><span class="mono">{esc(e["edge_id"])}</span></td><td>{esc(e["src_en"] or sp(e["src_yi"]))}</td>'
          f'<td>{esc(e["edge_type"])}</td><td>{esc(places[e["target_id"]]["en"])}</td><td class="num">{esc(e["date_start"])}</td></tr>')
    a('</table></div>')
    a(f'<p>The itineraries hold <b>{len(deaths_x)}</b> deaths or burials in France, including people killed after '
      'deportation from Paris. The bio layer records a death only where the entry says "died in X". '
      '"Taken by the Germans in Paris" leaves no edge.</p>')
    a('<details><summary>All deaths and burials in France in the itineraries</summary><div class="tablewrap"><table>'
      '<tr><th>date</th><th>entry</th><th>place</th><th>what the entry says</th><th>source</th></tr>')
    for it, s in sorted(deaths_x, key=lambda x: str(x[1].get("t_start") or "9999")):
        d = "; ".join(e.get("description", "") for e in s["events"][:2])
        a(f'<tr><td class="num">{when(s)}</td><td>{who(it)}</td><td>{esc(s["label_en"])}</td><td>{esc(d)}</td>'
          f'<td>{cite(it["person_id"], s.get("evidence",""))}</td></tr>')
    a('</table></div></details></section>')

    # §3 productions
    a('<section><h2>3 · Production events</h2>')
    a(f'<p>The KG anchors <b>{len(K["events"])}</b> production events to France. Both are the January 1889 Paris staging of '
      'Katzenelenbogen\'s <span class="yi">רש"י</span>, which a troupe in the Latin Quarter billed as Goldfadn\'s. Because '
      'the plays KG covers only Lateiner and Hurwitz, everything else performed in France is outside it.</p>')
    a(f'<p>The itineraries name <b>{len(perf)}</b> performances of <b>{len(plays)}</b> distinct titles in France. The most frequent:</p><p>')
    a(" · ".join(f'<span class="yi">{esc(t)}</span> <span class="count">{n}</span>' for t, n in plays.most_common(30)))
    a('</p><details><summary>All named performances in France, by date</summary><div class="tablewrap"><table>'
      '<tr><th>date</th><th>play</th><th>troupe / venue</th><th>place</th><th>entry</th><th>source</th></tr>')
    for it, s, e in sorted(perf, key=lambda x: str(x[2].get("date") or x[1].get("date_start") or x[1].get("t_start") or "9999")):
        tv = " · ".join(sp(x) for x in (s.get("org", ""), e.get("venue", "")) if x)
        a(f'<tr><td class="num">{esc(e.get("date") or "") or when(s)}</td><td class="rtlcell">{esc(sp(e["play"]))}</td>'
          f'<td class="rtlcell">{esc(tv)}</td><td>{esc(s["label_en"])}</td><td>{who(it)}</td>'
          f'<td>{cite(it["person_id"], s.get("evidence",""))}</td></tr>')
    a('</table></div></details></section>')

    # §4 orgs
    a(f'<section><h2>4 · Organizations located in France</h2><p>{len(K["orgs"])} orgs carry a KG '
      '<span class="mono">located_in</span> or <span class="mono">toured_in</span> edge to Paris, France, Metz or Drancy. "People" counts the person edges into each org. '
      'Touring companies get <span class="mono">toured_in</span> since the 2026-11-15 fix.</p>')
    a('<div class="tablewrap"><table><tr><th>node</th><th>organization</th><th>type</th><th>relation</th><th class="num">people</th></tr>')
    loc_of = defaultdict(set)
    for e in located:
        loc_of[e["source_id"]].add(f'{e["edge_type"]} {places[e["target_id"]]["en"]}')
    for o, v in sorted(K["orgs"].items(), key=lambda x: (-org_people.get(x[0], 0), x[1]["yi"])):
        fl = ""
        a(f'<tr><td><span class="mono">{esc(o)}</span></td><td class="rtlcell">{esc(sp(v["yi"]))}</td>'
          f'<td>{esc(v["type"])}{fl}</td><td>{esc(", ".join(sorted(loc_of[o])))}</td><td class="num">{org_people.get(o, 0)}</td></tr>')
    a('</table></div>')
    a('<p>The itineraries name these troupes and venues for stations in France most often (verbatim, unmerged):</p><p>')
    a(" · ".join(f'<span class="yi">{esc(t)}</span> <span class="count">{n}</span>' for t, n in venues.most_common(40)))
    a('</p></section>')

    # §5 narrative
    a('<section><h2>5 · What the lexicon narrates: four French layers</h2>')
    a(f'<p>Stations are grouped by year, as stated or interpolated from narrative order. {undated} stations carry no date '
      'and appear only in the timeline page.</p>')
    for i, title in enumerate(PER):
        rows = per_st[i]
        a(f'<h3>{title} <span class="count">{len(rows)} stations · {len({it["person_id"] for it, _ in rows})} entries</span></h3>')
        a(PERIOD_TEXT[i])
        a('<details><summary>Stations</summary><div class="tablewrap"><table><tr><th>date</th><th>entry</th>'
          '<th>place</th><th>kind</th><th>troupe / org</th><th>what happened</th><th>source</th></tr>')
        for it, s in sorted(rows, key=lambda x: (int(x[1]["t_start"]), x[0]["heading"])):
            d = "; ".join(e.get("description", "") for e in s["events"][:2])
            a(f'<tr><td class="num">{when(s)}</td><td>{who(it)}</td><td>{esc(s["label_en"])}</td>'
              f'<td>{esc(s["verb"])}</td><td class="rtlcell">{esc(sp(s["org"]))}</td><td>{esc(d)}</td>'
              f'<td>{cite(it["person_id"], s.get("evidence",""))}</td></tr>')
        a('</table></div></details>')
    a('</section>')

    # §6 places
    a('<section><h2>6 · France beyond Paris</h2><p>Paris accounts for '
      f'{places_fr["Paris"]} of {len(S)} stations. The other places:</p><div class="tablewrap"><table>'
      '<tr><th>place</th><th class="num">stations</th><th>who</th></tr>')
    for p, n in places_fr.most_common():
        if p == "Paris":
            continue
        whos = sorted({sp(it["heading"]).split(",")[0] for it, s in S if (s["label_en"] or "(unresolved)") == p})
        a(f'<tr><td>{esc(p)}</td><td class="num">{n}</td><td class="rtlcell">{esc(" · ".join(whos[:12]))}'
          f'{" …" if len(whos) > 12 else ""}</td></tr>')
    a('</table></div><p class="note">"France" alone means the entry names the country without a city. '
      'Gemini assigned the country, so a station in Metz before 1918 can still read "France". London appears where an '
      'entry runs "London–Paris" together.</p></section>')

    # §7 war
    war = sorted(per_st[2], key=lambda x: int(x[1]["t_start"]))
    a(f'<section><h2>7 · 1939–1945</h2>{WAR_TEXT}<div class="tablewrap"><table><tr><th>date</th><th>entry</th>'
      '<th>place</th><th>kind</th><th>what happened</th><th>source</th></tr>')
    for it, s in war:
        d = "; ".join(e.get("description", "") for e in s["events"][:2])
        a(f'<tr><td class="num">{when(s)}</td><td>{who(it)}</td><td>{esc(s["label_en"])}</td><td>{esc(s["verb"])}</td>'
          f'<td>{esc(d)}</td><td>{cite(it["person_id"], s.get("evidence",""))}</td></tr>')
    a('</table></div></section>')

    # §8 coverage + method
    kg_src = {e["provenance_person_id"] for e in K["edges"] if e["provenance_person_id"]}
    a(f"""<section><h2>8 · Coverage: KG vs. lexicon</h2>
<div class="tablewrap"><table><tr><th>layer</th><th class="num">France facts</th><th class="num">entries</th></tr>
<tr><td>KG (bio + org + events)</td><td class="num">{len(K['edges'])}</td><td class="num">{len(kg_src)}</td></tr>
<tr><td>itineraries (this dossier)</td><td class="num">{len(S)} stations, {sum(len(s['events']) for _, s in S)} events</td><td class="num">{len(ents)}</td></tr>
</table></div>
<p>Most KG France edges are org→place edges ({len(located)} of {len(K['edges'])}).
The KG has no person→place edge for "played in Paris". The itinerary extraction has been run three times now
(Istanbul, Danzig, France) and is the obvious source for <span class="mono">stayed_in / toured_in</span> edges.</p>
</section>
<section><h2>9 · Method</h2>
<ol>
<li><b>Corpus.</b> {len(ids)} entries of <span class="mono">people/entry_texts.tsv</span> that name Paris, France, Metz,
Alsace-Lorraine, Drancy, Vichy, Pithiviers, Gurs, Nice or Marseille (points stripped). {len(ids) - len(ents) - len(failed)} of them
yielded no French station (the word is a book title, the adjective "French", or similar).</li>
<li><b>Extraction.</b> <span class="mono">itinerary/extract_itineraries.py</span> (Gemini 3 Flash, 6 shards), with a France
prompt that adds a <span class="mono">country</span> field to every station. {len(failed)} entries returned malformed JSON
twice and are not covered: {", ".join(f'<span class="mono">{esc(p)}</span>' for p in failed)}.</li>
<li><b>Resolution.</b> Shared resolver (<span class="mono">build_itinerary_data.py</span>), then the France-only
<span class="mono">france_dossier/resolve_france.py</span> for spellings the gazetteer lacks. A station counts as French
when Gemini says so or its QID is French.</li>
<li><b>Sources.</b> Each row links to the Leksikon volume, columns and archive.org scan via
<span class="mono">lexicon_ref.py</span> (vol. 7 is not online). Review table:
<span class="mono">plays/france_dossier/france_stations_resolved.tsv</span>.</li>
</ol></section>
</main></body></html>""")
    p = DOCS / "france_dossier.html"
    p.write_text("".join(out), encoding="utf-8")
    print(f"wrote {p} ({p.stat().st_size:,} bytes); stations {len(S)}, entries {len(ents)}, "
          f"perf {len(perf)}, failed {len(failed)}; "
          f"panel {len(panel)} entries, {(DOCS / 'france_entries.json').stat().st_size:,} bytes")


PERIOD_TEXT = [
    """<p>Paris is a station on the road west, and a place to study: the Sorbonne, the art academy. The 1889 Exposition
drew Goldfadn's troupe, Katsman and others. The Stein Brothers quartet's Paris exhibition appearance is dated 1887 in Frand's entry. A troupe in the
Latin Quarter staged <span class="yi">רש"י</span> (January 1889). Leon Gelis's and Morris Axelrod's troupes, Anna Held
and Max Rosenthal, and the International Socialist Congress of 1889 (Krantz, Miller) also appear.</p>""",
    """<p>The densest layer. It covers the founding of the Yiddish actors' union (Troyman, 1914) and the Peretz-Theater
(1915), then the resident companies: Blumenthal, Burshteyn (the Yiddish Folks-Theater, 1925; the first revue theatre,
1926), Kompaneyets, the theatre in the rue de Lancry, and PIAT under David Licht (from 1934). Metz had a Yiddish community
stage after 1919. Guests came constantly: the Vilna Troupe (also Metz, Nancy, Strasbourg), Maurice Schwartz (1924, 1929),
Kessler, Zaslavsky, Fishzon, Molly Picon at the Alhambra (1931), the Yiddish Bande (1938), and Modicut's puppets
(1929).</p>""",
    """<p>Flight to Bordeaux, Vichy and the south. Internment, with theatre in the camps: Rosenfarb's group in
Beaune-la-Rolande and Gedulda's in Drancy (1941). Then the round-ups in Paris, deportation, Vittel as an exchange camp, and
the Resistance. See §7.</p>""",
    """<p>Survivors and their deaths: Max Kompaneyets in 1949, "from the effects of Nazi torture". Slovès's plays written
after the liberation, and postwar recitals, amateur circles and guest tours.</p>""",
]
WAR_TEXT = """<p>Every French station dated 1939–45. The bio layer records almost none of this (see the problems above).</p>"""

if __name__ == "__main__":
    main()
