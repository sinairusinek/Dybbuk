# prose + map script for build_dybbuk_pages.py (exec'd there)
INTRO = """
<section>
<h2>Data problems found — and fixed</h2>
<p>All four problems this dossier first reported were fixed on 2026-11-15 (details in §9–§10).</p>
<ol>
<li><span class="flag">Lateiner's comedy was credited to his wife.</span> The possessive „לאַטיינערס" had auto-linked to
Izela Lateiner (<span class="mono">person:927</span>), so she "authored" ten of his plays, the Dybbuk among them. Five
more spellings (<span class="yi">יוסף לאַטיינער</span>, <span class="yi">י. לאטיינער</span> …) had minted stray "unmatched" authors.
<b>Fixed:</b> all six spellings are ALIGNed to Joseph Lateiner (<span class="mono">person:683</span>) in
<span class="mono">kg_link_review.tsv</span>, and 26 author edges moved.</li>
<li><span class="flag">The Odessa date was an OCR error, not a KG error.</span> The transcription of Mogulesko's entry read
<span class="yi">אין אָדעס אין 1889</span>; the printed page (<a href="https://archive.org/details/nybc201090/page/n199/mode/1up">vol. 2,
cols. 1183–1184</a>) reads <b>1880</b>, as the KG event <span class="mono">EV-0194</span> already had it. <b>Fixed</b> in the vol. 2
TEI and <span class="mono">entry_texts.tsv</span>. Blank's entry (<a href="https://archive.org/details/nybc201089/page/n101/mode/1up">vol. 1,
cols. 171–172</a>) really does print 1889 for the Lateiner–Mogulesko troupe in Odessa. That is the Leksikon's own claim and
it stands as such.</li>
<li><span class="flag">The Danzig dossier cited deleted events.</span> <b>Fixed:</b> the page now explains that both Danzig events
(the Dybbuk and „גרינע פֿעלדער") left the L/H KG with the 11-01 attribution guard, and links here.</li>
<li><span class="flag">Premiere place and four impossible dates.</span> Nachbush's entry made the Vilna Troupe premiere happen
"in Vilna", and four stagings were dated 1914–16, before the play existed. In each case the extractor took the actor's
engagement date or the troupe's name. <b>Fixed</b> by hand corrections in <span class="mono">build_dybbuk_pages.py</span>, listed in §10.</li>
</ol>
</section>
"""
WORKS_NOTE = """
<p class="note">"Called the Dybbuk" covers more than one play. Lateiner's comedy also goes by its subtitle
<span class="yi">די באָבע חוה</span>. Ansky's play goes by its first title, <span class="yi">צווישן צוויי וועלטן</span>
(Between Two Worlds), and in Hebrew <span class="yi">הדיבוק</span>. Shomer's play was a pastiche of three Goldfadn
pieces released under the name <span class="yi">דער דיבוק</span>. The parodies have their own titles:
<span class="yi">מיטן כּח פֿון דבוק</span>, <span class="yi">דבוקיאַדע</span>, <span class="yi">די דריי דבוקים</span>, and
Modicut's puppet <span class="yi">דבוק</span>. Rocca's opera <i>Il Dibuk</i> (1934) reached the Warsaw Opera. The 1937
film is Waszyński's, from Alter Kacyzne's screenplay. Ansky's other play, <span class="yi">טאָג און נאַכט</span>, is
listed in §10 but is not a Dybbuk.</p>
"""
PROBLEMS = """
<p>The automatic checks and hand corrections behind the list at the top.</p>
<ul>
<li><b>Names are merged for display only.</b> Spellings such as Herman / Hermann, Morewski / Morevski / Morevsky,
Arnstein / Marek Arnstein and Ben-Ari / R. Ben-Ari are grouped by surname and first initial. People with the
same surname and different first names (Ester-Rokhl and Ida Kaminska, Jonas and Zygmunt Turkow, Melekh and
Shloyme Hershkovitsh) stay apart. No KG or people_db row is touched.</li>
<li><b>Roles come from the sentence.</b> "Director" sometimes means whoever led the troupe. Where an entry
calls someone both actor and director, both rows appear.</li>
</ul>
"""
METHOD = """<section><h2>{sec} · Method</h2>
<ol>
<li><b>Corpus.</b> All 146 entries of <span class="mono">people/entry_texts.tsv</span> that contain
<span class="yi">דיבוק · דבוק · דיבעק · צווישן צוויי וועלטן · באָבע חוה</span> (points stripped).</li>
<li><b>Extraction.</b> <span class="mono">plays/dybbuk_dossier/extract_dybbuk.py</span> (Gemini 3 Flash,
temperature 0). Each mention is classified to a work, and its facts are drawn as type, place, troupe, venue, dates
and people with role and character, each with a verbatim quote.</li>
<li><b>Checks.</b> <span class="mono">flatten_dybbuk.py</span> resolves places with the itinerary resolver and
attaches Leksikon volume and columns plus the archive.org scan link (vols. 1–6) via
<span class="mono">lexicon_ref.py</span>. <span class="mono">build_dybbuk_pages.py</span> verifies quotes against the
entry text, flags impossible dates and applies the hand overrides.</li>
<li><b>Review table.</b> <span class="mono">plays/dybbuk_dossier/dybbuk_facts.tsv</span> holds one row per fact.</li>
</ol></section>"""
WORK_TEXT = {
 "ansky": """<p>The tables below follow the play from Ansky's writing (1911–14, Tarnów in his own entry) and
Bialik's Hebrew translation (Vilna, 1918), through the Habima studio rehearsals in Moscow (Stanislavski, then
Vakhtangov, with designs by Altman and music by Engel), to the Vilna Troupe premiere in Warsaw on 9 December 1920,
directed by David Herman with music by M. Shneur. They then trace its spread: Maurice Schwartz's Yiddish Art Theatre
in New York (1921, music by Cherniavsky), the Vilna Troupe's own tours to New York, London, Western Europe and
Romania, Polish, German and English versions, provincial troupes from Łódź to Kishinev and Buenos Aires, and the
Łódź ghetto. Most of it falls in the 1920s.</p>""",
 "lateiner": """<p>Lateiner's third play, written in Iași (his own entry dates it 1877). The entries tell of
stagings in Botoșani (Juvelier as Falik, with Itsik-Mendl Berman's troupe) and Odessa (Mogulesko as Bobe Khave),
with Gimpel's troupe in Lemberg (Lager as Falek, 1888), and of Shoengold "as the lover", recalled by
B. Weinstein. This is the only Dybbuk in the plays KG (see §9). Its characters are encoded in the YiDraCor edition,
<span class="mono">YiDraCor/tei/Dybbuk4Dracor.xml</span>.</p>""",
 "shomer": """<p>Fishzon's entry tells how Shomer, competing with Goldfadn, stitched together
<span class="yi">באָבע יאָכנע</span>, <span class="yi">די באָבע מיטן אייניקל</span> and a third piece and released them
as <span class="yi">דער דיבוק</span>. Young Avrom Fishzon debuted in it as the old grandmother and toured southern Russia
in these "bobe roles". The extractor's dates (1870/1880) are approximate.</p>""",
 "parody": """<p>The Dybbuk's success bred parodies almost at once. In Warsaw, Kipnis and Yeushson's
<span class="yi">מיטן כּח פֿון דבוק</span> premiered on 3 April 1921, directed by Samberg. In New York in 1926,
Zuni Maud's puppet Dybbuk opened at Modicut (music by Gelbart, with Yosl Kotler), and Modicut toured it to Paris,
London and Antwerp in 1929. In Warsaw in 1931, R. Shoshana's <span class="yi">די דריי דבוקים</span> was staged with
Morris Lampe and Lazar Kahan.</p>""",
 "opera_music": """<p>One entry, Władysław Weintraub's, says the Warsaw Opera invited him to design
the Dybbuk with "the music of the Italian composer Rocca", that is, Lodovico Rocca's <i>Il Dibuk</i>.</p>""",
 "film": """<p>Before the 1937 film there were failed projects: Morevski's Berlin plan (1921), a 1925 Merkur
scenario he was to co-direct with Biegański, and Kacyzne's manuscript, which Lubitsch read and turned down. The 1937
film by Michał Waszyński, from Kacyzne's adaptation, starred Liebgold (Khonon), Lili Liliana (Leah), Morevski,
Bozyk and Dina Halpern (Frade).</p>""",
}
MAP_JS = r"""
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const D = __PAYLOAD__;
const map = L.map('map', {scrollWheelZoom:false}).setView([48, 15], 4);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 12,
  attribution: '&copy; OpenStreetMap contributors'}).addTo(map);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const on = Object.fromEntries(Object.keys(D.works).map(w => [w, true]));
const layer = L.layerGroup().addTo(map);
function draw(){
  layer.clearLayers();
  const g = {};
  for (const p of D.pts){ if(!on[p.w]) continue;
    const k = p.w + '|' + p.q + '|' + (p.ph==='c' ? 'c' : 's');
    (g[k] ||= {p, items: []}).items.push(p); }
  const groups = Object.values(g).sort((a,b) => b.items.length - a.items.length);
  for (const {p, items} of groups){
    const col = D.works[p.w][1], creation = p.ph === 'c';
    const region = !/^Q\d+$/.test(p.q);
    const m = L.circleMarker([p.lat, p.lon], {radius: 4 + 2.2*Math.sqrt(items.length),
      color: col, weight: creation ? 2.5 : 1, fillColor: col,
      fillOpacity: creation ? 0.08 : 0.55, dashArray: region ? '3 3' : null});
    items.sort((a,b) => String(a.y||'9').localeCompare(String(b.y||'9')));
    m.bindPopup('<b>' + esc(p.en) + '</b> · ' + esc(D.works[p.w][0]) + '<br>' + items.map(i =>
      '<div style="margin-top:.35rem"><b>' + esc(i.y || '—') + '</b> ' + esc(i.ft) +
      (i.tr ? ' · <span dir="rtl">' + esc(i.tr) + '</span>' : '') +
      (i.ve ? ' · <span dir="rtl">' + esc(i.ve) + '</span>' : '') +
      (i.list ? ' <i>(one of several places)</i>' : '') + '<br>' + esc(i.d) + '<br><span dir="rtl">' +
      esc(i.h) + '</span> ' + (i.u ? '<a href="' + esc(i.u) + '" target="_blank">' + esc(i.c) + '</a>' : esc(i.c)) +
      '</div>').join(''));
    m.addTo(layer);
  }
}
const fl = document.getElementById('filters');
for (const [w, [name, col]] of Object.entries(D.works)){
  const n = D.pts.filter(p => p.w === w).length;
  const l = document.createElement('label');
  l.innerHTML = '<input type="checkbox" checked> <span class="dot" style="background:' + col + '"></span>' + esc(name) + ' <span class="count">' + n + '</span>';
  l.querySelector('input').onchange = e => { on[w] = e.target.checked; draw(); };
  fl.appendChild(l);
}
draw();
</script>
"""
