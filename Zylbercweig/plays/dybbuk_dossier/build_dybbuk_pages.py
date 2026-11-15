#!/usr/bin/env python3.11
"""Build docs/Visualizations/dybbuk_dossier.html from dybbuk_data.json (+ the KG).

    python3.11 flatten_dybbuk.py && python3.11 build_dybbuk_pages.py

Checks applied here (reported on the page, never silently dropped):
  * verbatim: each fact's evidence quote must occur in its entry (points/punctuation
    ignored); a mention whose quotes never name a dybbuk is marked unverified.
  * date sanity: an Ansky staging dated before 1920 outside the Habima studio.
  * OVERRIDES: hand reclassifications after reading the entry.
"""
import csv, html, json, re, sys, unicodedata
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLAYS = HERE.parent
DOCS = PLAYS.parent.parent / "docs" / "Visualizations"
ENTRY_TEXTS = PLAYS.parent / "people" / "entry_texts.tsv"
csv.field_size_limit(10**9)
POINTS = re.compile(r"[֑-ׇ]")
STEM = re.compile(r"דיבוק|דבוק|דיבעק|דבעק|דבוקים|צווישן צוויי וועלטן|צווישען צוויי וועלטען|באבע חוה")
esc = html.escape

WORK_META = {
    "ansky": ("S. An-ski, „דער דיבוק“ / „צווישן צוויי וועלטן“", "#0e6f8a"),
    "lateiner": ("Joseph Lateiner, „דער דיבוק, אָדער די באָבע חוה“", "#a1462e"),
    "shomer": ("Shomer (N. M. Shaykevitsh), „דער דיבוק“", "#7a5c1e"),
    "parody": ("Parodies and travesties", "#8a3f8f"),
    "opera_music": ("Opera: Lodovico Rocca, „Il Dibuk“", "#3f6e2a"),
    "film": ("Film: „דער דיבוק“ (1937) and earlier film projects", "#444"),
}
CREATION = {"writing", "translation", "adaptation", "revision", "composition", "design",
            "rehearsal", "reading", "publication"}
STAGING = {"premiere", "production", "performance", "tour", "film"}

# (entry heading substring, evidence substring) -> new work code, reason
OVERRIDES = [
    ("שלום-עליכם", "יידן-ליבע", "figurative",
     "the assistant director's 'dybbuk of love for Jews' — figurative, not the play"),
    ("דאגים", "טאָג און נאַכט", "related",
     "„טאָג און נאַכט“ — Ansky's OTHER play, completed from his fragments by Dogim and Buzgan"),
]

# (person_id, mention, fact) -> field fixes, reason. Checked against the entry text 2026-11-15.
FACT_FIXES = {
    ("P-2-facs_112_tr_1744279063", 0, 0): ({"date_start": ""}, "1915 is when Lares joined the Vilna Troupe; the entry dates no Dybbuk performance"),
    ("P-2-facs_310_tr_1744365749", 0, 0): ({"place": "", "place_en": "", "points": [], "label_en": "", "lat": None, "lon": None},
                                           "the entry names no place for the premiere; 'Vilna' came from the troupe's name (it opened in Warsaw)"),
    ("P-2-facs_310_tr_1744365749", 0, 1): ({"date_start": ""}, "1916 is the start of Nachbush's touring with the Vilna Troupe, not of the Dybbuk"),
    ("P-2-facs_330_tr_1744365791", 0, 0): ({"date_start": ""}, "1914 belongs to Summerson's Argentine career in general; no Dybbuk date is given"),
    ("P-6-facs_105_tr_1747825152", 0, 0): ({"date_start": ""}, "1914 is when Fridman joined Kompaneyets's troupe in Melitopol; the Dybbuk came later, undated"),
}

ROLE_ORDER = ["author", "translator", "adaptor", "director", "assistant_director", "composer",
              "set_designer", "costume_designer", "choreographer", "conductor", "actor",
              "producer", "manager", "prompter", "reader", "critic", "other"]
ROLE_LABEL = {"author": "Author", "translator": "Translator", "adaptor": "Adaptor / reviser",
              "director": "Director", "assistant_director": "Assistant director",
              "composer": "Composer", "set_designer": "Set designer",
              "costume_designer": "Costume designer", "choreographer": "Choreographer",
              "conductor": "Conductor", "actor": "Actor", "producer": "Producer",
              "manager": "Manager / impresario", "prompter": "Prompter", "reader": "Reader",
              "critic": "Critic / witness", "other": "Other"}


def bidi(name):
    """Escape, then isolate each „…“ Yiddish title so it doesn't scramble the English around it."""
    return re.sub(r"„([^“]*)“", r'„<bdi dir="rtl" class="yi">\1</bdi>“', esc(name))


def nrm(s):
    s = unicodedata.normalize("NFC", s or "")
    s = POINTS.sub("", s).replace("פֿ", "פ")
    s = re.sub(r"[„“”\"'׳״‟‚‘’\[\](){}.,:;!?…—–\-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ---- people: display-level name merging (the data is never rewritten) ----
def surname_key(name_en):
    n = re.sub(r"\[.*?\]|\(.*?\)", " ", name_en or "")
    n = n.replace(",", " ").strip()
    toks = [t for t in re.split(r"\s+", n) if t]
    if not toks:
        return "", ""
    if "," in (name_en or "") and len(toks) > 1:     # "Blank, Yekhiel"
        last, first = toks[0], toks[1]
    else:
        last, first = toks[-1], (toks[0] if len(toks) > 1 else "")
    k = last.lower()
    k = k.replace("w", "v").replace("ch", "kh").replace("sch", "sh").replace("tz", "ts")
    k = re.sub(r"(sky|skiy|ski|sk[iy]j)$", "ski", k)
    k = re.sub(r"(nn)$", "n", k)
    k = re.sub(r"[aeo]$", "", k) if len(k) > 5 else k    # Orleska/Orlesko, Kaminska!
    k = k.replace("-", "")
    return k, first[:1].lower() if first and not re.match(r"^(dr|prof|r)\.?$", first.lower()) else ""


SAME_PERSON = {  # spelling variants the rules miss; display-level only
    "s an ski": "S. An-ski", "s an sky": "S. An-ski", "an ski": "S. An-ski",
    "h n bialik": "H. N. Bialik", "bialik": "H. N. Bialik",
    "madiokat": "Modicut (puppet theatre)", "modim liampe": "Morris Lampe",
    "lazar kahn": "Lazar Kahan", "rose shoshana": "R. Shoshana", "shoshana": "R. Shoshana",
    "al stein": "Alyosha Stein", "alex stein": "Alyosha Stein",
    "azra elmit": "Elmira", "b yeushson": "B. Yeushson",
    "shloyme naumov zaslavsky": "Shloyme Naumov",
    "lateiner joseph": "Joseph Lateiner", "mogulesko sigmund": "Zygmunt Mogulesko",
    "blank yekhiel": "Yekhiel Blank", "lager aron": "Aron Lager",
    "itsik mendl berman": "Itsik-Mendl Berman",
}
KEEP_APART = {("kaminsk", "e"), ("kaminsk", "i")}   # Ester-Rokhl vs Ida Kaminska


def build_people(facts):
    """-> {work: {role: [person dicts]}} with merged spellings."""
    occ = []   # (work, canonical-key, record)
    for f in facts:
        for p in f["people"]:
            nm = (p.get("name_en") or POINTS.sub("", p.get("name", ""))).strip()
            fixed = SAME_PERSON.get(nrm(nm).lower())
            if fixed:
                nm = fixed
            occ.append((f["work"], nm, p, f))
    # group by surname key; merge initial-only/first-less names into a unique full form
    groups = defaultdict(list)
    for w, nm, p, f in occ:
        sk, fi = surname_key(nm)
        groups[sk].append((w, nm, fi, p, f))
    canon = {}
    for sk, items in groups.items():
        firsts = {fi for _, _, fi, _, _ in items if fi}
        for w, nm, fi, p, f in items:
            if not fi and len(firsts) == 1:
                fi = next(iter(firsts))
            canon[id(p)] = (sk, fi)
    # display name = most frequent full form inside (sk, fi)
    disp = defaultdict(Counter)
    for w, nm, p, f in occ:
        disp[canon[id(p)]][nm] += 1 + len(nm) / 100
    out = defaultdict(lambda: defaultdict(dict))
    for w, nm, p, f in occ:
        key = canon[id(p)]
        name = disp[key].most_common(1)[0][0]
        role = p.get("role") or "other"
        if role not in ROLE_LABEL:
            role = "other"
        rec = out[w][role].setdefault(key, {"name": name, "yi": Counter(), "chars": Counter(),
                                            "details": Counter(), "src": {}, "entry": "",
                                            "years": set(), "places": Counter(), "troupes": Counter()})
        rec["yi"][POINTS.sub("", p.get("name", ""))] += 1
        if p.get("character"):
            rec["chars"][POINTS.sub("", p["character"]).strip("„“\"' ")] += 1
        if p.get("role_detail"):
            rec["details"][POINTS.sub("", p["role_detail"])] += 1
        if p.get("is_entry_subject"):
            rec["entry"] = f["person_id"]
        rec["src"][f["person_id"]] = (f["heading"], f["cols"], f["scan"])
        y = (f.get("date_start") or "")[:4]
        if y.isdigit():
            rec["years"].add(int(y))
        if f.get("label_en"):
            rec["places"][f["label_en"]] += 1
        if f.get("troupe"):
            rec["troupes"][POINTS.sub("", f["troupe"])] += 1
    return out


def load_texts(ids):
    t = {}
    with open(ENTRY_TEXTS, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["person_id"] in ids:
                t[r["person_id"]] = nrm(r["entry_text"])
    return t


def verbatim(q, text):
    q = nrm(q)
    if not q:
        return False
    if q in text:
        return True
    w = q.split()   # the model splices quotes across ellipses: 4-word windows, half must hit
    wins = [" ".join(w[i:i + 4]) for i in range(0, max(1, len(w) - 3), 2)]
    return bool(wins) and sum(x in text for x in wins) >= max(1, len(wins) // 2)


def src_link(heading, cols, scan, pid):
    lab = f'<span class="yi">{esc(heading)}</span>'
    meta = f'{esc(cols)}' if cols else esc(pid)
    if scan:
        return f'{lab} <a class="src" href="{esc(scan)}" title="{esc(pid)}">{meta}</a>'
    return f'{lab} <span class="src" title="{esc(pid)}">{meta}</span>'


def yr(f):
    y = (f.get("date_start") or "")[:4]
    return int(y) if y.isdigit() else None


def main():
    data = json.load(open(HERE / "dybbuk_data.json", encoding="utf-8"))
    facts, entries = data["facts"], data["entries"]
    texts = load_texts(set(entries))

    # ---- overrides + checks ----
    applied = []
    for f in facts:
        f["flags"] = []
        fx = FACT_FIXES.get((f["person_id"], f["mention"], f["fact"]))
        if fx:
            old = {k: f.get(k) for k in fx[0] if k != "points"}
            f.update(fx[0])
            applied.append((f["heading"], ", ".join(f"{k}={v}" for k, v in old.items() if v and k in ("date_start", "place_en")),
                            "cleared", fx[1], f))
        for h, ev, new, why in OVERRIDES:
            if h in f["heading"] and nrm(ev) in nrm(f["evidence"]):
                applied.append((f["heading"], f["work"], new, why, f))
                f["work"] = new
        f["verbatim"] = verbatim(f["evidence"], texts.get(f["person_id"], ""))
        if not f["verbatim"]:
            f["flags"].append("quote not found verbatim")
    # a mention is verified if any of its quotes is verbatim AND names a dybbuk
    by_mention = defaultdict(list)
    for f in facts:
        by_mention[(f["person_id"], f["mention"])].append(f)
    unverified = []
    for k, fs in by_mention.items():
        ok = any(f["verbatim"] and STEM.search(nrm(f["evidence"])) for f in fs)
        # quotes that are verbatim but elide the title: accept if the entry names it
        if not ok and any(f["verbatim"] for f in fs) and STEM.search(texts.get(k[0], "")):
            ok = "entry"
        for f in fs:
            f["verified"] = bool(ok)
        if not ok:
            unverified.append(fs[0])
    date_flags = []
    for f in facts:
        y = yr(f)
        if (f["work"] == "ansky" and f["fact_type"] in STAGING and y and y < 1920
                and "Moscow" not in (f.get("label_en") or "")):
            f["flags"].append(f"Ansky staging dated {y}, before the 1920 premiere")
            date_flags.append(f)

    good = [f for f in facts if f["verified"] and f["work"] in WORK_META]
    people = build_people(good)

    # ---- stats ----
    n_entries = len({f["person_id"] for f in good})
    n_people = sum(len(v) for w in people.values() for v in w.values())
    places = {(p[0]) for f in good for p in f["points"] if f["fact_type"] in STAGING | CREATION}
    n_stagings = sum(1 for f in good if f["fact_type"] in STAGING)
    by_work = Counter(f["work"] for f in good)
    ent_by_work = defaultdict(set)
    for f in good:
        ent_by_work[f["work"]].add(f["person_id"])

    # ---- KG slice: the Lateiner play is the only Dybbuk the L/H KG holds ----
    N = {r["node_id"]: r for r in csv.DictReader(open(PLAYS / "kg/nodes.tsv"), delimiter="\t")}
    kg_edges = [e for e in csv.DictReader(open(PLAYS / "kg/edges.tsv"), delimiter="\t")
                if "play:PL-0133" in (e["source_id"], e["target_id"])]
    kg_events = [v for v in csv.DictReader(open(PLAYS / "kg/events.tsv"), delimiter="\t")
                 if v["play_id"] == "play:PL-0133"]
    excl = [r for r in csv.DictReader(open(PLAYS / "kg/excluded_play_facts.tsv"), delimiter="\t")
            if re.search("דיבוק|דבוק", r["play_title_surface"])]
    excl_reasons = Counter(re.sub(r":.*", "", r["reason"]) for r in excl)
    lab = lambda i: N.get(i, {}).get("label_english") or POINTS.sub("", N.get(i, {}).get("label_yiddish", i))

    # ---- map payload ----
    pts = []
    for f in good:
        for q, en, lat, lon in f["points"]:
            if lat is None:
                continue
            pts.append({"w": f["work"], "q": q, "en": en, "lat": lat, "lon": lon,
                        "ph": "c" if f["fact_type"] in CREATION else ("s" if f["fact_type"] in STAGING else "r"),
                        "ft": f["fact_type"], "y": f["date_start"], "tr": POINTS.sub("", f["troupe"]),
                        "ve": POINTS.sub("", f["venue"]), "d": f["description"],
                        "h": f["heading"], "c": f["cols"], "u": f["scan"],
                        "list": len(f["points"]) > 1})

    H = []
    a = H.append
    a(HEAD)
    a(f"""<main>
<p class="eyebrow">Dybbuk · Plays KG × Zylbercweig Leksikon · 2026-11-15</p>
<h1>The Dybbuks</h1>
<p class="lede">Every play, parody, opera and film called <i>The Dybbuk</i> —
<span class="yi">דער דיבוק · דבוק · צווישן צוויי וועלטן</span> — that the Leksikon fun yidishn teater
mentions, with the people around each one, in every creative, professional and dramatic role, and
the historical geography of its writing, adaptation and productions.</p>
<p style="font-size:.9rem"><b>Companion dossiers:</b> <a href="istanbul_dossier.html">Istanbul</a> ·
<a href="danzig_dossier.html">Danzig</a> · <a href="france_dossier.html">France</a> ·
<a href="rumshinsky_dossier.html">Joseph Rumshinsky</a>.</p>
<div class="statrow">
  <div class="stat"><b>{len(by_work)}</b><span>distinct works</span></div>
  <div class="stat"><b>{n_entries}</b><span>lexicon entries</span></div>
  <div class="stat"><b>{n_people}</b><span>people × role</span></div>
  <div class="stat"><b>{n_stagings}</b><span>stagings / tours / films</span></div>
  <div class="stat"><b>{len(places)}</b><span>places</span></div>
  <div class="stat"><b>{len(kg_events)}</b><span>KG events (Lateiner only)</span></div>
</div>
""")
    a(INTRO)

    # §1 works table
    a('<section><h2>1 · The works</h2>')
    a('<p>Every mention was assigned to one work from its context: the author named, the date, '
      'the characters, the troupe. The counts are of lexicon entries and extracted facts.</p>')
    a('<div class="tablewrap"><table><tr><th>work</th><th>entries</th><th>facts</th>'
      '<th>earliest</th><th>titles used in the Leksikon</th></tr>')
    for w, (name, col) in WORK_META.items():
        fs = [f for f in good if f["work"] == w]
        if not fs:
            continue
        ys = [yr(f) for f in fs if yr(f)]
        titles = Counter(POINTS.sub("", f["work_title"]).strip("„“\"' ") for f in fs)
        alts = Counter(POINTS.sub("", f["alt_title"]).strip("„“\"' ") for f in fs if f["alt_title"])
        tl = " · ".join(f'<span class="yi">{esc(t)}</span>' for t, _ in (titles + alts).most_common(6) if t)
        a(f'<tr><td><span class="dot" style="background:{col}"></span><a href="#w-{w}">{bidi(name)}</a></td>'
          f'<td class="num">{len(ent_by_work[w])}</td><td class="num">{len(fs)}</td>'
          f'<td class="num">{min(ys) if ys else "—"}</td><td>{tl}</td></tr>')
    a('</table></div>')
    a(WORKS_NOTE)
    a('</section>')

    # §2 map
    a(f"""<section><h2>2 · Historical geography</h2>
<p>Each dot is a place where an entry puts a step in the life of a Dybbuk: writing, translation, music,
design or rehearsal (<b>ring</b>) or a premiere, production, tour or film (<b>filled</b>). Size grows
with the number of facts. A tour that names several cities puts a dot in each. Click a dot for its
facts and their Leksikon columns.</p>
<div id="filters" class="filters"></div>
<div id="map"></div>
<p class="note">Places resolve against the project gazetteer (<span class="mono">toponyms_gazetteer.csv</span>
+ KG place nodes). Regions ("Poland", "Romania", "America") are drawn at a centroid with a dashed edge.
{sum(1 for f in good if (f['place'] or f['place_en']) and not f['points'])} facts name a place that did
not resolve; they are listed in the tables below but not mapped.</p>
</section>""")

    # §3.. per work: chronology + people
    sec = 3
    for w, (name, col) in WORK_META.items():
        fs = [f for f in good if f["work"] == w]
        if not fs:
            continue
        a(f'<section id="w-{w}"><h2>{sec} · <span class="dot big" style="background:{col}"></span>{bidi(name)}</h2>')
        a(WORK_TEXT.get(w, ""))
        # chronology
        def row(f):
            ppl = "; ".join(
                f'{esc(p.get("name_en") or p.get("name",""))}'
                + (f' <span class="yi">({esc(POINTS.sub("", p["character"]))})</span>' if p.get("character") else "")
                + f' <span class="role">{esc(p.get("role",""))}</span>'
                for p in f["people"][:8]) + (" …" if len(f["people"]) > 8 else "")
            where = esc(f["label_en"] or f["place_en"] or "")
            if len(f["points"]) > 1:
                where = esc(", ".join(p[1] for p in f["points"]))
            fl = "".join(f' <span class="flag" title="{esc(x)}">⚑</span>' for x in f["flags"])
            tv = " · ".join(esc(POINTS.sub("", x)) for x in (f["troupe"], f["venue"]) if x)
            return (f'<tr><td class="num">{esc(f["date_start"] or "—")}{fl}</td>'
                    f'<td><span class="chip">{esc(f["fact_type"])}</span></td><td>{where}</td>'
                    f'<td class="rtlcell">{tv}</td><td>{ppl}</td>'
                    f'<td>{src_link(f["heading"], f["cols"], f["scan"], f["person_id"])}</td></tr>')
        key = lambda f: (f["date_start"] or "9999", f["label_en"] or "")
        for title, sel in (("Creation, translation and adaptation", CREATION),
                           ("Premieres, productions, tours, films", STAGING),
                           ("Reception and other", None)):
            sub = sorted([f for f in fs if (f["fact_type"] in sel if sel else
                          f["fact_type"] not in CREATION | STAGING)], key=key)
            if not sub:
                continue
            a(f'<h3>{title} <span class="count">{len(sub)}</span></h3>')
            a('<div class="tablewrap"><table><tr><th>date</th><th>step</th><th>place</th>'
              '<th>troupe · venue</th><th>people</th><th>source</th></tr>')
            a("".join(row(f) for f in sub))
            a('</table></div>')
        # people by role
        a(f'<h3>People by role <span class="count">{sum(len(v) for v in people[w].values())}</span></h3>')
        a('<div class="tablewrap"><table><tr><th>role</th><th>person</th><th>character / detail</th>'
          '<th>where · when</th><th>sources</th></tr>')
        for role in ROLE_ORDER:
            recs = people[w].get(role)
            if not recs:
                continue
            recs = sorted(recs.values(), key=lambda r: (-len(r["src"]), r["name"]))
            for i, r in enumerate(recs):
                yi = r["yi"].most_common(1)[0][0] if r["yi"] else ""
                det = " · ".join(f'<span class="yi">{esc(c)}</span>' for c, _ in r["chars"].most_common(3))
                if not det:
                    det = " · ".join(f'<span class="yi">{esc(c)}</span>' for c, _ in r["details"].most_common(2))
                yrs = sorted(r["years"])
                when = (f"{yrs[0]}" + (f"–{yrs[-1]}" if len(yrs) > 1 and yrs[-1] != yrs[0] else "")) if yrs else ""
                wh = ", ".join(p for p, _ in r["places"].most_common(3))
                srcs = " ".join(
                    (f'<a class="src" href="{esc(u)}" title="{esc(h)} · {esc(c)}">{esc(h.split(",")[0])}</a>'
                     if u else f'<span class="src" title="{esc(pid)}">{esc(h.split(",")[0])}</span>')
                    for pid, (h, c, u) in list(r["src"].items())[:5])
                me = ' <span class="chip" title="has own Leksikon entry">entry</span>' if r["entry"] else ""
                a(f'<tr><td>{ROLE_LABEL[role] if i == 0 else ""}</td>'
                  f'<td>{esc(r["name"])}{me}<br><span class="yi small">{esc(yi)}</span></td>'
                  f'<td>{det}</td><td>{esc(" · ".join(x for x in (wh, when) if x))}</td><td>{srcs}</td></tr>')
        a('</table></div>')
        a('</section>')
        sec += 1

    # KG section
    ev_rows = "".join(
        f'<tr><td><span class="mono">{esc(v["event_id"])}</span></td><td>{esc(v["event_type"])}</td>'
        f'<td>{esc(lab(v["place_id"]))}</td><td>{esc(lab(v["org_id"]) if v["org_id"] else lab(v["venue_id"]) if v["venue_id"] else "")}</td>'
        f'<td class="num">{esc(v["date_start"])}</td><td class="rtlcell">{esc(POINTS.sub("", v["evidence_sentence"])[:160])}</td></tr>'
        for v in kg_events)
    per_rows = "".join(
        f'<tr><td><span class="mono">{esc(e["edge_id"])}</span></td><td>{esc(lab(e["source_id"]))}</td>'
        f'<td>{esc(e["edge_type"])}</td><td class="rtlcell">{esc(POINTS.sub("", e["character"]))}</td>'
        f'<td class="num">{esc(e["date_start"])}</td></tr>'
        for e in kg_edges if e["source_id"].startswith("person:"))
    a(f"""<section id="kg"><h2>{sec} · What the knowledge graph holds</h2>
<p>The plays KG is built for Lateiner's and Hurwitz's plays only, so the one Dybbuk it holds is
<b>Lateiner's comedy</b>, <span class="mono">play:PL-0133</span>. It has {len(kg_events)} production events and
{sum(1 for e in kg_edges if e['source_id'].startswith('person:'))} person edges. The {len(excl)} facts the
extractor drew about other Dybbuks (from {len({r['fact_id'].split(':')[0] for r in excl})} entries) are held
back in <span class="mono">kg/excluded_play_facts.tsv</span> by the 11-01 attribution guard
({", ".join(f"{k} {v}" for k, v in excl_reasons.most_common())}). That is by design. It also means
that everything in §3, and the parody, opera and film sections, exists only in this dossier's extraction.</p>
<h3>Production events on PL-0133</h3>
<div class="tablewrap"><table><tr><th>event</th><th>type</th><th>place</th><th>org / venue</th><th>date</th><th>evidence</th></tr>{ev_rows}</table></div>
<h3>Person edges on PL-0133</h3>
<div class="tablewrap"><table><tr><th>edge</th><th>person</th><th>relation</th><th>character</th><th>date</th></tr>{per_rows}</table></div>
</section>""")
    sec += 1

    # data problems
    a(f'<section id="problems"><h2>{sec} · Data problems and checks</h2>')
    a(PROBLEMS)
    a(f'<h3>Ansky stagings dated before the 1920 premiere <span class="count">{len(date_flags)}</span></h3>')
    a('<p>The play was finished in 1916–17 and first staged on 9 December 1920; the Habima studio rehearsed it '
      'in Moscow from 1918. A staging dated earlier usually means the extractor took the date of the '
      'actor\'s engagement with the troupe, not of the Dybbuk. Any left after the hand corrections below are flagged ⚑ in the tables above.</p>')
    a('<div class="tablewrap"><table><tr><th>date</th><th>step</th><th>place</th><th>troupe</th><th>source</th></tr>')
    for f in sorted(date_flags, key=lambda f: f["date_start"]):
        a(f'<tr><td class="num">{esc(f["date_start"])}</td><td>{esc(f["fact_type"])}</td>'
          f'<td>{esc(f["label_en"] or f["place_en"])}</td><td class="rtlcell">{esc(POINTS.sub("", f["troupe"]))}</td>'
          f'<td>{src_link(f["heading"], f["cols"], f["scan"], f["person_id"])}</td></tr>')
    a('</table></div>')
    a(f'<h3>Reclassified by hand <span class="count">{len(applied)}</span></h3><ul>')
    for h, old, new, why, f in applied:
        a(f'<li><span class="yi">{esc(h)}</span>: <span class="mono">{esc(old)}</span> → '
          f'<span class="mono">{esc(new)}</span>. {esc(why)}.</li>')
    a('</ul>')
    a(f'<h3>Dropped as unverified <span class="count">{len(unverified)}</span></h3>')
    a('<p>These mentions are left out of every table above. None of their quotes occurs verbatim in the '
      'entry, or the quotes and the entry never name a dybbuk.</p><ul>')
    for f in unverified:
        a(f'<li><span class="yi">{esc(f["heading"])}</span> <span class="mono">{esc(f["person_id"])}</span>: '
          f'{esc(f["work"])}, {esc(f["fact_type"])}. {esc(f["description"])}</li>')
    a('</ul>')
    figur = [f for f in facts if f["work"] in ("figurative", "related", "other") and f["verified"]]
    a(f'<h3>Not a Dybbuk play <span class="count">{len(figur)}</span></h3><ul>')
    for f in figur:
        a(f'<li><span class="mono">{esc(f["work"])}</span> · <span class="yi">{esc(f["heading"])}</span>: '
          f'{esc(f["work_note"] or f["description"])} <span class="yi">„{esc(POINTS.sub("", f["evidence"])[:140])}“</span></li>')
    a('</ul></section>')
    sec += 1
    a(METHOD.replace("{sec}", str(sec)))
    a('</main>')
    a(MAP_JS.replace("__PAYLOAD__", json.dumps(
        {"pts": pts, "works": {k: v for k, v in WORK_META.items() if by_work.get(k)}},
        ensure_ascii=False)))
    a("</body></html>")
    out = DOCS / "dybbuk_dossier.html"
    out.write_text("".join(H), encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size:,} bytes); good facts {len(good)}, "
          f"unverified mentions {len(unverified)}, date flags {len(date_flags)}, overrides {len(applied)}")


HEAD = open(HERE / "page_head.html", encoding="utf-8").read() if (HERE / "page_head.html").exists() else ""
INTRO = WORKS_NOTE = PROBLEMS = METHOD = MAP_JS = ""
WORK_TEXT = {}
exec(open(HERE / "page_text.py", encoding="utf-8").read())   # prose + templates

if __name__ == "__main__":
    main()
