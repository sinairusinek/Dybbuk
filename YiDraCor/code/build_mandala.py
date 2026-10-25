"""Build the Lateiner–Hurwitz mandala page from the entity graph.

The page was assembled by hand in a session scratchpad and could not be
rebuilt by anyone else — §4 of `docs/mandala_spec.md` has listed that as the
standing debt since the first build, and a concurrent session has twice
overwritten hand-edits that were not yet committed.

So the page is now three committed pieces:

    code/mandala/head.html   doctype, CSS, chrome, up to `window.__MANDALA__=`
    <the payload>            built here, from data/entity_graph.json
    code/mandala/tail.html   `</script>`, the app's JS, `</html>`

`head.html` and `tail.html` are hand-written and stay hand-written — they are
the design. Only the payload is generated. Running this is idempotent: it
rebuilds the same bytes from the same graph.

Supersedes `enrich_mandala_songs.py`, which rewrote only the song field of an
already-built page.

    python3 code/build_mandala.py            # write the page
    python3 code/build_mandala.py --check    # verify it is up to date, write nothing
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAPH = ROOT / "data" / "entity_graph.json"
SRC = ROOT / "code" / "mandala"
OUT = ROOT.parent / "docs" / "Visualizations" / "lateiner_hurwitz_mandala.html"

MARKER = "window.__MANDALA__="

# --- genre -------------------------------------------------------------

# `genre` is empty on 86% of works, so colour comes from the free-text `tags`.
# A compound value ("historical operetta; operetta") resolves by the first
# family in this order that appears in it — so the order IS the priority.
FAMILIES = ["biblical", "historical", "operetta", "melodrama", "tsaytbild",
            "gezang", "comedy", "drama", "other", "untyped"]

FAMILY_PATTERNS = [
    ("biblical", r"biblical|bibl\.|tanakh"),
    ("historical", r"historical|histor"),
    ("operetta", r"operet|opera"),
    ("melodrama", r"melodram"),
    ("tsaytbild", r"tsaytbild|tsayt-bild|lebnsbild|lebensbild|zeitbild"),
    ("gezang", r"gezang|gesang|musical|music drama"),
    ("comedy", r"comedy|komedi|comic|humoresk|farce|vaudeville"),
    ("drama", r"drama|tragedy|tragedi"),
]


def family_of(work: dict) -> str:
    raw = " ".join(str(work.get(k) or "") for k in ("tags", "genre")).lower()
    if not raw.strip():
        return "untyped"
    for fam, pat in FAMILY_PATTERNS:
        if re.search(pat, raw):
            return fam
    return "other"


# --- dates -------------------------------------------------------------

def parse_date(work: dict):
    """The composition year, as precisely as the catalogue states it.

    The column mixes `1896`, `c. 1896`, `1896?`, `before 1900` and ranges, and
    the page renders each differently — so the shape is kept, never flattened
    to a bare number.
    """
    raw = str((work.get("extras") or {}).get(
        "Year of Composition (C) /Publication (PB) /Performance (PF)") or "").strip()
    if not raw:
        return None
    # A two-digit tail is an abbreviated range end: "1877-78" is 1877-1878,
    # and reading only the 4-digit tokens would flatten it to a single year.
    raw_range = re.search(r"\b(1[6-9]\d{2})\s*[-–—/]\s*(\d{2}|1[6-9]\d{2})\b", raw)
    years = re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", raw)
    if not years:
        return None
    low = raw.lower()
    note = ""
    m = re.search(r"\bacc\.?\s*([^()]+)", raw)
    if m:
        note = ("acc. " + m.group(1)).strip().rstrip("(,;")
    certain = not re.search(r"\?|\bc\.|\bca\.|\bcirca\b", low)
    out = {"raw": raw}
    if raw_range:
        lo, hi = raw_range.group(1), raw_range.group(2)
        hi = lo[:2] + hi if len(hi) == 2 else hi
        out.update(kind="range", **{"from": int(lo)}, to=int(hi),
                   certain=certain, note=note)
        return out
    if len(years) > 1:
        out.update(kind="range", **{"from": int(years[0])}, to=int(years[-1]),
                   certain=certain, note=note)
    elif re.search(r"\bbefore\b|\bante\b|<", low):
        out.update(kind="before", year=int(years[0]), certain=certain, note=note)
    elif re.search(r"\bafter\b|\bpost\b|>", low):
        out.update(kind="after", year=int(years[0]), certain=certain, note=note)
    elif re.search(r"\bc\.|\bca\.|\bcirca\b|\?", low):
        out.update(kind="about", year=int(years[0]), certain=certain, note=note)
    else:
        # The page's dateLine() reads a plain year as kind "year" with equal
        # from/to, not as "exact" — keep its vocabulary.
        y = int(years[0])
        out.update(kind="year", **{"from": y}, to=y, certain=certain, note=note)
    return out


ATTEST_FIELDS = ["JPRESS", "Silberzweig", "Berkovitsh", "Sieger"]

NOTE_FIELDS = {
    "sources": "Sources/Availability of Text",
    "copy": "do we have a copy",
    "draws_on": "Draws on existing material",
    "note": "Details/Notes",
    "run": "Success/Length of run",
}


# --- songs -------------------------------------------------------------

# Author strings as the songbooks print them, folded onto one name each. The
# raw value stays on the node; this is display only. A conditional attribution
# ("Yozef Latayner [if 'Jüdale der Blinde']") keeps its hedge.
AUTHOR_DISPLAY = {
    "yozef latayner": "Joseph Lateiner",
    "lateiner": "Joseph Lateiner",
    "hurwitz": "Moyshe Hurwitz",
    "goldfaden": "Avrom Goldfaden",
    "אברהם גולדפאדען": "Avrom Goldfaden",
    "גאלדפאדען": "Avrom Goldfaden",
    "שלמה שמולעוויטץ": "Sholem Shmulevitsh",
    "לואיס קאפעלמאן": "Louis Kapelman",
    "וואלף עהרענקראנץ": "Velvl Ehrenkrants",
    "סעם סמאלל": "Sam Small",
    "לואיס גילראד": "Louis Gilrod",
    "יצחק ריינגאלד": "Yitskhok Reyngold",
    "ד. בערנשטיין": "D. Bernshteyn",
    "m. rosenfeld": "Morris Rosenfeld",
    "m. zeifert": "Moyshe Zeifert",
    "feinman": "Zigmund Faynman",
    "anshel schor": "Anshel Schorr",
}

# Short labels for the 10 songbooks, so a citation is readable at 11px. The
# long form rides along as a tooltip.
SOURCE_SHORT = {
    "יידיש ביהנע": "Di yidishe bihne",
    "די אידישע ביהנע : א זאמעלבוך פון ליעדער און דעקלאמאציאנען פיר גראמאפאנען":
        "Di yidishe bihne (zamelbukh)",
    "Shund on Shellac": "Shund on Shellac",
    "ליעדער מאגאזין No. II": "Lider magazin II",
    "לידער אונד קופלעטן פון די נייעסטע טהעאטער שטיקע "
    "(פערלאג פון די היברו פאבלישינג קאמפאני NYPL)": "Lider un kupletn (HPC)",
    "אללע ליעדער : פון פראפ. הורוויצעס נייעסטע מייסטערווערק "
    "דער משוגענער פילאזאף / פערפאסט פון פראפ. מ' איש הורוויץ הלוי":
        "Alle lider: Der meshugener filozof",
    "פערשיעדענע דעקלאמאציאנען אונד טהעאטער-ליעדער":
        "Fershiedene deklamatsyonen un teater-lieder",
    "דאס פידעלע: יודישע טהעאטער און פאלקס ליעדער (די היברו פאבלישינג קאמפאני, NYPL)":
        "Dos fidele (HPC)",
    "אללע ליעדער : פון דיא בעריהמטע אפערא דוד בן ישי":
        "Alle lider: Dovid ben Yishai",
    "Yankev und Eysev notebook from the Perlmutter collection":
        "Yankev un Eysev notebook (Perlmutter)",
}


def author_display(raw: str) -> str:
    if not raw:
        return ""
    key = raw.strip().lower()
    if key in AUTHOR_DISPLAY:
        return AUTHOR_DISPLAY[key]
    m = re.match(r"^(.*?)\s*(\[.*\])$", raw.strip())
    if m and m.group(1).strip().lower() in AUTHOR_DISPLAY:
        return f"{AUTHOR_DISPLAY[m.group(1).strip().lower()]} {m.group(2)}"
    return raw.strip()


def source_short(raw: str) -> str:
    raw = (raw or "").strip().rstrip(".")
    for k, v in SOURCE_SHORT.items():
        if k.rstrip(".") == raw:
            return v
    return raw


def clean_label(s: str) -> str:
    """Drop a trailing catalogue id from a person or org name.

    The Leksikon index numbers ride along on the label —
    `אַבּראַמאָוויטש, מאַקס )מענדל( 6:1` — and they are an index key, not part
    of anyone's name. They stay on the node; only the display drops them.
    """
    return re.sub(r"\s*\d{1,6}:\d{1,3}(?:,\s*\d{1,6}:\d{1,3})*\s*$", "",
                  s or "").strip()


def num(v):
    """'6.0' -> 6. openpyxl hands every count over as a float."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == int(f) else f


def page_label(a: dict) -> str:
    p = num(a.get("page_printed"))
    if p is not None:
        return f"p. {p}"
    p = num(a.get("page_pdf"))
    return f"PDF p. {p}" if p is not None else ""


def has_hebrew(s: str) -> bool:
    return any("HEBREW" in unicodedata.name(c, "") for c in s)


def song_payload(n: dict, work: dict | None) -> dict:
    yid = (n.get("yiddish_title") or "").strip()
    label = (n.get("label") or "").strip()
    rom = label if label and not has_hebrew(label) else ""
    rom = rom or (n.get("romanized_title") or "").strip()
    src_sp = (n.get("title_in_source") or "").strip()
    if src_sp and src_sp.lower() == rom.lower():
        src_sp = ""

    s = {"id": n["id"]}
    if yid:
        s["yid"] = yid
    if rom:
        s["rom"] = rom
    if src_sp:
        s["src_title"] = src_sp

    auth = author_display(n.get("author_raw", ""))
    if auth:
        s["by"] = auth
        pw = (work or {}).get("playwright") or ""
        if pw and pw.split()[-1].lower() not in auth.lower():
            # A song credited to someone other than this play's author — the
            # finding the wheel marks in gold.
            s["other"] = True
    elif n.get("author_certainty") == "unknown":
        s["by_unknown"] = True

    att = []
    for a in n.get("attestations", []):
        it = {"src": source_short(a.get("source_publication", ""))}
        full = (a.get("source_publication") or "").strip()
        if full and it["src"] != full:
            it["full"] = full
        pg = page_label(a)
        if pg:
            it["pg"] = pg
        if a.get("source_url"):
            it["url"] = a["source_url"]
        r = num(a.get("recordings"))
        if r:
            it["rec"] = r
        att.append(it)
    if att:
        s["att"] = att

    r = num(n.get("recordings"))
    if r:
        s["rec"] = r
    if n.get("elaboration"):
        s["note"] = n["elaboration"]
    if n.get("link_status_to_work") == "reviewed":
        s["reviewed"] = True
    return s


# --- the payload -------------------------------------------------------

def build(graph: dict) -> dict:
    nodes = {n["id"]: n for n in graph["nodes"]}
    # 2 works are MERGED tombstones (Di tsigaynerin 3850, Virdzhinus 3920),
    # each redirecting to the work that won on the evidence. The page carried
    # them as live plays and drew them twice over.
    works = [n for n in graph["nodes"] if n["kind"] == "work"
             and n.get("status") != "MERGED"]

    people_by_work = defaultdict(list)
    orgs_by_work = defaultdict(list)
    editions_by_work = defaultdict(list)
    events_by_work = defaultdict(list)
    songs_by_work = defaultdict(list)

    # work -> edition, so an edition's imprint can be read off its own edges
    realised = defaultdict(list)
    for e in graph["edges"]:
        if e["rel"] == "realised_as":
            realised[e["src"]].append(e["dst"])

    # each edition's imprint, gathered from the edges pointing AT it
    imprint = defaultdict(dict)
    for e in graph["edges"]:
        dst = nodes.get(e["dst"], {})
        if dst.get("kind") != "edition":
            continue
        src = nodes.get(e["src"], {})
        if e["rel"] == "holds":
            imprint[e["dst"]]["holder"] = src.get("label")
        elif e["rel"] == "printed_by":
            imprint[e["dst"]]["printer"] = src.get("label")   # authoritative
        elif e["rel"] == "published":
            # The app's Editions panel reads `printer`, and these 15 edges are
            # what it has always shown there. `printed_by` (7 edges) is the
            # true printer and wins when a work has both.
            imprint[e["dst"]].setdefault("printer", src.get("label"))
    for e in graph["edges"]:
        if e["rel"] == "printed_in":
            imprint[e["src"]]["place"] = nodes.get(e["dst"], {}).get("label")

    # events: venue and place come off the event's own edges
    ev_venue, ev_place = {}, {}
    for e in graph["edges"]:
        if e["rel"] == "performed_at":
            # 11 events carry two `performed_at` edges (Bas-Sheve names both
            # the Thalia and the Bowery). The first is the one the catalogue
            # states; last-wins silently swapped the venue on every one.
            ev_venue.setdefault(e["src"], nodes.get(e["dst"], {}).get("label"))
        elif e["rel"] == "premiered_in":
            ev_place[e["src"]] = nodes.get(e["dst"], {}).get("label")

    for n in graph["nodes"]:
        if n["kind"] == "event" and n.get("work_id"):
            events_by_work[n["work_id"]].append({
                "id": n["id"],
                "date": n.get("date") or "",
                "type": n.get("event_type") or "",
                "venue": ev_venue.get(n["id"]) or ev_place.get(n["id"]) or "",
            })
        elif n["kind"] == "song" and n.get("work_id"):
            songs_by_work[n["work_id"]].append(n)

    for wid, eids in realised.items():
        for eid in eids:
            ed = nodes.get(eid)
            if not ed:
                continue
            row = {"id": eid, "kind": ed.get("kind_of") or ed.get("kind2")
                   or ("manuscript" if "MS" in (ed.get("folder") or "")
                       else "print")}
            row["year"] = num(ed.get("year_printed")) or ""
            row["folder"] = ed.get("folder") or ""
            imp = imprint.get(eid, {})
            for k in ("holder", "printer", "place"):
                row[k] = imp.get(k) or ""
            ns = len([s for s in songs_by_work.get(wid, [])])
            if ns:
                row["songs"] = ns
            editions_by_work[wid].append(row)

    # credits: every person/org edge that lands on a work, minus the
    # playwright — he is the centre already, and drawing him again on his own
    # play is the duplication Sinai called out in spec §3 B.
    for e in graph["edges"]:
        if e["rel"] in ("wrote", "realised_as", "part_of", "of_work"):
            continue
        dst, src = nodes.get(e["dst"], {}), nodes.get(e["src"], {})
        # Most credit edges point person/org -> work, but `performed_at` runs
        # work -> org. Reading only one direction dropped every venue from 24
        # plays.
        if dst.get("kind") == "work":
            other, wid = src, e["dst"]
        elif src.get("kind") == "work" and dst.get("kind") in ("person", "org"):
            other, wid = dst, e["src"]
        else:
            continue
        # `performed_at` on an ORG is a venue credit; the app's ROLE_LABEL
        # already renders it that way and the page has always said "venue".
        role = "venue" if (e["rel"] == "performed_at"
                           and other.get("kind") == "org") else e["rel"]
        row = {"id": other.get("id") or (e["dst"] if other is dst else e["src"]),
               "name": clean_label(other.get("label") or ""), "role": role}
        # A venue credited on two separate stagings of the same play yields
        # two identical `performed_at` edges; the credit is one fact. Distinct
        # ROLES for the same name are kept — Mogulesko both composed for and
        # acted in Tisa Esler, and the panel shows that as "also credited as".
        bucket = (people_by_work if other.get("kind") == "person"
                  else orgs_by_work if other.get("kind") == "org" else None)
        if bucket is not None and not any(r["id"] == row["id"] and r["role"] == row["role"]
                     for r in bucket[wid]):
            bucket[wid].append(row)

    authors = build_authors(graph, works)
    short_of = {a["id"]: a["short"] for a in authors}

    out_works = []
    for w in works:
        wid = w["id"]
        pw = w.get("playwright") or ""
        notes = {k: (w.get("extras") or {}).get(f)
                 for k, f in NOTE_FIELDS.items()}
        notes = {k: v for k, v in notes.items() if v}
        attest = [f for f in ATTEST_FIELDS if (w.get("extras") or {}).get(f)]
        out_works.append({
            "id": wid,
            "title": w.get("label") or "",
            "yiddish": w.get("yiddish_title") or "",
            "pw": pw,
            "fam": family_of(w),
            "tags": w.get("tags") or "",
            "attr": w.get("attribution") or "",
            "date": parse_date(w),
            "attest": attest,
            "notes": notes,
            "events": events_by_work.get(wid, []),
            "editions": editions_by_work.get(wid, []),
            "songs": [song_payload(s, w) for s in sorted(
                songs_by_work.get(wid, []),
                key=lambda s: (-(num(s.get("recordings")) or 0),
                               (s.get("label") or "").lower()))],
            "people": people_by_work.get(wid, []),
            "orgs": orgs_by_work.get(wid, []),
        })

    totals = {}
    for n in graph["nodes"]:
        if n.get("status") != "MERGED":
            totals[n["kind"]] = totals.get(n["kind"], 0) + 1

    return {"authors": authors, "works": out_works,
            "families": FAMILIES, "totals": totals}


def build_authors(graph: dict, works: list) -> list:
    """The two playwrights, with the portraits the page inlines.

    The portrait bytes are NOT in the graph — they are base64 in head.html's
    neighbour, the previous payload. They are carried over verbatim from the
    page being replaced, so this script never re-encodes an image.
    """
    prev = previous_payload()
    by_short = {a["short"]: a for a in (prev.get("authors") or [])}
    counts = {}
    for w in works:
        pw = w.get("playwright") or ""
        counts[pw] = counts.get(pw, 0) + 1
    out = []
    for short in ("Lateiner", "Hurwitz"):
        a = dict(by_short.get(short) or {"short": short})
        a["n"] = counts.get(short, a.get("n", 0))
        out.append(a)
    return out


def previous_payload() -> dict:
    """The payload currently in the built page, for fields the graph lacks."""
    if not OUT.exists():
        return {}
    h = OUT.read_text(encoding="utf-8")
    try:
        i = h.index(MARKER) + len(MARKER)
    except ValueError:
        return {}
    try:
        d, _ = json.JSONDecoder().raw_decode(h[i:])
        return d
    except json.JSONDecodeError:
        return {}


def render() -> str:
    graph = json.loads(GRAPH.read_text(encoding="utf-8"))
    payload = build(graph)
    head = (SRC / "head.html").read_text(encoding="utf-8")
    tail = (SRC / "tail.html").read_text(encoding="utf-8")

    # The chrome quoted 277/60/217 as literals, so dropping the two MERGED
    # tombstones left the masthead contradicting the wheel below it. The
    # counts come from the payload now and cannot drift again.
    works = payload["works"]
    rich = sum(1 for w in works
               if w["editions"] or w["events"] or w["songs"])
    counts = {
        "WORKS": len(works),
        "RICH": rich,
        "THIN": len(works) - rich,
        "EDITIONS": sum(len(w["editions"]) for w in works),
        "EVENTS": sum(len(w["events"]) for w in works),
        "SONGS": sum(len(w["songs"]) for w in works),
    }
    for token, value in counts.items():
        head = head.replace(f"<!--@{token}@-->", str(value))
    return head + json.dumps(payload, ensure_ascii=False,
                             separators=(",", ":")) + tail


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="verify the built page matches the sources; write nothing")
    args = ap.parse_args()

    page = render()
    if args.check:
        if not OUT.exists():
            print(f"MISSING {OUT}")
            return 1
        same = OUT.read_text(encoding="utf-8") == page
        print("up to date" if same else "STALE — run without --check")
        return 0 if same else 1

    OUT.write_text(page, encoding="utf-8")
    d = json.loads(page[page.index(MARKER) + len(MARKER):
                        page.index("</script>", page.index(MARKER))])
    print(f"wrote {OUT.relative_to(ROOT.parent)}  ({len(page)//1024} KB)")
    print(f"  works {len(d['works'])}, songs "
          f"{sum(len(w['songs']) for w in d['works'])}, "
          f"events {sum(len(w['events']) for w in d['works'])}, "
          f"editions {sum(len(w['editions']) for w in d['works'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
