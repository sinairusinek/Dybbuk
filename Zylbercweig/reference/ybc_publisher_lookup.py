"""Look up a publisher's imprints in the Yiddish Book Center's digital library.

The YBC's Digital Yiddish Library — 13,661 volumes — is hosted on the Internet
Archive as collection `nationalyiddishbookcenter`, and IA's advancedsearch API
is open. That matters because the catalogues we actually want all refuse
scripted access: yiddishbookcenter.org itself returns 403, as do NLI, WorldCat
and OSU. This is the one Yiddish-publishing catalogue we can query in bulk.

What it is good for, and what it is not:

  GOOD   Settling whether two spellings of a publisher are one firm, by showing
         every imprint under that surname with its CITY and YEAR. An imprint
         series that runs continuously in one city under one initial is one
         firm; two initials running concurrently in the same city are usually
         two (often related) firms; the same name in two cities with a clean
         break between them is a move.

  NOT    Authority control. There is no publisher authority file here: the
         `publisher` field is a transcribed imprint line, so one firm appears
         as "Sh. Goldfarb", "S. Goldfarb" and "Shelomoh Goldfarb", and the city
         appears as Ṿarshe / Ṿarsha / Ṿarshoy / Warszawa in the same breath.
         Cluster the forms yourself; do not trust a raw facet count.

  NOT    Complete. It is one library's holdings, heavily weighted to literary
         Yiddish and to what survived in America. An absence proves nothing:
         Alapin and Lidski both return 0 here and both are real Warsaw houses.

Search the ROMANIZED form, not the Hebrew. The metadata is romanized in a YIVO-
ish Library-of-Congress style with combining dots (ḳ ṭ ṿ ś), and Hebrew-script
publisher strings are present on only a fraction of records. Pass several
spellings: `bzhoza OR bzshoza` finds 135 items, `bzhoza` alone finds few.

Usage:
    python3 ybc_publisher_lookup.py goldfarb golfarb
    python3 ybc_publisher_lookup.py --field title "mishke un moshke"
    python3 ybc_publisher_lookup.py --city kultur      # group by city, not initial
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
import unicodedata
import urllib.parse
import urllib.request

API = "https://archive.org/advancedsearch.php"
COLLECTION = 'collection:"nationalyiddishbookcenter"'
UA = "Dybbuk-research/1.0 (Zylbercweig org pipeline)"
FIELDS = ("identifier", "title", "publisher", "year", "creator", "language")

def strip_marks(text: str) -> str:
    """Drop combining marks so ṿ/v, ḳ/k, ṭ/t compare equal."""
    return "".join(c for c in unicodedata.normalize("NFD", text)
                   if not unicodedata.combining(c))


# The city as the imprint line spells it, mapped to something groupable. The
# transcriptions vary per cataloguer, not per city.
CITY_FORMS = [
    (r"varsh|varsa|warsz|warzaw|warsaw", "Warsaw"),
    (r"vilna|vilne|wilno", "Vilna"),
    (r"nyu[ -]?yor|new york|n\.y\.", "New York"),
    (r"kiyev|kiev", "Kiev"),
    (r"minsk", "Minsk"),
    (r"moskve|moscow", "Moscow"),
    (r"berlin", "Berlin"),
    (r"lodzh|łódź|lodz", "Lodz"),
    (r"buenos", "Buenos Aires"),
    (r"tel[- ]?aviv|yerushalayim|jerusalem", "Palestine/Israel"),
    (r"riga|rige", "Riga"),
    (r"odes", "Odessa"),
    (r"paris|pariz", "Paris"),
    (r"london", "London"),
]


def ia_search(query: str, rows: int = 200) -> dict:
    params = [("q", query), ("rows", str(rows)), ("output", "json"),
              ("sort[]", "year asc")] + [("fl[]", f) for f in FIELDS]
    url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as fh:
        return json.load(fh)["response"]


def flat(value) -> str:
    if isinstance(value, list):
        return " / ".join(str(v) for v in value)
    return str(value or "")


def city_of(imprint: str) -> str:
    """Normalise the place half of an imprint line.

    Only the text BEFORE the first colon is the place -- "Ṿarshe : Sh.
    Goldfarb". Matching the whole string instead lets a publisher's own name
    decide the city, and a record whose place is missing then borrows one from
    the firm. Several transcriptions of one city (Ṿarshe / Ṿarsha / Ṿarshoy /
    Warszawa) must collapse, or a single firm fragments across four rows.
    """
    place = imprint.split(":")[0] if ":" in imprint else imprint
    # The API returns these romanizations DECOMPOSED -- Ṿ arrives as V + U+0307,
    # so a pattern written with a precomposed Ṿ silently never matches and one
    # city fragments into a row per transcription. Strip the combining marks
    # and compare the bare letters.
    low = strip_marks(place).lower()
    for pattern, name in CITY_FORMS:
        if re.search(pattern, low):
            return name
    head = place.strip(" []").strip()
    return head[:22] or "?"


def initial_of(imprint: str, surname: str) -> str:
    """The given-name or initial standing before the surname in the imprint."""
    bare = strip_marks(imprint)
    m = re.search(r"([\w\.]+)\s+" + re.escape(strip_marks(surname)), bare, re.I)
    if not m:
        return "(bare)"
    token = m.group(1).strip(".")
    # "Farlag Goldfarb" / "Ṿarshe : Goldfarb" carry no personal name
    if token.lower() in ("farlag", "ferlag", "fg", "aroysgegebn", "fun", ":"):
        return "(bare)"
    return token


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("terms", nargs="+", help="romanized spellings to OR together")
    ap.add_argument("--field", default="publisher",
                    help="IA field to search (default publisher; try title)")
    ap.add_argument("--city", action="store_true",
                    help="group by city rather than by the name before the surname")
    ap.add_argument("--rows", type=int, default=200)
    ap.add_argument("--list", action="store_true", help="print every item")
    a = ap.parse_args()

    ors = " OR ".join(a.terms)
    query = f"{COLLECTION} AND {a.field}:({ors})"
    resp = ia_search(query, a.rows)
    docs = resp["docs"]
    print(f"{resp['numFound']} item(s) for {a.field}:({ors})"
          f"{'' if resp['numFound'] <= a.rows else f'  [showing {len(docs)}]'}\n")
    if not docs:
        print("Nothing — but note an absence here proves nothing; the YBC holds "
              "one library's volumes, not the whole Yiddish book trade.")
        return

    surname = a.terms[0]
    groups: dict[tuple, list[int]] = collections.defaultdict(list)
    for d in docs:
        imprint = flat(d.get("publisher"))
        key = (city_of(imprint),) if a.city else (initial_of(imprint, surname),
                                                  city_of(imprint))
        year = d.get("year")
        groups[key].append(int(year) if str(year).isdigit() else 0)

    width = max(len(" · ".join(k)) for k in groups)
    for key, years in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        real = sorted(y for y in years if y)
        span = f"{real[0]}–{real[-1]}" if real else "no dates"
        print(f"  {' · '.join(key):{width}}  n={len(years):3d}  {span}")

    if a.list:
        print()
        for d in docs:
            print(f"  {d.get('year','?')}  {flat(d.get('publisher'))[:70]:70}  "
                  f"{flat(d.get('title'))[:50]}")
            print(f"        https://archive.org/details/{d.get('identifier')}")

    print("\nReading these: one initial running continuously in one city is one "
          "firm; two initials concurrent in the same city are usually two "
          "related firms; one name in two cities with a clean break is a move. "
          "Spelling variants of the SAME initial (Sh./S./Shelomoh) are the "
          "cataloguer's, not the firm's.")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        sys.exit(f"Internet Archive unreachable: {e}")
