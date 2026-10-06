"""Reconcile the editions dataset against the project databases.

Reads data/editions.json (built by build_editions_dataset.py) and resolves every
real-world entity it names — playwrights, credited people, venues, places,
publishers — against the canonical databases:

  people  → Zylbercweig/people/people_db.tsv            (db_id)
  orgs    → Zylbercweig/organizations/core_db.tsv        (db_id)
  places  → Zylbercweig/zibn-shtern/.../toponyms_gazetteer.csv  (qid)

Every entity lands in exactly one bucket:

  LINKED    an exact match on a normalised name or variant → carries a db id
  PROPOSED  a high-confidence automatic candidate, NOT a decision; needs a human
            to confirm. Order-insensitive token matching, which is what the
            catalogue's "Surname, Given" vs people_db's "Given Surname" needs.
  GAP       no candidate, or several equally good ones → an open question

Outputs:
  data/entity_graph.json   nodes + edges, every node carrying its link status
  data/entity_gaps.tsv     the ledger: one row per open question

Nothing here writes a decision to any database. PROPOSED rows are proposals.
"""
from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(10 ** 7)

import work_layer

ROOT = Path(__file__).resolve().parent.parent          # YiDraCor/
REPO = ROOT.parent                                      # Dybbuk/
EDITIONS = ROOT / "data" / "editions.json"
PEOPLE_DB = REPO / "Zylbercweig" / "people" / "people_db.tsv"
CORE_DB = REPO / "Zylbercweig" / "organizations" / "core_db.tsv"
GAZETTEER = (REPO / "Zylbercweig" / "zibn-shtern" / "data" / "working"
             / "toponyms_gazetteer.csv")
OUT_GRAPH = ROOT / "data" / "entity_graph.json"
OUT_GAPS = ROOT / "data" / "entity_gaps.tsv"

# People we know the catalogue names by surname alone in the author column.
AUTHOR_DB_ID = {683: "Joseph Lateiner", 684: "Moyshe (Ish Halevi) Hurwitz"}

# ---------------------------------------------------------------------------
# Confirmed human decisions. Everything here was reviewed and accepted by a
# person; the reviewer and date are recorded so a later reader can tell these
# apart from the matcher's own automatic PROPOSED candidates.
#
# reviewer: Sinai · 2026-10-05
# Accepted the ten org spelling variants surfaced as near-spelling hints, and
# identified 'מאדאם ליפצין' (Madame Lipzin) as Keni Lipzin.
REVIEWED_BY = "Sinai 2026-10-05"

ORG_DECISION = {
    # Bucharest garden theatres; my first pass mis-transliterated both names
    # (corrected by Sinai 2026-10-05: "Jigniza", "Pomul Verde").
    "jignitatheatre": ("113", "Gradina Lieblich Jigniza"),
    "pamolverdi": ("137", "פּאמול ווערדי - Gradina Pomul verde"),
    "windsortheater": ("134", "Windsor Theatre"),
    "poolstheatre": ("123", "Poole's Theatre"),
    "tsentraltheatre": ("151", "Tsentral Theater"),
    "peoplestheatre": ("120", "People's Theatre NY"),
    "folkstheatre": ("365", "Public Theatre (Folks Theatre)"),
    "kesslersthaliatheatre": ("130", "Thalia Theatre|Bowery Theatre"),
    "thomashefskyspeopletheatre": ("156", "Thomashefsky Theatre"),
    "amkroytetfreundbuchhandlung": ("63", "אַמקרויט עט פריינד| Amkroyt un Fraynd"),
    "amkrautfreundbuchhandlung": ("63", "אַמקרויט עט פריינד| Amkroyt un Fraynd"),
    "diyudishebihne": ("1849", "די אידישע ביהנע"),
}

PERSON_DECISION = {
    "מאדאםליפצין": ("762", "קעני ליפצין (סאַכאַר קריינע סאָניעס)"),
    # Bare surname on the Goles Rusland cast list, credited `actor` alongside a
    # separate `actress` credit for Sophie Karp (723). Sinai 2026-10-05: Max.
    "קארפ": ("703", "מאַקס קאַרפּ / Max Karp"),
    # The source reads 'ה.מ ראזענטהאל' — ה׳ מ׳ = הערר מאקס (Herr Max), so this is
    # Max Rosenthal, NOT the Morris Rosenfeld the PersonKey column claims.
    # Sinai 2026-10-05.
    "המראזענטהאל": ("878", "מאַקס ראָזענטאַל / Max Rosenthal"),
    "ראזענפעלדמאריס": ("878", "מאַקס ראָזענטאַל / Max Rosenthal"),
    # קאנארד = William Conrad, ALREADY in people_db — a link, not a mint.
    # Leksikon heading "קאָנראַד, וויליאַם" (vol.4) plus a mention placing him with
    # Kessler (721), Prager (2695) and Tobias (3079); an 1897 Thalia poster (LoC
    # POS-TH-1897.T43) lists "Herr Konrad" with Prager, Kalich, Dina Feinman,
    # Kessler and Berl Bernstein (859). Our credit is Khurbn Yerusholayim 1898.
    "קאנארד": ("1464", "וויליאַם קאַנראָד / William Conrad"),
    # --- minted 2026-10-05, evidence in people/new_mints_evidence.tsv.
    # All five carry probably_not_zylbercweig=1: present in people_db but NOT a
    # Leksikon entry. Minkowski appears in the Leksikon only as 2 bare-surname
    # mentions; the other four are absent from it entirely.
    "מינקאווסקי": ("3822", "גיאַקאָמאָ מינקאָווסקי / Giacomo Minkowski"),
    "איימיסימאוויטש": ("3823", "איימי סימאָוויטש / Amy Simovitch"),
    "ליבאראבאן": ("3824", "ל. י. באַראַבאַן / L. Y. Baraban"),
    "joachimkurantman": ("3825", "יואכים קוראַנטמאַן / Joachim Kurantman"),
    "אידאקאמינסקא": ("3826", "אידא קאַמינסקאַ / Ida Kamińska"),
    # --- 23 proposals confirmed wholesale by Sinai 2026-10-05.
    # All were order-reversed or alias-extended forms of the same name;
    # the matcher found them, a person approved them.
    "וואהלהערמאןצבי": ("906", "הערמאַן וואָהל (צבי)"),
    "פרידזעללואיס": ("695", "לואיס פרידזעל"),
    "אבראמאוויטשמאקסמענדל": ("694", "מאַקס אַבּראַמאָוויטש (מענדל)"),
    "ווילענסקימערימריםקאץ": ("3264", "מערי ווילענסקי (מרים קאץ)"),
    "ברוידייוסף": ("852", "יוסף בּרוידי"),
    "שאיעוויטשישראל": ("2907", "ישראל שאַיעוויטש"),
    "גימפעלאדאלף": ("1766", "אַדאָלף גימפעל"),
    "שלאסבערגיצחק": ("925", "יצחק שלאָסבּערג"),
    "קעסלערדוד": ("721", "דוד קעסלער"),
    "קארפסאפיע": ("723", "סאׇפיע קאַרפּ"),
    "טאמאשעוופסקיבאריסברוךאהרן": ("693", "בּאָריס טאָמאַשעוו[פ]סקי (בּרוך-אהרן)"),
    "יאוועלירקלמן": ("2061", "קלמן יאָוועליר"),
    "פראגעררעגינע": ("2695", "רעגינע פּראַגער"),
    "סאנדלערפרץ": ("857", "פּרץ סאַנדלער"),
    "שמולעוויטששלמה": ("3611", "שלמה שמולעוויטש"),
    "פיינמאןזיגמונד": ("714", "זיגמונד פיינמאַן"),
    "ליאוולעאמשהלייב": ("932", "לעאָ ליאָוו (משה-לייבּ)"),
    "בערנשטייןבערלבורשטין": ("859", "בּערל בּערנשטיין (בּורשטין)"),
    "סענדלעריעקבקאפל": ("880", "יעקב סענדלער"),
    "רומשינסקייוסף": ("916", "יוסף רומשינסקי (רומשיסקין)"),
    "פערלמוטערארנאלד": ("2645", "אַרנאָלד פּערלמוטער (אהרל'ע)"),
    "מאגולעסקאזיגמונט": ("685", "זיגמוּנט מאׇגוּלעסקאׇ (זעליג מאָגילעווסקי)"),
    "henryrussotto": ("850", "הענרי רוסאָטאַ (חיים נעסוויזשקי)"),
    # The Leksikon files him under his stage name with the birth name in
    # brackets — "טאָביאַס, סעמועל [שמואל טאַבאַטשניקאָוו]" — so the catalogue's
    # birth-name form reaches people_db only through this link.
    "שמואלטאבאטשניקאוו": ("3079", "סעמועל טאָביאַס / Samuel Tobias"),
}

# Sub-city districts. Kima has an id for the Lower East Side (19633), but all
# 25 editions that name it also carry a named venue whose core_db row holds a
# street address (46-48 Bowery, 199 Bowery, …), so the district is redundant as
# a locus and is dropped rather than modelled — Sinai's call, 2026-10-05.
# A district is kept only where no venue is known for the event.
DISTRICT_REDUNDANT_IF_VENUE_KNOWN = {
    "lowereastsidenewyorkny": ("kima:19633", "Lower East Side (New York, N.Y.)"),
}

# Latin-script historical names the gazetteer lacks. It DOES carry these
# exonyms, but only in Yiddish (לעמבערג for Lviv, ווילנע/ווילנא for Vilnius)
# — the romanised forms a title page or an English catalogue prints are
# absent, and there are no German forms at all (no Pressburg, Breslau,
# Danzig). Checked against the gazetteer 2026-10-05.
# `Vilna` alone would otherwise match Vilna Governorate / Vilna Ghetto, i.e.
# the region and the WWII ghetto rather than the city.
PLACE_EXONYM = {
    "lemberg": ("Q36036", "Lviv"),
    "vilna": ("Q216", "Vilnius"),
    "vilne": ("Q216", "Vilnius"),
    "wilno": ("Q216", "Vilnius"),
    # source typos, confirmed by Sinai 2026-10-05
    "clevelandohaio": ("Q37320", "Cleveland"),
    "clevelandohio": ("Q37320", "Cleveland"),
}


# ---------------------------------------------------------------- normalisation

def strip_points(s: str) -> str:
    """NFKD then drop combining marks — Yiddish points must go before any
    comparison or ostensibly identical names will not match."""
    return "".join(c for c in unicodedata.normalize("NFKD", str(s or ""))
                   if not unicodedata.combining(c))


def sm(s) -> str:
    """Squash to a comparable key: unpointed, alphanumerics only, lowercase."""
    return re.sub(r"[^\w]+", "", strip_points(s)).lower()


def decision_key(s) -> str:
    """`sm()` with the Zylbercweig `vol:col` locator removed.

    Catalogue person strings trail a lexicon locator (" 208:2") that would
    otherwise have to be written into every decision key verbatim.
    """
    return sm(re.sub(r"\d+:\d+", " ", strip_points(s)))


# Imprint boilerplate around a publisher's actual name. The catalogue writes
# 'Farlag "Kultur" (Dzika 13)' and 'Verlag von Benjamin Munk Buchhandlung'
# where core_db holds the bare 'Kultur' / 'Benjamin Munk'.
_IMPRINT_NOISE = re.compile(
    r"""\b(farlag|verlag(\s+von)?|buchhandlung|buchdruckerei|druck(\s+von)?|
         drukeray|press|printing|publishers?|publishing|et|und|and)\b""",
    re.I | re.X)


def imprint_core(s: str) -> str:
    """A publisher string reduced to its distinguishing name.

    Drops imprint boilerplate, a parenthesised street address, and quotes, so
    'Farlag "Kultur" (Dzika 13)' and 'Kultur' compare equal.
    """
    s = re.sub(r"\([^)]*\)", " ", str(s or ""))     # (Dzika 13)
    s = s.replace("&", " ")
    s = _IMPRINT_NOISE.sub(" ", s)
    return s


def toks(s) -> frozenset:
    """Order-insensitive token set, locators and punctuation removed.

    The catalogue stores people as "Surname, Given (Alias) 936:1" while
    people_db stores "Given Surname (Alias)" — only a set comparison matches
    those, and the trailing `vol:col` lexicon locator has to come out first.
    """
    s = strip_points(s)
    s = re.sub(r"\d+:\d+", " ", s)       # lexicon locator
    s = re.sub(r"[^\w\s]+", " ", s)      # incl. the RTL-mangled parens
    return frozenset(t for t in s.split() if len(t) > 1 and not t.isdigit())


# ---------------------------------------------------------------- db loading

# people_db has no `deprecated`/`merged_into` column the way core_db does, so a
# merge is recorded in the survivor's `source` as "merged_from=<id>". Rows named
# there are skipped on load: keeping them indexed makes every merged pair read
# as an ambiguous match forever.
_MERGED_FROM = re.compile(r"merged_from=(\d+)")


def load_people() -> tuple[dict, dict]:
    """Return (exact index, token index) over people_db, minus merged rows."""
    exact, tokidx = {}, defaultdict(list)
    raw_rows = list(csv.DictReader(PEOPLE_DB.open(encoding="utf-8"), delimiter="\t"))
    merged = {m for r in raw_rows
              for m in _MERGED_FROM.findall(r.get("source") or "")}
    for r in raw_rows:
        if r["db_id"] in merged:
            continue
        names = [r.get("hebname"), r.get("english"), r.get("alternative_name")]
        names += (r.get("name_variants") or "").split("|")
        label = (r.get("hebname") or r.get("english") or "").strip()
        for n in names:
            if not (n or "").strip():
                continue
            exact.setdefault(sm(n), (r["db_id"], label))
            tokidx[toks(n)].append((r["db_id"], label))
    return exact, dict(tokidx)


def load_orgs() -> tuple[dict, dict, dict]:
    """Return (exact index, composite index, token index) over core_db.

    core_db stores some venues as a single `A|B` row (two names for one
    building), so index the halves too — otherwise "Bowery Theatre" misses
    the row called "Thalia Theatre|Bowery Theatre".
    """
    exact, parts, tokidx = {}, {}, defaultdict(list)
    with CORE_DB.open(encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if (r.get("deprecated") or "").strip().lower() in ("y", "yes", "true", "1"):
                continue
            label = (r.get("name") or r.get("name_yiddish") or "").strip()
            fields = [r.get("name"), r.get("name_yiddish"),
                      r.get("name_yiddish_translit")]
            fields += (r.get("name_variants") or "").split("|")
            for n in fields:
                if not (n or "").strip():
                    continue
                exact.setdefault(sm(n), (r["db_id"], label))
                # `A|B` rows name one body two ways; index each half, and each
                # half's imprint core, so a publisher written out in full still
                # finds the bare name core_db stores.
                for half in str(n).split("|"):
                    if half.strip():
                        parts.setdefault(sm(half), (r["db_id"], label))
                        core = sm(imprint_core(half))
                        if core:
                            parts.setdefault(core, (r["db_id"], label))
                        tokidx[toks(half)].append((r["db_id"], label))
    return exact, parts, dict(tokidx)


def bare_place(s: str) -> str:
    """A place name without its disambiguating qualifier.

    534 of the gazetteer's 1018 labels carry one — "Iași (Romania)",
    "Podgorze (Krakow, Poland)" — and the corpus writes its own
    ("Iași (Rumenia)"), so neither side matches until both are stripped.
    """
    return re.sub(r"\([^)]*\)", " ", str(s or ""))


def load_places() -> tuple[dict, dict]:
    """Return (exact index, token index) over the toponym gazetteer."""
    exact, tokidx = {}, defaultdict(list)
    with GAZETTEER.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            label = (r.get("label_en") or r.get("kima_rom") or "").strip()
            names = [r.get("label_en"), r.get("label_yi"),
                     r.get("kima_rom"), r.get("kima_heb")]
            # the gazetteer delimits variants with ';', not '|' — splitting on
            # the wrong character indexed all of them as one unusable string,
            # which is why the Yiddish exonyms (ווילנע, לעמבערג) never matched
            names += re.split(r"[;|]", r.get("variants") or "")
            for n in names:
                if not (n or "").strip():
                    continue
                exact.setdefault(sm(n), (r["qid"], label))
                # also index the name without its qualifier, so a corpus
                # string that carries none (or a different one) still lands
                bare = sm(bare_place(n))
                if bare:
                    exact.setdefault(bare, (r["qid"], label))
                    tokidx[toks(bare_place(n))].append((r["qid"], label))
                tokidx[toks(n)].append((r["qid"], label))
    return exact, dict(tokidx)


# ---------------------------------------------------------------- resolution

def nearest_spellings(raw: str, exact: dict, limit: int = 3,
                      floor: float = 0.86) -> list[tuple]:
    """Closest db names by character similarity — a hint, never a link.

    Catches one-character orthographic variance ("Theater"/"Theatre",
    "Pool's"/"Poole's") that token matching treats as a miss.
    """
    from difflib import SequenceMatcher
    key = sm(raw)
    if len(key) < 5:
        return []
    scored = []
    for k, (db_id, label) in exact.items():
        if abs(len(k) - len(key)) > 4:
            continue
        r = SequenceMatcher(None, key, k).ratio()
        if r >= floor:
            scored.append((r, db_id, label))
    scored.sort(key=lambda x: -x[0])
    out, seen = [], set()
    for _, db_id, label in scored:
        if db_id in seen:
            continue
        seen.add(db_id)
        out.append((db_id, label))
        if len(out) >= limit:
            break
    return out


def propose(raw: str, tokidx: dict) -> list[tuple]:
    """Order-insensitive candidates for `raw`, best first.

    A candidate qualifies when one token set contains the other (so
    "Karp" ⊂ "Sophie Karp"), scored by overlap over the longer set. Several
    equal-scoring candidates means ambiguous, which is a GAP, not a proposal.
    """
    t = toks(raw)
    if not t:
        return []
    scored = []
    for k, vals in tokidx.items():
        if not k:
            continue
        inter = len(k & t)
        if inter and (k <= t or t <= k):
            scored.append((inter / max(len(k), len(t)), k, vals))
    scored.sort(key=lambda x: -x[0])
    return scored


def resolve(raw: str, exact: dict, tokidx: dict | None = None,
            parts: dict | None = None, decisions: dict | None = None) -> dict:
    """Resolve one name string to LINKED / PROPOSED / GAP.

    `decisions` holds reviewer-confirmed links, which win over every automatic
    rule below and are stamped with the reviewer so they read as decisions
    rather than as the matcher's own guesses.
    """
    key = sm(raw)
    dkey = decision_key(raw)
    if decisions and (key in decisions or dkey in decisions):
        db_id, label = decisions.get(key) or decisions[dkey]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "reviewed", "reviewer": REVIEWED_BY}
    if key in DISTRICT_REDUNDANT_IF_VENUE_KNOWN:
        db_id, label = DISTRICT_REDUNDANT_IF_VENUE_KNOWN[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "kima-district", "reviewer": REVIEWED_BY,
                "redundant_if_venue_known": True}
    if key in exact:
        db_id, label = exact[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "exact"}

    # a historical German/Yiddish name the gazetteer does not carry
    if key in PLACE_EXONYM:
        db_id, label = PLACE_EXONYM[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "exonym"}

    # the incoming string carries a qualifier the db label does not, or vice
    # versa ("Iași (Rumenia)" vs "Iași (Romania)")
    bare = sm(bare_place(raw))
    if bare and bare != key and bare in exact:
        db_id, label = exact[bare]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "qualifier-stripped"}

    # a half of a composite `A|B` db row
    if parts and key in parts:
        db_id, label = parts[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "composite-half"}

    # the same, after stripping imprint boilerplate off a publisher string
    if parts:
        core = sm(imprint_core(raw))
        if core and core in parts:
            db_id, label = parts[core]
            return {"status": "LINKED", "db_id": db_id, "matched": label,
                    "method": "imprint-core"}

    if tokidx:
        scored = propose(raw, tokidx)
        if scored:
            top = scored[0]
            # several distinct ids tie at the top score → genuinely ambiguous
            tied = [s for s in scored if abs(s[0] - top[0]) < 1e-9]
            ids = {v[0] for s in tied for v in s[2]}
            if len(ids) > 1:
                return {"status": "GAP", "reason": "ambiguous",
                        "candidates": sorted(
                            {(v[0], v[1]) for s in tied for v in s[2]})[:6]}
            # Guard against a proposal resting on a single shared token:
            # "Rozenfeld, Moris" → a row called just "Moris", or a
            # neighbourhood → its city. Worse than admitting a gap. A
            # one-token name on BOTH sides (a mononym) is still allowed.
            score, k, vals = top
            t = toks(raw)
            if len(k & t) < 2 and max(len(k), len(t)) > 1:
                return {"status": "GAP", "reason": "weak match (one token only)",
                        "candidates": [(v[0], v[1]) for v in vals][:6]}
            db_id, label = vals[0]
            return {"status": "PROPOSED", "db_id": db_id, "matched": label,
                    "method": "token-set", "score": round(score, 3)}

    # Nothing matched. Offer the closest spellings as a hint for the reviewer:
    # "Windsor Theater" vs core_db's "Windsor Theatre" differ by one character,
    # which token matching cannot bridge but a human resolves instantly.
    near = nearest_spellings(raw, exact)
    if near:
        return {"status": "GAP", "reason": "no candidate (near spellings offered)",
                "candidates": near}
    return {"status": "GAP", "reason": "no candidate"}


# ---------------------------------------------------------------- graph build

def person_raw(role_row: dict) -> str:
    return (role_row.get("PersonKey")
            or role_row.get("PersonName as appears in Source if there is no key")
            or "").strip()


def split_venues(v) -> list[str]:
    """Venue cells hold `A|B` for one building under two names."""
    return [p.strip() for p in str(v or "").split("|") if p.strip()]


def main() -> int:
    editions = json.loads(EDITIONS.read_text(encoding="utf-8"))["editions"]
    ppl_exact, ppl_tok = load_people()
    org_exact, org_parts, org_tok = load_orgs()
    plc_exact, plc_tok = load_places()
    print(f"indexes: people={len(ppl_exact)} orgs={len(org_exact)} "
          f"places={len(plc_exact)}")

    # ---- the work layer -------------------------------------------------
    # Works are minted from the catalogue, not from the editions: all 277
    # catalogued plays get a node, including the 252 with no surviving text, so
    # a poster or a song for a lost play has something to attach to. See
    # docs/work_layer_proposal.md.
    import openpyxl
    cat = openpyxl.load_workbook(work_layer.CATALOGUE, read_only=True,
                                 data_only=True)
    works = work_layer.load_works(cat)
    work_idx = work_layer.work_title_index(works)
    songs, song_ids = work_layer.load_songs(cat, work_idx, works)
    work_layer.save_song_ids(song_ids)
    print(f"work layer: {len(works)} works, {len(songs)} songs "
          f"({sum(1 for x in songs if x['work_id'])} linked to a work)")

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    gaps: list[dict] = []

    # Authorship is a property of the WORK, so every work gets its `wrote`
    # edge here — not inside the edition loop, which would reach only the 27
    # works that happen to have a surviving edition and leave 250 authorless.
    # The catalogue spells the author two ways: the db_id `683.0` for Lateiner
    # and the bare string `Hurwitz`, which has db_id 684.
    PLAYWRIGHT_DB_ID = {"Lateiner": 683, "Hurwitz": 684}
    for w_node in works.values():
        nodes[w_node["id"]] = dict(w_node)
        aid = PLAYWRIGHT_DB_ID.get(w_node["playwright"])
        if aid:
            pid = f"person:dbid{aid}"
            nodes.setdefault(pid, {
                "id": pid, "kind": "person",
                "label": AUTHOR_DB_ID.get(aid, w_node["playwright"]),
                "status": "LINKED", "db_id": aid,
                "matched": AUTHOR_DB_ID.get(aid, ""), "method": "author_id",
                "role": "playwright"})
            # `attribution` carries the catalogue's own verdict verbatim, so a
            # `false ascription` work still names the playwright it was ascribed
            # to — with the edge saying the ascription is disputed.
            edges.append({"src": pid, "dst": w_node["id"], "rel": "wrote",
                          "attribution": w_node["attribution"]})
    for s_node in songs:
        nodes[s_node["id"]] = dict(s_node)
        # A song is a Work in its own right that is part_of the play work, not
        # an attribute of it: it carries its own sheet music, recordings and
        # composer (Sinai 2026-10-05).
        if s_node["work_id"]:
            edges.append({"src": s_node["id"], "dst": s_node["work_id"],
                          "rel": "part_of"})
        else:
            gaps.append({"kind": "song", "label": s_node["label"],
                         "reason": f"song's play key {s_node['play_key']!r} "
                                   f"resolves to no single work",
                         "candidates": ""})

    def node(kind: str, raw: str, res: dict, **extra) -> str:
        nid = f"{kind}:{sm(raw) or 'blank'}"
        if nid not in nodes:
            nodes[nid] = {"id": nid, "kind": kind, "label": raw, **res, **extra}
            if res.get("status") == "GAP":
                cands = "; ".join(f"{i} {l}" for i, l in res.get("candidates", []))
                gaps.append({"kind": kind, "label": raw,
                             "reason": res.get("reason", ""),
                             "candidates": cands or res.get("note", "")})
        return nid

    for e in editions:
        folder = e.get("folder") or e.get("title")
        eid = f"edition:{folder}"
        expr = e.get("expression") or {}
        nodes[eid] = {
            "id": eid, "kind": "edition", "label": e.get("title"),
            "status": "LINKED" if e.get("expression_id") else "GAP",
            "expression_id": e.get("expression_id"),
            "yiddish_title": e.get("catalogue_yiddish_name"),
            "year_printed": e.get("year_printed"),
            "transkribus_doc_id": e.get("transkribus_doc_id"),
            "folder": folder,
            "counts": {k: len(e.get(k) or []) for k in
                       ("productions", "roles", "songs", "performance_events")},
        }

        # ---- the WEMI spine: this edition realises a catalogued work.
        # expression_id joins all 28 edition rows to a catalogue work.
        wid = work_layer.expression_id(e.get("expression_id"))
        work_target = f"work:{wid}" if wid in works else None
        if work_target:
            edges.append({"src": work_target, "dst": eid, "rel": "realised_as"})
            # Sanity-check the join rather than trusting it. The edition's own
            # title comes off the physical title page, so it is an independent
            # witness to the expression_id copied from the catalogue.
            #
            # A short title against a long catalogue one is NORMAL — the
            # catalogue keeps the full `X oder Y` form while the title page
            # carries only `X` (Ezra / "Ezre oder der ewiger Jude"). So compare
            # against EVERY alternative in the work's title, and only complain
            # when the edition's title matches none of them. That is what
            # separates a real mix-up from an abbreviation.
            w_label = works[wid]["label"]
            ed_title = (e.get("title") or "").strip()
            # Confirmed alternative titles: the edition's title page and the
            # catalogue name the same play differently, and a human has checked
            # which. Reviewed so they stop resurfacing in the ledger.
            #
            # reviewer: Sinai 2026-10-06
            CONFIRMED_ALT_TITLE = {
                # Zylbercweig: performed as „גבריאל דער מאַלער“ and in Europe
                # as חינקע און פּינקע.
                "3877": "HinkePinke",
                # Zylbercweig: often performed in Europe as „מישקע און מאָשקע“
                # and „די אייראפעער אין אַמעריקע“.
                "3838": "MishkeMashke-Kultur1910",
                # Zylbercweig: staged in Europe as 'Soreh Shayndel fun
                # Yehupets'; Berkovitsh also lists 'soreh shayndel'.
                "3833": "SoreSheyndel",
                # Sinai 2026-10-06: "Meshumed IS an alternative title for Goles
                # Rusland."
                "3879": "Lateiner_Meshumed",
                # 3933's own `comments` column reads "Yosef in egipten", and it
                # is the catalogue's ONLY Joseph play — the other Egypt titles
                # are Hurwitz's K'sav toyreh (3983) and Yetsies mitsrayim
                # (4043). The MS title page credits Lateiner
                # ("פערפאסט פון לאטיינער"), matching 3933's author 683.
                "3933": "MS_YoysefInEgipten",
                # The same title in a different romanisation or short form —
                # Sinai 2026-10-06: "the next four are ok". Kept listed rather
                # than widened into the fold rules, because each rests on a
                # human reading of the title page, not on a transform.
                "3963": "MS_BasKoyen",        # Bas Koyen / Bas Cohen oder, Malka Alexandra
                "4012": "MS_DiTsveyTnoim",    # tnoim / tanoyim
                "4014": "MS_YaakovEsav",      # Yaakov-Esav / Yanḳev un Eysev
                "3830": "MS_Emigration",      # German "nach America" / Yiddish "nokh Amerike"
            }
            if CONFIRMED_ALT_TITLE.get(wid) == folder:
                ed_title = ""        # confirmed; not a question any more
            alts = [a for a in re.split(r"\boder\b|\bodr\b|,", w_label)
                    if work_layer.translit_key(a)]
            ed_key = work_layer.translit_key(ed_title)
            if ed_title and ed_key and not any(
                    work_layer.translit_key(a).startswith(ed_key)
                    or ed_key.startswith(work_layer.translit_key(a))
                    for a in alts):
                gaps.append({
                    "kind": "edition-field",
                    "label": f"{folder} · expression_id",
                    # Phrased as a review item, not a verdict: several of
                    # these are genuine alternative performance titles rather
                    # than errors. Zylbercweig records that Gabriel (3877) was
                    # played in Europe as חינקע און פּינקע, and the catalogue
                    # itself gives Mishke Mashke the play_key "Di grinhorns".
                    # But MS_KhurbnYerusholaim pointing at 3887 "Khave oder di
                    # shlang" IS wrong — and the duplicate row holds the correct
                    # 3891. A human has to tell these apart.
                    # Phrased as a review item, not a verdict: several of
                    # these are genuine alternative performance titles rather
                    # than errors. Zylbercweig records that Gabriel (3877) was
                    # played in Europe as חינקע און פּינקע, and the catalogue
                    # itself gives Mishke Mashke the play_key "Di grinhorns".
                    # But MS_KhurbnYerusholaim pointing at 3887 "Khave oder di
                    # shlang" IS wrong — and the duplicate row holds the correct
                    # 3891. A human has to tell these apart. The reason stays
                    # generic so it groups in the summary; the specifics go in
                    # `candidates`.
                    "reason": "work/edition titles differ — alternative title, "
                              "or a wrong expression_id? needs review",
                    "candidates": (f"expression_id {wid} = {w_label!r}; "
                                   f"edition title = {ed_title!r}")})
        else:
            gaps.append({"kind": "edition-field",
                         "label": f"{folder} · expression_id",
                         "reason": f"expression_id {wid!r} is not a catalogued "
                                   f"work, so the edition has no work",
                         "candidates": ""})

        # Work-level facts attach to the work when we have one, and fall back to
        # the edition only when we do not. Authorship, cast, composers and
        # performances are properties of the PLAY: hanging them on a printing
        # made the graph claim Mishke Mashke was performed in 1889 by a book
        # printed in 1911.
        wl = work_target or eid

        # ---- playwright: already an id in the data, so LINKED by construction
        aid = expr.get("author_id")
        if aid:
            pid = f"person:dbid{aid}"
            nodes.setdefault(pid, {
                "id": pid, "kind": "person", "label": e.get("author"),
                "status": "LINKED", "db_id": aid,
                "matched": AUTHOR_DB_ID.get(aid, ""), "method": "author_id",
                "role": "playwright"})
            # Only when this edition has no work: the work layer already
            # emitted the authorship edge for every one of the 277 works, so
            # repeating it here would double-count.
            if not work_target:
                edges.append({"src": pid, "dst": wl, "rel": "wrote"})
        else:
            gaps.append({"kind": "person", "label": e.get("author") or "(blank)",
                         "reason": "edition has no author_id", "candidates": ""})

        # ---- credited people
        for r in e.get("roles") or []:
            raw = person_raw(r)
            if not raw:
                continue
            res = resolve(raw, ppl_exact, ppl_tok, decisions=PERSON_DECISION)
            # Role cells sometimes carry the character too ("actor: Lemekh");
            # keep the relation clean and move the character to its own field.
            role_raw = str(r.get("Role") or "credited").strip()
            rel, _, character = role_raw.partition(":")
            nid = node("person", raw, res, role=rel.strip().lower())
            # Coarse granularity: the Leksikon attests that this person played
            # this role in this play, with no event named. That claim stands on
            # its own evidence and is not a placeholder — see
            # work_layer.coarse_to_fine.
            edges.append({"src": nid, "dst": wl, "rel": rel.strip().lower(),
                          "character": character.strip() or None,
                          "level": "work" if work_target else "edition",
                          "source": r.get("source"), "context": r.get("context")})

        # ---- venues + premiere places, from productions
        for p in e.get("productions") or []:
            # A hafakot row typed `publication` is a print event, not a staging:
            # its `Theatre` cell holds the publishing house (e.g. "The
            # International Biblioteque"), so it must not become a venue edge.
            is_publication = str(p.get("Type") or "").strip().lower() == "publication"
            for v in split_venues(p.get("Theatre")):
                res = resolve(v, org_exact, org_tok, org_parts, decisions=ORG_DECISION)
                if is_publication:
                    nid = node("org", v, res, org_role="publisher")
                    edges.append({"src": nid, "dst": eid, "rel": "published",
                                  "year": p.get("Year"), "source": p.get("source")})
                    continue
                nid = node("org", v, res, org_role="venue")
                edges.append({"src": wl, "dst": nid, "rel": "performed_at",
                              "year": p.get("Year"), "type": p.get("Type")})
            if p.get("PremierePlace"):
                res = resolve(p["PremierePlace"], plc_exact, plc_tok)
                nid = node("place", p["PremierePlace"], res)
                edges.append({"src": wl, "dst": nid, "rel": "premiered_in",
                              "year": p.get("Year")})

        # ---- performance EVENTS, as nodes of their own.
        # These rows carry a date, so they identify a specific staging rather
        # than the general fact that the play was performed somewhere. That is
        # the fine granularity: a cast fact attached here can say who was on
        # stage that night, which an edge to the work never can.
        for ev in e.get("performance_events") or []:
            venues = (split_venues(ev.get("venue"))
                      + split_venues(ev.get("venue_alt")))
            date = ev.get("date")
            ev_target = wl
            if date and work_target:
                # One node per (work, date, first venue): the report lists a
                # venue and an alternative spelling of the same venue, not two
                # venues, so the event must not be split on them.
                ev_id = (f"event:{wid}-{re.sub(r'[^0-9]', '', str(date))}"
                         f"-{sm(venues[0]) if venues else 'novenue'}")
                if ev_id not in nodes:
                    nodes[ev_id] = {
                        "id": ev_id, "kind": "event", "label":
                            f"{nodes[work_target]['label']} · {date}",
                        "status": "LINKED", "method": "performance_events_report",
                        "date": date, "event_type": ev.get("event_type"),
                        "work_id": work_target,
                    }
                    edges.append({"src": ev_id, "dst": work_target,
                                  "rel": "of_work"})
                ev_target = ev_id
            for v in venues:
                res = resolve(v, org_exact, org_tok, org_parts, decisions=ORG_DECISION)
                nid = node("org", v, res, org_role="venue")
                edges.append({"src": ev_target, "dst": nid, "rel": "performed_at",
                              "date": date, "type": ev.get("event_type"),
                              "level": "event" if ev_target != wl else "work"})

        # ---- imprint: publisher org + publication place
        if e.get("publisher"):
            res = resolve(e["publisher"], org_exact, org_tok, org_parts, decisions=ORG_DECISION)
            nid = node("org", e["publisher"], res, org_role="publisher")
            edges.append({"src": nid, "dst": eid, "rel": "published"})
        if e.get("publication_place"):
            res = resolve(e["publication_place"], plc_exact, plc_tok)
            nid = node("place", e["publication_place"], res)
            edges.append({"src": eid, "dst": nid, "rel": "printed_in"})

        # ---- field-level completeness questions.
        # Imprint fields are meaningless for the manuscript track (no publisher,
        # no print year), so only ask about them for printed editions.
        # The folder prefix is not a reliable track test: Lateiner_Meshumed and
        # HurbanYerushalaim_820938_duplicate are manuscripts without an `MS_`
        # prefix. `transkribus_ready` carries the track explicitly.
        is_ms = ("manuscript" in str(e.get("transkribus_ready") or "").lower()
                 or str(e.get("folder") or "").startswith(("MS_", "YIVO_")))
        imprint_fields = () if is_ms else (
            ("year_printed", "no print year"),
            ("publisher", "no publisher"),
            ("publication_place", "no publication place"),
        )
        for field, q in imprint_fields:
            # A title page that names only a printer has no publisher to find.
            # Khurbn Yerusholaim (BN 63.433) reads "Тип. Н. Старовольскаго,
            # Варшава Гуся 18. 1908" — a printing house and nothing else — so
            # the empty `publisher` is a finding, not an unanswered question.
            if field == "publisher" and not e.get("publisher") and e.get("printer"):
                continue
            if not e.get(field):
                gaps.append({"kind": "edition-field", "label": f"{folder} · {field}",
                             "reason": q, "candidates": ""})

        # ---- holding library, as a real org edge.
        # The `library` column is populated for the printed editions; for the
        # manuscripts the shelfmark sits in free-text `notes` ("YIVO rg8-1-f4179"),
        # so recover YIVO from there and carry the folio on the edge.
        folio = re.search(r"YIVO[\s,]*(?:RG\s*8|rg8)[\w\-.:]*",
                          str(e.get("notes") or ""), re.I)
        lib_name = (e.get("library") or "").strip()
        if not lib_name and folio:
            lib_name = "YIVO Institute for Jewish Research"
        if lib_name:
            res = resolve(lib_name, org_exact, org_tok, org_parts,
                          decisions=ORG_DECISION)
            nid = node("org", lib_name, res, org_role="library")
            edges.append({"src": nid, "dst": eid, "rel": "holds",
                          "shelfmark": (e.get("library_signature")
                                        or (folio.group(0).strip() if folio else None))})
        else:
            gaps.append({"kind": "edition-field", "label": f"{folder} · library",
                         "reason": "no holding library", "candidates": ""})
        if not (e.get("performance_events") or []):
            gaps.append({"kind": "edition-field",
                         "label": f"{folder} · performance_events",
                         "reason": "no performance events in the DB report",
                         "candidates": ""})

    # Two witnesses of one work carry the same work-level facts, so redirecting
    # them onto the shared work emits each edge twice: Khurbn Yerusholayim
    # (3891) has both the 1916 YIVO manuscript and the 1908 Biblioteka Narodowa
    # print, and the catalogue gives each the same 15 roles and 16 songs. That
    # is correct — the fact belongs to the play, not to either printing — so
    # identical edges are collapsed rather than double-counted.
    seen_edge, uniq_edges = set(), []
    for ed in edges:
        sig = json.dumps(ed, sort_keys=True, ensure_ascii=False)
        if sig in seen_edge:
            continue
        seen_edge.add(sig)
        uniq_edges.append(ed)
    if len(uniq_edges) != len(edges):
        print(f"  collapsed {len(edges) - len(uniq_edges)} duplicate edges "
              f"(work-level facts shared by several witnesses)")
    edges = uniq_edges

    # editions.csv lists Lateiner_Meshumed twice, so dedupe the ledger.
    seen_gap, uniq_gaps = set(), []
    for g in sorted(gaps, key=lambda g: (g["kind"], g["label"])):
        sig = (g["kind"], g["label"], g["reason"])
        if sig in seen_gap:
            continue
        seen_gap.add(sig)
        uniq_gaps.append(g)

    with OUT_GAPS.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, delimiter="\t",
                           fieldnames=["kind", "label", "reason", "candidates"])
        w.writeheader()
        w.writerows(uniq_gaps)

    # Drop a district premiere edge when the same edition already names a venue:
    # the venue's street address is the better locus (Sinai 2026-10-05). The
    # district node itself is dropped too if nothing references it any more.
    venued = {e["src"] for e in edges if e["rel"] == "performed_at"}
    redundant = {n["id"] for n in nodes.values()
                 if n.get("redundant_if_venue_known")}
    before = len(edges)
    edges = [e for e in edges
             if not (e["dst"] in redundant and e["src"] in venued)]
    still_used = {e["dst"] for e in edges} | {e["src"] for e in edges}
    for nid in redundant - still_used:
        nodes.pop(nid, None)
    print(f"  dropped {before - len(edges)} redundant district edges")

    graph = {
        "nodes": list(nodes.values()),
        "edges": edges,
        "gaps": uniq_gaps,
        "summary": {
            "editions": sum(1 for n in nodes.values() if n["kind"] == "edition"),
            "by_kind": dict(Counter(n["kind"] for n in nodes.values())),
            "by_status": dict(Counter(n.get("status") for n in nodes.values())),
            "edges": len(edges),
            "gaps": len(uniq_gaps),
            "gaps_by_reason": dict(Counter(g["reason"] for g in uniq_gaps)),
        },
    }
    OUT_GRAPH.write_text(json.dumps(graph, ensure_ascii=False, indent=2),
                         encoding="utf-8")

    print(f"wrote {OUT_GRAPH.relative_to(REPO)}")
    print(f"wrote {OUT_GAPS.relative_to(REPO)}")
    print()
    for k, v in graph["summary"].items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
