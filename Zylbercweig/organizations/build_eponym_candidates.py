#!/usr/bin/env python3
"""Propose eponym edges: an org named after a person should link that person.

Reads core_db.tsv (live orgs) + person_hub.tsv (the heading->person bridge),
extracts person-name spans from org names, resolves them against a surname
index, and writes a review queue with a confidence tier per row.

Writes NOTHING into person_org_edges.tsv. The queue is a proposal; the
Streamlit view "Eponym links" turns confirmed rows into edges with
edge_source=eponym (see apply_eponym_decisions.py).

Tiers:
  high   - surname + given name both matched, exactly one hub
  medium - surname matched to exactly one hub, no given name to corroborate
  low    - surname matched to several hubs, or a multi-name list

Idempotent: regenerating overwrites the queue but preserves no decisions --
decisions live in eponym_decisions.tsv, keyed by (db_id, hub_id).
"""
from __future__ import annotations

import csv
import pathlib
import re
import unicodedata as ud

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
CORE_DB = BASE / "organizations" / "core_db.tsv"
PERSON_HUB = BASE / "people" / "person_hub.tsv"
OUT_TSV = BASE / "organizations" / "eponym_candidates.tsv"

HEB = re.compile(r"[֐-׿]+")
FINAL_MAP = str.maketrans("ןםךףץ", "נמכפצ")

FIELDS = [
    "db_id", "org_name_yiddish", "org_name", "org_type", "pattern",
    "span", "hub_id", "person_heading", "person_db_ids", "n_hub_matches",
    "tier", "notes",
]


def sm(s: str) -> str:
    """NFKD first -- decomposes the precomposed ligatures (U+FB4E etc.) --
    then drop Hebrew points. Stripping points without NFKD matches nothing
    and looks exactly like a clean absence."""
    s = ud.normalize("NFKD", s or "")
    return "".join(c for c in s if not ("֑" <= c <= "ׇ"))


def key(s: str) -> str:
    """Comparison key: skeleton + final letters folded. A possessive after a
    final form writes גאָלדפאַדענס, not גאָלדפאַדעןס, so folding is required
    on both sides."""
    return sm(s).translate(FINAL_MAP).strip()


# Pattern words, point-stripped so they match the corpus spelling.
TRUPE = sm("טרופּע")
FUN = sm("פֿון")
UN = sm("און")
SAMEKH = sm("ס")

# A name-list that is really a venue/place, not people. The write-up's warning:
# a blanket comma rule catches "Olimpic Theatre (St. Louis, MO)".
NOT_PERSON = re.compile(
    r"\b(theatre|theater|opera|company|hall|garden|park|club|union|society|"
    r"school|library|museum|press|publish|st\.|ave|avenue|street|MO|NY|PA|OH)\b",
    re.I,
)


def load_live_orgs() -> list[dict]:
    rows = list(csv.DictReader(CORE_DB.open(encoding="utf-8"), delimiter="\t"))
    return [
        r for r in rows
        if (r.get("db_id") or "").strip().isdigit()
        and not (r.get("deprecated") or "").strip()
        and not (r.get("merged_into") or "").strip()
        and not (r.get("out_of_project") or "").strip()
    ]


def split_heading(ch: str) -> tuple[str, list[str]]:
    """'surname, given [alias]' -> (surname, [given tokens]).

    A comma is NOT reliable. 660 of 2,897 hubs (22%) write the heading as
    'surname given' with no comma -- and because the old code took everything
    before the comma as the surname, those 660 indexed under their FULL name
    and were invisible to every surname lookup. Joseph Kessler
    (`קעסלער יאָזעף`, person db 761) was therefore never offered as a
    candidate for `יאָזעף קעסלערס טרופע`, leaving only David Kessler
    (`קעסלער, דוד`) on the list -- which the judge correctly refused, making a
    real person look absent from the corpus.

    So: comma when there is one, else first whitespace token as the surname.
    Bracketed aliases contribute given-name tokens, never the surname.
    """
    ch = (ch or "").strip()
    if not ch:
        return "", []
    alias = " ".join(re.findall(r"\[([^\]]*)\]", ch))
    main = re.sub(r"\[[^\]]*\]", " ", ch)
    if "," in main:
        sur, _, rest = main.partition(",")
    else:
        toks = main.split()
        sur, rest = (toks[0], " ".join(toks[1:])) if toks else ("", "")
    givens = [t for t in HEB.findall(key(f"{rest} {alias}")) if len(t) > 1]
    return key(sur), givens


# A bracket INSIDE a token is an editorial note on the spelling, not an alias:
# `טאָמאַשעוו[פ]סקי` means the surname is attested both with and without that
# letter. The blanket `\[...\]` -> " " in split_heading turned it into TWO
# tokens (טאמאשעוו + סקי), so no surname lookup could ever match it -- and
# Boris Tomashevsky, who has 122 mentions and his own entry (HUB-D693), was
# reported `absent` for db1365. Same shape as the comma bug: a heading-format
# quirk hiding a well-attested person. Only 4 headings carry one, but one of
# them is his.
#
# A bracket at a token boundary (`גרינבערג פעני [טאָמאַשעווסקי]`) is a real
# alias and must keep its space, so the lookaround is deliberately strict.
_INTOKEN_BRACKET = re.compile(r"(?<=\S)\[([^\]]*)\](?=\S)")


def heading_variants(ch: str) -> list[str]:
    """Readings of a heading whose token carries a bracketed letter.

    Both readings are indexed because the bracket asserts both spellings are
    attested: keeping the letter and dropping it. Returns the heading unchanged
    when there is no in-token bracket, so every other heading is untouched.
    """
    ch = (ch or "").strip()
    if not _INTOKEN_BRACKET.search(ch):
        return [ch]
    return [_INTOKEN_BRACKET.sub(r"\1", ch), _INTOKEN_BRACKET.sub("", ch)]


def load_surname_index() -> tuple[dict, dict]:
    """surname key -> [hub]; and (surname, given) -> [hub]."""
    hubs = list(csv.DictReader(PERSON_HUB.open(encoding="utf-8"), delimiter="\t"))
    by_surname: dict[str, list] = {}
    by_full: dict[tuple, list] = {}
    for h in hubs:
        heading = h.get("canonical_heading") or ""
        # Usually one reading; two only for the 4 in-token-bracket headings.
        readings = heading_variants(heading)
        sur, givens = split_heading(readings[0])
        if not sur:
            continue
        by_surname.setdefault(sur, []).append(h)
        for alt in readings[1:]:
            alt_sur, alt_givens = split_heading(alt)
            if alt_sur and alt_sur != sur:
                if h not in by_surname.setdefault(alt_sur, []):
                    by_surname[alt_sur].append(h)
                for g in alt_givens:
                    if h not in by_full.setdefault((alt_sur, g), []):
                        by_full[(alt_sur, g)].append(h)
        # A single letter is an initial (עפּשטיין, מ. ב.) and must not
        # corroborate: ה. ב. עפשטיין and מ. ב. עפּשטיין are different people
        # and matched as "high" until this guard. split_heading already drops
        # length-1 tokens.
        for g in givens:
            by_full.setdefault((sur, g), []).append(h)
        # Name surfaces carry attested spellings the heading may not use
        # ('יאָזעף קעסלער', 'דזשאָזעף קעסלער', 'קעסלערן'). Index the LAST
        # token of each surface as a surname variant -- Yiddish name surfaces
        # are given-then-surname, so the last token is the surname. Indexing
        # every token would make given names like יאָזעף into surname keys and
        # flood the low tier with false candidates.
        for surface in (h.get("top_surfaces") or "").split("|"):
            toks = HEB.findall(key(surface))
            if not toks:
                continue
            t = toks[-1]
            if len(t) > 2 and t != sur:
                by_surname.setdefault(t, [])
                if h not in by_surname[t]:
                    by_surname[t].append(h)
    return by_surname, by_full


def org_text(r: dict) -> str:
    """Yiddish name, falling back to `name` -- db641 לערמאַןס טרופּע has an
    EMPTY name_yiddish and carries its name in `name`."""
    y = (r.get("name_yiddish") or "").strip()
    return y if HEB.search(y) else (r.get("name") or "").strip()


def classify(text: str, name_en: str) -> tuple[str, list[str]]:
    """-> (pattern, [candidate spans])"""
    y = sm(text)
    m = re.search(TRUPE + r"\s+" + FUN + r"\s+(.+)$", y)
    if m:
        return "trupe_fun", HEB.findall(m.group(1))
    m = re.search(r"(\S+?)" + SAMEKH + r"\s+" + TRUPE, y)
    if m:
        return "possessive", HEB.findall(m.group(1))
    if "," in y or re.search(r"\s" + UN + r"\s", y) or "," in name_en or " and " in name_en.lower():
        if NOT_PERSON.search(name_en) or NOT_PERSON.search(text):
            return "namelist_venue", []
        return "namelist", HEB.findall(y)
    return "", []


# Words that appear inside these patterns but are not surnames.
#
# Hand-listing these does not scale -- Sinai spotted `מיט` ("with") and
# `אייגענע` ("own") surviving into the queue, and a survey then found ~100
# more: function words, institution vocabulary, place names and adjectives.
# So the list below covers what a frequency rule cannot, and
# generic_vocabulary() derives the rest from the corpus itself.
STOP = {key(w) for w in (
    # structure words of the patterns themselves
    "טרופּע", "פֿון", "און", "די", "דער", "דאָס", "אין", "א", "אַ",
    "קאָמפּאַניע", "טעאַטער", "אַנסאַמבל", "פאַמיליע", "ברידער", "שוועסטער",
    # prepositions, pronouns, auxiliaries -- never a name
    "מיט", "פאַר", "אונטער", "אויף", "אויפן", "פונם", "פֿונעם", "ביי", "צו",
    "וועלכע", "וועלכער", "האָט", "האבן", "זיין", "זיינע", "איר", "זיך",
    "אייגענע", "אייגענער", "אייגן", "נאָמען", "אויפן נאָמען", "נאָך",
    "ערשטער", "ערשטע", "צווייטער", "נייע", "נייער", "אַלטע", "יונגע",
    "גרויסע", "קליינע", "זעלבסט", "הילף", "מיטל", "בלינדער",
    # art/company vocabulary
    "קונסט", "דראַמע", "דראַמאַטישע", "דראַמאַטישער", "דראַמאַטישן",
    "אָפּערע", "אָפּערעטע", "אָפּערעטן", "מוזיק", "סצענע", "בינע",
    "קאָמעדיע", "קוואַרטעט", "כאָר", "קאַפּעליע", "ארקעסטער", "גרופּע",
    "קאָלעקטיוו", "סטודיע", "אַקטיאָר", "אַרטיסטן", "קינסטלער", "שוישפּילער",
    "דיכטער", "שרייבער", "מאָלער", "מוזיקער", "כאָריסטן", "קאָמפּאָזיטאָרן",
    "דירעקציע", "דירעקטאָר", "מענעדזשמענט", "קאַסירער", "גאַסט",
    "וואַנדערטרופּע", "טורניי", "פּראָווינץ", "פּריוואַטער", "ליבהאָבער",
    # institutions
    "ייִדיש", "יידיש", "קלוב", "פֿאַראיין", "פאראיינ", "געזעלשאַפט", "שול",
    "שולע", "ישיבה", "תלמוד", "תורה", "אוניווערזיטעט", "גימנאַזיע",
    "אינסטיטוט", "אַקאַדעמי", "קאָלעדזש", "סקול", "הויכשול", "דראַמשול",
    "האַנדלסשול", "פּראָגימנאַזיע", "לערנאַנשטאַלט", "סעמינאַר",
    "פֿאַרבאַנד", "פאַרבאַנד", "פאַראייניקונג", "יוניאָן", "יוניע", "רינג",
    "אַגודה", "אָפּטייל", "אָפּטיילונג", "סעקשאָן", "בראַנטש", "לאָקאַל",
    "קרייז", "קאָמיטעט", "אַרכיוו", "ביבליאָטעק", "מוזיי", "פאַרלאַג",
    "דרוקעריי", "פּאַבלישערס", "בוך", "ליטעראַטור", "ליטעראַטן",
    "זשורנאַליסטן", "וויסנשאַפט", "געשיכטע", "יוריספּרודענץ", "עקאָנאָמיע",
    "פּאָליטישער", "פּעדאַגאָגישן", "סטודענטן", "יונגוועלט", "בירגער",
    "אַרבעטער", "ארבעטער", "קינא", "פילם", "קעמפּ", "הויף", "הויז", "זאַל",
    "קאָאָפּעראַטיוו", "קאָאָפּעראַטיווע", "קאָאָפּעראַטיווער", "פאַקט",
    "אַלמנה", "יתומים", "יתומות", "זון", "זין", "געברידער", "מחבר", "מוכר",
    "פאַרקויף", "נעמער", "פּובליסיטי", "קרעפטן", "לייט", "פריינט", "מוסטער",
    # nationality / region adjectives
    "רוסישער", "פּוילישער", "רומענישער", "ענגלישן", "ווייסרוסלענדישע",
    "אַרגענטינער", "לאַדזשער", "בוקאַרעשטער", "טיפליסער", "זאַקסישן",
    "מלוכהשן", "סאָוויעט", "וועלטלעכער", "ראַדיקאַלער", "אַקאַדעמישן",
    "טעכנישער", "אַרטיסטישער", "וואָקאַלער", "באַוועגלעכן", "יודנטום",
)}

# A span occurring in a great many organisation names is vocabulary, not a
# surname: `יו`/`ניו`/`יארק` (New York), `פאר`, `יידישן`. Derived from the
# corpus so it stays true as core_db grows, rather than hand-maintained.
# The threshold is deliberately high: `בן` (17) and `שלום` (17) are real name
# elements and must survive, while `יו` (314) and `פאר` (283) must not.
GENERIC_ORG_NAME_THRESHOLD = 20


def generic_vocabulary(orgs: list[dict]) -> set[str]:
    """Span skeletons that occur in >= THRESHOLD distinct live org names."""
    counts: dict[str, int] = {}
    for r in orgs:
        blob = key(f"{r.get('name_yiddish') or ''} {r.get('name') or ''}")
        for tok in set(HEB.findall(blob)):
            if len(tok) > 1:
                counts[tok] = counts.get(tok, 0) + 1
    return {t for t, n in counts.items() if n >= GENERIC_ORG_NAME_THRESHOLD}


def settlement_names() -> set[str]:
    """Place-name skeletons, from the settlements the RAs already confirmed.

    `ניו יארק`, `שיקאגא`, `פאריז` and `קאנאדע` were surviving into the queue as
    candidate eponyms: each is below the generic-frequency threshold, so only a
    gazetteer removes them. Using the review file's own
    `confirmed_settlement_yiddish` keeps this true to decisions already made
    rather than to a city list invented here. Adjectival forms (אָדעסער,
    ווילנער) are added, since an org is named for a place that way.
    """
    path = BASE / "organizations" / "org_addresses_review.tsv"
    out: set[str] = set()
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            for field in ("confirmed_settlement_yiddish", "extracted_settlements"):
                for part in re.split(r"[|,;]", r.get(field) or ""):
                    for tok in HEB.findall(key(part)):
                        if len(tok) > 2:
                            out.add(tok)
                            out.add(tok + key("ער"))   # אָדעס -> אָדעסער
    return out


def _neighbours(y: str, span: str) -> list[str]:
    """Hebrew tokens immediately before/after `span` in the org name -- the only
    place a given name may sit to corroborate this surname."""
    toks = HEB.findall(y)
    keys = [key(t) for t in toks]
    k = key(span)
    out: list[str] = []
    for i, kk in enumerate(keys):
        if kk == k or (kk.startswith(k) and len(kk) - len(k) <= 1):
            if i > 0:
                out.append(keys[i - 1])
            if i + 1 < len(keys):
                out.append(keys[i + 1])
    return [o for o in out if o not in STOP]


def main() -> None:
    orgs = load_live_orgs()
    by_surname, by_full = load_surname_index()
    generic = generic_vocabulary(orgs) | settlement_names()
    out: list[dict] = []
    stats: dict[str, int] = {}

    for r in orgs:
        text = org_text(r)
        name_en = (r.get("name") or "").strip()
        pattern, spans = classify(text, name_en)
        if not pattern:
            continue
        stats[pattern] = stats.get(pattern, 0) + 1
        if not spans:
            continue

        # A 2-letter span is usually noise, but not always: `אַש` is Sholem
        # Asch. Keep a short span only when it is a known surname.
        spans = [s for s in spans
                 if key(s) not in STOP and key(s) not in generic
                 and (len(key(s)) > 2 or key(s) in by_surname)]
        multi = len(spans) > 1

        for sp in spans:
            k = key(sp)
            # A possessive ס is glued to the surname; try the bare stem too.
            variants = [k]
            if k.endswith(key("ס")) and len(k) > 2:
                variants.append(k[:-1])
            hit, used = None, None
            for v in variants:
                if v in by_surname:
                    hit, used = by_surname[v], v
                    break
            if not hit:
                out.append({
                    "db_id": r["db_id"], "org_name_yiddish": r.get("name_yiddish") or "",
                    "org_name": name_en, "org_type": r.get("org_type") or "",
                    "pattern": pattern, "span": sp, "hub_id": "", "person_heading": "",
                    "person_db_ids": "", "n_hub_matches": "0", "tier": "unresolved",
                    "notes": "no surname match",
                })
                continue

            # Corroborate with a given name ADJACENT to this span. Anywhere-in-
            # the-name is too loose: טרופּע פֿון ספּיוואַקאָווסקי-סעם אַדלער
            # carries two eponyms, and a floating given name would attach to
            # whichever surname matched first.
            adj = _neighbours(sm(text), sp)
            full = [h for h in hit
                    if any((used, o) in by_full and h in by_full[(used, o)] for o in adj)]

            if len(full) == 1:
                chosen, tier, note = full, "high", "surname+given, unique"
            elif len(hit) == 1:
                tier = "low" if multi and pattern == "namelist" else "medium"
                chosen, note = hit, "unique surname match"
            else:
                chosen, tier, note = hit, "low", f"ambiguous: {len(hit)} hubs share this surname"

            for h in chosen[:12]:
                out.append({
                    "db_id": r["db_id"], "org_name_yiddish": r.get("name_yiddish") or "",
                    "org_name": name_en, "org_type": r.get("org_type") or "",
                    "pattern": pattern, "span": sp, "hub_id": h["hub_id"],
                    "person_heading": h.get("canonical_heading") or "",
                    "person_db_ids": h.get("db_ids") or "",
                    "n_hub_matches": str(len(hit)), "tier": tier, "notes": note,
                })

    out.sort(key=lambda d: (
        {"high": 0, "medium": 1, "low": 2, "unresolved": 3}[d["tier"]], int(d["db_id"])))
    with OUT_TSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(out)

    print(f"live orgs: {len(orgs)}")
    print("patterns matched:")
    for k, v in sorted(stats.items(), key=lambda kv: -kv[1]):
        print(f"  {k:16s} {v:5d}")
    tiers: dict[str, int] = {}
    seen: set = set()
    for d in out:
        tiers[d["tier"]] = tiers.get(d["tier"], 0) + 1
        seen.add(d["db_id"])
    print(f"\ncandidate rows: {len(out)}  covering {len(seen)} orgs")
    for t in ("high", "medium", "low", "unresolved"):
        print(f"  {t:12s} {tiers.get(t, 0):5d}")
    print(f"\nwrote {OUT_TSV.relative_to(BASE)}")


if __name__ == "__main__":
    main()
