"""Produce a TSV of unresolved settlement strings with suggested Wikidata
QIDs, English names, and categories so a human can vet and append rows
to kimatch_matched_full.tsv.

Output columns:
  yiddish               — the raw unresolved string seen in clusters
  occurrences           — how many times it appears in org_alignment_review
  suggested_english     — best-guess English name (Latin)
  suggested_qid         — Wikidata QID; either existing kimatch QID (for a
                          variant of a city already there) or a fresh QID
                          to add
  suggested_category    — settlement | neighborhood | country | region |
                          ghetto | unknown
  basis                 — kimatch_variant (same QID as existing kimatch
                          entry) | wikidata_new (city not yet in kimatch)
                          | exclude (non-settlement) | unknown
  notes                 — anything to flag

Rows are sorted by occurrences descending. The curated mapping below is
seeded from common Eastern European / North American place names; extend
it as Zalmen surfaces more locations.
"""
from __future__ import annotations

import csv
import sys
import unicodedata
from collections import Counter
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_resolver import get_resolver  # noqa: E402

csv.field_size_limit(10**9)
_ALIGN = _HERE / "org_alignment_review.tsv"
_OUT = _HERE / "unresolved_settlements_punchlist.tsv"


# Curated map. Keys are the raw Yiddish string. Values:
#   (suggested_english, suggested_qid, category, basis, notes)
#
# basis = "kimatch_variant" → QID matches an entry already in kimatch
#                              (just need to add this Yiddish form as a
#                              variant alongside).
#       = "wikidata_new"    → city missing from kimatch entirely; add a
#                              fresh row.
#       = "exclude"         → not a settlement; resolver should keep
#                              ignoring it.
CURATED: dict[str, tuple[str, str, str, str, str]] = {
    # ── Warsaw variants (existing kimatch Q270) ────────────────────────
    "וואַרשעווער":          ("Warsaw",  "Q270",   "settlement",  "kimatch_variant", "adjectival form"),
    "וואַרשא":              ("Warsaw",  "Q270",   "settlement",  "kimatch_variant", "alef-final variant"),
    "וואַרשאַ":             ("Warsaw",  "Q270",   "settlement",  "kimatch_variant", "alef-final variant"),
    "וואַרשאָ":             ("Warsaw",  "Q270",   "settlement",  "kimatch_variant", "alef-final variant"),
    # ── Łódź variants (existing kimatch Q580) ─────────────────────────
    "לאָדז":                ("Łódź",    "Q580",   "settlement",  "kimatch_variant", "missing final dash-shin"),
    # ── Bronx (not in kimatch) ────────────────────────────────────────
    "בראַנקס":              ("Bronx",   "Q18426", "neighborhood","wikidata_new",    "NYC borough"),
    "בראנקס":               ("Bronx",   "Q18426", "neighborhood","wikidata_new",    "NYC borough"),
    "בראָנקס":              ("Bronx",   "Q18426", "neighborhood","wikidata_new",    "NYC borough"),
    # ── Harlem (not in kimatch) ───────────────────────────────────────
    "האַרלעם":              ("Harlem",  "Q41183", "neighborhood","wikidata_new",    "Manhattan neighborhood"),
    # ── Cities missing from kimatch ───────────────────────────────────
    "באַקו":                ("Baku",        "Q9248",   "settlement", "wikidata_new", "Azerbaijan"),
    "באָקו":                ("Baku",        "Q9248",   "settlement", "wikidata_new", "Azerbaijan"),
    "וויניפּעג":            ("Winnipeg",    "Q2096",   "settlement", "wikidata_new", "Canada"),
    "מעקסיקאָ":             ("Mexico City", "Q1489",   "settlement", "wikidata_new", "ambiguous: also country Q96"),
    "מינכען":               ("Munich",      "Q1726",   "settlement", "wikidata_new", "Germany"),
    "מינכן":                ("Munich",      "Q1726",   "settlement", "wikidata_new", "Germany"),
    "האָמעל":               ("Homel",       "Q66263",  "settlement", "wikidata_new", "Belarus (Gomel)"),
    "ניקאָלאַיעוו":         ("Mykolaiv",    "Q131136", "settlement", "wikidata_new", "Ukraine"),
    "פּאָלטאַווע":          ("Poltava",     "Q125286", "settlement", "wikidata_new", "Ukraine"),
    "פּאָלטאָווע":          ("Poltava",     "Q125286", "settlement", "wikidata_new", "Ukraine"),
    "מעץ":                  ("Metz",        "Q22690",  "settlement", "wikidata_new", "France"),
    "מילוואַקיי":           ("Milwaukee",   "Q37836",  "settlement", "wikidata_new", "USA"),
    "ליבאַווע":             ("Liepāja",     "Q41159",  "settlement", "wikidata_new", "Latvia (Libau)"),
    "מעליטאָפּאָל":         ("Melitopol",   "Q156915", "settlement", "wikidata_new", "Ukraine"),
    "אַנטווערפן":           ("Antwerp",     "Q12892",  "settlement", "wikidata_new", "Belgium"),
    "מאָהילעוו":            ("Mogilev",     "Q193581", "settlement", "wikidata_new", "Belarus"),
    "כאָסטאָן":             ("Boston",      "Q100",    "settlement", "wikidata_new", "USA"),
    "באָסטאָן":             ("Boston",      "Q100",    "settlement", "wikidata_new", "USA"),
    "בראַנזווייל":          ("Brownsville", "Q991279", "neighborhood","wikidata_new", "Brooklyn"),
    # ── More cities not in kimatch ────────────────────────────────────
    "וואַשינגטאָן":         ("Washington, D.C.","Q61",   "settlement", "wikidata_new", "USA"),
    "ראָסטאָוו":            ("Rostov-on-Don",  "Q34125", "settlement", "wikidata_new", "Russia"),
    "קליוולענד":            ("Cleveland",      "Q5083",  "settlement", "wikidata_new", "USA"),
    "קאַאייראָ":            ("Cairo",          "Q85",    "settlement", "wikidata_new", "Egypt"),
    "קאַאיראָ":             ("Cairo",          "Q85",    "settlement", "wikidata_new", "Egypt"),
    "סאַנדאמיר":            ("Sandomierz",     "Q486948","settlement", "wikidata_new", "Poland"),
    "מלאַווע":              ("Mława",          "Q572394","settlement", "wikidata_new", "Poland"),
    "מאַריענבאַד":          ("Mariánské Lázně","Q183689","settlement", "wikidata_new", "Czech Republic (Marienbad)"),
    "לידע":                 ("Lida",           "Q188317","settlement", "wikidata_new", "Belarus"),
    "לויוויטש":             ("Łowicz",         "Q372680","settlement", "wikidata_new", "Poland"),
    "ל. א.":                ("Los Angeles",    "Q65",    "settlement", "wikidata_new", "abbreviation"),
    "טשעלסי":               ("Chelsea",        "Q3245",  "neighborhood","wikidata_new","Manhattan (or London Q207937)"),
    "טיפליס":               ("Tbilisi",        "Q994",   "settlement", "wikidata_new", "Georgia (Tiflis)"),
    "טאַרנאָוו":            ("Tarnów",         "Q102317","settlement", "wikidata_new", "Poland"),
    "וולאַצלאַוועק":        ("Włocławek",      "Q146521","settlement", "wikidata_new", "Poland"),
    "וולאָצלאַוועק":        ("Włocławek",      "Q146521","settlement", "wikidata_new", "Poland"),
    "האַמעל":               ("Homel",          "Q66263", "settlement", "wikidata_new", "Belarus variant"),
    "האָמל":                ("Homel",          "Q66263", "settlement", "wikidata_new", "Belarus variant"),
    "האַמבורג":             ("Hamburg",        "Q1055",  "settlement", "wikidata_new", "Germany"),
    "האַוואַנאַ":           ("Havana",         "Q1563",  "settlement", "wikidata_new", "Cuba"),
    "דניעפּראָפּיעטראָווסק": ("Dnipro",         "Q43295", "settlement", "wikidata_new", "Ukraine (Dnipropetrovsk)"),
    "בערן":                 ("Bern",           "Q70",    "settlement", "wikidata_new", "Switzerland"),
    "בענדער":               ("Bender",         "Q200639","settlement", "wikidata_new", "Moldova (Tighina)"),
    "ביעלאָצערקאָוו":       ("Bila Tserkva",   "Q33529", "settlement", "wikidata_new", "Ukraine"),
    "ב. א.":                ("Buenos Aires",   "Q1486",  "settlement", "kimatch_variant", "abbreviation"),
    'ב"א':                  ("Buenos Aires",   "Q1486",  "settlement", "kimatch_variant", "abbreviation"),
    "שעדלעץ":               ("Siedlce",        "Q14660", "settlement", "wikidata_new", "Poland"),
    "שאַוול":               ("Šiauliai",       "Q160413","settlement", "wikidata_new", "Lithuania"),
    "קוראָוו":              ("Kurów",          "Q926807","settlement", "wikidata_new", "Poland"),
    "קוטנע":                ("Kutno",          "Q1141454","settlement","wikidata_new", "Poland"),
    "קאָלאָמעאַ":           ("Kolomyia",       "Q210077","settlement", "wikidata_new", "Ukraine"),
    "פּעטערסבורג":          ("Saint Petersburg","Q656",  "settlement", "wikidata_new", "Russia"),
    "פּיאַטריקאָוו":        ("Piotrków Trybunalski","Q204130","settlement","wikidata_new","Poland"),
    "פּאָניאַטאָוו":        ("Poniatowa",      "Q1132344","settlement","wikidata_new", "Poland"),
    "סובאָלק":              ("Suwałki",        "Q199689","settlement", "wikidata_new", "Poland"),
    "סאָכאַטשאָוו":         ("Sochaczew",      "Q461617","settlement", "wikidata_new", "Poland"),
    "מילאַן":               ("Milan",          "Q490",   "settlement", "wikidata_new", "Italy"),
    "מונקאָטש":             ("Mukachevo",      "Q183959","settlement", "wikidata_new", "Ukraine (Munkács)"),
    "מאַקאַראָוו":          ("Makariv",        "Q1953005","settlement","wikidata_new", "Ukraine"),
    "ליסאַבאָן":            ("Lisbon",         "Q597",   "settlement", "wikidata_new", "Portugal"),
    "לייפּציק":             ("Leipzig",        "Q2079",  "settlement", "wikidata_new", "Germany"),
    "לאָמזשע":              ("Łomża",          "Q189603","settlement", "wikidata_new", "Poland"),
    "טאָמסק":               ("Tomsk",          "Q5687",  "settlement", "wikidata_new", "Russia"),
    "זשענעווע":             ("Geneva",         "Q71",    "settlement", "wikidata_new", "Switzerland"),
    "זשאָלקיעוו":           ("Zhovkva",        "Q205620","settlement", "wikidata_new", "Ukraine (Żółkiew)"),
    "וויליאַמסבורג":        ("Williamsburg",   "Q745140","neighborhood","wikidata_new", "Brooklyn"),
    "וואָלאָזשין":          ("Valozhyn",       "Q1144213","settlement","wikidata_new", "Belarus"),
    "האָליוואוד":           ("Hollywood",      "Q34006", "neighborhood","wikidata_new", "Los Angeles"),
    "דרעזדען":              ("Dresden",        "Q1731",  "settlement", "wikidata_new", "Germany"),
    "דימונה":               ("Dimona",         "Q193791","settlement", "wikidata_new", "Israel"),
    "גאָלדינגען":           ("Kuldīga",        "Q187606","settlement", "wikidata_new", "Latvia (Goldingen)"),
    "בערדיאָנסק":           ("Berdyansk",      "Q187814","settlement", "wikidata_new", "Ukraine"),
    "ביראָבידזשאַן":        ("Birobidzhan",    "Q5717",  "settlement", "wikidata_new", "Russia"),
    "בופֿאָלאָ":            ("Buffalo",        "Q40435", "settlement", "wikidata_new", "USA"),
    "באַראַנאָוויטש":       ("Baranavichy",    "Q156970","settlement", "wikidata_new", "Belarus (Baranowicze)"),
    "איזמאַאיל":            ("Izmail",         "Q200687","settlement", "wikidata_new", "Ukraine"),
    "אַמסטערדאָם":          ("Amsterdam",      "Q727",   "settlement", "wikidata_new", "Netherlands"),
    "אַטלאַנטאָ":           ("Atlanta",        "Q23556", "settlement", "wikidata_new", "USA"),
    "כראַנזוויל":           ("Brownsville",    "Q991279","neighborhood","wikidata_new", "Brooklyn variant"),
    "בראָנזווייל":          ("Brownsville",    "Q991279","neighborhood","wikidata_new", "Brooklyn variant"),
    # ── More kimatch variants (city already in kimatch, different spelling) ──
    "אָדעסע":               ("Odesa",          "Q1874",  "settlement", "kimatch_variant", "Odesa variant"),
    "אַנטווערפּען":         ("Antwerp",        "Q12892", "settlement", "wikidata_new", "Belgium"),
    "אַנטווערפּן":          ("Antwerp",        "Q12892", "settlement", "wikidata_new", "Belgium"),
    "לעמבער":               ("Lviv",           "Q36036", "settlement", "kimatch_variant", "Lemberg truncated"),
    "לעמבערנ":              ("Lviv",           "Q36036", "settlement", "kimatch_variant", "Lemberg typo (nun for gimel)"),
    "לידז":                 ("Łódź",           "Q580",   "settlement", "kimatch_variant", "Lodz truncated typo"),
    "לאָנראָן":             ("London",         "Q84",    "settlement", "kimatch_variant", "London typo (r for d)"),
    "קראָקעוו":             ("Kraków",         "Q31487", "settlement", "kimatch_variant", "Kraków variant"),
    "קייעוו":               ("Kyiv",           "Q1899",  "settlement", "kimatch_variant", "Kiev variant"),
    "פֿילאדעלפֿייע":        ("Philadelphia",   "Q1345",  "settlement", "kimatch_variant", "Philadelphia variant"),
    "ניקאָ'אַיעוו":         ("Mykolaiv",       "Q131136","settlement", "wikidata_new", "Mykolaiv typo"),
    "פּאָלטאָוואָ":         ("Poltava",        "Q125286","settlement", "wikidata_new", "Poltava variant"),
    "פּאָלטאָוואַ":         ("Poltava",        "Q125286","settlement", "wikidata_new", "Poltava variant"),
    "פּאָלטאווע":           ("Poltava",        "Q125286","settlement", "wikidata_new", "Poltava variant"),
    "האָרלעם":              ("Harlem",         "Q41183", "neighborhood","wikidata_new", "Manhattan variant"),
    "בראָנסקער":            ("Bronx",          "Q18426", "neighborhood","wikidata_new", "Bronx adjectival"),
    "בראַנקסער":            ("Bronx",          "Q18426", "neighborhood","wikidata_new", "Bronx adjectival"),
    "יאָס'":                ("Iași",           "Q46852", "settlement", "kimatch_variant", "Iași with apostrophe"),
    # ── Ghettos, countries, named regions — include with their proper
    #    Wikidata QID. Generic descriptors (province) stay excluded.
    "וואַרשעווער געטאָ":    ("Warsaw Ghetto",  "Q160122","ghetto",   "wikidata_new", "sub-city historical site"),
    "לאָדזשער געטאָ":       ("Łódź Ghetto",    "Q204540","ghetto",   "wikidata_new", "sub-city historical site"),
    "ווילנער געטאָ":        ("Vilna Ghetto",   "Q156859","ghetto",   "wikidata_new", "sub-city historical site"),
    "ריגער געטאָ":          ("Riga Ghetto",    "Q1370115","ghetto",  "wikidata_new", "sub-city historical site"),
    "אַמעריקע":             ("America",        "Q30",    "country",  "wikidata_new", "USA"),
    "אָמעריקע":             ("America",        "Q30",    "country",  "wikidata_new", "USA"),
    "אמעריקע":              ("America",        "Q30",    "country",  "wikidata_new", "USA"),
    "אָרץ-ישראל":           ("Land of Israel", "Q23793", "region",   "wikidata_new", "Eretz Israel"),
    "ארץ-ישראל":            ("Land of Israel", "Q23793", "region",   "wikidata_new", "Eretz Israel"),
    "דרום-אפריקע":          ("South Africa",   "Q258",   "country",  "wikidata_new", ""),
    "בעסאַראַביע":          ("Bessarabia",     "Q189834","region",   "wikidata_new", "historical"),
    "פּוילן":               ("Poland",         "Q36",    "country",  "wikidata_new", ""),
    "רוסלאַנד":             ("Russia",         "Q159",   "country",  "wikidata_new", ""),
    "אָרגענטינע":           ("Argentina",      "Q414",   "country",  "wikidata_new", ""),
    "אַרגענטינע":           ("Argentina",      "Q414",   "country",  "wikidata_new", ""),
    "קאָליפֿאָרניע":        ("California",     "Q99",    "region",   "wikidata_new", "US state"),
    # ── Truly generic descriptors — keep excluded ─────────────────────
    "רוסלאַנד און בעסאַראַביע": ("Russia and Bessarabia","","region","exclude", "compound region — not a single place"),
    "פּוילישער פּראָווינץ": ("Polish province","",        "region",   "exclude", "generic descriptor"),
    "פּראָווינץ":           ("province",       "",       "region",   "exclude", "generic descriptor"),
}


def _norm(s: str) -> str:
    """Normalize for robust dict matching: NFKD-decompose, drop combining
    marks (niqqud), collapse to a canonical bare Hebrew form. This makes
    'אַמעריקע' (alef+patah), 'אָמעריקע' (alef+qamatz), 'אמעריקע' (bare),
    and 'אַמעריקע' (U+FB2E presentation form) all match the same entry.
    """
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("־", "-").strip()
    return s


def _suffix_strip(s: str) -> str | None:
    """Strip a common gentilic/adjectival suffix and return the bare form."""
    for suf in ("עווער", "ערן", "ערס", "ער"):
        if s.endswith(suf):
            return s[: -len(suf)] + "ע"
    return None


def main() -> None:
    R = get_resolver()
    counts: Counter[str] = Counter()
    with _ALIGN.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            for s in (p.strip() for p in (row.get("extracted_settlements") or "").split("|")):
                if s and not R.resolve(s):
                    counts[s] += 1

    # Build a normalized lookup map from CURATED so visually-identical
    # but differently-encoded strings match.
    curated_norm = {_norm(k): v for k, v in CURATED.items()}

    out_rows = []
    for s, n in counts.most_common():
        ns = _norm(s)
        if ns in curated_norm:
            en, qid, cat, basis, notes = curated_norm[ns]
        else:
            # Try suffix-stripped form against curated + resolver
            stripped = _suffix_strip(s)
            if stripped and _norm(stripped) in curated_norm:
                en, qid, cat, basis, notes = curated_norm[_norm(stripped)]
                notes = (notes + "; " if notes else "") + f"adjectival of {stripped!r}"
            elif stripped:
                h = R.resolve(stripped)
                if h:
                    en, qid, cat, basis, notes = (h.english or h.yiddish, h.qid,
                                                  "settlement", "kimatch_variant",
                                                  f"adjectival of {stripped!r}")
                else:
                    en, qid, cat, basis, notes = "", "", "unknown", "unknown", ""
            else:
                en, qid, cat, basis, notes = "", "", "unknown", "unknown", ""
        out_rows.append({
            "yiddish": s,
            "occurrences": n,
            "suggested_english": en,
            "suggested_qid": qid,
            "suggested_category": cat,
            "basis": basis,
            "notes": notes,
        })

    with _OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["yiddish", "occurrences", "suggested_english",
                        "suggested_qid", "suggested_category", "basis", "notes"],
            delimiter="\t",
        )
        w.writeheader()
        w.writerows(out_rows)

    by_basis = Counter(r["basis"] for r in out_rows)
    cov_occ = sum(r["occurrences"] for r in out_rows if r["basis"] != "unknown")
    total_occ = sum(r["occurrences"] for r in out_rows)
    print(f"wrote {_OUT}")
    print(f"unique unresolved strings : {len(out_rows)}")
    print(f"total occurrences         : {total_occ}")
    print(f"covered occurrences       : {cov_occ} ({cov_occ/total_occ:.0%})")
    print(f"by basis                  : {dict(by_basis)}")


if __name__ == "__main__":
    main()
