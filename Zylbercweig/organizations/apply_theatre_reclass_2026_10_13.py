"""Reclassify mis-typed Theatre rows in core_db.tsv (Ruthie, 2026-10-13).

Ruthie noticed the Theatre list contains many non-theatres. Sinai gave six
org_type rules; each row below was then read individually, so the decisions are
per-row judgements, not regex output. Rules, as given:

  קלוב                                    -> Amateur
  music hall / kleinkunst / vaudeville /
    variety (+ miniatur, cabaret)          -> Kleinkunst
  Gesellschaft / kreiz(n)                  -> Theatre-related Society/ Union
  symphonies / choirs (+ philharmonic,
    opera, gezangs-farein)                 -> Musical organization
  שול                                      -> Religious institutions/organizations
  Studios                                  -> Theatre education

A seventh rule — plural טהעאטערס -> GENERIC — does NOT apply here: GENERIC is a
decision value in org_alignment_review.tsv, not an org_type, and core_db has no
plural-טעאַטערס rows at all. It is handled on the cluster list instead, by
apply_theatre_plural_generic_2026_10_13.py.

Idempotent: only rewrites rows still typed Theatre.

Usage:
    python3 Zylbercweig/organizations/apply_theatre_reclass_2026_10_13.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
CORE_DB_TSV = HERE / "core_db.tsv"
REVIEWER = "claude (rules by Sinai, flagged by Ruthie)"

AMATEUR = "Amateur"
KLEINKUNST = "Kleinkunst"
SOCIETY = "Theatre-related Society/ Union"
MUSIC = "Musical organization"
RELIGIOUS = "Religious institutions/organizations"
THEATRE_EDU = "Theatre education"
EDUCATION = "Education"

# db_id -> (new_type, rule, note)
DECISIONS: dict[str, tuple[str, str, str]] = {
    # ---- קלוב -> Amateur -------------------------------------------------
    "404":  (AMATEUR, "klub", "Hebrew Dramatic Club (London) — drama club"),
    "707":  (AMATEUR, "klub", "Berdichev workers' club named for Lozovski"),
    "1123": (AMATEUR, "klub", "Central Jewish workers' club named for Lenin"),
    "1760": (AMATEUR, "klub", "Princess Club"),
    "1763": (AMATEUR, "klub", "'Arbeter fraynt-klub'"),
    "1783": (AMATEUR, "klub", "Weinkort Club"),
    "1979": (AMATEUR, "klub", "'Arbeter fraynt-klub' (dup of 1763)"),
    # 1119 is a drama-KREIZ hosted at a club -> circle wins, see society block.
    # 2910 'Frensis klub teater' = a named theatre house, klub is part of the
    # proper name, not a club identity -> left as Theatre.

    # amateur drama circles that carry no קלוב/קרייז token but are plainly
    # amateur-society bodies in the lexicon's wording:
    "221":  (AMATEUR, "amateur", "Di Fraye Yidishe Folks-Bine — amateur folk stage"),
    "840":  (AMATEUR, "amateur", "Fraye yidishe folks-bine, Austria"),
    "2849": (AMATEUR, "amateur", "Philadelphia Yidishe Folks-Bine"),
    "1788": (AMATEUR, "amateur", "Unzer Vinkl — Erets-Yisroel drama circle"),
    "1785": (AMATEUR, "amateur", "Unzer Vinkl (Kharkov) — drama circle"),
    "1787": (AMATEUR, "amateur", "Unzer Vinkl (Minsk) — drama circle"),
    "427":  (AMATEUR, "amateur", "Unzer vinkl (Our Corner) — drama circle"),
    "307":  (AMATEUR, "amateur", "Kunst vinkl — amateur art corner"),
    "1092": (AMATEUR, "amateur", "Kiev 'Kunst-vinkl'"),

    # ---- music hall / kleinkunst / vaudeville / variety -> Kleinkunst ----
    "950":  (KLEINKUNST, "kleinkunst", "Kleyn-kunst teater 'Remikat'"),
    "2663": (KLEINKUNST, "kleinkunst", "Kleynkunst-teater 'ARRT'"),
    "1109": (KLEINKUNST, "kleinkunst", "Miniatur-teater 'Kometa'"),
    "1508": (KLEINKUNST, "kleinkunst", "Miniatur-teater"),
    "1531": (KLEINKUNST, "kleinkunst", "Odessa miniatur-teater (David Keyzerovitsh)"),
    "1775": (KLEINKUNST, "kleinkunst", "Metropolitan Music Hall"),
    "1841": (KLEINKUNST, "kleinkunst", "Yidishn kinstler kabaret"),
    "1918": (KLEINKUNST, "kleinkunst", "Wertheim's Variete"),
    "2676": (KLEINKUNST, "kleinkunst", "Eldritch Vaudeville House"),
    "636":  (KLEINKUNST, "kleinkunst", "Azazel — Warsaw kleynkunst/satirical revue"),
    "2667": (KLEINKUNST, "kleinkunst", "Nay-Azazel — kleynkunst revue"),
    "2828": (KLEINKUNST, "kleinkunst", "Yidishn teater 'Nay-Azazel'"),
    "1878": (KLEINKUNST, "kleinkunst", "Sambatyon — Warsaw kleynkunst revue"),
    "2664": (KLEINKUNST, "kleinkunst", "Sambatyon"),
    "1500": (KLEINKUNST, "kleinkunst", "Azazel-Sambatyon"),
    "2905": (KLEINKUNST, "kleinkunst", "'Der krumer shpigl' — satirical revue"),
    "1756": (KLEINKUNST, "kleinkunst", "Reklam — Vienna kleynkunst"),
    "2673": (KLEINKUNST, "kleinkunst", "Khaliastre — kleynkunst troupe/revue"),
    "1980": (KLEINKUNST, "kleinkunst", "'Maydim' — Warsaw marionette/kleynkunst"),
    "2686": (KLEINKUNST, "kleinkunst", "Yidish marionetn-teater"),
    "1127": (KLEINKUNST, "kleinkunst", "Shpinvebs — kleynkunst"),
    "949":  (KLEINKUNST, "kleinkunst", "Dovke — kleynkunst"),
    "2825": (KLEINKUNST, "kleinkunst", "Freykunst — kleynkunst"),
    "1802": (KLEINKUNST, "kleinkunst", "Khad gadya — kleynkunst"),
    "1277": (KLEINKUNST, "kleinkunst", "Balagantshik — cabaret"),
    "727":  (KLEINKUNST, "kleinkunst", "Balagneyden — kleynkunst"),
    "728":  (KLEINKUNST, "kleinkunst", "Balagneyden (dup)"),
    "729":  (KLEINKUNST, "kleinkunst", "Balagneyden (dup)"),
    "1371": (KLEINKUNST, "kleinkunst", "'Di muze' — kleynkunst"),
    "1372": (KLEINKUNST, "kleinkunst", "'Di muze' (dup)"),
    "1541": (KLEINKUNST, "kleinkunst", "Internatsional teater fun kleyne farmes"),
    "1658": (KLEINKUNST, "kleinkunst", "Alukrainishn melukhe-teater fun kleyne farmen 'Gezkult'"),
    "2675": (KLEINKUNST, "kleinkunst", "Madikat — kleynkunst"),
    "1836": (KLEINKUNST, "kleinkunst", "YIKTOV — kleynkunst"),
    "1780": (KLEINKUNST, "kleinkunst", "YIKUT — kleynkunst"),
    "2674": (KLEINKUNST, "kleinkunst", "'HaMatate' — Hebrew satirical revue"),

    # ---- Gesellschaft / kreiz -> Theatre-related Society/ Union ----------
    "606":  (SOCIETY, "gezelshaft", "Literarish-dramatishe gezelshaft"),
    "621":  (SOCIETY, "gezelshaft", "Di dortike yidishe dramatishe gezelshaft"),
    "1283": (SOCIETY, "gezelshaft", "Yidishe teater gezelshaft"),
    "947":  (SOCIETY, "gezelshaft", "Vilner kunstgezelshaft"),
    "1774": (SOCIETY, "gezelshaft", "Gezelshaft 'Fraye yidishe bine'"),
    "674":  (SOCIETY, "gezelshaft", "Kovner yidisher dramatisher gezelshaft"),
    "419":  (SOCIETY, "gezelshaft", "The Yiddish Theatre Society"),
    "717":  (SOCIETY, "kreiz", "Kinder-libhober-kreyz"),
    "724":  (SOCIETY, "kreiz", "Yidishn dramatishn kreyz"),
    "743":  (SOCIETY, "kreiz", "'Dramkreyz M. Vintshevski'"),
    "872":  (SOCIETY, "kreiz", "Yidishn dramatishn kreyz"),
    "1119": (SOCIETY, "kreiz", "Dramkreyz at the Minsk club 'Royter boyer' — circle, not club"),
    "1120": (SOCIETY, "kreiz", "Kultur-kreyz named for Turgenev"),
    "1501": (SOCIETY, "kreiz", "Literarish-dramatishn kreyz named for Y. Pirozhnikov"),
    "1921": (SOCIETY, "kreiz", "Drame-kreyz 'Di bine'"),
    "1832": (SOCIETY, "farein", "Kovner yidishn dramatishn farein"),
    "1922": (SOCIETY, "farein", "Goldfaden-farein"),

    # ---- symphonies / choirs / opera / philharmonic -> Musical organization
    "31":   (MUSIC, "opera", "The Russian-Jewish Opera Company"),
    "32":   (MUSIC, "opera", "The Hebrew Opera and Dramatic Company"),
    "33":   (MUSIC, "opera", "The Roumanian Opera Company"),
    "145":  (MUSIC, "opera", "Grand Opera House"),
    "155":  (MUSIC, "music", "Academy of Music"),
    "371":  (MUSIC, "opera", "Rumanian Opera, Bucharest"),
    "532":  (MUSIC, "opera", "Yaser opere"),
    "1099": (MUSIC, "opera", "Zimin opera"),
    "1192": (MUSIC, "opera", "Kiever opere-hoyz"),
    "1213": (MUSIC, "opera", "Rusishn operetn-teater"),
    "1282": (MUSIC, "opera", "Rusishn operetn-teater 'Buf'"),
    "1530": (MUSIC, "opera", "Rusisher operete in Odes"),
    "1534": (MUSIC, "opera", "Italyenisher opere"),
    "1543": (MUSIC, "opera", "Ortike opere"),
    "1657": (MUSIC, "opera", "Kharkover rusishn opere-teater"),
    "1835": (MUSIC, "opera", "Lemberger poylisher opere"),
    "1879": (MUSIC, "opera", "Yidishn operetn-teater Tsentral"),
    "1905": (MUSIC, "opera", "Dos Grend Opere Hoyz"),
    "2677": (MUSIC, "opera", "Metropolitan-opere"),
    "2815": (MUSIC, "opera", "Berliner komisher opere"),
    "2830": (MUSIC, "opera", "Milaner opere"),
    "2873": (MUSIC, "opera", "Metropolitan-opere (dup)"),
    "2886": (MUSIC, "opera", "City Center Opera Company"),
    "2913": (MUSIC, "opera", "Metropolitan Opera House"),
    "1027": (MUSIC, "opera", "Riger komedye un operetn-teater"),
    "517":  (MUSIC, "philharmonic", "Vilner filharmonye"),
    "586":  (MUSIC, "philharmonic", "Keshenyever filharmonye"),
    "1795": (MUSIC, "philharmonic", "Lodzher 'Filharmonye'-zal"),
    "2804": (MUSIC, "philharmonic", "Sverdlovsker filharmonye"),
    "1901": (MUSIC, "philharmonic", "Filharmanik-oyditoryum"),
    "1805": (MUSIC, "music", "Ekedemi ov Myuzik — Academy of Music"),
    "1927": (MUSIC, "gezangs-farein", "Petersaner yidishn gezangs-farein — singing society"),
    "1128": (MUSIC, "orchestra", "Orkester fun rusishn teater"),
    "2672": (MUSIC, "music", "Carnegie Hall — concert hall"),
    "2891": (MUSIC, "music", "Hollywood Bowl — concert venue"),

    # ---- שול -> Religious institutions/organizations ---------------------
    # 1189 'Shnayderisher shul' is a TAILORS' shul (trade synagogue) in the
    # lexicon's usage, so the שול rule fires as religious.
    "1189": (RELIGIOUS, "shul", "Shnayderisher shul — tailors' synagogue"),
    "1816": (RELIGIOUS, "temple", "'Templ' — synagogue used as a hall"),
    "1814": (RELIGIOUS, "temple", "Feynman kunst-templ"),

    # ---- Studios -> Theatre education -----------------------------------
    "1094": (THEATRE_EDU, "studio", "Kiever teater-studye 'Onheyb'"),
    "1121": (THEATRE_EDU, "studio", "Dramstudye Robfak"),
    "1122": (THEATRE_EDU, "studio", "Rafalskis yidisher studye"),
    "1188": (THEATRE_EDU, "studio", "Yidishe teater-studye"),
    "1190": (THEATRE_EDU, "studio", "Kiever dramstudye"),
    "1203": (THEATRE_EDU, "studio", "Kiever studye"),
    "1285": (THEATRE_EDU, "studio", "Yidishe teater-studye"),
    "1502": (THEATRE_EDU, "studio", "Vaysruslendish-yidisher melukhe-studye"),
    "2899": (THEATRE_EDU, "studio", "Actors Laboratory — acting studio"),

    # ---- not theatres, but none of the six rules names them -------------
    # Caught while reading all 539; each is plainly not a theatre.
    "740":  ("Heritage Institution", "museum", "Natsyonaln muzey in Yerushalayim"),
    "2816": ("Heritage Institution", "museum", "Grend Muzeum"),
    "735":  ("Heritage Institution", "kultur-center", "Kultur-senter"),
    "1195": ("Heritage Institution", "kinstler-heym", "Kinstler-heym — artists' house"),
    "1833": ("Heritage Institution", "folks-hoyz", "Folks-hoyz — people's house"),
    "1656": ("Heritage Institution", "arbeter-hoyz", "Yidishn arbeter-hoyz"),
    "1804": ("Media (Radio/ Film/TV)", "kino", "Kino 'Arkadia' — cinema"),
    "1124": ("Not an organization", "park", "Gubernators gartn — a park"),
    "1204": ("Not an organization", "park", "Alekseyevs park — a park"),
    "1912": ("Not an organization", "garden", "Shimen Marks gertndl — a garden"),
    "1212": ("Not an organization", "granary", "R' Noyekh Reyneses shpaykhler — a granary"),
    "2249": ("Not an organization", "street", "Brodvey — Broadway, a street"),
    "1813": ("Not an organization", "address", "'Philadelphia bay Onshel Shor' — a person's place"),
    "1353": ("Not an organization", "person", "Leon Blank — a person, not an org"),
    "700":  ("Not an organization", "event", "Mozgovoys forshtelungen — performances, not an org"),
    "1577": ("Not an organization", "event", "Hebreyishe forshtelungen (Katsenelzon) — performances"),
    "1843": ("Not an organization", "event", "Di yudishe kinstler shpile — performances"),
    "1189_": (EDUCATION, "shule", "placeholder, unused"),
}
DECISIONS.pop("1189_", None)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(CORE_DB_TSV, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    by_id = {r["db_id"].strip(): r for r in rows}

    changed = 0
    already = 0
    missing: list[str] = []
    not_theatre: list[str] = []
    per_rule: dict[str, int] = {}

    for db_id, (new_type, rule, note) in DECISIONS.items():
        row = by_id.get(db_id)
        if row is None:
            missing.append(db_id)
            continue
        cur = (row.get("org_type") or "").strip()
        if cur.lower() != "theatre":
            if cur == new_type:
                already += 1
            else:
                not_theatre.append(f"{db_id} (is '{cur}', wanted '{new_type}')")
            continue
        row["org_type"] = new_type
        changed += 1
        per_rule[rule] = per_rule.get(rule, 0) + 1
        label = row.get("name") or row.get("name_yiddish") or ""
        print(f"  {db_id:>5}  Theatre -> {new_type:<38} [{rule}]  {label[:45]}  — {note}")

    print()
    for rule, n in sorted(per_rule.items(), key=lambda kv: -kv[1]):
        print(f"  {rule:<16} {n}")
    print()
    print(f"decisions: {len(DECISIONS)}  changed: {changed}  already-applied: {already}")
    if not_theatre:
        print(f"  NOT Theatre any more (left alone): {len(not_theatre)}")
        for s in not_theatre:
            print(f"    {s}")
    if missing:
        print(f"  WARN not in core_db: {', '.join(missing)}")

    remaining = sum(1 for r in rows if (r.get("org_type") or "").strip().lower() == "theatre")
    print(f"still typed Theatre after this pass: {remaining}")

    if args.dry_run:
        print("\n--dry-run: core_db.tsv NOT written")
        return

    with open(CORE_DB_TSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator="\r\n")
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {CORE_DB_TSV}")


if __name__ == "__main__":
    main()
