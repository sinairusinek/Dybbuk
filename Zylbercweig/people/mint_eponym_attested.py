#!/usr/bin/env python3
"""Mint the people the Leksikon mentions but never gave an entry.

These came out of the eponym track: an organisation is named after someone, the
judge read the name from the sentences, and nothing in person_hub.tsv matches --
because the Leksikon mentions them inside other people's entries and never gave
them a heading of their own. `attested_in_leksikon = mentions_only`, so
`probably_not_zylbercweig` stays EMPTY per README_new_mints.md: a mention counts
as attestation.

Approved by Sinai 2026-10-15.

NOT minted here, and why:
  - 4 already exist under a spelling the recall index could not key:
    אייווען אַבראַמסאָן = db735, אַלעקסאנדער מאַגיידזאָן = db2371,
    סידי טאָל = db3070 (טהאל), חיהלע פֿרידמאַן = HUB-P-5-facs_604.
  - 6 are not theatre people (rabbinic/literary yeshiva eponyms) -> they go to
    names_not_minted.tsv, kept rather than deleted.
  - מינע און דיאַנאַ בלומעפעלד is NOT two people: מינע is מיינע, "my", in
    Jonas Turkow's first-person narration. Diana Blumenfeld has her own entry.
  - ברידער קאָכאַנסקי: the Leksikon never names them; count unknown.

  python3 mint_eponym_attested.py            # dry run
  python3 mint_eponym_attested.py --execute
"""
from __future__ import annotations

import argparse
import csv
import pathlib

csv.field_size_limit(10 ** 9)

HERE = pathlib.Path(__file__).resolve().parent
PEOPLE_DB = HERE / "people_db.tsv"
EVIDENCE = HERE / "new_mints_evidence.tsv"

# (yiddish, english, gender, born, died, role, org_db_ids, n_mentions,
#  surfaces, leksikon_quote, open_questions)
MINTS = [
    ("יעקב ליבערט", "Yankev Libert", "Male", "", "", "actor, troupe director",
     "1402|790|998|1057", 25, "יעקב ליבערט",
     "25 mentions, no heading. The Leksikon has a ליבּערט, יאַנקעוו entry (db2282) "
     "already confirmed as the eponym of db790, db998 and db1057 -- this row is the "
     "SAME NAME and may be that person; see open_questions.",
     "May duplicate db2282 ליבּערט, יאַנקעוו. Minted separately because the recall pass "
     "read him at db1402 where 2282 was not a candidate. CHECK BEFORE LINKING."),
    ("אַדאָלף סעגאָל", "Adolf Segol", "Male", "", "", "actor",
     "855|860|1381|1388", 16,
     "אַדאָלף סעגאַל | אַדאָלף סעגאָל | אַדאָלף סעגאל | אָדאָלף סעגאָל",
     "16 mentions across 4 spellings, no heading. "
     "'דורכן רומעניש-יידישן אַקטיאָר אַדאָלף סעגאָל אַריבער אין 1923 אויף דער יידישער בינע'",
     "One person under two vocalisations (אַדאָלף / אָדאָלף); minted once."),
    ("יעקב ראַטבאום", "Yankev Rotbaum", "Male", "", "", "director",
     "1482", 11, "יעקב ראַטבאום | יעקב ראָטבאָום | יעקב ראָטבאום",
     "11 mentions, no heading. 'אויפֿגעפֿירט אין יוני 1953 אין ברעסלוי א\"ר פֿון יעקב ראַטבאום'",
     "Likely the Polish-Jewish director Jakub Rotbaum (1901-1994); not verified here."),
    ("אידאַ קאַמינסקי", "Ida Kaminska", "Female", "", "", "actress",
     "864", 8, "אידא קאַמינסקי | אידא קאָמינסקי | אידא קאמינסקי",
     "8 mentions, NO heading of her own, though her mother אסתר-רחל (db3610), father "
     "אַברהם יצחק (db3613) and sister רעגינאַ (db3612) all have entries. "
     "'און אידא (קאָמינסקי), וואָס דאָרפן טאַקע שפּילן אַ מאַמע מיט אַ טאַכטער'",
     "The surface 'אידא קאַמינסקאַ' is currently misattached to her sister Regina's hub "
     "HUB-D3612 (b.1894, d.1913) -- that needs correcting separately."),
    ("לייזער בערנשטיין", "Leyzer Bernshteyn", "Male", "", "", "actor",
     "889", 7, "לייזער בערנשטיין", "7 mentions, no heading.", ""),
    ("בעטי קעניג", "Bety Kenig", "Female", "", "", "actress",
     "1400", 7, "בעטי קעניג",
     "7 mentions, no heading. The Leksikon has קעניג דינה (db3617) and "
     "קעניג, מאָריס (db2102); Bety is unheaded.", ""),
    ("לאָלאַ פאָלמאַן", "Lola Folman", "Female", "", "", "actress, singer",
     "960", 4, "לאָלאַ פאָלמאַן | לאָלאַ פֿאָלמאַן", "4 mentions, no heading.", ""),
    ("יעקב בערלין", "Yankev Berlin", "Male", "", "", "troupe director",
     "1580", 4, "יעקב בערלין | יעקב קאָן-בערלין",
     "4 mentions, no heading. Co-names db1580 with Molly Cohen.", ""),
    ("אַלעקס שטיין", "Alex Shteyn", "Male", "", "", "actor",
     "883", 3, "אַלעקס שטיין", "3 mentions, no heading.", ""),
    ("סאָניע ברין", "Sonye Brin", "Female", "", "", "actress",
     "1644", 2, "ברין סאָניע | סאָניע ברין",
     "2 mentions, no heading. 'שפּילט דערנאָך מיט קאָניעווסקא, ברייטמאַן און סאָניע ברין "
     "אין קרעמענטשוג'. The Leksikon has ברין, שלום (db1394) and ברין, אַליאַ.", ""),
    ("גינאַ זלאָטאַיאָ", "Gina Zlotayo", "Female", "", "", "actress",
     "2326", 2, "גינאַ זלאָטאַיאָ",
     "2 mentions, no heading. Ruthie independently named her in the RA workbook "
     "comments as 'גינאַ זלאָטאַיאָס'.", ""),
    ("ליאַ לאָרגמאַן", "Lea Largman", "Female", "", "", "actress",
     "825", 1, "ליאַ לאָרגמאַן",
     "1 mention, no heading. Ruthie independently named her as 'Lea Largman'. "
     "NOT ברין, אַליאַ -- that surname-only match is the documented trap.", ""),
    ("עדוואַרד מאָרגולעס", "Edvard Morgules", "Male", "", "", "troupe director",
     "893", 1, "עדוואַרד מאָרגולעס",
     "1 mention, no heading. 'באַלד טרעט וו. אַריין אין עדוואַרד מאָרגולעסעס טרופּע אין סטרי'",
     "The Leksikon has מאַרגאָליס, עדוואַרד (יקותיאל) db2394, b.1850, surface "
     "'יקותיאָל מאַרגולעס'. Same given name and a close surname -- POSSIBLY the same "
     "person. Check before linking."),
    ("יוסף ווייסהאָף", "Yosef Veyshof", "Male", "", "", "actor",
     "983", 1, "יוסף ווייסהאָף", "1 mention, no heading.", ""),
    ("מישע נעמער", "Mishe Nemer", "Male", "", "", "actor",
     "1361", 1, "מישע נעמער", "1 mention, no heading.", ""),
    ("מיכאַאיל הערמאָלענסקי", "Mikhail Hermolenski", "Male", "", "", "actor",
     "1424", 1, "מיכאַאיל הערמאָלענסקי", "1 mention, no heading.", ""),
    ("מאַקסים עלווינג", "Maksim Elving", "Male", "", "", "actor",
     "1438", 1, "מאַקסים עלווינג",
     "1 mention, no heading. 'געשפּילט דייטש טעאַטער מיט אַ וואַנדער-טרופּע א'אַ פֿון "
     "זאַקסישן הויף-שוישפּילער מאַקסים עלווינג' -- a Saxon court actor.", ""),
    ("פֿראַנץ סטעפּאַנסקי", "Frants Stepanski", "Male", "", "", "actor",
     "1470", 1, "פראַנץ סטעפאַנסקי", "1 mention, no heading.", ""),
    ("הערטאָ וויטש", "Herto Vitsh", "", "", "", "actor",
     "1478", 1, "הערטאָ וויטש", "1 mention, no heading.",
     "Gender not stated in the Leksikon."),
    ("לאה ראַטבאום", "Leye Rotbaum", "Female", "", "", "actress",
     "1482", 1, "לאה ראַטבאום",
     "1 mention, no heading. 'האָט ער געשפּילט אין וואַרשע א'אַ פֿון יעקב און לאה ראַטבאום' "
     "-- a couple; db1482 names both.", ""),
    ("מאָלי קאָהן", "Molly Cohen", "Female", "", "", "troupe director",
     "1580", 1, "מאלי קאָהן",
     "1 mention, no heading. 'דערנאָך געשפּילט ביי מאלי קאָהן אין קליוולאַנד'", ""),
    ("יאָזער פֿיליפּ", "Yozer Filip", "Male", "", "", "actor",
     "1638", 1, "יאָזער פֿיליפּ", "1 mention, no heading.", ""),
    ("איפּאָליטאָוו-אַנדרעיעוו", "Ippolitov-Andreyev", "", "", "", "troupe director",
     "1725", 1, "איפּאָליטאָוו-אַנדרעיעוו",
     "1 mention, no heading. 'שפּילט ער אין דערדאָזיקער טרופּע א'ר פֿון "
     "איפּאָליטאָוו-אַנדרעיעוו און י. ביעלאָקין'",
     "Surname only -- no given name anywhere in the corpus. Gender unknown."),
    # The Czystogórski brothers: named in the Leksikon only as a collective,
    # identified individually from Polish sources.
    ("לודוויק טשיסטאָגורסקי", "Ludwik Czystogórski", "Male", "1855", "1933-01-28",
     "actor, operetta singer, theatre director",
     "", 2, "ברידער טשיסטאָגורסקי",
     "The Leksikon names only 'די ברידער טשיסטאָגורסקי' as a collective, directing a "
     "Polish-language staging of Goldfaden's שולמית that toured the Polish provinces "
     "in 1888: 'א נייע פוילישע טרופע אונטער דער דירעקציע פונ די ברידער טשיסטאגורסקי "
     "(וועגנ וועלכע מהאט געמורמלט, אז זיי זיינענ לאדזשער יידנ)'. Named individually "
     "from Polish sources, not from the Leksikon.",
     "Real surname REINBERG; Czystogórski is the Polish stage name. The Leksikon's "
     "1888 Lodz attribution is hearsay ('מ'האט געמורמלט') and is NOT confirmed by the "
     "Polish sources, which place the family in Warsaw with Ludwik's own company from "
     "1891. Possible 4th sibling: Bellari-Czystogórska Helena (H. Reinberg, "
     "1878-1940 Warsaw, singer), relation unstated."),
    ("פֿעליקס טשיסטאָגורסקי", "Feliks Feliksiewicz", "Male", "", "",
     "singer, actor", "", 2, "ברידער טשיסטאָגורסקי",
     "Named in the Leksikon only within 'די ברידער טשיסטאָגורסקי' (see Ludwik).",
     "Real surname REINBERG, stage name Feliksiewicz. NOT Dionizy Feliksiewicz "
     "(d.1897 Kraków), a different family."),
    ("בענעדיקט טשיסטאָגורסקי", "Benedykt Remy", "Male", "", "",
     "singer, actor", "", 2, "ברידער טשיסטאָגורסקי",
     "Named in the Leksikon only within 'די ברידער טשיסטאָגורסקי' (see Ludwik).",
     "Real surname REINBERG, stage name Remy."),
]

CZYSTO_SOURCES = (
    "Słownik biograficzny teatru polskiego 1765-1965 (PWN, 1973): Czystogórski "
    "Ludwik p.113, Feliksiewicz Feliks pp.161-162, Remy Benedykt p.591; IPN "
    "Indeks nazwisk, pseudonimów i przydomków, which files all three under one "
    "surname -- 'Reinberg (Remy) Benedykt', 'Reinberg (Feliksiewicz) Feliks', "
    "'Reinberg (Czystogórski) Ludwik', all on pp.202 and 214, with each stage "
    "name redirected 'zob. Reinberg'"
)
CZYSTO_URL = ("https://pl.wikipedia.org/wiki/Ludwik_Czystog%C3%B3rski ; "
              "https://ipn.gov.pl/download/1/1083303/Indeksosob.pdf")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    raw = PEOPLE_DB.read_bytes()
    term = "\r\n" if raw.count(b"\r\n") else "\n"
    rows = list(csv.DictReader(PEOPLE_DB.open(encoding="utf-8"), delimiter="\t"))
    fields = list(rows[0].keys())
    ids = sorted(int(r["db_id"]) for r in rows
                 if (r.get("db_id") or "").strip().isdigit())
    next_id = ids[-1] + 1

    ev_rows = list(csv.DictReader(EVIDENCE.open(encoding="utf-8"), delimiter="\t"))
    ev_fields = list(ev_rows[0].keys())
    ev_raw = EVIDENCE.read_bytes()
    ev_term = "\r\n" if ev_raw.count(b"\r\n") else "\n"

    new_people, new_ev = [], []
    for n, (yid, eng, gender, born, died, role, orgs, nmen, surfaces,
            quote, oq) in enumerate(MINTS):
        db_id = str(next_id + n)
        czysto = "טשיסטאָגורסקי" in yid
        r = {k: "" for k in fields}
        r.update({
            "db_id": db_id, "hebname": yid, "english": eng, "gender": gender,
            "date_born": born, "date_died": died,
            "external_source": (CZYSTO_SOURCES if czysto else
                                f"Zylbercweig Leksikon ({nmen} mention"
                                f"{'s' if nmen != 1 else ''}, no entry)"),
            "professional_role_perf": role,
            "name_variants": surfaces,
            "source": "eponym attested sweep (Sinai 2026-10-15)",
            "raw": f"{eng}|{db_id}|{yid}" if eng else f"|{db_id}|{yid}",
        })
        new_people.append(r)

        e = {k: "" for k in ev_fields}
        e.update({
            "db_id": db_id, "name_english": eng, "name_yiddish": yid,
            "attested_in_leksikon": "mentions_only",
            "leksikon_evidence": quote,
            "primary_attestation": (CZYSTO_SOURCES if czysto else
                                    f"Zylbercweig Leksikon, {nmen} mention"
                                    f"{'s' if nmen != 1 else ''} in other people's "
                                    f"entries; surfaces: {surfaces}"),
            "source_type": ("Polish theatre reference works" if czysto
                            else "Leksikon mention"),
            "citation_url": CZYSTO_URL if czysto else "",
            "role": role,
            "corroborating_people": (
                "eponym of core_db org(s) " + orgs if orgs else
                "named only as a collective, 'די ברידער טשיסטאָגורסקי'"),
            "open_questions": oq,
            "minted_by": "Sinai",
            "minted_at": "2026-10-15",
        })
        new_ev.append(e)

    print(f"people_db: {len(rows)} -> {len(rows) + len(new_people)} rows")
    print(f"minting {len(new_people)} people at {next_id}-{next_id + len(new_people) - 1}")
    print(f"evidence: {len(ev_rows)} -> {len(ev_rows) + len(new_ev)} rows")
    print(f"line endings preserved: people_db {term!r}, evidence {ev_term!r}")
    print()
    for r in new_people:
        print(f"  {r['db_id']}  {r['hebname'][:26]:<26} {r['english'][:26]:<26} "
              f"{r['professional_role_perf'][:28]}")

    if not args.execute:
        print("\nDRY RUN -- nothing written. Re-run with --execute.")
        return

    with PEOPLE_DB.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator=term)
        w.writeheader()
        w.writerows(rows + new_people)
    with EVIDENCE.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ev_fields, delimiter="\t",
                           lineterminator=ev_term, extrasaction="ignore")
        w.writeheader()
        w.writerows(ev_rows + new_ev)
    print(f"\nwrote {PEOPLE_DB.name} and {EVIDENCE.name}")


if __name__ == "__main__":
    main()
