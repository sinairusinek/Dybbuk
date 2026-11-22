#!/usr/bin/env python3
"""SPLIT rows (all types) and OTHER-typed rows the text can settle, 2026-11-22.

Approved by Sinai 2026-11-22 ("yes, go ahead") on the review built in that
session from every row's source passages (organizations_clustered.tsv +
people/entry_texts.tsv). This file is the ledger: every change names the
passage words that decide it.

Kinds of change
  P  partition: mentions of one SPLIT cluster that the passages show belong to
     a different body move to <cid>_Pnn (organizations_clustered.tsv too).
  N  enumerated name ("yeshivas of Grodno and Lomzhe"): review-only children
     <cid>_Nnn, one per named part; the parent retires to SPLIT_DONE.
  W  whole-row decision (incl. the remainder of a partitioned parent).
  T  OTHER rows retyped per CLASSIFICATION_POLICY.md (review row + its db row).

Left alone on purpose: the 4 "levels of one body" rows (American Jewish
Congress, Histadrut, JNF, United Hebrew Trades) and the IKUF organisation part
-> Maty's Doc; the plural "X's troupes" rows and the Shubert/Dillingham/Ziegfeld
houses -> the team Doc on agencies/managements; DEFER rows; rows a person
decided after the review was built (the script skips them and says so).

Writes (dry-run by default): core_db.tsv, org_alignment_review.tsv,
organizations_clustered.tsv, activity_log.tsv. New ids from origin's
high-water mark at run time. Idempotent. Run sync_address_cards.py afterwards.

Usage: python3 apply_split_other_2026_11_22.py [--apply]
"""
from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10 ** 9)
ORG = Path(__file__).resolve().parent
sys.path.insert(0, str(ORG.parent / "zalmen"))
import mention_removals  # noqa: E402

REVIEWER = "Sinai"
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TAG = "SPLIT/OTHER review 2026-11-22"
SRC = "apply_split_other_2026_11_22.py"
HEAD, SENT = "_ - heading", "_ - organizations - _ - relations - _ - original_sentence"

ED, TE, TH = "Education", "Theatre education", "Theatre"
JP, NJP, REL = "Jewish political bodies", "Non-Jewish political bodies", "Religious institutions/organizations"
PUB, JN, LAB = "Publisher", "Journals/ Newspapers", "Labour (factory/workshop)"
KK, HER, CUL = "Kleinkunst", "Heritage Institution", "Cultural/workers' club"
TU, TRS = "Trade Union / Professional Association", "Theatre-related Society/ Union"

# key: (English name, Yiddish name, address, parent_db_id, org_type)
MINTS = {
    "bonn_univ": ("University of Bonn", "אוניווערזיטעט פֿון באָן", "באָן", "", ED),
    "slutsk_yeshiva": ("Slutsk yeshiva", "סלוצקער ישיבה", "סלוצק", "", ED),
    "brisk_yeshiva": ("Brisk yeshiva", "בריסקער ישיבה", "בריסק", "", ED),
    "dimitrovsky_school": ("Dimitrovsky's music school, Kiev", "דימיטראָווסקיס מוזיקאַלישע שול", "קיעוו", "", ED),
    "unzer_studio": ("Theatre studio of the Unzer Teater society, New York (1923, M. Elkin)",
                     "טעאַטער-סטודיע ביי „אונזער טעאַטער“", "ניו יאָרק", "", TE),
    "odessa_art": ("Odessa Art School", "אָדעסער קונסט-שול", "אָדעס", "", ED),
    "marondi_school": ("Marondi's school (singing)", "מאַראָנדיס שול", "", "", ED),
    "kl_warsaw_school": ("Kultur-Lige theatre school, Warsaw (M. Weichert, 1929)",
                         "טעאַטער-שול ביי דער „קולטור-ליגע“ (וואַרשע)", "וואַרשע", "4698", TE),
    "siemaszko_school": ("Wanda Siemaszkowa's theatre school, Lviv", "וואַנדאַ שיעמאַשקאָס טעאַטער-שול", "לעמבערג", "", TE),
    "ghetto_hebrew_studio": ("Underground Hebrew theatre studio, Warsaw ghetto",
                             "העברעישע טעאַטער-סטודיע (וואַרשעווער געטאָ)", "וואַרשעווער געטאָ", "", TE),
    "merkaz_hebrew_studio": ("Hebrew theatre studio at Merkaz HaTseirim, Warsaw (1919, Sh. Kohn)",
                             "העברעישע טעאַטער-סטודיע אין „מרכז הצעירים“", "וואַרשע, קרולעווסקע גאַס", "", TE),
    "moscow_theatre_inst": ("Theatre institute, Moscow", "טעאַטראַלן אינסטיטוט (מאָסקווע)", "מאָסקווע", "", TE),
    "kyiv_theatre_inst": ("Theatre institute, Kiev", "טעאַטראַלן אינסטיטוט (קיעוו)", "קיעוו", "", TE),
    "brooklyn_pharmacy": ("Brooklyn College of Pharmacy", "ברוקלין קאָלעדזש (פֿאַרמאַצעווטיק)", "ניו יאָרק", "", ED),
    "folkspartei_ru": ("Folkspartei (Russia / Ukraine)", "פֿאָלקס-פּאַרטיי (רוסלאַנד)", "", "", JP),
    "romania_sp": ("Socialist Party of Romania", "סאָציאַליסטישע פּאַרטיי (רומעניע)", "", "", NJP),
    "cpusa": ("Communist Party of the USA", "קאָמוניסטישע פּאַרטיי (אַמעריקע)", "", "", NJP),
    "cp_soviet": ("Communist Party of the Soviet Union (Bolsheviks)", "קאָמוניסטישע פּאַרטיי (ראַטנפֿאַרבאַנד)", "", "", NJP),
    "zo_egypt": ("Zionist Organization in Egypt", "ציוניסטישע אָרגאַניזאַציע אין עגיפּטן", "", "", JP),
    "zps_krakow": ("Jewish Socialist Party, Kraków (1918)", "יידישע סאָציאַליסטישע פּאַרטיי (קראָקע)", "קראָקע", "", JP),
    "zion_weekly": ("Zion (weekly)", "„ציון“ (וואָכנבלאַט)", "", "", JN),
    "zion_vienna": ("Zion society, Vienna", "פֿאַריין „ציון“ (ווין)", "ווין", "", JP),
    "sheikeles_salon": ("Sholem Sheikeles' salon, East Side, New York", "שלום שייקעלעס סאַלאָן", "ניו יאָרק, איסט סייד", "", KK),
    "dik_salon": ("Dik's salon, East Side, New York", "דיקס סאַלאָן", "ניו יאָרק, איסט סייד", "", KK),
    "leszno27_shop": ("Shop at Leszno 27, Warsaw ghetto", "שאָפּ אויף לעשנאָ 27", "וואַרשעווער געטאָ, לעשנאָ 27", "", LAB),
    "oyfgang_monthly": ("Oyfgang (monthly for beginners, America)", "„אויפֿגאַנג“ (חודש-זשורנאַל, אַמעריקע)", "", "", JN),
    "oyfgang_sighet": ("Oyfgang (Sighet periodical)", "„אויפֿגאַנג“ (מאַרמאַראָש סיגעט)", "מאַרמאַראָש סיגעט", "", JN),
    "oyfgang_ba": ("Oyfgang publishing house, Buenos Aires", "פֿאַרלאַג „אויפֿגאַנג“ (בוענאָס איירעס)", "בוענאָס איירעס", "", PUB),
    "m_goldfarb": ("M. Goldfarb publishing house, Warsaw", "פערלאַג מ. גאָלדפֿאַרב", "וואַרשע", "", PUB),
    "melitopol_choral": ("Choral synagogue, Melitopol", "מעליטאָפּאָלער כאָר-שול", "מעליטאָפּאָל", "", REL),
    "kishinev_choral": ("Choral synagogue, Kishinev", "קעשענעווער כאָר-שול", "קעשענעוו", "", REL),
    "bialystok_choral": ("Choral synagogue, Białystok", "ביאַליסטאָקער כאָר-שול", "ביאַליסטאָק", "", REL),
    "nozyk": ("Nożyk Synagogue, Warsaw", "נאָזשיקס שול", "וואַרשע", "", REL),
    "moma_mexico": ("Museum of Modern Art, Mexico City", "מוזעום פֿאַר מאָדערנע קונסט (מעקסיקע סיטי)", "מעקסיקע סיטי", "", HER),
    "moma_ny": ("Museum of Modern Art, New York", "מוזעום פֿאַר מאָדערנע קונסט (ניו יאָרק)", "ניו יאָרק", "", HER),
    "cloak_suit": ("American Cloak and Suit Manufacturers' Association", "אַמעריקען קלאַוק ענד סוט מאַנופֿעקטשורערס אַסאָסיאיישאָן", "", "", TU),
    "lit_edu_comm": ("Education Commissariat for Lithuania", "בילדונגס-קאָמיסאַריאַט פֿאַר ליטע", "", "", NJP),
    "tsib": ("ציב (Poale Zion bureau), Vienna 1919", "ציב", "ווין", "", JP),
}

# ---- P: (parent, [(heading contains, sentence contains)], decision, target, child settlement, child type, evidence)
PARTIAL = [
    ("ORG-C00242", [("קאָלמאַנאָוויטש", "")], "NEW", "MINT:dimitrovsky_school", "קיעוו", "",
     "Kalmanovich 'אַ שילער אין אַ מוזיקאַלישער שול אונטער דער אָנפֿירונג פֿון ... דימיטראָווסקי' -- named by its head"),
    ("ORG-C00563", [("אונזער טעאַטער", "")], "NEW", "MINT:unzer_studio", "ניו יאָרק", TE,
     "Unzer Teater society (New York, 1923) 'געגרינדעט ... אַ טעאַטער-סטודיע מיט מ. עלקין אַלס פֿאַרוואַלטער'"),
    ("ORG-C01058", [("ראָגאָף", "")], "ALIGN", "2020", "ניו יאָרק", "",
     "Rogoff graduated the New York college -- City College (Maty: external source supports CCNY)"),
    ("ORG-C02040_Q01", [("טשערטאָוו", "")], "NEW", "MINT:odessa_art", "אָדעס", "",
     "Chertov 'אַוועק קיין אָדעס שטודירן אין דער דאָרטיקער קונסט-שול' -- the Odessa Art School"),
    ("ORG-C02040_Q01", [("אַדלער סעראַ", "")], "NEW", "MINT:marondi_school", "", "",
     "'דער דירעקטאָר פֿון דער קונסט-שול, מאַראָנדי, גיט איר אַ פֿרייען פּלאַץ' -- named by its head"),
    ("ORG-C02928", [("גרובער", "")], "NEW", "MINT:siemaszko_school", "לעמבערג", TE,
     "'וואַנדאַ שיעמאַשקאָ, וועלכע האָט אָנגעפֿירט מיט אַ טעאַטער-שול אין לעמבערג'"),
    ("ORG-C03094", [("מאָשקאָוויטש", "")], "NEW", "MINT:ghetto_hebrew_studio", "וואַרשעווער געטאָ", TE,
     "Moshkovitz engaged as director of the underground Hebrew studio in the Warsaw ghetto"),
    ("ORG-C03357", [("גאָטלאָבער", "")], "ALIGN", "1259", "פּעטערבורג", "",
     "Gottlober 'פֿון מפיצי השכלה' seeking support for HaBoker Or -- the society (Petersburg), not the Vilna school"),
    ("ORG-C03396", [("אַרטעף", "")], "ALIGN", "2026", "ניו יאָרק", TE,
     "ARTEF entry: the drama school attached to the New York theatre (teachers Mestel, Teitelbaum...)"),
    ("ORG-C03605", [("גערמאַן", "")], "NEW", "MINT:moscow_theatre_inst", "מאָסקווע", TE,
     "'אַריינגעטראָטן אין טעאַטראָלן אינסטיטוט אין מאָסקווע' (1925)"),
    ("ORG-C07327", [("קאַרפּמאַן", "")], "NEW", "MINT:brooklyn_pharmacy", "ניו יאָרק", "",
     "Karpman studied pharmacy, left at the 1918 draft -- before Brooklyn College existed (1930)"),
    ("ORG-C00970_Q01", [("טאָמער צונזער", ""), ("ראַבאָי", ""), ("גאַדינער", "")], "ALIGN", "2936", "ניו יאָרק", PUB,
     "books 'פֿאַרלאַג איקוף, ניו יאָרק, 1950' / 'פֿ\"ג איקוף (ניו-יאָרק)' 1968 / 'דער ניו יאָרקער איקוף ... אַרויסגעגעבן' -- the IKUF press"),
    ("ORG-C00621_Q01", [("פּאַלעי", ""), ("האָכבאַן", "")], "ALIGN", "2499", "ניו יאָרק", "",
     "Paley (New York 1901) and the Drama Guild helped by the Arbeter Institute, Socialist Party and Arbeter Ring -- the US Socialist Party"),
    ("ORG-C00621_Q01", [("דרוקער", "")], "NEW", "MINT:romania_sp", "", "",
     "Drucker joined the socialist party on returning home at the war's end, then wandered over Romania"),
    ("ORG-C02237", [("מאַרגאָלעס", ""), ("לעוויט", ""), ("לעלטשוק", "")], "NEW", "MINT:cpusa", "", "",
     "joined 1922 after arriving in America 1921 / Harlem, New York / after Unzer Teater, joined ARTEF -- the American party"),
    ("ORG-C02237", [("גאַדינער", ""), ("נוסינאָוו", ""), ("שוואַרצער", "")], "NEW", "MINT:cp_soviet", "", "",
     "Red Army 1918 then Moscow / 'צוריקגעקומען אין ראַטנפֿאַרבאַנד ... סוף 1919' / 1940, fled to Tashkent 1941 -- the Soviet party"),
    ("ORG-C01308_Q01", [("טייטש", "")], "NEW", "MINT:folkspartei_ru", "", "",
     "Teitsch, 1917 lecturing across Ukraine, 'אַ קורצע צייט ... אין דער פֿאָלקס-פּאַרטיי', then Moscow -- not the Polish party"),
    ("ORG-C01762", [("אַסטראָווסקי", ""), ("באָרענשטיין", "")], "ALIGN", "2497", "", "",
     "'אַרבעטער רינג, שולן, ייוואָ, אַרבעטער-קאָמיטעט, ציקאָ' -- the Jewish Labor Committee in America"),
    ("ORG-C02698", [("ניימאַן", "")], "NEW", "MINT:zo_egypt", "", "",
     "'צענטראַל-קאָמיטעט פֿון דער ציוניסטישער אָרגאַניזאַציע אין עגיפּטען'"),
    ("ORG-C02944", [("וולאַדעק", "")], "ALIGN", "2472", "ניו יאָרק", "",
     "Vladeck, in America from 1910, writes in the organs of the Jewish socialist party -- the Jewish Socialist Federation"),
    ("ORG-C05671", [("סיגעטער", "")], "NEW", "MINT:zion_weekly", "", JN,
     "'פֿון די וואָכנבלעטער ציון און דיאַ וואָהרהייט' -- a weekly, not the society"),
    ("ORG-C00142_Q01", [("ניי-אַזאַזעל", "")], "ALIGN", "1875", "וואַרשעווער געטאָ", "",
     "Sandler turned his theatre into a shop -- Chaim Sandler's shop, Nowolipie 72 (Maty)"),
    ("ORG-C01268_Q01", [("פֿרידמאַן", "")], "NEW", "MINT:oyfgang_monthly", "", JN,
     "Friedman, in America, 'מיטרעדאַקטאָר פֿון חודש-זשורנאָל פֿאַר אָנפֿאַנגער אויפֿגאַנג'"),
    ("ORG-C01268_Q01", [("דרוקער", "")], "NEW", "MINT:oyfgang_sighet", "מאַרמאַראָש סיגעט", JN,
     "'אָנגעהויבן שרייבן אין ... אויפֿגאַנג (מאַרמאַראָש סיגעט)' -- a periodical"),
    ("ORG-C04431", [("מעסטעל", ""), ("שאָר [שור], משה", "")], "ALIGN", "50", "וואַרשע", "",
     "both read 'פערלאַג ש. גאָלדפֿאַרב' -- Sinai's 10-10 publisher-audit split"),
    ("ORG-C04431", [("ראַקאָוו", ""), ("שאָר [שור], אַנשל", "")], "NEW", "MINT:m_goldfarb", "וואַרשע", "",
     "both read 'פערלאַג מ. גאָלדפֿאַרב' -- the separate M. Goldfarb imprint (10-10 audit)"),
    ("ORG-C01232", [("מאַרקאָוויטש", "")], "NEW", "MINT:melitopol_choral", "מעליטאָפּאָל", "",
     "Markovitch sang 2 years in the choral synagogue, Melitopol (cantor Schechtman)"),
    ("ORG-C01232", [("אַסטראָף", "")], "NEW", "MINT:kishinev_choral", "קעשענעוו", "",
     "Astroff, Kishinev: 'דער אויבערקאַנטאָר פֿון דער כאָר-שול, קילימניק'"),
    ("ORG-C01232", [("גרינהויז", "")], "NEW", "MINT:bialystok_choral", "ביאַליסטאָק", "",
     "Greenhaus, Białystok childhood, sang with Berman in the choral synagogue"),
    ("ORG-C01232", [("ווינאָגראָדאָוו", "")], "ALIGN", "1113", "ווילנע", "",
     "Vinogradov, Vilna childhood, 'אין כאָר-שול ביים חזן מיכאָלאָווסקי' -- the Vilna choral synagogue"),
    ("ORG-C02849_Q01", [("שאַכט", "וו. פּ. א"), ("סלאָוואָ-ליס", "וו. פּ. א")], "ALIGN", "3307", "ניו יאָרק", "",
     "'יידישער מלוכה-טעאַטער (וו. פּ. א.)' 1936, 1938-39 -- the WPA Federal Theatre's Yiddish unit, not Poland"),
    ("ORG-C00562", [("בליאַכער", "")], "ALIGN", "1085", "ווילנע", "",
     "'ערשטער יידישער דראַמאַטישער סטודיע אין ווילנע'"),
    ("ORG-C01166_Q01", [("פעלדמאַן, אהרן", "")], "", "", "", TU,
     "Feldman: shop worker, secretary of Local 156, then Local 5, writes in the union organ 'פֿאָרשריט' -- "
     "a needle-trades local, not the vaudeville actors' Local 5; which union is not stated"),
]

# ---- N: parent -> [(child Yiddish name, settlement, decision, target, child type, evidence)]
ENUM = {
    "ORG-C00063": [("אוניווערזיטעט פֿון קעניגסבערג", "קעניגסבערג", "ALIGN", "2074", "", "named part"),
                   ("אוניווערזיטעט פֿון באָן", "באָן", "NEW", "MINT:bonn_univ", "", "named part; rare kind + city")],
    "ORG-C00542": [("סלוצקער ישיבה", "סלוצק", "NEW", "MINT:slutsk_yeshiva", "", "named part; famous yeshiva"),
                   ("סלאָבאָדקער ישיבה", "סלאָבאָדקע", "ALIGN", "1973", "", "named part")],
    "ORG-C02501": [("וועסט-סייד יידישן סענטער", "לאָס אַנדזשעלעס", "ALIGN", "1903", "",
                    "Newman's parallel biography lists the Westside Center and the Institute as separate posts"),
                   ("אינסטיטוט פֿאַר יידישער דערציונג", "לאָס אַנדזשעלעס", "ALIGN", "1956", "", "as above")],
    "ORG-C03485_Q02": [("ישיבה פֿון בערעזין", "בערעזין", "GENERIC", "", "", "town yeshiva, not individuated"),
                       ("מירער ישיבה", "מיר", "ALIGN", "2040", "", "named part")],
    "ORG-C04803": [("גראָדנער ישיבה", "גראָדנע", "ALIGN", "2006", "", "named part"),
                   ("לאָמזשער ישיבה", "לאָמזשע", "ALIGN", "1950", "", "named part")],
    "ORG-C06628": [("ראָוונער ישיבה", "ראָוונע", "GENERIC", "", "", "town yeshiva, not individuated"),
                   ("בריסקער ישיבה", "בריסק", "NEW", "MINT:brisk_yeshiva", "", "named part; famous yeshiva")],
    "ORG-C00660": [("שלום שייקעלעס סאַלאָן", "ניו יאָרק", "NEW", "MINT:sheikeles_salon", "", "named part"),
                   ("דיקס סאַלאָן", "ניו יאָרק", "NEW", "MINT:dik_salon", "", "named part ('און אַנדערע' names nothing)")],
    "ORG-C04226": [("לאָקאַל 5", "ניו יאָרק", "ALIGN", "2363", "", "named part: the vaudeville actors' Local 5"),
                   ("לאָקאָל 1", "ניו יאָרק", "", "", "", "named part; which body 'Local 1' was is not stated")],
    "ORG-C04884": [("טרופּע פֿון קאַמינסקי", "", "ALIGN", "809", "", "named part"),
                   ("גענפערס טרופּע", "", "ALIGN", "254", "", "named part"),
                   ("ליפּאָווסקיס טרופּע", "", "ALIGN", "759", "", "named part"),
                   ("ראַפּעלס טרופּע", "", "ALIGN", "469", "", "named part"),
                   ("שאַראַוונערס טרופּע", "", "ALIGN", "629", "", "named part")],
}

# ---- W: (cid, decision, target, why, new review type or "")
GEN_SPLIT = "SPLIT not needed: every mention is the bare kind with a town (or none) -- a kind, not one school"
WHOLE = [
    ("ORG-C00494", "GENERIC", "", GEN_SPLIT + " ('יידישער רעגירונגס-שול')", ""),
    ("ORG-C00690", "GENERIC", "", GEN_SPLIT + " ('רוסישע שטאָט-שול', tier rule)", ""),
    ("ORG-C03452", "GENERIC", "", GEN_SPLIT + " ('פּוילישע גימנאַזיע', gymnasium rule)", ""),
    ("ORG-C03762", "GENERIC", "", GEN_SPLIT + " ('רוסישער רעאַל-שול', tier rule)", ""),
    ("ORG-C04383", "GENERIC", "", GEN_SPLIT + " ('מינסקער/פּינסקער ישיבה', town yeshiva)", ""),
    ("ORG-C04899", "GENERIC", "", GEN_SPLIT + " (bare 'קאַלעדזש')", ""),
    ("ORG-C07357", "GENERIC", "", GEN_SPLIT + " ('ישיבות פֿון מיהאַלאָוויץ-קאַשוי', plural town yeshivas)", ""),
    ("ORG-C02125", "GENERIC", "", "bare 'אַרבעטער-פֿאַריין' (a workers' association) in two unrelated passages", ""),
    ("ORG-C05962", "GENERIC", "", "'מיט דייטשע טרופּעס איבער דער פּראָווינץ' -- German troupes in general", ""),
    # remainders of partitioned parents
    ("ORG-C00242", "GENERIC", "", "Brodin: 'שטודירט און גראָדווירט די מוזיקאַלישע שול', no head, no place", ""),
    ("ORG-C00563", "ALIGN", "2033", "Potashinski, after the Weichert/Herman school, 'זומער 1925 ... מיט דער טעאַטער-סטודיע דעם טורנעע' -- that school's studio", TE),
    ("ORG-C01058", "GENERIC", "", "Buchwald: lecturer in chemistry at 'ניו-יאָרקער קאָלעדזש' -- which college is not stated", ""),
    ("ORG-C02040_Q01", "GENERIC", "", "Raskin 'באַזוכט אַ קונסט-שול' in Odessa -- 'an art school'", ""),
    ("ORG-C02928", "NEW", "MINT:kl_warsaw_school", "Fackel 1929 'ביי דער קולטור-ליגע געגרינדעט די טעאַטער-שול א'א פֿון מ. ווייכערט' -- not the 1922 school db 2033, not the Kiev studio db 795", TE),
    ("ORG-C03094", "NEW", "MINT:merkaz_hebrew_studio", "Kohn 1919 'געגרינדעט אין מרכז הצעירים אויף קרולעווסקע גאַס אַ העברעישע טעאַטער-סטודיע'", TE),
    ("ORG-C03357", "ALIGN", "3737", "Bastomski 'געענדיקט די פֿאָלקס-שול פֿון מפיצי השכלה' -- the Vilna school (as C01806)", ""),
    ("ORG-C03396", "ALIGN", "799", "Lampe 'קינסטלערישער אָנפֿירער פֿון אַ דראַמסטודיע אין וויטעבסק'", TE),
    ("ORG-C03605", "NEW", "MINT:kyiv_theatre_inst", "Nikolaievsky: Kiev theatre institute, graduated 1936 (Maty's source read)", TE),
    ("ORG-C07327", "ALIGN", "4942", "Kaplan (born 1935) 'אויך 2 סעמעסטערס אין ברוקליין-קאַלעדזש'", ""),
    ("ORG-C00198_Q03", "ALIGN", "189", "concert tours 'פֿאַרן אַרבעטער רינג' / founded a union -- the Arbeter Ring itself; no branch named", ""),
    ("ORG-C00621_Q01", "GENERIC", "", "Marmor 1897 'אַריינגעטראָטן אין דער געהיימער סאַציאַליסטישער אָרגאַניזאַציע' -- unnamed", ""),
    ("ORG-C02237", "", "", "Taubenshlag 1918 'אַריבער צו דער קאָמוניסטישער פּאַרטיי' before emigrating 1920 -- country not stated; left undecided", ""),
    ("ORG-C01308_Q01", "ALIGN", "2454", "Kahan (Warsaw, 'דאָס פֿאָלק' became its organ) and Gendel (Polish Jews) -- the Folkspartei in Poland", ""),
    ("ORG-C01762", "GENERIC", "", "Kharkov: the theatre committee 'גייט אַרונטער אונטערן קאָנטראָל פֿון אַן אַרבעטער-קאָמיטעט' -- an ad-hoc committee", ""),
    ("ORG-C02698", "GENERIC", "", "'אַריינגעטראָטן אין דער ציוניסטישער אָרגאַניזאַציע' / 'געוואָרן אַ מיטגליד' -- the movement at large", ""),
    ("ORG-C02944", "NEW", "MINT:zps_krakow", "both Stav entries: Kraków, 1918, 'יידישער סאַציאַליסטישער פּאַרטיי'", ""),
    ("ORG-C05671", "NEW", "MINT:zion_vienna", "Dolinger 'אַ מיטגליד אין פֿאַריין ציון אין ווין'", ""),
    ("ORG-C00142_Q01", "NEW", "MINT:leszno27_shop", "Riba 'געאַרבעט אין שאָפּ אויף לעשנאַ 27'", ""),
    ("ORG-C01268_Q01", "NEW", "MINT:oyfgang_ba", "Batoshansky 1927 'פֿאַרלאַג אויפֿגאַנג, בוענאָס איירעס' -- not db 2944 (that row holds the Havana journal)", ""),
    ("ORG-C01232", "", "", "Kustin 'אין כאָר-שול ביים חזן סיראָטאַ' -- city not stated (Sirota served Vilna and Warsaw); left undecided", ""),
    ("ORG-C02430", "NEW", "MINT:nozyk", "5 passages, all 'נאָזשיקס שול', Warsaw -- one synagogue; SPLIT not needed", ""),
    ("ORG-C00096_Q05", "ALIGN", "3463", "Levin, 1926 in Argentina, 'שפּילט אין פֿאָלקס-טעאַטער, אָלימפּאָ און אַמבו' -- Buenos Aires", ""),
    ("ORG-C00157_Q01", "ALIGN", "316", "46 passages, all New York (Brooklyn and Brownsville Liberty are Q02/Q03); SPLIT not needed", ""),
    ("ORG-C00157_Q05", "ALIGN", "316", "Besarabov 1921 in America 'ביי בערנאַרד יאַנג אין ליבערטי-טעאַטער'", ""),
    ("ORG-C00190_Q04", "ALIGN", "2239", "Newman 1932 Folksbiene: Leivick's 'הירש לעקערט', Peretz -- the New York Folksbiene", ""),
    ("ORG-C00481_Q01", "ALIGN", "840", "33 passages, all the Vienna Fraye Yidishe Folks-Bine (Deutsch, Mestel); SPLIT not needed", ""),
    ("ORG-C00515_Q04", "ALIGN", "130", "Hurwitz's Thalia 1900 in America; Groper 1899 with Kessler, Moshkovitch, Mogulesko -- the Bowery Thalia", ""),
    ("ORG-C02849_Q01", "ALIGN", "2846", "remaining 22 passages: the State Jewish Theatre in Poland (1949-)", ""),
    ("ORG-C00562", "ALIGN", "2033", "Potashinski and Perenson 'ערשטער יידישער דראַמאַטישער שול ... פֿון דיר. מ. ווייכערט'", ""),
    ("ORG-C01166_Q01", "ALIGN", "2363", "12 passages: Local 5, the Yiddish vaudeville actors' union", ""),
    # OTHER
    ("ORG-C01977_Q01", "NEW", "MINT:moma_mexico", "museum -> Heritage Institution (policy)", HER),
    ("ORG-C01977_Q02", "NEW", "MINT:moma_ny", "museum -> Heritage Institution (policy)", HER),
    ("ORG-C07435", "NEW", "MINT:cloak_suit", "employers' association -> Trade Union / Professional Association (policy)", TU),
    ("ORG-C04163", "NEW", "MINT:lit_edu_comm", "commissariat -> Non-Jewish political bodies (policy)", NJP),
    ("ORG-C01082", "NEW", "MINT:tsib", "Ratman, Vienna 1919, 'ארבעט אין ציב און אינעם באַראָכאָוו-אַרכיוו', then the Poale Zion world-conference split", JP),
    ("ORG-C02324", "GENERIC", "", "'יידישן קאָמיוניטי סענטער' in Chicago, unnamed -> Cultural/workers' club kind", CUL),
    ("ORG-C02298", "NOT_AN_ORG", "", "'יידישער קולטור-באַוועגונג' -- a movement, not a body", ""),
    ("ORG-C03619", "NOT_AN_ORG", "", "'דאָריאַ ניקאָדעמים' -- Dario Niccodemi, the playwright (Scampolo), misread as a body", ""),
    ("ORG-C03078", "GENERIC", "", "'אַן אומלעגאַלן סטודענטן-פֿאַריין' -- unnamed", ""),
    ("ORG-C05813", "GENERIC", "", "'סטודענטישע קאָלאָניעס' -- plural, unnamed", ""),
    ("ORG-C00715", "GENERIC", "", "'זשאַרגאָנישע קאָמיטעטן' -- plural, spread over the provinces", ""),
]

# ---- T: OTHER rows retyped, decision kept (review row + aligned db row if still OTHER)
RETYPE = {
    "ORG-C00133": ED, "ORG-C03702": ED,                 # research institutes
    "ORG-C02294_Q01": CUL, "ORG-C03110": CUL,           # Beit Am, ghetto youth club
    "ORG-C07288": TRS,                                  # Bleich Memorial Institute for Living Theatre
    "ORG-C02842": LAB,                                  # Tebenses' shop
    "ORG-C07295": JP,                                   # Young Judaea
    "ORG-C03178": ED,                                   # 'Di Bin' scouts (youth-educational)
}
# db 2944 'אויפֿגאַנג' is typed Publisher but its only cluster (C01268_Q02) is the Havana journal
DB_FIX = {"2944": {"org_type": JN, "address": "האַוואַנאַ"}}


def sk(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))


def read(p: Path):
    raw = p.read_bytes().decode("utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw, newline=""), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(p: Path, fields, rows, nl):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, delimiter="\t", lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    p.write_bytes(buf.getvalue().encode("utf-8"))


def pipes(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def origin_high_water() -> int:
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ORG, capture_output=True, timeout=120)
    out = subprocess.run(["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"],
                         cwd=ORG, capture_output=True, text=True, timeout=120, check=True).stdout
    return max(int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t") if r["db_id"].isdigit())


def main() -> int:
    apply = "--apply" in sys.argv
    kf, core, knl = read(ORG / "core_db.tsv")
    af, al, anl = read(ORG / "org_alignment_review.tsv")
    cf, cl, cnl = read(ORG / "organizations_clustered.tsv")
    byid = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    removed = mention_removals.load_removed_keys()
    log: list[list[str]] = []
    errors = 0
    counts: dict[str, int] = {}

    def bump(k):
        counts[k] = counts.get(k, 0) + 1

    def L(action, target, decision, text, extra):
        log.append([NOW, REVIEWER, "org_review", action, target, decision, f"{TAG}: {text}",
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    def note(row, text):
        prev = (row.get("reviewer_notes") or "").strip()
        row["reviewer_notes"] = f"{TAG}: {text}" + (" | " + prev if prev else "")
        row["reviewer"], row["reviewed_at"] = REVIEWER, NOW

    def unlink_everywhere(cid, keep=""):
        for r in core:
            if r["db_id"] != keep and cid in pipes(r["linked_cluster_ids"]):
                r["linked_cluster_ids"] = " | ".join(x for x in pipes(r["linked_cluster_ids"]) if x != cid)
                L("unlink", r["db_id"], "", f"cluster {cid} unlinked", {"cluster": cid})
                bump("unlink")

    def link(cid, d):
        t = byid[d]
        if cid not in pipes(t["linked_cluster_ids"]):
            t["linked_cluster_ids"] = " | ".join(pipes(t["linked_cluster_ids"]) + [cid])
            bump("link")

    next_id = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    have = {sk(r["name_yiddish"]): r["db_id"] for r in core if not r["deprecated"].strip()}

    def resolve(target: str) -> str:
        nonlocal next_id, errors
        if not target.startswith("MINT:"):
            if target and (target not in byid or byid[target]["deprecated"].strip()):
                print(f"ERROR target db {target} missing or deprecated")
                errors += 1
            return target
        name, yid, addr, parent, typ = MINTS[target[5:]]
        if sk(yid) in have:
            return have[sk(yid)]
        d = str(next_id)
        next_id += 1
        row = {f: "" for f in kf}
        row.update(db_id=d, name=name, name_yiddish=yid, address=addr, parent_db_id=parent, org_type=typ)
        core.append(row)
        byid[d] = row
        have[sk(yid)] = d
        L("mint", d, "NEW", name, {"name_yiddish": yid, "parent_db_id": parent, "org_type": typ})
        bump("mint")
        print(f"  mint db {d}  {name}")
        return d

    def stale(cid, expect=("", "SPLIT")):
        """A row a person decided after the review was built is left alone."""
        r = arow[cid]
        return r["decision"].strip() not in expect and r["reviewer"].strip() not in (REVIEWER, "")

    # ---- P
    used = {x["cluster_id"].strip() for x in cl} | set(arow)
    for parent, specs, dec, target, place, ctype, ev in PARTIAL:
        prow = arow.get(parent)
        if prow is None:
            print(f"ERROR no review row {parent}")
            errors += 1
            continue
        mine = [x for x in cl if x["cluster_id"].strip() == parent
                and any(sk(h) in sk(x[HEAD]) and sk(s) in sk(x[SENT]) for h, s in specs)]
        mine = [x for x in mine if mention_removals.mention_key(x) not in removed]
        if not mine:
            print(f"  skip {parent} {specs[0][0]}…: no matching mention left (already moved?)")
            continue
        heads = {sk(h) for h, _ in specs}
        got = {h for h in heads if any(h in sk(x[HEAD]) for x in mine)}
        allrows = [x for x in cl if x["cluster_id"].strip() == parent]
        if got != heads or len(mine) == len(allrows):
            print(f"ERROR {parent}: missing {heads - got} or spec selects the whole cluster")
            errors += 1
            continue
        n = 1
        while f"{parent}_P{n:02d}" in used:
            n += 1
        child = f"{parent}_P{n:02d}"
        used.add(child)
        d = resolve(target) if target else ""
        ids = {id(x) for x in mine}
        left = len(allrows) - len(mine)
        for x in allrows:
            if id(x) in ids:
                x["cluster_id"] = child
            x["cluster_size"] = str(len(mine) if id(x) in ids else left)
        a = dict(prow)
        a.update(cluster_id=child, cluster_size=str(len(mine)), decision=dec, aligned_db_id=d,
                 extracted_settlements=place, candidate_db_ids="", candidate_scores="", candidate_methods="",
                 reviewer_notes="", org_type=ctype or (byid[d]["org_type"] if d else prow["org_type"]))
        note(a, f"split off {parent} ({len(mine)} mention(s)) -> {dec or 'undecided'}{' db ' + d if d else ''}. {ev}")
        al.insert(al.index(prow) + 1, a)
        arow[child] = a
        prow["cluster_size"] = str(left)
        note(prow, f"{len(mine)} mention(s) split off to {child} ({dec or 'undecided'}{' db ' + d if d else ''})")
        if d:
            link(child, d)
        L("split", child, dec, f"{len(mine)} mention(s) of {parent} -> {child}; {ev}",
          {"from_cluster": parent, "aligned_db_id": d, "n_mentions": len(mine), "mentions": [x[HEAD] for x in mine]})
        bump("split")
        print(f"  split {parent} -> {child} ({len(mine)}) {dec or '-'} {d}")

    # ---- N
    for parent, parts in ENUM.items():
        prow = arow.get(parent)
        if prow is None:
            print(f"ERROR no review row {parent}")
            errors += 1
            continue
        if prow["decision"].strip() == "SPLIT_DONE":
            continue
        at = al.index(prow)
        made = []
        for i, (yid, place, dec, target, ctype, ev) in enumerate(parts, 1):
            cid = f"{parent}_N{i:02d}"
            if cid in arow:
                print(f"ERROR {cid} already exists")
                errors += 1
                continue
            d = resolve(target) if target else ""
            c = {h: "" for h in af}
            c.update(cluster_id=cid, canonical_yiddish=yid, name_variants=prow["name_variants"],
                     org_type=ctype or (byid[d]["org_type"] if d else prow["org_type"]),
                     extracted_settlements=place, decision=dec, aligned_db_id=d)
            note(c, f"name-split of {parent} ('{prow['canonical_yiddish']}') -> {dec or 'undecided'}"
                    f"{' db ' + d if d else ''}; {ev}")
            al.insert(at + i, c)
            arow[cid] = c
            if d:
                link(cid, d)
            made.append(f"{cid} {dec or 'undecided'}{' ' + d if d else ''}")
            L("split", cid, dec, f"name-split of {parent}: {yid}", {"from_cluster": parent, "aligned_db_id": d})
            bump("name_child")
        was = prow["decision"]
        prow["decision"], prow["aligned_db_id"] = "SPLIT_DONE", ""
        note(prow, f"{was} -> SPLIT_DONE; the name lists its parts: {', '.join(made)}")
        unlink_everywhere(parent)
        L("alignment", parent, "SPLIT_DONE", f"split into {len(made)} named parts", {"children": made})
        print(f"  name-split {parent}: {made}")

    # ---- W
    for cid, dec, target, why, ntype in WHOLE:
        r = arow.get(cid)
        if r is None:
            print(f"ERROR no review row {cid}")
            errors += 1
            continue
        d = resolve(target) if target else ""
        if (r["decision"], r["aligned_db_id"].strip()) == (dec, d) and (not ntype or r["org_type"] == ntype):
            continue
        if stale(cid):
            print(f"  SKIP {cid}: {r['reviewer']} decided {r['decision']} {r['aligned_db_id']} at {r['reviewed_at']}")
            continue
        was = (r["decision"], r["aligned_db_id"], r["org_type"])
        r["decision"], r["aligned_db_id"] = dec, d
        if ntype:
            r["org_type"] = ntype
        note(r, f"{was[0] or 'undecided'}{' db ' + was[1] if was[1] else ''} -> {dec or 'undecided'}"
                f"{' db ' + d if d else ''}{' (type ' + was[2] + ' -> ' + ntype + ')' if ntype and ntype != was[2] else ''}. {why}")
        L("alignment", cid, dec, why, {"aligned_db_id": d, "was": list(was), "org_type": r["org_type"]})
        bump("decision")
        unlink_everywhere(cid, keep=d)
        if d:
            link(cid, d)

    # partitioned parents left SPLIT / undecided hold no db link (e.g. C04431 on db 2861)
    for parent in {p[0] for p in PARTIAL}:
        if arow[parent]["decision"].strip() in ("SPLIT", ""):
            unlink_everywhere(parent)

    # ---- T
    for cid, typ in RETYPE.items():
        r = arow[cid]
        if r["org_type"] != typ:
            old = r["org_type"]
            r["org_type"] = typ
            note(r, f"type {old} -> {typ} (CLASSIFICATION_POLICY.md); decision kept")
            L("retype", cid, r["decision"], f"{old} -> {typ}", {"org_type": typ})
            bump("retype")
        d = r["aligned_db_id"].strip()
        if d in byid and byid[d]["org_type"] != typ:
            L("retype_db", d, "", f"{byid[d]['org_type']} -> {typ} (via {cid})", {"org_type": typ})
            byid[d]["org_type"] = typ
            bump("retype_db")
    for d, fix in DB_FIX.items():
        r = byid[d]
        for k, v in fix.items():
            if r[k] != v:
                L("edit_db", d, "", f"{k}: {r[k]!r} -> {v!r} (its only cluster C01268_Q02 is the Havana journal)", {k: v})
                r[k] = v
                bump("db_fix")

    print(f"\n{counts}; {len(log)} log rows; {errors} errors")
    if errors:
        print("errors: nothing written")
        return 2
    if not apply:
        print("DRY RUN -- re-run with --apply")
        return 0
    write(ORG / "core_db.tsv", kf, core, knl)
    write(ORG / "org_alignment_review.tsv", af, al, anl)
    write(ORG / "organizations_clustered.tsv", cf, cl, cnl)
    if log:
        p = ORG / "activity_log.tsv"
        b = p.read_bytes()
        buf = io.StringIO()
        csv.writer(buf, delimiter="\t", lineterminator="\r\n").writerows(log)
        p.write_bytes(b + (b"" if b.endswith(b"\n") else b"\r\n") + buf.getvalue().encode("utf-8"))
    print("WROTE core_db, org_alignment_review, organizations_clustered, activity_log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
