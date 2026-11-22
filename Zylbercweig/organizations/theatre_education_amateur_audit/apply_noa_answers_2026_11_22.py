#!/usr/bin/env python3
"""Apply Noa's answers to Part C (C1–C20) of the Doc
"Theatre education and Amateur — applied changes and cases for Noa (2026-11-15)"
https://docs.google.com/document/d/1uP0p68o3nOwzMynFTxMITeWP81N0abOjsAsITcAfJsg/edit
(answers read 2026-11-22; Doc modifiedTime 2026-11-22T12:11Z).

Answers: Confirm on C1–C7, C10–C12, C14–C20; Other on C8 (ballet school -> Theatre
education), C9 (Bamberg DP-camp studio -> Amateur), C13 (HaChaver stays Amateur); C18
Confirm with a question (why not GENERIC for 324/3197 — answered in the report).

Every proposal was re-read against its entry text (people/entry_texts.tsv) before it
was applied. Where the text settles a point the proposal had wrong, the text wins and
the change is marked TEXT (reviewer stamp Sinai, not Noa):
  C7   Ruskin's "studio of the Yiddish Theatre Society" is in New York (1923–24, after
       his 1921 emigration), not the Vilna society -> its own record, no parent.
  C15  795: Kiev and Moscow are ONE studio that moved (Lauter: "טעאַטער-סטודיע קולטור-ליגע
       (קיעוו-מאָסקווע)" 1919–24; Zaslavski: "די קיעווער סטודיע … ווען זי פֿאָרט קיין מאָסקווע");
       only Warsaw (Shteyn -> existing 5064) and Kovno (Fisher 1922–24, back in Kovno
       from 1920) are other bodies.
  C15  474: Kaminski's "Fareynikte dramatishe trupe" (Zhelazo) is db 239, where three
       other "Kaminski's united troupe" mentions are already aligned.
  C16  1951 "דראַמאַטישער שול" is the Yung-Argentine drama school = existing db 2211;
       1188's two heading-only mentions are the Lexicon's own entries for Weichert's
       "Yidishe teater-studio", Warsaw (later name of the Yidishe dramatishe studio) = 2037.

Writes (dry-run by default): core_db.tsv, org_alignment_review.tsv,
organizations_clustered.tsv, org_addresses_review.tsv (existing cards: type, links;
merged rows' cards dropped), activity_log.tsv. Ids minted from origin's high-water mark.
Idempotent. Run ../sync_address_cards.py afterwards for the new rows' cards.

Usage: python3 apply_noa_answers_2026_11_22.py [--apply]
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
HERE = Path(__file__).resolve().parent
ORG = HERE.parent
sys.path.insert(0, str(ORG.parent / "zalmen"))
import mention_removals  # noqa: E402

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TAG = "Noa's answers 2026-11-22 (TE/Amateur Part C)"
SRC = "theatre_education_amateur_audit/apply_noa_answers_2026_11_22.py"
HEAD, SENT = "_ - heading", "_ - organizations - _ - relations - _ - original_sentence"

TE, AM, TH, TC = "Theatre education", "Amateur", "Theatre", "Traveling Company"
MUS, CUL, TRS, JN = "Musical organization", "Cultural/workers' club", "Theatre-related Society/ Union", "Journals/ Newspapers"
NAO = "Not an organization"

# key: (name, name_yiddish, address, parent_db_id, org_type, case)
MINTS = {
    "herman_tour": ("Herman's studio — touring company (1925)", "„הערמאַנס סטודיע“ — טורנע-גרופּע (1925)", "", "", TC, "C1"),
    "kiev_studio_theatre": ("Kiev studio-theatre (1925)", "קיעווער סטודיאָ-טעאַטער", "קיעוו", "", TH, "C2"),
    "phil_culture_org": ("Yiddish Culture Organization, Philadelphia", "יידישע קולטור-אָרגאַניזאַציע (פֿילאַדעלפֿיע)", "פֿילאַדעלפֿיע", "", CUL, "C3"),
    "ny_tg_studio": ("Studio of the Yiddish Theatre Society, New York (1923–24)", "סטודיע פֿון דער יידישער טעאַטער-געזעלשאַפֿט (ניו יאָרק)", "ניו יאָרק", "", TE, "C7"),
    "vilna_tg_troupe": ("Troupe of the Yiddish Theatre Society, Vilna (1919–20)", "טרופּע פֿון דער יידישער טעאַטער-געזעלשאַפֿט (ווילנע, 1919–20)", "ווילנע", "1080", TC, "C7"),
    "harfe_drama": ("Harfe drama group, Lodz (1908–13)", "דראַמאַטישע גרופּע ביי „האַרפע“ (לאָדזש)", "לאָדזש", "3033", AM, "C14"),
    "kl_kovno": ("Yiddish theatre school at the Kultur-Lige, Kovno (1922–24)", "יידישע טעאַטער-שול ביי דער „קולטור-ליגע“ (קאָוונע)", "קאָוונע", "", TE, "C15"),
    "hdc_ny": ("Hebrew Dramatic Club, New York (1900s)", "„היברו דראַמאַטיק קלאָב“ (ניו יאָרק)", "ניו יאָרק", "", AM, "C15"),
    "dk_london": ("Dramatishe Kunst workers' group, London (Goldenburg)", "„דראַמאַטישע קונסט“ (לאָנדאָן)", "לאָנדאָן", "", AM, "C15"),
    "litdram_plan": ("Literary-dramatic theatre planned by Herman and Hirshbein (1908, not realised)", "ליטעראַריש-דראַמאַטיש טעאַטער (הערמאַן און הירשביין, 1908)", "", "", TH, "C15"),
    "warsaw_calendar": ("Varshever yudisher kalendar (1889)", "וואַרשעווער יודישער קאָלענדאַר", "וואַרשע", "", JN, "C15"),
    "minsk_art_lovers": ("Society of art lovers, Minsk (1901)", "מינסקער געזעלשאַפֿט פֿון קונסט-ליבהאָבער", "מינסק", "", TRS, "C17"),
    "lb_brisk": ("Liebhaber group, Brisk (1917)", "גרופּע „ליבהאָבער“ (בריסק, 1917)", "בריסק", "", AM, "C16"),
    "lb_warsaw": ("Liebhaber group, Warsaw (1906)", "גרופּע „ליבהאָבער“ (וואַרשע, 1906)", "וואַרשע", "", AM, "C16"),
    "lb_sinkov": ("Abe Sinkov's liebhaber group", "אייב סינקאָפֿס גרופּע „ליבהאָבער“", "", "", AM, "C16"),
    "lb_ny_lyceum": ("Liebhaber group, New York (Schwartz, Schwartzberg, Fishl Zinger)", "גרופּע „ליבהאָבער“ (ניו יאָרק; שוואַרץ, שוואַרצבערג, פֿישל זינגער)", "ניו יאָרק", "", AM, "C16"),
    "lb_gutentog": ("Karl Gutentog's liebhaber group", "קאַרל גוטענטאָגס גרופּע „ליבהאָבער“", "", "", AM, "C16"),
    "lb_sitawa": ("Liebhaber Yiddish theatre, Sitawa", "„ליבהאָבער“ יידיש טעאַטער (סיטאַוואַ)", "סיטאַוואַ", "", AM, "C16"),
    "dk_colony": ("Dramatic circle in the summer colony near Warsaw (Sh. Blyakher)", "דראַמאַטישער קרייז אין דער זומער-קאָלאָניע לעבן וואַרשע", "וואַרשע", "", AM, "C16"),
    "dk_kolno": ("Dramatic circle, Kolno (1915–18)", "דראַמאַטישער קרייז (קאָלנע)", "קאָלנע", "", AM, "C16"),
    "kiev_studio_1918": ("Yiddish theatre studio, Kiev (1918)", "יידישע טעאַטער-סטודיע (קיעוו, 1918)", "קיעוו", "", TE, "C16"),
    "novak_studio": ("Novak's Yiddish theatre studio, Kiev", "נאָוואַקס יידישע טעאַטער-סטודיע (קיעוו)", "קיעוו", "", TE, "C16"),
    "pinski_dg": ("Dramatic Society, New York (D. Pinski chairman)", "דראַמאַטישע געזעלשאַפֿט (ניו יאָרק; פּינסקי פֿאָרזיצער)", "ניו יאָרק", "", TRS, "C16"),
    "detroit_dg": ("Dramatic society, Detroit (1928)", "דראַמאַטישע געזעלשאַפֿט (דעטראָיט, 1928)", "דעטראָיט", "", AM, "C16"),
}

# Mention moves, in order. (case, cluster, [(heading, sentence-substring)] or None = whole cluster
# or remainder, target: db id | "MINT:key" | "GENERIC", evidence, by)
N, S = "Noa", "Sinai"
MOVES = [
    ("C1", "ORG-C03521", [("הירשפעלד", "שפּילן איבער דער פּראָווינץ")], "MINT:herman_tour", "Hirshfeld 1925 'פֿאָרט ה. מיט דער דאָזיקער סטודיע שפּילן איבער דער פּראָווינץ'", N),
    ("C1", "ORG-C03521", None, "800", "pupils / studio mentions of Herman's studio (Wolfshtat, Mansdorf) -> the studio db 800", N),
    ("C1", "ORG-C00532", None, "MINT:herman_tour", "Poznanski 'פֿאָרט ער אַרויס … אין אַ גרופּע א״נ הערמאַנ׳ס סטודיע'", N),
    ("C1", "ORG-C00563", None, "MINT:herman_tour", "Patashinski 'זומער 1925 דורכגעמאַכט מיט דער טעאַטער-סטודיע דעם טורנעע איבער פּוילן' — the same 1925 tour", S),
    ("C1", "ORG-C03172", None, "800", "Rozenfarb, Weinshtok 'שטודירט אין דוד הערמאַנס טעאַטער-סטודיע' -> the studio db 800", N),
    ("C2", "ORG-C05284", None, "MINT:kiev_studio_theatre", "Epelboym: 'אין אַ קיעווער סטודיאָ-טעאַטער' (1925) — a theatre, not the training studio", N),
    ("C7", "ORG-C01326_Q01", [("רוסקין", "סטודיע")], "MINT:ny_tg_studio", "Ruskin emigrated 1921 to America, 'טרעט אַריין אין 1923-24 אין דער סטודיע פֿון דער יידישער טעאַטער-געזעלשאַפט', then 1924 Bronx 'Unzer Teater' — New York, not Vilna (TEXT)", S),
    ("C7", "ORG-C01326_Q01", [("עלמית", "טרופּע"), ("ווילנער טרופּּע", "טרופּע"), ("אַזראַ", "געזעלשאָפט")], "MINT:vilna_tg_troupe", "end 1919 the Vilna society 'האָט אָרגאַניזירט אַ טרופּע' (Elmit, Birnboym, Azro, Zhelazo + local liebhaber), Azro its leader; toured the provinces", N),
    ("C14", "ORG-C01292_Q01", [("שושנה", "האַרפע"), ("קאַהאַן, לאַזאַר", "האַרפע"), ("יעדוואַב", "אויפפֿירונגען")], "MINT:harfe_drama", "the 1908–10 performances of Harfe and its 'literary-dramatic' work -> Amateur unit; choir, conductor and literary evenings stay on the host", N),
    ("C15", "ORG-C00562", None, "2033", "Patashinski 1927, Perenson 1922 'ערשטער יידישער דראַמאַטישער שול (א״א דר. ווייכערט [און דוד הערמאַן])' -> Weichert's school db 2033", N),
    ("C15", "ORG-C02169", None, "5064", "Shteyn 'לערער אין דער דראַמאַטישער שול ביי דער קולטור-ליגע אין וואַרשע, א'א פֿון דר. ווייכערט' -> the Warsaw Kultur-Lige school db 5064", N),
    ("C15", "ORG-C00162", None, "MINT:kl_kovno", "Fisher back in Kovno from 1920, 'קומט אָן אין 1922 אין דער יידישער טעאַטער-שול ביי דער קולטור-ליגע … ביז 1924', only then to the USSR — Kovno (TEXT)", S),
    ("C15", "ORG-C00808", [("שוואַרץ, סעם", "היברו"), ("לאַסקי", "היברו"), ("גראָסמאַן, יעקב", "היברו")], "MINT:hdc_ny", "Schwartz (came 1903), Lasky (came 1907, co-founder), Grossman (NY, c. 1906, co-founder): the New York amateur club, not Smith's London club", N),
    ("C15", "ORG-C00726", [("גאָלדענבורג", "אַרבעטער גרופּע")], "MINT:dk_london", "Goldenburg in London 'געגרינדעט אַן אַרבעטער גרופּע אין דראַמאַטישע קונסט'", N),
    ("C15", "ORG-C01452", [("הערמאַן, דוד", "ליטעראָריש")], "MINT:litdram_plan", "Herman 1908 'פּלאַנט צוזאַמען מיט פּרץ הירשביין צו גרינדן אַ ליטעראַריש-דראַמאַטיש טעאַטער', 'וואָס ווערט אָבער ניט פֿאַרווירקלעכט'", N),
    ("C15", "ORG-C06875", None, "239", "Zhelazo 'אין דער פֿאַרייניקטע דראַמאַטישע טרופּע א״ד פֿון קאַמינסקי' = Kaminski's united troupe, db 239 (C04437, C05016, C07137 already there) (TEXT)", S),
    ("C15", "ORG-C00021", None, "MINT:warsaw_calendar", "Bernas 'אַרויסגעגעבן אין 1889 אין וואַרשע דעם וואַרשעווער יודישען קאָלענדאַר' — a publication, extraction error on the troupe", N),
    ("C16", "ORG-C02956", [("לידין", "ליבהאָבער")], "MINT:lb_brisk", "Lidin, Brisk 1917: 'דעביוטירט ער ביי אַ גרופּע ליבהאָבער' (Rakov's Talmid-khokhem)", N),
    ("C16", "ORG-C02956", [("לאַטאָוויטש", "ליבהאָבער")], "MINT:lb_warsaw", "Latovitsh, Warsaw 1906: with a liebhaber group in the Bagatela theatre", N),
    ("C16", "ORG-C02956", [("סינקאָף", "ליבהאָבער")], "MINT:lb_sinkov", "Sinkov 'שטעלט ער צונויף (אין עלטער פֿון 16 יאָר) אַ גרופּע ליבהאָבער', later its manager — founder named", N),
    ("C16", "ORG-C02956", [("אויערבאַך, סעם", "פֿאָרשטעלונג")], "MINT:lb_ny_lyceum", "Auerbach: the liebhaber group of Schwartz, Schwartzberg, Fishl Zinger, Roza Finkental (Manhattan Lyceum, Brownsville)", N),
    ("C16", "ORG-C02956", [("גוטענטאָג", "ליבהאָבער")], "MINT:lb_gutentog", "Gutentog 'האָט ער אָרגאַניזירט אַ גרופּע ליבהאָבער' (Shulamis, Dos poylishe yingl) — founder named", N),
    ("C16", "ORG-C02956", None, "GENERIC", "Migdalski: 'באַקענט זיך מיט אַ גרופּע ליבהאָבער' and wandered the provinces — no place, no founder", N),
    ("C16", "ORG-C01488", [("מאַריענהאָף", "ליבהאָבער")], "MINT:lb_sitawa", "Marienhof 'אָנגעהויבן שפּילן מיט ליבהאָבער יידיש טעאַטער אין סיטאַוואַ'", N),
    ("C16", "ORG-C01488", None, "GENERIC", "Feld 'פֿון דער פֿריעסטער יוגנט געשפּילט מיט ליבהאָבער יידיש טעאַטער' — no place in the sentence", N),
    ("C16", "ORG-C07057", [("רובין, גרשון", "ענטינס")], "363", "Rubin 'אַ תלמיד פֿון יענעם ענטינס דראַמאַטיק קלוב' = Entin's Progressive Dramatic Club db 363", N),
    ("C16", "ORG-C07057", None, "GENERIC", "Reingold 1895 'אַלס פּרעזידענט פֿון אַ דראַמאַטיק-קלוב' — no place, no name", N),
    ("C16", "ORG-C02833_Q01", [("בליאַכער, שבתי", "קאָלאָניע")], "MINT:dk_colony", "Sh. Blyakher founded a drama circle in the summer colony near Warsaw (14 performances)", N),
    ("C16", "ORG-C02833_Q01", None, "MINT:dk_kolno", "Brisman, Kolno 1915 'געגרינדעט אַ דראַמאַטישן קרייז' (and 1918 'מיטן אָרטיקן דראַמאַטישן קרייז אין קאָלנע')", N),
    ("C16", "ORG-C01439_Q01", None, "2211", "from the 'Yung-Argentine' entry: its drama school, teachers Dr. Zhitnitski … = db 2211 (TEXT)", S),
    ("C16", "ORG-C02283_Q01", [("ליובאָמירסקי", "טעאַטער-סטודיע")], "MINT:kiev_studio_1918", "Lubomirski, Kiev 1918 'אין אַ יידישער טעאַטער-סטודיע וואָס ווערט דאָרט אָרגאַניזירט'", N),
    ("C16", "ORG-C02283_Q01", [("קאַסיאַקאָוו", "נאָוואַק")], "MINT:novak_studio", "Kasyakov (b. 1908), at 16 'די יידישע טעאַטערסטודיע אין קיעוו א'א פֿון נאָוואַק'", N),
    ("C16", "ORG-C02283_Q01", None, "2037", "the Lexicon's own entries '\"יידיש טעאַטער-סטודיע\" אין וואַרשע' / '\"יידישע טעאַטער-סטודיע\"' (later name of the Yidishe dramatishe studio; Weichert's programme 1932) = db 2037 (TEXT)", S),
    ("C16", "ORG-C00721", [("פּינסקי, דוד", "דראַמאַטישער")], "MINT:pinski_dg", "Pinski 'פֿאָזיצער פֿון דער געוו. דראַמאַטישער געזעלשאַפֿט און אונזער טעאַטער' (New York)", N),
    ("C16", "ORG-C00721", None, "MINT:detroit_dg", "Detroit 1928: 'די דעמאָלטיקע דראַמאַטישע געזעלשאַפֿט האָט אויפֿגעפֿירט' Bergelson's play — stages plays (Rule 4)", N),
    ("C17", "ORG-C02041", None, "MINT:minsk_art_lovers", "Chertov 1901 decorator 'אין דער מינסקער געזעלשאַפֿט פֿון קונסט-ליבהאָבער' — patrons, not the drama society", N),
]

# db retypes: db -> (type, case, why, by)
RETYPE = {
    "1203": (TE, "C2", "'אַ גרופּע פֿון דער קיעווער סטודיע' joined the Belarusian state studio — a training studio", N),
    "1752": (AM, "C4", "Idramst, Buenos Aires 1936: staged a play; no pupils shown", N),
    "796": (TH, "C5", "Di Maske, Lemberg: an art council and a set designer", N),
    "3826": (TH, "C6", "a university stage that appears only as the place of a recital is a venue", N),
    "1319": (TE, "C8", "ballet school: trains stage performers (Noa: Other -> Theatre education)", N),
    "4298": (AM, "C9", "DP-camp studios were typically performing groups (Noa: Other -> Amateur)", N),
    "2179": (TE, "C10", "drama courses (Rule 1); db already TE since 11-21, review row still Education", N),
    "319": (MUS, "C11", "Lira, Częstochowa: music society; the professionals performed as guests", N),
    "3193": (TC, "C12", "the Bergen-Belsen Kazet-Teater: toured Belgium and France 1945", N),
    "3033": (MUS, "C14", "Harfe, Lodz: song society (choir, conductor Zelman) with a drama group", N),
    "4631": (MUS, "C14", "Harfe, Paris: the C14 reading of Harfe as a song society (Doc: 'then Musical organization')", N),
    "404": (TH, "C15", "Smith's Princess Street club, London 1880s: professional (Adler, Goldfaden); the NY amateur club is split off", N),
    "228": (NAO, "C18", "'Dramatic Arts': a discipline, no mentions", N),
    "324": (AM, "C18", "'Literary Drama Club': name only, no mentions (seed row, kept like 230–232)", N),
    "3197": (AM, "C18", "Abrams, in America after 1907, president of the literary-dramatic club", N),
}
# db field edits: db -> {field: value}, case, why
EDIT = [
    ("1203", {"address": "קיעוו"}, "C2", "Kiev"),
    ("2202", {"parent_db_id": "MINT:phil_culture_org"}, "C3", "studio at the Yiddish Culture Organization, Philadelphia"),
    ("319", {"name": "Lira (Częstochowa)", "name_yiddish": "ליראַ (טשענסטאָכאָוו)", "address": "טשענסטאָכאָוו"}, "C11", "was 'Lira (as drama society)'; during the German occupation of Częstochowa (Biro)"),
    ("3193", {"name": "Kazet-Teater (Bergen-Belsen)", "name_yiddish": "קאַצעט-טעאַטער (בערגען-בעלזען)", "parent_db_id": ""}, "C12",
     "Noa: this is the Kazet-Teater; parent 3051 (survivors' world federation, set 11-21 as 'drama section') cleared — the host was the camp's central committee, no record"),
    ("3033", {"address": "לאָדזש"}, "C14", "Lodz Harfe"),
    ("795", {"address": "קיעוו / מאָסקווע"}, "C15", "one studio, Kiev 1919 -> Moscow, to 1924 (Lauter)"),
    ("404", {"address": "לאָנדאָן"}, "C15", "Smith's club, 8 Princess Street"),
    ("606", {"address": "פֿילאַדעלפֿיע"}, "C15", "Kanievski's society, Philadelphia"),
    ("474", {"address": "פּערט"}, "C15", "Berenson: the group in Perth"),
    ("446", {"address": "מינסק"}, "C17", "Minsk Yiddish drama society"),
]
MERGE = [("1976", "800", "C1"), ("1977", "800", "C1"), ("3678", "800", "C1"), ("1951", "2211", "C16")]
DEPRECATE = [("2276", "C16"), ("4316", "C16"), ("4604", "C16"), ("4607", "C16"), ("4609", "C16"), ("1188", "C16"), ("3047", "C16")]


def sk(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))


def read(p):
    raw = p.read_bytes().decode("utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw, newline=""), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(p, f, rows, nl):
    b = io.StringIO()
    w = csv.DictWriter(b, fieldnames=f, delimiter="\t", lineterminator=nl)
    w.writeheader()
    w.writerows(rows)
    p.write_bytes(b.getvalue().encode("utf-8"))


def pipes(v):
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def high_water():
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ORG, capture_output=True, timeout=120)
    out = subprocess.run(["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"], cwd=ORG,
                         capture_output=True, text=True, check=True).stdout
    return max(int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t") if r["db_id"].isdigit())


def main():
    apply = "--apply" in sys.argv
    kf, core, knl = read(ORG / "core_db.tsv")
    af, al, anl = read(ORG / "org_alignment_review.tsv")
    cf, cl, cnl = read(ORG / "organizations_clustered.tsv")
    df, cards, dnl = read(ORG / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    card = {r["db_id"]: r for r in cards}
    removed = mention_removals.load_removed_keys()
    used = {x["cluster_id"].strip() for x in cl} | set(arow)
    log, report, errors = [], [], []

    def L(action, target, decision, text, extra, who, case):
        log.append([NOW, who, "org_review", action, target, decision, f"{TAG} {case}: {text}",
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    def note(row, text, who):
        p = (row.get("reviewer_notes") or "").strip()
        row["reviewer_notes"] = f"{TAG}: {text}" + (" | " + p if p else "")
        row["reviewer"], row["reviewed_at"] = who, NOW

    def setlinks(d, ids):
        by[d]["linked_cluster_ids"] = " | ".join(ids)
        if d in card:
            card[d]["linked_cluster_ids"] = " | ".join(ids)

    def unlink_everywhere(cid, keep=""):
        for r in core:
            if r["db_id"] != keep and cid in pipes(r["linked_cluster_ids"]):
                setlinks(r["db_id"], [x for x in pipes(r["linked_cluster_ids"]) if x != cid])

    def link(cid, d):
        if cid not in pipes(by[d]["linked_cluster_ids"]):
            setlinks(d, pipes(by[d]["linked_cluster_ids"]) + [cid])

    nxt = max(high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    have = {(sk(r["name_yiddish"]), r["parent_db_id"]): r["db_id"] for r in core if not r["deprecated"].strip()}

    def resolve(t):
        nonlocal nxt
        if not t.startswith("MINT:"):
            return t
        name, yid, addr, parent, typ, case = MINTS[t[5:]]
        if (sk(yid), parent) in have:
            return have[(sk(yid), parent)]
        d = str(nxt)
        nxt += 1
        row = {f: "" for f in kf}
        row.update(db_id=d, name=name, name_yiddish=yid, address=addr, parent_db_id=parent, org_type=typ)
        core.append(row)
        by[d] = row
        have[(sk(yid), parent)] = d
        L("mint", d, "NEW", name, {"name_yiddish": yid, "parent_db_id": parent, "org_type": typ}, N, case)
        report.append(f"{case} mint {d} [{typ}] {name}")
        return d

    # ---- moves
    for case, cid, specs, target, why, who in MOVES:
        prow = arow.get(cid)
        if prow is None:
            errors.append(f"{case}: no review row {cid}")
            continue
        allr = [x for x in cl if x["cluster_id"].strip() == cid]
        live = [x for x in allr if mention_removals.mention_key(x) not in removed]
        if specs is None:
            mine = None
        else:
            mine = [x for x in live if any(sk(h) in sk(x[HEAD]) and sk(s) in sk(x[SENT]) for h, s in specs)]
            if not mine:
                report.append(f"{case} skip {cid} {specs[0][0]}: no mention left (already moved?)")
                continue
            got = {sk(h) for h, _ in specs if any(sk(h) in sk(x[HEAD]) for x in mine)}
            if got != {sk(h) for h, _ in specs}:
                errors.append(f"{case} {cid}: a spec matched nothing: {specs}")
                continue
            if len(mine) == len(allr):
                mine = None  # the spec takes the whole cluster
        if target == "GENERIC":
            d = ""
        else:
            d = resolve(target)
            if d not in by or by[d]["deprecated"].strip():
                errors.append(f"{case} target {target} -> {d} missing or deprecated")
                continue
        if mine is None:  # whole cluster
            dec = "GENERIC" if target == "GENERIC" else "ALIGN"
            if (prow["decision"], prow["aligned_db_id"].strip()) == (dec, d) and (not d or cid in pipes(by[d]["linked_cluster_ids"])):
                continue
            was = f"{prow['decision'] or 'undecided'} {prow['aligned_db_id']}".strip()
            prow["decision"], prow["aligned_db_id"] = dec, d
            if d:
                prow["org_type"] = by[d]["org_type"]
            note(prow, f"{was} -> {dec}{' db ' + d if d else ''}. {why}", who)
            unlink_everywhere(cid, keep=d)
            if d:
                link(cid, d)
            L("alignment", cid, dec, why, {"aligned_db_id": d, "was": was, "n_mentions": len(allr)}, who, case)
            report.append(f"{case} {cid} ({len(allr)}) -> {dec} {d}")
            continue
        n = 1
        while f"{cid}_P{n:02d}" in used:
            n += 1
        child = f"{cid}_P{n:02d}"
        used.add(child)
        ids = {id(x) for x in mine}
        left = len(allr) - len(mine)
        for x in allr:
            if id(x) in ids:
                x["cluster_id"] = child
            x["cluster_size"] = str(len(mine) if id(x) in ids else left)
        a = dict(prow)
        a.update(cluster_id=child, cluster_size=str(len(mine)), decision="ALIGN" if d else "GENERIC", aligned_db_id=d,
                 candidate_db_ids="", candidate_scores="", candidate_methods="", reviewer_notes="",
                 org_type=by[d]["org_type"] if d else prow["org_type"])
        note(a, f"split off {cid} ({len(mine)} mention(s)) -> {a['decision']}{' db ' + d if d else ''}. {why}", who)
        al.insert(al.index(prow) + 1, a)
        arow[child] = a
        prow["cluster_size"] = str(left)
        note(prow, f"{len(mine)} mention(s) split off to {child} ({a['decision']}{' db ' + d if d else ''})", who)
        if d:
            link(child, d)
        L("split", child, a["decision"], f"{len(mine)} mention(s) of {cid} -> {child}; {why}",
          {"from_cluster": cid, "aligned_db_id": d, "mentions": [x[HEAD] for x in mine]}, who, case)
        report.append(f"{case} split {cid} -> {child} ({len(mine)}) -> {d or 'GENERIC'}")

    # ---- edits
    for d, fields, case, why in EDIT:
        r = by[d]
        for k, v in fields.items():
            v = resolve(v) if v.startswith("MINT:") else v
            if r[k] != v:
                L("set_parent" if k == "parent_db_id" else "edit_db", d, "", f"{k}: {r[k]!r} -> {v!r} ({why})", {k: v}, N, case)
                report.append(f"{case} edit {d} {k}: {r[k]!r} -> {v!r}")
                r[k] = v
                if d in card and k in card[d]:
                    card[d][k] = v

    # ---- retypes (db row, its card, every linked review row)
    for d, (typ, case, why, who) in RETYPE.items():
        r = by[d]
        if r["org_type"] != typ:
            L("retype_db", d, typ, f"{r['org_type']} -> {typ}: {why}", {"was": r["org_type"]}, who, case)
            report.append(f"{case} retype {d} {r['org_type']} -> {typ}")
            r["org_type"] = typ
        if d in card and card[d]["org_type"] != typ:
            card[d]["org_type"] = typ
        for c in pipes(r["linked_cluster_ids"]):
            if c in arow and arow[c]["org_type"] != typ:
                old = arow[c]["org_type"]
                arow[c]["org_type"] = typ
                note(arow[c], f"type {old} -> {typ} (db {d}, {case})", who)

    # ---- merges
    for dup, into, case in MERGE:
        r = by[dup]
        if r["merged_into"] == into:
            continue
        for c in pipes(r["linked_cluster_ids"]):
            if c in arow and arow[c]["aligned_db_id"].strip() != into:
                arow[c]["aligned_db_id"], arow[c]["decision"] = into, "ALIGN"
                note(arow[c], f"db {dup} merged into {into}", N)
            link(c, into)
        for k in core:
            if k["parent_db_id"] == dup:
                k["parent_db_id"] = into
                report.append(f"{case} reparent {k['db_id']} {dup} -> {into}")
        r["deprecated"], r["merged_into"], r["linked_cluster_ids"] = "true", into, ""
        cards[:] = [k for k in cards if k["db_id"] != dup]
        card.pop(dup, None)
        log.append([NOW, N, "org_addresses", "db_merge", into, "MERGE_INTO", f"{TAG} {case}: merged db {dup} into {into}",
                    json.dumps({"merged_db_id": dup, "source": SRC}, ensure_ascii=False)])
        report.append(f"{case} merge {dup} -> {into}")

    # ---- retire bare labels with no live link left
    for d, case in DEPRECATE:
        r = by[d]
        if r["deprecated"].strip():
            continue
        live = [c for c in pipes(r["linked_cluster_ids"]) if arow.get(c, {}).get("decision") not in ("GENERIC", None)]
        if live:
            errors.append(f"{case} cannot retire {d}: still linked {live}")
            continue
        if any(k["parent_db_id"] == d for k in core):
            errors.append(f"{case} cannot retire {d}: has children")
            continue
        r["deprecated"], r["linked_cluster_ids"] = "true", ""
        if d in card:
            card[d]["linked_cluster_ids"] = ""
        L("deprecate_db", d, "", "bare label retired: every mention now on a one-place record or GENERIC", {}, N, case)
        report.append(f"{case} deprecate {d}")

    # sanity: every moved/linked cluster is linked from exactly one live db
    for case, cid, *_ in MOVES:
        for c in [k for k in arow if k == cid or k.startswith(cid + "_P")]:
            owners = [r["db_id"] for r in core if c in pipes(r["linked_cluster_ids"]) and not r["deprecated"].strip()]
            dec = arow[c]["decision"]
            if dec == "ALIGN" and owners != [arow[c]["aligned_db_id"]]:
                errors.append(f"link check {c}: aligned {arow[c]['aligned_db_id']} owners {owners}")
            if dec == "GENERIC" and owners:
                errors.append(f"link check {c}: GENERIC but linked from {owners}")

    print("\n".join(report))
    (HERE / "applied_noa_answers_2026_11_22.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(f"\n{len(log)} log rows; {len(errors)} errors")
    for e in errors:
        print("ERROR", e)
    if errors:
        return 2
    if not apply:
        print("DRY RUN — re-run with --apply")
        return 0
    write(ORG / "core_db.tsv", kf, core, knl)
    write(ORG / "org_alignment_review.tsv", af, al, anl)
    write(ORG / "organizations_clustered.tsv", cf, cl, cnl)
    write(ORG / "org_addresses_review.tsv", df, cards, dnl)
    p = ORG / "activity_log.tsv"
    b = io.StringIO()
    csv.writer(b, delimiter="\t", lineterminator="\r\n").writerows(log)
    raw = p.read_bytes()
    p.write_bytes(raw + (b"" if raw.endswith(b"\n") else b"\r\n") + b.getvalue().encode("utf-8"))
    print("WROTE core_db, org_alignment_review, organizations_clustered, org_addresses_review, activity_log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
