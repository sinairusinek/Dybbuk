#!/usr/bin/env python3
"""Build the HTML for Noa's review Doc (2026-11-15) from applied_2026_11_15.json.
Upload: rclone copyto <html> jeckedrive:"<title>" --drive-root-folder-id <folder> --drive-import-formats html
"""
import html
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
A = json.loads((HERE / "applied_2026_11_15.json").read_text())
E = html.escape


def yi(s):  # Yiddish run, right-to-left
    return f'<span dir="rtl" lang="yi">{E(s)}</span>'


def table(head, rows):
    h = "".join(f"<th>{E(x)}</th>" for x in head)
    b = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table border="1" cellpadding="4" style="border-collapse:collapse">{"<tr>" + h + "</tr>"}{b}</table>'


P = []
P.append("<h1>Theatre education and Amateur: what was applied, and cases for your review</h1>")
P.append("<p>15 November 2026. Follows the Doc <i>Theatre education and Amateur — categories and audit (2026-11-08)</i>, "
         "where Sinai answered the open questions today. <b>Please answer in this Doc</b>: write under each case "
         "(or comment on it). Nothing in Part C has been written to the database yet.</p>")

P.append("<h2>Part A. What was decided and applied</h2>")
P.append("<p>Sinai accepted the two-question criteria and Rules 1–6 (drama schools move to Theatre education; a studio "
         "is Theatre education unless the text shows no pupils; a dramatic society that stages its own plays is Amateur; "
         "a circle at a host is its own child record; ליבהאָבער is not always amateur; bare labels are GENERIC). "
         "He accepted Mendl Elkin's children's studio as Theatre education, left the ballet school open, and asked "
         "for the child records to be made and documented. The definitions in CLASSIFICATION_POLICY.md were updated.</p>")
P.append("<p>Every proposal was re-read against its sentences before it was applied. Human type decisions were "
         "checked against the rules, not accepted automatically. Three bear on this area: "
         "Maaty's Amateur for the Kremenchuk workers' studio (3735) agrees with Rule 2: the entry calls it "
         f"{yi('אַרבעטער-ליבהאָבער-טעאַטער')} and names no pupils. Two contradict a rule, and are in Part C (C10, C20).</p>")
n_ret = sum(1 for x in A if x["kind"] == "retype")
P.append(table(["Applied 15 Nov", "Records"], [
    ["Education → Theatre education (drama schools, studios, courses)", "96"],
    ["Theatre → Theatre education (studios typed as theatres)", "6"],
    ["Theatre / Society / other → Amateur (dramatic societies and circles), listed in A1", "33"],
    ["Other retypes (professional troupes out of Amateur, student orchestra, art studio, camp, Russian theatre, Pressers' union, Elkin)", str(n_ret - 135)],
    ["New child records for drama circles at a host (A2)", str(sum(1 for x in A if x['kind'] == 'child'))],
    ["Host mentions moved into an existing unit record (A2)", str(sum(1 for x in A if x['kind'] == 'to_existing'))],
    ["Existing unit records linked to their host (parent_db_id) (A2)", str(sum(1 for x in A if x['kind'] == 'parent'))],
]))
P.append("<p>Not touched: 20 culture/workers' clubs and culture societies, pending a question to Sinai (his answer "
         "that clubs \"can become a subtype of amateur\" conflicts with Rule 3, which says a host is never typed Amateur). "
         "Ledger of all 397 records: <code>theatre_education_amateur_audit/decisions_2026_11_15.tsv</code>.</p>")

# A1 documented moves
P.append("<h3>A1. Moves into Amateur (documented, as Sinai asked)</h3>")
P.append("<p>Each line is one record, with the evidence the move rests on. If any reading is wrong, say so and it will be reverted.</p>")
rows = []
for x in A:
    if x["kind"] == "retype" and x["now"] == "Amateur":
        rows.append([x["db_id"], yi(x["name"]), E(x["was"]), E(x["evidence"])])
P.append(table(["db", "Name", "Was", "Evidence"], rows))

# A2 children
P.append("<h3>A2. Circles at a host: child records (Rule 3), one report each</h3>")
P.append("<p>The drama-unit mention left the host record and now belongs to the circle, which carries "
         "<code>parent_db_id</code> = the host. The host keeps its own type. Where a host cluster also held other "
         "mentions, the circle's mention was split off as a new cluster (<code>…_P02</code>). "
         "Cities not in the sentence were read from the entry and are marked so.</p>")
rows = []
for x in A:
    if x["kind"] == "child":
        quotes = "<br>".join(yi(s[:160]) + f" <small>({E(h[:30])})</small>" for (_c, _n, _k, ms) in x["moved"] for (h, s) in ms)
        rows.append([x["db_id"], yi(x["name"]), f'{x["host"]} {yi(x["host_name"])}<br><small>{E(x["host_type"])}</small>', quotes, E(x["evidence"][:220])])
P.append(table(["New db", "Circle", "Host (type kept)", "Mention moved", "Evidence"], rows))
rows = []
for x in A:
    if x["kind"] == "to_existing":
        n = sum(m[2] for m in x["moved"])
        rows.append([x["db_id"], yi(x["name"]), f'{x["host"]} {yi(x["host_name"])}', str(n), E(x["evidence"])])
P.append("<p>Moved into a unit record that already existed:</p>")
P.append(table(["Unit db", "Unit", "From host", "Mentions", "Why"], rows))
rows = [[x["db_id"], yi(x["name"]), f'{x["host"]} {yi(x["host_name"])}', E(x["evidence"])] for x in A if x["kind"] == "parent"]
P.append("<p>Parent links set on existing unit records:</p>")
P.append(table(["db", "Unit", "Parent", "Note"], rows))
P.append("<p>Two host records were left as they are, because the text shows no circle: the host's own members "
         f"put on one play (Tiferet Bachurim, Boysk 1903, db 1947: {yi('האָבן די מיטגלידער … אויפֿגעפירט די פּיעסע')}; "
         "kibbutz Bamaale, Płońsk, db 3273). This is the same as a school whose pupils stage a play (Rule 1).</p>")

# Part B
P.append("<h2>Part B. Surveys Sinai asked for</h2>")
P.append("<h3>B1. Studios: how many were easy, and where the border is</h3>")
P.append("<p>74 records have “studio” in their name or in their sentences. "
         "<b>9</b> are not drama studios at all: film studios (Libin 3478, Warner Bros. 3689), a studio's journal (4281), "
         "art and painting studios (Murashko 1176, Exter 3822, Kultur-Lige art studio 1235, “18 painters” 3847) and a dance studio (1319). "
         "<b>2</b> are performing-only workers' studios, Amateur by the Rule 2 exception (Kremenchuk 3735, Tiflis 4360). "
         "<b>About 51 were easy</b>: the sentences speak only of pupils, teachers, entering, studying, finishing "
         "(Weichert's studio, the Belarusian state studio, the GOSET school, the Kultur-Lige studios, the Artef studio, the Kiev studios). "
         "Several of these also staged plays (Granovsky's studio 1919, Rafalsky's studio, Royte Blitn in Bila Tserkva 1928, "
         "the Sholem-Aleichem studio, Onheyb in Kiev), but under Rule 2 a student production does not change the type, so they too were easy. "
         "Where the theatre the studio became already has its own record (the Sholem-Aleichem studio 1507 and the Sholem-Aleichem "
         "Theatre-Studio 4495), the Gimpel two-institution pattern is already in place.</p>")
P.append("<p><b>12 were border cases.</b> They cluster around three lines:</p><ol>"
         "<li><b>Training versus performing as a company.</b> A studio that went on tour or became a theatre "
         "(David Herman's studio, the Kiev studio-theatre, the Vilna Theatre Society's studio and troupe). Rule 2 says: two institutions. "
         "The open question is where the line falls in the mentions.</li>"
         "<li><b>“Studio” as the name of a performing group with no visible pupils</b> (Philadelphia youth studio, Idramst, "
         "the Spartak workers' theatre studio, the Bamberg DP-camp studio). Rule 2's exception applies only if the text shows no pupils, "
         "and these texts are silent either way.</li>"
         "<li><b>Directing, pedagogy and the university.</b> Meyerhold's State Higher Directing Workshop (1320, applied as Theatre education) "
         "was both a school for directors and Meyerhold's laboratory theatre; Piscator's Dramatic Workshop at the New School (2138) was a school "
         "that produced plays; the Studiobühne of Münster University (3826) is a university stage. "
         "A working line is proposed in C6.</li></ol>"
         "<p>Two of the twelve were settled here: Lodz Hazomir's studio mentions joined the existing studio record (797), "
         "and Spartak waits for the club question. The ballet school stays open, as Sinai asked. The rest are in Part C.</p>")

P.append("<h3>B2. Kinds of amateur group: circles, liebhaber, dramatic societies, studios, clubs</h3>")
P.append("<p>After today's changes Amateur holds about 143 records. Sorted by the noun in the name, with what their sentences say:</p>")
P.append(table(["Form", "Records", "What the contexts show"], [
    [f"Circle / section / group ({yi('קרייז, סעקציע, גרופּע, קאָלעקטיוו, ווינקל')})", "≈58",
     "The commonest form, mostly Eastern Europe 1900s–1940s. More than a quarter of the sentences name a host (ביים / ביי דער): "
     "Hazomir, the Bund, Poale Zion, Hashomer Hatzair, the Kultur-Lige, a library, a workers' home, a cooperative colony, "
     "an academic culture society. A director is often named (א\"פ פֿון), and this is very often where a career begins (debut, first roles). "
     "Soviet circles bear dedicatory names (named after Kalinin). A <i>section</i> is always part of a host body."],
    [f"Liebhaber ({yi('ליבהאָבער')})", "≈17",
     "The older word: most mentions date 1880s–1920s, in the Russian Empire, Poland and early Argentina. It usually names people rather than an "
     f"institution, written in quotation marks: {yi('מיט אַ גרופּע „ליבהאָבער“')}, {yi('די „ליבהאָבער“ פֿון פּרץ-פֿאַראיין')}. "
     "Several groups wandered the provinces (Migdalski; Piekarnik 1907), i.e. close to semi-professional touring. "
     "Russian and Hebrew liebhaber groups occur too (Odessa 1887–89; Habima in Lwów)."],
    [f"Dramatic society / farein ({yi('דראַמאַטישע געזעלשאַפֿט, פֿאַראיין')}; English “society”)", "≈25",
     "A formal association: founders and a chairman (פֿאָרזיצער), a fixed city in the name (Chicago, Kovno, Libau, Brisk, Milwaukee), "
     "a regular director for years (Kadison in Kovno, ten years), mergers (Chicago 1917), ambitious repertoire (the Dybbuk, Gordin). "
     "Literary-dramatic societies also held readings (Bergelson in Chicago 1929). This is where semi-professional cases sit: "
     "Lira in Częstochowa performed with professional actors."],
    ["Studio", "2", "Only two workers' studios with no pupils (Kremenchuk 1917–20, Tiflis). Every other studio is Theatre education (B1)."],
    [f"Club ({yi('דראַמאַטיק קלאָב')})", "≈29",
     "Almost wholly American (New York, Chicago, Philadelphia, Boston, Los Angeles, Milwaukee), and named in English: "
     "Young Men's, Young Hebrews, Delancey Street. Many are named after a star or a playwright (David Kessler, Jacob Gordin, Morris Morrison, "
     "Nathan Goldberg, Hamlet) and so are partly fan clubs. The language is that of membership (מיטגליד, פּרעזידענט). "
     "Entin's Progressive Dramatic Club also gave theatre lectures and had pupils (תלמיד), so it reaches into training."],
    ["Other named groups", "≈12", "Proper-named stages and troupes (Perhift, Loża Lumina, Kometa, Di Bine, Yung-Argentine)."],
]))
P.append("<p><b>Is the difference only in the name?</b> Mostly yes. Circle, farein, society and club are largely the same thing "
         "named in different places and periods: “club” and “society” are the American English-loan forms of the East European "
         "“circle” and “farein”, and “liebhaber” is the pre-1914 word that the party and union era replaced with “circle”. "
         "Three real differences do run through the material, and none of them is the noun:</p><ol>"
         "<li><b>Attached or independent.</b> A section or circle at a host versus a free-standing society or club. "
         "This is already recorded by <code>parent_db_id</code> (Rule 3), so it needs no subtype.</li>"
         "<li><b>Status.</b> Purely amateur versus semi-professional: touring liebhaber groups, the “half-professional” troupes "
         "(Loża Lumina 2287, Dovke 2308, while the half-professional PIAT 1818 and Philadelphia Folksbine 2849 are typed Theatre), "
         "DP-camp sections. This is the form/status split the first Doc mentioned. If a subcategory is wanted, this is the one that carries information.</li>"
         "<li><b>Club as group versus club as place.</b> The American “dramatic club” is the performing group itself. "
         "The East European and Soviet workers' or culture club (the Minsk club Royter Boyer, the Radom Groser-club, Spartak in Buenos Aires) "
         "is a host and a venue that has a drama circle. Sinai's “clubs (not as venues)” draws exactly this line.</li></ol>"
         "<p><b>Proposal:</b> no subtypes by name. If anything, an optional status tag (amateur / semi-professional) on Amateur records, "
         "kept like the troupe tags rather than as a type.</p>")

P.append("<h3>B3. Caveats found, and what was done about each</h3><ul>"
         f"<li>{yi('ליבהאָבער-ראָלן')} = <b>lover roles</b> (the amoureux), 13 sentences in the corpus. Only one was mislinked to an Amateur "
         "record: the Lemberg Chaverim troupe (2278), now Company on Tour. The others sit correctly on professional troupes (Kompaneets, Sobsey, Axelrad…).</li>"
         f"<li>{yi('קונסט-ליבהאָבער')} / {yi('מוזיק-ליבהאָבער')} = art or music <b>lovers</b>, i.e. patrons. One such mention, the Minsk "
         f"„{yi('געזעלשאַפֿט פֿון קונסט-ליבהאָבער')}“ (1901), sits on the Amateur record 446 (the Minsk Yiddish drama society): see C17.</li>"
         "<li>“Dramatic” in a name does not mean amateur. Horowitz's Israelitische Dramatische Gesellschaft (Bucharest 1877, db 44), "
         "the London Hebrew Dramatic Club of the 1880s (404), and the Fareynikte Dramatishe Kompanye (474) were professional.</li>"
         "<li>“Studio” has non-theatre senses: film, art, dance, a journal (B1).</li>"
         "<li>“Club” has two senses, group and host/venue (B2.3).</li>"
         "<li>Amateur does not record language. Russian amateur circles (3510, 2791, 2818), Russian liebhaber groups (1563, 2294) and Hebrew ones "
         "(2289, 2306, 4675) are in Amateur; moving 3510 out of Non-Yiddish Theatre loses that information.</li>"
         "<li>Semi-professional groups are spread across Amateur, Theatre and Traveling Company (B2.2).</li>"
         "<li>Probable duplicates turned up again (Appendix). None were merged.</li></ul>")

# Part C cases
C = []


def case(title, ctx, q, prop):
    C.append((title, ctx, q, prop))


case("C1. David Herman's studio (1976; also 800, 1977, 3678)",
     f"Pupils: {yi('גייט שפּעטער איבער אין דוד הערמאַנס סטודיע')} (Wolfshtat), {yi('שטודירט אין דוד הערמאַנס טעאַטער-סטודיע')} (Weinshtok). "
     f"Company: Mansdorf {yi('געשפּילט אין דוד הערמאָןס סטודיע (דעביוטירט פּראָפעסיאנעל פּסח 1925 …)')}; Hirshfeld {yi('אין 1925 פֿאָרט ה. מיט דער דאָזיקער סטודיע שפּילן איבער דער פּראָווינץ')}; "
     f"Poznanski {yi('פֿאָרט ער אַרויס … אין אַ גרופּע א״נ „הערמאַנ׳ס סטודיע“')}. 800 is the studio at Warsaw Hazomir (now linked to it as parent); 1977 and 3678 have the same two pupil sentences.",
     "Is “Herman's studio” on tour in 1925 a second institution (Gimpel pattern), or the studio's own student tour?",
     "Two institutions: the studio (Theatre education; merge 1976/1977/3678 into 800) and a touring company “Herman's studio”, 1925 (Traveling Company) taking the two touring mentions.")
case("C2. Kiev studio (1203), typed Theatre",
     f"{yi('ס׳איז אויך צוגעקומען אַ גרופּע פֿון דער קיעווער סטודיע')} (a group from the Kiev studio joined the Belarusian state studio) and {yi('אויפֿגעפֿירט געוואָרן … אין אַ קיעווער סטודיאָ-טעאַטער')} (1925).",
     "Same institution? Studio or theatre?",
     "Probably two different things under a generic name. Split: the first sentence goes to Theatre education; the 1925 “studio-theatre” stays Theatre.")
case("C3. Youth theatre studio at the Yiddish Culture Organization, Philadelphia (2202)",
     f"Chayele Ash staged with it Motl Peysi (1966–67) and Green Fields (1967–68): {yi('א. האָט מיט דער „יוגנטלעכער טעאַטער-סטודיע“ … אויפֿגעפֿירט')}. No pupils or teachers are named.",
     "Training studio for youth (Theatre education) or a youth performing group (Amateur, Rule 2 exception)?",
     "Theatre education by default (a youth studio usually teaches), with a child link to the Culture Organization. Not applied yet.")
case("C4. „Idramst“, Buenos Aires (1752), typed Traveling Company",
     f"{yi('אין אַפּריל 1936 איז די פּיעסע … אויפֿגעפֿירט געוואָרן אין בוענאָס איירעס … דורך „אידדראַמסט“')}. The acronym may stand for a Yiddish dramatic studio; the sentence does not say.",
     "Do you know the group? Studio with pupils, or an amateur stage?",
     "Amateur, unless the expansion of the acronym shows a school.")
case("C5. Di Maske, Lemberg (796), typed Theatre education",
     f"{yi('איז געוואָרן אַ מיטגליד אין קינסטלערישן ראַט פון „מאַסקע“ און איר טעאַטער-דעקאָראַטאָר')} (Fritz Kleinman). The press audit of 1 Nov called it a Lemberg theatre studio and retyped its cluster Theatre; the record was never changed.",
     "Theatre (an art council and a set designer) or a studio?",
     "Theatre.")
case("C6. Studiobühne of Münster University (3826), typed Education",
     f"Horowitz-Silberman gave a recital {yi('אין דער סטודיע-בינע פֿון מינסטער אוניווערזיטעט')} — the sentence names Berlin and Münster University together.",
     "The directing/pedagogy line at a university.",
     "Working line: a student drama group is Amateur (Brooklyn College group 3845, the Moscow Western University circle 4751, "
     "the Lublin students' circle 4684). A university drama department or course is Theatre education. A university stage that only appears "
     "as the place of a performance is a venue: Theatre. So 3826 → Theatre.")
case("C7. Yiddish Theatre Society, Vilna (1080)",
     f"Officers and members: Vigodski founder and chairman, Elkin, Ran… The society's studio: {yi('טרעט אַריין אין 1923-24 אין דער סטודיע פֿון דער יידישער טעאַטער-געזעלשאַפט')} (Ruskin). "
     f"A troupe it organised: {yi('סוף 1919 … וועלכע האָט אָרגאַניזירט אַ טרופּע')}; Azro {yi('דער אָנפירער … פון יידישן טעאַטער וועלכן ס׳עפנט די „יידישע טעאַטער-געזעלשאָפט“')}.",
     "Should the studio and the troupe become two children of the society? Is the 1919 troupe a record we already have?",
     "Society keeps Theatre-related Society/ Union; child 1 = the society's studio (Theatre education); child 2 = the 1919 troupe (Traveling Company), if it is not an existing troupe record.")
case("C8. Ballet school of Helena Laskowicz, Vilna (1319)",
     f"{yi('די קליינע נאַדינקע אין דער ווילנער טאַנץ-סטודיע … אַריינגעגעבן … אַ זייער טאַלענטירטע שילערין')}.",
     "Sinai left dance and ballet open. Your view: Theatre education (stage performers) or Education?",
     "No proposal; it stays Education until decided.")
case("C9. Bamberg DP-camp studio (4298), Theatre education",
     f"{yi('געקומען אין באַמבערגער לאַגער, און דאָרט געגרינדעט די „באַמבערגער יידישע דראַמאַטישע סטודיע“')}.",
     "DP-camp “studios” usually performed. Keep Theatre education?",
     "Keep, unless you know it as a performing group.")
case("C10. Julius Halle's drama courses, Vienna (2179): a human decision against a rule",
     f"{yi('באַזוכט ער די דראַמאַטישע קורסן פֿון אויבעררעזשיסער פֿון ווינער שטאָט-טעאַטער יוליוס האַלע')} (Sigal, twice). "
     "Maaty changed its type Education → Theatre on 13 Sept (Org Cards); the change never reached the database, which still says Education.",
     "Rule 1 says drama courses are Theatre education, not Theatre. Do you agree?",
     "Theatre education. Not applied, because it overrides Maaty's recorded choice.")
case("C11. Lira, Częstochowa (319, “as drama society”)",
     f"{yi('טרעט זי אויף אין „ליראַ“ מיט די אַקטיאָרן הערשקאָוויטש, לאָשקאָווסקאַ, צימבאַליסט, אייזענבערג און גליקמאַן אין אַ ריי פּיעסן')}. Its twin record 320 (“as drama school”) is now Theatre education.",
     "Amateur, or a music-drama society that brought in professional actors (host)?",
     "If Lira was the Częstochowa music society: Musical organization as host, and the performances were guest appearances by professionals, not an amateur group.")
case("C12. Drama section in Bergen-Belsen (3193 and 3194, the same sentence twice), typed Welfare",
     f"{yi('מ׳האָט געשפּילט דעם נאָרמאַלן בעסערן יידישן טעאַטער-רעפּערטואַר נישט נאָר אין בעלזען, נאָר אויך אין בעלגיע און פֿראַנקרייך')} (1945).",
     "Amateur, or the Kazet-Teater company (touring, partly professional)?",
     "Merge the two; type Traveling Company if this is the Kazet-Teater, otherwise Amateur. The host is the camp's central committee.")
case("C13. HaChaver, Lisbon (2313), typed Amateur",
     f"{yi('צוזאַמענגענומען עטלעכע מיטגלידער פֿון „החבר“ און מיט זיי אויפֿגעפירט')} (Zhitomirski).",
     "No circle is named; the members staged a play. What kind of body was HaChaver?",
     "Not Amateur. Retype to the host's own type once known (probably a Zionist or cultural society).")
case("C14. Harfe (3033), typed Theatre-related Society/ Union",
     f"Literary-dramatic society ({yi('ליטעראַריש-דראַמאַטישע געזעלשאַפטן ווי „האַרפע“')}); performances 1908–10 ({yi('אויפֿגעטראָטן אין „גרויסעס טעאַטער“ אין דער געזעלשאַפֿט „האַרפע“')}); "
     f"but also a song society: {yi('כאָר-מיטגליד אין געזאַנג-פֿאַריין „האַרפע“')}, Zelman conducted Lodz Harfe. A second Harfe (4631, Paris 1923) has a drama circle; it waits with the clubs.",
     "One Lodz society (Musical organization) with a drama section, or an Amateur society?",
     "Host = Musical organization; child Amateur unit for the 1908–10 performances.")
case("C15. Six splits proposed by the audit",
     "<ul>"
     "<li><b>1085</b> Vilna Yiddish Dramatic Studio also holds two Warsaw mentions of Weichert's “First Yiddish Dramatic School” (Patashinski 1927, Perenson 1922) → move those to 2033/2037. Type already Theatre education.</li>"
     "<li><b>795</b> Kultur-Lige theatre studio: Kiev (1918–19, Loyter, Irenyeva), Moscow (Feygin 1922, Sokol 1924), Warsaw (Shteyn, teacher) → three records.</li>"
     "<li><b>404</b> Hebrew Dramatic Club: London 1880s, Smith's Princess Street Club, a professional theatre (10 mentions) versus an American amateur club of the 1900s (Schwartz 1903, Lasky, Grossman 1906) → two records.</li>"
     "<li><b>2252</b> Dramatishe Kunst: the Lodz society (13 mentions; type now Amateur) versus Goldenburg's London workers' group → split off the London mention.</li>"
     "<li><b>606</b> Literary-dramatic society: Philadelphia (Kanievski, Shulweis) versus Herman and Hirshbein's planned literary-dramatic theatre → split.</li>"
     "<li><b>474</b> Fareynikte Dramatishe Kompanye: Perth (Berenson), Kaminski's troupe (Zhelazo), and the Warsaw Jewish Calendar of 1889 (an extraction error) → three.</li></ul>",
     "Approve each, or mark the ones you doubt.",
     "Approve all six.")
case("C16. Eleven GENERIC labels",
     "Bare labels holding unrelated groups: 2276 „גרופּע ליבהאָבער“ (Rakov 1917, Brisk 1917, provinces, 1895…), 4316 „ליבהאָבער“ יידיש טעאַטער, "
     "4604 דראַמאַטיק קלוב, 4607/4609 דראַמאַטישן קרייז (the same two sentences: a colony 1919, Kolno 1915), 1951 דראַמאַטישער שול, "
     "1188 יידישע טעאַטער-סטודיע (Kiev, Novak's studio…), 3047 דראַמאַטישער געזעלשאַפֿט (Detroit 1928, Pinski), and three English labels with no mentions (230, 231, 232). "
     "A difficulty: records like 724 and 872 (“a Yiddish dramatic circle” founded in Rezhitse 1917 / Riga 1917) are just as bare in name but hold one place each, and were kept as Amateur.",
     "Mark these GENERIC (record retired, mentions unlinked)? Or split them into one record per place and founder, like 724 and 872?",
     "Split where a sentence names a place or a founder (most of 2276, 4607, 1188, 3047); GENERIC only for sentences with neither. The three empty English labels are old seed rows: keep them (they may be cited by the earlier T-Migrants crosswalk).")
case("C17. The Minsk “society of art lovers” on the Minsk drama society (446)",
     f"Chertov {yi('געאַרבעט אַלס דעקאָראַטאָר-מאָלער אין דער מינסקער געזעלשאַפֿט פֿון קונסט-ליבהאָבער')} (1901), linked to the Amateur record whose other mention is Teitelbaum, director of the Minsk Yiddish drama society.",
     "Is the society of art lovers the same body as the drama society?",
     "No: a patrons' society (Theatre-related Society/ Union), its own record.")
case("C18. Three records with no mentions, judged by name only",
     "228 “Dramatic Arts” (Amateur; a discipline, not an org?), 324 “Literary Drama Club” (Kleinkunst), 3197 ליטעראָריש-דראָמאָטישן קלאָב (only: Abrams was its president).",
     "Change or leave?",
     "228 → Not an organization; 324 and 3197 → Amateur by Rule 4 only if you are comfortable deciding by the name.")
case("C19. “Studio of 18 painters”, New York (3847), typed OTHER",
     f"{yi('האָט ז. אָנגעפֿירט אין ניו-יאָרק מיט אַ סטודיע פֿון 18 מאָלער')} (Zolotarov).",
     "An art school (Education) or an artists' collective (OTHER)?",
     "Education only if it taught; the sentence suggests a collective, so OTHER may be right.")
case("C20. New Yiddish Americans (2277): Ruthie's Amateur against Rule 3",
     f"{yi('אָרגאַניזירט צוויי דראַמאַטישע קרייזן ביי די „נייע-יידיש אַמעריקאַנער“')} (Furman, Philadelphia); "
     f"Chayele Ash staged {yi('מיט דער אָרגאַניזאַציע')} Goldfaden's Kishefmakherin (1962). Ruthie changed it Traveling Company → Amateur on 7 July.",
     "Rule 3 makes this a host with two circles. It is also a culture organisation, so it waits for Sinai's club decision.",
     "For information. It will be settled with the clubs.")

P.append("<h2>Part C. Cases for your review</h2>")
P.append("<p>Each case gives the text, the question and a proposal. Please write <b>Confirm</b>, <b>Other: …</b> or <b>Don't know</b> under it.</p>")
for title, ctx, q, prop in C:
    P.append(f"<h3>{E(title)}</h3><p><b>Text:</b> {ctx}</p><p><b>Question:</b> {E(q)}</p><p><b>Proposal:</b> {E(prop)}</p><p><b>Your answer:</b> </p>")

P.append("<h2>Appendix. Probable duplicates (not merged)</h2><ul>"
         "<li>Belarusian Yiddish state studio, Moscow: 601, 1132, 1502, 1506, 4299 (and maybe Rafalsky's 1122)</li>"
         "<li>Granovsky's studio: 1105, 1285 · David Herman's studio: 800, 1976, 1977, 3678 · Weichert's school: 2033, 2037 (+ two mentions on 1085)</li>"
         "<li>Kovno Yiddish dramatic society: 674, 1832, 2760 · Chicago Yiddish (literary-)dramatic society: 512, 607</li>"
         "<li>Pirozhnikov's literary-dramatic circle at the Bund, Vilna 1908: 1501, 2283</li>"
         "<li>Club of the kustari, Zhitomir: 2710, 2730 · “dramatic circle”: 4607, 4609 · Bergen-Belsen drama section: 3193, 3194</li>"
         "<li>Pressers' Local of the International (2305) and Dress-Pressers Local 60 (3495)?</li></ul>")

out = HERE / "noa_review_2026_11_15.html"
out.write_text("<!doctype html><html><head><meta charset='utf-8'><title>Theatre education and Amateur — for Noa (2026-11-15)</title></head><body>"
               + "\n".join(P) + "</body></html>", encoding="utf-8")
print(out, out.stat().st_size)
