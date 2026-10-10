# Are the recurring A2 titles one paper or many?

The question behind A2: 26 of the 50 newspapers to mint carry a title that
Galron himself records in several cities. If any of those were one paper with
regional editions, a parent record could connect them — core_db already
supports this (`parent_db_id`; db692 *Forverts (New York City)* → db249).

Twelve were researched against Wikidata, the YIVO Encyclopedia, Wikipedia,
LoC and USHMM on 2026-10-10.

**The answer is mostly no. Ten of twelve are unrelated papers sharing a common
title, and no title justifies a parent spanning all of Galron's cities.**

| Title | Cities | Verdict | Confidence |
|---|---|---|---|
| דער פֿרייע וואָרט / Dos Fraye Vort | Feldafing, Baranowicze, Vilna, Lviv | unrelated | high |
| דאָס נייע לעבן / Dos Naye Lebn | Bialystok, Lodz–Warsaw, London, NY, Kalisz | unrelated | high |
| דער יוד / Der Yud | Altona, Berlin, Leipzig, Paris, Warsaw-Krakow, Vienna | **two traditions** | high |
| די אידישע וועלט / Di Yidishe Velt | St Petersburg, Kovno, Copenhagen, Cleveland | **partly one paper** | medium |
| די וואָך / Di Vokh | Berdychiv, Stanislav, Kovel, Riga | unrelated | medium |
| דער אמת / Der Emes | Boston, Warsaw, Moscow, Kovno | unrelated | high |
| Jidisze Cajtung | Landsberg, Lviv | **Landsberg = one paper** | high |
| היינט / Haynt | Warsaw, Riga | **parent + named-after** | medium |
| טאג / Tog | Vilna, New York | unrelated | high |
| דער פֿריינד / Der Fraynd | New York, St Petersburg | unrelated | high |
| המעורר / HaMeorer | Jaffa, London | unrelated | medium |
| הדור / HaDor | New York, Krakow | unrelated | high |

## What to actually do

**No family parents.** Generic titles — *The Truth*, *The Week*, *The New Life*,
*The Free Word* — were used independently by papers with no connection. Minting
one org per place, as instructed, is correct for all of them.

**Three specific merges, not parents.** Where a link is real it covers a subset
of Galron's cities, so it is a same-entity merge of named entries:

- *Di Yidishe Velt*: St Petersburg (1912) and the Vilna run (1913–Apr 1915,
  ed. Shmuel Niger, pub. Kletskin) are ONE journal that moved. Kovno,
  Copenhagen and Cleveland are unrelated — and further unrelated *Yidishe Velt*
  titles exist (Warsaw 1928, Vilkaviškis 1934–35, Montreal, Vancouver).
- *Jidisze Cajtung*: Landsberg's DP-camp *Landsberger Lager-Cajtung* (f. Oct
  1945, Latin type for want of Hebrew fonts) renamed itself *Jidisze Cajtung*
  in 1946 — one paper under two titles. Nothing to do with Lviv. **This
  vindicates the Tier B row pairing ORG-C07330 with Landsberger Lager-Cajtung.**
- *Haynt*: Warsaw 1906–1939 (as *Yidishes Tageblat* to 1908, f. Yatskan) is the
  parent; Riga's *Haynt* / *Haynt Batog* and *Parizer Haynt* are named after it
  with no shared ownership. A named-after relation is defensible; a parent
  organization is not.

**דער יוד needs splitting, not merging.** It conflates two publishing
traditions under one romanisation. The Yiddish *Der Yud* (Q16131075) is the
Aḥiasaf Zionist weekly 1899–1902, edited in Warsaw and printed in Kraków and
Vienna to evade tsarist censorship — Warsaw/Kraków/Vienna are ONE paper. The
German *Der Jude* is separate and itself at least four unconnected magazines
(Selig 1768–72, Riesser 1832, Buber/Schocken 1916–28, a New York German weekly
from 1895); Altona, Leipzig and Paris belong there. Galron's seven entries are
therefore 2+ distinct orgs, and the Vienna entry could belong to either run.
Needs a human check against the actual holdings.

## Limits of this round

NLI JPress blocks automated fetching (403/410), and its per-paper description
pages carry explicit "continues / continued by" notes — the best next source
for the medium-confidence rows: *Di Yidishe Velt* (Kovno, Copenhagen,
Cleveland), *Di Vokh* (Berdychiv, Stanislav, Riga), *HaMeorer* (Jaffa).

Wikidata coverage of this corpus is thin: *Dos Naye Lebn*, *Di Vokh*,
*Di Yidishe Velt*, *HaMeorer* and the Jaffa/Kraków *HaDor* have no usable items,
so these cannot be resolved by QID.

Sources: [YIVO Der Fraynd](https://devencyclopedia.yivo.org/article/1673) ·
[YIVO Der Emes](https://encyclopedia.yivo.org/article.aspx/Emes_Der) ·
[YIVO Di Yudishe Velt](https://encyclopedia.yivo.org/article.aspx/Yudishe_Velt_Di) ·
[YIVO Newspapers and Periodicals](https://devencyclopedia.yivo.org/article/20) ·
[Haynt](https://en.wikipedia.org/wiki/Haynt) ·
[Der Tog](https://en.wikipedia.org/wiki/Der_Tog) ·
[Der Jude](https://en.wikipedia.org/wiki/Der_Jude) ·
[Q16131075 Der Yud](https://www.wikidata.org/wiki/Q16131075) ·
[Q1967268 Der Emes](https://www.wikidata.org/wiki/Q1967268) ·
[Q12405872 Der Fraind](https://www.wikidata.org/wiki/Q12405872) ·
[LoC Jidisze Cajtung](https://www.loc.gov/item/sn2001058034/) ·
[USHMM Landsberg DP Camp](https://encyclopedia.ushmm.org/content/en/article/landsberg-displaced-persons-camp)

## Not resolved by this round

Sinai's three `RESEARCH_FURTHER` rows are a different question and remain open —
they concern whether two specific entries are the same paper, not whether a
title spans cities:

- ORG-C03832 and db1849 → `דיא אידישע ביהנע` (New York 1909–1910, JPress)
- ORG-C06119 → `ארבעטער צייטונג` (Chernivtsi 1921–1925, DiFMOE)

*Arbeter-Tsaytung* is itself a title Galron records in both Chernivtsi and
Kovno, so it belongs to the same class as the twelve above and was not among
them. Worth adding to a second research round, together with the JPress
description pages that this round could not open.
