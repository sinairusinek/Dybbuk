# C1a decided by location

Sinai, 2026-10-10: "did you consider the locations of the zylbercweig
claimants? this should decide it."

It does. `address` is empty on nearly every core_db press row, but the
**clusters** carry a settlement, and where they do not, the mention sentence
usually names the city outright. Pulling both settles 7 of the 8 groups.

| Galron paper | Galron place | What the claimants' locations say | Verdict |
|---|---|---|---|
| דער אידישער זשורנאל | **Toronto** 1949–62 | No claimant is in Toronto. `ORG-C00692`/`db1768` *Yidisher Kurier* = **Chicago**; the rest have no city, and their mentions name none. All nine are different papers sharing the adjective *yidisher*. | REJECT all 9 |
| וואָכנבלאט | **Toronto** 1953–63 | `ORG-C04074`/`db1851` = **N.Y.**; `db1937` is *Nyu-Yorker Vokhnblat* by name; `db710` is *Rovner*. None Toronto. | REJECT all 5 |
| דער פֿריַינד \<ניו־יורק\> | **New York** 1922 | `ORG-C04537` *Familien Fraynd* = **Warsaw**; `ORG-C04538` *Der Hoyz-Fraynd* is Spektor's Warsaw journal (mentions date it 1884–94, decades before the 1922 NY paper). Claimants share only the word *fraynd*. | REJECT all 5 |
| די קינדער וועלט | **Warsaw** 1927–33 | `ORG-C01212` *Kanader Adler* = **Montreal**; `ORG-C07275` *Der Kenader Id* and `ORG-C07205` *Kenader Yugend* are Canadian by name and mention; `ORG-C02182` *Kinder Land* = **New York**. A Warsaw children's paper matches none of them. | REJECT all 5 |
| דער אַרבּייטער | **Copenhagen** 1915–16 | `ORG-C01318_Q01`/`db1871` *Arbeter Heym* = **Warsaw**. No claimant is Danish. | REJECT all 4 |
| די אבענד צייטונג | **New York** 1906 | `ORG-C00365`/`db1845` *Abend Blat* = **N.Y.** — right city, but *Abend Blat* and *Abend Tsaytung* are two different NY papers. `ORG-C03775` *Kdoyshim Band* is not a periodical at all. | REJECT all 4 |
| פילם נייעס | **Warsaw** 1936–38 | `ORG-C02657` *Farbert-Film* = **Warsaw** — right city, but a film company, not a paper. `db19` *Sifriyat Poalim* is an Israeli publisher. | REJECT all 3 |
| קולטור | **Vilna** 1937 | The one group location cannot settle: `db1140` *Farlag Kultur* and `db1230` *Kultur-Farlag* are publishers (so `NOT_A_NEWSPAPER` regardless), leaving `db1847` *Yidishe Kultur* with no city. **Still open.** | 2 NOT_A_NEWSPAPER, 1 open |

**Why this works as a blanket rule:** every one of these groups formed because
the Galron title is short and generic (*Vokhnblat*, *Zhurnal*, *Fraynd*,
*Kultur*). Once the city is known, the geography alone separates them — a
Toronto weekly is not a Chicago daily — without needing to judge the titles.

## Sinai's three C1b/C2 questions

**Is `ORG-C03144` אידישער זשורנאַליסט in Toronto?** No. Its single mention is
`געדרוקט אַרטיקלען... אין 'אידישער זשורנאַליסט'` — no city, no date. Nothing
places it in Toronto, and *Yidisher Zhurnalist* ("Jewish Journalist") is a
different title from *Der Yidisher Zhurnal* anyway. → **REJECT**

**Does Galron's פֿאָלקס־שטימע have a link, and is it from Lodz?** Yes, and yes
— it is Lodz–Warsaw:
- `galron-0826` פֿאָלקס־שטימע · **לודז׳–ווארשה (פולין)** · 1945–1991 ·
  Academica · <https://academica.edu.pl/reading/readStruct?uid=37406701>

So `ORG-C03416` *Lodzsher Folksshtime*, whose mention reads
`דערנאָך „לאָדזשער פֿאָלקסשטימע"`, is in the right city. But the dates are the
problem: Galron's run starts **1945**, and the Zylbercweig lexicon closes well
before that — this mention is a pre-war career note. Likely a **different,
earlier** Lodz Folks-shtime. Worth a look at the scans. The other
Galron *Folksshtime* (`galron-0450`) is Copenhagen 1915–16, so not it either.

**Does `ORG-C01415` פּראָגרעס talk about a Ladino newspaper?** No — and this is
the sharpest catch. Galron's match is `galron-0871` **איל פרוגריסו** (*El
Progreso*), New York 1915, language **לאדינו (Ladino)**. Zylbercweig's
פּראָגרעס is a **Warsaw publishing house**: three of its four mentions read
`פֿאַרלאָג „פּראָגרעס“ אין וואַרשע` and `פ"ג „פּראָגרעס“, וואַרשע, 1909`.
A Warsaw Yiddish *publisher* against a New York *Ladino newspaper* — wrong
city, wrong language, wrong kind of organization.
→ **REJECT**, and `NOT_A_NEWSPAPER` on the Zylbercweig side.

The matcher reached it because the skeleton of פּראָגרעס and פרוגריסו both
reduce to `frgrs`, and the Ladino title's language was never consulted. Worth
noting as a gap: **language is a column in the union list and the matcher
ignores it.**
