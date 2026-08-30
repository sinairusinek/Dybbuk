"""Apply Noa and Judith's answers to the speech-prefix half of the speaker-label
handoff.

Source: the returned copy of `docs/handoff_2026-08-16_speaker_labels.md`
(Google Doc 1v-t0NPfNtNFwxZ...), fetched 2026-08-30. 115 of 121 labels ticked.
The castList half (the eight `role`-span questions carried by the 08-16
addendum) was applied separately by `apply_castlist_answers_2026_08_20`; this
script does not touch those spans.

WHY THIS IS A cast_dict EDIT, NOT A SPAN EDIT
---------------------------------------------
The questionnaire asked one question per LABEL, not per span, and the pipeline
already resolves a label to an xmlid through the cast_dict: `prefix_variants`
maps a surface form onto an existing role, and a new `roles` entry mints one.
So every answer lands as a cast_dict change, and the ordinary resolver pass
(`auto_resolve_flags`) then tags the spans. Three answer-shapes:

  variant    — the label is another spelling of a role the castList has.
               Appended to that role's `prefix_variants`.
  mint       — a character who speaks but was never listed. New `roles` entry.
  collective — a group, no individual cast entry. New entry, `collective: true`.
  non-speaker— the span is mis-tagged. Recorded in NOT_SPEAKER for the flag
               sweep to drop; no cast_dict change.

COMMENT OVER TICK
-----------------
Confirmed with Sinai 2026-08-30. Where the ticked box and the written comment
disagree, the comment is the answer. This matters in a dozen places: the form
offered no "merge into a role the castList lacks" box, so an answer meaning
"this is Pavel, same as the other three Pavels" could only be entered by
ticking *mint a new role* and saying so in the comment. Taking the ticks
literally would mint four separate Pavels, three separate Prosecutors, and so
on. Each such case is commented `# tick: mint / comment: <x>` below.

THE DUET PRONOUNS
-----------------
Not handled here — they are per-scene and live in `speaker_overrides.json`.
Emigration's is written (pp.113-117, ער=iekil / זיא=snie; see that file for the
evidence). Ben HaDor's and Tissa-Essler's are in OVERRIDES below.

HELD BACK — six labels, listed in HELD. Nothing is guessed.

  python3.11 -m annotation.apply_speaker_answers_2026_08_30 --dry-run
  python3.11 -m annotation.apply_speaker_answers_2026_08_30
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
NOTE = "Noa/Judith 2026-08-30 (speech-prefix handoff)"

MESH   = "Lateiner_Meshumed"
EMIG   = "MS_Emigration"
TISSA  = "MS_TissaEssler"
YOYSEF = "MS_YoysefInEgipten"
KHURBN = "MS_KhurbnYerusholaim"
BENHA  = "MS_BenHaDor"
TNOIM  = "MS_DiTsveyTnoim"
BASKOY = "MS_BasKoyen"
YAAKOV = "MS_YaakovEsav"

# ── variants: label is another spelling of a role the castList already has ──
# {play: {xmlid: [surface, ...]}}
VARIANTS = {
    MESH: {
        # 65 spans. The text settles it: on p.26 this speaker unmasks —
        # "(דעסמארקירט זיך) היער בין איך דיין יוסיף דיין זאהן" — and Yankev
        # answers "מיין זאהן! מיין יאסיף!". Osip is the Russian name Yosef
        # takes as the meshumed. Verified in page_annotated, not just the doc.
        "iusf": ["אסיפ", "אסייפ", "יאסיפ", "יוסיף!", "יוסעל"],
        "ivn": ["איווין"],
        "nikli_nikitin": ["ניקיטיוו"],
        # tick: mint fv/fvel/fvil/fvl / comment: all four are Pavel Kuter.
        # One role, four printed spellings.
        "fv": [],           # placeholder, real entry is in MINT below
        # tick: mint flism, fulits / comment: "merge with flismn".
        # flismn was minted from the castList addendum on 08-20.
        "flismn": ["פאליסמ", "פֿאליסמ", "פוליצ"],
    },
    EMIG: {
        "b_mshkvitsh": ["משקאוו"],
        "shfinner": ["שפיינע"],
        # "Typo/misreading for Shifra. Printed as 'שרה'" — p.93.
        "shfrh": ["שרה"],
        # tick: mint kenel / comment: "Typo/variant for Waiter. Merge with kelner."
        "kelner": ["קענעל"],
        # tick: mint 1_diener / comment: "Can be merged with diener or kept
        # separate." Merged: it is the same functional role, and the numeral
        # is a stage convention for which servant enters first.
        "diener": ["1 דיענער"],
    },
    TISSA: {
        "dnil_brtshi": ["בארי", "בארשי"],
        "etvsh": ["עטוואס"],
        # tick: mint frkurr/frkurt/frigur / comment: all "merge with frkurf".
        "frkurf": ["פראקוראר", "פראקוראט", "פריגור"],
        # tick: mint ferz/frez/frezi / comment: all "abbreviation for frezedent".
        "frezedent": ["פערז", "פרעז'", "פרעזי"],
        "fetsheli": ["פעטשעלר", "פעצעלי", "פעצעלע"],
        "grsmnn": ["גראסס"],
        "mrits": ["מאריטץ"],
        "ssr_slumsi": ["סאלא"],
        "shndmsh": ["שאנדאש"],
    },
    YOYSEF: {
        "ikhtsel": ["יאחסעל", "יאחצען", "יאבסעל", "יאחצעג"],
        "bnimn": ["ביימן"],
    },
    KHURBN: {
        "gdilihu_bn_khikum": ["גדליה"],
        "ishmel": ["ישמא"],
    },
    BENHA: {
        "sufur": ["סו פירא", "סוּ פֿירא"],
        "khnu": ["תנו"],
        "shlmun": ['״שלמון״'],
    },
    TNOIM: {
        "br_nzir": ["בר-נזיר", "בר - נזיר"],
        "bn_fdus": ['"בן פדות', "=פדות", "פדות]"],
        "izrelis": ["יזאעלית", "יעראעלית"],
    },
    BASKOY: {
        # "קעניגין"/"קעניגען" = the Queen = Alexandra. "עניגען" is the same
        # word with the initial kuf dropped by OCR.
        "lkhsndrh_hmlkhh": ["קעניגין", "קעניגען", "עניגען"],
        "izbl": ["יוסף איזבל?"],
        'iunsn': ['מ״ר יונתן'],
        "fgil": ["פדיאל"],
        "shmuldil": ["שאלמיאל"],
    },
    YAAKOV: {
        "eshu": ["עש"],
    },
}

# ── mint: speaks but was never in the castList ──────────────────────────────
MINT = {
    MESH: {
        "fv": {"form": "פֿאוו", "bare": "פאוו",
               # tick on פאוו was BOTH `ivn` and `mint fv`; the comment says
               # "Pavel / Pavel Kuter", and p.39 has יוד asking for "פֿאוויל
               # קוטער". So: one Pavel, not Ivan. The four spellings below are
               # separate questionnaire entries whose comments all say merge.
               "prefix_variants": ["פאוו", "פֿאוו", "פאוועל", "פֿאוועל",
                                   "פאוויל", "פֿאוויל", "פאוול", "פֿאוול"],
               "note": "Pavel Kuter; unlisted. 4 printed spellings merged"},
        "iud": {"form": "יוד", "bare": "יוד",
                "note": "unlisted; p.39 street scene"},
        "sld": {"form": "סאלד", "bare": "סאלד",
                "note": "abbrev. of סאלדאט (soldier); unlisted"},
        "ktsf": {"form": "קאצאפ", "bare": "קאצאפ", "note": "unlisted"},
        "shfin": {"form": "שפיאן", "bare": "שפיאן",
                  "note": "the Spy; unlisted functional role"},
        # tick: mint geystl / comment: "Diminutive of geyst. Merge with geyst".
        "geyst": {"form": "גייסט", "bare": "גייסט",
                  "prefix_variants": ["גייסטל"],
                  "note": "the Ghost; unlisted. גייסטל merged in"},
        "kind": {"form": "קינד", "bare": "קינד", "note": "unlisted ('Child')"},
        # tick was BOTH `ivn` and `mint ivniv`, but the comment is explicit:
        # "full name in text is שטעפֿאן איוואניוו, a government servant
        # testifying in court, DISTINCT from 'ivn'". So mint.
        "ivniv": {"form": "איוואניוו", "bare": "איוואניוו",
                  "note": "Stepan Ivanov; distinct from ivn per Noa"},
    },
    EMIG: {
        "kelner": {"form": "קעלנער", "bare": "קעלנער",
                   "note": "the Waiter; unlisted functional role, 30 spans"},
        "diener": {"form": "דיענער", "bare": "דיענער",
                   "note": "the Servant; unlisted functional role"},
        "b_dul_gelburd": {"form": "אב דול געלבורדא", "bare": "אב דול געלבורדא",
                          "note": "unlisted"},
    },
    TISSA: {
        "ihnn": {"form": "יאהאננא", "bare": "יאהאננא",
                 "note": "Johanna, Eszter Solymosi's mother; unlisted"},
        "insh": {"form": "יאנאש", "bare": "יאנאש", "note": "Janos; unlisted"},
        "mzel": {"form": "אמזעל", "bare": "אמזעל",
                 "note": "Amsel, court witness; unlisted"},
        "verhvi": {"form": "ווערהאוואי", "bare": "ווערהאוואי",
                   "note": "Verhovay; unlisted"},
        "iulin": {"form": "יוליאנא", "bare": "יוליאנא",
                  "note": "Juliana; unlisted"},
        "diener": {"form": "דיענער", "bare": "דיענער",
                   "note": "court attendant; unlisted functional role"},
    },
    YOYSEF: {
        "mnshh": {"form": "מנשה", "bare": "מנשה",
                  "note": "Menashe, Joseph's son; unlisted (castList has `mrd`, "
                          "which is מרד — a different entry)"},
        "frim": {"form": "אפרים", "bare": "אפרים",
                 "note": "Ephraim, Joseph's son; unlisted"},
        "rkhl": {"form": "רחל", "bare": "רחל",
                 "note": "Rachel, Joseph's mother; unlisted"},
        "khlimish": {"form": "חאלימיש", "bare": "חאלימיש", "note": "unlisted"},
        "diner": {"form": "דינער", "bare": "דינער",
                  "note": "the Servant; unlisted functional role"},
    },
    KHURBN: {
        "shenk": {"form": "שענק", "bare": "שענק",
                  "note": "the Cupbearer; unlisted functional role"},
    },
    BENHA: {
        "lte": {"form": "אלטע", "bare": "אלטע",
                "note": "the Old Woman, comic trio pp.32-33; unlisted"},
        "iunge": {"form": "יונגע", "bare": "יונגע",
                  "note": "the Young Woman, comic trio pp.32-33; unlisted"},
        "frzun": {"form": "פֿראזון", "bare": "פראזון", "note": "unlisted, p.19"},
    },
}

# ── collective: a group, no individual cast entry ───────────────────────
# Nothing to do here. `alle`, `beyde`, `chor`, `meydkhen_chor` are STANDING
# collective xmlids resolved by skeleton in schema.COLLECTIVE_XMLID, not by
# cast_dict entries. The nine labels the RAs ticked as collective and that the
# map did not yet carry — אלע 4 / אלע4 / אללע 4 / אלע 3 / אלע צוזאמען / כהר /
# חאהר / Beide / מעדכען חאהר — were added to that map in the same commit,
# along with two matcher fixes they exposed (a leading `!` blocked `!אללע`,
# and Latin-script `Beide` needed a case-insensitive lookup).
COLLECTIVE: dict = {}

# New collective entries (those with no standing COLLECTIVE_XMLID id).
COLLECTIVE_MINT = {
    MESH: {
        "flitsey": {"form": "פאליציי", "bare": "פאליציי",
                    "note": "the police, as a group"},
        "froyen": {"form": "פֿרויען", "bare": "פרויען", "note": "the women"},
    },
    YOYSEF: {
        "medkhen_khhr": {"form": "מעדכען חאהר", "bare": "מעדכען חאהר",
                         "note": "the Maidens' Chorus"},
        # Ticked "mint a new role", but the label is a plural group and the
        # comment calls it one ("Unlisted group/role ('Angels')"), so it is
        # minted as a collective rather than as a character.
        "englen": {"form": "ענגלען", "bare": "ענגלען",
                   "note": "the Angels"},
    },
    KHURBN: {
        "helden": {"form": "העלדען", "bare": "העלדען", "note": "the Heroes"},
        "khhnim": {"form": "כהנים", "bare": "כהנים", "note": "the Priests"},
    },
    TNOIM: {
        "ndere": {"form": "אַנדערע", "bare": "אנדערע",
                  "note": "'Others' — a chorus subset"},
    },
    YAAKOV: {
        "flk": {"form": "פֿאלק", "bare": "פאלק",
                "note": "the People/Crowd, 8 spans"},
    },
}

# ── per-scene overrides (duet pronouns) ─────────────────────────────────────
# Emigration's is already written to data/MS_Emigration/speaker_overrides.json.
OVERRIDES = {
    TISSA: [{
        "pages": [22],
        "context": f"{NOTE}: duet on p.22. ער = Moritz Scharf, זיא = Barbara, "
                   f"both named in the comments and both already in the castList.",
        "labels": {"ער": "mrits", "זיא": "brbr"},
    }],
    TNOIM: [{
        "pages": [46],
        "context": f"{NOTE}: duet on p.46. The lines name their own singers — "
                   f"'אָט איז הילל - און אויך הילנאַ' / 'אָט נחמן'. Noa's comments "
                   f"say map ער->nkhmn and זיע->hiln, both in the castList.",
        "labels": {"ער": "nkhmn", "זיע": "hiln"},
    }],
    BENHA: [{
        "pages": [17],
        "context": f"{NOTE}: the lovers' duet on p.17. Held back — see HELD. "
                   f"Noa's comment identifies the SCENE but not the singers, "
                   f"and the pp.32-33 trio is a different scene sharing the "
                   f"label ער.",
        "labels": {},
    }],
}

# ── not a speaker: span is mis-tagged ───────────────────────────────────────
NOT_SPEAKER = {
    (BENHA,  "Refrain"): "musical heading for a song's chorus section",
    (BASKOY, "פראזע 1"): "structural musical cue ('Phrase 1'), not a speaker",
    (TNOIM,  "אי כר"):   "OCR of אַיֶכָּה inside R. Yochanan's own line",
    (YAAKOV, "169030"):  "catalogue number / OCR digit artefact",
    (YOYSEF, "סע"):      "mis-tagged; ticked 'not a speaker'",
}

# ── held back: nothing guessed ──────────────────────────────────────────────
HELD = {
    (YOYSEF, "נאר"):
        "Comment is 'לא יודעת כרגע' ('I don't know at the moment') — an "
        "explicit non-answer, and no box carries it. p.19.",
    (BENHA, "ער"):
        "7 spans over two DIFFERENT scenes (lovers' duet p.17; comic trio "
        "pp.32-33), so one xmlid cannot cover them and the comment names "
        "neither singer. Needs the two scenes answered separately.",
    (BENHA, "זיא"):
        "Same duet as Ben HaDor's ער — the comment says only 'the female "
        "speaker in the duet on p.17', naming nobody.",
    (BENHA, "צורהלפידות"):
        "Comment says it is Tsroyh AND Lfidus singing in unison, i.e. a "
        "span covering two roles — the same shape as the castList "
        "addendum's split cases, which needs a span edit, not a cast_dict "
        "entry. Both roles exist (tsroyh, lfidus).",
    (EMIG, "ירוחם שפרה"):
        "Comment says split to irukhm + shfrh — again a two-role span, "
        "needing a span edit rather than a variant.",
    (TNOIM, "שמעון פדות"):
        "Ticked bn_fdus, but the comment says 'Shimon and Pdos performing "
        "together... tag as a combined group'. Tick and comment disagree "
        "about ARITY, not spelling, so neither reading is safe to assume.",
}


def load(play):
    p = REPO / "data" / play / "cast_dict.json"
    return p, json.loads(p.read_text(encoding="utf-8"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()

    plays = sorted({*VARIANTS, *MINT, *COLLECTIVE, *COLLECTIVE_MINT, *OVERRIDES})
    if a.only:
        plays = [p for p in plays if p == a.only]
    n_var = n_new = 0

    for play in plays:
        path, d = load(play)
        roles = d["roles"]
        changed = []

        for xid, spec in {**MINT.get(play, {}),
                          **COLLECTIVE_MINT.get(play, {})}.items():
            if xid in roles:
                changed.append(f"  = {xid:16} already present, left alone")
                continue
            entry = {"form": spec["form"], "bare": spec["bare"],
                     "source": "body",
                     "notes": [f"{NOTE}: {spec['note']}"]}
            if spec.get("collective") or xid in COLLECTIVE_MINT.get(play, {}):
                entry["collective"] = True
            if spec.get("prefix_variants"):
                entry["prefix_variants"] = list(spec["prefix_variants"])
            roles[xid] = entry
            changed.append(f"  + {xid:16} {spec['form']}  — {spec['note']}")
            n_new += 1

        for src in (VARIANTS.get(play, {}), COLLECTIVE.get(play, {})):
            for xid, surfaces in src.items():
                if not surfaces:
                    continue
                if xid not in roles:
                    changed.append(f"  !! {xid}: not in castList and not "
                                   f"minted — variants NOT applied")
                    continue
                have = roles[xid].setdefault("prefix_variants", [])
                add = [s for s in surfaces if s not in have]
                if not add:
                    continue
                have.extend(add)
                roles[xid].setdefault("notes", []).append(
                    f"{NOTE}: +{len(add)} prefix variant(s)")
                changed.append(f"  ~ {xid:16} += {', '.join(add)}")
                n_var += len(add)

        if changed:
            print(f"\n{play}")
            for c in changed:
                print(c)
        if changed and not a.dry_run:
            path.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8")

        ov = OVERRIDES.get(play)
        if ov and any(s["labels"] for s in ov):
            f = REPO / "data" / play / "speaker_overrides.json"
            cur = json.loads(f.read_text(encoding="utf-8")) if f.exists() \
                else {"scenes": []}
            pages = {tuple(s["pages"]) for s in cur["scenes"]}
            new = [s for s in ov if s["labels"] and tuple(s["pages"]) not in pages]
            if new:
                cur["scenes"].extend(new)
                print(f"  overrides: +{len(new)} scene(s) "
                      f"{[s['pages'] for s in new]}")
                if not a.dry_run:
                    f.write_text(json.dumps(cur, ensure_ascii=False, indent=2)
                                 + "\n", encoding="utf-8")

    print(f"\n{'DRY RUN — ' if a.dry_run else ''}"
          f"{n_new} roles minted, {n_var} prefix variants added")
    print(f"{len(NOT_SPEAKER)} labels marked not-a-speaker, "
          f"{len(HELD)} held back:")
    for (play, label), why in HELD.items():
        print(f"  {play:22} {label:14} {why.splitlines()[0]}")
    print("\nNext: python3.11 -m annotation.auto_resolve_flags  (re-resolve "
          "spans against the updated cast_dicts)")


if __name__ == "__main__":
    main()
