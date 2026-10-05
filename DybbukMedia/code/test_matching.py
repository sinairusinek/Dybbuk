"""Regression tests for the transliteration-tolerant title matcher.

Yiddish romanisation is unstandardised across these sources, so the matcher
folds the systematic alternations. Every POSITIVE below is a spelling actually
found in one of the source sheets; every CONTROL is a real Yiddish play that is
NOT in this db and must never be claimed.

Run: python3 DybbukMedia/code/test_matching.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import load_entities, load_title_index, propose_title

POSITIVES = {
    "Al Nahares Bovl": "edition:AlNaharotBavel-Amkreut&Freund1909",
    "Al Naharot Bavel": "edition:AlNaharotBavel-Amkreut&Freund1909",
    "Kidesh Hashem, oder der yidisher minister": "edition:KidushHashem",
    "Khinke-Pinke": "edition:HinkePinke",
    "Ishe-roe": "edition:IshahRaah",
    "Di seyder nakht": "edition:Di_seyder_nakht_Emkroyt_1908",
    "Yoysef in Egypten": "edition:MS_YoysefInEgipten",
    "Khurbn Yerusholaim": "edition:MS_KhurbnYerusholaim",
    "Bas koyen": "edition:MS_BasKoyen",
    "Di tsvey tnoim": "edition:MS_DiTsveyTnoim",
    "Emigration": "edition:MS_Emigration",
    "Ezre oder der ewiger Jude [eybiker yid]": "edition:Ezra-Emkroyt1908",
    "Mishke Mashke": "edition:MishkeMashke-Kultur1910",
    "Meshumed": "edition:Lateiner_Meshumed",
    "Sore Sheyndel": "edition:SoreSheyndel",
    "Bas-Sheve": "edition:BasSheva",
    "Der man untern tish": "edition:DerManUnterTiff",
    "Shimshon Hagibor": "edition:YIVO_ShimshonHagibor",
    "Tissa Essler": "edition:MS_TissaEssler",
    "Dovid's fidele": "edition:DovidsFidele-1904",
    "Dos yudishe herts": "edition:DosYudisheHerts-1910",
    "Yudale der Blinder": "edition:Yudale_der_blinder,_Emkroyt1908",
    "Blimele di perl fun Varshe oder graf un yid": "edition:Blimele-AhronFaust1903",
    "Yaakov und Esav": "edition:MS_YaakovEsav",
    "Yakov un Eysev": "edition:MS_YaakovEsav",
    "Ben Hador": "edition:MS_BenHaDor",
}

# Real Yiddish plays that are NOT Lateiner/Hurwitz editions in this db.
CONTROLS = [
    "Yom hakhupe", "Shloymke shlimazl", "Dinele, oder a gast fun yener velt",
    "Der sotn in gan-eydn", "Di grinhorns", "Shloym'ke un Rikl", "Hamlet",
    "Der Dibuk", "Di kishefmakherin", "Shulamis", "Bar Kokhba",
    "Di yeshive bokher",
]


def main() -> int:
    ents = load_entities()
    idx = load_title_index(ents)
    failures = []

    for text, want in POSITIVES.items():
        got, status = propose_title(ents, idx, text)
        if got != want:
            failures.append(f"  {text!r}\n    want {want}\n    got  {got or '(GAP)'}")

    for text in CONTROLS:
        got, _ = propose_title(ents, idx, text)
        if got:
            failures.append(f"  FALSE POSITIVE {text!r} -> {got}")

    total = len(POSITIVES) + len(CONTROLS)
    if failures:
        print(f"FAIL {len(failures)}/{total}")
        print("\n".join(failures))
        return 1
    print(f"ok  {len(POSITIVES)} titles anchored, "
          f"{len(CONTROLS)} controls correctly refused")
    return 0


if __name__ == "__main__":
    sys.exit(main())
