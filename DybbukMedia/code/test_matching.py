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
    # Every play title anchors to the WORK, never to a printing: the edition is
    # reachable through `realised_as`, so the anchor must not depend on which
    # spelling a given source sheet happened to use.
    #
    # Three of these are findings rather than mere spelling variants:
    #  * 'Khinke-Pinke' -> work 3877 'Gabriel oder di libe fun a yidisher froy'.
    #    Not a mismatch: Zylbercweig records Gabriel was played in Europe under
    #    the title חינקע און פּינקע, and edition:HinkePinke realises 3877.
    #  * 'Mishke Mashke' -> work 3838 'Di grinhorns', which is that edition's
    #    own catalogue_play_key — the play's catalogue title differs from the
    #    printing's title page.
    #  * 'Yom hakhupe', 'Di grinhorns' and 'Der sotn in gan-eydn' were CONTROLS
    #    while the graph held only editions. They are real Lateiner works that
    #    were invisible because no edition of them survives.
    'Al Nahares Bovl': 'work:3915',                                   # Tsiyen, oder al nahares Bovl
    'Al Naharot Bavel': 'work:3915',                                  # Tsiyen, oder al nahares Bovl
    'Kidesh Hashem, oder der yidisher minister': 'work:3892',         # Kidesh Hashem, oder der yidisher
    'Khinke-Pinke': 'work:3877',                                      # Gabriel oder di libe fun a yidis
    'Ishe-roe': 'work:3884',                                          # Ishe roeh
    'Di seyder nakht': 'work:3847',                                   # Di seyder nakht, oder der bilbl-
    'Yoysef in Egypten': 'work:3933',                                 # Yoysef un zayne brider
    'Bas koyen': 'work:3963',                                         # Bas Cohen oder, Malka Alexandra
    'Di tsvey tnoim': 'work:4012',                                    # Di tsvey tanoyim
    'Emigration': 'work:3830',                                        # Di emigratsyon nokh Amerike
    'Ezre oder der ewiger Jude [eybiker yid]': 'work:3875',           # Ezre oder der ewiger Jude [eybik
    'Mishke Mashke': 'work:3838',                                     # Di grinhorns
    'Meshumed': 'work:3879',                                          # Goles Rusland
    'Sore Sheyndel': 'work:3833',                                     # Di farblondzhete neshome
    'Bas-Sheve': 'work:3798',                                         # Bas-Sheve
    'Der man untern tish': 'work:3817',                               # Der man untern tish
    'Shimshon Hagibor': 'work:3959',                                  # Shimshun Hagiber
    'Tissa Essler': 'work:3944',                                      # Tisa Esler
    "Dovid's fidele": 'work:3869',                                    # Dovids fidele
    'Dos yudishe herts': 'work:3866',                                 # Dos yidishe harts
    'Yudale der Blinder': 'work:3927',                                # Yidele oder der emes un sheyker
    'Yaakov und Esav': 'work:4014',                                   # Yanḳev un Eysev
    'Yakov un Eysev': 'work:4014',                                    # Yanḳev un Eysev
    'Ben Hador': 'work:4010',                                         # Ben Hador
    'Yom hakhupe': 'work:3929',                                       # Yom ha-khupe
    'Di grinhorns': 'work:3838',                                      # Di grinhorns
    'Der sotn in gan-eydn': 'work:3825',                              # Der sotn in gan-eydn, oder di sh
    'Blimele di perl fun Varshe oder graf un yid': 'work:3801',       # Blimele di perl fun Varshe oder 
}

# Real Yiddish plays that are NOT Lateiner/Hurwitz editions in this db.
# Plays by OTHER authors, absent from the Lateiner/Hurwitz catalogue entirely.
# Most of the former controls had to be promoted to POSITIVES: Yom hakhupe,
# Dinele, Der sotn in gan-eydn, Di grinhorns, Shlomke un Rikl, Der dibek and
# Der yeshive-bokher are all real Lateiner works. `Der dibek` is Lateiner's own
# play, not Ansky's; `Der yeshive-bokher` sits in the catalogue as a
# `false ascription`, which by decision still gets a node. That churn is itself
# the measure of what the work layer made visible.
# Each verified absent from the catalogue before being used here. Picking
# controls by intuition failed repeatedly: `Uriel Acosta` turned out to be
# Hurwitz's "Kolonye Shomron-Samarye oder Uriel Akosta in khalat" (4024), and
# `Der Oytser` a Lateiner work (3820). Famous plays by Goldfaden, Gordin,
# Sholem Aleichem, Hirschbein, Asch and Singer are the safe ground.
CONTROLS = [
    "Hamlet", "Di kishefmakherin", "Shulamis", "Bar Kokhba",
    "Der yidisher kenig Lir", "Mirele Efros", "Got fun nekome",
    "Tevye der milkhiker", "Grine felder", "Yoshe Kalb",
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
