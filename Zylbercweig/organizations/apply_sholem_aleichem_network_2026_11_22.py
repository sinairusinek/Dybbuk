"""Sholem Aleichem school network: same rule as apply_network_parents_2026_11_22.py (Sinai 11-22).

The Sholem Aleichem Folk Institute (1929, New York, 1918-) ran the Sholem Aleichem folk
schools, the mitlshul and Camp Boiberik. A generic sub-unit ALIGNs to it; an individuating one
(a number, the one mitlshul) gets its own row under it. Detroit's Sholem Aleichem Institute
(4368), Chicago's Folk Institute (1942) and the Harbin school (1975) are local bodies, not
SAFI units (Chicago's link to SAFI is asked in the team Doc).

  ALIGN 1929: C00538 (children of the SA folk schools), C07243 (first teachers of the SA
              folk schools), C06930 ("a SA folk school")
  MINT under 1929: C07239 "SA school 3" (numbered), C01518 "SA mitlshul" (graduated)
  ALIGN 4368: C00197_Q02 Detroit "SA folk institute" (member) = the Detroit institute
  ALIGN 1975: C01007 Harbin "SA garden" (the school's kindergarten; same secretary, Shumsky)
  set_parent 1929: 1938 Camp Boiberik, 2238 SA school (NY), 1941 SA school 2
Left: C01645_Q02 (DEFER = Ruthie's queue; NY "SA Institute" schools = 1929, proposed in the
Doc); 1940/1941 are duplicate "SA school 2" rows both linking C00854 (flagged).

Idempotent. Usage: python3.11 apply_sholem_aleichem_network_2026_11_22.py [--apply --reviewer Sinai]
"""
from pathlib import Path
import argparse
import apply_network_parents_2026_11_22 as m

SAFI = "1929"
m.SRC = Path(__file__).name
m.TAG = "Sholem Aleichem network 2026-11-22"
m.MINTS = {
    "sa3": ("Sholem Aleichem School 3 (Sholem Aleichem Folk Institute)", "שלום עליכם שול 3", "", SAFI),
    "samitl": ("Sholem Aleichem Mitlshul (Sholem Aleichem Folk Institute)", "שלום-עליכם-מיטלשול", "", SAFI),
}
m.ALIGN = {
    "ORG-C00538": (SAFI, "children of the Sholem Aleichem folk schools (plural, no place): the network"),
    "ORG-C07243": (SAFI, "among the first teachers of the Sholem Aleichem folk schools: the network"),
    "ORG-C06930": (SAFI, "'a Sholem Aleichem folk school' (indefinite): the network"),
    "ORG-C07239": ("MINT:sa3", "assistant teacher in Sholem Aleichem School 3: the number individuates"),
    "ORG-C01518": ("MINT:samitl", "graduated the Sholem Aleichem mitlshul: the network had one"),
    "ORG-C00197_Q02": ("4368", "Detroit 'Sholem Aleichem folk institute' = Detroit's Sholem Aleichem Institute"),
    "ORG-C01007": ("1975", "Harbin 'Sholem Aleichem garden' = the Harbin SA school's kindergarten (same secretary)"),
}
m.PARENTS = {"1938": SAFI, "2238": SAFI, "1941": SAFI}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    m.main(a.apply, a.reviewer.strip())
