"""Three GENERIC rows whose own sentence individuates them (Sinai 11-22; text-resolvable, so ours).

  C07166_Q01 'שולמית' -> NEW: Yardeni "continued his music studies at Shulamit" in
             Eretz-Israel = the Shulamit Conservatory, Tel Aviv (founded 1910).
  C07166_Q02 'שולמית' (Warsaw) -> NEW: König joined the Polish-language "Shulamit" choir
             in Warsaw at age 10; a different body from the conservatory.
  C01713 'ברייטענער שול 4' -> NEW: Leib A. staged a play there; the number individuates.
             Parent network (Arbeter Ring? SA Folk Institute?) asked in the team Doc.
  C00069 'צווישאָ' -> NEW: Zhitnitsky's entry is set in Buenos Aires, so the Argentine
             TsVISHO, not the Polish TsIShO (1111).

Idempotent. Usage: python3.11 apply_individuated_generics_2026_11_22.py [--apply --reviewer Sinai]
"""
from pathlib import Path
import argparse
import apply_network_parents_2026_11_22 as m

m.SRC = Path(__file__).name
m.TAG = "individuated generics 2026-11-22"
m.MINTS = {
    "shulamit_tlv": ("Shulamit Conservatory, Tel Aviv", "קאָנסערוואַטאָריע „שולמית“ (תל-אביב)", "תל-אביב", ""),
    "shulamit_waw": ("Shulamit choir, Warsaw", "כאָר „שולמית“ (וואַרשע)", "וואַרשע", ""),
    "brighton4": ("Brighton School 4", "ברייטענער שול 4", "", ""),
    "tsvisho_ar": ("TsVISHO (Central Secular Yiddish School Organization), Argentina",
                   "צווישאָ (אַרגענטינע)", "אַרגענטינע", ""),
}
TYPES = {"כאָר „שולמית“ (וואַרשע)": "Musical organization"}
m.ALIGN = {
    "ORG-C07166_Q01": ("MINT:shulamit_tlv", "music studies at 'Shulamit' in Eretz-Israel = the Shulamit Conservatory, Tel Aviv"),
    "ORG-C07166_Q02": ("MINT:shulamit_waw", "the Polish-language 'Shulamit' choir, Warsaw (König, age 10)"),
    "ORG-C01713": ("MINT:brighton4", "'Brighton school 4': the number individuates"),
    "ORG-C00069": ("MINT:tsvisho_ar", "Zhitnitsky (Buenos Aires: Di prese, YIVO Argentina, IFT) was president of "
                   "the TsVISHO and the Sholem Aleichem schools: the Argentine school organization, not the Polish TsIShO 1111"),
}
m.PARENTS = {}

_orig_dump = m._dump
def _dump(p, fields, rows, term):
    if p.name in ("core_db.tsv", "org_addresses_review.tsv"):
        key = "name_yiddish" if p.name == "core_db.tsv" else "canonical_yiddish"
        for r in rows:
            if r.get(key) in TYPES and r.get("org_type") != TYPES[r[key]]:
                r["org_type"] = TYPES[r[key]]
    _orig_dump(p, fields, rows, term)
m._dump = _dump

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    m.main(a.apply, a.reviewer.strip())
