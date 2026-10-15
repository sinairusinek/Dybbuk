#!/usr/bin/env python3
"""Sinai's rulings on the 2026-10-15 eponym mint, applied.

1. MERGE two mints into the pre-existing people Sinai confirmed as the same
   person. The mint rows are retired, not deleted: their surfaces and evidence
   move onto the survivor, which keeps its id.

     3841 עדוואַרד מאָרגולעס -> 2394 מאַרגאָליס, עדוואַרד (יקותיאל)
     3829 יעקב ליבערט       -> 2282 ליבּערט, יאַנקעוו

   Sinai also approved db735, db2371 and db3070 as good matches, but those
   three were never minted -- the mint script recognised them as existing and
   routed them to names_not_minted.tsv. They are links the eponym edges can
   use directly, with no merge needed.

2. DETACH the surface `אידא קאַמינסקאַ` (x56) from HUB-D3612 רעגינאַ
   קאַמינסקאַ. Regina was born 1894 and died 1913-09-28; Ida Kaminska
   (1899-1980) is a different person with no entry of her own, and 56
   attestations of her name were being credited to her dead sister. Ida stays
   minted (3832); the surface is routed to her.

3. DETACH the surface `חיהלע פֿרידמאַן` from HUB-P-5-facs_604
   חיהלע אָש (פֿורמאַן). Sinai: פורמאן is not פרידמאן. Confirmed: the Ash entry
   gives her birth as 1920 in Kishinev, daughter of the actors Avrom and Polya
   Ash -- פֿורמאַן is a married name in parentheses. The פֿרידמאַן mention runs a
   1927 collective ("1927 - אין קאָלעקטיוו פֿון חיהלע פֿרידמאַן און גוטהערץ"),
   which a seven-year-old did not. The match was `exact` on the given name
   alone. חיהלע פֿרידמאַן was never minted (she was excluded as "already
   exists" on the strength of exactly this bad surface); detaching it leaves
   her unrecorded, which names_not_minted.tsv is corrected to say.

4. Diana Blumenfeld: the db row 1345 IS hers (דיאַנאַ בּלומענפעלד-טורקאָוו).
   Nothing to mint. Recorded here only so the question is answered in the data.

  python3 fix_eponym_mint_followups.py            # dry run
  python3 fix_eponym_mint_followups.py --execute
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import unicodedata

csv.field_size_limit(10 ** 9)


def nfc(s: str) -> str:
    """Compare Hebrew through NFC.

    person_hub_members stores PRECOMPOSED forms -- `פֿ` is the single codepoint
    U+FB4E, while a literal typed here is U+05E4 + U+05BF. A `==` between them
    is False and the detach silently does nothing, which is exactly how the
    pointed-rule bug in survey_empty_type.py went unnoticed. Normalise both.
    """
    return unicodedata.normalize("NFC", s or "")

HERE = pathlib.Path(__file__).resolve().parent
PEOPLE_DB = HERE / "people_db.tsv"
EVIDENCE = HERE / "new_mints_evidence.tsv"
HUB = HERE / "person_hub.tsv"
HUB_MEMBERS = HERE / "person_hub_members.tsv"

# minted id -> (survivor db_id, why)
#
# Only TWO merges are needed. Sinai named four good matches -- 735, 2371, 2394,
# 3070 -- but Abramson (735), Magidzohn (2371) and Thal (3070) were never
# minted: the mint script had already recognised them as existing and routed
# them to names_not_minted.tsv. Nothing to merge there; they are links the
# eponym edges can simply use. Same for חיהלע פֿרידמאַן, never minted either.
MERGES = {
    "3841": ("2394", "db2394 מאַרגאָליס, עדוואַרד (יקותיאל) carries the surface "
                     "יקותיאָל מאַרגולעס; same given name, same surname family"),
    "3829": ("2282", "db2282 ליבּערט, יאַנקעוו = Yankev Libert, b.1874 Warsaw, "
                     "Goldfaden's boys' choir 1885. The 25 יעקב ליבערט mentions "
                     "name his partners as ליבערט, ווייסמאַן, ראַקייטין and "
                     "יעקב ליבערט און ראָקיטין -- precisely the partners of "
                     "db1057 (ליבערט און ראָקייטין) and db1402 (יעקב ליבערט, "
                     "רודאָלף זאַסלאַווסקי און ה. ווייסמאַן). One person."),
}

# (hub_id, surface) pairs to detach, with the reason
DETACH = [
    ("HUB-D3612", "אידא קאַמינסקאַ",
     "Ida Kaminska (1899-1980) is not her sister Regina (1894-1913-09-28). "
     "x56 attestations, against Regina's own x5 and x2."),
    ("HUB-P-5-facs_604_tr_1739220749", "חיהלע פֿרידמאַן",
     "חיהלע אָש (פֿורמאַן) was born 1920; חיהלע פֿרידמאַן ran a collective in "
     "1927. פורמאן is not פרידמאן -- matched on the given name alone."),
]


def read(p: pathlib.Path) -> tuple[list[dict], list[str], str]:
    raw = p.read_bytes()
    term = "\r\n" if raw.count(b"\r\n") else "\n"
    rows = list(csv.DictReader(p.open(encoding="utf-8"), delimiter="\t"))
    return rows, list(rows[0].keys()), term


def write(p: pathlib.Path, rows: list[dict], fields: list[str], term: str) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator=term, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    people, p_fields, p_term = read(PEOPLE_DB)
    ev, e_fields, e_term = read(EVIDENCE)
    hub, h_fields, h_term = read(HUB)
    mem, m_fields, m_term = read(HUB_MEMBERS)

    by_id = {r["db_id"]: r for r in people}

    # ── 1. merges ──────────────────────────────────────────────────────────
    print("MERGES")
    keep_people, merged = [], 0
    for r in people:
        did = r["db_id"]
        if did in MERGES:
            surv_id, why = MERGES[did]
            surv = by_id[surv_id]
            # carry the loser's surfaces onto the survivor, union not overwrite
            have = [v for v in (surv.get("name_variants") or "").split("|") if v.strip()]
            for v in (r.get("name_variants") or "").split("|"):
                if v.strip() and v.strip() not in have:
                    have.append(v.strip())
            surv["name_variants"] = "|".join(have)
            note = f"merged_from {did} ({r.get('hebname')}) 2026-10-15 Sinai"
            surv["source"] = ((surv.get("source") or "").strip() + "; " + note).lstrip("; ")
            print(f"  {did} {r.get('hebname'):<26} -> {surv_id} {surv.get('hebname')[:28]}")
            print(f"       surfaces now: {surv['name_variants'][:88]}")
            merged += 1
            continue
        keep_people.append(r)
    print(f"  merged {merged}; people_db {len(people)} -> {len(keep_people)}")

    keep_ev = [r for r in ev if r["db_id"] not in MERGES]
    print(f"  evidence {len(ev)} -> {len(keep_ev)}")

    # ── 2 + 3. detach misattributed surfaces ───────────────────────────────
    print("\nDETACH SURFACES")
    drop = {(h, nfc(s)) for h, s, _ in DETACH}
    keep_mem = []
    for r in mem:
        if (r["hub_id"], nfc(r["member_id"])) in drop and r["member_kind"] == "surface":
            print(f"  {r['hub_id']}  -{r['member_id']}  ({r['detail']})")
            continue
        keep_mem.append(r)
    print(f"  members {len(mem)} -> {len(keep_mem)}")

    for h in hub:
        for hub_id, surface, _ in DETACH:
            if h["hub_id"] == hub_id:
                surfs = [s for s in (h.get("top_surfaces") or "").split("|")
                         if s.strip() and nfc(s.strip()) != nfc(surface)]
                if (h.get("top_surfaces") or "") != "|".join(surfs):
                    print(f"  {hub_id} top_surfaces -> {'|'.join(surfs) or '(none)'}")
                h["top_surfaces"] = "|".join(surfs)
                h["n_surfaces"] = str(len(surfs))

    if not args.execute:
        print("\nDRY RUN -- nothing written. Re-run with --execute.")
        return

    write(PEOPLE_DB, keep_people, p_fields, p_term)
    write(EVIDENCE, keep_ev, e_fields, e_term)
    write(HUB, hub, h_fields, h_term)
    write(HUB_MEMBERS, keep_mem, m_fields, m_term)
    print("\nwrote people_db.tsv, new_mints_evidence.tsv, person_hub.tsv, "
          "person_hub_members.tsv")


if __name__ == "__main__":
    main()
