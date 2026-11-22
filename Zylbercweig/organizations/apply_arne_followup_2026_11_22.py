"""Settle what the lexicon text decides in Arne's leftovers (2026-11-22).

Inputs read by Claude (Sonnet readers, checked by Claude against the mention sentences):
  - vienna_berlin_arne_open_questions.tsv (47 rows Arne left as questions after the 06-28 ingest)
  - arne_batch_2026-06-21_audit_for_RA.tsv (94-row batch 2, never done by an RA)
Only text-decided changes are applied here; core_db merges with more than one cluster and the cases needing outside
research are left (see arne_followup_2026_11_22_open.tsv).

  python3.11 apply_arne_followup_2026_11_22.py                    # dry run
  python3.11 apply_arne_followup_2026_11_22.py --apply --reviewer Sinai
"""
from __future__ import annotations
import argparse, datetime as _dt, json
from pathlib import Path
from apply_education_q2_2026_11_22 import read, write, pipes, origin_high_water

HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
AL, CORE = HERE / "org_alignment_review.tsv", HERE / "core_db.tsv"

REALIGN = {  # cluster -> (new db_id, reason)
    "ORG-C03191_Q03": ("1886", "Berlin 1921 Yidisher kunstler-teater (Azro), not NYC YAT nor Vilna Troupe proper"),
    "ORG-C05791": ("1886", "'newly founded Yiddish Art Theatre (Azro)', Berlin 1921 = 1886"),
    "ORG-C00045": ("180", "Axelrad in Czernowitz (Tsipkus, Bria 1911, Melzer...) = 180; 340 is Morris Axelrad, Paris"),
    "ORG-C02769": ("180", "same Czernowitz Axelrad troupe as 180 (Krakow 1912 = tour)"),
    "ORG-C00235_Q03": ("1839", "German theatre in Czernowitz with a director = the German Stadttheater 1839"),
    "ORG-C05485": ("1743", "one mention, 'troupes of Reisch' = Reisch troupe; no basis for a split"),
}
DECIDE = {  # cluster -> (decision, reason)
    "ORG-C05541": ("NOT_AN_ORG", "Grosse Berliner Kunstausstellung: an exhibition (event), not an org"),
    "ORG-C01929": ("NOT_AN_ORG", "names a person (director David Edelhofer); no venue in the text"),
    "ORG-C05905": ("GENERIC", "plural description: German cafe-chantants / Singspielhallen"),
}
RETYPE = {  # db_id -> (new org_type, reason)
    "1887": ("Traveling Company", "'Deutsch-Yiddish' = firm name of Librescu + Berger's troupe, Berlin 1880/81"),
    "4810": ("Business", "Hugo Baruch's atelier for sets/costumes: a workshop, not education"),
    "4839": ("Business", "stage-decoration studio (Herman, Koyfman, Reynman, Baruch)"),
    "4594": ("Non-Yiddish Theatre", "Victor Barnowsky's German theatre, Berlin"),
    "1843": ("Kleinkunst", "Yiddish Kinstler-Shpiln, Vienna: cabaret ensemble"),
    "1841": ("Kleinkunst", "Yiddish Kinstler-Kabaret, Vienna"),
    "4440": ("Non-Yiddish Theatre", "Jarno-Buehnen: Josef Jarno's German theatres, Vienna"),
    "3121": ("Publisher", "farlag/committee publishing an author's works"),
    "4458": ("Non-Yiddish Theatre", "Koenigliches Residenztheater Munich"),
    "2922": ("Non-Yiddish Theatre", "Carl-Schultze-Theater Hamburg (German)"),
}
PARENT = {  # db_id -> (parent db_id, reason)
    "4307": ("2361", "1933 Berlin 'Kultur-Bund Theater' (Kandel co-founder) = the theatre of the Juedischer Kulturbund (2361), not the Bund"),
}
RETIRE = {  # db_id -> (merged_into or "", reason); only rows left with no cluster by the steps above
    "4330": ("1886", "duplicate of 1886 (Azro's Yidisher kunstler-teater, Berlin 1921)"),
    "863": ("180", "duplicate of 180 (Axelrad troupe, Czernowitz)"),
    "4390": ("", "art exhibition, not an org (ORG-C05541 -> NOT_AN_ORG)"),
    "4393": ("", "a person (David Edelhofer), not an org (ORG-C01929 -> NOT_AN_ORG)"),
}
MINT = {  # cluster -> (name, org_type, settlement, reason)
    "ORG-C07105": ("Berliner Neue Bühne", "Non-Yiddish Theatre", "Berlin",
                   "Emanuel Reicher's German Neue Buehne; was wrongly aligned to 1716 (Merkur's Yiddish Freie Buehne 1919)"),
    "ORG-C05107": ("Verein Zion (Wien)", "Amateur", "Vienna", "named Vienna association hosting amateur literary performances"),
    "ORG-C03625": ("Czernowitzer Schulverein", "Education", "Czernowitz", "named school society, not generic"),
    "ORG-C06257": ("Münchner Schauspielhaus", "Non-Yiddish Theatre", "Munich", "named German house, not a generic playhouse"),
    "ORG-C02372": ("Israelitische Religionsgemeinde Leipzig", "Religious institutions/organizations", "Leipzig",
                   "Leipzig kehile; named kehiles are typed Religious institutions (3028, 3079, 3180)"),
}


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kf, core, knl = read(CORE)
    af, al, anl = read(AL)
    byid = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    log = []

    def L(view, action, target, decision, note, extra):
        log.append([now, reviewer or "-", view, action, target, decision, note,
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    def unlink(db, cid):
        r = byid.get(db)
        if r is not None:
            r["linked_cluster_ids"] = "|".join(x for x in pipes(r.get("linked_cluster_ids", "")) if x != cid)

    def link(db, cid):
        r = byid[db]
        ids = pipes(r.get("linked_cluster_ids", ""))
        if cid not in ids:
            r["linked_cluster_ids"] = "|".join(ids + [cid])

    def stamp(a, note):
        a["reviewer"], a["reviewed_at"] = reviewer or "-", now
        old = a.get("reviewer_notes", "")
        a["reviewer_notes"] = f"[11-22 text-read] {note}" + (f" || {old}" if old else "")

    for cid, (db, why) in REALIGN.items():
        a = arow[cid]; old = a["aligned_db_id"]
        assert db in byid and byid[db].get("deprecated", "").lower() != "true", db
        print(f"  realign {cid:15} {a['decision'] or '-'} {old or '-'} -> ALIGN {db}")
        unlink(old, cid); link(db, cid)
        a["decision"], a["aligned_db_id"] = "ALIGN", db
        stamp(a, why)
        L("org_review", "alignment", cid, "ALIGN", why, {"aligned_db_id": db, "was": old})
    for cid, (dec, why) in DECIDE.items():
        a = arow[cid]; old = a["aligned_db_id"]
        print(f"  decide  {cid:15} {a['decision'] or '-'} {old or '-'} -> {dec}")
        unlink(old, cid)
        a["decision"], a["aligned_db_id"] = dec, ""
        stamp(a, why)
        L("org_review", "alignment", cid, dec, why, {"was": old})
    for db, (t, why) in RETYPE.items():
        r = byid[db]
        print(f"  retype  {db:>5} {r['org_type']} -> {t}")
        L("org_review", "set_org_type", db, t, why, {"was": r["org_type"]})
        r["org_type"] = t
    for db, (par, why) in PARENT.items():
        r = byid[db]
        print(f"  parent  {db:>5} {r['parent_db_id'] or '-'} -> {par}")
        L("org_review", "set_parent", db, par, why, {"was": r["parent_db_id"]})
        r["parent_db_id"] = par
    for db, (into, why) in RETIRE.items():
        r = byid[db]
        assert not pipes(r.get("linked_cluster_ids", "")), (db, r["linked_cluster_ids"])
        assert not any(db in pipes(a["aligned_db_id"]) for a in al), db
        print(f"  retire  {db:>5} -> merged_into {into or '-'}")
        r["deprecated"], r["merged_into"] = "true", into
        L("org_review", "merge" if into else "deprecate", db, into or "deprecated", why, {})
    nxt = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    for cid, (name, t, city, why) in MINT.items():
        a = arow[cid]; old = a["aligned_db_id"]
        new = str(nxt); nxt += 1
        row = {k: "" for k in kf}
        row.update(db_id=new, name=name, name_yiddish=a["canonical_yiddish"], org_type=t, linked_cluster_ids=cid)
        core.append(row); byid[new] = row
        unlink(old, cid)
        print(f"  mint    {cid:15} {a['decision'] or '-'} {old or '-'} -> NEW {new} {name} ({t}, {city})")
        a["decision"], a["aligned_db_id"] = "NEW", new
        stamp(a, f"{why}; settlement {city}")
        L("org_review", "mint_solo", cid, "NEW", why, {"db_id": new, "name": name, "org_type": t, "settlement": city, "was": old})
    print(f"{len(log)} log rows")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    write(CORE, kf, core, knl); write(AL, af, al, anl)
    p = HERE / "activity_log.tsv"
    b = p.read_bytes()
    with p.open("ab") as f:
        if not b.endswith(b"\n"):
            f.write(b"\r\n")
        for row in log:
            f.write(("\t".join(row) + "\r\n").encode("utf-8"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
