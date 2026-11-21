"""Camps, hospitality and agency types (Sinai, 2026-11-21).

Sinai approved: (1) the summer-camp classification, (2) retyping the existing hotel /
tavern rows to Hospitality and the concert/theatre bureaus to Theatre/concert agency,
(3) reopening the hotel/café/restaurant clusters reviewers had set aside before the
Hospitality type existed.

  RETYPE   core_db + cards org_type
  PARENT   camp -> its movement, only where the Lexicon text names it
  MINT     "Unzer Kemp" (cluster ORG-C01414, was NOT_AN_ORG): Hospitality, parent = Farband 405
           ("„אונזער קעמפּ“ פֿון יידיש-נאַציאָנאַלן אַרבעטער-פֿאַרבאַנד, אין היילאַנד מילס, נ. י.")
  REOPEN   11 clusters: decision cleared, org_type = Hospitality, note kept for the reviewer

Left alone: Kinderland 206 (text links it to both the Arbeter-ring, 1925, and the Ordn);
Brandeis 2112 (no movement); Drancy / Poniatowa / Kitchener stay "Not an organization".
Ids from origin's high-water mark. Idempotent.

Usage: python3.11 apply_camps_hospitality_2026_11_21.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, csv, datetime as _dt, json, sys
from pathlib import Path
from apply_publisher_audit_mints import origin_high_water
from apply_amateur_hosts_2026_11_21 import _rw, _dump

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
HOSP, AGENCY = "Hospitality", "Theatre/concert agency"

RETYPE = {
    # adult vacation camps (resorts with a cultural programme)
    "3296": (HOSP, "Camp Lakeland: adult summer camp, actors' and reciters' summer culture work"),
    "3706": (HOSP, "Camp Nit Gedayget: adult summer camp; amateur groups performed there"),
    "3836": (HOSP, "Camp Unity: adult summer camp"),
    # hotels, taverns, beer halls
    **{d: (HOSP, "hotel / tavern / beer hall") for d in
       ["3468", "3600", "3700", "4393", "4484", "4716", "4721", "1757"]},
    # concert / theatre bureaus and agencies
    **{d: (AGENCY, "concert / theatre bureau or agency") for d in
       ["659", "4549", "4775", "4794", "4795"]},
}
PARENT = {
    "2075": ("189", "Arbeter-ring camp (א\"ר-קעמפּ) Yungvelt"),
    "2086": ("189", "Arbeter-ring camp Kinder-ring"),
    "2087": ("189", "Arbeter-ring camp, South Haven"),
    "3795": ("189", "א\"ר קעמפּ"),
    "1938": ("1929", "„קעמפּ בויבעריק ביים שלום עליכם פֿאָלקסאינסטיטוט“"),
    "4297": ("UNZER", "„דער קינדער-וועלט אין אונדזער קעמפּ“"),
}
UNZER = dict(cluster="ORG-C01414", name="Unzer Kemp", yi="אונדזער קעמפּ", typ=HOSP,
             addr="היילאַנד מילס, נ. י.", parent="405")
REOPEN = ["ORG-C01565", "ORG-C01826", "ORG-C02081", "ORG-C05339", "ORG-C05916", "ORG-C06042",
          "ORG-C07046", "ORG-C01185", "ORG-C01804", "ORG-C02428", "ORG-C04985"]


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, core, ct = _rw(HERE / "core_db.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    af, align, at = _rw(HERE / "org_alignment_review.tsv")
    by = {r["db_id"]: r for r in core}
    cards_by = {c["db_id"]: c for c in cards}
    al_by = {r["cluster_id"]: r for r in align}
    log = []

    # mint Unzer Kemp
    have = [r for r in core if UNZER["cluster"] in r.get("linked_cluster_ids", "")]
    if have:
        unzer = have[0]["db_id"]; print(f"  Unzer Kemp exists: db {unzer}")
    else:
        unzer = str(max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1)
        row = {f: "" for f in cf}
        row.update(db_id=unzer, name=UNZER["name"], name_yiddish=UNZER["yi"], org_type=HOSP,
                   address=UNZER["addr"], linked_cluster_ids=UNZER["cluster"], parent_db_id=UNZER["parent"])
        core.append(row); by[unzer] = row
        log.append([now, reviewer, "org_review", "NEW", UNZER["cluster"], unzer,
                    "Unzer Kemp — Farband summer camp (Highland Mills NY); was NOT_AN_ORG before the Hospitality type",
                    json.dumps({"org_type": HOSP, "parent_db_id": "405", "source": SRC}, ensure_ascii=False)])
        print(f"  mint db {unzer}  Unzer Kemp [{HOSP}] parent 405")
    a = al_by[UNZER["cluster"]]
    if a["decision"] != "NEW" or a["aligned_db_id"] != unzer:
        prev = a["decision"]
        a.update(decision="NEW", aligned_db_id=unzer, org_type=HOSP, reviewer=reviewer, reviewed_at=now,
                 reviewer_notes=(a["reviewer_notes"] + " | " if a["reviewer_notes"] else "")
                 + f"11-21 Sinai: was {prev}; adult summer camp of the Farband → Hospitality")

    for d, (typ, why) in RETYPE.items():
        r = by[d]
        if r["org_type"] != typ:
            log.append([now, reviewer, "org_addresses", "set_type", d, f"org_type={typ}",
                        f"was {r['org_type']}: {why}", json.dumps({"source": SRC})])
            print(f"  {d}: {r['org_type']} -> {typ}   {r['name_yiddish']}")
            r["org_type"] = typ
        if d in cards_by:
            cards_by[d]["org_type"] = typ

    for d, (p, why) in PARENT.items():
        p = unzer if p == "UNZER" else p
        r = by[d]
        if r["parent_db_id"] == p:
            continue
        if r["parent_db_id"]:
            print(f"  SKIP {d}: already has parent {r['parent_db_id']}"); continue
        r["parent_db_id"] = p
        if d in cards_by:
            cards_by[d]["parent_db_id"] = p
        log.append([now, reviewer, "org_addresses", "set_parent", d, f"parent_db_id={p}",
                    f"camp of its movement: {why}", json.dumps({"source": SRC}, ensure_ascii=False)])
        print(f"  {d}: parent -> {p}   {r['name_yiddish']}")

    for c in REOPEN:
        a = al_by[c]
        if a["decision"] in ("NOT_AN_ORG", "DEFER"):
            prev, who = a["decision"], a["reviewer"]
            a["reviewer_notes"] = (a["reviewer_notes"] + " | " if a["reviewer_notes"] else "") + \
                f"REOPENED 11-21 (Sinai): was {prev} by {who}; new type Hospitality — align or mint"
            a.update(decision="", org_type=HOSP, aligned_db_id="", reviewer="", reviewed_at="")
            log.append([now, reviewer, "org_review", "reopen", c, "decision=", f"was {prev} by {who}; Hospitality type now exists",
                        json.dumps({"source": SRC})])
            print(f"  reopen {c} {a['canonical_yiddish']}  (was {prev}/{who})")

    print(f"\n{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    _dump(HERE / "core_db.tsv", cf, core, ct)
    _dump(HERE / "org_addresses_review.tsv", df, cards, dt)
    _dump(HERE / "org_alignment_review.tsv", af, align, at)
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
