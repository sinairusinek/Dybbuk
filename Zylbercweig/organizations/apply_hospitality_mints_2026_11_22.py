"""Hospitality candidates: mint the rest, unmint Unzer Kemp (Sinai, 2026-11-22).

The 11 hotel/cafe/restaurant clusters reopened on 11-21 (apply_camps_hospitality_2026_11_21.py)
each become their own Hospitality entity. Sinai had already minted three in the app:
  C01185 -> 4990, C01804 -> 4992  (alignment + core agree; left as they are)
  C01565 -> 4991                  (core row exists, but a later app save dropped the
                                   alignment decision; re-pointed, not re-minted)
The other 8 are minted here from origin's high-water mark.

Unzer Kemp (cluster ORG-C01414, db 4946, minted 11-21) is unminted and left undecided:
  db 4946 deprecated and unlinked; the cluster's decision cleared, its Hospitality pre-type kept;
  Camp Kinder-Velt 4297 loses its parent 4946 (it was set only because of Unzer Kemp).

Idempotent. Run sync_address_cards.py afterwards for the new cards.
Usage: python3.11 apply_hospitality_mints_2026_11_22.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, csv, datetime as _dt, json, sys
from pathlib import Path
from apply_publisher_audit_mints import origin_high_water
from apply_amateur_hosts_2026_11_21 import _rw, _dump

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
HOSP = "Hospitality"
CLUSTERS = ["ORG-C01185", "ORG-C01565", "ORG-C01804", "ORG-C01826", "ORG-C02081", "ORG-C02428",
            "ORG-C04985", "ORG-C05339", "ORG-C05916", "ORG-C06042", "ORG-C07046"]
UNZER_CLUSTER, UNZER_DB, UNZER_CHILD = "ORG-C01414", "4946", "4297"


def pipes(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, core, ct = _rw(HERE / "core_db.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    af, align, at = _rw(HERE / "org_alignment_review.tsv")
    by = {r["db_id"]: r for r in core}
    cards_by = {c["db_id"]: c for c in cards}
    al_by = {r["cluster_id"]: r for r in align}
    log = []
    next_id = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1

    def note(a, text):
        prev = (a.get("reviewer_notes") or "").strip()
        a["reviewer_notes"] = (prev + " | " if prev else "") + f"11-22 Sinai: {text}"

    for c in CLUSTERS:
        a = al_by[c]
        live = [r for r in core if c in pipes(r["linked_cluster_ids"]) and r["deprecated"].strip() != "true"]
        if len(live) > 1:
            print(f"  ERROR {c}: linked from several live rows {[r['db_id'] for r in live]}"); continue
        if live:
            d = live[0]["db_id"]
        else:
            d = str(next_id); next_id += 1
            row = {f: "" for f in cf}
            row.update(db_id=d, name=a["canonical_yiddish"], name_yiddish=a["canonical_yiddish"], org_type=HOSP,
                       address=pipes(a["extracted_addresses"])[0] if pipes(a["extracted_addresses"]) else "",
                       linked_cluster_ids=c)
            core.append(row); by[d] = row
            print(f"  mint db {d}  {a['canonical_yiddish']}  ({a['extracted_settlements']})")
        if by[d]["org_type"] != HOSP:
            print(f"  NOTE {d} typed {by[d]['org_type']!r}, set to {HOSP}"); by[d]["org_type"] = HOSP
        if (a["decision"], a["aligned_db_id"]) != ("NEW", d):
            was = (a["decision"], a["aligned_db_id"])
            a.update(decision="NEW", aligned_db_id=d, org_type=HOSP, reviewer=reviewer, reviewed_at=now)
            note(a, f"minted db {d} (Hospitality)" + (" — app mint whose alignment save was lost" if live and not was[0] else ""))
            log.append([now, reviewer, "org_review", "alignment", c, "NEW",
                        f"Hospitality candidates 11-22: own entity, db {d}",
                        json.dumps({"aligned_db_id": d, "was": list(was), "source": SRC}, ensure_ascii=False)])
            if not live or was[0] != "NEW":
                print(f"  {c} -> NEW {d}   {a['canonical_yiddish']}   (was {was})")

    # ---- unmint Unzer Kemp
    u = by[UNZER_DB]
    if u["deprecated"].strip() != "true":
        u["deprecated"] = "true"
        u["linked_cluster_ids"] = " | ".join(x for x in pipes(u["linked_cluster_ids"]) if x != UNZER_CLUSTER)
        log.append([now, reviewer, "org_review", "unmint", UNZER_DB, "deprecated",
                    "Unzer Kemp unminted (Sinai 11-22); cluster ORG-C01414 left undecided",
                    json.dumps({"cluster": UNZER_CLUSTER, "source": SRC})])
        print(f"  deprecate db {UNZER_DB} Unzer Kemp")
    a = al_by[UNZER_CLUSTER]
    if a["decision"] or a["aligned_db_id"]:
        was = (a["decision"], a["aligned_db_id"], a["reviewer"])
        note(a, f"UNMINTED (was {was[0]} db {was[1]} by {was[2]}); left undecided")
        a.update(decision="", aligned_db_id="", reviewer="", reviewed_at="")
        log.append([now, reviewer, "org_review", "reopen", UNZER_CLUSTER, "decision=",
                    f"was {was[0]} db {was[1]}: Unzer Kemp unminted, left undecided",
                    json.dumps({"was": list(was), "source": SRC}, ensure_ascii=False)])
        print(f"  reopen {UNZER_CLUSTER} (was {was[0]} {was[1]})")
    ch = by[UNZER_CHILD]
    if ch["parent_db_id"] == UNZER_DB:
        ch["parent_db_id"] = ""
        if UNZER_CHILD in cards_by:
            cards_by[UNZER_CHILD]["parent_db_id"] = ""
        log.append([now, reviewer, "org_addresses", "set_parent", UNZER_CHILD, "parent_db_id=",
                    "parent Unzer Kemp 4946 unminted", json.dumps({"source": SRC})])
        print(f"  {UNZER_CHILD} {ch['name']}: parent 4946 cleared")

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
