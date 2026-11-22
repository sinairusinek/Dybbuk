"""Movement parents get their movement's type (Sinai, 2026-11-22; Org-types Doc §B).

Sinai: "the parents should be their own movement type, not theater". Both are typed
the way CLASSIFICATION_POLICY.md and every other row of the same movement are typed
(Arbeter-ring London 2478, its branches; Poale Zion 2439, 2482, 2555, ...):

  189  Arbeter Ring (Workmen's Circle)  Musical organization -> Jewish political bodies
  362  Poale Zion                        Theatre              -> Jewish political bodies

The children were already linked and typed (apply_camps_hospitality_2026_11_21.py and
the amateur-hosts pass), and this script only checks them:
  189: camps 2075, 2086, 2087, 3795, 4989 (Education: children's camps)
  362: drama sections 2303, 2783 (Amateur, Rule 3)

Idempotent. Usage: python3.11 apply_movement_parents_2026_11_22.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, csv, datetime as _dt, json, sys
from pathlib import Path
from apply_amateur_hosts_2026_11_21 import _rw, _dump

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
POL = "Jewish political bodies"
RETYPE = {
    "189": "Arbeter Ring: workers' fraternal-political movement, the Arbeter-ring class in policy",
    "362": "Poale Zion: the Labour Zionist party",
}
CHILDREN = {"189": ({"2075", "2086", "2087", "3795", "4989"}, "Education"),
            "362": ({"2303", "2783"}, "Amateur")}


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, core, ct = _rw(HERE / "core_db.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    cards_by = {c["db_id"]: c for c in cards}
    log = []

    for d, why in RETYPE.items():
        r = by[d]
        if r["org_type"] != POL:
            log.append([now, reviewer, "org_addresses", "set_type", d, f"org_type={POL}",
                        f"was {r['org_type']}: {why}", json.dumps({"source": SRC})])
            print(f"  {d}: {r['org_type']} -> {POL}   {r['name']}")
            r["org_type"] = POL
        if d in cards_by:
            cards_by[d]["org_type"] = POL

    for p, (want, typ) in CHILDREN.items():
        have = {r["db_id"] for r in core if r["parent_db_id"] == p and r["deprecated"] != "true"}
        if have != want:
            print(f"  CHECK parent {p}: children {sorted(have)} != expected {sorted(want)}")
        for c in sorted(have):
            if by[c]["org_type"] != typ:
                print(f"  CHECK child {c} of {p}: typed {by[c]['org_type']!r}, expected {typ}")
    print(f"\n{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    _dump(HERE / "core_db.tsv", cf, core, ct)
    _dump(HERE / "org_addresses_review.tsv", df, cards, dt)
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
