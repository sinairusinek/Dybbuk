"""Org-type audit fixes (Sinai approved 2026-11-21).

Reads type_audit_2026_11_21.tsv (the audit list: name cues checked against
org_type, each conflict read against its mention sentences) and applies:
  RETYPE   org_type in core_db and on the address card
  RENAME   2179: name_yiddish said ווילנער שטאָט-טעאַטער; the row is Julius
           Halle's drama courses in Vienna
  PARENT   4482 -> 4939, 3193 -> 3051, 4891 -> 4641
Merges (4840->4781, 1246->1239, 3459->1902, 3194->3193) are done afterwards by
apply_dup_merges_2026_11_21.py.

Idempotent. Writes core_db.tsv, org_addresses_review.tsv, activity_log.tsv.
Usage: python3 apply_type_audit_2026_11_21.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import sys
from pathlib import Path

from apply_amateur_hosts_2026_11_21 import _dump, _rw

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
VALID = {"Theatre education", "Musical organization", "Religious institutions/organizations",
         "Cultural/workers' club", "Amateur", "Journals/ Newspapers", "Media (Radio/ Film/TV)",
         "Education", "Library", "Theatre", "Circus", "Publisher", "Jewish political bodies"}
RENAME = {"2179": "דראַמאַטישע קורסן ביי יוליוס האַללע (ווין)"}
PARENT = {"4482": ("4939", "children's theatre at the Jewish Sport Center, Mexico City"),
          "3193": ("3051", "drama section of the world federation of Bergen-Belsen survivors"),
          "4891": ("4641", "choir of the Tłomackie synagogue")}


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, core, ct = _rw(HERE / "core_db.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    cby = {c["db_id"]: c for c in cards}
    with (HERE / "type_audit_2026_11_21.tsv").open(encoding="utf-8") as f:
        audit = list(csv.DictReader(f, delimiter="\t"))
    log = []
    for a in audit:
        d, new = a["db_id"], a["proposed"].split(" + ")[0].strip()
        if new.startswith("merge"):
            new = None  # loser: type is moot
        if new is None:
            continue
        assert new in VALID, (d, new)
        r = by[d]
        if r["org_type"] != new:
            if r["org_type"] != a["current_type"]:
                print(f"  SKIP {d}: type is now {r['org_type']!r}, audit saw {a['current_type']!r}")
                continue
            log.append([now, reviewer, "org_addresses", "set_org_type", d, new,
                        f"type audit 11-21: {a['why']}",
                        json.dumps({"from": r["org_type"], "source": SRC}, ensure_ascii=False)])
            print(f"  {d}: {r['org_type']!r} -> {new!r}")
            r["org_type"] = new
        c = cby.get(d)
        if c is not None and c.get("org_type") != new:
            c["org_type"] = new
    for d, yi in RENAME.items():
        r = by[d]
        if r["name_yiddish"] != yi:
            log.append([now, reviewer, "org_addresses", "rename", d, "name_yiddish",
                        f"was {r['name_yiddish']!r}: the row is Julius Halle's drama courses, Vienna",
                        json.dumps({"source": SRC})])
            if r["name_yiddish"] and r["name_yiddish"] not in r.get("name_variants", ""):
                pass  # the old name is wrong, not a variant: not kept
            r["name_yiddish"] = yi
            if d in cby:
                cby[d]["canonical_yiddish"] = yi
            print(f"  {d}: name_yiddish -> {yi}")
    for d, (p, why) in PARENT.items():
        r = by[d]
        if r.get("parent_db_id") != p:
            assert not r.get("parent_db_id"), d
            r["parent_db_id"] = p
            log.append([now, reviewer, "org_addresses", "set_parent", d, f"parent_db_id={p}",
                        f"type audit 11-21: {why}", json.dumps({"source": SRC})])
            print(f"  {d}: parent -> {p}")
        if d in cby:
            cby[d]["parent_db_id"] = p
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
