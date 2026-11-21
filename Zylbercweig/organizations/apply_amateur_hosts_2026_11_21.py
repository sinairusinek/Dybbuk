"""Host organizations for amateur drama sections (Sinai, 2026-11-21).

SCHEMA_DRAFT §6b: an Amateur row that is a drama section / circle OF a host
organization gets parent_db_id = the host. Sinai: "host-organizations that do not
exist yet: create them now."

  LINK  six sections whose host is already in core_db
  MINT  thirteen hosts that are not in core_db, each then parent of its section
  skip  2784 (Częstochowa 1909) and 3510 (Ashgabat military circle): no host is named

Hosts are minted without clusters: they occur only inside the section's name.
Their city goes in `address`, as on 4750 etc. Ids come from origin's high-water mark
(never hand-assigned). Idempotent. Writes core_db.tsv, org_addresses_review.tsv
(cards) and activity_log.tsv. Run sync_address_cards.py afterwards to create the new
hosts' cards, then this script again to stamp the parents on the cards.

Usage: python3 apply_amateur_hosts_2026_11_21.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import sys
from pathlib import Path

from apply_publisher_audit_mints import origin_high_water

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name

LINK = {  # section db -> (existing host db, why)
    "2297": ("2999", "Riga Arbeter-heym (the section's mention is in Riga, 1920)"),
    "2307": ("698", "Hashomer Hatsair (Chełm): the mention is in Chełm, 1920"),
    "2303": ("362", "Poale Zion; the section is in Łowicz. Same convention as Bund sections → 997"),
    "2783": ("362", "Poale Zion (the club of comrades of Poale Zion; no city stated)"),
    "2296": ("3180", "the Ashkenazi kehile in Mexico (the mention is in Mexico, 1960)"),
    "2279": ("2602", "Groser-klub; the section is at the Radom Groser-klub 'Arbeter-vinkl'"),
}
MINT = [  # (section db, host name_yiddish, English name, org_type, address, why)
    ("916", "קולטור-ליגע (פּאַריז)", "Kultur-lige (Paris)", "Cultural/workers' club", "פּאַריז",
     "host of the Paris drama circle of the Kultur-lige"),
    ("2314", "קולטור-ליגע (יאַס)", "Kultur-lige (Iași)", "Cultural/workers' club", "יאַס",
     "host of the Iași Kultur-lige drama circle"),
    ("1119", "קלוב „רויטער בויער“ (מינסק)", "Club 'Royter Boyer' (Minsk)", "Cultural/workers' club", "מינסק",
     "host of the Minsk club's drama circle"),
    ("2280", "ביבליאָטעק (גאָסטינין)", "Library (Gostynin)", "Library", "גאָסטינין",
     "host of the Gostynin library drama circle (Adam Domb, 1915)"),
    ("2300", "אַרבעטער-רינג (לאָנדאָן)", "Workers' Circle (London)", "Welfare/Aid organization", "לאָנדאָן",
     "host of the London Arbeter-ring drama circle; the London friendly society, not the US Workmen's Circle (189)"),
    ("2312", "קאָאָפּעראַטיווע קאָלאָניע (ניו יאָרק)", "Cooperative Colony (New York)", "Cultural/workers' club", "ניו יאָרק",
     "host of the Cooperative Colony drama section (New York, 1929)"),
    ("2785", "ספּאָרט-צענטער (מעקסיקע)", "Sport Center (Mexico)", "Sports/Recreation", "מעקסיקע",
     "host of the Yiddish theatre group at the Sport Center (Mexico, 1956)"),
    ("2811", "יידישער אַרבעטער קולטור קלוב (טאָראָנטאָ)", "Yiddish Workers' Culture Club (Toronto)", "Cultural/workers' club", "טאָראָנטאָ",
     "host of the drama circle (Toronto, 1931)"),
    ("2812", "יידישער אַקאַדעמישער קולטור-פֿאַראיין (לייפּציג)", "Jewish Academic Cultural Society (Leipzig)", "Cultural/workers' club", "לייפּציג",
     "host of the Leipzig drama circle (Lubotski)"),
    ("3845", "ברוקלין קאָלעדזש", "Brooklyn College", "Education", "ניו יאָרק",
     "host of the Brooklyn College theatre group (1967)"),
    ("4360", "צענטראַלער יידישער קולטור-הויז (טיפליס)", "Central Yiddish Culture House (Tiflis)", "Cultural/workers' club", "טיפליס",
     "host of the workers' studio in Tiflis"),
    ("4616", "האַנטווערקער-פֿאַראיין (טשענסטאָכאָוו)", "Craftsmen's Union (Częstochowa)", "Trade Union / Professional Association", "טשענסטאָכאָוו",
     "host of the craftsmen's (mostly tailors') drama circle"),
    ("4677", "אַרבעטער-היים (וואַרשע)", "Arbeter-heym (Warsaw)", "Cultural/workers' club", "וואַרשע",
     "host of the Warsaw Arbeter-heym drama section"),
]


def _rw(p: Path):
    term = "\r\n" if b"\r\n" in p.read_bytes()[:4096] else "\n"
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd), term


def _dump(p: Path, fields, rows, term) -> None:
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator=term)
        w.writeheader()
        w.writerows(rows)


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, core, ct = _rw(HERE / "core_db.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    by_name = {r["name_yiddish"]: r for r in core if r.get("name_yiddish")}
    nxt = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    log, parent_of = [], {}

    for sec, (host, why) in LINK.items():
        assert by[sec]["org_type"] == "Amateur", sec
        parent_of[sec] = (host, why)

    for sec, yi, en, typ, addr, why in MINT:
        assert by[sec]["org_type"] == "Amateur", sec
        if yi in by_name:
            host = by_name[yi]["db_id"]
            print(f"  exists db {host}  {yi}")
        else:
            host = str(nxt); nxt += 1
            row = {f: "" for f in cf}
            row.update(db_id=host, name=en, name_yiddish=yi, org_type=typ, address=addr)
            core.append(row); by[host] = row; by_name[yi] = row
            log.append([now, reviewer, "org_addresses", "mint", host, "NEW", f"{yi} — {why}",
                        json.dumps({"org_type": typ, "address": addr, "child_db_id": sec, "source": SRC},
                                   ensure_ascii=False)])
            print(f"  mint db {host}  {yi}  [{typ}]  (parent of {sec})")
        parent_of[sec] = (host, why)

    for sec, (host, why) in parent_of.items():
        r = by[sec]
        if r.get("parent_db_id") != host:
            if r.get("parent_db_id"):
                print(f"  SKIP {sec}: already has parent {r['parent_db_id']}"); continue
            r["parent_db_id"] = host
            log.append([now, reviewer, "org_addresses", "set_parent", sec, f"parent_db_id={host}",
                        f"amateur drama section of its host (§6b): {why}", json.dumps({"source": SRC})])
            print(f"  core_db {sec}: parent_db_id -> {host}")
    nc = 0
    for c in cards:
        if c["db_id"] in parent_of and c.get("parent_db_id") != parent_of[c["db_id"]][0]:
            c["parent_db_id"] = parent_of[c["db_id"]][0]; nc += 1
    print(f"  cards stamped: {nc}")

    print(f"\n{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME to write"); return
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
