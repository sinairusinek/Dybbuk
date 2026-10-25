"""Split the Hebrew-stylistics project's rows out of people_db.tsv.

people_db.tsv has another project's person table concatenated into it: medieval
and modern Hebrew-literature figures (Ibn Ezra, Mapu, Nachman of Breslov, Ruth
Almog, Abba Kovner) that were never Zylbercweig Leksikon people. They are inert
-- nothing in the corpus references them -- but they look like people to every
matcher and candidate generator that reads people_db.tsv.

THE RULE: a row is foreign when `source` is exactly "DBKey" (not "DBKey+Report")
AND no person_hub claims its db_id.

Both halves are required, and id range is NOT the rule:
  * `source=DBKey` alone would take db 383 (Sholem Aleichem) and 569 (Gabriel
    Rozenthal) -- both are real Leksikon people an RA aligned to entries, which
    is exactly what hub membership records.
  * An id-band rule would take db 522 (Mendele Mokher Sefarim, id < 750 but
    carrying a real Zylbercweig citation) and would miss nothing extra, since
    the foreign rows span 1-692 in three separate clumps rather than one block.
  * `external_source` being empty is NOT the signature: 843 rows lack it,
    including ~360 genuine Yiddish theatre people in the 3250-3999 range.

Rows are MOVED, never deleted: they are written to people_db_hebrew_stylistics.tsv
(same columns) for the Hebrew-stylistics project, which has no repo yet.

  python3.11 extract_foreign_people.py
  python3.11 extract_foreign_people.py --apply --reviewer Sinai
"""
from __future__ import annotations

import argparse, csv, datetime, pathlib, sys

HERE = pathlib.Path(__file__).resolve().parent
DB = HERE / "people_db.tsv"
OUT = HERE / "people_db_hebrew_stylistics.tsv"
HUB = HERE / "person_hub.tsv"
csv.field_size_limit(10**9)

FOREIGN_SOURCE = "DBKey"          # exactly this, never "DBKey+Report"


def terminator(path: pathlib.Path) -> str:
    raw = path.read_bytes()
    crlf = raw.count(b"\r\n")
    return "\r\n" if crlf > (raw.count(b"\n") - crlf) else "\n"


def hub_claimed_db_ids() -> set[str]:
    out: set[str] = set()
    with open(HUB, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            for d in (r.get("db_ids") or "").split("|"):
                if d.strip():
                    out.add(d.strip())
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", help="required with --apply")
    a = ap.parse_args()
    if a.apply and not a.reviewer:
        print("--reviewer is required with --apply"); return 2

    with open(DB, encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        fields, rows = list(rdr.fieldnames), list(rdr)

    claimed = hub_claimed_db_ids()
    foreign, keep = [], []
    for r in rows:
        if r["source"].strip() == FOREIGN_SOURCE and r["db_id"].strip() not in claimed:
            foreign.append(r)
        else:
            keep.append(r)

    print(f"people_db.tsv            {len(rows)} rows")
    print(f"  -> Zylbercweig         {len(keep)}")
    print(f"  -> Hebrew stylistics   {len(foreign)}")

    # Rows the rule spares BECAUSE a hub claims them: the whole point of the
    # second clause. Print them so the exemption is auditable, not implicit.
    spared = [r for r in rows
              if r["source"].strip() == FOREIGN_SOURCE and r["db_id"].strip() in claimed]
    print(f"\nspared by hub membership ({len(spared)}):")
    for r in spared:
        print(f"  {r['db_id']:>5} {r['hebname'][:30]:30} {r['english'][:26]}")

    # Guard: nothing being moved may carry Zylbercweig evidence.
    bad = [r for r in foreign if r["external_source"].strip()
           or r["mentioned_in"].strip() or r["professional_role_perf"].strip()
           or r["character_perf"].strip() or r["author_of"].strip()]
    if bad:
        print(f"\nREFUSING: {len(bad)} rows carry Zylbercweig evidence "
              f"(e.g. db {bad[0]['db_id']}). Review before moving.")
        return 1

    if foreign:
        ids = sorted(int(r["db_id"]) for r in foreign if r["db_id"].strip().isdigit())
        print(f"\nmoved id range: {ids[0]}-{ids[-1]}")
        print("sample:")
        for r in foreign[:6]:
            print(f"  {r['db_id']:>5} {r['hebname'][:32]:32} {r['english'][:28]}")

    if not a.apply:
        print("\n(dry run — pass --apply --reviewer NAME to write)")
        return 0

    today = datetime.date.today().isoformat()
    note = f"moved_from=people_db.tsv ({a.reviewer} {today})"
    for r in foreign:
        r["source"] = f"{r['source'].strip()} ; {note}"

    term = terminator(DB)
    for path, data in ((OUT, foreign), (DB, keep)):
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                               lineterminator=term, quoting=csv.QUOTE_MINIMAL)
            w.writeheader(); w.writerows(data)
        tmp.replace(path)
    print(f"\nAPPLIED: wrote {len(foreign)} rows to {OUT.name}, "
          f"{len(keep)} remain in {DB.name} (terminator={term!r})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
