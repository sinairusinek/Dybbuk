"""Merge one people_db.tsv row into another (db-row-level person merge).

Follows the Kalman Juvelier precedent (1066 -> 2061, commit 4d99a7db9):
the SURVIVOR keeps its db_id, absorbs the loser's name variants, and records
`merged_from=<loser> (<reviewer> <date>)` in its `source` column; the loser row
is deleted. Every downstream reference must already point at the survivor --
this script refuses to run otherwise, since it does not rewrite references.

  python3.11 merge_db_rows.py --survivor 383 --loser 3492 --reviewer Sinai
  python3.11 merge_db_rows.py --survivor 383 --loser 3492 --reviewer Sinai --apply
"""
from __future__ import annotations

import argparse, csv, datetime, pathlib, re, sys

HERE = pathlib.Path(__file__).resolve().parent
DB = HERE / "people_db.tsv"
csv.field_size_limit(10**9)

# TSVs that may legitimately carry a db_id reference; checked for loser refs.
REF_GLOBS = ("*.tsv",)


def read_db() -> tuple[list[str], list[dict]]:
    with open(DB, encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        return list(rdr.fieldnames), list(rdr)


def line_terminator(path: pathlib.Path) -> str:
    """people_db.tsv is bare-LF; never silently convert a file's terminators."""
    raw = path.read_bytes()
    return "\r\n" if raw.count(b"\r\n") > (raw.count(b"\n") - raw.count(b"\r\n")) else "\n"


def find_refs(db_id: str, skip: set[str]) -> dict[str, int]:
    """Count rows in sibling TSVs that carry db_id in ANY column (exact cell match)."""
    hits: dict[str, int] = {}
    for g in REF_GLOBS:
        for p in sorted(HERE.glob(g)):
            if p.name in skip:
                continue
            n = 0
            try:
                with open(p, encoding="utf-8", newline="") as f:
                    for row in csv.reader(f, delimiter="\t"):
                        # exact cell match only: 383 must not match "3830" or a
                        # HUB-D383 substring, and must not match a person db id
                        # used as some other column's value by coincidence.
                        if any(c.strip() == db_id for c in row):
                            n += 1
            except (UnicodeDecodeError, csv.Error):
                continue
            if n:
                hits[p.name] = n
    return hits


def merge_variants(survivor: dict, loser: dict) -> tuple[str, list[str]]:
    """Union of name_variants + the loser's hebname/english, pipe-delimited.

    The loser's hebname is the point of the merge when it carries a birth name
    the survivor lacks (Sholem Rabinovitsh behind Sholem-Aleykhem).
    """
    have = [v.strip() for v in survivor["name_variants"].split("|") if v.strip()]
    known = {v for v in have}
    known |= {survivor["hebname"].strip(), survivor["english"].strip()}
    added = []
    for cand in (loser["hebname"], loser["english"], loser["alternative_name"],
                 *loser["name_variants"].split("|")):
        c = cand.strip()
        if c and c not in known:
            added.append(c); known.add(c); have.append(c)
    return "|".join(have), added


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--survivor", required=True)
    ap.add_argument("--loser", required=True)
    ap.add_argument("--reviewer", required=True, help="who decided this merge")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if a.survivor == a.loser:
        print("survivor and loser are the same id"); return 2

    fields, rows = read_db()
    by_id = {r["db_id"].strip(): r for r in rows}
    for which, i in (("survivor", a.survivor), ("loser", a.loser)):
        if i not in by_id:
            print(f"{which} db_id {i} not found in people_db.tsv"); return 2

    surv, lose = by_id[a.survivor], by_id[a.loser]

    # --- refuse if the loser is still referenced anywhere -------------------
    refs = find_refs(a.loser, skip={DB.name})
    print(f"loser {a.loser} external references: {refs or 'none'}")
    if refs:
        print("REFUSING: rewrite those references to the survivor first.")
        return 1

    # idempotence: a re-run must not re-stamp or re-append
    stamp = f"merged_from={a.loser}"
    if stamp in surv["source"]:
        print(f"already merged ({stamp} present in survivor source) — nothing to do")
        return 0

    new_variants, added = merge_variants(surv, lose)
    today = datetime.date.today().isoformat()
    new_source = f"{surv['source'].strip()} ; {stamp} ({a.reviewer} {today})".strip(" ;").strip()

    print(f"\nsurvivor {a.survivor}: {surv['hebname']} / {surv['english']}")
    print(f"loser    {a.loser}: {lose['hebname']} / {lose['english']}")
    print(f"\n  name_variants: {surv['name_variants']!r}\n              -> {new_variants!r}")
    print(f"     added: {added or 'nothing new'}")
    print(f"  source:        {surv['source']!r}\n              -> {new_source!r}")
    print(f"\n  delete row db_id={a.loser}")

    # fields the loser holds that the survivor does not -- surfaced, never
    # silently dropped: a merge must not lose data.
    lost = {k: lose[k].strip() for k in fields
            if k not in ("db_id", "raw", "source", "hebname", "english",
                         "alternative_name", "name_variants")
            and lose[k].strip() and not surv[k].strip()}
    if lost:
        print(f"\n  WARNING: loser-only field values that would be DROPPED: {lost}")
        print("  copy them onto the survivor by hand before applying.")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    surv["name_variants"] = new_variants
    surv["source"] = new_source
    out = [r for r in rows if r["db_id"].strip() != a.loser]
    assert len(out) == len(rows) - 1, "expected exactly one row removed"

    term = line_terminator(DB)
    tmp = DB.with_suffix(".tsv.tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator=term, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(out)
    tmp.replace(DB)   # atomic
    print(f"\nAPPLIED: {len(rows)} -> {len(out)} rows, terminator={term!r}")
    print("Commit people_db.tsv with the merge rationale in the message.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
