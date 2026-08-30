"""Ingest Ruthie's edited troupe-tag Google Sheet back into troupe_tags.tsv.

Counterpart to build_troupe_tag_sheet.py. She works in the sheet
"Troupe tags review <date>"; this reads the exported .xlsx and folds her
decisions into troupe_tags.tsv + troupe_tag_review.tsv, keyed on db_id.

IMPORTANT — how she actually uses the sheet (observed 2026-08-27):
she edits tags IN PLACE in the `draft_tags` column and sets status=reviewed.
The `tags` column that build_troupe_tag_sheet.py emitted is gone from her
copy, and she leaves `reviewer`/`reviewed_at` blank on new rows. So the
authoritative tag value is `draft_tags` on any row with status=reviewed,
NOT a separate `tags` column.

Re-mirror troupe_tags.tsv / troupe_tag_review.tsv from the zalmen-data branch
BEFORE running (main's copies are a frozen mirror):

    git fetch origin zalmen-data
    git checkout FETCH_HEAD -- Zylbercweig/organizations/troupe_tags.tsv \\
                               Zylbercweig/organizations/troupe_tag_review.tsv

Then:

    python3.11 Zylbercweig/organizations/ingest_troupe_tag_sheet.py <sheet.xlsx>          # dry run
    python3.11 Zylbercweig/organizations/ingest_troupe_tag_sheet.py <sheet.xlsx> --execute
"""
from __future__ import annotations
import argparse, csv, datetime as dt, pathlib, re, sys, collections

csv.field_size_limit(10**9)
ORG = pathlib.Path(__file__).resolve().parent
TAGS = ORG / "troupe_tags.tsv"
REVIEW = ORG / "troupe_tag_review.tsv"
SEP = " | "

# The closed 16-tag vocabulary (2026-08-19). Do not extend without Ruthie.
VOCAB = [
    "Family Company", "Impresario Company", "Star Company", "Ensemble Company",
    "Cooperative Company", "Institutional Company", "Ad Hoc Company",
    "Children's Company", "Operetta / Opera Company", "German-Jewish Company",
    "Amateur Company", "Kleinkunst / Revue / Cabaret Company",
    "Marionette / Puppet Company", "Non-Jewish Company",
    "Hebrew-Language Company", "Not a Troupe",
]
# Free-typed spellings seen in her sheet -> canonical vocabulary term.
ALIASES = {
    "opera/operetta company": "Operetta / Opera Company",
    "opera/ operetta company": "Operetta / Opera Company",
    "opera/operetta": "Operetta / Opera Company",
    "opera/opertta company": "Operetta / Opera Company",
    "operetta/opera company": "Operetta / Opera Company",
    "ensambel company": "Ensemble Company",
    "ensamble company": "Ensemble Company",
    "star troupe": "Star Company",
    "ad-hoc company": "Ad Hoc Company",
}
_CANON = {t.lower(): t for t in VOCAB}


def canon_tag(raw: str) -> tuple[str | None, str | None]:
    """-> (canonical tag, None) or (None, the unmappable string)."""
    s = " ".join(raw.split()).strip(" .;")
    if not s:
        return None, None
    k = s.lower()
    if k in _CANON:
        return _CANON[k], None
    if k in ALIASES:
        return ALIASES[k], None
    return None, s


def split_tags(cell: str) -> tuple[list[str], list[str]]:
    """Split on | or · (she uses both). -> (canonical tags, unmapped strings)."""
    good, bad = [], []
    for part in re.split(r"[|·]", cell or ""):
        tag, err = canon_tag(part)
        if tag and tag not in good:
            good.append(tag)
        elif err:
            bad.append(err)
    return good, bad


def read_tsv(p: pathlib.Path) -> tuple[list[str], dict[str, dict]]:
    with open(p, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), {row["db_id"]: row for row in r}


def write_tsv(p: pathlib.Path, fields: list[str], rows: dict[str, dict]) -> None:
    tmp = p.with_suffix(p.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for db in sorted(rows, key=lambda x: int(x)):
            w.writerow(rows[db])
    tmp.replace(p)  # atomic


def load_sheet(path: pathlib.Path) -> list[dict]:
    try:
        import openpyxl
    except ImportError:
        sys.exit("need openpyxl:  python3.11 -m pip install openpyxl")
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True).worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    hi = next((i for i, r in enumerate(rows[:20])
               if r and str(r[0] or "").strip() == "db_id"), None)
    if hi is None:
        sys.exit(f"{path.name}: no header row starting with db_id in the first 20 rows")
    hdr = [str(c).strip() if c else "" for c in rows[hi]]
    idx = {h: i for i, h in enumerate(hdr) if h}
    out = []
    for r in rows[hi + 1:]:
        if not r or r[0] in (None, ""):
            continue
        get = lambda h: (str(r[idx[h]]).strip()
                         if h in idx and idx[h] < len(r) and r[idx[h]] is not None else "")
        try:  # openpyxl gives numeric ids back as "35.0"
            db = str(int(float(get("db_id"))))
        except ValueError:
            continue
        out.append({"db_id": db, "status": get("status").lower(),
                    "draft_tags": get("draft_tags"), "tags": get("tags"),
                    "comment": get("comment"), "reviewer": get("reviewer"),
                    "reviewed_at": get("reviewed_at"), "name": get("name")})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sheet", type=pathlib.Path, help="exported .xlsx of her sheet")
    ap.add_argument("--execute", action="store_true", help="write (default: dry run)")
    ap.add_argument("--reviewer", default="Ruthie", help="reviewer for rows with none")
    args = ap.parse_args()

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sheet = load_sheet(args.sheet)
    tfields, tags = read_tsv(TAGS)
    rfields, review = read_tsv(REVIEW)

    added = changed = unchanged = skipped_blank = 0
    unmapped: collections.Counter = collections.Counter()
    changes, blanks = [], []

    for row in sheet:
        if row["status"] != "reviewed":
            continue
        db = row["db_id"]
        # `tags` wins if her copy still has that column; otherwise draft_tags.
        good, bad = split_tags(row["tags"] or row["draft_tags"])
        for b in bad:
            unmapped[b] += 1
        if bad and not good:
            continue  # nothing usable — don't overwrite a good existing value
        if not good and not row["comment"]:
            # reviewed, no tags, no comment: almost certainly an unfilled row.
            skipped_blank += 1
            blanks.append((db, row["name"][:40]))
            continue

        val = SEP.join(good)
        prev = tags.get(db)
        prev_set = tuple(sorted(split_tags(prev["tags"])[0])) if prev else None
        if prev is None:
            added += 1
            changes.append(("ADD", db, row["name"][:34], "", val))
        elif prev_set != tuple(sorted(good)):
            changed += 1
            changes.append(("CHG", db, row["name"][:34], prev["tags"], val))
        else:
            unchanged += 1
            # keep going: comment/reviewer may still need refreshing

        rec = prev or {f: "" for f in tfields}
        rec["db_id"] = db
        rec["tags"] = val
        if row["comment"]:
            rec["comment"] = row["comment"]
        rec["reviewer_notes"] = "from sheet ingest"
        rec["reviewer"] = row["reviewer"] or (prev or {}).get("reviewer") or args.reviewer
        rec["reviewed_at"] = row["reviewed_at"] or (prev or {}).get("reviewed_at") or stamp
        tags[db] = rec

        rrec = review.get(db) or {f: "" for f in rfields}
        rrec.update(db_id=db, status="reviewed", final_tags=val,
                    reviewer=rec["reviewer"], reviewed_at=rec["reviewed_at"])
        review[db] = rrec

    print(f"sheet:            {len(sheet)} rows, "
          f"{sum(1 for r in sheet if r['status'] == 'reviewed')} reviewed")
    print(f"troupe_tags.tsv:  {len(tags)} rows after ingest "
          f"(was {len(read_tsv(TAGS)[1])})")
    print(f"  added   {added}\n  changed {changed}\n  same    {unchanged}")
    if skipped_blank:
        print(f"  skipped {skipped_blank} reviewed-but-empty rows (no tags, no comment)")
        for db, nm in blanks[:8]:
            print(f"      db{db:<5} {nm}")
        if len(blanks) > 8:
            print(f"      ... and {len(blanks) - 8} more")
    if unmapped:
        print("\n  UNMAPPED tag strings (add to ALIASES or ask her):")
        for k, v in unmapped.most_common():
            print(f"      {v:3d}  {k!r}")
    print("\n  first changes:")
    for kind, db, nm, old, new in changes[:12]:
        print(f"      {kind} db{db:<5} {nm:<34} {old or '—'!s:<38} -> {new}")
    if len(changes) > 12:
        print(f"      ... and {len(changes) - 12} more")

    if not args.execute:
        print("\nDRY RUN — nothing written. Re-run with --execute.")
        return
    write_tsv(TAGS, tfields, tags)
    write_tsv(REVIEW, rfields, review)
    print(f"\nwrote {TAGS.name} and {REVIEW.name}")


if __name__ == "__main__":
    main()
