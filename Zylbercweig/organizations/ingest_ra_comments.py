#!/usr/bin/env python3
"""Harvest the RA's free-text comments column into a reviewable ledger.

The workbook asks for a person from a dropdown. When the dropdown holds nobody
who fits -- the span names someone with no entry in the lexicon -- Ruthie wrote
the name in a comments column instead, in Yiddish or in Latin transcription
("Leo Largman", "חיים סאנדלער Chaim Sandler"). That is an ANSWER, and the
ingest that reads `person_name` never saw it: 19 of her 22 identifications sat
in a column no script read, and the next workbook build asked them again.

Three kinds of comment, kept apart because they need different work:

  resolved     the name resolves to exactly one hub on surname AND given name
               -> a proposal, same standing as any other; still hers to confirm.
  attested     no hub, but the judge independently READ the same name from the
               same sentences and the corpus has it as a mention. Two readers
               agreeing on a person the hub table lacks -> a people-side mint.
  unresolved   no hub and no corroboration yet. Recorded so it stops being
               re-asked, and so the name is not lost.

NOT an eponym decision and NOT an edge: these name people who mostly have no
hub_id to point an edge at. This file is the record that the question was
answered, and the queue for whoever mints the missing people.

`not relevant` / `garbage` comments mark a span that is not a name at all
(מיט = "with", ערשטער = "first"); they become REJECT rows in the eponym
decisions ledger via ingest_eponym_ra_workbook.py, not rows here.

  python3 ingest_ra_comments.py <filled.xlsx> [--reviewer NAME]   # dry run
  python3 ingest_ra_comments.py <filled.xlsx> --execute --reviewer Ruthie
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib

from openpyxl import load_workbook

from build_eponym_candidates import HEB, key, load_surname_index

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
ORGS = BASE / "organizations"
RECALL = ORGS / "eponym_recall_candidates.tsv"
DECISIONS = ORGS / "eponym_decisions.tsv"
OUT = ORGS / "ra_comment_identifications.tsv"

# A comment that rejects the span rather than naming anyone.
NOT_A_NAME = {"not relevant", "irrelevant", "garbage", "none", "-", "n/a"}

FIELDS = [
    "outcome", "db_id", "span", "comment", "org_name",
    "hub_id", "person_heading", "person_db_id",
    "judge_outcome", "judge_read_name", "n_mentions",
    "reviewer", "reviewed_at", "notes",
]


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    return list(csv.DictReader(p.open(encoding="utf-8"), delimiter="\t"))


def resolve(name: str, by_full: dict) -> list[dict]:
    """Hubs matching the comment on surname AND given name together.

    Never on the surname alone: `ליאַ לאָרגמאַן` matches `ברין, אַליאַ` and
    `זיגמונד טורקאָוו` matches `זיגמונד שוואַרץ` -- different people. A Latin
    transcription yields no Hebrew tokens and so resolves to nothing, which is
    `unresolved`, not a claim that the person is absent.
    """
    toks = [t for t in HEB.findall(key(name)) if len(t) > 1]
    for i, a in enumerate(toks):
        for j, b in enumerate(toks):
            if i != j and (a, b) in by_full:
                return by_full[(a, b)]
    return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workbook")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--reviewer", default="")
    ap.add_argument("-o", "--out", default=str(OUT))
    args = ap.parse_args()

    ws = load_workbook(args.workbook, data_only=True)["troupes"]
    head = [c.value for c in ws[1]]
    col = {name: i for i, name in enumerate(head)}
    cmt_col = next((h for h in head if h and "comment" in str(h).lower()), None)
    if not cmt_col:
        raise SystemExit("no comments column on the 'troupes' sheet")

    _by_surname, by_full = load_surname_index()
    recall = {(r["db_id"], r["span"]): r for r in read_tsv(RECALL)}
    existing = read_tsv(pathlib.Path(args.out))
    have = {(r["db_id"], r["span"]) for r in existing}
    # A span she ALSO answered in person_name is already a decision; its comment
    # is a note on that answer ("יידל גאלדפאדן" beside the picked hub), not an
    # unanswered question. Keeping it here would re-pose a closed case.
    answered = {(d["db_id"], d["span"]) for d in read_tsv(DECISIONS)
                if (d.get("decision") or "").upper() == "CONFIRM"}

    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows, skipped_not_a_name, skipped_dupe, skipped_answered = [], 0, 0, 0
    seen_names: set[tuple] = set()

    for r in range(2, ws.max_row + 1):
        comment = str(ws.cell(r, col[cmt_col] + 1).value or "").strip()
        if not comment:
            continue
        if comment.lower() in NOT_A_NAME:
            skipped_not_a_name += 1
            continue
        db_id = str(ws.cell(r, col["db_id"] + 1).value or "").strip()
        span = str(ws.cell(r, col["name in troupe"] + 1).value or "").strip()
        if (db_id, span) in have:
            skipped_dupe += 1
            continue
        if (db_id, span) in answered:
            skipped_answered += 1
            continue
        # One org, one name, several spans: db864 wrote "אידא קאמינסקי" on both
        # אידא and קאמינסקי. That is one identification, not two.
        if (db_id, comment) in seen_names:
            skipped_dupe += 1
            continue
        seen_names.add((db_id, comment))

        hubs = resolve(comment, by_full)
        rec = recall.get((db_id, span)) or {}
        n_men = (rec.get("n_mentions") or "").strip()

        if len(hubs) == 1:
            outcome = "resolved"
        elif hubs:
            outcome = "ambiguous"
        elif rec.get("outcome") == "attested" or (n_men.isdigit() and int(n_men) > 0):
            outcome = "attested"
        else:
            outcome = "unresolved"

        h = hubs[0] if len(hubs) == 1 else {}
        rows.append({
            "outcome": outcome,
            "db_id": db_id,
            "span": span,
            "comment": comment,
            "org_name": str(ws.cell(r, col["troupe (Yiddish)"] + 1).value or "").strip(),
            "hub_id": h.get("hub_id", ""),
            "person_heading": h.get("canonical_heading", ""),
            "person_db_id": (h.get("db_ids", "") or "").split(",")[0].strip(),
            "judge_outcome": rec.get("outcome", ""),
            "judge_read_name": rec.get("name_read_by_judge", ""),
            "n_mentions": n_men,
            "reviewer": args.reviewer,
            "reviewed_at": now,
            "notes": "",
        })

    order = {"resolved": 0, "attested": 1, "ambiguous": 2, "unresolved": 3}
    rows.sort(key=lambda d: (order[d["outcome"]], int(d["db_id"])))

    total = len(rows) + skipped_not_a_name + skipped_dupe + skipped_answered
    print(f"comments found:        {total}")
    print(f"  not-a-name (skipped): {skipped_not_a_name}")
    print(f"  already decided     : {skipped_answered}")
    print(f"  dupe / recorded     : {skipped_dupe}")
    print(f"  new rows            : {len(rows)}")
    for k in ("resolved", "attested", "ambiguous", "unresolved"):
        n = sum(1 for d in rows if d["outcome"] == k)
        if n:
            print(f"     {k:11s} {n}")
    agree = sum(1 for d in rows if d["judge_read_name"]
                and d["outcome"] == "attested")
    print(f"  corroborated by the judge reading the same name: {agree}")

    if not args.execute:
        print("\nDRY RUN -- nothing written. Re-run with --execute --reviewer NAME.")
        return
    if not args.reviewer:
        raise SystemExit("--execute needs --reviewer")

    path = pathlib.Path(args.out)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(existing + rows)
    print(f"\nwrote {path.name}  ({len(existing) + len(rows)} rows total)")


if __name__ == "__main__":
    main()
