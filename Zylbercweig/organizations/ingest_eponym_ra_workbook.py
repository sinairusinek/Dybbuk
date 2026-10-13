#!/usr/bin/env python3
"""Read a filled eponym RA workbook back into eponym_decisions.tsv.

The reviewer fills column `person_name` on the "troupes" sheet from the
dropdown (values look like `גראָדנער, רייטשעל [ריינאלדס] [1883]` -- heading
plus the person's db id in trailing brackets). This maps each answer back to a
hub_id and appends a CONFIRM row; `NONE` / `none` / `-` records a REJECT so the
span is not asked again.

Decisions are keyed (db_id, hub_id) exactly as the retired Streamlit view wrote
them, so apply_eponym_decisions.py consumes them unchanged.

  --dry-run (default)   report what would be added
  --execute             append to eponym_decisions.tsv
  --reviewer NAME       stamped on every row (required for --execute)
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import pathlib
import re
import shutil

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
DECISIONS = BASE / "organizations" / "eponym_decisions.tsv"
CANDIDATES = BASE / "organizations" / "eponym_candidates.tsv"
PERSON_HUB = BASE / "people" / "person_hub.tsv"

FIELDS = ["db_id", "hub_id", "span", "decision", "tier", "pattern",
          "org_name_yiddish", "person_heading", "person_db_ids",
          "reviewer", "reviewed_at", "notes"]

NONE_TOKENS = {"none", "NONE", "-", "—", "n/a", "na", "no", "nobody", "אין"}


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workbook")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--reviewer", default="")
    args = ap.parse_args()

    if args.execute and not args.reviewer:
        raise SystemExit("--execute needs --reviewer NAME "
                         "(every decision is reviewer-stamped)")

    from openpyxl import load_workbook

    wb = load_workbook(args.workbook, data_only=True)
    ws = wb["troupes"]
    head = [c.value for c in ws[1]]
    col = {name: i for i, name in enumerate(head)}

    hubs = read_tsv(PERSON_HUB)
    # heading+id -> hub, and db_id -> hub, for mapping the answer back.
    by_label, by_dbid = {}, {}
    for h in hubs:
        hd = (h.get("canonical_heading") or "").strip()
        db = (h.get("db_ids") or "").split(",")[0].strip()
        if db:
            by_dbid.setdefault(db, h)
            by_label[f"{hd} [{db}]"] = h
        by_label.setdefault(hd, h)

    cands = read_tsv(CANDIDATES)
    cand_by = {}
    for c in cands:
        cand_by.setdefault((c["db_id"], c["span"]), []).append(c)

    existing = read_tsv(DECISIONS)
    have = {f"{d['db_id']}|{d['hub_id']}" for d in existing}

    now = _dt.datetime.now().isoformat(timespec="seconds")
    new, unmatched, blank = [], [], 0

    for r in range(2, ws.max_row + 1):
        answer = (ws.cell(r, col["person_name"] + 1).value or "")
        answer = str(answer).strip()
        if not answer:
            blank += 1
            continue
        db_id = str(ws.cell(r, col["db_id"] + 1).value or "").strip()
        span = str(ws.cell(r, col["name in troupe"] + 1).value or "").strip()
        group = cand_by.get((db_id, span)) or []
        tier = group[0]["tier"] if group else ""
        pattern = group[0]["pattern"] if group else ""
        org_y = str(ws.cell(r, col["troupe (Yiddish)"] + 1).value or "")

        if answer in NONE_TOKENS or answer.lower() in NONE_TOKENS:
            # No person: record against the first candidate hub if there is
            # one, else a bare marker row so the span is not re-asked.
            hub_id = group[0]["hub_id"] if group and group[0].get("hub_id") else ""
            new.append({
                "db_id": db_id, "hub_id": hub_id, "span": span,
                "decision": "REJECT", "tier": tier, "pattern": pattern,
                "org_name_yiddish": org_y, "person_heading": "",
                "person_db_ids": "", "reviewer": args.reviewer,
                "reviewed_at": now, "notes": "RA workbook: no person",
            })
            continue

        hub = by_label.get(answer)
        if not hub:
            m = re.search(r"\[(\d+)\]\s*$", answer)   # trailing [id]
            if m:
                hub = by_dbid.get(m.group(1))
        if not hub:
            unmatched.append((r, db_id, span, answer))
            continue

        key = f"{db_id}|{hub['hub_id']}"
        if key in have:
            continue
        new.append({
            "db_id": db_id, "hub_id": hub["hub_id"], "span": span,
            "decision": "CONFIRM", "tier": tier, "pattern": pattern,
            "org_name_yiddish": org_y,
            "person_heading": hub.get("canonical_heading") or "",
            "person_db_ids": hub.get("db_ids") or "",
            "reviewer": args.reviewer, "reviewed_at": now,
            "notes": "RA workbook",
        })
        have.add(key)

    print(f"workbook rows:      {ws.max_row - 1}")
    print(f"  blank (unanswered): {blank}")
    print(f"  new decisions:      {len(new)} "
          f"(CONFIRM {sum(1 for d in new if d['decision'] == 'CONFIRM')}, "
          f"REJECT {sum(1 for d in new if d['decision'] == 'REJECT')})")
    print(f"  unmatched answers:  {len(unmatched)}")
    for r, db, sp, a in unmatched[:15]:
        print(f"     row {r}: db{db} {sp} -> {a!r}")

    if not args.execute:
        print("\nDRY RUN -- nothing written. Re-run with --execute --reviewer NAME.")
        return
    if not new:
        print("\nnothing to add.")
        return

    if DECISIONS.exists():
        stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(DECISIONS, DECISIONS.with_suffix(f".tsv.pre_ingest_{stamp}"))
    rows = existing + new
    with DECISIONS.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for d in rows:
            w.writerow({**{k: "" for k in FIELDS}, **d})
    print(f"\nwrote {DECISIONS.relative_to(BASE)}  ({len(rows)} rows total)")


if __name__ == "__main__":
    main()
