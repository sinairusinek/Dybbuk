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

NONE_TOKENS = {"none", "NONE", "-", "—", "n/a", "na", "no", "nobody", "אין",
               # Ruthie's own vocabulary on the Drive workbook: "garbage" for a
               # span that is not a name at all (מיט = "with"), and "not
               # relevant" in the comments column for the same thing.
               "garbage", "not relevant", "irrelevant",
               # Added 2026-10-25 from the 10-21 workbook revision. Without
               # these, "Troupe (not person)" read as a PERSON NAME and the
               # span was silently confirmed against a candidate hub -- the
               # exact opposite of her answer (db1614 פאקט, db1624, db1630).
               "troupe (not person)", "troupe(not person)", "not a person",
               "not person"}

# Out-of-scope rather than not-a-person: the span IS someone's name, but the
# person is outside this corpus. Recorded as REJECT so the span stops being
# asked, with the reason preserved -- never folded into "no person", which
# would assert something false about the name (db1031 גארסקי "Non Jewish").
OUT_OF_SCOPE_TOKENS = {"non jewish", "non-jewish", "not jewish"}


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
        else:
            # Facsimile-derived hub (HUB-P-*) with no people_db row. The builder
            # labels a proposed one `heading [hub_id]`, which matches neither the
            # bracketed db form nor the trailing-[digits] regex below, so index
            # that spelling explicitly.
            hub_id = (h.get("hub_id") or "").strip()
            if hub_id:
                by_label[f"{hd} [{hub_id}]"] = h
        by_label.setdefault(hd, h)

    cands = read_tsv(CANDIDATES)
    cand_by = {}
    for c in cands:
        cand_by.setdefault((c["db_id"], c["span"]), []).append(c)

    existing = read_tsv(DECISIONS)
    have = {f"{d['db_id']}|{d['hub_id']}" for d in existing}
    # Decisions are keyed (db_id, hub_id) because that is what an edge is, but
    # the WORKBOOK is keyed (db_id, span) -- and one org routinely names one
    # person across several spans: db1335 אַסתר-רחל קאַמינסקי fills four
    # (אברהם, אסתר, יצחק, רחל). Deduping on the pair alone recorded only the
    # first and left the other spans undecided, so the next build re-asked 31
    # questions Ruthie had already answered. Dedup on the triple instead; the
    # extra rows are harmless because apply_eponym_decisions collapses them to
    # one edge per (org, person).
    have_triples = {(d["db_id"], d["hub_id"], d.get("span") or "")
                    for d in existing}

    now = _dt.datetime.now().isoformat(timespec="seconds")
    new, unmatched, blank = [], [], 0

    # She also rejects a span from the comments column, writing "not relevant"
    # there rather than in person_name (11 spans on the Drive workbook). Read
    # that as the same answer: it is a rejection either way, and ignoring it
    # left those spans in the queue to be asked again.
    cmt_col = next((h for h in head
                    if h and "comment" in str(h).lower()), None)

    for r in range(2, ws.max_row + 1):
        answer = (ws.cell(r, col["person_name"] + 1).value or "")
        answer = str(answer).strip()
        if not answer and cmt_col:
            note = str(ws.cell(r, col[cmt_col] + 1).value or "").strip()
            if note.lower() in NONE_TOKENS or note.lower() in OUT_OF_SCOPE_TOKENS:
                answer = note
        if not answer:
            blank += 1
            continue
        db_id = str(ws.cell(r, col["db_id"] + 1).value or "").strip()
        span = str(ws.cell(r, col["name in troupe"] + 1).value or "").strip()
        group = cand_by.get((db_id, span)) or []
        tier = group[0]["tier"] if group else ""
        pattern = group[0]["pattern"] if group else ""
        org_y = str(ws.cell(r, col["troupe (Yiddish)"] + 1).value or "")

        out_of_scope = answer.lower() in OUT_OF_SCOPE_TOKENS
        if out_of_scope or answer in NONE_TOKENS or answer.lower() in NONE_TOKENS:
            # No person: record against the first candidate hub if there is
            # one, else a bare marker row so the span is not re-asked.
            hub_id = group[0]["hub_id"] if group and group[0].get("hub_id") else ""
            # Same triple guard as the CONFIRM path: re-running the ingest on a
            # workbook whose REJECTs are already recorded must not append them
            # twice (db1426 לאבעל got two identical REJECT rows this way).
            if (db_id, hub_id, span) in have_triples:
                continue
            have_triples.add((db_id, hub_id, span))
            new.append({
                "db_id": db_id, "hub_id": hub_id, "span": span,
                "decision": "REJECT", "tier": tier, "pattern": pattern,
                "org_name_yiddish": org_y, "person_heading": "",
                "person_db_ids": "", "reviewer": args.reviewer,
                "reviewed_at": now,
                "notes": (f"RA workbook: out of scope ({answer})"
                          if out_of_scope else "RA workbook: no person"),
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

        # Decisions are keyed (db_id, hub_id) because that is what an edge is,
        # but the WORKBOOK is keyed (db_id, span) -- and one org routinely
        # names one person across several spans: db1335 אַסתר-רחל קאַמינסקי
        # fills four (אברהם, אסתר, יצחק, רחל). Skipping on the pair alone
        # recorded only the first and left the other spans with no decision, so
        # the next build re-asked 31 questions Ruthie had already answered.
        # Skip only when this exact SPAN is already decided (see have_triples).
        if (db_id, hub["hub_id"], span) in have_triples:
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
        have_triples.add((db_id, hub["hub_id"], span))

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
