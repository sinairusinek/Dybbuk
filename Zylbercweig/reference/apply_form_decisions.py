#!/usr/bin/env python3
"""Write the review form's decisions into the review TSV and the ledger.

The form (press_review_form.html) saves one document per row to the artifact's
`db` collection `decisions`, keyed `<entity>__<galron_row_id>`:

    {"decision": "...", "note": "...", "reviewer": "<opaque id>", "at": "..."}

Export them with the Artifact tool, then feed the directory to this script:

    read_db  url=<artifact url>  db_op=list  collection=decisions  out_dir=DIR
    python3 apply_form_decisions.py --from DIR [--reviewer Sinai] [--execute]

Each JSON file is named <doc_id>.json, so the filename carries the key. A row
already decided in the TSV is left alone unless the decision differs, which is
reported as a clash rather than silently overwritten -- the same rule
backfill_review_decisions.py uses.

`reviewer` in the db is the viewer's opaque artifact id, which means nothing to
the corpus, so --reviewer supplies the name to record (default: Sinai).

Idempotent. Line endings preserved per file. Dry run unless --execute.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REVIEW = HERE / "galron_press_match_review.tsv"
LEDGER = HERE / "galron_decisions_round1.tsv"
VALID = {"CONFIRM", "REJECT", "RESEARCH_FURTHER", "NOT_A_NEWSPAPER"}


def read(path: Path):
    csv.field_size_limit(10 ** 9)
    raw = path.read_text(encoding="utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(path: Path, headers, rows, nl):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=headers, delimiter="\t",
                       lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())


def entity_of(r: dict) -> str:
    cid = (r.get("cluster_id") or "").strip()
    return cid if cid else f"db{(r.get('db_id') or '').strip()}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", required=True,
                    help="directory of exported decision JSON files")
    ap.add_argument("--reviewer", default="Sinai")
    ap.add_argument("--execute", action="store_true")
    a = ap.parse_args()

    src = Path(a.src)
    files = sorted(src.rglob("*.json"))
    if not files:
        print(f"no .json under {src}", file=sys.stderr)
        return 2

    decisions: dict[str, dict] = {}
    for f in files:
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"  SKIP {f.name}: not JSON ({exc})")
            continue
        d = (payload.get("decision") or "").strip()
        if not d:
            continue
        if d not in VALID:
            print(f"  SKIP {f.name}: decision {d!r} is not one of {sorted(VALID)}")
            continue
        decisions[f.stem] = {"decision": d, "note": (payload.get("note") or "").strip()}

    print(f"exported decisions: {len(decisions)}")

    headers, rows, nl = read(REVIEW)
    lhead, ledger, lnl = read(LEDGER)
    index = {f"{entity_of(r)}__{(r.get('galron_row_id') or '').strip()}": r for r in rows}

    applied = skipped = clash = orphan = 0
    notes: list[str] = []
    new_ledger: list[dict] = []

    for key, got in sorted(decisions.items()):
        row = index.get(key)
        if row is None:
            orphan += 1
            notes.append(f"  NO SUCH ROW {key} = {got['decision']}")
            continue
        existing = (row.get("decision") or "").strip()
        if existing == got["decision"]:
            skipped += 1
            continue
        if existing:
            clash += 1
            notes.append(f"  CLASH {key}: tsv={existing!r} form={got['decision']!r}")
            continue
        row["decision"] = got["decision"]
        row["reviewer"] = a.reviewer
        row["note"] = got["note"] or "decided on the review form"
        applied += 1
        new_ledger.append({
            "stage": "cluster" if (row.get("cluster_id") or "").strip() else "db",
            "tier": (row.get("tier") or "").strip() or (row.get("band") or "").strip(),
            "entity": entity_of(row),
            "galron_title": row.get("galron_title", ""),
            "decision": got["decision"],
            "reviewer": a.reviewer,
            "note": row["note"],
        })

    print(f"  applied:         {applied}")
    print(f"  already correct: {skipped}")
    print(f"  clashes:         {clash}")
    print(f"  no matching row: {orphan}")
    decided = sum(1 for r in rows if (r.get("decision") or "").strip())
    print(f"\nreview decided after this: {decided} / {len(rows)}")
    print(f"ledger after this: {len(ledger) + len(new_ledger)}")
    if notes:
        print("\nnotes:")
        print("\n".join(notes))

    if not a.execute:
        print("\nDRY RUN — nothing written. Re-run with --execute.")
        return 0

    write(REVIEW, headers, rows, nl)
    ledger.extend({k: r.get(k, "") for k in lhead} for r in new_ledger)
    write(LEDGER, lhead, ledger, lnl)
    print(f"\nWROTE {REVIEW.name} and {LEDGER.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
