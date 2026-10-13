#!/usr/bin/env python3
"""Backfill `decision`/`reviewer`/`note` into galron_press_match_review.tsv
from galron_decisions_round1.tsv.

Why: round 1 recorded its 160 decisions in the LEDGER only, under a different
tier scheme (A1/A2/C1a vs the review file's A-exact-skeleton/C-phonetic-plus).
The review file's own `decision` column stayed empty on all 499 rows, so every
tool that reads it as a worklist reports 0% decided and re-offers settled rows.

Join: the ledger's `entity` is the review row's `cluster_id`, or `db<db_id>`
when the row has no cluster. Two passes:
  1. entity + exact galron_title
  2. entity alone, when the ledger shortened Galron's title (dropped a Cyrillic
     alias, a <city> qualifier, or vocalization) and the entity has exactly one
     review row -- never when it has several.

Rows the ledger decided that have no review row at all are reported and left
alone: db934 is an unlinked judgement with no Galron candidate, and two C1b/C2
CONFIRMs resolved to a different cluster than the matcher proposed.

Idempotent: a row already carrying the intended decision is skipped, and an
existing DIFFERENT decision is never overwritten -- it is reported as a clash.
Line endings are preserved (detected per file; this one is LF).

Run with --execute to write; default is a dry run.
"""
from __future__ import annotations

import csv
import io
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REVIEW = HERE / "galron_press_match_review.tsv"
LEDGER = HERE / "galron_decisions_round1.tsv"


def read(path: Path) -> tuple[list[str], list[dict], str]:
    csv.field_size_limit(10 ** 9)
    raw = path.read_text(encoding="utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
    return list(reader.fieldnames), list(reader), nl


def entity_of(row: dict) -> str:
    cid = (row.get("cluster_id") or "").strip()
    if cid:
        return cid
    db = (row.get("db_id") or "").strip()
    return f"db{db}" if db else ""


def main() -> int:
    execute = "--execute" in sys.argv
    headers, rows, nl = read(REVIEW)
    _, ledger, _ = read(LEDGER)

    for col in ("decision", "reviewer", "note"):
        if col not in headers:
            print(f"ERROR: review file has no `{col}` column", file=sys.stderr)
            return 2

    by_ent_title: dict[tuple[str, str], list[dict]] = defaultdict(list)
    by_ent: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        e = entity_of(r)
        by_ent_title[(e, (r.get("galron_title") or "").strip())].append(r)
        by_ent[e].append(r)

    applied = skipped = clash = unmatched = ambiguous = 0
    clashes: list[str] = []
    orphans: list[str] = []

    for d in ledger:
        ent = (d.get("entity") or "").strip()
        title = (d.get("galron_title") or "").strip()
        decision = (d.get("decision") or "").strip()
        if not ent or not decision:
            continue

        targets = by_ent_title.get((ent, title), [])
        how = "entity+title"
        if not targets:
            cands = by_ent.get(ent, [])
            if len(cands) == 1:
                targets, how = cands, "entity only (ledger title shortened)"
            elif len(cands) > 1:
                ambiguous += 1
                orphans.append(f"  AMBIGUOUS {ent} {title!r}: {len(cands)} review rows, title no match")
                continue
            else:
                unmatched += 1
                orphans.append(f"  NO REVIEW ROW {ent} {title!r} = {decision} (tier {d.get('tier','')})")
                continue

        for t in targets:
            existing = (t.get("decision") or "").strip()
            if existing == decision:
                skipped += 1
                continue
            if existing:
                clash += 1
                clashes.append(f"  CLASH {ent} {title!r}: review={existing!r} ledger={decision!r}")
                continue
            t["decision"] = decision
            t["reviewer"] = (d.get("reviewer") or "").strip()
            note = (d.get("note") or "").strip()
            tier = (d.get("tier") or "").strip()
            prov = f"round 1 ledger (tier {tier})" if tier else "round 1 ledger"
            t["note"] = f"{note} [{prov}]" if note else f"[{prov}]"
            applied += 1
            if how != "entity+title":
                orphans.append(f"  JOINED BY ENTITY {ent}: ledger {title!r} -> review {(t.get('galron_title') or '')[:46]!r}")

    print(f"ledger decisions:   {len(ledger)}")
    print(f"  applied:          {applied}")
    print(f"  already correct:  {skipped}")
    print(f"  clashes:          {clash}")
    print(f"  no review row:    {unmatched}")
    print(f"  ambiguous:        {ambiguous}")
    decided = sum(1 for r in rows if (r.get("decision") or "").strip())
    print(f"\nreview rows decided after this: {decided} / {len(rows)}")
    if orphans:
        print("\nnotes:")
        print("\n".join(orphans))
    if clashes:
        print("\nCLASHES (left untouched):")
        print("\n".join(clashes))

    if not execute:
        print("\nDRY RUN — nothing written. Re-run with --execute.")
        return 0

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=headers, delimiter="\t",
                       lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    with open(REVIEW, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())
    print(f"\nWROTE {REVIEW} ({len(rows)} rows, terminator {nl!r})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
