#!/usr/bin/env python3
"""Turn the publisher decision form's output into ledger rows.

The form (publisher_decide_form.html) saves one document per cluster to the
artifact's `db` collection `publisher_decisions`:

    {"decision": "MINT|RETYPE|NOT_AN_ORG|RESEARCH", "note": "...", "at": "..."}

This script writes them into publisher_audit_decisions.tsv as rows the EXISTING
apply scripts understand -- it never touches core_db or the review ledger
itself. That keeps one write path into the corpus:

    read_db url=<artifact> db_op=list collection=publisher_decisions out_dir=DIR
    python3 apply_publisher_form_decisions.py --from DIR            # dry run
    python3 apply_publisher_form_decisions.py --from DIR --execute
    python3 apply_publisher_audit_mints.py    --reviewer Sinai --apply
    python3 apply_publisher_audit_clusters.py --reviewer Sinai --apply

Mapping, with the decision's note carried into the rationale:
    MINT        -> kind=mint,       action=mint_new_org, value=<cluster name>
    RETYPE      -> kind=retype,     action=set_cluster_type, value=<note>
                   (the note MUST name the org_type; a row without one is
                    reported and skipped rather than guessed)
    NOT_AN_ORG  -> kind=not_an_org, action=not_an_organization
    RESEARCH    -> kind=flag,       action=needs_research

Idempotent: a cluster already carrying a decision row of the same kind is
skipped. Dry-run by default. Line endings preserved.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
LEDGER = HERE / "publisher_audit_decisions.tsv"
DATA = HERE / "publisher_remaining_open.json"

MAP = {
    "MINT":       ("mint", "mint_new_org"),
    "RETYPE":     ("retype", "set_cluster_type"),
    "NOT_AN_ORG": ("not_an_org", "not_an_organization"),
    "RESEARCH":   ("flag", "needs_research"),
}


def read(path: Path):
    csv.field_size_limit(10 ** 9)
    raw = path.read_text(encoding="utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rows = list(csv.reader(io.StringIO(raw), delimiter="\t"))
    return rows[0], [r for r in rows[1:] if r], nl


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", required=True)
    ap.add_argument("--execute", action="store_true")
    a = ap.parse_args()

    files = sorted(Path(a.src).rglob("*.json"))
    if not files:
        print(f"no .json under {a.src}", file=sys.stderr)
        return 2

    names = {r["cluster_id"]: r for r in json.loads(DATA.read_text(encoding="utf-8"))}
    H, body, nl = read(LEDGER)
    ci, ki = H.index("cluster_id"), H.index("kind")
    seen = {(r[ci], r[ki]) for r in body if len(r) > max(ci, ki)}

    nums = [int(r[0][1:]) for r in body if r and r[0].startswith("P") and r[0][1:].isdigit()]
    nxt = max(nums, default=0) + 1

    new, skipped, bad = [], [], []
    for f in files:
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            bad.append(f"{f.name}: not JSON ({exc})")
            continue
        cid = f.stem
        dec = (payload.get("decision") or "").strip().upper()
        note = (payload.get("note") or "").strip()
        if not dec:
            continue
        if dec not in MAP:
            bad.append(f"{cid}: decision {dec!r} not one of {sorted(MAP)}")
            continue
        meta = names.get(cid)
        if meta is None:
            bad.append(f"{cid}: not among the open clusters")
            continue
        kind, action = MAP[dec]
        if (cid, kind) in seen:
            skipped.append(f"{cid}: already has a {kind} row")
            continue
        if dec == "RETYPE" and not note:
            bad.append(f"{cid}: RETYPE with no org_type in the note — skipped")
            continue
        value = meta["name"] if dec == "MINT" else (note if dec == "RETYPE" else "")
        why = (f"{note} " if note and dec != "RETYPE" else "")
        why += (f"Decided on the publisher form from the mention context "
                f"({meta.get('mentions','?')} mention(s), reads as "
                f"{meta.get('reads_as','?')}). Confirmed by Sinai.")
        new.append([f"P{nxt}", kind, "", cid, action, value, "high", why.strip()])
        nxt += 1

    print(f"exported decisions: {len(files)} file(s)")
    print(f"  new ledger rows: {len(new)}")
    print(f"  already present: {len(skipped)}")
    print(f"  rejected:        {len(bad)}")
    for b in bad:
        print(f"    {b}")
    if new:
        from collections import Counter
        print("  by kind:", dict(Counter(r[1] for r in new)))

    if not a.execute:
        print("\nDRY RUN — nothing written. Re-run with --execute.")
        return 0
    if not new:
        print("\nNothing to write.")
        return 0

    body.extend(new)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t", lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writerow(H)
    w.writerows(body)
    with open(LEDGER, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())
    print(f"\nWROTE {LEDGER.name}: +{len(new)} rows ({new[0][0]}..{new[-1][0]})")
    print("Now run apply_publisher_audit_mints.py and apply_publisher_audit_clusters.py.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
