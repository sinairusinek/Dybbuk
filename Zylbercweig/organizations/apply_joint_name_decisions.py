"""Apply the DB Audit "Joint names" decisions to core_db.tsv.

Source of truth: db_joint_name_decisions.tsv (written by the Zalmen view, keyed
by db_id). Run after pulling, so the app's own pushes are not clobbered.

The vocabulary and what each decision means as an edit:

  ONE_COMPANY   keep the row as it stands — one real company billed under
                several names. Normally a NO-OP, and that is the point: the
                row is already correct. The exception is a row whose parts
                were previously dispatched to other orgs (see below).
  SEVERAL       the row is several troupes -> needs the bundle treatment
                (resolve-or-mint per part). NOT automated here: it mints ids
                and rewrites linked_cluster_ids, which has no single authority
                (feedback_core_db_link_authority). Reported for a human.
  NOT_A_TROUPE  deprecate the row (reversible; never a hard delete).
  CHECK         deferred by the reviewer; no edit.

### Why ONE_COMPANY can still require an edit
db1076 is the case. Ruthie's 2026-10-04 doc answers treated it as FOUR
troupes and dispatched its single cluster ORG-C03241 onto three existing orgs
(db1016 אַשכנזי, db1403 קענער, db459 זיגלער), with a fourth part held.
Sinai and Ruthie then reviewed it together in the Joint names view and decided
ONE_COMPANY, which supersedes that. So the links added for the parts have to
come back off, leaving the cluster on db1076 alone.

That reversal is derived from ruthie_doc_answers_2026-10-04.tsv (the LINK rows
for the same bundle), not hardcoded, so it stays correct if more rows flip.

Backup to core_db.tsv.pre_jointname_<TS>. Dry-run by default.

Usage: python3 apply_joint_name_decisions.py [--apply]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import shutil
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
DECISIONS = HERE / "db_joint_name_decisions.tsv"
DOC_ANSWERS = HERE / "ruthie_doc_answers_2026-10-04.tsv"


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def main(apply: bool) -> None:
    _, decisions = _load(DECISIONS)
    _, doc = _load(DOC_ANSWERS)
    fields, rows = _load(CORE_DB)
    by_id = {r["db_id"].strip(): r for r in rows}

    # Links the doc round added, grouped by the bundle they came from.
    links_by_bundle: dict[str, list[tuple[str, str]]] = {}
    for a in doc:
        if a.get("kind") == "bundle_part" and a.get("decision", "").strip().upper() == "LINK":
            links_by_bundle.setdefault(a["bundle_db_id"].strip(), []).append(
                (a["part"].strip(), a["target_db_id"].strip())
            )

    unlinks: list[tuple[str, str, str, str]] = []  # bundle, cid, target, part
    deprecates: list[str] = []
    noops: list[str] = []
    manual: list[tuple[str, str]] = []
    problems: list[str] = []

    for d in decisions:
        db = d["db_id"].strip()
        dec = d["decision"].strip().upper()
        row = by_id.get(db)
        if row is None:
            problems.append(f"db{db}: decision {dec} but row not in core_db")
            continue

        if dec == "ONE_COMPANY":
            cids = [c.strip() for c in (row["linked_cluster_ids"] or "").split("|") if c.strip()]
            prior = links_by_bundle.get(db, [])
            undone = False
            for part, tgt in prior:
                trow = by_id.get(tgt)
                if trow is None:
                    problems.append(f"db{db}: prior LINK target db{tgt} missing")
                    continue
                for cid in cids:
                    if cid in (trow["linked_cluster_ids"] or ""):
                        unlinks.append((db, cid, tgt, part))
                        undone = True
            if not undone:
                noops.append(db)
        elif dec == "NOT_A_TROUPE":
            if (row.get("deprecated") or "").strip():
                noops.append(db)
            else:
                deprecates.append(db)
        elif dec == "SEVERAL":
            manual.append((db, "needs the bundle resolve-or-mint treatment"))
        elif dec == "CHECK":
            manual.append((db, "reviewer deferred"))
        else:
            problems.append(f"db{db}: unknown decision {dec!r}")

    print(f"{len(decisions)} decisions: "
          f"{len(noops)} already correct (no edit), "
          f"{len(unlinks)} link(s) to undo, "
          f"{len(deprecates)} to deprecate, "
          f"{len(manual)} for a human")

    if unlinks:
        print("\nUNDO prior part-links (ONE_COMPANY supersedes the doc round):")
        for db, cid, tgt, part in unlinks:
            t = by_id[tgt]
            print(f"  db{db}: remove {cid} from db{tgt} {(t['name_yiddish'] or t['name'])!r}"
                  f"   (was linked for part {part!r})")
    if deprecates:
        print("\nDEPRECATE:")
        for db in deprecates:
            r = by_id[db]
            print(f"  db{db} {(r['name_yiddish'] or r['name'])!r}")
    if manual:
        print("\nFOR A HUMAN (not applied):")
        for db, why in manual:
            print(f"  db{db}: {why}")
    if problems:
        print(f"\n*** PROBLEMS ({len(problems)}) — nothing written:")
        for p in problems:
            print(f"  {p}")
        return

    if not apply:
        print("\n(dry-run; pass --apply to write)")
        return
    if not (unlinks or deprecates):
        print("\nnothing to write — every decision is already reflected")
        return

    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    shutil.copy2(CORE_DB, CORE_DB.with_suffix(f".tsv.pre_jointname_{ts}"))

    for _db, cid, tgt, _part in unlinks:
        cur = [c.strip() for c in (by_id[tgt]["linked_cluster_ids"] or "").split("|") if c.strip()]
        by_id[tgt]["linked_cluster_ids"] = " | ".join(c for c in cur if c != cid)
    for db in deprecates:
        by_id[db]["deprecated"] = "1"

    _dump(CORE_DB, fields, rows)
    print(f"\nwrote: {len(unlinks)} unlink(s), {len(deprecates)} deprecated")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(ap.parse_args().apply)
