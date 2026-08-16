#!/usr/bin/env python3.11
"""Repair the cluster↔DB back-links in core_db.tsv.

Two files record the same fact from opposite ends:

  org_alignment_review.tsv  `aligned_db_id`        cluster -> DB row
  core_db.tsv               `linked_cluster_ids`   DB row  -> clusters

The settlement-audit view used to overwrite `aligned_db_id` when a reviewer
re-merged a cluster that an earlier session had already filed, without removing
the cluster from the old row's `linked_cluster_ids`. The result is 61 DB rows
still claiming clusters that now live somewhere else. The view no longer
creates these (see `_align_clusters_to_db`); this script cleans up the ones
already on disk.

`aligned_db_id` is the authority — it is what the audit UI, the index and the
export all read. `linked_cluster_ids` is a denormalised convenience column, so
it is the side that gets rewritten.

With one exception: a `REMOVE` in `db_audit_decisions.tsv` outranks
`aligned_db_id`. `apply_db_audit_decisions.py` drops the cluster from
`linked_cluster_ids` but leaves `aligned_db_id` naming the row, so a reviewer's
REMOVE survives *only* in the decisions file. Treating aligned_db_id as
authority there would resurrect 18 decisions by Sinai and Ruthie.

Four classes are handled:

  STALE   DB row lists a cluster whose aligned_db_id names a *different* row
          -> drop it from this row (the other row is its real owner)
  MISSING cluster's aligned_db_id names a row that does not list it back
          -> add it
  ORPHAN  DB row lists a cluster_id that is not in the alignment file at all
          -> reported, never touched: could be a genuinely deleted cluster or
             an id typo, and either way it needs a human
  REMOVED reviewer filed REMOVE for this (db_id, cluster) pair
          -> never added back, and dropped if present: a human already ruled

Clusters pointing at a DB row that no longer exists are *not* repaired here.
They need re-filing, not a link edit, and the audit view now flags them
in place with "⚠️ points at missing DB N".

Dry-run by default; pass --apply to write. Writes with the same dialect the
Streamlit app uses (utf-8, tab, CRLF via csv's default lineterminator) and
preserves the existing column order, so the diff is confined to the one column.

    python3.11 Zylbercweig/organizations/repair_cluster_backlinks.py
    python3.11 Zylbercweig/organizations/repair_cluster_backlinks.py --apply
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
ALIGN_FILE = HERE / "org_alignment_review.tsv"
CORE_DB_FILE = HERE / "core_db.tsv"
DECISIONS_FILE = HERE / "db_audit_decisions.tsv"

csv.field_size_limit(10**8)


def _read(path: pathlib.Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def _write(path: pathlib.Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def _split_ids(value: str) -> list[str]:
    return [p.strip() for p in (value or "").replace(",", "|").split("|") if p.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write the repair (default: report only)")
    ap.add_argument("--no-drop-stale", dest="drop_stale", action="store_false",
                    help="report stale links but leave them in place")
    ap.add_argument("--add-missing", action="store_true",
                    help=("also add the back-link for every cluster whose "
                          "aligned_db_id names a row that omits it. OFF by "
                          "default: most of these were never written by the "
                          "audit view at all — they are clusters filed by "
                          "org_review or by the pipeline, which only ever set "
                          "aligned_db_id. Backfilling them is a legitimate "
                          "normalisation but touches ~800 rows, so it is a "
                          "separate decision from repairing the corruption."))
    args = ap.parse_args()

    a_headers, a_rows = _read(ALIGN_FILE)
    db_headers, db_rows = _read(CORE_DB_FILE)

    # (db_id, cluster_id) pairs a reviewer has explicitly unlinked.
    removed: set[tuple[str, str]] = set()
    if DECISIONS_FILE.exists():
        _, d_rows = _read(DECISIONS_FILE)
        for r in d_rows:
            if (r.get("decision") or "").strip().upper() == "REMOVE":
                removed.add(((r.get("db_id") or "").strip(),
                             (r.get("cluster_id") or "").strip()))

    db_ids = {r.get("db_id", "").strip() for r in db_rows}
    # cluster_id -> the DB row it says it belongs to (only live rows count)
    owner: dict[str, str] = {}
    dangling: list[tuple[str, str]] = []
    for r in a_rows:
        cid = (r.get("cluster_id") or "").strip()
        tgt = (r.get("aligned_db_id") or "").strip()
        if not cid or not tgt:
            continue
        if tgt in db_ids:
            owner[cid] = tgt
        else:
            dangling.append((cid, tgt))

    known_clusters = {(r.get("cluster_id") or "").strip() for r in a_rows}

    stale: list[tuple[str, str, str]] = []    # (db_id, cluster, real_owner)
    orphan: list[tuple[str, str]] = []        # (db_id, cluster)
    missing: list[tuple[str, str]] = []       # (db_id, cluster)
    dropped: list[tuple[str, str]] = []       # (db_id, cluster) — REMOVE'd
    blocked: list[tuple[str, str]] = []       # (db_id, cluster) — REMOVE'd, not re-added
    changed_rows = 0

    for row in db_rows:
        dbid = (row.get("db_id") or "").strip()
        listed = _split_ids(row.get("linked_cluster_ids", ""))
        keep: list[str] = []
        for cid in listed:
            if (dbid, cid) in removed:
                dropped.append((dbid, cid))   # reviewer already ruled: unlink
            elif cid not in known_clusters:
                orphan.append((dbid, cid))
                keep.append(cid)          # untouched: needs a human
            elif cid in owner and owner[cid] != dbid:
                stale.append((dbid, cid, owner[cid]))
                if not args.drop_stale:
                    keep.append(cid)
            else:
                keep.append(cid)
        # Clusters that name this row but are absent from its list.
        for cid, tgt in owner.items():
            if tgt == dbid and cid not in keep:
                if (dbid, cid) in removed:
                    blocked.append((dbid, cid))
                    continue
                missing.append((dbid, cid))
                if args.add_missing:
                    keep.append(cid)
        # dict.fromkeys: dedupe while preserving order — a few rows list the
        # same cluster twice after years of appends.
        new_value = " | ".join(dict.fromkeys(keep))
        if new_value != (row.get("linked_cluster_ids") or ""):
            changed_rows += 1
            row["linked_cluster_ids"] = new_value

    print(f"DB rows: {len(db_rows)}   clusters with a live target: {len(owner)}")
    stale_verb = "link dropped" if args.drop_stale else "LEFT AS IS (--no-drop-stale)"
    print(f"\nSTALE   {len(stale):4}  {stale_verb} — cluster lives on another row")
    for dbid, cid, real in stale[:15]:
        print(f"          DB {dbid} ⊅ {cid}   (really DB {real})")
    if len(stale) > 15:
        print(f"          … and {len(stale) - 15} more")

    missing_verb = "link added" if args.add_missing else "LEFT AS IS (pass --add-missing)"
    print(f"\nMISSING {len(missing):4}  {missing_verb} — cluster names this row")
    for dbid, cid in missing[:15]:
        print(f"          DB {dbid} ⊇ {cid}")
    if len(missing) > 15:
        print(f"          … and {len(missing) - 15} more")

    print(f"\nORPHAN  {len(orphan):4}  listed cluster not in the alignment file — LEFT AS IS")
    for dbid, cid in orphan[:15]:
        print(f"          DB {dbid} → {cid}")
    if len(orphan) > 15:
        print(f"          … and {len(orphan) - 15} more")

    print(f"\nREMOVED {len(dropped):4}  link dropped — reviewer filed REMOVE in "
          f"{DECISIONS_FILE.name}")
    for dbid, cid in dropped[:15]:
        print(f"          DB {dbid} ⊅ {cid}")
    if len(dropped) > 15:
        print(f"          … and {len(dropped) - 15} more")
    print(f"        {len(blocked):4}  further REMOVE'd pair(s) NOT re-added by --add-missing")

    print(f"\nDANGLING {len(dangling):3}  cluster points at a DB row that no longer exists —")
    print("            LEFT AS IS, re-file from the audit view (⚠️ badge)")
    for cid, tgt in dangling[:15]:
        print(f"          {cid} → DB {tgt}")

    print(f"\n{changed_rows} DB row(s) would change.")
    if not args.apply:
        print("Dry run — nothing written. Re-run with --apply to commit the repair.")
        return 0
    if not changed_rows:
        print("Nothing to write.")
        return 0

    backup = CORE_DB_FILE.with_suffix(".tsv.pre_backlink_repair")
    backup.write_bytes(CORE_DB_FILE.read_bytes())
    _write(CORE_DB_FILE, db_headers, db_rows)
    print(f"Written. Backup at {backup.name}")
    print("org_alignment_review.tsv untouched (aligned_db_id is the authority).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
