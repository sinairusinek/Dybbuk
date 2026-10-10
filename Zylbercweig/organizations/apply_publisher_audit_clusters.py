"""Apply the cluster-side publisher-audit decisions Sinai accepted (2026-10-10).

Source of truth: publisher_audit_decisions.tsv. The companion
apply_publisher_audit.py handles the core_db side (out_of_project flags and
restored org_types); this one writes the cluster ledger,
org_alignment_review.tsv, plus the linked_cluster_ids those alignments imply.

Sinai accepted every decision EXCEPT the ones filed for human review, which he
is working himself. So this script deliberately applies:

  align       ALIGN + aligned_db_id on the cluster row, and the cluster added
              to that core_db row's linked_cluster_ids. Both sides, because a
              cluster pointing at a db row that does not point back is exactly
              the half-written link feedback_core_db_link_authority warns about.
  retype      the cluster's org_type corrected -- record labels, film studios,
              unions, funds and the rest that `Publisher` had absorbed. No
              decision is stamped: the type is a property, and typing a cluster
              does not settle whether it aligns or mints.
  not_an_org  NOT_AN_ORG, the ledger's existing vocabulary for a book title,
              almanac or imprint formula sitting in the org table.
  reject      the cluster is explicitly NOT the db row it scores against; the
              reason goes in reviewer_notes so the next pass does not re-propose
              it. No decision value -- the cluster still needs its own.

and deliberately does NOT apply:

  mint / flag / duplicate   Sinai's to review. A mint coins a core_db id and a
                            duplicate retires one; neither should happen from a
                            script while the decision is still open.

Type vocabulary follows org_alignment_review.tsv's own spellings
("Musical organization", "Welfare/Aid organization", "Labour
(factory/workshop)"), which differ in casing from core_db's. The ledger being
written is the authority for its own column.

Idempotent on every path: a row already carrying the target value is skipped
and not logged, so a second --apply appends nothing. Tested, not assumed.

Backups to <file>.pre_publisher_clusters_<TS>. Dry-run by default.

Usage: python3 apply_publisher_audit_clusters.py --reviewer Sinai [--apply]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import io
import json
import shutil
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
REVIEW = HERE / "org_alignment_review.tsv"
DECISIONS = HERE / "publisher_audit_decisions.tsv"
ACTIVITY_LOG = HERE / "activity_log.tsv"

LOG_COLUMNS = ("ts", "reviewer", "view", "action", "target_id", "decision",
               "note", "extra")
LOG_VIEW = "org_review"
SOURCE = DECISIONS.name

TODAY = _dt.date.today().isoformat()
# Kinds this script is allowed to write. mint/flag/duplicate are excluded by
# design -- see the module docstring.
APPLIED_KINDS = ("align", "retype", "not_an_org", "reject")

# A decision carries both a `kind` (which branch applies it) and an `action`
# (what it does). They are redundant, and a row where they disagree is a
# drafting error, not an instruction: P056 was authored kind=align /
# action=reject and would have written a link to the very row it was rejecting.
# Refuse the whole run rather than guess which column meant it.
VALID_ACTIONS = {
    "align": {"add_cluster_link", "note_only"},
    "retype": {"set_cluster_type", "keep_separate"},
    "not_an_org": {"not_an_organization"},
    "reject": {"reject"},
    "mint": {"mint_new_org", "add_to_mint"},
    "flag": {"needs_research"},
    "duplicate": {"merge_candidate"},
    "split": {"split_cluster"},
    "out_of_project": {"set_out_of_project"},
    "restore_type": {"set_org_type"},
}


def check_coherent(decisions: list[dict]) -> None:
    bad = [f"{d.get('decision_id')}: kind={d.get('kind')!r} "
           f"action={d.get('action')!r}"
           for d in decisions
           if (d.get("action") or "").strip()
           not in VALID_ACTIONS.get((d.get("kind") or "").strip(), set())]
    if bad:
        raise SystemExit("decision rows whose kind and action disagree:\n  "
                         + "\n  ".join(bad))


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def log_rows(entries: list[dict]) -> int:
    """Append to activity_log.tsv, preserving its line endings.

    Bare-LF up to 2026-08-30T08:28 and CRLF after; ts-sorted, so an append
    belongs at the tail and takes the tail's CRLF.
    """
    if not entries:
        return 0
    if not ACTIVITY_LOG.exists():
        raise SystemExit(f"missing {ACTIVITY_LOG.name}; refusing to create it")
    existing = ACTIVITY_LOG.read_bytes()
    header = existing.split(b"\n", 1)[0].rstrip(b"\r").decode("utf-8").split("\t")
    if tuple(header) != LOG_COLUMNS:
        raise SystemExit(f"{ACTIVITY_LOG.name} header is {header}, "
                         f"expected {list(LOG_COLUMNS)}")
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(LOG_COLUMNS), delimiter="\t",
                       lineterminator="\r\n")
    w.writerows(entries)
    with ACTIVITY_LOG.open("ab") as f:
        if not existing.endswith((b"\n", b"\r\n")):
            f.write(b"\r\n")
        f.write(buf.getvalue().encode("utf-8"))
    return len(entries)


def log_entry(reviewer: str, action: str, target_id: str, decision: str,
              note: str, **extra) -> dict:
    return {
        "ts": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reviewer": reviewer,
        "view": LOG_VIEW,
        "action": action,
        "target_id": str(target_id),
        "decision": decision,
        "note": note.replace("\n", " ").replace("\t", " ").strip()[:500],
        "extra": json.dumps({"source": SOURCE, **extra}, ensure_ascii=False),
    }


def stamp(row: dict, reviewer: str) -> None:
    """Every decision write carries its reviewer. No exceptions.

    feedback_zalmen_stamp_reviewer_on_every_decision_write: 1322 rows lost
    attribution because three helpers wrote `decision` without this.
    """
    row["reviewer"] = reviewer
    row["reviewed_at"] = _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def add_note(row: dict, text: str) -> bool:
    cur = (row.get("reviewer_notes") or "").strip()
    if text in cur:
        return False
    row["reviewer_notes"] = f"{cur} | {text}".strip(" |") if cur else text
    return True


def add_cluster(row: dict, cid: str) -> bool:
    cur = [c.strip() for c in (row.get("linked_cluster_ids") or "").split("|")
           if c.strip()]
    if cid in cur:
        return False
    row["linked_cluster_ids"] = " | ".join(cur + [cid])
    return True


def main(apply: bool, reviewer: str) -> None:
    db_fields, db_rows = _load(CORE_DB)
    db = {r["db_id"]: r for r in db_rows}
    rv_fields, rv_rows = _load(REVIEW)
    rv = {r["cluster_id"]: r for r in rv_rows}
    _, decisions = _load(DECISIONS)
    check_coherent(decisions)

    done: list[str] = []
    skipped: list[str] = []
    entries: list[dict] = []

    for d in decisions:
        kind = (d.get("kind") or "").strip()
        if kind not in APPLIED_KINDS:
            continue
        cid = (d.get("cluster_id") or "").strip()
        did = (d.get("db_id") or "").strip()
        pid = d.get("decision_id", "")
        rationale = (d.get("rationale") or "").strip()
        if not cid:
            continue
        row = rv.get(cid)
        if row is None:
            skipped.append(f"{pid} {cid}: not in {REVIEW.name}")
            continue

        # P045 is a note_only alternative reading, not an instruction to write.
        if (d.get("action") or "").strip() == "note_only":
            skipped.append(f"{pid} {cid}: note_only, nothing to write")
            continue

        if kind == "align":
            dbrow = db.get(did)
            if dbrow is None:
                skipped.append(f"{pid} {cid}: db {did} missing")
                continue
            cur = (row.get("decision") or "").strip()
            if cur and cur != "ALIGN":
                skipped.append(f"{pid} {cid}: already decided {cur}; left alone")
                continue
            touched = False
            if cur != "ALIGN" or (row.get("aligned_db_id") or "").strip() != did:
                if apply:
                    row["decision"] = "ALIGN"
                    row["aligned_db_id"] = did
                    add_note(row, f"publisher audit {TODAY} ({pid}): {rationale}")
                    stamp(row, reviewer)
                touched = True
            backlink = (cid not in [c.strip() for c in
                        (dbrow.get("linked_cluster_ids") or "").split("|")])
            if backlink and apply:
                add_cluster(dbrow, cid)
            if not touched and not backlink:
                skipped.append(f"{pid} {cid}: already aligned to {did}")
                continue
            done.append(f"{pid} ALIGN      {cid} -> db {did}"
                        f"{'  +backlink' if backlink else ''}")
            entries.append(log_entry(
                reviewer, "alignment", cid, "ALIGN",
                f"{pid}: {rationale}", aligned_db_id=did, backlink=backlink))

        elif kind == "retype":
            new = (d.get("value") or "").strip()
            if (d.get("action") or "").strip() != "set_cluster_type" or not new:
                skipped.append(f"{pid} {cid}: not a cluster retype")
                continue
            old = (row.get("org_type") or "").strip()
            if old == new:
                skipped.append(f"{pid} {cid}: org_type already {new!r}")
                continue
            if apply:
                row["org_type"] = new
                add_note(row, f"publisher audit {TODAY} ({pid}): "
                              f"org_type {old or 'empty'} -> {new}. {rationale}")
                # A retype is a review act even though it sets no decision.
                stamp(row, reviewer)
            done.append(f"{pid} RETYPE     {cid} {old or '(empty)'} -> {new}")
            entries.append(log_entry(
                reviewer, "org_type_change", cid, f"org_type={new}",
                f"{pid}: {rationale}", before=old, after=new))

        elif kind == "not_an_org":
            cur = (row.get("decision") or "").strip()
            if cur == "NOT_AN_ORG":
                skipped.append(f"{pid} {cid}: already NOT_AN_ORG")
                continue
            if cur:
                skipped.append(f"{pid} {cid}: already decided {cur}; left alone")
                continue
            if apply:
                row["decision"] = "NOT_AN_ORG"
                add_note(row, f"NOT_AN_ORG {TODAY} (publisher audit {pid}): "
                              f"{rationale}")
                stamp(row, reviewer)
            done.append(f"{pid} NOT_AN_ORG {cid}")
            entries.append(log_entry(
                reviewer, "db_decision", cid, "NOT_AN_ORG", f"{pid}: {rationale}"))

        elif kind == "reject":
            # Records that this cluster is NOT the db row it scores against.
            # Leaves `decision` empty: the cluster still needs one of its own.
            note = (f"publisher audit {TODAY} ({pid}): NOT db {did} — "
                    f"{rationale}")
            if (row.get("reviewer_notes") or "").find(pid) >= 0:
                skipped.append(f"{pid} {cid}: rejection already noted")
                continue
            if apply:
                add_note(row, note)
                stamp(row, reviewer)
            done.append(f"{pid} REJECT     {cid} is NOT db {did}")
            entries.append(log_entry(
                reviewer, "pair_decision", cid, "REJECT_PAIR",
                f"{pid}: {rationale}", rejected_db_id=did))

    print(f"{'APPLY' if apply else 'DRY-RUN'}  "
          f"{len(done)} change(s), {len(skipped)} skipped\n")
    for line in done:
        print("  " + line)
    if skipped:
        print()
        for line in skipped:
            print("  skip " + line)

    if not apply:
        print("\nNothing written. Re-run with --apply to write.")
        return
    if not done:
        print("\nNothing to write; files untouched.")
        return

    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    for path, fields, rows in ((REVIEW, rv_fields, rv_rows),
                               (CORE_DB, db_fields, db_rows)):
        shutil.copy2(path, path.with_suffix(f".tsv.pre_publisher_clusters_{ts}"))
        _dump(path, fields, rows)
    n = log_rows(entries)
    print(f"\nwrote {REVIEW.name} and {CORE_DB.name} (backups .pre_publisher_clusters_{ts}); "
          f"logged {n} row(s) to {ACTIVITY_LOG.name}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", required=True,
                    help="who is making these decisions; stamped on every row")
    a = ap.parse_args()
    main(a.apply, a.reviewer)
