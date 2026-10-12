"""Apply the structural half of the publisher audit (reviewed 2026-10-10).

Source of truth: publisher_audit_decisions.tsv, and the HTML review surface
docs/Visualizations/publisher_audit.html built from it.

This script applies ONLY the two classes of decision that are mechanical and
reversible. Everything judgement-bearing — the cluster alignments, the mints,
the cluster re-typings, the duplicate merges — is left for review in the app,
because each one writes a link or a row that `feedback_core_db_link_authority`
says has no single authority to repair afterwards.

  out_of_project  db_ids 0-23, the modern Israeli trade-publisher block
                  (Am Oved, Keter, Zmora Bitan, Modan, Schocken, Sifriyat
                  Poalim, Yediot...). These are not Zylbercweig-corpus orgs and
                  no cluster will ever match them. Four of them were ALREADY
                  flagged (3, 4, 13, 14), which is the evidence that this block
                  was being flagged deliberately and the pass stopped halfway.
                  `out_of_project=true` is honoured by org_review.py and six
                  scripts, so flagging removes them from the matching queues
                  without deleting anything.

  restore_type    db_ids 47-65, a block whose `org_type` is EMPTY in core_db
                  while org_type_canonical_mapping.tsv records `Publisher` for
                  every one of them. The lost type is not cosmetic: it kept
                  these rows out of every publisher queue, which is why db 54
                  "Di internatsyonale bibliothek" was never noticed to duplicate
                  db 2332, and why the Amkroyt, Elizeum, Progres, Kunst, Munk
                  and Roth clusters all read as unmatchable.

Both are idempotent: a row already carrying the target value is skipped and NOT
logged, so a second --apply appends nothing. That is tested, not assumed — see
the module docstring note in apply_ruthie_doc_answers.py for why unconditional
logging produced phantom decisions there.

Every change is logged to activity_log.tsv stamped with --reviewer, which is
required alongside --apply. The app's zalmen/activity_log.py cannot be reused
from a CLI: it reads the reviewer from st.session_state and silently returns
when there is none.

Backup to core_db.tsv.pre_publisher_audit_<TS>. Dry-run by default.

Usage: python3 apply_publisher_audit.py --reviewer Sinai [--apply]
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
DECISIONS = HERE / "publisher_audit_decisions.tsv"
ACTIVITY_LOG = HERE / "activity_log.tsv"

LOG_COLUMNS = ("ts", "reviewer", "view", "action", "target_id", "decision",
               "note", "extra")
LOG_VIEW = "db_audit"
SOURCE = DECISIONS.name

# Only these two kinds are applied here; the rest stay in the review surface.
APPLIED_KINDS = ("out_of_project", "restore_type")


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

    The file is bare-LF up to 2026-08-30T08:28 and CRLF after, because the app
    switched terminators mid-life; it is ts-sorted, so an append belongs at the
    tail and takes the tail's CRLF.
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
    chunk = buf.getvalue().encode("utf-8")

    with ACTIVITY_LOG.open("ab") as f:        # append: never rewrite the file
        if not existing.endswith((b"\n", b"\r\n")):
            f.write(b"\r\n")
        f.write(chunk)
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


def main(apply: bool, reviewer: str) -> None:
    fields, db = _load(CORE_DB)
    for col in ("out_of_project", "org_type"):
        if col not in fields:
            raise SystemExit(f"core_db.tsv has no {col} column; aborting")

    by_id = {r["db_id"]: r for r in db}
    _, decisions = _load(DECISIONS)

    changes: list[tuple[str, str, str, str]] = []   # kind, db_id, before, after
    skipped: list[tuple[str, str, str]] = []        # kind, db_id, why
    entries: list[dict] = []

    for d in decisions:
        kind = (d.get("kind") or "").strip()
        if kind not in APPLIED_KINDS:
            continue
        db_id = (d.get("db_id") or "").strip()
        value = (d.get("value") or "").strip()
        row = by_id.get(db_id)
        if row is None:
            skipped.append((kind, db_id, "no such db_id in core_db"))
            continue

        col = "out_of_project" if kind == "out_of_project" else "org_type"
        before = (row.get(col) or "").strip()

        # Idempotence: already at the target value -> no write, no log.
        if before.lower() == value.lower():
            skipped.append((kind, db_id, f"{col} already {value!r}"))
            continue

        # Never silently overwrite a DIFFERENT non-empty org_type: the canonical
        # mapping only authorises filling a blank, not re-typing a decided row.
        if kind == "restore_type" and before:
            skipped.append((kind, db_id,
                            f"org_type is {before!r}, not blank; left alone"))
            continue

        if apply:
            row[col] = value
        changes.append((kind, db_id, before, value))
        name = row.get("name") or row.get("name_yiddish") or ""
        entries.append(log_entry(
            reviewer,
            "org_type_change" if kind == "restore_type" else "db_decision",
            db_id,
            f"{col}={value}",
            f"{d.get('decision_id','')}: {d.get('rationale','')}",
            column=col, before=before, after=value, name=name,
        ))

    print(f"{'APPLY' if apply else 'DRY-RUN'}  "
          f"{len(changes)} change(s), {len(skipped)} skipped\n")
    for kind, db_id, before, after in changes:
        nm = by_id[db_id].get("name") or by_id[db_id].get("name_yiddish") or ""
        print(f"  {kind:15s} db {db_id:5s} {before!r:12s} -> {after!r:12s} {nm[:40]}")
    if skipped:
        print()
        for kind, db_id, why in skipped:
            print(f"  skip {kind:15s} db {db_id:5s} {why}")

    if not apply:
        print("\nNothing written. Re-run with --apply to write.")
        return
    if not changes:
        print("\nNothing to write; core_db and activity_log untouched.")
        return

    stamp = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    backup = CORE_DB.with_suffix(f".tsv.pre_publisher_audit_{stamp}")
    shutil.copy2(CORE_DB, backup)
    _dump(CORE_DB, fields, db)
    n = log_rows(entries)
    print(f"\nwrote {CORE_DB.name} (backup {backup.name}); "
          f"logged {n} row(s) to {ACTIVITY_LOG.name}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", required=True,
                    help="who is making these decisions; stamped into activity_log")
    a = ap.parse_args()
    main(a.apply, a.reviewer)
