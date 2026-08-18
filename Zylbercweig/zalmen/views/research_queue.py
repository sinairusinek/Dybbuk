"""Shared research-queue writer used by org-alignment Defer and settlement-audit
Research actions.

A deferred entry is one the reviewer has chosen not to decide on yet because it
needs out-of-corpus research. The cluster-research skill consumes
`research_queue.tsv` to know what to investigate. The optional `comment` field
lets the reviewer pin a specific question/focus on the row.
"""
from __future__ import annotations

import csv
import datetime as _dt
import pathlib
from atomic_io import atomic_write

BASE = pathlib.Path(__file__).parents[2]
RESEARCH_QUEUE = BASE / "organizations" / "research_queue.tsv"
RESEARCH_HEADERS = [
    "queued_at", "reviewer", "kind", "target_id", "qid", "org_type",
    "label", "comment", "status",
]
ADMIN_REVIEWER = "Sinai"


def _migrate_if_needed() -> None:
    """If an older queue exists without the `comment` column, rewrite it with
    the canonical headers so writers and the cluster-research skill agree."""
    if not RESEARCH_QUEUE.exists():
        return
    with RESEARCH_QUEUE.open(newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        existing = list(r.fieldnames or [])
        rows = list(r)
    if existing == RESEARCH_HEADERS:
        return
    for row in rows:
        for h in RESEARCH_HEADERS:
            row.setdefault(h, "")
    with atomic_write(RESEARCH_QUEUE) as f:
        w = csv.DictWriter(f, fieldnames=RESEARCH_HEADERS, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def queue_research(
    *,
    kind: str,
    target_id: str,
    qid: str,
    org_type: str,
    label: str,
    reviewer: str,
    comment: str = "",
) -> None:
    _migrate_if_needed()
    is_new = not RESEARCH_QUEUE.exists()
    with RESEARCH_QUEUE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RESEARCH_HEADERS, delimiter="\t")
        if is_new:
            w.writeheader()
        w.writerow({
            "queued_at": _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "reviewer": reviewer,
            "kind": kind,
            "target_id": target_id,
            "qid": qid,
            "org_type": org_type,
            "label": label,
            "comment": comment,
            "status": "queued",
        })
