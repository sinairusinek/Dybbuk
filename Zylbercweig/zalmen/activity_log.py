"""Append-only activity log.

Records every reviewer action (cluster pair decision, alignment decision,
address/settlement confirmation, etc.) as one row in
`organizations/activity_log.tsv` and pushes the file to GitHub so the log
survives Streamlit Cloud redeploys.

Existing views also stamp `reviewer`/`reviewed_at` columns on their own
review TSVs; this log is a parallel, full-coverage audit trail used by the
Activity tab.
"""

from __future__ import annotations

import csv
import json
import pathlib
from datetime import datetime, timezone

import streamlit as st
from atomic_io import atomic_write

LOG_PATH = pathlib.Path(__file__).parents[1] / "organizations" / "activity_log.tsv"
REPO_PATH = "Zylbercweig/organizations/activity_log.tsv"

COLUMNS = ("ts", "reviewer", "view", "action", "target_id", "decision", "note", "extra")


def _ensure_header() -> None:
    if LOG_PATH.exists() and LOG_PATH.stat().st_size > 0:
        return
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with atomic_write(LOG_PATH) as f:
        csv.writer(f, delimiter="\t").writerow(COLUMNS)


def log_action(
    view: str,
    action: str,
    target_id: str = "",
    decision: str = "",
    note: str = "",
    *,
    push: bool = True,
    **extra,
) -> None:
    """Append one row to the activity log and (optionally) push to GitHub.

    `view`     — e.g. "org_clusters", "org_review", "org_addresses".
    `action`   — short verb, e.g. "pair_decision", "alignment", "address_save".
    `target_id`— cluster_id / pair_id / db_id / xml_id, whatever identifies the row.
    `decision` — e.g. MERGE, SPLIT, ALIGN, NEW_ORG, DISMISS, etc.
    `note`     — short free-text context.
    `extra`    — additional fields serialized as JSON in the `extra` column.
    """
    reviewer = st.session_state.get("reviewer", "") if hasattr(st, "session_state") else ""
    if not reviewer:
        return  # don't pollute the log with unattributed actions

    _ensure_header()
    row = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        reviewer,
        view,
        action,
        target_id,
        decision,
        note.replace("\n", " ").replace("\t", " ").strip()[:500],
        json.dumps(extra, ensure_ascii=False) if extra else "",
    )
    with open(LOG_PATH, "a", encoding="utf-8", newline="") as f:
        csv.writer(f, delimiter="\t").writerow(row)

    if push:
        try:
            from zalmen.github_sync import push_file_to_github
            push_file_to_github(REPO_PATH, LOG_PATH, f"chore: log {view}/{action}")
        except Exception:
            pass
