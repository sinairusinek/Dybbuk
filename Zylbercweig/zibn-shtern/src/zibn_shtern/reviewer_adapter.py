from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class ReviewerDecision:
    row_id: Any
    status: str
    canonical_name: str | None = None
    notes: str | None = None


def build_review_queue(audit_df: pd.DataFrame) -> pd.DataFrame:
    """Create a queue consumable by a reviewer UI/workflow.

    This structure is intentionally simple so it can be remapped to the
    Hasidigital kima reviewer model once that schema is imported.
    """
    queue = audit_df.loc[audit_df["needs_review"]].copy()

    queue["review_status"] = "pending"
    queue["canonical_name"] = ""
    queue["review_notes"] = ""

    preferred = [
        "source_id",
        "place_name",
        "place_name_normalized",
        "language",
        "context",
        "page_ref",
        "flag_empty_place_name",
        "flag_duplicate_exact",
        "flag_very_short",
        "flag_suspicious_characters",
        "review_status",
        "canonical_name",
        "review_notes",
    ]
    cols = [c for c in preferred if c in queue.columns]
    extras = [c for c in queue.columns if c not in cols]
    return queue[cols + extras]
