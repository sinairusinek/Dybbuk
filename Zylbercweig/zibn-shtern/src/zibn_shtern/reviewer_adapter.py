"""
reviewer_adapter.py
-------------------
Adapter connecting the Zylbercweig Kimatch review queue to the generic
Kimatch ReviewBackend interface, so the Kimatch Streamlit review page can
be used to work through fuzzy / no-match places.

Data contract
~~~~~~~~~~~~~
Input  : data/working/kimatch_review.tsv
         Columns: source_value, english_name, wikidata_qid, match_status,
                  fuzzy_candidates (JSON), fuzzy_confidence, entry_ids, contexts

Decisions: data/working/kimatch_decisions.json
           Dict keyed by source_value → {"action": str, "kima_id": str, "notes": str}

Actions
~~~~~~~
  map_to:<kima_id>   — confirmed match; source_value is a new variant of kima_id
  skip               — not a real place / irrelevant
  ambiguous          — cannot decide now
  no_match           — confirmed not in Kima (genuine gap)
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ── locate repos ──────────────────────────────────────────────────────────────
_HERE      = Path(__file__).resolve()
_ZIBN_ROOT = _HERE.parents[2]          # zibn-shtern/

# On Streamlit Cloud the Kimatch repo is the app root (cwd).
# Locally it lives at the hardcoded path below.
_KIMATCH_LOCAL = Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
_KIMATCH = _KIMATCH_LOCAL if _KIMATCH_LOCAL.exists() else Path.cwd()

if str(_KIMATCH) not in sys.path:
    sys.path.insert(0, str(_KIMATCH))

from kimatch.data.loader import KimaDB
from kimatch.review.backend import ReviewBackend
from kimatch.review.models import ActionOption, Candidate, ReviewItem

# ── paths ─────────────────────────────────────────────────────────────────────
# Review TSV and decisions JSON are stored in the Kimatch repo so they are
# accessible (and writable via GitHub API) on Streamlit Cloud.
_KIMATCH_DATA = _KIMATCH / "data" / "zylbercweig"
_REVIEW_TSV   = _KIMATCH_DATA / "kimatch_review_full.tsv"
_DECISIONS    = _KIMATCH_DATA / "kimatch_decisions_full.json"
_PLACES_CSV   = _KIMATCH / "20250126KimaPlacesCSVx.csv"
_VARIANTS_TSV = _KIMATCH / "Kima-Variants-20250929.tsv"

# Repo-relative path used when pushing decisions to GitHub
_DECISIONS_REPO_PATH = "data/zylbercweig/kimatch_decisions_full.json"


# ── legacy stub (kept for backwards-compat with old pipeline code) ─────────────

@dataclass
class ReviewerDecision:
    row_id: Any
    status: str
    canonical_name: str | None = None
    notes: str | None = None


def build_review_queue(audit_df):  # type: ignore[no-untyped-def]
    """Legacy stub — create a basic review queue from an audit DataFrame."""
    import pandas as pd

    queue = audit_df.loc[audit_df["needs_review"]].copy()
    queue["review_status"] = "pending"
    queue["canonical_name"] = ""
    queue["review_notes"] = ""
    preferred = [
        "source_id", "place_name", "place_name_normalized", "language",
        "context", "page_ref", "review_status", "canonical_name", "review_notes",
    ]
    cols = [c for c in preferred if c in queue.columns]
    extras = [c for c in queue.columns if c not in cols]
    return queue[cols + extras]


# ── ZylbercweigReviewBackend ──────────────────────────────────────────────────

class ZylbercweigReviewBackend(ReviewBackend):
    """ReviewBackend for working through kimatch_review.tsv in the Kimatch UI."""

    def __init__(
        self,
        review_tsv: Path = _REVIEW_TSV,
        decisions_json: Path = _DECISIONS,
        places_csv: Path = _PLACES_CSV,
        variants_tsv: Path = _VARIANTS_TSV,
    ) -> None:
        self._review_tsv    = review_tsv
        self._decisions_json = decisions_json
        self._places_csv    = places_csv
        self._variants_tsv  = variants_tsv

        self._items: list[ReviewItem] = []
        self._decisions: dict[str, dict] = {}
        self.db: KimaDB | None = None
        # Optional category pre-filter; empty set = no filter (show all)
        self.category_filter: set[str] = set()

    # ── load ──────────────────────────────────────────────────────────────────

    def load(self) -> None:
        import csv

        self.db = KimaDB.load(
            places_csv=self._places_csv,
            variants_tsv=self._variants_tsv,
        )

        self._items = []
        with self._review_tsv.open(encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh, delimiter="\t"):
                sv          = row.get("source_value", "").strip()
                en          = row.get("english_name", "").strip()
                qid         = row.get("wikidata_qid", "").strip()
                status      = row.get("match_status", "").strip()
                confidence  = row.get("fuzzy_confidence", "").strip()
                entry_ids   = row.get("entry_ids", "").strip()
                contexts_raw = row.get("contexts", "").strip()

                try:
                    candidates_raw = json.loads(row.get("fuzzy_candidates", "[]") or "[]")
                except json.JSONDecodeError:
                    candidates_raw = []

                contexts = [c for c in contexts_raw.split("|") if c]

                wikidata_yi       = row.get("wikidata_yi", "").strip()
                resolved_category = row.get("resolved_category", "").strip()

                item = ReviewItem(
                    name=sv,
                    match_status=status.lower(),
                    confidence=confidence,
                    contexts=contexts,
                    metadata={
                        "english_name":      en,
                        "wikidata_qid":      qid,
                        "wikidata_yi":       wikidata_yi,
                        "entry_ids":         entry_ids,
                        "occurrences":       str(len(entry_ids.split("|")) if entry_ids else 0),
                        "fuzzy_candidates_raw": candidates_raw,
                        "resolved_category": resolved_category,
                    },
                    raw=row,
                )
                self._items.append(item)

        # Load persisted decisions
        if self._decisions_json.exists():
            with self._decisions_json.open(encoding="utf-8") as fh:
                self._decisions = json.load(fh)
        else:
            self._decisions = {}

    # ── items ─────────────────────────────────────────────────────────────────

    def get_items(self) -> list[ReviewItem]:
        if not self.category_filter:
            return self._items
        return [
            item for item in self._items
            if (item.metadata.get("resolved_category") or "other") in self.category_filter
        ]

    def get_candidates(self, item: ReviewItem) -> list[Candidate]:
        if self.db is None:
            return []
        candidates = []
        for c in item.metadata.get("fuzzy_candidates_raw", []):
            kima_id = c.get("kima_id")
            kplace  = self.db.get(int(kima_id)) if kima_id else None

            names: dict[str, str] = {
                "rom": c.get("rom", ""),
                "heb": c.get("heb", ""),
            }

            if kplace:
                # External IDs
                if kplace.wikidata_id:
                    names["wikidata"] = kplace.wikidata_id
                if kplace.geonames_id:
                    names["geonames"] = kplace.geonames_id
                if kplace.viaf_id:
                    names["viaf"] = kplace.viaf_id

                # Existing Yiddish/Hebrew variants (first 5)
                existing = [
                    v.variant for v in self.db.get_variants(kplace.kima_id)
                    if v.variant
                ]
                if existing:
                    names["variants"] = " · ".join(existing[:5])
                    if len(existing) > 5:
                        names["variants"] += f" … (+{len(existing)-5})"

            candidates.append(Candidate(
                authority_id=f"kima:{kima_id}",
                names=names,
                url=kplace.kima_url if kplace else None,
            ))
        return candidates

    def get_action_options(self, item: ReviewItem, candidates: list[Candidate]) -> list[ActionOption]:
        options = []

        # One "map to" option per fuzzy candidate
        for c in candidates:
            kima_id = c.authority_id.replace("kima:", "")
            rom     = c.names.get("rom", "")
            options.append(ActionOption(
                label=f"Map to kima:{kima_id} — {rom}",
                action=f"map_to:{kima_id}",
                suggested_id=kima_id,
            ))

        # Manual kima ID entry
        options.append(ActionOption(
            label="Map to a different Kima ID (enter below)",
            action="map_to:__manual__",
            suggested_id="",
        ))

        # No match found by our methods (doesn't mean it's not in Kima)
        options.append(ActionOption(
            label="No match found",
            action="no_match_found",
            suggested_id="",
        ))

        return options

    # ── decisions ─────────────────────────────────────────────────────────────

    def get_decision(self, name: str) -> tuple[str, str]:
        dec = self._decisions.get(name, {})
        return dec.get("action", ""), dec.get("kima_id", "")

    def save_decision(self, name: str, action: str, suggested_id: str) -> None:
        self._decisions[name] = {
            "action":  action,
            "kima_id": suggested_id,
        }
        self._decisions_json.parent.mkdir(parents=True, exist_ok=True)
        with self._decisions_json.open("w", encoding="utf-8") as fh:
            json.dump(self._decisions, fh, ensure_ascii=False, indent=2)

        # Push to GitHub so decisions persist across Streamlit Cloud redeploys.
        # Fails silently if credentials are absent (local dev); shows warning if
        # credentials exist but push fails.
        try:
            import streamlit as st
            from kimatch.review.github_sync import push_file_to_github
            ok = push_file_to_github(
                _DECISIONS_REPO_PATH,
                self._decisions_json,
                f"chore: save kimatch decision for '{name}'",
            )
            if not ok:
                st.toast(
                    "⚠️ Your decision was recorded but could not be saved to GitHub. "
                    "Please contact Sinai before continuing.",
                    icon="⚠️",
                )
        except Exception:
            pass  # streamlit not available (CLI context) — skip silently

    # ── filtering ─────────────────────────────────────────────────────────────

    def classify_item(self, item: ReviewItem) -> str:
        action, _ = self.get_decision(item.name)
        if action == "ambiguous":
            return "ambiguous"
        if action:
            return "auto"   # decided (map_to / no_match_found / skip)
        # Undecided — split by original match status so user can filter
        # "fuzzy" (had candidates) vs "no_match" (nothing found at all)
        return item.match_status  # "fuzzy" or "no_match"

    def classify_item_category(self, item: ReviewItem) -> str:
        """Return the resolved_category for this item, or 'other'."""
        return item.metadata.get("resolved_category", "") or "other"

    # ── search ────────────────────────────────────────────────────────────────

    def search_authority(self, query: str) -> list[Candidate]:
        """Search KimaDB by substring and return matching candidates."""
        if not self.db or not query.strip():
            return []
        hits = self.db.search_by_substring(query.strip())
        results = []
        for place in hits[:10]:
            names: dict[str, str] = {
                "rom": place.primary_rom,
                "heb": place.primary_heb,
            }
            if place.wikidata_id:
                names["wikidata"] = place.wikidata_id
            existing = [v.variant for v in self.db.get_variants(place.kima_id) if v.variant]
            if existing:
                names["variants"] = " · ".join(existing[:5])
                if len(existing) > 5:
                    names["variants"] += f" … (+{len(existing)-5})"
            results.append(Candidate(
                authority_id=f"kima:{place.kima_id}",
                names=names,
                url=place.kima_url,
            ))
        return results

    # ── commit ────────────────────────────────────────────────────────────────

    def commit(self) -> tuple[bool, str]:
        decided = sum(1 for d in self._decisions.values() if d.get("action"))
        return True, f"{decided} decision(s) saved to {self._decisions_json.name}"
