"""Tests for zibn_shtern.corrections — fix_death_site_burial, fix_city_state,
fix_column_assignment, and fix_qid_overrides.

All tests use synthetic DataFrames; no live Wikidata calls are made.
fix_city_state tests patch the wikidata_client names at the point where
corrections.py imported them (zibn_shtern.corrections.*).
"""
from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from zibn_shtern.corrections import (
    fix_city_state,
    fix_column_assignment,
    fix_death_site_burial,
    fix_qid_overrides,
)
from zibn_shtern.triage import UNIFIED_COLUMNS, ensure_unified_schema

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

_COLUMN_DEFAULTS: dict[str, object] = {
    "entry_id": "e1",
    "context": "death_place",
    "source_role": "place",
    "source_value": "Lodz",
    "clustered_value": "lodz",
    "qid": "Q580",
    "qid_source": "place",
    "place_qid_conflict": False,
    "wikidata_label_en": "Lodz",
    "wikidata_label_yi": "",
    "wikidata_type": "city",
    "resolved_category": "settlement",
    "other_type": "",
    "cemetery": "",
    "burial_city": "",
    "death_site": "",
    "settlement": "Q580",
    "province": "",
    "country": "",
    "neighborhood": "",
    "other": "",
    "review_flags": "",
    "needs_review": False,
    "correction_applied": "",
    "death_burial_conflict": False,
}


def _make_df(**overrides) -> pd.DataFrame:
    """Return a single-row unified DataFrame with *overrides* applied."""
    row = {**_COLUMN_DEFAULTS, **overrides}
    return ensure_unified_schema(pd.DataFrame([row]))


# ---------------------------------------------------------------------------
# Wikidata detail stubs used by fix_city_state tests
# ---------------------------------------------------------------------------

# A synthetic city detail that already has p31_labels so _enrich_detail
# skips the secondary fetch_qid_labels call.
_DETAIL_CITY_Q999: dict = {
    "label_en": "Lodz City",
    "label_yi": "לאדז שטאט",
    "p31": ["Q515"],
    "p17": ["Q36"],
    "p131": [],
    "p31_labels": ["city"],         # satisfies SETTLEMENT_KEYWORDS check
    "p17_labels": ["Poland"],
    "p131_labels": [],
}

_CANDIDATE_Q999 = {
    "qid": "Q999",
    "description": "city in Poland",   # "city" in description triggers fast path
}

_MOCK_CACHE_PATH = "/tmp/test_qid_cache.json"


# ===========================================================================
# fix_death_site_burial
# ===========================================================================

class TestFixDeathSiteBurial:
    """Pure DataFrame transform — no mocking required."""

    def test_death_to_burial_mirrored(self) -> None:
        df = _make_df(source_role="place", death_site="Q7342", burial_city="")
        out = fix_death_site_burial(df)
        assert out.at[0, "burial_city"] == "Q7342"
        assert out.at[0, "death_site"] == "Q7342"
        assert out.at[0, "correction_applied"] == "death_burial_mirrored"

    def test_burial_to_death_mirrored(self) -> None:
        df = _make_df(source_role="place", death_site="", burial_city="Q7342")
        out = fix_death_site_burial(df)
        assert out.at[0, "death_site"] == "Q7342"
        assert out.at[0, "burial_city"] == "Q7342"
        assert out.at[0, "correction_applied"] == "death_burial_mirrored"

    def test_same_value_no_change(self) -> None:
        """Both fields equal → no copy needed, no correction stamp, no conflict."""
        df = _make_df(source_role="place", death_site="Q7342", burial_city="Q7342")
        out = fix_death_site_burial(df)
        assert out.at[0, "death_site"] == "Q7342"
        assert out.at[0, "burial_city"] == "Q7342"
        assert out.at[0, "correction_applied"] == ""
        assert out.at[0, "death_burial_conflict"] is False or out.at[0, "death_burial_conflict"] == False

    def test_conflict_different_values(self) -> None:
        """Both populated with different values → conflict flag, both preserved."""
        df = _make_df(source_role="place", death_site="Q7342", burial_city="Q9999")
        out = fix_death_site_burial(df)
        assert out.at[0, "death_site"] == "Q7342"
        assert out.at[0, "burial_city"] == "Q9999"
        assert out.at[0, "death_burial_conflict"] == True
        # Conflict rows are NOT stamped as mirrored.
        assert out.at[0, "correction_applied"] == ""

    def test_non_place_role_untouched(self) -> None:
        """source_role != 'place' must not be touched at all."""
        df = _make_df(source_role="country", death_site="Q7342", burial_city="")
        out = fix_death_site_burial(df)
        assert out.at[0, "burial_city"] == ""
        assert out.at[0, "correction_applied"] == ""

    def test_both_empty_no_change(self) -> None:
        df = _make_df(source_role="place", death_site="", burial_city="")
        out = fix_death_site_burial(df)
        assert out.at[0, "death_site"] == ""
        assert out.at[0, "burial_city"] == ""
        assert out.at[0, "correction_applied"] == ""

    def test_existing_correction_not_overwritten(self) -> None:
        """An already-stamped correction_applied must not be replaced."""
        df = _make_df(
            source_role="place",
            death_site="Q7342",
            burial_city="",
            correction_applied="state_to_city",
        )
        out = fix_death_site_burial(df)
        # Copy still happens (value is mirrored)…
        assert out.at[0, "burial_city"] == "Q7342"
        # …but the earlier stamp is preserved.
        assert out.at[0, "correction_applied"] == "state_to_city"


# ===========================================================================
# fix_city_state
# ===========================================================================

class TestFixCityState:
    """Province-typed place rows: substitute a city QID when one is found."""

    def _province_place(self, **overrides) -> pd.DataFrame:
        return _make_df(
            source_role="place",
            resolved_category="province",
            wikidata_label_en="Lodz Province",
            settlement="",
            province="Lodz Province",
            **overrides,
        )

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={})
    @patch("zibn_shtern.corrections.fetch_entity_data", return_value=_DETAIL_CITY_Q999)
    @patch("zibn_shtern.corrections.fetch_qid_labels", return_value={})
    @patch(
        "zibn_shtern.corrections.search_settlement_by_label",
        return_value=[_CANDIDATE_Q999],
    )
    def test_city_candidate_found(
        self, mock_search, mock_labels, mock_entity, mock_load, mock_save
    ) -> None:
        df = self._province_place()
        details: dict = {}
        out = fix_city_state(df, details, _MOCK_CACHE_PATH)

        assert out.at[0, "qid"] == "Q999"
        assert out.at[0, "resolved_category"] == "settlement"
        assert out.at[0, "correction_applied"] == "state_to_city"
        assert out.at[0, "wikidata_label_en"] == "Lodz City"
        assert out.at[0, "settlement"] == "Lodz City"
        # details dict must be updated as a side-effect
        assert "Q999" in details

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={})
    @patch("zibn_shtern.corrections.fetch_entity_data", return_value=None)
    @patch("zibn_shtern.corrections.fetch_qid_labels", return_value={})
    @patch(
        "zibn_shtern.corrections.search_settlement_by_label",
        return_value=[{"qid": "Q999", "description": ""}],
    )
    def test_no_city_found_row_unchanged(
        self, mock_search, mock_labels, mock_entity, mock_load, mock_save
    ) -> None:
        df = self._province_place()
        original_qid = df.at[0, "qid"]
        details: dict = {}
        out = fix_city_state(df, details, _MOCK_CACHE_PATH)

        assert out.at[0, "qid"] == original_qid
        assert out.at[0, "correction_applied"] == ""

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={})
    @patch("zibn_shtern.corrections.search_settlement_by_label", return_value=[])
    def test_wrong_source_role_skipped(
        self, mock_search, mock_load, mock_save
    ) -> None:
        """source_role != 'place' → mask is empty, no search is performed."""
        df = _make_df(source_role="country", resolved_category="province")
        details: dict = {}
        out = fix_city_state(df, details, _MOCK_CACHE_PATH)

        mock_search.assert_not_called()
        assert out.at[0, "correction_applied"] == ""

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={})
    @patch("zibn_shtern.corrections.search_settlement_by_label", return_value=[])
    def test_wrong_category_skipped(
        self, mock_search, mock_load, mock_save
    ) -> None:
        """resolved_category != 'province' → mask is empty, no search is performed."""
        df = _make_df(source_role="place", resolved_category="settlement")
        details: dict = {}
        out = fix_city_state(df, details, _MOCK_CACHE_PATH)

        mock_search.assert_not_called()
        assert out.at[0, "correction_applied"] == ""


# ===========================================================================
# fix_column_assignment
# ===========================================================================

class TestFixColumnAssignment:
    """Misfiled role reassignment — no Wikidata calls, uses a details dict."""

    def test_place_province_moved_to_province(self) -> None:
        df = _make_df(
            source_role="place",
            resolved_category="province",
            settlement="",
            province="Masovia",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "province"
        assert out.at[0, "correction_applied"] == "moved_to_province"

    def test_place_country_moved_to_country(self) -> None:
        df = _make_df(
            source_role="place",
            resolved_category="country",
            settlement="",
            country="Poland",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "country"
        assert out.at[0, "correction_applied"] == "moved_to_country"

    def test_country_role_province_category_moved(self) -> None:
        df = _make_df(
            source_role="country",
            resolved_category="province",
            country="",
            province="Masovia",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "province"
        assert out.at[0, "correction_applied"] == "moved_to_province"

    def test_country_role_settlement_category_moved(self) -> None:
        """Country-role row whose QID is actually a settlement is reassigned."""
        df = _make_df(
            source_role="country",
            resolved_category="settlement",
            country="",
            settlement="Lodz",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "settlement"
        assert out.at[0, "correction_applied"] == "moved_to_settlement"
        assert out.at[0, "country"] == ""
        assert out.at[0, "settlement"] == "Lodz"

    def test_country_role_other_non_country_type_moved(self) -> None:
        """Country-role row with explicit non-country other_type is reassigned."""
        df = _make_df(
            source_role="country",
            resolved_category="other",
            other_type="peninsula",
            country="",
            other="Crimea",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "other"
        assert out.at[0, "correction_applied"] == "moved_to_other"
        assert out.at[0, "country"] == ""
        assert out.at[0, "other"] == "Lodz"

    def test_province_role_country_category_moved(self) -> None:
        df = _make_df(
            source_role="province",
            resolved_category="country",
            province="",
            country="Poland",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "country"
        assert out.at[0, "correction_applied"] == "moved_to_country"

    def test_province_role_is_country_qid_moved(self) -> None:
        """Province-role row whose QID is_country() → moved to country with
        resolved_category and category columns updated."""
        qid = "Q36"
        details = {
            qid: {
                "p31": ["Q6256"],
                "p17": [],
                "p131": [],
                "p31_labels": ["sovereign state"],  # matches COUNTRY_KEYWORDS
                "p17_labels": [],
                "p131_labels": [],
            }
        }
        # resolved_category is "other" (not "country" or "province") so the
        # is_country branch fires, not the category-equality branches.
        df = _make_df(
            source_role="province",
            resolved_category="other",
            qid=qid,
            province="Poland",
            other_type="sovereign state",
        )
        out = fix_column_assignment(df, details=details)
        assert out.at[0, "source_role"] == "country"
        assert out.at[0, "correction_applied"] == "moved_to_country"
        assert out.at[0, "resolved_category"] == "country"
        assert out.at[0, "other_type"] == ""

    def test_already_corrected_row_skipped(self) -> None:
        """Rows with a non-empty correction_applied are left untouched."""
        df = _make_df(
            source_role="place",
            resolved_category="province",
            correction_applied="state_to_city",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "place"
        assert out.at[0, "correction_applied"] == "state_to_city"

    def test_settlement_in_province_column_left_for_review(self) -> None:
        """A settlement QID filed under source_role='province' is left for humans."""
        df = _make_df(
            source_role="province",
            resolved_category="settlement",
            settlement="Lodz",
            province="",
        )
        out = fix_column_assignment(df, details={})
        assert out.at[0, "source_role"] == "province"
        assert out.at[0, "correction_applied"] == ""


# ===========================================================================
# fix_qid_overrides
# ===========================================================================

# Synthetic Wikidata detail for Q7835 (Crimea)
_DETAIL_CRIMEA_Q7835: dict = {
    "label_en": "Crimea",
    "label_yi": "קרים",
    "p31": ["Q34763", "Q15239622"],
    "p17": ["Q212"],
    "p131": [],
    "p31_labels": ["peninsula", "disputed territory"],
    "p17_labels": ["Ukraine"],
    "p131_labels": [],
}


class TestFixQidOverrides:
    """QID substitution and category overrides — patches load/save cache."""

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={"Q7835": _DETAIL_CRIMEA_Q7835})
    def test_cream_tea_replaced_with_crimea(
        self, mock_load, mock_save
    ) -> None:
        """Q1139315 (cream tea) in province role → Q7835 (Crimea), province."""
        df = _make_df(
            source_role="province",
            qid="Q1139315",
            clustered_value="cream tea",
            wikidata_label_en="cream tea",
            resolved_category="other",
            other_type="unknown",
            other="cream tea",
        )
        details: dict = {}
        out = fix_qid_overrides(df, details, _MOCK_CACHE_PATH)

        assert out.at[0, "qid"] == "Q7835"
        assert out.at[0, "wikidata_label_en"] == "Crimea"
        assert out.at[0, "wikidata_label_yi"] == "קרים"
        assert out.at[0, "resolved_category"] == "province"
        assert out.at[0, "source_role"] == "province"
        assert out.at[0, "correction_applied"] == "qid_override"
        assert out.at[0, "province"] == "Crimea"
        assert out.at[0, "other"] == ""
        assert "Q7835" in details

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={"Q7835": _DETAIL_CRIMEA_Q7835})
    def test_category_override_existing_crimea(
        self, mock_load, mock_save
    ) -> None:
        """Q7835 already present but classified as other/peninsula → province."""
        df = _make_df(
            source_role="country",
            qid="Q7835",
            wikidata_label_en="Crimea",
            resolved_category="other",
            other_type="peninsula",
            other="Crimea",
        )
        details: dict = {}
        out = fix_qid_overrides(df, details, _MOCK_CACHE_PATH)

        assert out.at[0, "resolved_category"] == "province"
        assert out.at[0, "source_role"] == "province"
        assert out.at[0, "correction_applied"] == "category_override"
        assert out.at[0, "province"] == "Crimea"
        assert out.at[0, "other"] == ""
        assert out.at[0, "other_type"] == ""

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={})
    def test_already_corrected_row_skipped(
        self, mock_load, mock_save
    ) -> None:
        """Rows with existing correction_applied are not touched."""
        df = _make_df(
            source_role="province",
            qid="Q1139315",
            correction_applied="state_to_city",
        )
        details: dict = {}
        out = fix_qid_overrides(df, details, _MOCK_CACHE_PATH)

        assert out.at[0, "qid"] == "Q1139315"
        assert out.at[0, "correction_applied"] == "state_to_city"

    @patch("zibn_shtern.corrections.save_cache")
    @patch("zibn_shtern.corrections.load_cache", return_value={})
    def test_non_matching_qid_untouched(
        self, mock_load, mock_save
    ) -> None:
        """A QID not in QID_OVERRIDES or CATEGORY_OVERRIDES is left alone."""
        df = _make_df(
            source_role="province",
            qid="Q999999",
        )
        details: dict = {}
        out = fix_qid_overrides(df, details, _MOCK_CACHE_PATH)

        assert out.at[0, "qid"] == "Q999999"
        assert out.at[0, "correction_applied"] == ""
