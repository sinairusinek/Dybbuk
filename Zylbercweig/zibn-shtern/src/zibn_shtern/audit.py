from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


@dataclass
class AuditRules:
    required_columns: list[str]
    source_place_column: str
    trim_whitespace: bool
    collapse_internal_spaces: bool
    flag_empty: bool
    flag_duplicate_exact: bool
    suspicious_regex: str | None
    very_short_threshold: int


def load_audit_rules(path: str | Path) -> AuditRules:
    with Path(path).open("r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    normalization = raw.get("normalization", {})
    flags = raw.get("flags", {})

    return AuditRules(
        required_columns=raw.get("required_columns", ["place"]),
        source_place_column=str(raw.get("source_place_column", "place")),
        trim_whitespace=bool(normalization.get("trim_whitespace", True)),
        collapse_internal_spaces=bool(normalization.get("collapse_internal_spaces", True)),
        flag_empty=bool(flags.get("empty_place_name", True)),
        flag_duplicate_exact=bool(flags.get("duplicate_exact", True)),
        suspicious_regex=flags.get("suspicious_characters_regex"),
        very_short_threshold=int(flags.get("very_short_threshold", 2)),
    )


def _normalize_name(value: Any, rules: AuditRules) -> str:
    if pd.isna(value):
        return ""

    normalized = str(value)
    if rules.trim_whitespace:
        normalized = normalized.strip()
    if rules.collapse_internal_spaces:
        normalized = " ".join(normalized.split())
    return normalized


def audit_dataframe(df: pd.DataFrame, rules: AuditRules) -> pd.DataFrame:
    missing = [col for col in rules.required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    place_col = rules.source_place_column
    if place_col not in df.columns:
        raise ValueError(f"Configured source place column not found: {place_col}")

    out = df.copy()
    out["place_name_original"] = out[place_col]
    out["place_name_normalized"] = out[place_col].apply(lambda x: _normalize_name(x, rules))

    if rules.flag_empty:
        out["flag_empty_place_name"] = out["place_name_normalized"].eq("")
    else:
        out["flag_empty_place_name"] = False

    if rules.flag_duplicate_exact:
        out["flag_duplicate_exact"] = out.duplicated(subset=["place_name_normalized"], keep=False)
    else:
        out["flag_duplicate_exact"] = False

    out["flag_very_short"] = out["place_name_normalized"].str.len().fillna(0) <= rules.very_short_threshold

    if rules.suspicious_regex:
        pattern = re.compile(rules.suspicious_regex)
        out["flag_suspicious_characters"] = out["place_name_normalized"].apply(
            lambda x: bool(pattern.search(x)) if x else False
        )
    else:
        out["flag_suspicious_characters"] = False

    out["needs_review"] = out[
        [
            "flag_empty_place_name",
            "flag_duplicate_exact",
            "flag_very_short",
            "flag_suspicious_characters",
        ]
    ].any(axis=1)

    return out
