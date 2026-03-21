from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml


def load_kimatch_config(config_path: str | Path) -> dict:
    with Path(config_path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def export_for_kimatch(df: pd.DataFrame, output_path: str | Path) -> None:
    """Minimal exporter placeholder for Kimatch ingestion.

    Expected to be adapted once Kimatch input schema is finalized.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cols = [c for c in ["source_id", "place_name", "place_name_normalized", "language"] if c in df.columns]
    if not cols:
        cols = list(df.columns)

    df[cols].to_csv(output_path, index=False)
