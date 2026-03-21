from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_places(path: str | Path) -> pd.DataFrame:
    """Load place data from CSV, TSV, JSON, or Excel."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".tsv", ".txt"}:
        return pd.read_csv(path, sep="\t")
    if suffix == ".json":
        return pd.read_json(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    raise ValueError(f"Unsupported input format: {suffix}")


def save_dataframe(df: pd.DataFrame, path: str | Path) -> None:
    """Persist dataframe in CSV, TSV, JSON, or Excel format."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
        return
    if suffix in {".tsv", ".txt"}:
        df.to_csv(path, sep="\t", index=False)
        return
    if suffix == ".json":
        df.to_json(path, orient="records", force_ascii=False, indent=2)
        return
    if suffix in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
        return

    raise ValueError(f"Unsupported output format: {suffix}")
