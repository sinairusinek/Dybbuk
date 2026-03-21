from __future__ import annotations

import argparse

from zibn_shtern.io import load_places
from zibn_shtern.kimatch_bridge import export_for_kimatch


def main() -> None:
    parser = argparse.ArgumentParser(description="Export corrected rows in Kimatch-friendly shape")
    parser.add_argument("--input", required=True, help="Corrected CSV/JSON")
    parser.add_argument("--output", required=True, help="Output CSV path for Kimatch")
    args = parser.parse_args()

    df = load_places(args.input)
    export_for_kimatch(df, args.output)


if __name__ == "__main__":
    main()
