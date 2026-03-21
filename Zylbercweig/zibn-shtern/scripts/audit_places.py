from __future__ import annotations

import argparse

from zibn_shtern.audit import audit_dataframe, load_audit_rules
from zibn_shtern.io import load_places, save_dataframe


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit extracted place names")
    parser.add_argument("--input", required=True, help="Input CSV/TSV/JSON/XLSX with extracted places")
    parser.add_argument("--rules", default="configs/audit_rules.yaml", help="Audit rules YAML")
    parser.add_argument("--report", required=True, help="Output CSV/TSV/JSON/XLSX report path")
    args = parser.parse_args()

    df = load_places(args.input)
    rules = load_audit_rules(args.rules)
    audited = audit_dataframe(df, rules)
    save_dataframe(audited, args.report)


if __name__ == "__main__":
    main()
