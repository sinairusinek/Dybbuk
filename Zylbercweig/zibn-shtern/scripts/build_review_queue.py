from __future__ import annotations

import argparse

from zibn_shtern.io import load_places, save_dataframe
from zibn_shtern.reviewer_adapter import build_review_queue


def main() -> None:
    parser = argparse.ArgumentParser(description="Build reviewer queue from audit report")
    parser.add_argument("--audit", required=True, help="Audit CSV/JSON")
    parser.add_argument("--output", required=True, help="Reviewer queue CSV/JSON")
    args = parser.parse_args()

    audit_df = load_places(args.audit)
    queue_df = build_review_queue(audit_df)
    save_dataframe(queue_df, args.output)


if __name__ == "__main__":
    main()
