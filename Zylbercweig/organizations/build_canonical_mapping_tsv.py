"""Emit a TSV showing each row's original org_type and new canonical type."""
from __future__ import annotations
import csv
from pathlib import Path

HERE = Path(__file__).parent
BEFORE = HERE / "core_db.tsv.pre_canonical_backup"
AFTER = HERE / "core_db.tsv"
OUT = HERE / "org_type_canonical_mapping.tsv"


def load(path: Path) -> dict[str, dict]:
	with path.open(newline="", encoding="utf-8") as f:
		return {r["db_id"]: r for r in csv.DictReader(f, delimiter="\t")}


def main() -> None:
	before = load(BEFORE)
	after = load(AFTER)
	with OUT.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["db_id", "name", "original_type", "canonical_type", "changed"])
		for db_id in sorted(before, key=lambda x: int(x)):
			b = before[db_id]
			a = after.get(db_id, {})
			orig = b.get("org_type", "")
			canon = a.get("org_type", "")
			w.writerow([db_id, b.get("name", ""), orig, canon, "yes" if orig != canon else ""])
	print(f"Wrote {OUT.name}")


if __name__ == "__main__":
	main()
