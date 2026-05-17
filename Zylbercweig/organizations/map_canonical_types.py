"""One-shot script: rewrite core_db.tsv org_type values to canonical types."""
from __future__ import annotations
import csv
import shutil
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "core_db.tsv"
BACKUP = HERE / "core_db.tsv.pre_canonical_backup"

DIRECT_MAP = {
	"theatre": "Theatre",
	"Theatre": "Theatre",
	"publisher": "Publisher",
	"Publisher": "Publisher",
	"Traveling Company": "Traveling Company",
	"Printer/Publisher": "Printer/Publisher",
	"troupe": "Traveling Company",
	"Troupe": "Traveling Company",
	"Company on Tour": "Company on Tour",
	"Printer": "Printer",
	"school": "Education",
	"School": "Education",
	"Education": "Education",
	"Amateur": "Amateur",
	"Musical organization": "Musical organization",
	"Kleinkunst": "Kleinkunst",
	"Media (Radio/ Film)": "Media (Radio/ Film/TV)",
	"Media (Radio/ Film/TV)": "Media (Radio/ Film/TV)",
	"Library": "Library",
	"Heritage Institution": "Heritage Institution",
	"Union": "Labour",
	"Organization": "Society/ Union",
	"Journals/ Newspapers": "Journals/ Newspapers",
	"Circus": "Circus",
	"": "",
}

# Row-id overrides for context-based corrections (db_id -> canonical_type).
ID_OVERRIDES = {
	"558": "Musical organization",  # Moscow Philharmonic was mis-typed "school"
}


def main() -> None:
	shutil.copy2(SRC, BACKUP)
	with SRC.open(newline="", encoding="utf-8") as f:
		reader = csv.DictReader(f, delimiter="\t")
		fieldnames = reader.fieldnames
		rows = list(reader)

	unmapped: dict[str, int] = {}
	for row in rows:
		original = row.get("org_type", "")
		if row["db_id"] in ID_OVERRIDES:
			row["org_type"] = ID_OVERRIDES[row["db_id"]]
			continue
		if original in DIRECT_MAP:
			row["org_type"] = DIRECT_MAP[original]
		else:
			unmapped[original] = unmapped.get(original, 0) + 1

	with SRC.open("w", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
		writer.writeheader()
		writer.writerows(rows)

	print(f"Backup: {BACKUP.name}")
	print(f"Rows: {len(rows)}")
	if unmapped:
		print("UNMAPPED values encountered (left unchanged):")
		for v, n in sorted(unmapped.items(), key=lambda x: -x[1]):
			print(f"  {n:4d}  {v!r}")
	else:
		print("All values mapped.")


if __name__ == "__main__":
	main()
