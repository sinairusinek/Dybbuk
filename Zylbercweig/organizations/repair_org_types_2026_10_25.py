"""Repair org_type in core_db.tsv and org_addresses_review.tsv (2026-10-25).

Three passes, all idempotent:

1. Restore types the app blanked one save at a time
   (org_type_blank_restore_2026_10_25.tsv, 106 rows found by walking
   core_db's history). 85 are the Organization Cards bug: the type box
   lowercased the stored value, never found it among the capitalised options,
   fell back to "(unset)" and saved that. 21 predate the Cards editor; their
   cause is unknown but every lost value is plainly right. Fills empty cells
   only.
2. Re-run apply_empty_type_backfill.py: the 145 approved keyword-rule types of
   2026-10-13 were applied, then 136 of them were wiped three hours later by
   the stale-checkout save 2e271fd05. Its ledger still holds them.
3. Normalise every org_type to the spelling in Zalmen's dropdown
   (_ORG_TYPE_OPTIONS in zalmen/views/org_review.py), matching by casefold:
   minting wrote `.lower()`, so 23 types were split in two or three
   (575 'traveling company' vs 125 'Traveling Company'). All rows, retired ones
   included — the spelling carries no meaning, and leaving them would keep the
   split in the file.

Usage:
    python3 Zylbercweig/organizations/repair_org_types_2026_10_25.py [--dry-run]
"""
from __future__ import annotations

import argparse
import ast
import collections
import csv
import subprocess
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
CORE_DB_TSV = HERE / "core_db.tsv"
ADDR_TSV = HERE / "org_addresses_review.tsv"
RESTORE_TSV = HERE / "org_type_blank_restore_2026_10_25.tsv"
ORG_REVIEW_PY = HERE.parent / "zalmen" / "views" / "org_review.py"


def _dropdown_canon() -> dict[str, str]:
	"""casefold -> spelling, read from the app's own option list (one source)."""
	tree = ast.parse(ORG_REVIEW_PY.read_text(encoding="utf-8"))
	for node in tree.body:
		if isinstance(node, ast.Assign) and any(
			isinstance(t, ast.Name) and t.id == "_ORG_TYPE_OPTIONS" for t in node.targets
		):
			opts = ast.literal_eval(node.value)
			return {o.casefold(): o for o in opts if o}
	raise SystemExit("_ORG_TYPE_OPTIONS not found in org_review.py")


def _read(p: Path) -> tuple[list[str], list[dict[str, str]]]:
	with open(p, encoding="utf-8", newline="") as f:
		r = csv.DictReader(f, delimiter="\t")
		return list(r.fieldnames or []), list(r)


def _write(p: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
	# Both files are CRLF; csv's default would be too, but say so explicitly.
	with open(p, "w", encoding="utf-8", newline="") as f:
		w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator="\r\n")
		w.writeheader()
		w.writerows(rows)


def main() -> None:
	ap = argparse.ArgumentParser()
	ap.add_argument("--dry-run", action="store_true")
	args = ap.parse_args()
	canon = _dropdown_canon()

	# ---- 1. restore single-save blankings -----------------------------------
	db_fields, db_rows = _read(CORE_DB_TSV)
	by_id = {r["db_id"].strip(): r for r in db_rows}
	_, restore = _read(RESTORE_TSV)
	restored = collections.Counter()
	for r in restore:
		row = by_id.get(r["db_id"].strip())
		if row is None or (row.get("org_type") or "").strip():
			continue
		row["org_type"] = r["restore_type"]
		restored[r["cause"]] += 1
	print(f"1. restored from {RESTORE_TSV.name}: {dict(restored) or 0}")
	if not args.dry_run:
		_write(CORE_DB_TSV, db_fields, db_rows)

	# ---- 2. the 10-13 approved backfill, wiped by 2e271fd05 -----------------
	cmd = [sys.executable, str(HERE / "apply_empty_type_backfill.py")]
	if args.dry_run:
		cmd.append("--dry-run")
		print("2. (dry-run: pass 1 not written, so pass 2 counts may overlap)")
	out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
	print("2. " + " / ".join(l for l in out.splitlines() if l.startswith("Approved")))

	# ---- 3. one spelling per type, in both files ----------------------------
	for path in (CORE_DB_TSV, ADDR_TSV):
		fields, rows = _read(path)
		changed = collections.Counter()
		unknown = collections.Counter()
		for row in rows:
			t = (row.get("org_type") or "").strip()
			if not t:
				continue
			c = canon.get(t.casefold())
			if c is None:
				unknown[t] += 1
			elif c != row["org_type"]:
				changed[c] += 1
				row["org_type"] = c
		if path == ADDR_TSV:
			# Cards blanked the address row in the same save; mirror core_db back.
			db_types = {r["db_id"].strip(): (r.get("org_type") or "").strip()
				for r in _read(CORE_DB_TSV)[1]}
			for row in rows:
				if not (row.get("org_type") or "").strip():
					t = canon.get(db_types.get(row.get("db_id", "").strip(), "").casefold())
					if t:
						row["org_type"] = t
						changed["(filled from core_db)"] += 1
		print(f"3. {path.name}: {sum(changed.values())} cells re-spelled/filled")
		for k, n in changed.most_common():
			print(f"     {n:5}  -> {k}")
		if unknown:
			print(f"   NOT in the dropdown, left as is: {dict(unknown)}")
		if not args.dry_run:
			_write(path, fields, rows)

	_, final = _read(CORE_DB_TSV)
	c = collections.Counter((r.get("org_type") or "").strip() for r in final)
	print(f"\ncore_db: {len(c)} distinct org_type values, {c['']} empty")
	if args.dry_run:
		print("--dry-run: nothing written")


if __name__ == "__main__":
	main()
