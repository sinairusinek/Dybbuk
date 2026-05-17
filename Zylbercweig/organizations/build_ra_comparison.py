"""Build RA-vs-auto comparison reports.

Inputs:
  ra_tag_canonical.tsv                                  (org_type → canonical_type lookup)
  organizations_clustered_canonical_mapping.tsv         (mentions, per-row auto mapping)
  org_alignment_review_canonical_mapping.tsv            (clusters)
  org_addresses_review_canonical_mapping.tsv            (DB working)
  core_db_canonical_mapping.tsv                         (DB canonical)

Outputs:
  ra_vs_auto_comparison.tsv   per-row comparison
  ra_vs_auto_summary.tsv       (original_type, auto, ra) pivot with disagreement counts
  review_punchlist.tsv         dedup'd punchlist (flagged + disagreements)
"""
from __future__ import annotations
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

RA_PATH = HERE / "ra_tag_canonical.tsv"
MAPPINGS: list[tuple[str, Path]] = [
	("organizations_clustered", HERE / "organizations_clustered_canonical_mapping.tsv"),
	("org_alignment_review", HERE / "org_alignment_review_canonical_mapping.tsv"),
	("org_addresses_review", HERE / "org_addresses_review_canonical_mapping.tsv"),
	("core_db", HERE / "core_db_canonical_mapping.tsv"),
]

OUT_COMPARE = HERE / "ra_vs_auto_comparison.tsv"
OUT_SUMMARY = HERE / "ra_vs_auto_summary.tsv"
OUT_PUNCHLIST = HERE / "review_punchlist.tsv"


def load_ra() -> dict[str, str]:
	ra: dict[str, str] = {}
	with RA_PATH.open(newline="", encoding="utf-8") as f:
		reader = csv.reader(f, delimiter="\t")
		next(reader, None)
		for row in reader:
			if not row:
				continue
			key = row[0].strip()
			val = row[1].strip() if len(row) > 1 else ""
			ra[key] = val
	return ra


def main() -> None:
	ra = load_ra()
	print(f"RA mapping: {len(ra)} tags ({sum(1 for v in ra.values() if v)} with canonical, {sum(1 for v in ra.values() if not v)} blank)")

	compare_rows: list[list[str]] = []
	# summary[(orig, auto, ra)] = count
	pivot: Counter[tuple[str, str, str]] = Counter()
	# per-tag disagreement totals
	tag_disagree: Counter[str] = Counter()
	tag_total: Counter[str] = Counter()

	punchlist: dict[tuple[str, str, str], dict] = {}  # dedup key → row

	for src_name, path in MAPPINGS:
		if not path.exists():
			print(f"  MISSING: {path.name}")
			continue
		with path.open(newline="", encoding="utf-8") as f:
			reader = csv.DictReader(f, delimiter="\t")
			for row in reader:
				orig = row.get("original_type", "")
				auto = row.get("canonical_type", "")
				name = row.get("name", "")
				row_id = row.get("row_id", "")
				decided_via = row.get("decided_via", "")
				needs_review = row.get("needs_review", "")
				review_reason = row.get("review_reason", "")

				ra_val = ra.get(orig, None)
				if ra_val is None:
					status = "tag_not_in_ra"
				elif ra_val == "":
					status = "ra_blank"
				elif ra_val == auto:
					status = "agree"
				else:
					status = "disagree"

				compare_rows.append([
					src_name, row_id, name, orig, auto, ra_val or "", status,
					decided_via, needs_review, review_reason,
				])
				pivot[(orig, auto, ra_val or "")] += 1
				tag_total[orig] += 1
				if status == "disagree":
					tag_disagree[orig] += 1

				is_disagree_via_context = (status == "disagree" and decided_via == "context")
				if needs_review == "yes" or is_disagree_via_context:
					key = (name, orig, auto)
					if key not in punchlist:
						punchlist[key] = {
							"source_file": src_name,
							"row_id": row_id,
							"name": name,
							"original_type": orig,
							"auto_canonical_type": auto,
							"ra_canonical_type": ra_val or "",
							"match_status": status,
							"decided_via": decided_via,
							"needs_review": needs_review,
							"review_reason": review_reason,
							"count_across_files": 1,
						}
					else:
						punchlist[key]["count_across_files"] += 1

	# Write compare
	with OUT_COMPARE.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["source_file", "row_id", "name", "original_type",
					"auto_canonical_type", "ra_canonical_type", "match_status",
					"decided_via", "needs_review", "review_reason"])
		w.writerows(compare_rows)

	# Write summary sorted by disagreement count
	rows_pivot = [
		(orig, auto, ra_v, n, ("disagree" if ra_v and ra_v != auto
							  else ("agree" if ra_v else "ra_blank")))
		for (orig, auto, ra_v), n in pivot.items()
	]
	rows_pivot.sort(key=lambda r: (-tag_disagree[r[0]], r[0], -r[3]))

	with OUT_SUMMARY.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["original_type", "auto_canonical", "ra_canonical", "n_rows",
					"status", "tag_total_rows", "tag_disagree_rows"])
		for orig, auto, ra_v, n, status in rows_pivot:
			w.writerow([orig, auto, ra_v, n, status,
						tag_total[orig], tag_disagree[orig]])

	# Write punchlist
	with OUT_PUNCHLIST.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["source_file", "row_id", "name", "original_type",
					"auto_canonical_type", "ra_canonical_type", "match_status",
					"decided_via", "needs_review", "review_reason",
					"count_across_files"])
		for k, p in punchlist.items():
			w.writerow([p["source_file"], p["row_id"], p["name"], p["original_type"],
						p["auto_canonical_type"], p["ra_canonical_type"],
						p["match_status"], p["decided_via"],
						p["needs_review"], p.get("review_reason", ""),
						p["count_across_files"]])

	# Console summary
	total_rows = len(compare_rows)
	status_counts = Counter(r[6] for r in compare_rows)
	print(f"\nTotal rows compared: {total_rows}")
	for s in ("agree", "disagree", "ra_blank", "tag_not_in_ra"):
		print(f"  {s:15} {status_counts.get(s, 0):>7}")
	print(f"\nTop disagreements by tag (count of disagreeing rows / total rows for that tag):")
	for orig, n in tag_disagree.most_common(15):
		print(f"  {n:5d} / {tag_total[orig]:5d}   {orig!r}")
	print(f"\nPunchlist: {len(punchlist)} unique (name, original_type, auto) tuples")
	print(f"\nFiles written:")
	for p in (OUT_COMPARE, OUT_SUMMARY, OUT_PUNCHLIST):
		print(f"  {p.name}  ({p.stat().st_size:,} bytes)")


if __name__ == "__main__":
	main()
