"""Enrich pi_punchlist.tsv with entry_name, volume, and context_text columns.

For mention-level rows: look up directly by positional alignment (row_idx in source).
For cluster-level rows: find a representative mention with the same cluster_id and grab its heading + file + sentence.
For DB rows (addresses_review/core_db): look up via linked_cluster_ids → find a mention.
"""
from __future__ import annotations
import csv, sys, re
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

MENTIONS = HERE / "organizations_clustered.tsv"
CLUSTERS = HERE / "org_alignment_review.tsv"
DB_WORKING = HERE / "org_addresses_review.tsv"
CORE_DB = HERE / "core_db.tsv"
PUNCH = HERE / "pi_punchlist.tsv"

HEADING = "_ - heading"
FILE_COL = "File"
SENT = "_ - organizations - _ - relations - _ - original_sentence"
CID = "cluster_id"

# Build cluster_id → (heading, file, sentence) lookup using first mention found
print("Building cluster_id → context lookup from mentions...")
cluster_lookup: dict[str, tuple[str, str, str]] = {}
with MENTIONS.open(newline="", encoding="utf-8") as f:
	for r in csv.DictReader(f, delimiter="\t"):
		cid = r.get(CID, "")
		if cid and cid not in cluster_lookup:
			sent = (r.get(SENT) or "").replace("\n", " ").strip()
			cluster_lookup[cid] = (r.get(HEADING, ""), r.get(FILE_COL, ""), sent)
print(f"  cluster_id contexts indexed: {len(cluster_lookup)}")

# Build db_id → linked cluster ids
db_to_clusters: dict[str, list[str]] = {}
for db_path in (DB_WORKING, CORE_DB):
	if not db_path.exists():
		continue
	with db_path.open(newline="", encoding="utf-8") as f:
		for r in csv.DictReader(f, delimiter="\t"):
			dbid = r.get("db_id", "")
			linked = r.get("linked_cluster_ids", "") or ""
			cids = [c.strip() for c in re.split(r"[,;|\s]+", linked) if c.strip()]
			if dbid and cids:
				db_to_clusters.setdefault(dbid, []).extend(cids)

# Build mention row_idx → (heading, file, sentence) for direct lookup
mention_idx_lookup: dict[int, tuple[str, str, str]] = {}
with MENTIONS.open(newline="", encoding="utf-8") as f:
	for i, r in enumerate(csv.DictReader(f, delimiter="\t")):
		sent = (r.get(SENT) or "").replace("\n", " ").strip()
		mention_idx_lookup[i] = (r.get(HEADING, ""), r.get(FILE_COL, ""), sent)


def _lookup_for_row(source_file: str, row_id: str) -> tuple[str, str, str]:
	if "organizations_clustered_canonical" in source_file:
		# mention-level row; row_id is cluster_id, but each row is one mention.
		# Use cluster_lookup (which already picks the first mention per cluster).
		return cluster_lookup.get(row_id, ("", "", ""))
	if "org_alignment_review_canonical" in source_file:
		return cluster_lookup.get(row_id, ("", "", ""))
	if "org_addresses_review" in source_file or "core_db" in source_file:
		cids = db_to_clusters.get(row_id, [])
		for c in cids:
			if c in cluster_lookup:
				return cluster_lookup[c]
		return ("", "", "")
	return ("", "", "")


# Enrich punchlist
with PUNCH.open(newline="", encoding="utf-8") as f:
	rdr = csv.DictReader(f, delimiter="\t")
	fields = list(rdr.fieldnames or [])
	rows = list(rdr)

new_fields = list(fields)
for c in ("entry_name", "volume", "context_text"):
	if c not in new_fields:
		new_fields.append(c)

for r in rows:
	heading, fname, sentence = _lookup_for_row(r.get("source_file", ""), r.get("row_id", ""))
	r["entry_name"] = heading
	r["volume"] = fname
	r["context_text"] = sentence[:500]

with PUNCH.open("w", newline="", encoding="utf-8") as f:
	w = csv.DictWriter(f, fieldnames=new_fields, delimiter="\t")
	w.writeheader()
	for r in rows:
		w.writerow({k: r.get(k, "") for k in new_fields})

filled = sum(1 for r in rows if r["entry_name"])
print(f"Enriched {len(rows)} rows. {filled} have entry_name; {sum(1 for r in rows if r['context_text'])} have context_text.")
