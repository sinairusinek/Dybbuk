"""Extract Arne's added columns from the Google Sheets export."""
import csv
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "vienna_berlin_audit_for_RA - vienna_berlin_audit_for_RA (1).tsv"
OUT = HERE / "vienna_berlin_audit_arne_delta_2026-06-14.tsv"

MISCLS = "incorrectly classified as theatre or theatre in German-speaking countries"
SRC_TO_DST = [
    ("kind", "kind"), ("id", "id"), ("city", "city"),
    (MISCLS, "miscls_theatre"),
    ("duplicate", "duplicate"),
    ("name_latin", "name_latin"),
    ("QID", "QID"),
    ("historic address", "historic_address"),
    ("current address", "current_address"),
    ("comments", "comments"),
]

with open(SRC, encoding="utf-8") as f:
    reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
    raw = list(reader)
header = raw[0]
rows = [dict(zip(header, r + [""] * (len(header) - len(r)))) for r in raw[1:]]

out = []
for r in rows:
    arne_cols = [MISCLS, "duplicate", "name_latin", "QID", "historic address", "current address", "comments"]
    if not any((r.get(c) or "").strip() for c in arne_cols):
        continue
    out.append({dst: (r.get(src) or "").strip() for src, dst in SRC_TO_DST})

with open(OUT, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=[dst for _, dst in SRC_TO_DST], delimiter="\t")
    w.writeheader()
    for r in out: w.writerow(r)
print(f"wrote {len(out)} rows -> {OUT.name}")
