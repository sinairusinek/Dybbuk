#!/usr/bin/env python3
"""
Create a stratified review sample from organizations_classified.tsv.

Strata (revised after population audit):
  Axis 1 – proper_name correctness:
    S1: proper_name + high                              → 12 rows  (pop 1,125)
    S2: proper_name + medium                            → 18 rows  (pop 15,324)
  Axis 2 – descriptive_term correctness:
    S3: descriptive_term + medium, WITH title (all)     →  5 rows  (pop 5 — exhaustive)
    S4: descriptive_term + high,   NO title             → 10 rows  (pop 187)
    S5: descriptive_term + medium, NO title             → 15 rows  (pop 4,022)
  Axis 3 – ambiguous recoverability:
    S6: ambiguous + low, HAS context (org_type/rel/loc) → 15 rows  (pop 3,889)
    S7: ambiguous + low, nearly empty                   → 10 rows  (pop 7,175)
  Axis 4 – random cross-check:
    S9: unstratified random                             → 15 rows
                                                        = 100 total

Notes:
  - descriptive_term + high + WITH title: 0 rows (stratum empty).
  - blank name_type: 0 rows in Python (awk artifacts).
"""

import csv, random, pathlib, sys

csv.field_size_limit(sys.maxsize)
random.seed(42)  # reproducible

SRC = pathlib.Path(__file__).with_name("organizations_classified.tsv")
OUT = pathlib.Path(__file__).with_name("review_sample_100.tsv")

# ── helpers ──────────────────────────────────────────────────────────

def is_missing(val: str) -> bool:
    v = val.strip().lower()
    return v in ("", "na", "n/a", "null", "-", "--", "_")

def has_context(row: dict) -> bool:
    """Row has at least org_type, a relation field, or a location field."""
    for k in row:
        if any(tag in k for tag in ("org_type", "relations", "locations", "descriptive_name")):
            if not is_missing(row[k]):
                return True
    return False

# ── column detection ─────────────────────────────────────────────────

def find_col(headers, fragment):
    for h in headers:
        if fragment in h:
            return h
    return None

# ── load ─────────────────────────────────────────────────────────────

with open(SRC, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    headers = reader.fieldnames
    rows = list(reader)

title_col = find_col(headers, "organizations - _ - title")
nt_col    = find_col(headers, "name_type")
conf_col  = find_col(headers, "confidence")

print(f"Loaded {len(rows)} rows")
print(f"  title col : {title_col}")
print(f"  name_type : {nt_col}")
print(f"  confidence: {conf_col}")

# ── bucket ───────────────────────────────────────────────────────────

buckets = {f"S{i}": [] for i in (1, 2, 3, 4, 5, 6, 7, 9)}

for i, r in enumerate(rows):
    nt   = r[nt_col].strip()   if nt_col   else ""
    conf = r[conf_col].strip() if conf_col else ""
    title_present = not is_missing(r[title_col]) if title_col else False

    if nt == "proper_name" and conf == "high":
        buckets["S1"].append(i)
    elif nt == "proper_name" and conf == "medium":
        buckets["S2"].append(i)
    elif nt == "descriptive_term" and conf == "medium" and title_present:
        buckets["S3"].append(i)
    elif nt == "descriptive_term" and conf == "high" and not title_present:
        buckets["S4"].append(i)
    elif nt == "descriptive_term" and conf == "medium" and not title_present:
        buckets["S5"].append(i)
    elif nt == "ambiguous" and conf == "low" and has_context(r):
        buckets["S6"].append(i)
    elif nt == "ambiguous" and conf == "low" and not has_context(r):
        buckets["S7"].append(i)
    # S9 drawn from entire file later

print("\nStratum populations:")
for s in sorted(buckets):
    print(f"  {s}: {len(buckets[s])}")

# ── sample targets ───────────────────────────────────────────────────

targets = {
    "S1": 12, "S2": 18,
    "S3": None,  # take all (5)
    "S4": 10, "S5": 15,
    "S6": 15, "S7": 10,
    "S9": 15,
}

selected_indices = set()
stratum_labels = {}

for s in sorted(targets):
    if s == "S9":
        continue
    pool = buckets[s]
    n = targets[s] if targets[s] is not None else len(pool)
    n = min(n, len(pool))
    chosen = random.sample(pool, n) if n < len(pool) else pool[:]
    for idx in chosen:
        selected_indices.add(idx)
        stratum_labels[idx] = s

# S9: random from full file, excluding already-selected
remaining = [i for i in range(len(rows)) if i not in selected_indices]
s9_sample = random.sample(remaining, min(15, len(remaining)))
for idx in s9_sample:
    selected_indices.add(idx)
    stratum_labels[idx] = "S9"

print(f"\nTotal sampled: {len(selected_indices)}")
for s in sorted(targets):
    cnt = sum(1 for v in stratum_labels.values() if v == s)
    print(f"  {s}: {cnt}")

# ── write ────────────────────────────────────────────────────────────

# Add review columns
extra_cols = [
    "review_stratum",
    "review_stratum_desc",
    "reviewer_correct",     # TRUE / FALSE / UNCERTAIN
    "reviewer_suggested_type",
    "reviewer_notes",
]

stratum_descs = {
    "S1": "proper_name + high confidence",
    "S2": "proper_name + medium confidence",
    "S3": "descriptive_term + medium, with title (exhaustive)",
    "S4": "descriptive_term + high, no title",
    "S5": "descriptive_term + medium, no title",
    "S6": "ambiguous + low, has context",
    "S7": "ambiguous + low, nearly empty",
    "S9": "random cross-check",
}

sorted_indices = sorted(selected_indices)

with open(OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers + extra_cols, delimiter="\t")
    writer.writeheader()
    for idx in sorted_indices:
        r = dict(rows[idx])
        s = stratum_labels[idx]
        r["review_stratum"] = s
        r["review_stratum_desc"] = stratum_descs[s]
        r["reviewer_correct"] = ""
        r["reviewer_suggested_type"] = ""
        r["reviewer_notes"] = ""
        writer.writerow(r)

print(f"\nWrote {len(sorted_indices)} rows → {OUT.name}")
