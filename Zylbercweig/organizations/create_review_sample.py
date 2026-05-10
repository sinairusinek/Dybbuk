#!/usr/bin/env python3
"""
Create stratified review samples from organizations_classified.tsv.

Produces two files:
1. review_sample_main.tsv  — stratified sample of classified rows (100 rows)
   Strata:
     S1:  proper_name + high                                 → 12 rows  (pop ~1,125)
     S2:  proper_name + medium                               → 18 rows  (pop ~15,322)
     S3:  descriptive_term + medium, WITH title (exhaustive) →  ≤5 rows
     S4:  descriptive_term + high,   NO title                → 10 rows  (pop ~187)
     S5:  descriptive_term + medium, NO title                → 15 rows  (pop ~4,029)
     S6:  ambiguous + low, HAS context                       → 15 rows  (pop ~3,889)
     S9:  unstratified random                                → 15 rows
     NOT: not_an_organization + high (spot-check)            → 10 rows

2. review_sample_person_ref.tsv — all person_ref-flagged rows (≤85 rows)

Both include reviewer columns for annotation.
"""

import csv, random, pathlib, sys

csv.field_size_limit(sys.maxsize)
random.seed(42)

SRC = pathlib.Path(__file__).with_name("organizations_classified.tsv")
OUT_MAIN = pathlib.Path(__file__).with_name("review_sample_main.tsv")
OUT_PERSON_REF = pathlib.Path(__file__).with_name("review_sample_person_ref.tsv")

# ── helpers ──────────────────────────────────────────────────────────

def is_missing(val: str) -> bool:
    v = val.strip().lower()
    return v in ("", "na", "n/a", "null", "none", "-", "--", "_")

def has_context(row: dict) -> bool:
    for k in row:
        if any(tag in k for tag in ("org_type", "relations", "locations", "descriptive_name")):
            if not is_missing(row[k]):
                return True
    return False

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

title_col    = find_col(headers, "organizations - _ - title")
nt_col       = find_col(headers, "name_type")
conf_col     = find_col(headers, "confidence")
flag_col     = find_col(headers, "review_flag")

print(f"Loaded {len(rows)} rows")
print(f"  title col      : {title_col}")
print(f"  name_type col  : {nt_col}")
print(f"  confidence col : {conf_col}")
print(f"  review_flag col: {flag_col}")

# ── bucket ───────────────────────────────────────────────────────────

buckets = {s: [] for s in ("S1","S2","S3","S4","S5","S6","S9","NOT")}

for i, r in enumerate(rows):
    nt    = r[nt_col].strip()   if nt_col   else ""
    conf  = r[conf_col].strip() if conf_col else ""
    flag  = r[flag_col].strip() if flag_col else ""
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
    elif nt == "not_an_organization":
        buckets["NOT"].append(i)
    # S9 drawn from all non-NOT rows later

print("\nStratum populations:")
for s in sorted(buckets):
    print(f"  {s}: {len(buckets[s])}")

# ── sample ───────────────────────────────────────────────────────────

targets = {
    "S1": 12,
    "S2": 18,
    "S3": None,   # exhaustive (≤5)
    "S4": 10,
    "S5": 15,
    "S6": 15,
    "NOT": 10,    # spot-check not_an_organization
    "S9": 15,
}

selected_indices = set()
stratum_labels: dict[int, str] = {}

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

# S9: random from non-not_an_org rows, excluding already-selected
not_org_indices = set(buckets["NOT"])
remaining = [i for i in range(len(rows))
             if i not in selected_indices and i not in not_org_indices]
s9_sample = random.sample(remaining, min(15, len(remaining)))
for idx in s9_sample:
    selected_indices.add(idx)
    stratum_labels[idx] = "S9"

print(f"\nTotal sampled (main): {len(selected_indices)}")
for s in sorted(targets):
    cnt = sum(1 for v in stratum_labels.values() if v == s)
    print(f"  {s}: {cnt}")

# ── review columns ───────────────────────────────────────────────────

extra_cols = [
    "review_stratum",
    "review_stratum_desc",
    "reviewer_correct",       # TRUE / FALSE / UNCERTAIN
    "reviewer_suggested_type",
    "reviewer_notes",
]

stratum_descs = {
    "S1":  "proper_name + high confidence",
    "S2":  "proper_name + medium confidence",
    "S3":  "descriptive_term + medium, with title (exhaustive)",
    "S4":  "descriptive_term + high, no title",
    "S5":  "descriptive_term + medium, no title",
    "S6":  "ambiguous + low, has context",
    "S9":  "random cross-check",
    "NOT": "not_an_organization spot-check",
}

# ── write main sample ─────────────────────────────────────────────────

sorted_indices = sorted(selected_indices)
with open(OUT_MAIN, "w", newline="", encoding="utf-8") as f:
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

print(f"\nWrote {len(sorted_indices)} rows → {OUT_MAIN.name}")

# ── write person_ref sample ──────────────────────────────────────────

person_ref_rows = [
    i for i, r in enumerate(rows)
    if flag_col and r[flag_col].strip() == "person_ref"
]
print(f"\nperson_ref flagged rows: {len(person_ref_rows)}")

pr_extra_cols = [
    "reviewer_is_person_ref",   # TRUE / FALSE
    "reviewer_suggested_type",
    "reviewer_org_candidate",   # if it IS a proper org, what name?
    "reviewer_notes",
]

with open(OUT_PERSON_REF, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers + pr_extra_cols, delimiter="\t")
    writer.writeheader()
    for idx in sorted(person_ref_rows):
        r = dict(rows[idx])
        r["reviewer_is_person_ref"] = ""
        r["reviewer_suggested_type"] = ""
        r["reviewer_org_candidate"] = ""
        r["reviewer_notes"] = ""
        writer.writerow(r)

print(f"Wrote {len(person_ref_rows)} rows → {OUT_PERSON_REF.name}")
