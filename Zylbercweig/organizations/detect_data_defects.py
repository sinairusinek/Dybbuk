"""Detect more cases like the two core_db defects surfaced by the
2026-05-27 off-candidate audit (see HANDOFF_data_defects_and_matcher.md):

Class A — `name_yiddish` contamination
  For each core_db row populated in BOTH `name` and `name_yiddish`, check whether
  the two fields agree. Low internal similarity = candidate contamination. Also
  surface cross-row collisions (X.name_yiddish == Y.name and similar), and flag
  rows whose Yiddish was likely backfilled from an alignment with blank reviewer.

Class B — duplicate DB entities & garbage-bucket alignments
  Pairs of core_db rows that score highly against each other under exact /
  token_set / paren-stripped Yiddish-run comparison are candidate duplicates.
  Separately: db rows with ≥3 aligned clusters whose canonicals do NOT mutually
  match are candidate "garbage buckets" (like db486 absorbing Korik/Zhukov/Perm).

Output: two reviewable TSVs in this directory. Nothing is auto-mutated.
"""
from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

from matching_core import script_runs as _core_script_runs, detect_script as _core_detect_script
from org_normalize import normalize_yiddish, token_key_set, token_set_similarity

CORE_DB = HERE / "core_db.tsv"
ALIGN = HERE / "org_alignment_review.tsv"
OUT_A = HERE / "db_yiddish_contamination_punchlist.tsv"
OUT_B_DUPS = HERE / "db_duplicate_pairs_punchlist.tsv"
OUT_B_BUCKETS = HERE / "db_garbage_bucket_punchlist.tsv"

def yiddish_runs(text: str) -> list[str]:
    """All maximal Yiddish-script runs in a string. Delegates to
    matching_core.script_runs (core 0.2.0+, see LEDGER row 18)."""
    return _core_script_runs(text, "hebrew") if text else []


def best_token_set(a: str, b: str) -> float:
    """token_set_similarity of two name strings, taking the max over each side's
    Yiddish runs as well as the raw forms — so a mixed-script name's embedded
    Yiddish part can be compared cleanly. 0.0 if either side has no content."""
    forms_a = [a] + yiddish_runs(a)
    forms_b = [b] + yiddish_runs(b)
    best = 0.0
    for fa in forms_a:
        for fb in forms_b:
            s = token_set_similarity(fa, fb)
            if s > best:
                best = s
    return best


# ── Load ──────────────────────────────────────────────────────────────────
with CORE_DB.open(newline="", encoding="utf-8") as f:
    db_rows = list(csv.DictReader(f, delimiter="\t"))
# Skip already-deprecated rows — they've been merged into another id and
# shouldn't keep showing up in dedup punchlists every run.
db_rows = [r for r in db_rows if (r.get("deprecated","") or "").strip().lower() != "true"]
with ALIGN.open(newline="", encoding="utf-8") as f:
    align_rows = list(csv.DictReader(f, delimiter="\t"))

# alignments per db_id
aligns_by_db: dict[str, list[dict[str, str]]] = defaultdict(list)
for r in align_rows:
    aid = (r.get("aligned_db_id") or "").strip()
    if aid:
        aligns_by_db[aid].append(r)

db_by_id = {r["db_id"]: r for r in db_rows if r.get("db_id", "").strip()}

# ── Class A: name_yiddish contamination ────────────────────────────────────
# (1) internal disagreement between name and name_yiddish
# (2) cross-row name collision (X.name_yiddish == Y.name) — exact equality of
#     normalized Yiddish forms across distinct rows is the strongest smoking gun.
norm_to_db: dict[str, list[str]] = defaultdict(list)
for r in db_rows:
    for field in ("name", "name_yiddish"):
        v = (r.get(field) or "").strip()
        if not v:
            continue
        for form in [v] + yiddish_runs(v):
            n = normalize_yiddish(form)
            if n and len(n) >= 3:
                norm_to_db[n].append(f"{r['db_id']}.{field}")

class_a: list[dict[str, str]] = []
for r in db_rows:
    db_id = (r.get("db_id") or "").strip()
    if not db_id:
        continue
    name = (r.get("name") or "").strip()
    name_yid = (r.get("name_yiddish") or "").strip()
    aligned = aligns_by_db.get(db_id, [])
    blank_rev = [a for a in aligned if not (a.get("reviewer") or "").strip()]
    reasons: list[str] = []
    intra_sim: float | None = None

    # (1) internal disagreement (only when BOTH populated AND same-script).
    # Cross-script pairs (English `name` + Yiddish `name_yiddish`) are
    # translations of the same entity — token-set can't bridge them, so
    # low_intra_sim there is a false positive (e.g. db151 Tsentral/צענטראַל).
    if name and name_yid:
        intra_sim = best_token_set(name, name_yid)
        name_script = _core_detect_script(name)
        yid_script = _core_detect_script(name_yid)
        same_script = name_script == yid_script and name_script != "unknown"
        if same_script and intra_sim < 0.30:
            reasons.append(f"low_intra_sim={intra_sim:.2f}")

    # (2) cross-row collision: name_yiddish matches another row's name.
    # EXCEPT: skip the collision when this row's `parent_db_id` points at the
    # other row — that's an intentional umbrella-with-branches relationship
    # (e.g. db696 Hashomer Hatzair Vilnius → parent_db_id=602 Hashomer Hatzair).
    parent_id = (r.get("parent_db_id") or "").strip()
    collisions: list[str] = []
    if name_yid:
        for form in [name_yid] + yiddish_runs(name_yid):
            n = normalize_yiddish(form)
            for tag in norm_to_db.get(n, []):
                other_db, other_field = tag.split(".")
                if other_db == db_id or other_field != "name":
                    continue
                if other_db == parent_id:
                    continue  # expected: umbrella parent shares the Yiddish name
                collisions.append(f"db{other_db}.name == this.name_yiddish ({form})")
    collisions = sorted(set(collisions))
    if collisions:
        reasons.append("cross_row_collision")

    # (3) likely backfilled from an unattributed alignment
    if name_yid and blank_rev and not name:
        # name empty but name_yiddish populated, and the aligned clusters that
        # could have backfilled it have blank reviewer
        reasons.append(f"backfilled_from_blank_reviewer_aligns={len(blank_rev)}")
    elif name_yid and blank_rev:
        # weaker signal: name populated, but blank-reviewer aligns exist
        # that could have driven a backfill
        pass

    if reasons:
        class_a.append({
            "db_id": db_id,
            "name": name,
            "name_yiddish": name_yid,
            "intra_token_sim": f"{intra_sim:.3f}" if intra_sim is not None else "",
            "cross_row_collisions": " | ".join(collisions),
            "aligned_cluster_count": str(len(aligned)),
            "blank_reviewer_aligns": " | ".join(a["cluster_id"] for a in blank_rev),
            "suspect_reasons": " | ".join(reasons),
        })

# Sort A: collisions first (strongest), then by lowest intra_sim
class_a.sort(key=lambda r: (
    "cross_row_collision" not in r["suspect_reasons"],
    float(r["intra_token_sim"]) if r["intra_token_sim"] else 1.0,
))

with OUT_A.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(class_a[0].keys()) if class_a else
                       ["db_id","name","name_yiddish","intra_token_sim",
                        "cross_row_collisions","aligned_cluster_count",
                        "blank_reviewer_aligns","suspect_reasons"],
                       delimiter="\t", lineterminator="\n")
    w.writeheader(); w.writerows(class_a)

# ── Class B: duplicate pairs ──────────────────────────────────────────────
# Compute high-similarity pairs (token_set on best form). To keep this O(N^2)
# tractable (~625 rows -> ~195K pairs), precompute Yiddish-run forms per row.
def all_forms(r: dict[str, str]) -> list[str]:
    out: list[str] = []
    for field in ("name", "name_yiddish", "name_yiddish_translit"):
        v = (r.get(field) or "").strip()
        if v:
            out.append(v)
            out.extend(yiddish_runs(v))
    # dedupe preserving order
    seen = set(); uniq = []
    for v in out:
        if v not in seen:
            seen.add(v); uniq.append(v)
    return uniq

forms_by_db = {r["db_id"]: all_forms(r) for r in db_rows if r.get("db_id", "").strip()}

class_b_pairs: list[dict[str, str]] = []
for a, b in combinations(forms_by_db.keys(), 2):
    fa, fb = forms_by_db[a], forms_by_db[b]
    if not fa or not fb:
        continue
    best = 0.0
    for x in fa:
        for y in fb:
            s = token_set_similarity(x, y)
            if s > best:
                best = s
                if best == 1.0:
                    break
        if best == 1.0:
            break
    if best >= 0.60:
        ra, rb = db_by_id[a], db_by_id[b]
        class_b_pairs.append({
            "db_a": a, "name_a": ra.get("name",""), "name_yid_a": ra.get("name_yiddish",""),
            "aligns_a": str(len(aligns_by_db.get(a, []))),
            "db_b": b, "name_b": rb.get("name",""), "name_yid_b": rb.get("name_yiddish",""),
            "aligns_b": str(len(aligns_by_db.get(b, []))),
            "token_set_sim": f"{best:.3f}",
        })

class_b_pairs.sort(key=lambda r: -float(r["token_set_sim"]))
with OUT_B_DUPS.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(class_b_pairs[0].keys()) if class_b_pairs else
                       ["db_a","name_a","name_yid_a","aligns_a","db_b","name_b",
                        "name_yid_b","aligns_b","token_set_sim"],
                       delimiter="\t", lineterminator="\n")
    w.writeheader(); w.writerows(class_b_pairs)

# ── Class B: garbage-bucket detection ─────────────────────────────────────
# A db row with ≥3 aligned clusters whose canonicals don't mutually match.
class_b_buckets: list[dict[str, str]] = []
for db_id, aligned in aligns_by_db.items():
    if len(aligned) < 3:
        continue
    canons = [(a["cluster_id"], (a.get("canonical_yiddish") or "").strip()) for a in aligned]
    canons = [(cid, c) for cid, c in canons if c]
    if len(canons) < 3:
        continue
    # mean pairwise token_set across canonicals
    pairs = list(combinations(canons, 2))
    if not pairs:
        continue
    sims = []
    low_pairs: list[str] = []
    for (cid1, c1), (cid2, c2) in pairs:
        s = best_token_set(c1, c2)
        sims.append(s)
        if s < 0.20:
            low_pairs.append(f"{cid1}↔{cid2}={s:.2f}")
    mean = sum(sims) / len(sims)
    if mean < 0.30:
        dbrow = db_by_id.get(db_id, {})
        class_b_buckets.append({
            "db_id": db_id,
            "name": dbrow.get("name",""),
            "name_yiddish": dbrow.get("name_yiddish",""),
            "aligned_cluster_count": str(len(aligned)),
            "mean_pairwise_sim": f"{mean:.3f}",
            "aligned_clusters": " | ".join(f"{cid}({c[:30]})" for cid,c in canons),
            "lowest_pair_examples": " | ".join(low_pairs[:5]),
        })

class_b_buckets.sort(key=lambda r: float(r["mean_pairwise_sim"]))
with OUT_B_BUCKETS.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(class_b_buckets[0].keys()) if class_b_buckets else
                       ["db_id","name","name_yiddish","aligned_cluster_count",
                        "mean_pairwise_sim","aligned_clusters","lowest_pair_examples"],
                       delimiter="\t", lineterminator="\n")
    w.writeheader(); w.writerows(class_b_buckets)

# ── Summary ───────────────────────────────────────────────────────────────
print(f"Class A — name_yiddish contamination candidates: {len(class_a)} → {OUT_A.name}")
print(f"  (sort: cross-row collisions first, then lowest intra-row similarity)")
print(f"Class B — duplicate DB-row pairs (sim ≥ 0.60):    {len(class_b_pairs)} → {OUT_B_DUPS.name}")
print(f"Class B — garbage-bucket alignments (≥3 mismatched aligns): {len(class_b_buckets)} → {OUT_B_BUCKETS.name}")
print("\nTop 5 Class A (most suspicious):")
for r in class_a[:5]:
    print(f"  db{r['db_id']:>4} {r['suspect_reasons']:<50} "
          f"name='{r['name'][:25]}' yid='{r['name_yiddish'][:25]}' "
          f"collide={r['cross_row_collisions'][:60]}")
print("\nTop 5 Class B duplicate pairs:")
for r in class_b_pairs[:5]:
    print(f"  db{r['db_a']}↔db{r['db_b']} sim={r['token_set_sim']} "
          f"'{(r['name_a'] or r['name_yid_a'])[:30]}' ↔ "
          f"'{(r['name_b'] or r['name_yid_b'])[:30]}' "
          f"aligns {r['aligns_a']}/{r['aligns_b']}")
print("\nTop 5 Class B buckets:")
for r in class_b_buckets[:5]:
    print(f"  db{r['db_id']:>4} mean={r['mean_pairwise_sim']} n={r['aligned_cluster_count']} "
          f"name='{(r['name'] or r['name_yiddish'])[:30]}'")
