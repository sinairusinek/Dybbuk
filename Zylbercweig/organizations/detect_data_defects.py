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
OUT_C_ORPHANS = HERE / "db_orphaned_links_punchlist.tsv"
# PI-confirmed benign variant pairs — rows where name and name_yiddish are
# legitimate orthographic/declension alternates of the same entity, not
# contamination. Skipped from the Class A low_intra_sim check. Append rows here
# (one per db_id) when PI confirms a variant pair is benign.
VARIANT_CONFIRMED = HERE / "name_variant_pairs_confirmed.tsv"

_CONFIRMED_BENIGN: set[str] = set()
if VARIANT_CONFIRMED.exists():
    with VARIANT_CONFIRMED.open(newline="", encoding="utf-8") as _f:
        for _r in csv.DictReader(_f, delimiter="\t"):
            _did = (_r.get("db_id") or "").strip()
            if _did:
                _CONFIRMED_BENIGN.add(_did)

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
# shouldn't keep showing up in dedup punchlists every run. Same for
# out-of-project rows (modern Israeli publishers, peripheral entities).
db_rows = [r for r in db_rows
           if (r.get("deprecated","") or "").strip().lower() != "true"
           and (r.get("out_of_project","") or "").strip().lower() != "true"]

# PI-confirmed distinct pairs (Class B false positives) — pairs that scored
# highly on token_set but are different entities (e.g. db26 NY Public Library
# vs db348 NY Public Theatre). Skipped from the Class B dup punchlist.
# Also suppress pairs that are already in db_pairs_pending_review.tsv (the
# human-curated work queue), so the active punchlist surfaces only NEW dups.
_CONFIRMED_DISTINCT: set[frozenset[str]] = set()
for _src in ("confirmed_distinct_pairs.tsv", "db_pairs_pending_review.tsv"):
    _p = HERE / _src
    if not _p.exists():
        continue
    with _p.open(newline="", encoding="utf-8") as _f:
        for _r in csv.DictReader(_f, delimiter="\t"):
            a, b = (_r.get("db_a","") or "").strip(), (_r.get("db_b","") or "").strip()
            if a and b:
                _CONFIRMED_DISTINCT.add(frozenset({a, b}))
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

    # (1) internal disagreement (only when BOTH populated AND same-script,
    # AND not a PI-confirmed benign variant pair).
    # Cross-script pairs (English `name` + Yiddish `name_yiddish`) are
    # translations of the same entity — token-set can't bridge them, so
    # low_intra_sim there is a false positive (e.g. db151 Tsentral/צענטראַל).
    if name and name_yid and db_id not in _CONFIRMED_BENIGN:
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

    # (3) likely backfilled from an unattributed alignment — kept ONLY as a
    # CO-signal, not a standalone reason. A 2026-06-01 sample of 8 random
    # blank-reviewer-backfill rows found 8/8 correctly aligned (token_sim ~1.0
    # between row's name_yiddish and the aligned cluster's canonical), so this
    # provenance signal on its own is noise. Worth flagging only when something
    # else (collision or intra-row disagreement) is already suspicious.
    if name_yid and blank_rev and not name and reasons:
        reasons.append(f"backfilled_from_blank_reviewer_aligns={len(blank_rev)}")

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

if class_a:
    with OUT_A.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(class_a[0].keys()),
                           delimiter="\t", lineterminator="\n")
        w.writeheader(); w.writerows(class_a)
elif OUT_A.exists():
    OUT_A.unlink()  # don't leave a stale empty file around

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
    # PI-confirmed distinct: skip.
    if frozenset({a, b}) in _CONFIRMED_DISTINCT:
        continue
    # Skip parent-child and sibling-via-parent pairs (umbrella-with-locals,
    # like Forverts db249 + db692/693/694/695 or Hashomer Hatzair db602 +
    # db696/697/698). These collide by design, not by error.
    ra, rb = db_by_id[a], db_by_id[b]
    pa = (ra.get("parent_db_id","") or "").strip()
    pb = (rb.get("parent_db_id","") or "").strip()
    if pa == b or pb == a or (pa and pa == pb):
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
if class_b_pairs:
    with OUT_B_DUPS.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(class_b_pairs[0].keys()),
                           delimiter="\t", lineterminator="\n")
        w.writeheader(); w.writerows(class_b_pairs)
elif OUT_B_DUPS.exists():
    OUT_B_DUPS.unlink()  # don't leave a stale empty file

# ── Class B: garbage-bucket detection ─────────────────────────────────────
# A db row with ≥3 aligned clusters whose canonicals don't mutually match.
# PI-confirmed clean buckets (multi-spelling consolidations / post-merge
# aggregations where the canonical-similarity metric is a false-positive
# because token-set can't bridge spelling variants of the same entity) are
# read from confirmed_clean_buckets.tsv and skipped.
_CONFIRMED_CLEAN_BUCKETS: set[str] = set()
_CLEAN_BUCKETS_FILE = HERE / "confirmed_clean_buckets.tsv"
if _CLEAN_BUCKETS_FILE.exists():
    with _CLEAN_BUCKETS_FILE.open(newline="", encoding="utf-8") as _f:
        for _r in csv.DictReader(_f, delimiter="\t"):
            _did = (_r.get("db_id") or "").strip()
            if _did:
                _CONFIRMED_CLEAN_BUCKETS.add(_did)

class_b_buckets: list[dict[str, str]] = []
for db_id, aligned in aligns_by_db.items():
    if len(aligned) < 3:
        continue
    if db_id in _CONFIRMED_CLEAN_BUCKETS:
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
if class_b_buckets:
    with OUT_B_BUCKETS.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(class_b_buckets[0].keys()),
                           delimiter="\t", lineterminator="\n")
        w.writeheader(); w.writerows(class_b_buckets)
elif OUT_B_BUCKETS.exists():
    OUT_B_BUCKETS.unlink()  # don't leave a stale empty file

# ── Class C — orphaned rows (active, but no linked clusters) ────────────────
# The settlement-audit re-align path (settlement_audit._align_clusters_to_db)
# drops a cluster from its previous owner but never deprecates a row it leaves
# empty. Moving a DB's LAST cluster strands it: active, but linked to nothing.
# Standing guard so future strandings surface instead of hiding as "no mentions".
# (db_rows already excludes deprecated/out_of_project; also drop merged_into.)
class_c = [
    {
        "db_id": r.get("db_id", ""),
        "name": r.get("name", ""),
        "name_yiddish": r.get("name_yiddish", ""),
        "org_type": r.get("org_type", ""),
    }
    for r in db_rows
    if not (r.get("merged_into", "") or "").strip()
    and not (r.get("linked_cluster_ids", "") or "").strip()
]
class_c.sort(key=lambda r: (r["org_type"], r["db_id"]))
if class_c:
    with OUT_C_ORPHANS.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(class_c[0].keys()),
                           delimiter="\t", lineterminator="\n")
        w.writeheader(); w.writerows(class_c)
elif OUT_C_ORPHANS.exists():
    OUT_C_ORPHANS.unlink()

# ── Summary ───────────────────────────────────────────────────────────────
print(f"Class A — name_yiddish contamination candidates: {len(class_a)} → {OUT_A.name}")
print(f"  (sort: cross-row collisions first, then lowest intra-row similarity)")
print(f"Class B — duplicate DB-row pairs (sim ≥ 0.60):    {len(class_b_pairs)} → {OUT_B_DUPS.name}")
print(f"Class B — garbage-bucket alignments (≥3 mismatched aligns): {len(class_b_buckets)} → {OUT_B_BUCKETS.name}")
print(f"Class C — orphaned rows (active, no linked clusters):       {len(class_c)} → {OUT_C_ORPHANS.name}")
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
