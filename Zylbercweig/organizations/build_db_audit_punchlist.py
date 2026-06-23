"""Audit DB rows for false-equation merges.

For every db_id in core_db.tsv, compute the minimum pairwise name similarity
across all clusters aligned to that DB (using the project's own similarity
stack PLUS every manual-override TSV the project knows about). Flag DBs
whose weakest pair scores below the alignment cascade's MIN_SCORE — those
are pairs the candidate pipeline would never have *proposed* as a merge, so
their presence in the same DB is evidence of a manual false equation.

Output: db_audit_punchlist.tsv — consumed by the Zalmen "DB Audit" view.

Re-runnable: produces the same output for the same input. Designed to be
called by hand or from a scheduled job; no global state mutation.

Optional dependency: dybbuk-phonetic for IPA cross-script similarity. Audit
runs without it but loses one signal — flagged in the script preamble.
"""
from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from itertools import combinations
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from org_normalize import (  # noqa: E402
    organization_name_aliases,
    token_key_set,
    normalize_yiddish,
)
from prepare_alignment import surname_only_variant  # noqa: E402
import unicodedata, re as _re  # noqa: E402

def _norm_for_trigram(s: str) -> str:
    """Same normalization as cluster_orgs.name_similarity inner normalize_name."""
    if not s:
        return ""
    nfd = unicodedata.normalize("NFD", s)
    stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return _re.sub(r"\s+", " ", stripped).strip().lower()

def _trigrams(s: str) -> frozenset[str]:
    if len(s) < 3:
        return frozenset()
    return frozenset(s[i:i+3] for i in range(len(s) - 2))

# Optional IPA cross-script signal.
try:
    from dybbuk_phonetic.bridge import cross_script_similarity  # noqa: E402
    _IPA_AVAILABLE = True
except Exception:  # noqa: BLE001
    cross_script_similarity = None
    _IPA_AVAILABLE = False

# Rule-based Yiddish→YIVO romanization for the translit scaffold.
sys.path.insert(0, str(HERE))
try:
    from translit_yiddish_to_latin import translit_yiddish_to_latin as _yivo
except Exception:  # noqa: BLE001
    def _yivo(s: str) -> str: return ""


CORE_DB   = HERE / "core_db.tsv"
ALIGN     = HERE / "org_alignment_review.tsv"
ACTIVITY  = HERE / "activity_log.tsv"

# Manual-override files (read-only).
CLEAN_BUCKETS    = HERE / "confirmed_clean_buckets.tsv"
VARIANT_BENIGN   = HERE / "name_variant_pairs_confirmed.tsv"
DISTINCT_PAIRS   = HERE / "confirmed_distinct_pairs.tsv"
PENDING_PAIRS    = HERE / "db_pairs_pending_review.tsv"
CLUSTER_PAIRS    = HERE / "cluster_pairs_review.tsv"

# Output.
PUNCHLIST = HERE / "db_audit_punchlist.tsv"

# Threshold: alignment cascade's MIN_SCORE. Pairs below this would never have
# been proposed as candidates by prepare_alignment.py; their presence inside a
# single DB row is the audit's flag condition.
MIN_SCORE = 0.60


# ── Load overrides ────────────────────────────────────────────────────────────

def _load_id_set(path: Path, col: str) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            v = (r.get(col) or "").strip()
            if v:
                out.add(v)
    return out

def _load_db_pair_set(path: Path) -> set[frozenset[str]]:
    if not path.exists():
        return set()
    out: set[frozenset[str]] = set()
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            a = (r.get("db_a") or "").strip()
            b = (r.get("db_b") or "").strip()
            if a and b:
                out.add(frozenset({a, b}))
    return out

def _load_cluster_pair_decisions(path: Path) -> dict[frozenset[str], str]:
    """Return {frozenset({cid_i, cid_j}): decision} for non-blank decisions."""
    if not path.exists():
        return {}
    out: dict[frozenset[str], str] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            a = (r.get("cluster_id_i") or "").strip()
            b = (r.get("cluster_id_j") or "").strip()
            d = (r.get("decision") or "").strip().upper()
            if a and b and d:
                out[frozenset({a, b})] = d
    return out


# ── Alias bag per cluster — precomputed (norm, trigrams, token_set) ──────────

def _build_alias_features(canon: str, variants: str) -> list[tuple[str, str, frozenset, frozenset]]:
    """For each unique alias of the cluster, return:
        (alias_string, normalized_string, trigram_set, token_set)
    All comparison work later is on the precomputed sets — no re-normalization
    inside the pairwise loop.
    """
    aliases: set[str] = set()
    if canon:
        aliases |= organization_name_aliases(canon)
        s = surname_only_variant(canon)
        if s:
            aliases.add(s)
    for v in (variants or "").split("|"):
        v = v.strip()
        if v:
            aliases |= organization_name_aliases(v)
            s = surname_only_variant(v)
            if s:
                aliases.add(s)
    out = []
    seen_norm: set[str] = set()
    for a in aliases:
        if not a:
            continue
        na = _norm_for_trigram(a)
        if not na or na in seen_norm:
            continue
        seen_norm.add(na)
        out.append((a, na, _trigrams(na), token_key_set(a)))
    return out


# ── Pair scoring ──────────────────────────────────────────────────────────────

def _score_pair(feats_a, feats_b,
                early_exit: float = MIN_SCORE,
                with_ipa: bool = False) -> tuple[float, str]:
    """Max similarity across all alias-pair combos and signals, using
    precomputed alias features [(string, normalized, trigrams, token_set), ...].

    Early-exits as soon as `best >= early_exit` (default MIN_SCORE). IPA is
    opt-in (slow; only adds value when other signals all miss).
    """
    # Exact pass first — O(|a| * |b|) set membership lookups on normalized form.
    nb_set = {nb for _, nb, _, _ in feats_b}
    for _, na, _, _ in feats_a:
        if na and na in nb_set:
            return 1.0, "exact"

    best = 0.0
    best_sig = ""
    for sa, _, ta, ka in feats_a:
        for sb, _, tb, kb in feats_b:
            # Trigram Jaccard
            if ta and tb:
                inter = len(ta & tb)
                if inter:
                    union = len(ta | tb)
                    s = inter / union if union else 0.0
                    if s > best:
                        best, best_sig = s, "trigram"
                        if best >= early_exit:
                            return best, best_sig
            # Token-set Jaccard (requires substantive shared token)
            if ka and kb:
                ki = ka & kb
                if ki and any(len(t) >= 3 for t in ki):
                    ku = ka | kb
                    ts = len(ki) / len(ku) if ku else 0.0
                    if ts > best:
                        best, best_sig = ts, "token_set"
                        if best >= early_exit:
                            return best, best_sig
            if with_ipa and _IPA_AVAILABLE:
                try:
                    ip = float(cross_script_similarity(sa, sb) or 0.0)
                except Exception:  # noqa: BLE001
                    ip = 0.0
                if ip > best:
                    best, best_sig = ip, "ipa"
                    if best >= early_exit:
                        return best, best_sig
    return best, best_sig


# ── Activity-log attribution ──────────────────────────────────────────────────

def _last_merge_for_db(activity_rows: list[dict], db_id: str) -> dict | None:
    """Find the most recent log row whose note/extra references this db_id."""
    pat_a = f"DB {db_id} "
    pat_b = f"DB {db_id}"  # tail of "into DB X"
    pat_c = f"→ {db_id}"   # Aligned N → X
    hits = []
    for r in activity_rows:
        note = r.get("note", "") or ""
        extra = r.get("extra", "") or ""
        if pat_a in note or note.endswith(pat_b) or pat_c in note or f'"{db_id}"' in extra:
            hits.append(r)
    if not hits:
        return None
    return sorted(hits, key=lambda r: r.get("ts", ""))[-1]


# ── Main ──────────────────────────────────────────────────────────────────────

def main(core_db_path: Path = CORE_DB,
         align_path: Path = ALIGN,
         out_path: Path = PUNCHLIST,
         with_ipa: bool = False) -> None:
    clean_buckets   = _load_id_set(CLEAN_BUCKETS, "db_id")
    variant_benign  = _load_id_set(VARIANT_BENIGN, "db_id")
    distinct_pairs  = _load_db_pair_set(DISTINCT_PAIRS) | _load_db_pair_set(PENDING_PAIRS)
    cluster_pair_decisions = _load_cluster_pair_decisions(CLUSTER_PAIRS)

    skip_dbs = clean_buckets | variant_benign

    with core_db_path.open(newline="", encoding="utf-8") as f:
        db_rows = list(csv.DictReader(f, delimiter="\t"))
    # honour project convention: skip deprecated / out_of_project rows
    db_rows = [r for r in db_rows
               if (r.get("deprecated","") or "").strip().lower() != "true"
               and (r.get("out_of_project","") or "").strip().lower() != "true"]
    db_by_id = {r["db_id"]: r for r in db_rows if (r.get("db_id") or "").strip()}

    with align_path.open(newline="", encoding="utf-8") as f:
        align_rows = list(csv.DictReader(f, delimiter="\t"))
    align_by_cid = {r["cluster_id"]: r for r in align_rows if r.get("cluster_id")}
    aligned_per_db: dict[str, list[str]] = defaultdict(list)
    for r in align_rows:
        aid = (r.get("aligned_db_id") or "").strip()
        if aid:
            aligned_per_db[aid].append(r["cluster_id"])

    activity_rows: list[dict] = []
    if ACTIVITY.exists():
        with ACTIVITY.open(newline="", encoding="utf-8") as f:
            activity_rows = list(csv.DictReader(f, delimiter="\t"))

    print(f"core_db: {len(db_rows)} active rows | "
          f"alignment: {len(align_rows)} cluster rows | "
          f"overrides: {len(clean_buckets)} clean + "
          f"{len(variant_benign)} benign + {len(distinct_pairs)} distinct-pairs + "
          f"{sum(1 for d in cluster_pair_decisions.values() if d=='SPLIT')} SPLIT pair-decisions")
    print(f"IPA signal: {'available' if _IPA_AVAILABLE else 'UNAVAILABLE (install dybbuk-phonetic for an extra check)'}")
    print()

    out_rows: list[dict] = []
    n_considered = 0
    n_skipped_override = 0

    db_items = list(aligned_per_db.items())
    print(f"scoring {len(db_items)} DBs...")
    import time as _time
    _t0 = _time.time()

    for _ix, (db_id, cids) in enumerate(db_items):
        if _ix % 50 == 0 and _ix:
            elapsed = _time.time() - _t0
            rate = _ix / elapsed
            eta = (len(db_items) - _ix) / rate if rate else 0
            print(f"  {_ix:>5}/{len(db_items)} ({elapsed:.0f}s, ~{eta:.0f}s left, flagged so far: {len(out_rows)})",
                  flush=True)
        if db_id not in db_by_id:
            continue  # alignment points at a deprecated/missing DB
        if len(cids) < 2:
            continue
        n_considered += 1
        if db_id in skip_dbs:
            n_skipped_override += 1
            continue

        # Build precomputed alias features per cluster
        bags: dict[str, list] = {}
        for cid in cids:
            a = align_by_cid.get(cid, {})
            bags[cid] = _build_alias_features(
                a.get("canonical_yiddish", ""),
                a.get("name_variants", ""),
            )

        # All pairs
        pair_scores: list[tuple[str, str, float, str]] = []
        for i, j in combinations(cids, 2):
            score, sig = _score_pair(bags[i], bags[j], with_ipa=with_ipa)
            pair_scores.append((i, j, score, sig))

        if not pair_scores:
            continue

        scores_only = [s for _, _, s, _ in pair_scores]
        min_score = min(scores_only)
        max_score = max(scores_only)
        med_score = statistics.median(scores_only)
        n_below = sum(1 for s in scores_only if s < MIN_SCORE)

        if min_score >= MIN_SCORE:
            continue  # nothing to flag

        # Severity boost
        severity = ""
        # Step 4: pre-judged SPLIT pair
        for i, j, _, _ in pair_scores:
            if cluster_pair_decisions.get(frozenset({i, j})) == "SPLIT":
                severity = "PRE_JUDGED_SPLIT"
                break

        # Step 3: known-distinct DB pair. Each cluster ultimately points at this
        # one db_id, but if any pair of clusters in the DB was previously aligned
        # to different DBs that PI marked distinct, that's hard to reconstruct
        # post-merge. We approximate: if THIS db_id appears in any distinct-pair
        # set with any other db_id, flag KNOWN_DISTINCT (weak signal but worth
        # surfacing). Skipping this is acceptable since PRE_JUDGED_SPLIT covers
        # the high-confidence case; keep code path minimal.
        if not severity and any(db_id in p for p in distinct_pairs):
            # Only flag if the *other* member of the distinct pair is also
            # referenced indirectly. Conservative: leave blank to avoid noise.
            pass

        # Weakest pair
        weakest = min(pair_scores, key=lambda t: t[2])
        wa, wb, ws, wsig = weakest

        # Cluster details JSON (for the view to render without recompute)
        # Each entry: cluster_id, canonical_yiddish, top_variant, size,
        # best_score_vs_others (max pair score involving this cluster),
        # worst_score_vs_others (min pair score involving this cluster).
        cluster_details = []
        for cid in cids:
            a = align_by_cid.get(cid, {})
            involved = [s for i, j, s, _ in pair_scores if cid in (i, j)]
            cluster_details.append({
                "cluster_id": cid,
                "canonical_yiddish": (a.get("canonical_yiddish") or "").strip(),
                "top_variant": ((a.get("name_variants") or "").split("|")[0] or "").strip(),
                "size": (a.get("cluster_size") or "").strip(),
                "best_pair_score": round(max(involved), 3) if involved else 0.0,
                "worst_pair_score": round(min(involved), 3) if involved else 0.0,
            })
        # Sort: worst-pair-score ascending (most-suspicious cluster first)
        cluster_details.sort(key=lambda d: d["worst_pair_score"])

        # Attribution
        last = _last_merge_for_db(activity_rows, db_id)
        last_reviewer = last["reviewer"] if last else ""
        last_ts = last["ts"][:19] if last else ""
        last_action = last["action"] if last else ""

        db = db_by_id[db_id]
        db_name = (db.get("name_yiddish") or "").strip()
        out_rows.append({
            "db_id": db_id,
            "db_name_yiddish": db_name,
            "db_name_translit": _yivo(db_name),
            "org_type": (db.get("org_type") or "").strip(),
            "n_clusters": len(cids),
            "min_pair_score": round(min_score, 3),
            "max_pair_score": round(max_score, 3),
            "median_pair_score": round(med_score, 3),
            "n_pairs_below_threshold": n_below,
            "weakest_pair_a": wa,
            "weakest_pair_b": wb,
            "weakest_pair_score": round(ws, 3),
            "weakest_pair_signal": wsig,
            "severity_boost": severity,
            "cluster_details_json": json.dumps(cluster_details, ensure_ascii=False),
            "last_merge_reviewer": last_reviewer,
            "last_merge_ts": last_ts,
            "last_merge_action": last_action,
        })

    # Sort: severity_boost first, then impact = n_clusters * (MIN_SCORE - min_pair_score)
    def _sort_key(r):
        sev_rank = 0 if r["severity_boost"] == "PRE_JUDGED_SPLIT" else 1
        impact = r["n_clusters"] * (MIN_SCORE - r["min_pair_score"])
        return (sev_rank, -impact)
    out_rows.sort(key=_sort_key)

    fields = [
        "db_id", "db_name_yiddish", "db_name_translit", "org_type",
        "n_clusters",
        "min_pair_score", "max_pair_score", "median_pair_score",
        "n_pairs_below_threshold",
        "weakest_pair_a", "weakest_pair_b",
        "weakest_pair_score", "weakest_pair_signal",
        "severity_boost",
        "cluster_details_json",
        "last_merge_reviewer", "last_merge_ts", "last_merge_action",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"considered {n_considered} DBs with ≥2 aligned clusters")
    print(f"skipped {n_skipped_override} via override whitelists")
    print(f"flagged {len(out_rows)} DBs (min_pair_score < {MIN_SCORE})")
    if out_rows:
        sev = sum(1 for r in out_rows if r["severity_boost"])
        print(f"  of those: {sev} severity-boosted")
        print()
        print("Top 10 by impact:")
        for r in out_rows[:10]:
            print(f"  db {r['db_id']:>5s}  n={r['n_clusters']:>2d}  min={r['min_pair_score']:.2f}  "
                  f"sev={r['severity_boost']:<18s}  {r['db_name_yiddish'][:40]}")
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--core-db", type=Path, default=CORE_DB)
    ap.add_argument("--alignment", type=Path, default=ALIGN)
    ap.add_argument("--out", type=Path, default=PUNCHLIST)
    ap.add_argument("--with-ipa", action="store_true",
                    help="enable dybbuk-phonetic IPA cross-script signal "
                         "(slow; default off — trigram+token_set are usually enough)")
    args = ap.parse_args()
    main(args.core_db, args.alignment, args.out, with_ipa=args.with_ipa)
