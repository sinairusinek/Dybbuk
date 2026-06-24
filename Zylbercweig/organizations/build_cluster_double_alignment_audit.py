"""Audit clusters aligned to more than one DB row.

A cluster_id should appear in exactly ONE db_row.linked_cluster_ids. If it
appears in two or more, that's a data-integrity bug — one of the alignments
is wrong. The resolution is *not* "merge the DBs": the two DBs may be
genuinely distinct entities and the cluster is misaligned to one of them
(e.g. cluster ORG-C02284 is in both DB 501 "Minsk State Yiddish Theatre" and
DB 574 "First Kiev State Yiddish Theatre" — different cities; the cluster
needs to leave one of them, not collapse the two DBs).

Scans core_db.tsv, finds every cluster_id appearing in ≥2 db rows, and emits
cluster_double_alignment.tsv. For each duplicate cluster the report shows:
- the conflicting db_ids and their names/types
- whether the DBs would auto-MERGE in db_dedup_review.tsv (if yes, the
  cluster gets resolved naturally by that merge; if no, human triage is
  required to pick which DB owns the cluster)
- a "suggested action" hint:
    MERGE_DBS  — the DBs are duplicates per db_dedup; cluster resolves via merge
    REMOVE_FROM_ONE — the DBs look distinct (e.g. different settlement
                      tokens, no dedup pair); reviewer must drop cluster
                      from one
    INVESTIGATE — ambiguous; manual inspection needed

Usage:
    .venv/bin/python3 Zylbercweig/organizations/build_cluster_double_alignment_audit.py
"""
from __future__ import annotations

import csv
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
DEDUP = HERE / "db_dedup_review.tsv"
OUT = HERE / "cluster_double_alignment_audit.tsv"


def _norm_settlement_tokens(s: str) -> set[str]:
    """Tokenize a name for shared-settlement detection.

    Hebrew/Yiddish city adjectives end in `-ער` (Minsker, Kiever, Warsawer...).
    Returns the lowercase NFD-stripped tokens; the *intersection* across two
    DB names tells us whether they're talking about the same place.
    """
    s = "".join(c for c in unicodedata.normalize("NFD", s or "") if unicodedata.category(c) != "Mn")
    s = re.sub(r"[׳״'\"\-\.\(\)\[\]]+", " ", s).lower()
    return {t for t in re.split(r"\s+", s) if len(t) >= 4}


def main() -> None:
    if not CORE_DB.exists():
        raise FileNotFoundError(f"Missing: {CORE_DB}")

    with CORE_DB.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    # cluster_id -> list of (db_id, row)
    by_cluster: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for r in rows:
        raw = (r.get("linked_cluster_ids") or "").strip()
        for cid in raw.split("|"):
            cid = cid.strip()
            if cid:
                by_cluster[cid].append((r["db_id"], r))

    duplicates = {cid: hits for cid, hits in by_cluster.items() if len(hits) >= 2}

    # Cross-reference with db_dedup_review.tsv to see if the DBs are already
    # flagged as duplicates. If yes, the natural fix is the DB merge; the
    # cluster gets resolved automatically.
    dedup_pairs: dict[tuple[str, str], dict] = {}
    if DEDUP.exists():
        with DEDUP.open(newline="", encoding="utf-8") as f:
            for d in csv.DictReader(f, delimiter="\t"):
                a, b = d["db_id_a"], d["db_id_b"]
                key = (a, b) if a < b else (b, a)
                dedup_pairs[key] = d

    out_rows: list[dict] = []
    for cid, hits in duplicates.items():
        db_ids = [h[0] for h in hits]
        names = []
        types = set()
        all_tokens = []
        for _, r in hits:
            n_yi = (r.get("name_yiddish") or "").strip()
            n_lat = (r.get("name") or "").strip()
            label = n_yi or n_lat or "(no name)"
            names.append(label)
            types.add((r.get("org_type") or "").strip().casefold())
            all_tokens.append(_norm_settlement_tokens(f"{n_yi} {n_lat}"))

        # Pairwise: are any two of these DBs in the dedup MERGE list?
        merge_flagged_pairs: list[str] = []
        review_flagged_pairs: list[str] = []
        unflagged_pairs: list[tuple[str, str]] = []
        for i in range(len(db_ids)):
            for j in range(i + 1, len(db_ids)):
                a, b = db_ids[i], db_ids[j]
                key = (a, b) if a < b else (b, a)
                d = dedup_pairs.get(key)
                tag = f"{a}↔{b}"
                if d and d["suggested_action"] == "MERGE":
                    merge_flagged_pairs.append(f"{tag}({d['score']})")
                elif d:
                    review_flagged_pairs.append(f"{tag}({d['score']})")
                else:
                    unflagged_pairs.append((a, b))

        # Settlement-token overlap across all involved DB rows.
        if len(all_tokens) >= 2:
            shared_tokens = set.intersection(*all_tokens) if all_tokens else set()
        else:
            shared_tokens = set()

        # Suggested action heuristic:
        # - If every pairing is on the MERGE list → the DB merge resolves it.
        # - Elif the names share settlement tokens AND every pair is on REVIEW
        #   list → likely real duplicate, will resolve once reviewed.
        # - Elif names are name-distinct (no shared settlement tokens and
        #   not on any dedup list) → cluster is misaligned; remove from one.
        # - Else: ambiguous.
        n_pairs = len(merge_flagged_pairs) + len(review_flagged_pairs) + len(unflagged_pairs)
        if n_pairs == 0:
            suggestion = "INVESTIGATE"  # only 1 DB? shouldn't happen but be safe
        elif len(merge_flagged_pairs) == n_pairs:
            suggestion = "MERGE_DBS"
        elif unflagged_pairs and not shared_tokens:
            suggestion = "REMOVE_FROM_ONE"
        elif review_flagged_pairs and shared_tokens:
            suggestion = "MERGE_DBS_PENDING_REVIEW"
        else:
            suggestion = "INVESTIGATE"

        out_rows.append({
            "cluster_id": cid,
            "n_db_ids": str(len(db_ids)),
            "db_ids": " | ".join(db_ids),
            "db_names": " || ".join(names),
            "types": " | ".join(sorted(t for t in types if t)) or "(empty)",
            "type_match": "Y" if len([t for t in types if t]) <= 1 else "N",
            "shared_settlement_tokens": " ".join(sorted(shared_tokens)),
            "dedup_merge_pairs": " ; ".join(merge_flagged_pairs),
            "dedup_review_pairs": " ; ".join(review_flagged_pairs),
            "unflagged_pairs": " ; ".join(f"{a}↔{b}" for a, b in unflagged_pairs),
            "suggested_action": suggestion,
            "reviewer_decision": "",
            "reviewer_notes": "",
        })

    out_rows.sort(key=lambda r: (
        # Highest-priority first: REMOVE_FROM_ONE (real data integrity bug),
        # then INVESTIGATE, then MERGE-pending, then MERGE (will self-resolve)
        {"REMOVE_FROM_ONE": 0, "INVESTIGATE": 1,
         "MERGE_DBS_PENDING_REVIEW": 2, "MERGE_DBS": 3}.get(r["suggested_action"], 9),
        -int(r["n_db_ids"]),
        r["cluster_id"],
    ))

    headers = [
        "cluster_id", "n_db_ids", "db_ids", "db_names", "types", "type_match",
        "shared_settlement_tokens",
        "dedup_merge_pairs", "dedup_review_pairs", "unflagged_pairs",
        "suggested_action", "reviewer_decision", "reviewer_notes",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)

    by_suggestion: dict[str, int] = defaultdict(int)
    for r in out_rows:
        by_suggestion[r["suggested_action"]] += 1
    print(f"Wrote {len(out_rows)} double-aligned clusters to {OUT}")
    for k in ("REMOVE_FROM_ONE", "INVESTIGATE", "MERGE_DBS_PENDING_REVIEW", "MERGE_DBS"):
        v = by_suggestion.get(k, 0)
        if v:
            print(f"  {k}: {v}")

    print("\nTop priorities (REMOVE_FROM_ONE — DBs look distinct):")
    n = 0
    for r in out_rows:
        if r["suggested_action"] != "REMOVE_FROM_ONE":
            continue
        print(f"  {r['cluster_id']:<20} dbs={r['db_ids']}  names={r['db_names'][:80]}")
        n += 1
        if n >= 10:
            break


if __name__ == "__main__":
    main()
