"""Find near-duplicate rows inside core_db.tsv.

Compares every pair of DB rows within the same org_type. For each surface name
field (name, name_yiddish), normalizes Unicode (NFKD→NFC, collapses Hebrew
presentation-form chars) and strips definite articles ("The ", "די ", "דער ",
etc.) before scoring with the same name_similarity used in prepare_alignment.

Emits Zylbercweig/organizations/db_dedup_review.tsv with a recommended keep/drop
and a suggested action (MERGE for high-confidence exact dupes, REVIEW for
borderline cases). Humans confirm; nothing is merged automatically — DB row
deletion would orphan any linked_cluster_ids.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/build_db_dedup_punchlist.py [--threshold 0.85]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
OUT = HERE / "db_dedup_review.tsv"

sys.path.insert(0, str(HERE))
from prepare_alignment import name_similarity  # noqa: E402

_ART_PREFIX = re.compile(
    r"^(?:the\s+|די\s+|דער\s+|דאָס\s+|דעם\s+|der\s+|das\s+|die\s+|la\s+|le\s+|el\s+)",
    re.IGNORECASE,
)


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", unicodedata.normalize("NFKD", s or ""))


def _norm(s: str) -> str:
    s = _ART_PREFIX.sub("", _nfc(s).strip())
    # Drop a trailing parenthetical (often a gloss like "(HAU)" or English
    # translation) which inflates dissimilarity even when the head matches.
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)
    return s.lower().strip()


def _link_count(row: dict) -> int:
    raw = (row.get("linked_cluster_ids") or "").strip()
    return sum(1 for x in raw.split("|") if x.strip())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--threshold", type=float, default=0.85,
                    help="Minimum similarity to include a pair (default 0.85)")
    args = ap.parse_args()

    if not CORE_DB.exists():
        raise FileNotFoundError(f"Missing: {CORE_DB}")

    with CORE_DB.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    by_type: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        t = (r.get("org_type") or "").strip()
        by_type[t].append(r)

    pairs: list[dict] = []
    for t, rs in by_type.items():
        for i in range(len(rs)):
            for j in range(i + 1, len(rs)):
                a, b = rs[i], rs[j]
                best_sim = 0.0
                best_fields = ("", "")
                for fa_key in ("name", "name_yiddish"):
                    for fb_key in ("name", "name_yiddish"):
                        fa, fb = a.get(fa_key, ""), b.get(fb_key, "")
                        na, nb = _norm(fa), _norm(fb)
                        if not na or not nb:
                            continue
                        sim = name_similarity(na, nb)
                        if sim > best_sim:
                            best_sim = sim
                            best_fields = (fa.strip(), fb.strip())
                if best_sim >= args.threshold:
                    keep, drop = (a, b) if _link_count(a) >= _link_count(b) else (b, a)
                    if _link_count(a) == _link_count(b):
                        # tie-break: lower numeric db_id wins (older entry)
                        try:
                            keep, drop = sorted([a, b], key=lambda r: int(r["db_id"]))
                        except ValueError:
                            pass
                    # Downgrade to REVIEW when only ONE side has a parenthetical
                    # qualifier — it often encodes a location ("(St. Louis)") or
                    # branch ("(Azro/Alomis branch)") that distinguishes the two
                    # entities even after paren-stripping made the heads match.
                    # Check both surface fields, not just the matched one: e.g.
                    # Forverts city editions match on Yiddish but the disambiguator
                    # lives in the Latin `name` field.
                    a_blob = f"{a.get('name','')} {a.get('name_yiddish','')}"
                    b_blob = f"{b.get('name','')} {b.get('name_yiddish','')}"
                    a_parens = set(re.findall(r"\(([^)]*)\)", a_blob))
                    b_parens = set(re.findall(r"\(([^)]*)\)", b_blob))
                    # Asymmetric: one side has parens the other lacks, OR both
                    # have parens but with disjoint content (e.g. "(New York)"
                    # vs "(Chicago)" → city-edition siblings, NOT duplicates).
                    asym_paren = bool(a_parens ^ b_parens) and bool(a_parens or b_parens)
                    if a_parens and b_parens and a_parens & b_parens:
                        asym_paren = False  # shared qualifier (e.g. "(HAU)" both sides)
                    if best_sim >= 0.97 and not asym_paren:
                        action = "MERGE"
                    else:
                        action = "REVIEW"
                    pairs.append({
                        "db_id_a": a["db_id"],
                        "db_id_b": b["db_id"],
                        "name_a": a.get("name", "").strip(),
                        "name_b": b.get("name", "").strip(),
                        "name_yi_a": a.get("name_yiddish", "").strip(),
                        "name_yi_b": b.get("name_yiddish", "").strip(),
                        "org_type": t,
                        "matched_fields": f"{best_fields[0]} ~ {best_fields[1]}",
                        "score": f"{best_sim:.3f}",
                        "linked_a": (a.get("linked_cluster_ids") or "").strip(),
                        "linked_b": (b.get("linked_cluster_ids") or "").strip(),
                        "suggested_action": action,
                        "suggested_keep": keep["db_id"],
                        "suggested_drop": drop["db_id"],
                        "reviewer_decision": "",
                        "reviewer_notes": "",
                    })

    pairs.sort(key=lambda p: (-float(p["score"]), p["db_id_a"], p["db_id_b"]))

    headers = [
        "db_id_a", "db_id_b", "name_a", "name_b", "name_yi_a", "name_yi_b",
        "org_type", "matched_fields", "score", "linked_a", "linked_b",
        "suggested_action", "suggested_keep", "suggested_drop",
        "reviewer_decision", "reviewer_notes",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(pairs)

    by_action: dict[str, int] = defaultdict(int)
    for p in pairs:
        by_action[p["suggested_action"]] += 1
    print(f"Wrote {len(pairs)} candidate pairs to {OUT}")
    for k, v in by_action.items():
        print(f"  {k}: {v}")
    print("\nTop 10:")
    for p in pairs[:10]:
        print(f"  {p['score']}  {p['suggested_action']:6}  "
              f"{p['db_id_a']:>4} -> {p['db_id_b']:<4}  "
              f"[{p['org_type'] or '(no type)'}]  "
              f"keep={p['suggested_keep']}  {p['matched_fields'][:80]}")


if __name__ == "__main__":
    main()
