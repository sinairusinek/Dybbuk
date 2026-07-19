"""Flag B3 decisions taken before the candidate set was repaired.

Commit 972355a2 restored disambiguation candidates that three defects had been
hiding from B3 (a hard gender gate that erased every actress, a bracket-strip
that split surnames, and exact-only surname lookup). Every decision recorded
before that commit was therefore made against an incomplete option list — most
consequentially the 118 טאמאשעווסקי decisions taken with Bessie Thomashefsky
absent from all 118.

This script diffs each decided mention's candidate set BEFORE vs AFTER the fix
and emits the affected decisions, so they can be re-reviewed rather than
silently inherited.

Priority:
  high — the reviewer ABSTAINED (family / other) and candidates were added.
         An abstain is exactly what you'd expect when the right person is not
         on the list, so these are the likely-wrong ones.
  low  — the reviewer picked a hub and candidates were added. A positive pick
         is real evidence, but a better option may now exist.

Baseline defaults to the last commit before the fix; override with --baseline
(any git revision, or a path to a resolutions TSV).

Run: python3.11 Zylbercweig/people/build_rereview_queue.py
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1]
RESOLUTIONS_TSV = HERE / "mention_surname_resolutions.tsv"
DECISIONS_TSV = HERE / "mention_resolution_decisions.tsv"
HUB_TSV = HERE / "person_hub.tsv"
OUT_TSV = HERE / "mention_rereview_queue.tsv"

# last commit before the candidate-restoration fix (972355a2)
DEFAULT_BASELINE = "7cd76db3"
REL_PATH = "Zylbercweig/people/mention_surname_resolutions.tsv"

OUT_FIELDS = [
    "priority", "mention_id", "surname", "host_heading", "sentence",
    "prior_decision_kind", "prior_hub_id", "prior_heading",
    "n_added", "added_hub_ids", "added_headings",
    "reviewer", "reviewed_at",
]


def _read_tsv(path: pathlib.Path) -> list[dict]:
    csv.field_size_limit(sys.maxsize)
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_baseline(baseline: str) -> list[dict]:
    """Baseline resolutions from a git revision, or a TSV path."""
    p = pathlib.Path(baseline)
    if p.exists():
        return _read_tsv(p)
    csv.field_size_limit(sys.maxsize)
    blob = subprocess.run(
        ["git", "-C", str(REPO), "show", f"{baseline}:{REL_PATH}"],
        capture_output=True, text=True, check=True).stdout
    return list(csv.DictReader(blob.splitlines(), delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", default=DEFAULT_BASELINE,
                    help="git revision or path holding the pre-fix resolutions")
    args = ap.parse_args()

    def cands(rows: list[dict]) -> dict[str, set[str]]:
        return {r["mention_id"]: {h for h in r["candidate_hub_ids"].split("|") if h}
                for r in rows}

    now_rows = _read_tsv(RESOLUTIONS_TSV)
    old = cands(load_baseline(args.baseline))
    new = cands(now_rows)
    by_mid = {r["mention_id"]: r for r in now_rows}
    hubs = {r["hub_id"]: r["canonical_heading"] for r in _read_tsv(HUB_TSV)}

    out = []
    for d in _read_tsv(DECISIONS_TSV):
        mid = d["mention_id"]
        added = sorted(new.get(mid, set()) - old.get(mid, set()))
        if not added:
            continue
        r = by_mid.get(mid, {})
        out.append({
            "priority": "high" if d["decision_kind"] in ("family", "other") else "low",
            "mention_id": mid,
            "surname": d["surname"],
            "host_heading": r.get("host_heading", ""),
            "sentence": r.get("sentence", ""),
            "prior_decision_kind": d["decision_kind"],
            "prior_hub_id": d["resolved_hub_id"],
            "prior_heading": d["resolved_heading"],
            "n_added": len(added),
            "added_hub_ids": "|".join(added),
            "added_headings": "|".join(hubs.get(h, h) for h in added),
            "reviewer": d.get("reviewer", ""),
            "reviewed_at": d.get("reviewed_at", ""),
        })

    out.sort(key=lambda r: (r["priority"] != "high", r["surname"], r["mention_id"]))
    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=OUT_FIELDS, delimiter="\t",
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(out)

    hi = [r for r in out if r["priority"] == "high"]
    print(f"baseline: {args.baseline}")
    print(f"decisions needing re-review: {len(out)}  (high {len(hi)} / low {len(out) - len(hi)})")
    print(f"wrote {OUT_TSV.relative_to(REPO)}")
    per: dict[str, int] = {}
    for r in hi:
        per[r["surname"]] = per.get(r["surname"], 0) + 1
    print("\nhigh-priority (abstained while candidates were missing), by surname:")
    for s, n in sorted(per.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4d}  {s}")


if __name__ == "__main__":
    main()
