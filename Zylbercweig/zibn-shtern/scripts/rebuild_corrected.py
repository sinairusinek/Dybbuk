#!/usr/bin/env python3
"""One-command rebuild of the corrected unified table + toponym spine, with all
"corrections on top" re-applied so manual review decisions survive regeneration.

`auto_reclassify.py` REGENERATES places_unified_corrected.csv from the raw
extraction, which OVERWRITES any manual corrections previously applied to it.
So the corrections (translit FP fixes + Kimatch review decisions) must be
re-applied every time, and the spine rebuilt afterwards. Running auto_reclassify
by itself leaves the corrected table missing those corrections.

This orchestrator runs, in order:
  1. auto_reclassify.py            → places_unified_corrected.csv  (regenerated)
  2. apply_translit_audit_fixes.py → translit false-positive corrections on top
  3. apply_kimatch_review_decisions.py --apply → Kimatch review-app decisions on
       top (person qids + unlink/clear; org placements → review_applied_org_qids.tsv;
       fetches decisions from the Kima `data` branch via gh)
  4. build_unified_toponyms.py     → toponyms_attestations.csv spine (+ gazetteer,
       unlinked, misresolved); also consumes review_applied_org_qids.tsv for orgs

Usage:
  python scripts/rebuild_corrected.py            # full regen + corrections + spine
  python scripts/rebuild_corrected.py --no-regen # skip step 1 (corrections + spine only)
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_HERE    = Path(__file__).resolve().parent
_RAW     = _HERE.parent / "data" / "raw" / "Zylbercweig-Extraction2026-02-05-places.tsv"
_UNIFIED = _HERE.parent / "data" / "working" / "places_unified_corrected.csv"


def _run(script: str, *args: str) -> None:
    cmd = [sys.executable, str(_HERE / script), *args]
    print(f"\n▶ {script} {' '.join(args)}", flush=True)
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"✗ step failed: {script} (exit {r.returncode})", file=sys.stderr)
        sys.exit(r.returncode)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-regen", action="store_true",
                    help="skip auto_reclassify (re-apply corrections + rebuild spine only)")
    args = ap.parse_args()

    if not args.no_regen:
        _run("auto_reclassify.py", "--input", str(_RAW), "--output", str(_UNIFIED))
    _run("apply_translit_audit_fixes.py")
    _run("apply_kimatch_review_decisions.py", "--apply")
    _run("build_unified_toponyms.py")
    print("\n✓ corrected unified + spine rebuilt with all corrections applied.")


if __name__ == "__main__":
    main()
