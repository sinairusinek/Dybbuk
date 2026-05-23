#!/usr/bin/env python3
"""Apply Kimatch review decisions back to canonical sources (feature A downstream).

Reads decisions from the Kimatch repo `data` branch (or --decisions PATH):
  data/zylbercweig/kimatch_decisions_full.json
Each entry is keyed by source_value (Yiddish spelling):
  flat        : {"action": "map_to:<kima_id>" | "wikidata:<qid>" | "no_match_found"
                            | "skip" | "ambiguous", "kima_id": ..., "note", "reviewer"}
  per-mention : {"mentions": {<record_id>: {"action": ..., "kima_id": ...}}, ...}

Targets (apply only `map_to:` / `wikidata:` placements; the rest are recorded):
  PERSON corpus → places_unified_corrected.csv (canonical; survives a spine rebuild).
                  Per-mention keyed by entry_id; flat by source_value. Rewrites qid +
                  qid_source + correction_applied; labels are re-enriched on rebuild.
  ORG corpus    → kima/review_applied_org_qids.tsv  (org place QIDs are resolved
                  downstream, not stored here — handoff for the org pipeline).
  split         → kima/review_split_punchlist.tsv   (route to the QID-exploder).
  no_match/skip/ambiguous → recorded in the log only.

Every applied change is appended to kima/matching_corrections_log.tsv.
DRY-RUN by default — pass --apply to write.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from collections import Counter
from pathlib import Path

csv.field_size_limit(10**7)

_ZIBN    = Path(__file__).resolve().parents[1]
_WORK    = _ZIBN / "data" / "working"
_KIMA_DIR = _WORK / "kima"
_UNIFIED = _WORK / "places_unified_corrected.csv"
_LOG     = _KIMA_DIR / "matching_corrections_log.tsv"
_ORG_OUT = _KIMA_DIR / "review_applied_org_qids.tsv"
_SPLIT_OUT = _KIMA_DIR / "review_split_punchlist.tsv"

_KIMATCH_LOCAL = Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
_KIMATCH = _KIMATCH_LOCAL if _KIMATCH_LOCAL.exists() else Path.cwd()
_KIMA_CSV = _KIMATCH / "20250126KimaPlacesCSVx.csv"
_DECISIONS_URL = ("https://raw.githubusercontent.com/sinairusinek/kimatch/"
                  "data/data/zylbercweig/kimatch_decisions_full.json")

_QID_SOURCE = "kimatch_review_2026-05-23"


def load_decisions(path: str | None) -> dict:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    with urllib.request.urlopen(_DECISIONS_URL, timeout=20) as fh:
        return json.load(fh)


def kima_qid_index() -> dict[str, str]:
    """kima_id → Wikidata QID (bare), from the Kima places CSV."""
    out: dict[str, str] = {}
    with _KIMA_CSV.open(encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            kid = (r.get("id") or "").strip()
            qid = (r.get("WikiData_Id") or "").strip()
            if kid and qid and qid != "NULL":
                out[kid] = qid
    return out


def target_qid(action: str, kima_qids: dict[str, str]) -> tuple[str, str]:
    """Return (qid, kind) where kind ∈ {place, unlink, none}.
    map_to/wikidata → place; unlink → unlink; everything else → none."""
    if action == "unlink":
        return "", "unlink"
    if action.startswith("map_to:"):
        kid = action.split(":", 1)[1]
        # Guard: a Wikidata QID stored as map_to:Q… is a wikidata placement. (#2)
        if kid.upper().startswith("Q") and kid[1:].isdigit():
            return kid.upper(), "place"
        return kima_qids.get(kid, ""), "place"
    if action.startswith("wikidata:"):
        return action.split(":", 1)[1], "place"
    return "", "none"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--decisions", help="local decisions JSON (default: fetch data branch)")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()

    decisions = load_decisions(args.decisions)
    kima_qids = kima_qid_index()
    print(f"Loaded {len(decisions)} decisions; {len(kima_qids)} kima→qid mappings.",
          file=sys.stderr)

    # Resolve every decision into person/org/split/other targets.
    #   person_by_value[source_value] = qid          (flat placement)
    #   person_by_entry[entry_id]     = qid          (per-mention placement)
    #   org_rows = [(source_value, record_id, qid)]  (handoff to org pipeline)
    #   split_rows = [(source_value, record_id)]
    person_by_value: dict[str, str] = {}
    person_by_entry: dict[str, str] = {}
    person_unlink_value: set[str] = set()
    person_unlink_entry: set[str] = set()
    org_rows: list[tuple[str, str, str, str]] = []   # (source_value, record_id, qid, action)
    split_rows: list[tuple[str, str]] = []
    log_rows: list[tuple] = []
    stats: Counter = Counter()
    no_qid: list[str] = []

    def route(sv, rid, act, qid, kind, reviewer, scope):
        is_org = rid.startswith("ORG-")
        if kind == "unlink":
            if is_org:
                org_rows.append((sv, rid, "", "unlink"))
            elif rid == "*":
                person_unlink_value.add(sv); org_rows.append((sv, "*", "", "unlink"))
            else:
                person_unlink_entry.add(rid)
            stats["unlink"] += 1
            log_rows.append((sv, rid, act, "", reviewer, scope))
            return
        if is_org:
            org_rows.append((sv, rid, qid, "link")); stats["org"] += 1
        elif rid == "*":
            person_by_value[sv] = qid; org_rows.append((sv, "*", qid, "link")); stats["flat"] += 1
        else:
            person_by_entry[rid] = qid; stats["person"] += 1
        log_rows.append((sv, rid, act, qid, reviewer, scope))

    for sv, dec in decisions.items():
        reviewer = dec.get("reviewer", "")
        mentions = dec["mentions"].items() if "mentions" in dec else [("*", dec)]
        scope = "per_mention" if "mentions" in dec else "flat"
        for rid, d in mentions:
            act = d.get("action", "")
            if act == "split":
                split_rows.append((sv, rid)); stats["split"] += 1
                continue
            qid, kind = target_qid(act, kima_qids)
            if kind == "none":
                stats[act or "unset"] += 1
                continue
            if kind == "place" and not qid:
                no_qid.append(f"{sv}/{rid} ({act})"); stats["map_no_qid"] += 1
                continue
            route(sv, rid, act, qid, kind, reviewer, scope)

    # ── apply to places_unified_corrected.csv (person corpus) ───────────────────
    rows = list(csv.DictReader(_UNIFIED.open(encoding="utf-8")))
    fields = list(rows[0].keys())
    changed = cleared = 0
    for r in rows:
        eid, sval = r["entry_id"].strip(), r["source_value"].strip()
        if eid in person_unlink_entry or sval in person_unlink_value:
            if r.get("qid", "").strip():           # clear a wrong/region link
                r["qid"] = ""; r["qid_source"] = _QID_SOURCE
                if "correction_applied" in r:
                    r["correction_applied"] = "True"
                cleared += 1
            continue
        new_qid = person_by_entry.get(eid) or person_by_value.get(sval)
        if new_qid and r.get("qid", "").strip() != new_qid:
            r["qid"] = new_qid; r["qid_source"] = _QID_SOURCE
            if "correction_applied" in r:
                r["correction_applied"] = "True"
            changed += 1

    # ── report ──────────────────────────────────────────────────────────────────
    print("\n=== Kimatch review → canonical apply " +
          ("(APPLY)" if args.apply else "(DRY-RUN)") + " ===")
    print(f"  decisions resolved: {dict(stats)}")
    print(f"  person rows: {changed} qid updates, {cleared} cleared (unlink)")
    print(f"  org handoff rows (review_applied_org_qids.tsv): {len(org_rows)}")
    print(f"  split punchlist rows: {len(split_rows)}")
    if no_qid:
        print(f"  ⚠ {len(no_qid)} placements had no resolvable QID: {', '.join(no_qid[:8])}"
              + (" …" if len(no_qid) > 8 else ""))

    if not args.apply:
        print("\nDry-run — no files written. Re-run with --apply to write.")
        return

    with _UNIFIED.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields); w.writeheader(); w.writerows(rows)
    with _ORG_OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["source_value", "record_id", "qid", "action"])
        w.writerows(org_rows)
    with _SPLIT_OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t"); w.writerow(["source_value", "record_id"])
        w.writerows(split_rows)
    write_header = not _LOG.exists()
    with _LOG.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        if write_header:
            w.writerow(["source_value", "record_id", "action", "qid", "reviewer", "scope"])
        w.writerows(log_rows)
    print(f"\nWrote {changed} qid updates + {cleared} clears to {_UNIFIED.name}, "
          f"{len(org_rows)} org rows, {len(split_rows)} split rows; "
          f"logged {len(log_rows)} to {_LOG.name}.")


if __name__ == "__main__":
    main()
