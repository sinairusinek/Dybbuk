#!/usr/bin/env python3.11
"""Merge drafts_shard_*.jsonl -> france_itinerary_drafts.jsonl (last good row per entry)."""
import json, glob
from pathlib import Path
H = Path(__file__).resolve().parent
best = {}
for f in sorted(glob.glob(str(H / "drafts_shard_*.jsonl"))):
    for l in open(f, encoding="utf-8"):
        r = json.loads(l)
        if "error" not in r or r["person_id"] not in best:
            best[r["person_id"]] = r
with open(H / "france_itinerary_drafts.jsonl", "w", encoding="utf-8") as o:
    for r in best.values():
        o.write(json.dumps(r, ensure_ascii=False) + "\n")
print(len(best), "entries,", sum("error" in r for r in best.values()), "errors")
