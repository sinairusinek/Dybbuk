#!/usr/bin/env python3.11
"""Step 2a: draw the pilot sample (seeded, reproducible).

  pilot_windows.tsv   40 windowed entries, stratified by volume (6 per volume
                      1-6, 4 from vol 7); windows over 8k chars excluded so the
                      gold stays readable.
  pilot_recall.tsv    30 entries with NO marker hit whose bio extraction calls
                      them `writer` -- the population most likely to have
                      published plays the regex missed. These go to the model
                      as FULL entry text.

    python3.11 make_pilot_sample.py
"""
from __future__ import annotations

import csv
import glob
import json
import pathlib
import random

HERE = pathlib.Path(__file__).resolve().parent
ZYL = HERE.parent.parent
csv.field_size_limit(10**9)
SEED = 20261101


def read(p):
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write(p, rows, fields):
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    rng = random.Random(SEED)
    wins = read(HERE / "candidate_windows.tsv")
    by_vol: dict[str, list] = {}
    for r in wins:
        if int(r["n_chars"]) <= 8000:
            by_vol.setdefault(r["volume"], []).append(r)
    pilot = []
    # The sample is PINNED: it was drawn once (2026-11-01) and gold-annotated.
    # Re-drawing after a windowing change moved 4 gold entries out (their
    # windows grew past 8k), so the ids are fixed and only the text refreshes.
    pinned = HERE / "pilot_ids.txt"
    if pinned.exists():
        ids = pinned.read_text().split()
        by_id = {r["xml_id"]: r for r in wins}
        by_vol = {}
        pilot = [by_id[i] for i in ids if i in by_id]
    for vol in sorted(by_vol):
        k = 4 if vol == "7" else 6
        pilot += rng.sample(sorted(by_vol[vol], key=lambda r: r["xml_id"]), k)
    write(HERE / "pilot_windows.tsv", pilot, list(wins[0].keys()))

    entry_type = {}
    for f in glob.glob(str(ZYL / "Zylbercweig_extraction" / "*III.json")):
        try:
            data = json.load(open(f, encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        for r in data:
            if r.get("entry_type"):
                entry_type[r["xml:id"]] = r["entry_type"]
    windowed = {r["xml_id"] for r in wins}
    entries = read(ZYL / "people" / "entry_texts.tsv")
    pool = [r for r in entries if r["xml_id"] not in windowed
            and entry_type.get(r["xml_id"]) == "writer"]
    recall = rng.sample(sorted(pool, key=lambda r: r["xml_id"]), 30)
    pinned_r = HERE / "pilot_recall_ids.txt"      # pinned too, same reason
    if pinned_r.exists():
        by_id = {r["xml_id"]: r for r in entries}
        recall = [by_id[i] for i in pinned_r.read_text().split()]
    for r in recall:
        r["window_text"] = r["entry_text"]
        r["n_chars"] = len(r["entry_text"])
    write(HERE / "pilot_recall.tsv", recall,
          ["xml_id", "person_id", "volume", "heading", "n_chars", "window_text"])
    print(f"pilot windows: {len(pilot)} ({sum(int(r['n_chars']) for r in pilot):,} chars)")
    print(f"recall probe: {len(recall)} of {len(pool)} unwindowed writers "
          f"({sum(r['n_chars'] for r in recall):,} chars)")


if __name__ == "__main__":
    main()
