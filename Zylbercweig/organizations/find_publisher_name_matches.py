#!/usr/bin/env python3
"""Find publisher matches hidden behind the word פֿאַרלאַג.

Sinai's observation, 2026-10-13: many of these names carry פֿאַרלאַג or its
abbreviation פ״ג, and the matcher misses matches because it compares whole
strings — "פֿאַרלאָג „די וועלט"" and "די וועלט" are the same firm written two ways.

This is the ORG case, not the press case. press_match_key.py deliberately keeps
periodical type words (צייטונג, בלאט, שטימע) because for a PAPER the type word
distinguishes: Folksblat and Folkstsaytung are different papers. For a FIRM it
does not — פֿאַרלאַג names the business form, like "Ltd." or "& Sons", so it is
stripped here.

Spelling varies widely and the normalizer must absorb all of it:
    פארלאג · פאַרלאַג · פאַרלאָג · פערלאַג · פערלאָג · פֿאַרלאַג · פּערלאג
    פ״ג · פ'ג · פֿ'ג          (the abbreviation)
    …-פֿאַרלאַג              (as a compound tail: שטיבל-פֿאַרלאַג)

Reports three kinds, and decides nothing:
  1. OUTSIDE  — an open cluster whose stripped name matches a live core_db row.
  2. WITHIN   — two or more open clusters that collapse to the same key.
  3. (both are candidates for review, never auto-applied: a shared name is not
     identity, which is the lesson of Kultur Minsk/Warsaw and Arbeter Heim.)

Usage: python3 find_publisher_name_matches.py [--json OUT]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import subprocess
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10 ** 9)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
OPEN = HERE / "publisher_remaining_open.json"

FINALS = {"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"}

# After normalise() the vowel points are gone and finals are folded, so every
# spelling above lands on one of these few forms.
TYPE_WORDS = re.compile(
    r"^(פארלאג|פארלאג|פערלאג|פרלאג|פרלג|פג|"
    r"דרוקעריי|דרוקערײ|אויסגאבע|ביבליאטעק|"
    r"farlag|ferlag|verlag|press|publishing|publishers|publisher)$"
)


def normalise(s: str) -> str:
    s = "".join(c for c in unicodedata.normalize("NFD", s or "")
                if not unicodedata.combining(c))
    s = s.replace("־", " ").replace("-", " ")
    s = re.sub(r"[\"״'`’“”„»«]", "", s)
    s = "".join(c if (c.isalnum() or c.isspace()) else " " for c in s)
    return " ".join("".join(FINALS.get(c, c) for c in t) for t in s.split())


def key(s: str) -> str:
    """The name with its business-form word removed."""
    return " ".join(t for t in normalise(s).split() if not TYPE_WORDS.match(t)).strip()


def git_show(path: str) -> str:
    return subprocess.run(["git", "show", f"origin/main:{path}"], cwd=REPO,
                          capture_output=True, text=True, check=True).stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", dest="out")
    a = ap.parse_args()

    rows = json.loads(OPEN.read_text(encoding="utf-8"))
    db = list(csv.DictReader(
        io.StringIO(git_show("Zylbercweig/organizations/core_db.tsv")), delimiter="\t"))
    live = [r for r in db
            if (r.get("deprecated") or "").strip().lower() != "true"
            and (r.get("out_of_project") or "").strip().lower() != "true"]

    index: dict[str, list[dict]] = defaultdict(list)
    for r in live:
        seen = set()
        for cand in [r.get("name_yiddish"), r.get("name"),
                     *(r.get("name_variants") or "").split("|")]:
            k = key(cand or "")
            if k and (k, r["db_id"]) not in seen:
                seen.add((k, r["db_id"]))
                index[k].append(r)

    outside, within_groups = [], []
    by_key: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        k = key(r["name"])
        if not k:
            continue
        by_key[k].append(r)
        for hit in index.get(k, []):
            outside.append({
                "cluster_id": r["cluster_id"], "cluster_name": r["name"],
                "city": r["city"], "mentions": r["mentions"],
                "db_id": hit["db_id"],
                "db_name": hit.get("name_yiddish") or hit.get("name", ""),
                "db_type": hit.get("org_type", ""),
                "key": k,
            })
    for k, v in by_key.items():
        if len(v) > 1:
            within_groups.append({
                "key": k,
                "same_city": len({x["city"] for x in v}) == 1 and v[0]["city"] != "—",
                "members": [{"cluster_id": x["cluster_id"], "name": x["name"],
                             "city": x["city"], "mentions": x["mentions"]} for x in v],
            })

    print(f"open clusters: {len(rows)}")
    print(f"  carry a business-form word: "
          f"{sum(1 for r in rows if key(r['name']) != normalise(r['name']))}")
    print(f"\nOUTSIDE — stripped name matches a live core_db row: {len(outside)}")
    for m in outside:
        print(f"  {m['cluster_id']:16s} {m['cluster_name'][:34]:34s} [{m['city'] or '—'}]"
              f"  ->  db {m['db_id']:5s} {m['db_name'][:26]:26s} [{m['db_type'][:20]}]")
    print(f"\nWITHIN — open clusters that collapse together: {len(within_groups)} group(s)")
    for g in within_groups:
        print(f"  «{g['key']}»{'  SAME CITY' if g['same_city'] else ''}")
        for m in g["members"]:
            print(f"      {m['cluster_id']:16s} {m['name'][:36]:36s} "
                  f"[{m['city'] or '—'}] {m['mentions']}m")

    print("\nNothing is decided here. A shared name is not identity — this audit "
          "has refused\nKultur Minsk/Warsaw, Arbeter Heim Warsaw/Riga and Yavneh "
          "Warsaw/Tel Aviv on research.")

    if a.out:
        Path(a.out).write_text(json.dumps(
            {"outside": outside, "within": within_groups}, ensure_ascii=False, indent=1),
            encoding="utf-8")
        print(f"\nwrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
