"""Propose cast_dict.json additions from `untagged speaker (unknown)` flags.

Reads a `data/review/needs_human_*.csv` (output of `auto_resolve_flags`) and
turns each unresolved `<label>:` speaker label into a proposal:

  - if `<label>` is within `--max-edits` (default 2) of an existing cast
    `bare` (nikud-stripped, Levenshtein) → propose adding `<label>` to that
    role's `prefix_variants`. Surfaces OCR/spelling drift (ביסיגג→bising,
    עמיליא→emilye, etc.).
  - else if `<label>` recurs ≥ `--min-occurrences` times (default 2) →
    propose a new body-only role with `{"declared": "body-only"}`.
  - else → list under `low_confidence` for manual review.

Output (one JSON per play under `data/review/cast_variant_proposals_<date>/`):
  {
    "play": "...",
    "extend_variants":   [{"xmlid": "...", "add": ["..."], "occurrences": N}],
    "new_body_only":     [{"xmlid_suggest": "...", "bare": "...", "occurrences": N}],
    "low_confidence":    [{"label": "...", "occurrences": N, "nearest": {...}}]
  }

The script is **read-only** w.r.t. cast_dict.json — it produces proposals only.
A human reviews the JSON and merges in the entries they accept. Intended to
run after every `auto_resolve_flags --play X` invocation on a newly
annotated play, BEFORE handing the residual flags off for manual review.

Usage:
  python -m annotation.suggest_cast_variants \
      --flags data/review/needs_human_2026-05-31.csv \
      --out   data/review/cast_variant_proposals_2026-05-31
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.lint_pages import REPO, load_editions

NIKUD = re.compile(r"[֑-ׇ]")
UNK_RE = re.compile(r"untagged speaker \(unknown\) '([^']+)'")


def strip_nikud(s: str) -> str:
    return NIKUD.sub("", s or "").strip()


def edit_distance(a: str, b: str, cap: int) -> int:
    """Bounded Levenshtein — returns cap+1 once distance is known to exceed cap."""
    if abs(len(a) - len(b)) > cap:
        return cap + 1
    if a == b:
        return 0
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i] + [0] * len(b)
        row_min = cur[0]
        for j, cb in enumerate(b, 1):
            cur[j] = min(
                prev[j] + 1,
                cur[j - 1] + 1,
                prev[j - 1] + (ca != cb),
            )
            row_min = min(row_min, cur[j])
        if row_min > cap:
            return cap + 1
        prev = cur
    return prev[-1]


def sanitize_xmlid(s: str) -> str:
    """Lowercase ASCII fallback — only useful as a SUGGESTION; human picks final id."""
    YIDDISH_TO_LATIN = {
        "א": "a", "ב": "b", "ג": "g", "ד": "d", "ה": "h", "ו": "v",
        "ז": "z", "ח": "kh", "ט": "t", "י": "y", "כ": "k", "ך": "k",
        "ל": "l", "מ": "m", "ם": "m", "נ": "n", "ן": "n", "ס": "s",
        "ע": "e", "פ": "p", "ף": "p", "צ": "ts", "ץ": "ts",
        "ק": "k", "ר": "r", "ש": "sh", "ת": "t",
    }
    out = []
    for ch in strip_nikud(s):
        out.append(YIDDISH_TO_LATIN.get(ch, "_"))
    raw = "".join(out)
    return re.sub(r"_+", "_", raw).strip("_") or "unknown"


def load_play_cast(play: str) -> dict:
    """Return {xmlid: {bare, form, prefix_variants}} for `play`."""
    f = REPO / "data" / play / "cast_dict.json"
    if not f.exists():
        return {}
    d = json.loads(f.read_text(encoding="utf-8"))
    out = {}
    for xmlid, info in d.get("roles", {}).items():
        out[xmlid] = {
            "bare": strip_nikud(info.get("bare") or info.get("form", "")),
            "form": info.get("form", ""),
            "prefix_variants": info.get("prefix_variants") or [],
        }
    return out


def nearest_role(label: str, cast: dict, cap: int):
    """Return (xmlid, distance) for the closest cast bare within cap, else (None, cap+1)."""
    best_xmlid, best_d = None, cap + 1
    for xmlid, info in cast.items():
        for surface in [info["bare"]] + [strip_nikud(v) for v in info["prefix_variants"]]:
            if not surface:
                continue
            d = edit_distance(label, surface, cap)
            if d < best_d:
                best_d = d; best_xmlid = xmlid
                if d == 0:
                    return best_xmlid, 0
    return best_xmlid, best_d


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flags", required=True,
                    help="needs_human_<date>.csv from auto_resolve_flags")
    ap.add_argument("--out", default=None,
                    help="output directory (default: data/review/cast_variant_proposals_<date>/)")
    ap.add_argument("--max-edits", type=int, default=2,
                    help="max Levenshtein distance to count as 'variant of existing role' (default 2)")
    ap.add_argument("--min-occurrences", type=int, default=2,
                    help="minimum recurrence count for 'new body-only role' (default 2)")
    args = ap.parse_args()

    flags_path = Path(args.flags) if Path(args.flags).is_absolute() else REPO / args.flags
    out_dir = (Path(args.out) if args.out and Path(args.out).is_absolute()
               else REPO / (args.out or f"data/review/cast_variant_proposals_{_dt.date.today()}"))
    out_dir.mkdir(parents=True, exist_ok=True)

    editions = load_editions()  # {folder: label}
    label_to_folder = {v: k for k, v in editions.items()}

    # gather unknown-speaker labels per play
    per_play: dict[str, Counter] = defaultdict(Counter)
    for r in csv.DictReader(open(flags_path, encoding="utf-8")):
        if r.get("category") != "untagged speaker":
            continue
        m = UNK_RE.match(r.get("issue/detail", ""))
        if not m:
            continue
        label = strip_nikud(m.group(1))
        if not label:
            continue
        folder = label_to_folder.get(r["edition"])
        if not folder:
            continue
        per_play[folder][label] += 1

    n_extend = n_new = n_low = 0
    for play, label_counts in sorted(per_play.items()):
        cast = load_play_cast(play)
        extend: dict[str, dict] = {}   # xmlid -> {add: [...], occurrences: int}
        new_body: list[dict] = []
        low: list[dict] = []
        for label, occ in label_counts.most_common():
            xmlid, dist = nearest_role(label, cast, args.max_edits)
            if xmlid and dist <= args.max_edits:
                e = extend.setdefault(xmlid, {"xmlid": xmlid, "add": [], "occurrences": 0,
                                              "nearest_distance": dist})
                if label not in e["add"]:
                    e["add"].append(label)
                e["occurrences"] += occ
                e["nearest_distance"] = min(e["nearest_distance"], dist)
            elif occ >= args.min_occurrences:
                new_body.append({
                    "xmlid_suggest": sanitize_xmlid(label),
                    "bare": label,
                    "occurrences": occ,
                })
            else:
                low.append({"label": label, "occurrences": occ})

        n_extend += sum(len(e["add"]) for e in extend.values())
        n_new += len(new_body)
        n_low += len(low)

        out_file = out_dir / f"{play}.json"
        out_file.write_text(
            json.dumps({
                "play": play,
                "extend_variants": list(extend.values()),
                "new_body_only": new_body,
                "low_confidence": low,
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    print(f"Wrote proposals for {len(per_play)} plays → {out_dir.relative_to(REPO)}")
    print(f"  prefix_variants additions:  {n_extend}")
    print(f"  new body-only roles:        {n_new}")
    print(f"  low-confidence (manual):    {n_low}")


if __name__ == "__main__":
    raise SystemExit(main())
