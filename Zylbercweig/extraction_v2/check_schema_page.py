"""Check docs/Visualizations/extraction_schema.html against the real outputs.

The other two schema surfaces in this project are GENERATED from a build
artifact, so they cannot drift:

  * docs/Visualizations/plays_kg_schema.html   <- Zylbercweig/plays/build_kg_schema_page.py
  * the Lateiner/Hurwitz Schema tab            <- YiDraCor/code/build_schema_tab.py

The extraction v2 page cannot be: it is a curated narrative of a multi-stage
pipeline — costs, decisions, what each run showed — and no single file holds
that. The numbers in it are therefore written by hand, and the failure mode is
silent: a re-run changes a count, the page keeps the old one, and nothing says
so. That is what this script is for.

It does NOT rewrite the page. It compares the counts the page states against
the files that produced them and prints each disagreement, leaving the decision
to a human: a differing number may mean the page is stale, or it may mean the
page is deliberately quoting the figure the run reported at the time.

    python3.11 check_schema_page.py            # from Zylbercweig/extraction_v2/
    python3.11 check_schema_page.py --strict   # exit 1 on any drift (for CI)

Add a CHECK entry whenever a step's headline number comes from a file on disk.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent                   # extraction_v2/
REPO = HERE.parent.parent                                # Dybbuk/
PAGE = REPO / "docs" / "Visualizations" / "extraction_schema.html"
DRAFT = HERE / "SCHEMA_DRAFT.md"

BEGIN = "<!-- SCHEMA-DATA:BEGIN"
END = "<!-- SCHEMA-DATA:END -->"


def _tsv_rows(path: Path) -> int | None:
    """Data rows (header excluded), or None when the file is absent."""
    if not path.exists():
        return None
    with path.open(encoding="utf-8", newline="") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def _jsonl_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as fh:
        return sum(1 for ln in fh if ln.strip())


# Each check names a step in the page's SEQUENCE block, the phrase the page
# writes the number against inside that step's `counts:` string, and the file
# the number should come from. The claim is PARSED OUT OF THE PAGE rather than
# repeated here: a second hand-kept copy of the figure would simply be a second
# thing that drifts, and the checker would then report stale numbers of its own.
#
# Add an entry whenever a step's headline number comes from a file on disk.
CHECKS: list[tuple[str, str, str, callable]] = [
    # (step, phrase in `counts:`,     file,                   counter)
    ("8d", "rows",                    "vocab_alignment.tsv",  _tsv_rows),
    ("8",  "parts",                   "defg_full.jsonl",      _jsonl_rows),
    ("8",  "stations",                "defg/stations.tsv",    _tsv_rows),
    ("8",  "events",                  "defg/events.tsv",      _tsv_rows),
    ("8",  "performance facts",       "defg/performance_facts.tsv", _tsv_rows),
    ("8",  "source uses",             "defg/source_uses.tsv", _tsv_rows),
    ("8",  "schema issues",           "defg/schema_issues.tsv", _tsv_rows),
]

# The entity and relation cards state what the D–G pass yielded. Those figures
# come straight off the flattened tables, so they are checked the same way:
# (label, phrase the card writes the number against, file, counter).
# `None` as the phrase means "the file's row count", matched anywhere in the
# hand-kept block rather than inside one step's `counts:` string.
# (`mentions.tsv`'s 137,035 total had a check here until the summary list that
# stated it was removed; a check for a figure the page does not state can only
# report a false absence, so it went with it. Restore both together.)
CARD_CHECKS: list[tuple[str, str, str, callable]] = [
    ("stations",            "stations",           "defg/stations.tsv",      _tsv_rows),
    ("events",              "events:",            "defg/events.tsv",        _tsv_rows),
    ("performance facts",   "performance facts",  "defg/performance_facts.tsv", _tsv_rows),
    ("source uses",         "source uses",        "defg/source_uses.tsv",   _tsv_rows),
    ("work relations",      "work→work relations", "defg/work_relations.tsv", _tsv_rows),
    ("event participants",  "event-participant rows", "defg/event_participants.tsv", _tsv_rows),
    ("event orgs",          "event→org rows",     "defg/event_orgs.tsv",    _tsv_rows),
]


def _page_block() -> str:
    """The hand-kept block, or the whole page when the markers are gone."""
    text = PAGE.read_text(encoding="utf-8")
    if BEGIN in text and END in text:
        return text[text.index(BEGIN):text.index(END)]
    return text


def _steps() -> dict[str, str]:
    """step id -> its `counts:` string, as the page writes it."""
    out = {}
    for m in re.finditer(r'step:"([^"]+)".*?counts:"((?:[^"\\]|\\.)*)"',
                         _page_block(), re.S):
        out.setdefault(m.group(1), m.group(2))
    return out


def _claims(text: str, phrase: str) -> list[int]:
    """EVERY number written against `phrase`, in document order.

    A figure is often stated more than once — the D–G row counts appear both
    in step 8's summary and on the entity card. Returning only the first match
    would let a stale copy hide behind a correct one, so each occurrence is
    checked and any that disagrees is reported.
    """
    pat = r"([\d,]+)\s*(?:</?[a-z][^>]*>\s*){0,4}" + re.escape(phrase)
    return [int(m.group(1).replace(",", "")) for m in re.finditer(pat, text)]


def _claim(counts: str, phrase: str) -> int | None:
    """The number the page writes immediately before `phrase`.

    Markup may sit between the two — the summary list puts the figure in a
    <code> and its label in the following <span> — so a short run of tags and
    whitespace is allowed between the number and the phrase, but no other
    text, which would mean the number belongs to something else.
    """
    m = re.search(r"([\d,]+)\s*(?:</?[a-z][^>]*>\s*){0,4}" + re.escape(phrase),
                  counts)
    return int(m.group(1).replace(",", "")) if m else None


def check_markers() -> list[str]:
    problems = []
    if not PAGE.exists():
        return [f"page missing: {PAGE}"]
    text = PAGE.read_text(encoding="utf-8")
    if BEGIN not in text or END not in text:
        problems.append(
            "the page has no SCHEMA-DATA markers — the hand-kept block is no "
            "longer delimited, so nothing warns an editor that it drifts.")
    return problems


def check_counts() -> tuple[list[tuple], list[str]]:
    """Compare each parsed claim with the file that should produce it."""
    drift, notes = [], []
    steps = _steps()
    for step, phrase, rel, counter in CHECKS:
        counts = steps.get(step)
        if counts is None:
            notes.append(f"step {step}: not on the page")
            continue
        claimed = _claim(counts, phrase)
        if claimed is None:
            notes.append(f"step {step}: no number stated for {phrase!r} "
                         f"(the page's wording changed — update the phrase "
                         f"in CHECKS)")
            continue
        actual = counter(HERE / rel)
        if actual is None:
            notes.append(f"step {step} {phrase}: {rel} not on disk")
        elif actual != claimed:
            drift.append((f"{step}: {phrase}", rel, claimed, actual))
    # The entity/relation cards and the summary list state these figures in
    # running prose. The summary list sits OUTSIDE the hand-kept markers (it is
    # rendered from the same data lower down), so these are matched against the
    # whole page rather than the marked block: a stale figure is worth catching
    # wherever on the page it is written.
    block = PAGE.read_text(encoding="utf-8")
    for label, phrase, rel, counter in CARD_CHECKS:
        claimed = _claims(block, phrase)
        if not claimed:
            notes.append(f"card {label!r}: no number stated for {phrase!r} "
                         f"(wording changed — update CARD_CHECKS)")
            continue
        actual = counter(HERE / rel)
        if actual is None:
            notes.append(f"card {label}: {rel} not on disk")
            continue
        # every place the figure is written must agree with the file
        for i, c in enumerate(dict.fromkeys(claimed)):
            if c != actual:
                where = (f"card: {label}" if len(set(claimed)) == 1
                         else f"card: {label} (copy {i + 1} of "
                              f"{len(set(claimed))})")
                drift.append((where, rel, c, actual))
    return drift, notes


def check_draft_mirror() -> list[str]:
    """The page mirrors SCHEMA_DRAFT.md §0; flag when the draft moved on."""
    if not DRAFT.exists() or not PAGE.exists():
        return []
    steps_draft = set(re.findall(r"^\|\s*(\d+[a-z+]*)\s*\|",
                                 DRAFT.read_text(encoding="utf-8"), re.M))
    page = PAGE.read_text(encoding="utf-8")
    block = page[page.index(BEGIN):page.index(END)] if BEGIN in page else page
    steps_page = set(re.findall(r'step:"([^"]+)"', block))
    only_draft = sorted(steps_draft - steps_page)
    return ([f"steps in SCHEMA_DRAFT.md but not on the page: "
             f"{', '.join(only_draft)}"] if only_draft else [])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--strict", action="store_true",
                    help="exit 1 when anything drifted (for CI)")
    args = ap.parse_args()

    marker_problems = check_markers()
    drift, missing = check_counts()
    mirror = check_draft_mirror()

    for p in marker_problems:
        print(f"MARKERS  {p}")

    if drift:
        print(f"\nDRIFT — the page states a number the file no longer gives "
              f"({len(drift)} of {len(CHECKS)} checks):\n")
        w = max(len(d[0]) for d in drift)
        print(f"  {'what':<{w}}  {'page says':>10}  {'file has':>10}  diff")
        for label, rel, claimed, actual in drift:
            print(f"  {label:<{w}}  {claimed:>10,}  {actual:>10,}  "
                  f"{actual - claimed:+,}   ({rel})")
        print("\n  A difference is not automatically an error: the page may be "
              "\n  quoting the figure a run reported at the time. Decide per "
              "row,\n  and update the page and SCHEMA_DRAFT.md §0 together.")
    else:
        print(f"counts: all {len(CHECKS) + len(CARD_CHECKS)} checks agree "
              f"with the files ({len(CHECKS)} step figures, "
              f"{len(CARD_CHECKS)} entity-card figures)")

    for m in missing:
        print(f"ABSENT   {m}")
    for m in mirror:
        print(f"MIRROR   {m}")

    bad = bool(drift or marker_problems)
    if args.strict and bad:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
