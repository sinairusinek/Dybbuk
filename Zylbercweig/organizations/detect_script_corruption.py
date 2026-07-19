"""Detect script-level corruption in extracted text — the class of defect found
on 2026-07-19 in volume_2IIIorg.json entry 459 (xml:id facs_292_tr_1744280734).

There, the extractor emitted Armenian-block homoglyphs in place of Yiddish for
one entry's org fields — `ձրւքեձ ֒ւհ ՠթքօւրձճթ` for `טרופּעס פֿון ליפּאָווסקי`.
The entry's own `entry` text was clean, so nothing upstream was wrong; the
corruption entered at extraction and rode all the way through clustering into
`org_alignment_review.tsv`, where a reviewer made three alignment decisions
against unreadable strings.

Two checks, both aimed at that failure mode:

Class A — unexpected script
  Any letter outside the corpus's attested repertoire (Hebrew, Latin, Cyrillic).
  Armenian, Greek, Georgian, CJK etc. are never legitimate here.

Class B — intra-token script mixing
  A single whitespace-delimited token containing Hebrew letters alongside
  letters of a script other than Latin/Cyrillic. This is the tell that Class A
  alone can miss on partially-corrupted strings: `ראאճօմ` (for `ראַדאָם`) opens
  with two real Hebrew letters before degrading into Armenian, so a per-field
  script census that only asks "is there Hebrew here?" would pass it.

Output: one reviewable TSV in this directory. Nothing is auto-mutated.
Exits 1 when findings exist, so it can gate a pipeline run.
"""
from __future__ import annotations

import csv
import json
import sys
import unicodedata
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent
EXTRACTION = HERE.parent / "Zylbercweig_extraction"
OUT = HERE / "script_corruption_punchlist.tsv"

# Letter scripts attested in this corpus: Yiddish body text, Latin bibliography,
# Russian/Ukrainian press citations. Greek is deliberately NOT allowed — the only
# Greek in the corpus is homoglyph corruption (`מאַנדελשטאָם` for `מאַנדעלשטאָם`).
ALLOWED_SCRIPTS = {"HEBREW", "LATIN", "CYRILLIC"}
# Scripts that may share a token with Hebrew without it being suspicious.
MIXABLE_WITH_HEBREW = {"LATIN", "CYRILLIC"}
# Unicode classes these as letters, but they function as punctuation and show up
# as ordinary OCR artifacts (º in "5 אַפּריל 1872", ʼ as an apostrophe variant).
# Counting them as letters produces noise, not findings.
IGNORE_CHARS = {"ª", "º", "ʼ", "ʻ", "ʹ", "ʺ", "ˮ"}

# Live pipeline TSVs. Backups (.pre_*, .2026-*) are snapshots of what the
# pipeline actually held and are deliberately not scanned.
LIVE_TSVS = [
    "organizations_clustered.tsv",
    "organizations_classified.tsv",
    "org_alignment_drafts.tsv",
    "org_alignment_review.tsv",
    "org_alignment_review_canonical_mapping.tsv",
    "organizations_clustered_canonical_mapping.tsv",
    "ra_vs_auto_comparison.tsv",
    "core_db.tsv",
    "skipped_no_name.tsv",
    "unresolved_settlements_punchlist.tsv",
    "settlement_unresolved_punchlist.tsv",
]


def script_of(ch: str) -> str | None:
    """Script name for a letter, or None for non-letters (digits, punctuation,
    marks). Nikud and other combining marks carry the Hebrew name in Unicode but
    are category Mn — treated as non-letters so they never drive a verdict."""
    if ch in IGNORE_CHARS:
        return None
    if unicodedata.category(ch) not in ("Lu", "Ll", "Lo", "Lt", "Lm"):
        return None
    try:
        return unicodedata.name(ch).split()[0]
    except ValueError:
        return None


def scan_value(text: str):
    """Yield (defect_class, detail) for one string."""
    if not text:
        return
    bad = {}
    for ch in text:
        sc = script_of(ch)
        if sc and sc not in ALLOWED_SCRIPTS:
            bad.setdefault(sc, set()).add(ch)
    if bad:
        detail = "; ".join(f"{sc}: {''.join(sorted(chs))}" for sc, chs in sorted(bad.items()))
        yield "A_unexpected_script", detail

    for token in text.split():
        scripts = {s for s in (script_of(c) for c in token) if s}
        if "HEBREW" in scripts:
            foreign = scripts - {"HEBREW"} - MIXABLE_WITH_HEBREW
            if foreign:
                yield "B_intra_token_mixing", f"{token} [{'+'.join(sorted(scripts))}]"


def walk_json(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from walk_json(v, f"{path}/{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk_json(v, f"{path}/{i}")
    elif isinstance(obj, str):
        yield path, obj


def main() -> int:
    findings = []

    for src in sorted(EXTRACTION.glob("*.json")):
        try:
            data = json.loads(src.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"skip {src.name}: {exc}", file=sys.stderr)
            continue
        for path, value in walk_json(data):
            for cls, detail in scan_value(value):
                # /<entry_index>/... — surface the entry so it can be re-extracted
                entry_idx = path.split("/")[1] if path.count("/") >= 1 else ""
                xml_id = ""
                if entry_idx.isdigit() and isinstance(data, list):
                    xml_id = data[int(entry_idx)].get("xml:id", "")
                findings.append({
                    "source": src.name, "locator": path, "entry_or_row": entry_idx,
                    "record_id": xml_id, "defect_class": cls, "detail": detail,
                    "value": value[:300],
                })

    for name in LIVE_TSVS:
        path = HERE / name
        if not path.exists():
            continue
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            id_field = next((c for c in (reader.fieldnames or [])
                             if c in ("cluster_id", "db_id", "_ - xml:id")), None)
            for row_no, row in enumerate(reader, start=2):
                for col, value in row.items():
                    if not isinstance(value, str):
                        continue
                    for cls, detail in scan_value(value):
                        findings.append({
                            "source": name, "locator": col, "entry_or_row": row_no,
                            "record_id": (row.get(id_field) or "") if id_field else "",
                            "defect_class": cls, "detail": detail, "value": value[:300],
                        })

    cols = ["source", "locator", "entry_or_row", "record_id", "defect_class", "detail", "value"]
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(findings)

    if not findings:
        print("no script corruption found")
        return 0

    by_source: dict[str, int] = {}
    for finding in findings:
        by_source[finding["source"]] = by_source.get(finding["source"], 0) + 1
    print(f"{len(findings)} finding(s) -> {OUT.name}")
    for src, n in sorted(by_source.items(), key=lambda kv: -kv[1]):
        print(f"  {n:5d}  {src}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
