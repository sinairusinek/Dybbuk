#!/usr/bin/env python3
"""Phase A1 organization name-type classification.

Classifies organization rows as:
- proper_name
- descriptive_term
- ambiguous

Inputs:
- organizations2026-03-08.tsv
- Organisations-Report-20260205-2154-xlsx (1).tsv

Output:
- organizations_classified.tsv (same rows + name_type + confidence)
"""

from __future__ import annotations

import argparse
import csv
import difflib
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

# Stable source column names used across this phase and downstream phases.
COL_ORG_TYPE = "_ - organizations - _ - org_type"
COL_TITLE = "_ - organizations - _ - title"
COL_CLUSTERED = "clustered organization"
COL_DESC = "_ - organizations - _ - descriptive_name"
COL_SETTLEMENT = "_ - organizations - _ - locations - _ - settlement"
COL_VENUE = "_ - organizations - _ - locations - _ - Venue"

COL_NAME_TYPE = "name_type"
COL_CONFIDENCE = "confidence"

NAME_TYPE_PROPER = "proper_name"
NAME_TYPE_DESCRIPTIVE = "descriptive_term"
NAME_TYPE_AMBIGUOUS = "ambiguous"

CONF_HIGH = "high"
CONF_MEDIUM = "medium"
CONF_LOW = "low"

POSSESSIVE_WORDS = {
    "מיין",
    "מײַן",
    "זיין",
    "זײַן",
    "איר",
    "אונדזער",
    "אונדזערע",
    "זייער",
}

PLURAL_FORMS = {
    "טרופּעס",
    "טרופעס",
    "טעאַטערס",
    "טעאטערס",
    "שולן",
    "קרייזן",
    "געזעלשאַפֿטן",
}

GENERIC_TERMS = {
    "בינע",
    "דראַמקרייז",
    "דראמקרייז",
    "וואַנדערטרופּע",
    "וואנדערטרופע",
    "קרייז",
    "טרופּע",
    "טרופע",
    "טעאַטער",
    "טעאטער",
    "שול",
    "קאָנסערוואַטאָריע",
    "קאָנסערוואטאריע",
    "קונסטקרייז",
    "פֿאַרלאַג",
    "פארלאג",
    "צייטונג",
    "געזעלשאַפֿט",
    "געזעלשאפט",
    "פֿאַראיין",
    "פאראיין",
    "ארגאניזאציע",
    "אָרגאַניזאַציע",
    "organization",
    "theatre",
    "theater",
    "troupe",
    "group",
    "school",
    "publisher",
    "newspaper",
    "union",
}

STOPWORDS = {
    "דער",
    "די",
    "דאָס",
    "דאס",
    "פון",
    "אין",
    "אויף",
    "ביי",
    "און",
    "צו",
    "מיט",
    "אַן",
    "אן",
    "a",
    "an",
    "the",
    "of",
    "in",
    "for",
}

INSTITUTION_TERMS = {
    "טעאַטער",
    "טעאטער",
    "טרופּע",
    "טרופע",
    "קאָנסערוואַטאָריע",
    "קאָנסערוואטאריע",
    "שול",
    "בינע",
    "קרייז",
    "געזעלשאַפֿט",
    "געזעלשאפט",
    "פֿאַראיין",
    "פאראיין",
    "theatre",
    "theater",
    "troupe",
    "academy",
    "conservatory",
    "school",
    "union",
    "society",
}

NON_THEATRE_TYPES = {"factory", "workplace"}

LATIN_PERSON_NAME_RE = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b")
YIDDISH_SURNAME_SUFFIXES = (
    "מאַן",
    "מאן",
    "בערג",
    "שטיין",
    "בוים",
    "פעלד",
    "וויטש",
    "סקי",
    "זאָן",
    "זון",
)

TOKEN_RE = re.compile(r"[^\w\s׳']+", flags=re.UNICODE)


def configure_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit = limit // 10


def clean_text(value: str) -> str:
    if value is None:
        return ""
    text = value.strip().lower()
    text = TOKEN_RE.sub(" ", text)
    text = " ".join(text.split())
    return text


def tokens(value: str) -> List[str]:
    text = clean_text(value)
    return text.split() if text else []


def normalized_for_match(value: str) -> str:
    text = clean_text(value)
    text = text.replace("׳", "").replace("'", "")
    return text


def has_possessive_or_plural(text_parts: Iterable[str]) -> bool:
    all_tokens: List[str] = []
    for part in text_parts:
        all_tokens.extend(tokens(part))
    if not all_tokens:
        return False
    return any(tok in POSSESSIVE_WORDS or tok in PLURAL_FORMS for tok in all_tokens)


def is_generic_only(text_parts: Iterable[str]) -> bool:
    all_tokens: List[str] = []
    for part in text_parts:
        all_tokens.extend(tokens(part))

    filtered = [tok for tok in all_tokens if tok not in STOPWORDS]
    if not filtered:
        return False

    return all(tok in GENERIC_TERMS for tok in filtered)


def has_person_name_pattern(title: str) -> bool:
    if not title:
        return False

    if LATIN_PERSON_NAME_RE.search(title):
        return True

    yid_tokens = tokens(title)
    if len(yid_tokens) < 2:
        return False

    return any(tok.endswith(YIDDISH_SURNAME_SUFFIXES) for tok in yid_tokens)


def has_geographic_institution_signal(row: Dict[str, str]) -> bool:
    settlement = (row.get(COL_SETTLEMENT) or "").strip()
    if not settlement:
        return False

    candidate = " ".join(
        [
            row.get(COL_TITLE, "") or "",
            row.get(COL_CLUSTERED, "") or "",
            row.get(COL_DESC, "") or "",
        ]
    )
    c_tokens = set(tokens(candidate))
    return any(term in c_tokens for term in INSTITUTION_TERMS)


def has_specific_venue(row: Dict[str, str]) -> bool:
    return bool((row.get(COL_VENUE) or "").strip())


def build_db_name_index(rows: Iterable[Dict[str, str]]) -> Tuple[set, List[str]]:
    exact = set()
    for row in rows:
        name = normalized_for_match(row.get("Name", "") or "")
        if name:
            exact.add(name)
    return exact, sorted(exact)


def db_name_match(candidates: Iterable[str], db_exact: set, db_list: List[str]) -> bool:
    for cand in candidates:
        norm = normalized_for_match(cand)
        if not norm:
            continue
        if norm in db_exact:
            return True
        matches = difflib.get_close_matches(norm, db_list, n=1, cutoff=0.93)
        if matches:
            return True
    return False


def classify_row(row: Dict[str, str], db_exact: set, db_list: List[str]) -> Tuple[str, str]:
    title = (row.get(COL_TITLE) or "").strip()
    desc = (row.get(COL_DESC) or "").strip()
    clustered = (row.get(COL_CLUSTERED) or "").strip()
    org_type = (row.get(COL_ORG_TYPE) or "").strip().lower()

    # Rule 1: direct/near DB match is strongest proper-name signal.
    if db_name_match([clustered, title], db_exact, db_list):
        return NAME_TYPE_PROPER, CONF_HIGH

    proper_score = 0
    descriptive_score = 0

    if not title and desc:
        descriptive_score += 2

    if has_possessive_or_plural([title, desc]):
        descriptive_score += 3

    if is_generic_only([title, desc]):
        descriptive_score += 2

    if org_type in NON_THEATRE_TYPES and is_generic_only([title, desc, org_type]):
        descriptive_score += 1

    if clustered and normalized_for_match(clustered) != normalized_for_match(desc):
        proper_score += 2

    if has_person_name_pattern(title):
        proper_score += 2

    if has_geographic_institution_signal(row):
        proper_score += 2

    if has_specific_venue(row):
        proper_score += 1

    # Resolve outcomes with conflict-aware logic.
    if proper_score >= 3 and descriptive_score == 0:
        return NAME_TYPE_PROPER, CONF_MEDIUM if proper_score < 4 else CONF_HIGH

    if descriptive_score >= 3 and proper_score == 0:
        return NAME_TYPE_DESCRIPTIVE, CONF_MEDIUM if descriptive_score < 4 else CONF_HIGH

    if descriptive_score >= 2 and proper_score <= 1:
        return NAME_TYPE_DESCRIPTIVE, CONF_MEDIUM

    if proper_score >= 2 and descriptive_score <= 1:
        return NAME_TYPE_PROPER, CONF_MEDIUM

    if proper_score >= 2 and descriptive_score >= 2:
        return NAME_TYPE_AMBIGUOUS, CONF_LOW

    return NAME_TYPE_AMBIGUOUS, CONF_LOW


def read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def write_tsv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: List[Dict[str, str]]) -> None:
    type_counts = Counter(row[COL_NAME_TYPE] for row in rows)
    confidence_counts = Counter(row[COL_CONFIDENCE] for row in rows)

    print("Summary: count per name_type")
    for key in [NAME_TYPE_PROPER, NAME_TYPE_DESCRIPTIVE, NAME_TYPE_AMBIGUOUS]:
        print(f"  {key}\t{type_counts.get(key, 0)}")

    print("\nSummary: count per confidence")
    for key in [CONF_HIGH, CONF_MEDIUM, CONF_LOW]:
        print(f"  {key}\t{confidence_counts.get(key, 0)}")

    cross = defaultdict(Counter)
    for row in rows:
        cross[row[COL_NAME_TYPE]][row[COL_CONFIDENCE]] += 1

    print("\nSummary: name_type x confidence")
    print("  name_type\thigh\tmedium\tlow")
    for ntype in [NAME_TYPE_PROPER, NAME_TYPE_DESCRIPTIVE, NAME_TYPE_AMBIGUOUS]:
        print(
            f"  {ntype}\t"
            f"{cross[ntype].get(CONF_HIGH, 0)}\t"
            f"{cross[ntype].get(CONF_MEDIUM, 0)}\t"
            f"{cross[ntype].get(CONF_LOW, 0)}"
        )


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]

    default_input = repo_root / "organizations2026-03-08.tsv"
    default_db = repo_root / "Organisations-Report-20260205-2154-xlsx (1).tsv"
    default_output = script_dir / "organizations_classified.tsv"

    parser = argparse.ArgumentParser(
        description="Classify organization rows as proper_name, descriptive_term, or ambiguous."
    )
    parser.add_argument("--input", type=Path, default=default_input, help="Path to extraction TSV.")
    parser.add_argument("--db", type=Path, default=default_db, help="Path to existing DB TSV.")
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help="Path for classified output TSV.",
    )
    return parser.parse_args()


def main() -> None:
    configure_csv_field_limit()
    args = parse_args()

    source_rows = read_tsv(args.input)
    db_rows = read_tsv(args.db)
    db_exact, db_list = build_db_name_index(db_rows)

    if not source_rows:
        raise ValueError("Input TSV has no data rows.")

    output_rows: List[Dict[str, str]] = []
    for row in source_rows:
        name_type, confidence = classify_row(row, db_exact, db_list)
        new_row = dict(row)
        new_row[COL_NAME_TYPE] = name_type
        new_row[COL_CONFIDENCE] = confidence
        output_rows.append(new_row)

    fieldnames = list(source_rows[0].keys())
    if COL_NAME_TYPE not in fieldnames:
        fieldnames.append(COL_NAME_TYPE)
    if COL_CONFIDENCE not in fieldnames:
        fieldnames.append(COL_CONFIDENCE)

    write_tsv(args.output, output_rows, fieldnames)
    print(f"Wrote {len(output_rows)} rows to: {args.output}")
    print_summary(output_rows)


if __name__ == "__main__":
    main()
