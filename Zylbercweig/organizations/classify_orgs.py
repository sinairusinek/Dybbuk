#!/usr/bin/env python3
"""Phase A1 organization name-type classification.

Classifies organization rows as:
- proper_name
- descriptive_term
- ambiguous

Also adds a review_flag column:
- person_ref  — the org is known only by a person's name (e.g. "ביי ליפּאָווסקי",
  bare surname "גליקמאַן"). Classified as proper_name (low confidence) — the
  person name is the provisional org name until a formal name is found.

Rows where title + clustered + descriptive_name are all empty are excluded
from the output entirely (written to skipped_no_name.tsv if they contain any
other content, otherwise silently dropped).

Inputs:
- Zylbercweig_extraction/organizations2026-03-08.tsv
- Zylbercweig_extraction/Organisations-Report-20260205-2154-xlsx (1).tsv

Output:
- organizations_classified.tsv  (classified rows + name_type + confidence + review_flag)
- skipped_no_name.tsv           (rows with no name fields but some other content)
"""

from __future__ import annotations

import argparse
import csv
import difflib
import re
import sys
import unicodedata
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
COL_REVIEW_FLAG = "review_flag"

NAME_TYPE_PROPER = "proper_name"
NAME_TYPE_DESCRIPTIVE = "descriptive_term"
NAME_TYPE_AMBIGUOUS = "ambiguous"
NAME_TYPE_NOT_ORG = "not_an_organization"

REVIEW_FLAG_PERSON_REF = "person_ref"

# Prepositions that introduce a person reference rather than an org name.
PERSON_REF_PREFIXES = {"ביי", "מיט", "אויף", "לויט", "פֿון", "פון"}

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
    "ישיבה",
    "אוניווערזיטעט",
    "הויכשול",
    "הויכשולע",
    "גימנאזיע",
    "אַקאַדעמיע",
    "אקדמיע",
    "ליציי",
    "theatre",
    "theater",
    "troupe",
    "academy",
    "conservatory",
    "school",
    "university",
    "yeshiva",
    "union",
    "society",
    "journal",
    "newspaper",
    "tribune",
    "press",
}

# org_type values that indicate a named publication — person worked there = org.
PUBLICATION_ORG_TYPES = {"newspaper", "publisher", "journal", "radio", "magazine"}

# Prepositions indicating institution+location ("university OF heidelberg"),
# not a person reference.
PLACE_PREPOSITIONS = {"פון", "פֿון", "אין", "פאַר", "of", "in", "from"}

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
    text = unicodedata.normalize("NFC", value).strip().lower()
    text = TOKEN_RE.sub(" ", text)
    text = " ".join(text.split())
    return text


# NFC-normalized versions of the term sets, built once at import time.
# Yiddish text in the wild uses mixed Unicode normalization forms for vowel
# diacritics (patah, shva …), so comparisons must normalize both sides.
_INSTITUTION_NFC = {unicodedata.normalize("NFC", t).lower() for t in INSTITUTION_TERMS}
_GENERIC_NFC = {unicodedata.normalize("NFC", t).lower() for t in GENERIC_TERMS}


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


# Location and relation field prefixes used for empty-row detection.
_LOCATION_PREFIX = "_ - organizations - _ - locations"
_RELATION_PREFIX = "_ - organizations - _ - relations"
_FAMILY_PREFIX = "_ - family_background"


def is_empty_row(row: Dict[str, str]) -> bool:
    """Return True if the row has no meaningful organization content at all.

    Checks every field that could carry org identity: title, descriptive_name,
    org_type, clustered name, all location sub-fields, all relation sub-fields.
    Rows that are empty here are JSON artefacts — org slots with no data.
    """
    core_keys = [COL_TITLE, COL_DESC, COL_ORG_TYPE, COL_CLUSTERED, COL_SETTLEMENT, COL_VENUE]
    for key in core_keys:
        if not is_missing(row.get(key, "")):
            return False
    for key, value in row.items():
        if key.startswith(_LOCATION_PREFIX) or key.startswith(_RELATION_PREFIX):
            if not is_missing(value):
                return False
    return True


def is_missing(value: str) -> bool:
    """Return True for blank / null-sentinel values."""
    if value is None:
        return True
    v = value.strip().lower()
    return v in ("", "na", "n/a", "null", "none", "-", "--", "_")


def person_reference_flag(title: str, desc: str, org_type: str = "") -> str:
    """Return REVIEW_FLAG_PERSON_REF if the row is known only by a person's name.

    Two signals:
    1. Starts with a person-reference preposition (ביי/מיט …) followed by a
       person-name pattern — e.g. "ביי אַברהם אַקסעלראָד".
    2. Consists of 1–2 content tokens, no institution term, matches person-name
       heuristics — e.g. "גליקמאַן", "זיידל העלמאַן".

    Explicit non-person patterns that are excluded:
    - org_type is a publication type (newspaper, journal, etc.) — a named
      publication where someone worked is an org, not a person reference.
    - The string contains a place preposition (פון/אין/of/in) — signals an
      institution+location pattern like "אוניווערזיטעט פון היידעלבערג".
    - The last content token is an institution term — e.g. "ניקייטינסקי-טעאַטער".

    Correctly flagged rows are provisional proper names: the person's name
    serves as the org identifier until a formal name is found.
    """
    # Named publications — person worked there = org, not a person reference.
    if org_type.strip().lower() in PUBLICATION_ORG_TYPES:
        return ""

    candidate = (title or desc or "").strip()
    if not candidate:
        return ""

    toks = tokens(candidate)
    if not toks:
        return ""

    # Institution+location pattern: contains a place preposition somewhere
    # in the string — e.g. "אוניווערזיטעט פון היידעלבערג", "ישיבה אין לעמבערג".
    if any(t in PLACE_PREPOSITIONS for t in toks):
        return ""

    # Last token is an institution term — e.g. "ניקייטינסקי-טעאַטער".
    # toks are already NFC-normalized via clean_text.
    if toks[-1] in _INSTITUTION_NFC or toks[-1] in _GENERIC_NFC:
        return ""

    # Signal 1: leading person-reference preposition + person-name pattern.
    if toks[0] in PERSON_REF_PREFIXES and len(toks) >= 2:
        rest = " ".join(toks[1:])
        if has_person_name_pattern(rest):
            return REVIEW_FLAG_PERSON_REF

    # Signal 2: bare 1–2 content tokens, no institution term, person-name pattern.
    non_stop = [t for t in toks if t not in STOPWORDS]
    if len(non_stop) <= 2:
        has_institution = any(t in _INSTITUTION_NFC or t in _GENERIC_NFC for t in non_stop)
        if not has_institution and has_person_name_pattern(candidate):
            return REVIEW_FLAG_PERSON_REF

    return ""


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

    # Pre-check A: row is entirely empty — no org content to classify.
    if is_empty_row(row):
        return NAME_TYPE_NOT_ORG, CONF_HIGH

    # Rule 1: direct/near DB match is strongest proper-name signal —
    # but skip it if the matched name is itself a generic term, to prevent
    # "יידישן טעאַטער", "שטאָטישע שול" etc. from being pulled into proper_name.
    db_candidates = [c for c in [clustered, title] if c and not is_generic_only([c])]
    if db_candidates and db_name_match(db_candidates, db_exact, db_list):
        # Double-check: if the title alone is generic, downgrade to medium.
        if is_generic_only([title]):
            return NAME_TYPE_DESCRIPTIVE, CONF_MEDIUM
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
        name_type = NAME_TYPE_PROPER
        confidence = CONF_MEDIUM if proper_score < 4 else CONF_HIGH
    elif descriptive_score >= 3 and proper_score == 0:
        name_type = NAME_TYPE_DESCRIPTIVE
        confidence = CONF_MEDIUM if descriptive_score < 4 else CONF_HIGH
    elif descriptive_score >= 2 and proper_score <= 1:
        name_type = NAME_TYPE_DESCRIPTIVE
        confidence = CONF_MEDIUM
    elif proper_score >= 2 and descriptive_score <= 1:
        name_type = NAME_TYPE_PROPER
        confidence = CONF_MEDIUM
    elif proper_score >= 2 and descriptive_score >= 2:
        name_type = NAME_TYPE_AMBIGUOUS
        confidence = CONF_LOW
    else:
        name_type = NAME_TYPE_AMBIGUOUS
        confidence = CONF_LOW

    # Post-check: if we landed on proper_name but the title is generics-only,
    # downgrade. This catches "אוניווערזיטעט", "פּאָבליק סקול" etc. that
    # accumulated proper_score from non-title signals.
    if name_type == NAME_TYPE_PROPER and is_generic_only([title]):
        name_type = NAME_TYPE_DESCRIPTIVE
        confidence = CONF_MEDIUM

    return name_type, confidence


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

    flag_counts = Counter(row.get(COL_REVIEW_FLAG, "") for row in rows)
    if flag_counts.get(REVIEW_FLAG_PERSON_REF, 0):
        print(f"\nreview_flag={REVIEW_FLAG_PERSON_REF}: {flag_counts[REVIEW_FLAG_PERSON_REF]} rows")


def has_name_fields(row: Dict[str, str]) -> bool:
    """Return True if the row has at least one of: title, clustered name, descriptive_name."""
    return (
        not is_missing(row.get(COL_TITLE, ""))
        or not is_missing(row.get(COL_CLUSTERED, ""))
        or not is_missing(row.get(COL_DESC, ""))
    )


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    zylbercweig_root = script_dir.parent  # Zylbercweig/

    default_input = zylbercweig_root / "Zylbercweig_extraction" / "organizations2026-03-08.tsv"
    default_db = zylbercweig_root / "Zylbercweig_extraction" / "Organisations-Report-20260205-2154-xlsx (1).tsv"
    default_output = script_dir / "organizations_classified.tsv"
    default_skipped = script_dir / "skipped_no_name.tsv"

    parser = argparse.ArgumentParser(
        description="Classify organization rows as proper_name, descriptive_term, or ambiguous."
    )
    parser.add_argument("--input", type=Path, default=default_input, help="Path to extraction TSV.")
    parser.add_argument("--db", type=Path, default=default_db, help="Path to existing DB TSV.")
    parser.add_argument("--output", type=Path, default=default_output, help="Path for classified output TSV.")
    parser.add_argument("--skipped", type=Path, default=default_skipped, help="Path for no-name skipped rows TSV.")
    return parser.parse_args()


def main() -> None:
    configure_csv_field_limit()
    args = parse_args()

    source_rows = read_tsv(args.input)
    db_rows = read_tsv(args.db)
    db_exact, db_list = build_db_name_index(db_rows)

    if not source_rows:
        raise ValueError("Input TSV has no data rows.")

    fieldnames = list(source_rows[0].keys())

    output_rows: List[Dict[str, str]] = []
    skipped_rows: List[Dict[str, str]] = []

    for row in source_rows:
        # Exclude rows with no name in any of the three name fields (K-M).
        # Rows that have other content (sentence, location) go to skipped_no_name.tsv
        # for potential future re-extraction; truly empty rows are silently dropped.
        if not has_name_fields(row):
            if not is_empty_row(row):
                skipped_rows.append(row)
            continue

        name_type, confidence = classify_row(row, db_exact, db_list)
        org_type = row.get(COL_ORG_TYPE, "") or ""
        flag = person_reference_flag(
            row.get(COL_TITLE, "") or "",
            row.get(COL_DESC, "") or "",
            org_type,
        )

        # Person references are provisional proper names.
        if flag == REVIEW_FLAG_PERSON_REF:
            name_type = NAME_TYPE_PROPER
            confidence = CONF_LOW

        new_row = dict(row)
        new_row[COL_NAME_TYPE] = name_type
        new_row[COL_CONFIDENCE] = confidence
        new_row[COL_REVIEW_FLAG] = flag
        output_rows.append(new_row)

    for col in (COL_NAME_TYPE, COL_CONFIDENCE, COL_REVIEW_FLAG):
        if col not in fieldnames:
            fieldnames.append(col)

    write_tsv(args.output, output_rows, fieldnames)
    print(f"Wrote {len(output_rows)} rows to: {args.output}")

    if skipped_rows:
        write_tsv(args.skipped, skipped_rows, list(source_rows[0].keys()))
        print(f"Wrote {len(skipped_rows)} no-name rows (with other content) to: {args.skipped}")

    dropped = len(source_rows) - len(output_rows) - len(skipped_rows)
    print(f"Dropped {dropped} fully empty rows (no content in any field)")

    print_summary(output_rows)


if __name__ == "__main__":
    main()
