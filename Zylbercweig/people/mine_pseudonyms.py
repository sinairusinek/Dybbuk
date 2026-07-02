"""Mine pseudonym/alias relations from heading + subheading + names_variants.

The Zylbercweig schema encodes aliases in three places:
  - heading bracketed suffix:  "אַדלער, יעקב פּ. [יעקב בן פּנחס]"
  - subheading bracketed text: "[ביילקע קאָלאַך]"
  - names_variants entries:    "בערטאַ קאַליך | בערטאָ קאַליש | ..."

This pass extracts every (primary_form, alias_form) pair and writes them to
people_aliases.tsv. The aliases TSV is then loadable by the alignment + dedup
passes to short-circuit pseudonym matches that pure similarity would miss
(e.g. שלום-עליכם ↔ שלום ראַבינאָוויטש).

Phase A change: alias hygiene — single-token aliases equal to a common
given name (given_name_counts >= 3) are dropped from blocking and scoring to
prevent spurious matches. Dropped aliases are logged to
aliases_suppressed_phaseA.tsv.

Output columns: person_id, xml_id, heading, primary_form, alias_form, source.
"""
from __future__ import annotations
import csv, re, sys, pathlib
from collections import Counter, defaultdict

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
from people_similarity import heading_to_canonical, split_name_tokens  # type: ignore

PEOPLE_TSV = HERE / "people_extracted.tsv"
OUT_TSV = HERE / "people_aliases.tsv"
SUPPRESSED_TSV = HERE / "aliases_suppressed_phaseA.tsv"

BRACKET = re.compile(r"[\[\(]([^\]\)]+)[\]\)]")
DATE_LIKE = re.compile(r"^[\d\s\-.–—:]+$")
DATE_WORDS = ("געב", "געשט", "געבוירן", "געשטאָרבן", "געב.", "געשט.")

GIVEN_NAME_THRESHOLD = 3  # Phase A: single-token alias with count >= this is suppressed


def is_date_phrase(s: str) -> bool:
    s = s.strip()
    if DATE_LIKE.fullmatch(s):
        return True
    return any(w in s for w in DATE_WORDS)


def build_given_name_counts(people: list[dict]) -> Counter:
    """Count how often each token appears in the given-name part of headings.

    For headings with a comma ("surname, given_name"), the given part is
    everything after the first comma. For comma-less headings, every token
    counts (we can't separate surname from given name, so we treat all tokens
    as potential given names to be conservative about suppression — single-
    token comma-less entries contribute their whole form).
    """
    counts: Counter = Counter()
    for p in people:
        h = (p.get("heading") or "").strip()
        # strip bracketed content from heading for this analysis
        h_plain = BRACKET.sub(" ", h).strip()
        if "," in h_plain:
            given_part = h_plain.split(",", 1)[1]
        else:
            given_part = h_plain
        for tok in split_name_tokens(given_part):
            counts[tok] += 1
    return counts


def main():
    with open(PEOPLE_TSV) as f:
        people = list(csv.DictReader(f, delimiter="\t"))

    given_name_counts = build_given_name_counts(people)

    rows = []
    suppressed_rows = []
    for p in people:
        primary = heading_to_canonical(p["heading"]) or p["heading"]
        seen_aliases = set()

        def add_alias(piece: str, source: str) -> bool:
            """Add alias if it passes hygiene. Returns True if added, False if suppressed."""
            nonlocal seen_aliases
            if not piece or is_date_phrase(piece):
                return False
            if piece in seen_aliases:
                return False
            seen_aliases.add(piece)
            # Phase A: alias hygiene — suppress single-token common given names
            tokens = split_name_tokens(piece)
            if len(tokens) == 1:
                tok = tokens[0]
                cnt = given_name_counts.get(tok, 0)
                if cnt >= GIVEN_NAME_THRESHOLD:
                    suppressed_rows.append({
                        "person_id": p["person_id"],
                        "alias": piece,
                        "reason": "single_token_common_given_name",
                        "count": cnt,
                    })
                    return False
            rows.append({
                "person_id": p["person_id"],
                "xml_id": p["xml_id"],
                "heading": p["heading"],
                "primary_form": primary,
                "alias_form": piece,
                "source": source,
            })
            return True

        # bracketed aliases in heading
        for m in BRACKET.finditer(p["heading"] or ""):
            piece = m.group(1).strip()
            add_alias(piece, "heading_bracket")

        # bracketed text in subheading
        for m in BRACKET.finditer(p.get("subheading") or ""):
            piece = m.group(1).strip()
            add_alias(piece, "subheading_bracket")

        # names_variants: every variant beyond the heading itself
        primary_normish = primary.replace(",", "").strip()
        for v in (p.get("names_variants") or "").split("|"):
            v = v.strip()
            if not v:
                continue
            # skip near-duplicates of primary
            if v == p["heading"] or v == primary or v == primary_normish:
                continue
            # skip bare initials
            if len(v.replace(".", "").strip()) <= 1:
                continue
            add_alias(v, "names_variant")

    with open(OUT_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=list(rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT_TSV} ({len(rows)} alias rows for "
          f"{len({r['person_id'] for r in rows})} distinct persons)")

    with open(SUPPRESSED_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=["person_id", "alias", "reason", "count"], delimiter="\t")
        w.writeheader()
        w.writerows(suppressed_rows)
    print(f"wrote {SUPPRESSED_TSV} ({len(suppressed_rows)} suppressed aliases)")

    # quick breakdown
    from collections import Counter as _Counter
    print("by source:", dict(_Counter(r["source"] for r in rows)))


if __name__ == "__main__":
    main()
