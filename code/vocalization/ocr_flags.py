"""
Detect possible OCR/HTR errors in tokens.

Strategies implemented:
  1. Edit-distance to reference vocabulary (page 6).
     A bare/untouched token whose consonantal form is exactly one edit away
     from a known reference word is a likely OCR variant of that word
     (e.g. ירוהם vs ירוחם, איםר vs איסר).
  2. Confusable-letter pairs.
     Above plus a heuristic boost when the differing letter pair is a
     known scribe/HTR confusable: ה↔ח, ם↔ס, ב↔כ, ר↔ד, נ↔ב, ז↔ו.
  3. Letter-level disagreement between line-level and region-level
     <Unicode> elements in the same PAGE-XML — already exposed by
     the earlier "line 32" finding; can be checked at parse time.
"""

from rules import strip_nikkud

# Pairs of Hebrew letters frequently confused by HTR / typesetting.
# Curated against this corpus:
#   - נג (זאנט↔זאגט, נאט↔גאט)  and  הח (התן↔חתן)  are real OCR errors.
#   - יו REMOVED: און/אין, די/דו, אייך/אויך are valid orthographic variants,
#     not OCR errors.
CONFUSABLES = {
    frozenset("הח"), frozenset("נג"),
    frozenset("םס"), frozenset("בכ"), frozenset("רד"),
}


def edit_distance_1(a: str, b: str) -> tuple[bool, tuple] | tuple[bool, None]:
    """Return (True, (i, ch_a, ch_b)) if strings differ by exactly one
    substitution at index i. False/None otherwise. (Substitution only — we
    don't currently chase insertions/deletions.)"""
    if len(a) != len(b):
        return False, None
    diffs = [(i, x, y) for i, (x, y) in enumerate(zip(a, b)) if x != y]
    if len(diffs) == 1:
        return True, diffs[0]
    return False, None


def find_ocr_candidates(token: str, vocab_keys, *, min_len: int = 4) -> list[dict]:
    """Given a token (possibly vocalized) and an iterable of consonantal
    reference keys from page 6, return zero or more OCR-suspicion records.

    Filtering: short tokens (< min_len) are returned only when the diff is in
    the confusable-pair list, since common short words are frequently 1 edit
    apart from each other (מאן↔מען, ווי↔אוי, האט↔נאט) without being errors.
    """
    key = strip_nikkud(token)
    if key in vocab_keys:
        return []
    out = []
    for ref in vocab_keys:
        ok, diff = edit_distance_1(key, ref)
        if not ok:
            continue
        i, ch_a, ch_b = diff
        confusable = frozenset((ch_a, ch_b)) in CONFUSABLES
        if len(key) < min_len and not confusable:
            continue
        out.append({
            "token": token,
            "consonants": key,
            "ref": ref,
            "diff_pos": i,
            "diff_chars": (ch_a, ch_b),
            "confusable_pair": confusable,
        })
    out.sort(key=lambda r: (not r["confusable_pair"], r["ref"]))
    return out


def confusable_swap_scan(tokens: list[str]) -> list[dict]:
    """For every token, try swapping each occurrence of a known confusable
    letter with its partner. If the swapped form appears in the same
    document's tokens more often than the original, flag the original as
    a likely OCR error.

    Catches recurring page-level errors deterministically and for free —
    e.g. התן (1×) → swap ה↔ח → חתן (7× on the page) → flag.

    Returns one record per (token, swap-yielding-more-common-form).
    """
    from collections import Counter
    keys = [strip_nikkud(t) for t in tokens]
    counts = Counter(keys)
    flagged = []
    seen_pairs = set()
    for tok, n in counts.items():
        for pair in CONFUSABLES:
            a, b = tuple(pair)
            for src, dst in ((a, b), (b, a)):
                if src not in tok:
                    continue
                # Try swapping each occurrence individually (most OCR errors
                # are single-letter swaps, not double).
                for i, ch in enumerate(tok):
                    if ch != src:
                        continue
                    swapped = tok[:i] + dst + tok[i+1:]
                    if swapped == tok:
                        continue
                    n_swap = counts.get(swapped, 0)
                    if n_swap > n:
                        sig = (tok, swapped)
                        if sig in seen_pairs:
                            continue
                        seen_pairs.add(sig)
                        flagged.append({
                            "token": tok,
                            "suspected": swapped,
                            "rare_count": n,
                            "common_count": n_swap,
                            "swap_pos": i,
                            "swap": (src, dst),
                        })
    return flagged


def find_intra_doc_variants(tokens: list[str], *, min_len: int = 4,
                            min_majority_count: int = 2) -> list[dict]:
    """Detect spelling variants of the same word WITHIN a single document.
    Token A is flagged if there exists a token B with:
      - same consonantal length
      - 1 substitution from A
      - count(B) >= min_majority_count and count(B) > count(A)
    This catches cases like ירוחם (frequent) vs ירוהם (rare) — exactly the
    pattern user noticed earlier.
    """
    from collections import Counter
    keys = [strip_nikkud(t) for t in tokens]
    counts = Counter(keys)
    suspects = []
    for k, n in counts.items():
        if len(k) < min_len:
            continue
        for k2, n2 in counts.items():
            if k2 == k:
                continue
            if n2 < min_majority_count or n2 <= n:
                continue
            ok, diff = edit_distance_1(k, k2)
            if not ok:
                continue
            i, ch_a, ch_b = diff
            confusable = frozenset((ch_a, ch_b)) in CONFUSABLES
            suspects.append({
                "rare": k, "rare_count": n,
                "common": k2, "common_count": n2,
                "diff_pos": i,
                "diff_chars": (ch_a, ch_b),
                "confusable_pair": confusable,
            })
    return suspects
