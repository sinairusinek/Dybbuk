"""Phonetic distance helpers with graceful fallback when optional deps are missing."""

from __future__ import annotations


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        for j, cb in enumerate(b, start=1):
            ins = cur[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            cur.append(min(ins, delete, sub))
        prev = cur
    return prev[-1]


def _normalized_lev_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    denom = max(len(a), len(b), 1)
    return max(0.0, 1.0 - (_levenshtein(a, b) / denom))


def phonetic_similarity(ipa_a: str, ipa_b: str) -> float:
    """Return similarity in [0, 1] between two IPA strings.

    Uses PanPhon feature edit distance when available; falls back to normalized
    Levenshtein similarity if PanPhon is unavailable.
    """
    a = (ipa_a or "").strip()
    b = (ipa_b or "").strip()
    if not a or not b:
        return 0.0

    try:
        from panphon.distance import Distance  # type: ignore

        dist = Distance()
        fed = float(dist.feature_edit_distance(a, b))
        # PanPhon feature edit distance is 0 for exact and can exceed 1 for long strings.
        # Convert to bounded similarity with linear decay.
        return max(0.0, 1.0 - (fed / max(len(a), len(b), 1)))
    except Exception:
        return _normalized_lev_similarity(a, b)
