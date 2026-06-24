"""Find near-duplicate rows inside core_db.tsv.

Implements the full entity-matching cascade prescribed by the entity-matching
skill (Hebrew/Yiddish ↔ Latin orgs):

  Stage 2  Exact name after strong normalization (NFD-strip nikud, remove
           geresh/apostrophe, hyphens; case-fold; strip org-tail tokens
           like טרופּע/קאָמפּאַניע/troupe/company/of/the/from; strip
           possessive עס/ס/'ס).
  Stage 3  Trigram-Jaccard on the normalized full name (skill default).
  Stage 4  Phonetic cross-script similarity (Yiddish→IPA→Daitch-Mokotoff),
           gated by stage-3 trigram floor 0.35.
  Stage 5  Surname-only variant: drop tail tokens, drop a leading given-name
           token when ≥2 tokens remain, then re-score with both trigram and
           phonetic (catches "Ber Hart" ~ "Hart", "George Beker" ~ "בעקער",
           OCR-corrupt tails like "אַרופּע" via fuzzy 1-edit tail match).

Bucketing is by case-folded org_type. Empty-typed rows are cross-compared
against every other bucket (otherwise pairs like 485 [Traveling Company] ~
302 [<empty>] never meet). Type-mismatch cross pairs are still emitted but
flagged in the signal column.

Final score = max(stage 2, stage 3, stage 4, stage 5 trigram, stage 5 phonetic).
Action: MERGE iff stage-2-or-3 ≥ 0.97 and no asymmetric paren qualifier;
otherwise REVIEW. Threshold for inclusion defaults to 0.55 (skill's person/
review band; surname-only matches reliably score ≥0.7 phonetically).

Emits Zylbercweig/organizations/db_dedup_review.tsv. Humans confirm; nothing
is merged automatically — DB row deletion would orphan linked_cluster_ids.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/build_db_dedup_punchlist.py [--threshold 0.55]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
OUT = HERE / "db_dedup_review.tsv"

sys.path.insert(0, str(HERE))
from prepare_alignment import name_similarity, cross_script_similarity  # noqa: E402

# Articles: stripped at the start of a surface name (legacy behavior).
_ART_PREFIX = re.compile(
    r"^(?:the\s+|די\s+|דער\s+|דאָס\s+|דעם\s+|der\s+|das\s+|die\s+|la\s+|le\s+|el\s+)",
    re.IGNORECASE,
)

# Generic org-tail tokens: dropped as whole tokens during surname extraction
# (skill SKILL.md §"Domain-specific layers" calls these out for Dybbuk orgs).
_TAIL_TOKENS = {
    # Yiddish (post-nikud-strip, lowercased)
    "טרופע", "טרופעס", "קאמפאניע", "קאמפאניעס", "טעאטער", "טעאטר",
    "פון", "פאר", "דעם", "די", "דער", "דאס",
    "פראווינץ", "פרובינץ", "פערוואלטונג", "אנסאמבל",
    "קאנסערוואטאריע", "אוניווערזיטעט", "ביבליאטעק", "פארלאג", "דרוקעריי",
    "שול", "חדר", "ישיבה", "סינאגאגע", "שולע", "געזעלשאפט", "פאראיין",
    # Latin
    "troupe", "troup", "troupes", "company", "co", "theatre", "theater",
    "ensemble", "players", "opera", "of", "the", "from",
    "provintz", "province", "wandering", "traveling",
    "conservatory", "conservatoire", "university", "library", "publisher",
    "printer", "school", "yeshiva", "synagogue", "society", "union", "association",
    "press", "publishing",
}

# Possessive endings on a single token.
_POSSESSIVE = re.compile(r"(?:עס|'ס|ס)$")


def _strip_marks(s: str) -> str:
    """NFD-decompose and drop combining marks (nikud, harakat, Latin diacritics)."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s or "")
        if unicodedata.category(c) != "Mn"
    )


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", unicodedata.normalize("NFKD", s or ""))


def _norm(s: str) -> str:
    """Strong normalization for the full-surface comparison (stage 2/3).

    NFD-strips combining marks, removes geresh/apostrophe/quote (which should
    NOT split tokens — they're orthographic, not word boundaries), collapses
    whitespace, lowercases, strips a leading article, and drops a trailing
    parenthetical (often a gloss like "(HAU)" that inflates dissimilarity).
    """
    s = _strip_marks(s)
    s = re.sub(r"[׳״'\"]+", "", s)
    s = _ART_PREFIX.sub("", s.strip())
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _is_tail(token: str) -> bool:
    if not token:
        return True
    if token in _TAIL_TOKENS:
        return True
    stripped = _POSSESSIVE.sub("", token)
    if stripped and stripped in _TAIL_TOKENS:
        return True
    # OCR-tolerant: any tail token within edit distance 1 (catches "אַרופּע"
    # → "טרופע" with ט→א OCR confusion; "טראופ" with ו insertion).
    if len(token) >= 4:
        for t in _TAIL_TOKENS:
            if abs(len(t) - len(token)) > 1:
                continue
            # Hamming + length-diff is a cheap edit-distance ≤1 upper bound.
            diffs = sum(1 for x, y in zip(token, t) if x != y) + abs(len(token) - len(t))
            if diffs <= 1:
                return True
    return False


def _surname(s: str) -> str:
    """Extract the surname-bearing tokens from an org name.

    Strips nikud, removes geresh/apostrophe/quotes (in-token, not as
    separator — important for קאָמפּאַניעיעצ'ס), splits on whitespace and
    hyphens, drops tail tokens (whole-token match, OCR-tolerant), strips
    possessive endings, and if ≥2 content tokens remain drops the leading
    one as a given-name (handles "Ber Hart troupe" → "Hart", "George Beker
    Troupe" → "Beker").
    """
    s = _strip_marks(s)
    s = re.sub(r"[׳״'\"]+", "", s)
    s = re.sub(r"[\-\.]+", " ", s)
    s = re.sub(r"\s*\([^)]*\)\s*", " ", s)
    s = s.lower()
    raw = [t for t in re.split(r"\s+", s) if t.strip()]
    content = []
    for t in raw:
        if _is_tail(t):
            continue
        t = _POSSESSIVE.sub("", t) or t
        content.append(t)
    if not content:
        return ""
    if len(content) >= 2:
        return " ".join(content[1:])
    return content[0]


def _link_count(row: dict) -> int:
    raw = (row.get("linked_cluster_ids") or "").strip()
    return sum(1 for x in raw.split("|") if x.strip())


def _linked_set(row: dict) -> set[str]:
    raw = (row.get("linked_cluster_ids") or "").strip()
    return {x.strip() for x in raw.split("|") if x.strip()}


def _type_key(row: dict) -> str:
    return (row.get("org_type") or "").strip().casefold()


def _score_pair(a: dict, b: dict) -> tuple[float, str, str, str]:
    """Return (best_score, signal, surface_a, surface_b).

    Cascade: stage-2/3 trigram on every (name|name_yiddish|translit) cross
    product, then stage-4 cross-script phonetic (gated by stage-3 ≥ 0.35),
    then stage-5 surname-only trigram + phonetic.
    """
    fields = ("name", "name_yiddish", "name_yiddish_translit")
    surfaces_a = [(k, a.get(k, "")) for k in fields if (a.get(k) or "").strip()]
    surfaces_b = [(k, b.get(k, "")) for k in fields if (b.get(k) or "").strip()]

    best = 0.0
    best_signal = ""
    best_fa = ""
    best_fb = ""

    # Stage 2/3: full-surface trigram on strongly-normalized text.
    max_trigram = 0.0
    for ka, fa in surfaces_a:
        for kb, fb in surfaces_b:
            na, nb = _norm(fa), _norm(fb)
            if not na or not nb:
                continue
            sim = name_similarity(na, nb)
            if sim > max_trigram:
                max_trigram = sim
            if sim > best:
                best = sim
                best_signal = "trigram_full"
                best_fa, best_fb = fa.strip(), fb.strip()

    # Stage 4: phonetic cross-script on full surfaces. Gate by stage-3 floor
    # to avoid false positives from short generic strings.
    if max_trigram >= 0.35:
        for ka, fa in surfaces_a:
            for kb, fb in surfaces_b:
                sim = cross_script_similarity(fa, fb)
                if sim > best:
                    best = sim
                    best_signal = "phonetic_full"
                    best_fa, best_fb = fa.strip(), fb.strip()

    # Stage 5: surname-only on each surface. Trigram + phonetic.
    surn_a = [(k, _surname(v)) for k, v in surfaces_a]
    surn_b = [(k, _surname(v)) for k, v in surfaces_b]
    for ka, sa in surn_a:
        if not sa:
            continue
        for kb, sb in surn_b:
            if not sb:
                continue
            t = name_similarity(sa, sb)
            if t > best:
                best = t
                best_signal = "surname_trigram"
                best_fa, best_fb = f"<surname>{sa}", f"<surname>{sb}"
            p = cross_script_similarity(sa, sb)
            if p > best:
                best = p
                best_signal = "surname_phonetic"
                best_fa, best_fb = f"<surname>{sa}", f"<surname>{sb}"

    return best, best_signal, best_fa, best_fb


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--threshold", type=float, default=0.55,
                    help="Minimum similarity to include a pair (default 0.55)")
    args = ap.parse_args()

    if not CORE_DB.exists():
        raise FileNotFoundError(f"Missing: {CORE_DB}")

    with CORE_DB.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    # Bucket by case-folded org_type. Empty-typed rows are compared against
    # every other bucket (otherwise empty/typed sibling pairs never meet).
    by_type: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_type[_type_key(r)].append(r)
    empties = by_type.get("", [])

    seen: set[tuple[str, str]] = set()
    pairs: list[dict] = []

    def consider(a: dict, b: dict, type_match: bool) -> None:
        ida, idb = a["db_id"], b["db_id"]
        key = (ida, idb) if ida < idb else (idb, ida)
        if key in seen:
            return
        seen.add(key)
        score, signal, fa, fb = _score_pair(a, b)
        if score < args.threshold:
            return
        keep, drop = (a, b) if _link_count(a) >= _link_count(b) else (b, a)
        if _link_count(a) == _link_count(b):
            try:
                keep, drop = sorted([a, b], key=lambda r: int(r["db_id"]))
            except ValueError:
                pass

        # Asymmetric-paren disambiguator: keep legacy logic to avoid merging
        # city-edition siblings like Forverts (NY) vs Forverts (Chicago).
        a_blob = f"{a.get('name','')} {a.get('name_yiddish','')}"
        b_blob = f"{b.get('name','')} {b.get('name_yiddish','')}"
        a_parens = set(re.findall(r"\(([^)]*)\)", a_blob))
        b_parens = set(re.findall(r"\(([^)]*)\)", b_blob))
        asym_paren = bool(a_parens ^ b_parens) and bool(a_parens or b_parens)
        if a_parens and b_parens and a_parens & b_parens:
            asym_paren = False

        # MERGE on either:
        # (1) Strong full-surface trigram (≥0.97). Surname-only and phonetic-
        #     only matches at high score never auto-MERGE because family-troupe
        #     namesakes (Kaminski/Kaminska, multiple Adlers) share surnames
        #     without being the same org.
        # (2) Shared linked_cluster_id (data integrity bug — one cluster
        #     aligned to two DBs) AND score ≥ 0.80 AND same type AND no asym
        #     paren. The score floor excludes the genuine "cluster misaligned
        #     to wrong DB" case (e.g. Minsk State Theatre 501 vs Kiev State
        #     Theatre 574 share a cluster at score 0.750 — different cities;
        #     not a merge but a cluster-removal). 0.80 reliably separates the
        #     two failure modes in current data.
        shared = _linked_set(a) & _linked_set(b)
        if score >= 0.97 and signal == "trigram_full" and not asym_paren and type_match:
            action = "MERGE"
        elif shared and score >= 0.80 and not asym_paren and type_match:
            action = "MERGE"
        else:
            action = "REVIEW"

        pairs.append({
            "db_id_a": a["db_id"],
            "db_id_b": b["db_id"],
            "name_a": a.get("name", "").strip(),
            "name_b": b.get("name", "").strip(),
            "name_yi_a": a.get("name_yiddish", "").strip(),
            "name_yi_b": b.get("name_yiddish", "").strip(),
            "org_type_a": a.get("org_type", "").strip(),
            "org_type_b": b.get("org_type", "").strip(),
            "matched_fields": f"{fa} ~ {fb}",
            "score": f"{score:.3f}",
            "signal": signal,
            "type_match": "Y" if type_match else "N",
            "linked_a": (a.get("linked_cluster_ids") or "").strip(),
            "linked_b": (b.get("linked_cluster_ids") or "").strip(),
            "suggested_action": action,
            "suggested_keep": keep["db_id"],
            "suggested_drop": drop["db_id"],
            "reviewer_decision": "",
            "reviewer_notes": "",
        })

    # Same-type buckets.
    for t, rs in by_type.items():
        for i in range(len(rs)):
            for j in range(i + 1, len(rs)):
                consider(rs[i], rs[j], type_match=True)

    # Empty-type rows × every typed bucket.
    for t, rs in by_type.items():
        if t == "":
            continue
        for a in empties:
            for b in rs:
                consider(a, b, type_match=False)

    pairs.sort(key=lambda p: (-float(p["score"]), p["db_id_a"], p["db_id_b"]))

    headers = [
        "db_id_a", "db_id_b", "name_a", "name_b", "name_yi_a", "name_yi_b",
        "org_type_a", "org_type_b", "matched_fields", "score", "signal",
        "type_match", "linked_a", "linked_b",
        "suggested_action", "suggested_keep", "suggested_drop",
        "reviewer_decision", "reviewer_notes",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(pairs)

    by_action: dict[str, int] = defaultdict(int)
    by_signal: dict[str, int] = defaultdict(int)
    for p in pairs:
        by_action[p["suggested_action"]] += 1
        by_signal[p["signal"]] += 1
    print(f"Wrote {len(pairs)} candidate pairs to {OUT}")
    for k, v in by_action.items():
        print(f"  {k}: {v}")
    print("  by signal:")
    for k, v in sorted(by_signal.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")
    print("\nTop 10:")
    for p in pairs[:10]:
        print(f"  {p['score']}  {p['suggested_action']:6}  "
              f"{p['signal']:18}  "
              f"{p['db_id_a']:>4} -> {p['db_id_b']:<4}  "
              f"keep={p['suggested_keep']}  {p['matched_fields'][:80]}")


if __name__ == "__main__":
    main()
