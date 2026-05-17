"""Survey + auto-classify empty-type DB rows in core_db.tsv.

Output: db_empty_type_review.tsv — one row per empty-type DB row, with a
keyword-based suggested type and confidence. Auto-fill trivial cases; leave
ambiguous ones for PI review.

After PI sets pi_decision=APPROVE and pi_type, run apply_empty_type_backfill.py.
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
CORE_DB_TSV = HERE / "core_db.tsv"
ALIGN_TSV = HERE / "org_alignment_review.tsv"
OUT_TSV = HERE / "db_empty_type_review.tsv"


# (regex_pattern_lowercase, type, signal_strength) — ordered roughly by specificity.
RULES: list[tuple[str, str, str]] = [
    # Theatre — single Yiddish/Latin token, strong
    (r"\bטעאַטער\b|\bטעאטער\b|\bטעאָטער\b", "Theatre", "strong"),
    (r"\btheatre\b|\btheater\b", "Theatre", "strong"),

    # Traveling Company — "troupe"/"טרופע" is essentially diagnostic
    (r"\bטרופּע\b|\bטרופע\b", "Traveling Company", "strong"),
    (r"\btroupe\b|\bkompani\w*\b", "Traveling Company", "strong"),
    (r"\bקאָמפּאַני\w*\b|\bקאָמפּאָני\w*\b", "Traveling Company", "medium"),

    # Journals/Newspapers
    (r"\bצייטונג\b|\bזשורנאַל\b|\bבלאַט\b", "Journals/ Newspapers", "strong"),
    (r"\bzeitung\b|\bjournal\b|\bgazette\b|\bnewspaper\b", "Journals/ Newspapers", "strong"),

    # Publisher / Printer (printer-specific signals win over generic press)
    (r"\bדפוס\b|\bפאַרלאַג\b|\bפארלאג\b|\bדרוקעריי\b", "Publisher", "strong"),
    (r"\bpublishing\b|\bpublisher\b|\bdrukerei\b|\bfarlag\b", "Publisher", "strong"),

    # Education
    (r"\bשול\b|\bסעמינאַר\b|\bאוניווערזיטעט\b|\bאַקאַדעמיע\b|\bגימנאַזיע\b", "Education", "strong"),
    (r"\bschool\b|\buniversity\b|\bseminar\w*\b|\bakademie\b|\bgymnasium\b", "Education", "strong"),

    # Library
    (r"\bביבליאָטעק\b", "Library", "strong"),
    (r"\blibrary\b", "Library", "strong"),

    # Theatre-related Society / Union / Club
    (r"\bפאַריין\b|\bפאריין\b|\bקלוב\b|\bגעזעלשאַפט\b", "Theatre-related Society/ Union", "medium"),
    (r"\bunion\b|\bsociety\b|\bclub\b|\bassociation\b|\bgesellschaft\b", "Theatre-related Society/ Union", "medium"),

    # Fraternal Order
    (r"\border of\b|\blodge\b", "Fraternal order", "strong"),
    (r"\bאָרדער\b", "Fraternal order", "medium"),

    # Religious institutions
    (r"\bבית\s*המדרש\b|\bביה\"כ\b|\bsynagogue\b|\bshul\b", "Religious institutions/organizations", "strong"),
]


def _has(s: str, pat: str) -> bool:
    return bool(re.search(pat, s, flags=re.IGNORECASE))


def classify(name: str, name_yid: str, addr: str) -> tuple[str, str, str]:
    """Return (suggested_type, confidence, hit_signals).

    confidence:
      high   — exactly one strong rule fires
      medium — exactly one medium rule fires, or one strong rule with conflicts
      low    — multiple rules fire on different types
      ""     — no rule fires
    """
    haystack = " ".join(filter(None, [name or "", name_yid or "", addr or ""])).lower()
    hits: list[tuple[str, str]] = []  # (type, strength)
    signals: list[str] = []
    seen_types: set[str] = set()
    for pat, t, strength in RULES:
        if re.search(pat, haystack):
            hits.append((t, strength))
            signals.append(f"{t}({strength})")
            seen_types.add(t)

    if not hits:
        return "", "", ""
    if len(seen_types) == 1:
        t = next(iter(seen_types))
        strengths = {h[1] for h in hits if h[0] == t}
        conf = "high" if "strong" in strengths else "medium"
        return t, conf, "|".join(signals)
    # Multiple types fired — prefer the most specific strong hit, mark low.
    strong_types = [t for t, s in hits if s == "strong"]
    if len(set(strong_types)) == 1:
        return strong_types[0], "medium", "|".join(signals)
    if strong_types:
        return strong_types[0], "low", "|".join(signals)
    return hits[0][0], "low", "|".join(signals)


def main() -> None:
    with open(CORE_DB_TSV, encoding="utf-8") as f:
        db_rows = list(csv.DictReader(f, delimiter="\t"))

    # Index aligned clusters per db_id (for context display)
    aligned_by_did: dict[str, list[str]] = {}
    with open(ALIGN_TSV, encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r.get("decision", "").strip() == "ALIGN":
                did = r.get("aligned_db_id", "").strip()
                if did:
                    aligned_by_did.setdefault(did, []).append(r.get("canonical_yiddish", ""))

    out_rows = []
    counts: dict[str, int] = {}
    for r in db_rows:
        if r.get("org_type", "").strip():
            continue
        name = r.get("name", "").strip()
        nyid = r.get("name_yiddish", "").strip()
        addr = r.get("address", "").strip()
        suggested, conf, signals = classify(name, nyid, addr)
        counts[conf or "(no_match)"] = counts.get(conf or "(no_match)", 0) + 1

        out_rows.append({
            "db_id": r["db_id"],
            "name": name,
            "name_yiddish": nyid,
            "address": addr,
            "linked_cluster_ids": r.get("linked_cluster_ids", ""),
            "aligned_cluster_canonicals": " | ".join(aligned_by_did.get(r["db_id"].strip(), [])),
            "suggested_type": suggested,
            "suggested_confidence": conf,
            "matched_signals": signals,
            "pi_decision": "",
            "pi_type": "",
            "reviewer": "",
            "reviewed_at": "",
        })

    fieldnames = list(out_rows[0].keys())
    with open(OUT_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)

    print(f"Wrote {len(out_rows)} rows to {OUT_TSV}")
    print("By suggested_confidence:", counts)


if __name__ == "__main__":
    main()
