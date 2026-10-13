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
import unicodedata
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

    # ── Added 2026-10-13 ────────────────────────────────────────────────
    # Theatre, unspaced compound: "מלוכהטעאַטער" written without the hyphen
    # escapes the \b-delimited rule above.
    (r"מלוכה-?טעאטער|טעאטער", "Theatre", "strong"),
    (r"opera\b|אָפּערע\b|אפערעטן", "Theatre", "strong"),

    # Journals/Newspapers — title-shape evidence the keyword list missed.
    # The Hebrew/Yiddish press titles in this DB are overwhelmingly either
    # definite-article forms (דער/די/דאָס + noun) or ha- prefixed Hebrew ones.
    (r"\btageblat\b|\btsayṭung\b|\btsaytung\b|\bvokh\b|\bbleter\b|\bpresse\b",
     "Journals/ Newspapers", "strong"),
    (r"טאגעבלאט|וואכנבלאט|בלעטער|פרעסע", "Journals/ Newspapers", "strong"),

    # Musical organization
    (r"פילהארמאניע|\bphilharmoni\w*\b|האזאמיר|hazomir", "Musical organization", "strong"),

    # Education — institute / commercial school / talmud torah / seminary
    (r"אינסטיטוט|\binstitut\w*\b", "Education", "strong"),
    (r"תלמוד\s*תורה|חדר\b|ישיבה", "Education", "strong"),
    (r"האנדלסשול|האנדלער-שול|קאמערץ-אינסטיטוט", "Education", "strong"),

    # Publisher — a bare personal name whose aligned clusters all carry
    # פֿאַרלאַג / דרוק / פּרעס is a publishing house, not a person.
    (r"\bפארלאג\b|\bפערלאג\b|\bדרוק\b|\bפרעס\b|\bpress\b", "Publisher", "strong"),

    # Welfare / aid
    (r"רויטן\s*קריי[צז]|\bred cross\b", "Welfare/Aid Organization", "strong"),

    # Jewish political bodies
    (r"\bבונד\b|\bpoale zion\b|השומר\s*הצעיר|\bfarband\b|\barbeter ring\b|ארבעטער-רינג",
     "Jewish political bodies", "strong"),
]


def _has(s: str, pat: str) -> bool:
    return bool(re.search(pat, s, flags=re.IGNORECASE))


def sm(s: str) -> str:
    """Normalise for matching.

    core_db.tsv stores Yiddish in the precomposed Hebrew *presentation* forms
    (U+FB2E ALEF WITH PATAH, U+FB2F ALEF WITH QAMATS, U+FB4B VAV WITH HOLAM,
    the FB1D-FB4F block generally), while the RULES above are written with the
    decomposed base-letter + point sequences. The two are NFC/NFD-equal but
    byte-different, so before this normalisation *every* rule containing a
    pointed letter silently never fired — which is why 108 obviously-typeable
    rows (בורג-טעאַטער, קעמי-טעאַטער, ...) came back with no suggestion at all.

    NFKD maps the presentation forms onto base + combining point; stripping the
    combining marks then makes pointed and unpointed spellings compare equal, so
    a rule written either way matches either storage form.
    """
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def classify(name: str, name_yid: str, addr: str, context: str = "") -> tuple[str, str, str]:
    """Return (suggested_type, confidence, hit_signals).

    confidence:
      high   — exactly one strong rule fires
      medium — exactly one medium rule fires, or one strong rule with conflicts
      low    — multiple rules fire on different types
      ""     — no rule fires
    """
    # `context` = the canonical names of the clusters already aligned to this
    # db row. It carries the generic head the db `name` field often drops
    # ("פֿאַרלאַג א. גיטלין" aligned to a row named just "ע. גיטלין"), so it is
    # the only signal that types the bare-personal-name publishers.
    haystack = sm(" ".join(filter(None, [name or "", name_yid or "", addr or "", context or ""])))
    hits: list[tuple[str, str]] = []  # (type, strength)
    signals: list[str] = []
    seen_types: set[str] = set()
    for pat, t, strength in RULES:
        if re.search(sm(pat), haystack):
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
        # Deprecated (merged away) and out-of-project rows are already hidden
        # from candidate dropdowns by prepare_alignment.py, so typing them buys
        # nothing — keep them out of the review queue.
        if (r.get("deprecated", "") or "").strip().lower() == "true":
            continue
        if (r.get("out_of_project", "") or "").strip().lower() == "true":
            continue
        name = r.get("name", "").strip()
        nyid = r.get("name_yiddish", "").strip()
        addr = r.get("address", "").strip()
        context = " | ".join(aligned_by_did.get(r["db_id"].strip(), []))
        suggested, conf, signals = classify(name, nyid, addr, context)
        counts[conf or "(no_match)"] = counts.get(conf or "(no_match)", 0) + 1

        out_rows.append({
            "db_id": r["db_id"],
            "name": name,
            "name_yiddish": nyid,
            "address": addr,
            "linked_cluster_ids": r.get("linked_cluster_ids", ""),
            "aligned_cluster_canonicals": context,
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
