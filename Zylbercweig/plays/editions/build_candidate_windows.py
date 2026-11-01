#!/usr/bin/env python3.11
"""Step 1 of the editions extraction: cut candidate windows out of the Lexicon.

Every Lexicon entry (people/entry_texts.tsv) is split into sentences; a
sentence is a HIT when its point-stripped form carries an imprint marker
(פֿאַרלאַג, געדרוקט, אַרויסגעגעבן, דערשינען, "(place, year)" ...). Each hit is
kept with one sentence either side (the splitter breaks on abbreviations like
"ז. 31" and initials, so a lone sentence often loses its title or its imprint),
and overlapping spans are merged. Windows keep the ORIGINAL pointed text: the
regex runs on sm() text only.

Output: candidate_windows.tsv, one row per entry with at least one hit.
Deterministic and idempotent; no LLM.

    python3.11 build_candidate_windows.py
"""
from __future__ import annotations

import csv
import pathlib
import re
import unicodedata as ud

HERE = pathlib.Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
OUT = HERE / "candidate_windows.tsv"

csv.field_size_limit(10**9)


def sm(s: str) -> str:
    """NFKD first, then strip points (the corpus stores precomposed U+FB2E/FB4E
    forms; stripping without decomposing silently matches nothing)."""
    s = ud.normalize("NFKD", s or "")
    s = "".join(c for c in s if not ("֑" <= c <= "ׇ"))
    return s.replace("װ", "וו").replace("ױ", "וי").replace("ײ", "יי")


# marker name -> pattern over sm() text. Names are recorded per window so the
# pilot can tell which markers earn their keep.
MARKERS = {
    "farlag": r"פארלאג",
    "drukt": r"דרוקט",              # געדרוקט, אָפּגעדרוקט, and "דרוקט ... אָפּ"
    "aroysgegebn": r"ארויסגעגעב",
    "dershinen": r"ערשי?ינען",      # דערשינען and the later דערשיינען
    "iz_aroys": r"(?:איז|זיינען|זענען|זיין)\s+ארויס\b",
    "farefntlekht": r"פארעפנטלעכט",
    "oysgabe": r"אויסגאבע",
    "bukh_form": r"בוך-?פארם|אין\s+בוך\b|באזונדער(?:ן)?\s+בוך",
    "hoytsoe": r"הוצאה|הוצאת",
    "publikirt": r"פובליקירט",
    "place_year": r"\(\s*[^()]{2,40},\s*1[89]\d\d\s*\)",
    "pages": r"\d+,?\s*זז?\.",        # page count: "(38 זז.)", "[78 ז.]", "28, ז."
    "bib_item": r"\[\d{1,2}\]\s",     # numbered item in a printed-works list
}
# "X.'s printed plays:" -- the Lexicon's end-of-entry bibliography. Its items
# carry no marker word and the splitter breaks them on initials, so from the
# header to the end of the entry is kept whole.
BIB_HEADER = re.compile(r"געדרוקטע\s+(?:פיעס|דראמ|ווערק|שאפונגען)")
MARKER_RE = {k: re.compile(v) for k, v in MARKERS.items()}

# Sentence end = . ! ? followed by whitespace. Crude on purpose: the ±1
# context absorbs the false breaks.
SENT_SPLIT = re.compile(r"(?<=[.!?])\s+")

FIELDS = ["xml_id", "person_id", "volume", "heading", "n_hits", "markers",
          "n_chars", "window_text"]


def windows_for(text: str) -> tuple[list[str], list[str], int]:
    sents = SENT_SPLIT.split(text)
    hits, markers = [], set()
    bib_from = None
    for i, s in enumerate(sents):
        n = sm(s)
        found = [k for k, rx in MARKER_RE.items() if rx.search(n)]
        if bib_from is None and BIB_HEADER.search(n):
            bib_from = i
            found.append("bib_header")
        if found:
            hits.append(i)
            markers.update(found)
    keep = {j for i in hits for j in (i - 1, i, i + 1) if 0 <= j < len(sents)}
    if bib_from is not None:
        keep.update(range(bib_from, len(sents)))
    keep = sorted(keep)
    runs, cur = [], []
    for j in keep:
        if cur and j != cur[-1] + 1:
            runs.append(cur)
            cur = []
        cur.append(j)
    if cur:
        runs.append(cur)
    chunks = [" ".join(sents[j] for j in run) for run in runs]
    return chunks, sorted(markers), len(hits)


def main() -> None:
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    out = []
    for r in rows:
        chunks, markers, n_hits = windows_for(r["entry_text"])
        if not chunks:
            continue
        text = " … ".join(chunks)
        out.append({"xml_id": r["xml_id"], "person_id": r["person_id"],
                    "volume": r["volume"], "heading": r["heading"],
                    "n_hits": n_hits, "markers": "|".join(markers),
                    "n_chars": len(text), "window_text": text})
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n")
        w.writeheader()
        w.writerows(out)
    print(f"entries: {len(rows)}  with windows: {len(out)}  "
          f"window chars: {sum(r['n_chars'] for r in out):,}  -> {OUT.name}")


if __name__ == "__main__":
    main()
