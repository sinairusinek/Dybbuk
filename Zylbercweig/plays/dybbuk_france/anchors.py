"""Resolve curated (start, end) anchors to verbatim spans of the original (pointed) entry text."""
import csv, re, unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
csv.field_size_limit(10**9)
POINT = re.compile(r"[֑-ׇ]")
QUOTES = set("„“”\"'׳״‘’`")
LIG = {"װ": "וו", "ױ": "וי", "ײ": "יי"}


def norm_map(s):
    """Normalised string + index map back to s (points/rafe dropped, quotes unified, ws collapsed)."""
    out, idx = [], []
    for i, c in enumerate(s):
        for d in unicodedata.normalize("NFD", c):
            if POINT.match(d):
                continue
            d = LIG.get(d, d)
            for x in d:
                if x in QUOTES:
                    x = '"'
                if x.isspace():
                    if out and out[-1] == " ":
                        continue
                    x = " "
                out.append(x)
                idx.append(i)
    return "".join(out), idx


def norm(s):
    return norm_map(s)[0].strip()


def load_texts(ids=None):
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        return {r["person_id"]: r for r in csv.DictReader(f, delimiter="\t") if ids is None or r["person_id"] in ids}


def resolve(text, anchors):
    """-> list of verbatim segments, or raises ValueError naming the missing anchor."""
    n, idx = norm_map(text)
    segs = []
    for a, b in anchors:
        na, nb = norm(a), norm(b)
        s = n.find(na)
        if s < 0:
            raise ValueError(f"start not found: {a}")
        e = n.find(nb, s)
        if e < 0:
            raise ValueError(f"end not found after start: {b}")
        e += len(nb)
        if e - s > 1500:
            raise ValueError(f"span too long ({e - s}): {a} … {b}")
        segs.append(text[idx[s]: idx[e - 1] + 1])
    return segs
