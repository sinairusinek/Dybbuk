#!/usr/bin/env python3.11
"""Generate the entry-panel JSON a dossier page's ENTRY_PANEL fetches.

Istanbul's docs/istanbul_entries.json was hand-made; this is the reusable
generator. Output shape: {person_id: {"h": heading, "v": volume, "t": entry_text}}.

By default it emits the dossier subject's own entry IN FULL plus, for every
entry that merely mentions him, an excerpt window around each mention (so the
network/timeline/map panels can open any node's source without shipping an
8-9 MB payload). Excerpted records carry "x": 1 so the panel can label them.

Usage:
    python3.11 make_entry_json.py --config rumshinsky.json
    python3.11 make_entry_json.py --config rumshinsky.json --full
    python3.11 make_entry_json.py --config rumshinsky.json --subject-only
"""
import argparse, csv, json, re, unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
DOCS = HERE.parent.parent.parent / "docs"

csv.field_size_limit(10**8)
POINTS = re.compile(r"[֑-ׇ]")


def strip_points(s):
    return POINTS.sub("", unicodedata.normalize("NFC", s or ""))


GAP = "\n\n   · · ·\n\n"


def excerpt(text, rx, window):
    """Merged +/-window-char windows around every match; GAP marks elisions."""
    spans = []
    for m in rx.finditer(strip_points(text)):
        a, b = max(0, m.start() - window), min(len(text), m.end() + window)
        if spans and a <= spans[-1][1]:
            spans[-1] = (spans[-1][0], max(spans[-1][1], b))
        else:
            spans.append((a, b))
    if not spans:
        return text
    out = GAP.join(text[a:b] for a, b in spans)
    if spans[0][0] > 0:
        out = GAP.lstrip("\n") + out
    if spans[-1][1] < len(text):
        out = out + GAP.rstrip("\n")
    return out


def nikud_tolerant(base):
    """Regex matching `base` with any combining points between its letters."""
    return "".join(re.escape(c) + r"[֑-ׇ]*" for c in base)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--subject-only", action="store_true",
                    help="emit only the subject's entry, not the mentioning entries")
    ap.add_argument("--full", action="store_true",
                    help="emit mentioning entries in full instead of excerpted")
    ap.add_argument("--window", type=int, default=900,
                    help="chars of context kept on each side of a mention (default 900)")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    cfg = json.loads((HERE / args.config).read_text(encoding="utf-8"))
    subject = cfg["entry_person_id"]
    rx = re.compile("|".join(nikud_tolerant(b) for b in cfg["name_regex_base"]))
    out_path = Path(args.out) if args.out else DOCS / f"{cfg['slug']}_entry.json"

    entries, mentioning = {}, 0
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            pid, text = row["person_id"], row["entry_text"]
            is_subject = pid == subject
            hit = (not args.subject_only) and bool(rx.search(strip_points(text)))
            if not (is_subject or hit):
                continue
            if hit and not is_subject:
                mentioning += 1
            rec = {"h": row["heading"], "v": row["volume"], "t": text}
            if hit and not is_subject and not args.full:
                ex = excerpt(text, rx, args.window)
                if len(ex) < len(text):
                    rec["t"], rec["x"] = ex, 1
            entries[pid] = rec

    out_path.write_text(json.dumps(entries, ensure_ascii=False), encoding="utf-8")
    size = out_path.stat().st_size
    n_ex = sum(1 for e in entries.values() if e.get("x"))
    print(f"wrote {out_path} — {len(entries)} entries "
          f"({mentioning} mentioning + subject), {n_ex} excerpted, {size/1024:.0f} KB")
    if subject not in entries:
        raise SystemExit(f"ERROR: subject entry {subject} not found in {ENTRY_TEXTS}")


if __name__ == "__main__":
    main()
