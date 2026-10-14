#!/usr/bin/env python3
"""Split ORG-C00440 צענטראַל into its publisher and its theatre.

Sinai, 2026-10-13: "if not all the צענטראל references talk about the publisher
shouldn't we split?" They do not. The cluster's 7 mentions are two different
Warsaw institutions that share a name:

  PUBLISHER (-> ORG-C00440_Q01)
    [2956]  published_by    "aroysgegebn durkhn farlag צענטראַל in Varshe 1911"
    [14550] published_by    "פ\"ג צענטראַל, וואַרשע, 1922"
    [10270] employee        "mainly as a TRANSLATOR in פֿ'ג צענטראַל"
            — categorised Employment_Performance by the extractor, but פֿ'ג is
            farlag: the sentence governs, not the category.

  THEATRE (-> ORG-C00440_Q02)
    [527]   performer       "all the theatres from the Vilner to צענטראַל"
    [5652]  artistic_staff  decorations for Opatoshu's 'Heym Blut' ('צענטראַל')
    [6405]  performer       "engaged in the newly-opened THEATRE צענטראַל" 1931
    [9463]  performer       "dernokh in צענטראָל"

The theatre half is NOT minted: it is the Warsaw Tsentral Theater, already
db 151 (Leshno st. 1), which already holds the cluster טעאַטער צענטראַל
(ORG-C00610). The publisher half is minted, with ORG-C03167 פֿאַרלאַג
"צענטראַל" attached to it.

Writes organizations_clustered.tsv (the source of truth for cluster membership)
and org_alignment_review.tsv (one review row per half, replacing the parent).
The `_Q` suffix is the live convention in both files. Backups are written
alongside. Idempotent: if ORG-C00440_Q01 already exists, nothing is done.

Usage: python3 split_tsentral.py [--apply]
"""
from __future__ import annotations

import csv
import io
import shutil
import sys
from datetime import datetime
from pathlib import Path

csv.field_size_limit(10 ** 9)

HERE = Path(__file__).resolve().parent
CLUSTERED = HERE / "organizations_clustered.tsv"
ALIGN = HERE / "org_alignment_review.tsv"

PARENT = "ORG-C00440"
PUB, THEA = f"{PARENT}_Q01", f"{PARENT}_Q02"

SENT = "_ - organizations - _ - relations - _ - original_sentence"

# The publisher rows, identified by a stable fragment of their own sentence
# rather than by row index, so the script survives a re-sort of the source.
# Matched against the UNPOINTED sentence: the corpus writes פֿאַרלאָג with
# points and a literal comparison fails on the diacritics
# (feedback_yidracor_strip_points_before_regex).
PUBLISHER_MARKS = (
    "ארויסגעגעבן דורכן פארלאג",   # Frishman, Warsaw 1911
    'פ"ג „צענטראל“, ווארשע,1922',  # Zylbercweig, Warsaw 1922
    "אלס איבערזעצער",              # Epelboym, translator in פ'ג
)


def unpointed(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", s or "")
                   if not unicodedata.combining(c))


def read(path: Path):
    raw = path.read_text(encoding="utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(path: Path, fields, rows, nl):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, delimiter="\t",
                       lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())


def main() -> int:
    apply = "--apply" in sys.argv
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    cfields, crows, cnl = read(CLUSTERED)
    afields, arows, anl = read(ALIGN)

    # The two files can fall out of step: the Zalmen app rewrites
    # org_alignment_review.tsv wholesale and has reverted this split there while
    # organizations_clustered.tsv (which it never writes) stayed split. So the
    # two halves are checked INDEPENDENTLY and each is repaired on its own.
    src_done = any((r.get("cluster_id") or "").strip() == PUB for r in crows)
    aln_done = any((r.get("cluster_id") or "").strip() == PUB for r in arows)
    if src_done and aln_done:
        print(f"{PUB} already in both files — nothing to do.")
        return 0
    if src_done:
        print(f"source already split; repairing {ALIGN.name} only.")

    if src_done:
        pub = [r for r in crows if (r.get("cluster_id") or "").strip() == PUB]
        thea = [r for r in crows if (r.get("cluster_id") or "").strip() == THEA]
        mine = pub + thea
    else:
        mine = [r for r in crows if (r.get("cluster_id") or "").strip() == PARENT]
    if not mine:
        print(f"ERROR: no source rows for {PARENT}", file=sys.stderr)
        return 2

    if not src_done:
        pub, thea = [], []
        for r in mine:
            s = unpointed(r.get(SENT) or "")
            (pub if any(m in s for m in PUBLISHER_MARKS) else thea).append(r)

    print(f"{PARENT}: {len(mine)} source rows -> "
          f"{len(pub)} publisher + {len(thea)} theatre")
    if len(pub) != 3 or len(thea) != 4:
        print("ERROR: expected 3 publisher and 4 theatre rows; the source has "
              "changed. Re-read the sentences before trusting this split.",
              file=sys.stderr)
        for r in mine:
            print(f"   {'PUB ' if r in pub else 'THEA'} «{(r.get(SENT) or '')[:90]}»")
        return 2
    for r in pub:
        print(f"   PUB  «{(r.get(SENT) or '')[:84]}»")
    for r in thea:
        print(f"   THEA «{(r.get(SENT) or '')[:84]}»")

    if not src_done:
        for r in pub:
            r["cluster_id"] = PUB
            r["cluster_size"] = str(len(pub))
        for r in thea:
            r["cluster_id"] = THEA
            r["cluster_size"] = str(len(thea))

    # one review row per half, built from the parent's
    parent_row = next((r for r in arows
                       if (r.get("cluster_id") or "").strip() == PARENT), None)
    if parent_row is None:
        print(f"ERROR: {PARENT} not in {ALIGN.name}", file=sys.stderr)
        return 2
    idx = arows.index(parent_row)

    def half(cid, size, org_type, note):
        row = dict(parent_row)
        row["cluster_id"] = cid
        row["cluster_size"] = str(size)
        row["org_type"] = org_type
        row["decision"] = ""
        row["aligned_db_id"] = ""
        row["reviewer"] = ""
        row["reviewed_at"] = ""
        prev = (parent_row.get("reviewer_notes") or "").strip()
        row["reviewer_notes"] = (prev + " | " if prev else "") + note
        return row

    new_pub = half(PUB, len(pub), "Publisher",
                   "split from ORG-C00440 2026-10-13 (P252): the publisher half "
                   "— Warsaw, 1911 and 1922, with a translator on staff.")
    new_thea = half(THEA, len(thea), "Theatre",
                    "split from ORG-C00440 2026-10-13 (P252): the theatre half "
                    "— performers, artistic staff and Venue=צענטראַל. This is the "
                    "Warsaw Tsentral Theater, already db 151.")
    arows[idx:idx + 1] = [new_pub, new_thea]

    print(f"\n{ALIGN.name}: {PARENT} -> {PUB} (Publisher) + {THEA} (Theatre)")

    if not apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0

    shutil.copy2(ALIGN, ALIGN.with_suffix(f".tsv.pre_tsentral_split_{ts}"))
    if not src_done:
        shutil.copy2(CLUSTERED, CLUSTERED.with_suffix(f".tsv.pre_tsentral_split_{ts}"))
        write(CLUSTERED, cfields, crows, cnl)
    write(ALIGN, afields, arows, anl)
    print(f"\nWROTE both files (backups .pre_tsentral_split_{ts})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
