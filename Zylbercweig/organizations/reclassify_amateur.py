"""Reclassify org entities as 'Amateur' when their name carries the canonical
Yiddish amateur-theatre signals:
  - 'ליבהאָבער' (= lover/amateur)  — any spelling
  - 'דראַמקרייז' (= drama-circle)  — combined word
  - 'דראַמאַטיש...' + 'קרייז'      — drama-stem + circle as separate words

Schools/institutes/conservatories matched only by the third rule are excluded:
those are professional training, not amateur companies.
"""
from __future__ import annotations
import csv
from pathlib import Path

HERE = Path(__file__).parent
CORE = HERE / "core_db.tsv"
ALIGN = HERE / "org_alignment_review.tsv"

LIB_STRINGS = ("ליבהאָבער", "ליבהאבער", "ליבאבער", "ליבהובער")
DRAMKREYZ_STRINGS = ("דראַמקרייז", "דראָמקרייז", "דראמקרייז", "דראַמקרייזל", "דראָמקרייזל", "דראַמקרעז")
DRAM_STEMS = ("דראַמאַטיש", "דראָמאַטיש", "דראמאטיש", "דראַמאַטייש", "דראַמאטיש", "דראָמאָטיש")
CIRCLE_STRINGS = ("קרייז", "קרייזל", "קרײַז", "קרײז")
SCHOOL_STRINGS = ("שול", "אינסטיטוט", "קורסן", "קלאַסע", "אַקאַדעמיע", "קאָנסערוואַטאָריע")


def load(path):
    with open(path, encoding="utf-8") as f:
        rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        raw = list(rd)
    if not raw: return [], []
    return raw[0], [dict(zip(raw[0], r + [""] * (len(raw[0]) - len(r)))) for r in raw[1:]]


def write(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\", quotechar="")
        w.writerow(header)
        for r in rows:
            w.writerow([str(r.get(k, "") or "").replace("\t", " ").replace("\n", " ").replace("\\", "/")
                        for k in header])


def classify(text: str) -> str | None:
    if any(s in text for s in LIB_STRINGS):
        return "lib"
    if any(s in text for s in DRAMKREYZ_STRINGS):
        return "dramkreyz"
    if any(s in text for s in DRAM_STEMS) and any(c in text for c in CIRCLE_STRINGS):
        if any(s in text for s in SCHOOL_STRINGS):
            return None  # dramatic school, not amateur
        return "dram+circle"
    return None


def main():
    # --- core_db ---
    core_h, core = load(CORE)
    core_changed = []
    for r in core:
        text = " | ".join((r.get(k) or "") for k in ("name", "name_yiddish", "name_variants"))
        why = classify(text)
        if why and r["org_type"].strip().lower() != "amateur":
            prev = r["org_type"]
            r["org_type"] = "Amateur"
            core_changed.append((r["db_id"], why, prev, text[:60]))
    write(CORE, core_h, core)
    print(f"core_db: {len(core_changed)} rows reclassified to Amateur")
    for db_id, why, prev, snippet in core_changed:
        print(f"  db {db_id:>4}  {why:12} {prev!r:18} -> Amateur   {snippet}")

    # --- org_alignment_review ---
    align_h, align = load(ALIGN)
    align_changed = []
    for r in align:
        text = " | ".join((r.get(k) or "") for k in ("canonical_yiddish", "name_variants"))
        why = classify(text)
        if why and r["org_type"].strip().lower() != "amateur":
            prev = r["org_type"]
            r["org_type"] = "Amateur"
            align_changed.append((r["cluster_id"], why, prev, text[:60]))
    write(ALIGN, align_h, align)
    print(f"\nalignment: {len(align_changed)} clusters reclassified to Amateur")
    for cid, why, prev, snippet in align_changed:
        print(f"  {cid:25} {why:12} {prev!r:25} -> Amateur   {snippet}")


if __name__ == "__main__":
    main()
