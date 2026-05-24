"""
Empirical rule discovery from page 6.

Tests hypotheses:
  1. Speaker names are not vocalized.
  2. ע is preceded by segol (consonant + segol + ע).
  3. אַ (pasekh-alef) is the standard /a/.
  4. אָ (komets-alef) is the standard /o/.
  5. וּ (shuruk) is the standard /u/.

Reports per-rule consistency: how often the rule holds when applicable.
"""

import re
import unicodedata
from pathlib import Path
from collections import Counter
from lxml import etree

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NIKKUD = set(chr(c) for c in range(0x0591, 0x05C8))
HEB_LETTER = lambda c: 0x05D0 <= ord(c) <= 0x05EA

# Named nikkud
SEGOL = "ֶ"
PATAH = "ַ"
KAMATZ = "ָ"
HIRIQ = "ִ"
TSERE = "ֵ"
SHURUK_DAGESH = "ּ"  # dagesh in vav = shuruk
HOLAM = "ֹ"
SHEVA = "ְ"
NIKKUD_NAME = {
    SEGOL: "segol", PATAH: "patah", KAMATZ: "kamatz", HIRIQ: "hiriq",
    TSERE: "tsere", SHURUK_DAGESH: "dagesh/shuruk", HOLAM: "holam", SHEVA: "sheva",
}

repo_root = Path(__file__).resolve().parents[2]
project = "Yudale_der_blinder,_Emkroyt1908"
ref_path = repo_root / "data" / project / "page" / "0006_OTgwNjYwMDE.111007894.xml"


def get_lines(tree):
    """Yield each <Unicode> text as a line."""
    for el in tree.iter(f"{{{PAGE_NS}}}Unicode"):
        if el.text:
            yield el.text


def parse_token(tok: str):
    """Return list of (letter, [marks]) pairs."""
    out = []
    for ch in tok:
        if HEB_LETTER(ch):
            out.append([ch, []])
        elif ch in NIKKUD and out:
            out[-1][1].append(ch)
    return out


def main():
    tree = etree.parse(str(ref_path))
    lines = list(get_lines(tree))
    print(f"Page 6: {len(lines)} lines")

    # --- Rule 1: speaker names unvocalized ---
    # Heuristic: token immediately before ':' at line start
    speaker_marked = 0
    speaker_total = 0
    speakers = Counter()
    for line in lines:
        m = re.match(r"\s*([א-ת֑-ׇ]+):", line)
        if m:
            sp = m.group(1)
            speaker_total += 1
            speakers[sp] += 1
            if any(c in NIKKUD for c in sp):
                speaker_marked += 1
    print(f"\nRule 1 — speaker names unvocalized:")
    print(f"  speaker tokens found: {speaker_total}")
    print(f"  carrying any nikkud:  {speaker_marked}")
    print(f"  distinct speakers:    {dict(speakers)}")

    # --- Rule 2: consonant before ע gets segol ---
    # Walk every token, find positions of ע, look at the previous letter's marks.
    prev_to_ayin = Counter()
    ayin_carries = Counter()
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            for i, (ltr, marks) in enumerate(letters):
                if ltr == "ע":
                    ayin_carries[tuple(marks) or ("∅",)] += 1
                    if i > 0:
                        prev_marks = letters[i-1][1]
                        # Take vowel marks (skip dagesh-only)
                        vowels = [m for m in prev_marks if m != SHURUK_DAGESH]
                        key = tuple(NIKKUD_NAME.get(m, m) for m in vowels) or ("∅",)
                        prev_to_ayin[key] += 1
    print(f"\nRule 2 — vowel on consonant immediately before ע:")
    for k, n in prev_to_ayin.most_common():
        print(f"  {n:4d}  {'+'.join(k)}")
    print(f"  ע itself carries:")
    for k, n in ayin_carries.most_common():
        names = [NIKKUD_NAME.get(c, c) for c in k] if k != ("∅",) else ["∅"]
        print(f"  {n:4d}  {'+'.join(names)}")

    # --- Rule 3,4,5: aleph vowels and shuruk ---
    aleph_marks = Counter()
    vav_marks = Counter()
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            for ltr, marks in parse_token(tok):
                if ltr == "א":
                    key = tuple(NIKKUD_NAME.get(m, m) for m in marks) or ("∅",)
                    aleph_marks[key] += 1
                if ltr == "ו":
                    key = tuple(NIKKUD_NAME.get(m, m) for m in marks) or ("∅",)
                    vav_marks[key] += 1
    print(f"\nRule 3/4 — א marks:")
    for k, n in aleph_marks.most_common():
        print(f"  {n:4d}  {'+'.join(k)}")
    print(f"\nRule 5 — ו marks:")
    for k, n in vav_marks.most_common():
        print(f"  {n:4d}  {'+'.join(k)}")

    # --- Rule 6: vowel on consonant before single י vs. double יי ---
    prev_to_single_yod = Counter()
    prev_to_double_yod = Counter()
    single_yod_marks = Counter()
    double_yod_first_marks = Counter()
    double_yod_second_marks = Counter()
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            i = 0
            while i < len(letters):
                ltr, marks = letters[i]
                if ltr == "י":
                    if i + 1 < len(letters) and letters[i+1][0] == "י":
                        # Double yod
                        double_yod_first_marks[tuple(marks) or ("∅",)] += 1
                        double_yod_second_marks[tuple(letters[i+1][1]) or ("∅",)] += 1
                        if i > 0:
                            pm = [m for m in letters[i-1][1] if m != SHURUK_DAGESH]
                            key = tuple(NIKKUD_NAME.get(m, m) for m in pm) or ("∅",)
                            prev_to_double_yod[key] += 1
                        i += 2
                        continue
                    else:
                        # Single yod
                        single_yod_marks[tuple(marks) or ("∅",)] += 1
                        if i > 0:
                            pm = [m for m in letters[i-1][1] if m != SHURUK_DAGESH]
                            key = tuple(NIKKUD_NAME.get(m, m) for m in pm) or ("∅",)
                            prev_to_single_yod[key] += 1
                i += 1
    def show(label, c):
        print(f"  {label}:")
        for k, n in c.most_common():
            names = [NIKKUD_NAME.get(x, x) for x in k] if k != ("∅",) else ["∅"]
            print(f"    {n:4d}  {'+'.join(names)}")

    print(f"\nRule 6 — context around single י vs. double יי:")
    print(f"  vowel on consonant before single י:")
    for k, n in prev_to_single_yod.most_common():
        print(f"    {n:4d}  {'+'.join(k)}")
    print(f"  vowel on consonant before double יי:")
    for k, n in prev_to_double_yod.most_common():
        print(f"    {n:4d}  {'+'.join(k)}")
    show("single י itself carries", single_yod_marks)
    show("first of יי carries", double_yod_first_marks)
    show("second of יי carries", double_yod_second_marks)

    # --- Rule 7: sheva on first of consonant cluster ---
    # Operationalization: for every adjacent (L1, L2) within a word where
    # L2 is a "hard" consonant (not in {א ע ו י} which can carry vowels),
    # tally what L1 carries. Skip if L1 is itself in {א ע}.
    MATRES = set("אעוי")
    SOFT_VOWELS = set("אע")  # never start clusters
    cluster_first_marks = Counter()
    cluster_examples = []
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            for i in range(len(letters) - 1):
                l1, m1 = letters[i]
                l2, _ = letters[i+1]
                if l1 in SOFT_VOWELS:
                    continue
                if l2 in MATRES:
                    continue  # next is vowel-carrier, not a cluster
                # Skip if l1 is the very first letter of the word (often unmarked)
                if i == 0:
                    continue
                vowels = [m for m in m1 if m != SHURUK_DAGESH]
                key = tuple(NIKKUD_NAME.get(m, m) for m in vowels) or ("∅",)
                cluster_first_marks[key] += 1
                if key == ("sheva",) and len(cluster_examples) < 6:
                    cluster_examples.append(tok)
    print(f"\nRule 7 — vowel on first letter of consonant cluster (mid-word, prev letter consumed a vowel):")
    for k, n in cluster_first_marks.most_common():
        print(f"  {n:4d}  {'+'.join(k)}")
    print(f"  sheva examples: {cluster_examples}")

    # --- Rule 8: word-final ן / ם ---
    final_n_marks = Counter()
    final_m_marks = Counter()
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            if not letters:
                continue
            last_l, last_m = letters[-1]
            if last_l == "ן":
                final_n_marks[tuple(last_m) or ("∅",)] += 1
            elif last_l == "ם":
                final_m_marks[tuple(last_m) or ("∅",)] += 1
    show("word-final ן carries", final_n_marks)
    show("word-final ם carries", final_m_marks)

    # --- Rule 9: וו (double vav) ---
    double_vav_first = Counter()
    double_vav_second = Counter()
    prev_to_double_vav = Counter()
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            i = 0
            while i < len(letters):
                if letters[i][0] == "ו" and i+1 < len(letters) and letters[i+1][0] == "ו":
                    double_vav_first[tuple(letters[i][1]) or ("∅",)] += 1
                    double_vav_second[tuple(letters[i+1][1]) or ("∅",)] += 1
                    if i > 0:
                        pm = [m for m in letters[i-1][1] if m != SHURUK_DAGESH]
                        key = tuple(NIKKUD_NAME.get(m, m) for m in pm) or ("∅",)
                        prev_to_double_vav[key] += 1
                    i += 2
                    continue
                i += 1
    print("\nRule 9 — double וו:")
    show("first ו carries", double_vav_first)
    show("second ו carries", double_vav_second)
    show("vowel on consonant before וו", prev_to_double_vav)

    # --- Rule 10: dagesh distribution on ב פ כ ש ---
    DAGESH = "ּ"
    SHIN_DOT = "ׁ"
    SIN_DOT = "ׂ"
    for letter in "בפכשת":
        bare = 0
        with_dagesh = 0
        with_shin_dot = 0
        with_sin_dot = 0
        any_mark = 0
        total = 0
        for line in lines:
            for tok in re.findall(r"[א-ת֑-ׇ']+", line):
                for ltr, marks in parse_token(tok):
                    if ltr == letter:
                        total += 1
                        if DAGESH in marks: with_dagesh += 1
                        if SHIN_DOT in marks: with_shin_dot += 1
                        if SIN_DOT in marks: with_sin_dot += 1
                        if marks: any_mark += 1
                        else: bare += 1
        print(f"\nRule 10 — {letter}: total {total}, bare {bare}, any-mark {any_mark}, dagesh {with_dagesh}, shin-dot {with_shin_dot}, sin-dot {with_sin_dot}")

    # --- Rule 11: prefix גע- (past participle / nominal) ---
    # Find tokens starting with ג ע ... ; check vowel on ג and on ע
    ge_g_marks = Counter()
    ge_e_marks = Counter()
    ge_examples = []
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            if len(letters) >= 2 and letters[0][0] == "ג" and letters[1][0] == "ע":
                ge_g_marks[tuple(letters[0][1]) or ("∅",)] += 1
                ge_e_marks[tuple(letters[1][1]) or ("∅",)] += 1
                if len(ge_examples) < 8:
                    ge_examples.append(tok)
    print("\nRule 11 — prefix גע-:")
    show("ג carries", ge_g_marks)
    show("ע carries", ge_e_marks)
    print(f"  examples: {ge_examples}")

    # --- Rule 12: prefix פאר / פֿאר ---
    far_examples = []
    far_first_marks = Counter()
    far_a_marks = Counter()  # the א
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            if len(letters) >= 3 and letters[0][0] == "פ" and letters[1][0] == "א" and letters[2][0] == "ר":
                far_first_marks[tuple(letters[0][1]) or ("∅",)] += 1
                far_a_marks[tuple(letters[1][1]) or ("∅",)] += 1
                if len(far_examples) < 8:
                    far_examples.append(tok)
    print("\nRule 12 — prefix פאר-:")
    show("פ carries", far_first_marks)
    show("א carries", far_a_marks)
    print(f"  examples: {far_examples}")

    # --- Rule 13: word-final ה ---
    final_h_marks = Counter()
    final_h_examples = []
    for line in lines:
        for tok in re.findall(r"[א-ת֑-ׇ']+", line):
            letters = parse_token(tok)
            if letters and letters[-1][0] == "ה":
                final_h_marks[tuple(letters[-1][1]) or ("∅",)] += 1
                if len(final_h_examples) < 6:
                    final_h_examples.append(tok)
    print("\nRule 13 — word-final ה:")
    show("ה carries", final_h_marks)
    print(f"  examples: {final_h_examples}")

    # --- Rule 14: common suffixes — vowel before suffix-ר / suffix-ן / suffix-ט ---
    # Specifically: pattern (consonant)(ע)(ר|ן|ט|ם) at word end → likely segol on consonant before ע
    # Already covered partly by Rule 2; here check word-end ער / ען / עט / עם blocks
    for suffix in ["ער", "ען", "עט", "עם", "עס", "טע"]:
        prev_consonant_marks = Counter()
        suffix_internal_marks = Counter()
        examples = []
        for line in lines:
            for tok in re.findall(r"[א-ת֑-ׇ']+", line):
                letters = parse_token(tok)
                if len(letters) < len(suffix) + 1:
                    continue
                tail = "".join(l[0] for l in letters[-len(suffix):])
                if tail == suffix:
                    pre = letters[-len(suffix)-1]
                    pm = [m for m in pre[1] if m != SHURUK_DAGESH]
                    key = tuple(NIKKUD_NAME.get(m, m) for m in pm) or ("∅",)
                    prev_consonant_marks[key] += 1
                    if len(examples) < 5:
                        examples.append(tok)
        print(f"\nRule 14 — word-final -{suffix}: vowel on consonant just before -{suffix}:")
        for k, n in prev_consonant_marks.most_common():
            print(f"    {n:4d}  {'+'.join(k)}")
        print(f"  examples: {examples}")

    # --- Rule 15: ר / ל / נ / מ as cluster-first specifically ---
    # Refine Rule 7 to specific letters
    for letter in "רלנמד":
        marks = Counter()
        n_in_cluster = 0
        for line in lines:
            for tok in re.findall(r"[א-ת֑-ׇ']+", line):
                letters = parse_token(tok)
                for i in range(len(letters) - 1):
                    if letters[i][0] != letter:
                        continue
                    if letters[i+1][0] in MATRES:
                        continue
                    if i == 0:
                        continue
                    n_in_cluster += 1
                    vowels = [m for m in letters[i][1] if m != SHURUK_DAGESH]
                    key = tuple(NIKKUD_NAME.get(m, m) for m in vowels) or ("∅",)
                    marks[key] += 1
        if n_in_cluster:
            print(f"\nRule 15 — {letter} as cluster-first (n={n_in_cluster}):")
            for k, n in marks.most_common():
                print(f"    {n:4d}  {'+'.join(k)}")


if __name__ == "__main__":
    main()
