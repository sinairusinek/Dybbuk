"""
Compare pipeline-vocalized PAGE-XML output (in page_final/) against a
DraCor TEI version of the same play.

The two text streams are unaligned (different edition, different OCR, different
line breaks), so we compare at the bag-of-tokens level keyed on bare consonants:

  * pipeline_vocab[bare_key] -> most-common pipeline form
  * dracor_vocab[bare_key]   -> most-common DraCor form
  * For each shared key, classify the relationship.

We don't try to align by sentence or page; we just measure how well our
vocalization choices match DraCor's editor.
"""

import argparse
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from lxml import etree

NIK = re.compile(r"[֑-ׇ]")
TOKEN = re.compile(r"[֐-׿]+")
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
TEI_NS = "http://www.tei-c.org/ns/1.0"

# Vowel marks we treat as "vowels" (excludes dagesh, shin-dot, sin-dot).
VOWELS = set("ְֱֲֳִֵֶַָׇֻ")  # sheva, hatafs, hiriq, tsere, segol, patah, kamatz, qubuts, kamatz-katan
HOLAM = "ֹ"
MATRES_SHIFTABLE = set("יעאה")  # vav excluded: holam-vav and וו digraph are legit


def shift_vowels_off_matres(tok: str) -> str:
    """If a matres letter carries a vowel (other than holam on vav) and the
    immediately-preceding consonant has no vowel, move the vowel to that
    consonant. Mirrors the RA's convention (consonant carries vowel, matres
    is bare) and undoes the DerMann-DraCor editor's opposite choice.
    """
    out = []  # list of [letter, marks]
    for ch in tok:
        if "א" <= ch <= "ת":
            out.append([ch, []])
        elif "֑" <= ch <= "ׇ" and out:
            out[-1][1].append(ch)
        else:
            out.append([ch, []])
    changed = True
    while changed:
        changed = False
        for i in range(1, len(out)):
            ltr, marks = out[i]
            if ltr not in MATRES_SHIFTABLE:
                continue
            vowel_marks = [m for m in marks if m in VOWELS]
            if not vowel_marks:
                continue
            prev_ltr, prev_marks = out[i-1]
            if not ("א" <= prev_ltr <= "ת"):
                continue
            if any(m in VOWELS for m in prev_marks):
                continue
            # Move vowels back
            for v in vowel_marks:
                marks.remove(v)
                prev_marks.append(v)
            changed = True
            break
    return "".join(l + "".join(m) for l, m in out)


def strip_nik(s):
    return NIK.sub("", s)


def nfc(s):
    return unicodedata.normalize("NFC", s)


def read_xml_maybe_skip_preamble(path):
    """DraCor downloads can have a browser preamble line. Skip it."""
    data = Path(path).read_bytes()
    if not data.lstrip().startswith(b"<"):
        # find first '<' byte
        i = data.find(b"<")
        if i > 0:
            data = data[i:]
    return etree.fromstring(data)


def page_xml_tokens(page_dir):
    """Yield NFC tokens from line-level <Unicode> in every page, excluding
    speaker prefixes and bracketed stage directions."""
    from rules import speaker_span, bracket_spans
    for p in sorted(Path(page_dir).glob("[0-9]*.xml")):
        try:
            root = etree.parse(str(p)).getroot()
        except Exception as e:
            print(f"WARN: parse {p.name}: {e}", file=sys.stderr)
            continue
        for tl in root.iter(f"{{{PAGE_NS}}}TextLine"):
            for te in tl.findall(f"{{{PAGE_NS}}}TextEquiv"):
                u = te.find(f"{{{PAGE_NS}}}Unicode")
                if u is not None and u.text:
                    line = nfc(u.text)
                    sp = speaker_span(line)
                    brackets = bracket_spans(line)
                    for m in TOKEN.finditer(line):
                        s, e = m.span()
                        if sp and s >= sp[0] and e <= sp[1]:
                            continue
                        if any(bs <= s and e <= be for bs, be in brackets):
                            continue
                        yield m.group(0)
                    break


def _iter_text_skipping(el, skip_tags):
    """Yield text fragments under `el`, skipping any subtree whose tag is in
    skip_tags. Uses lxml text/tail walk."""
    if el.tag in skip_tags:
        # Still take .tail (the text *after* the skipped element)
        if el.tail:
            yield el.tail
        return
    if el.text:
        yield el.text
    for child in el:
        yield from _iter_text_skipping(child, skip_tags)
    if el.tail:
        yield el.tail


def tei_tokens(tei_path, normalize=False, exclude_speakers_stages=True):
    root = read_xml_maybe_skip_preamble(tei_path)
    body = root.find(f".//{{{TEI_NS}}}body") or root
    skip = set()
    if exclude_speakers_stages:
        skip = {f"{{{TEI_NS}}}speaker", f"{{{TEI_NS}}}stage"}
    text = "".join(_iter_text_skipping(body, skip))
    # body itself contributes via its own .text (handled), but _iter_text_skipping
    # was called on body so its tail is irrelevant.
    for tok in TOKEN.findall(nfc(text)):
        if normalize:
            tok = shift_vowels_off_matres(tok)
        yield tok


def majority_vocab(tokens):
    counts = defaultdict(Counter)
    for t in tokens:
        counts[strip_nik(t)][t] += 1
    return {k: c.most_common(1)[0][0] for k, c in counts.items()}, counts


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--page-dir", required=True,
                    help="data/{play}/page_final/")
    ap.add_argument("--tei", required=True,
                    help="path to DraCor TEI xml")
    ap.add_argument("--csv", default=None,
                    help="write all per-key differences to this CSV for manual review")
    ap.add_argument("--normalize-tei", action="store_true",
                    help="shift vowels off matres letters in DraCor TEI before comparing "
                         "(undoes the DerMann-DraCor editor's convention)")
    ap.add_argument("--keep-speakers-stages", action="store_true",
                    help="don't exclude speaker/stage tokens (default: exclude)")
    args = ap.parse_args()

    pipe_toks = list(page_xml_tokens(args.page_dir))
    dracor_toks = list(tei_tokens(args.tei,
                                  normalize=args.normalize_tei,
                                  exclude_speakers_stages=not args.keep_speakers_stages))
    if args.normalize_tei:
        print("(TEI vowels shifted off matres letters)")
    print(f"pipeline tokens: {len(pipe_toks):,} ({len(set(strip_nik(t) for t in pipe_toks)):,} unique keys)")
    print(f"dracor   tokens: {len(dracor_toks):,} ({len(set(strip_nik(t) for t in dracor_toks)):,} unique keys)")

    pipe_v, pipe_c = majority_vocab(pipe_toks)
    drac_v, drac_c = majority_vocab(dracor_toks)

    shared = set(pipe_v) & set(drac_v)
    only_pipe = set(pipe_v) - set(drac_v)
    only_drac = set(drac_v) - set(pipe_v)
    print(f"\nshared bare keys: {len(shared)}   only-pipeline: {len(only_pipe)}   only-dracor: {len(only_drac)}")

    exact = 0
    both_voc = 0
    only_drac_voc = 0
    only_pipe_voc = 0
    both_bare = 0
    diff_voc = 0
    diffs = []

    for k in shared:
        p, d = pipe_v[k], drac_v[k]
        p_voc, d_voc = bool(NIK.search(p)), bool(NIK.search(d))
        if not p_voc and not d_voc:
            both_bare += 1
        elif p_voc and not d_voc:
            only_pipe_voc += 1
        elif d_voc and not p_voc:
            only_drac_voc += 1
            diffs.append(("we-left-bare", k, p, d))
        else:
            both_voc += 1
            if nfc(p) == nfc(d):
                exact += 1
            else:
                diff_voc += 1
                diffs.append(("differ", k, p, d))

    print(f"\nAmong {len(shared)} shared keys:")
    print(f"  both-bare              {both_bare:5}")
    print(f"  both-vocalized exact   {exact:5}  ({100*exact/max(1,both_voc):.1f}% of both-voc)")
    print(f"  both-vocalized differ  {diff_voc:5}")
    print(f"  only pipeline voc      {only_pipe_voc:5}")
    print(f"  only dracor voc        {only_drac_voc:5}")

    # Weight by token frequency
    pipe_token_freq = sum(sum(c.values()) for c in pipe_c.values())
    weighted_exact = 0
    weighted_both_voc = 0
    for k in shared:
        p, d = pipe_v[k], drac_v[k]
        if NIK.search(p) and NIK.search(d):
            weight = sum(pipe_c[k].values())
            weighted_both_voc += weight
            if nfc(p) == nfc(d):
                weighted_exact += weight
    if weighted_both_voc:
        print(f"  token-weighted exact match (both-voc): "
              f"{100*weighted_exact/weighted_both_voc:.1f}% "
              f"({weighted_exact}/{weighted_both_voc} tokens)")

    # Also surface keys vocalized only by one side
    only_us = [(k, pipe_v[k]) for k in pipe_v
               if NIK.search(pipe_v[k]) and k not in drac_v]
    only_dr = [(k, drac_v[k]) for k in drac_v
               if NIK.search(drac_v[k]) and k not in pipe_v]
    only_us.sort(key=lambda x: -sum(pipe_c[x[0]].values()))
    only_dr.sort(key=lambda x: -sum(drac_c[x[0]].values()))

    # Top divergences (by pipeline frequency)
    diffs.sort(key=lambda x: -sum(pipe_c[x[1]].values()))
    print(f"\nTop 30 divergences by pipeline token frequency:")
    print(f"  {'kind':14} {'count':>5}  {'pipeline':<22} {'dracor':<22}")
    for kind, k, p, d in diffs[:30]:
        cnt = sum(pipe_c[k].values())
        print(f"  {kind:14} {cnt:5}  {p:<22} {d:<22}")

    if args.csv:
        import csv
        with open(args.csv, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["kind", "bare_key", "pipeline_form", "dracor_form",
                        "pipeline_count", "dracor_count"])
            for kind, k, p, d in diffs:
                w.writerow([kind, k, p, d,
                            sum(pipe_c[k].values()), sum(drac_c[k].values())])
            for k, d in only_dr:
                w.writerow(["only-dracor-vocalized", k, "", d, 0,
                            sum(drac_c[k].values())])
            for k, p in only_us:
                w.writerow(["only-pipeline-vocalized", k, p, "",
                            sum(pipe_c[k].values()), 0])
        print(f"\nWrote {args.csv}")


if __name__ == "__main__":
    main()
