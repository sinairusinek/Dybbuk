"""
Leave-one-out evaluation of the rules+dict stage against hand-corrected
gold pages in data/{project}/page_final/.

For each gold page P:
  - build a dictionary from the OTHER gold pages (NFC-normalized)
  - vocalize the raw page/{P} with rules+dict
  - compare token-by-token against page_final/{P}

Writes per-token outputs to artifacts/eval_vs_gold/ and prints a summary.
Does NOT touch page_final/.
"""

import logging
import unicodedata
from collections import Counter
from pathlib import Path
from lxml import etree

import vocalize_from_reference as vr
from rules import WORD_RE, NIKKUD_RE, strip_nikkud

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

import argparse

repo_root = Path(__file__).resolve().parents[2]
out_dir = Path(__file__).parent / "artifacts" / "eval_vs_gold"
out_dir.mkdir(parents=True, exist_ok=True)

project = "Yudale_der_blinder,_Emkroyt1908"
raw_dir = repo_root / "data" / project / "page"
gold_dir = repo_root / "data" / project / "page_final"
GOLD_PAGES: list[str] = []


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def line_tokens(tree):
    out = []
    for el in vr.iter_unicode(tree):
        if el.text:
            out.append(WORD_RE.findall(nfc(el.text)))
        else:
            out.append([])
    return out


def eval_page(held: str) -> dict:
    others = [etree.parse(str(gold_dir / p)) for p in GOLD_PAGES if p != held]
    vocab = vr.build_dictionary(others)
    vr.learn_yii_preferences(vocab.values())

    raw_tree = etree.parse(str(raw_dir / held))
    gold_tree = etree.parse(str(gold_dir / held))

    # Snapshot raw tokens BEFORE in-place mutation by vocalize_line.
    raw_tokens = line_tokens(raw_tree)

    stats: Counter = Counter()
    predicted_lines = []
    for el in vr.iter_unicode(raw_tree):
        if el.text:
            trace: list = []
            new = vr.vocalize_line(nfc(el.text), vocab, stats, trace)
            el.text = new
            predicted_lines.append(new)
        else:
            predicted_lines.append("")

    pred_tokens = [WORD_RE.findall(l) for l in predicted_lines]
    gold_tokens = line_tokens(gold_tree)

    n = min(len(pred_tokens), len(gold_tokens), len(raw_tokens))
    needs_voc = 0      # gold token has nikkud
    correct = 0        # predicted == gold (NFC)
    correct_consonants = 0  # at least we didn't change consonants
    raw_already = 0    # raw already matched gold (free win)
    missed_dict = []   # gold-vocalized tokens we left bare or wrong

    for i in range(n):
        if len(pred_tokens[i]) != len(gold_tokens[i]) or len(raw_tokens[i]) != len(gold_tokens[i]):
            continue
        for r, p, g in zip(raw_tokens[i], pred_tokens[i], gold_tokens[i]):
            r, p, g = nfc(r), nfc(p), nfc(g)
            if strip_nikkud(p) != strip_nikkud(g):
                continue  # consonant divergence, OCR concern not vocab
            if not NIKKUD_RE.search(g):
                continue  # gold left bare; not a vocalization decision
            needs_voc += 1
            if r == g:
                raw_already += 1
            if p == g:
                correct += 1
                correct_consonants += 1
            else:
                correct_consonants += 1
                missed_dict.append((r, p, g))

    raw_xml_out = out_dir / f"{held.replace('.xml','')}_pred.xml"
    raw_tree.write(str(raw_xml_out), encoding="UTF-8",
                   xml_declaration=True, standalone=True)

    return {
        "page": held,
        "vocab_size": len(vocab),
        "stats": dict(stats),
        "needs_voc": needs_voc,
        "correct": correct,
        "raw_already_correct": raw_already,
        "missed_examples": missed_dict[:15],
        "missed_total": len(missed_dict),
    }


def main():
    global project, raw_dir, gold_dir, GOLD_PAGES
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=project,
                    help="folder name under data/ (default: Yudale)")
    ap.add_argument("--gold-pages", nargs="*",
                    help="restrict gold to these page filenames (default: all in page_final/)")
    args = ap.parse_args()
    project = args.project
    raw_dir = repo_root / "data" / project / "page"
    gold_dir = repo_root / "data" / project / "page_final"
    all_gold = sorted(p.name for p in gold_dir.glob("*.xml"))
    if args.gold_pages:
        GOLD_PAGES = [p for p in all_gold if p in args.gold_pages]
    else:
        GOLD_PAGES = all_gold
    print(f"Gold pages: {GOLD_PAGES}")
    print()
    grand = Counter()
    for held in GOLD_PAGES:
        r = eval_page(held)
        grand["needs_voc"] += r["needs_voc"]
        grand["correct"] += r["correct"]
        grand["raw_already"] += r["raw_already_correct"]
        pct = 100 * r["correct"] / r["needs_voc"] if r["needs_voc"] else 0
        free = 100 * r["raw_already_correct"] / r["needs_voc"] if r["needs_voc"] else 0
        print(f"{r['page']}  vocab={r['vocab_size']:3}  "
              f"needs_voc={r['needs_voc']:4}  "
              f"pred_correct={r['correct']:4} ({pct:5.1f}%)  "
              f"raw_already={r['raw_already_correct']:4} ({free:5.1f}%)")
        if r["missed_examples"]:
            print(f"   misses (showing {len(r['missed_examples'])} / {r['missed_total']}):")
            for raw, pred, gold in r["missed_examples"]:
                print(f"     raw={raw:14}  pred={pred:14}  gold={gold}")
        print()
    tot_pct = 100 * grand["correct"] / grand["needs_voc"]
    tot_free = 100 * grand["raw_already"] / grand["needs_voc"]
    print(f"OVERALL  needs_voc={grand['needs_voc']}  "
          f"correct={grand['correct']} ({tot_pct:.1f}%)  "
          f"raw_already={grand['raw_already']} ({tot_free:.1f}%)")


if __name__ == "__main__":
    main()
