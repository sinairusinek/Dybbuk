"""
Rules+dict-only batch vocalizer. No API calls.

Usage:
  python vocalize_dir.py --in <dir-of-page-xml> --out <dir>
  (dict source is the standard Yudale page_final/ gold)

Pre-existing nikkud that violates rules.py is preserved AND the line gets an
<unclear> annotation on that token.
"""

import argparse
import logging
from collections import Counter
from pathlib import Path
from lxml import etree

import vocalize_from_reference as vr
import dracor_dict as dd
from unclear_tags import add_unclear_annotations

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

repo_root = Path(__file__).resolve().parents[2]
DEFAULT_GOLD_DIR = repo_root / "data" / "Yudale_der_blinder,_Emkroyt1908" / "page_final"
DRACOR_TEIS = [
    (repo_root / "data" / "Yudale_der_blinder,_Emkroyt1908" /
        "yi000003-lateiner-yudale-der-blinder.tei.xml", False),
    (repo_root / "data" / "DerManUnterTiff" / "derMannDracor.xml", True),
]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in", dest="indir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--gold-dir", default=str(DEFAULT_GOLD_DIR),
                    help="directory of RA-corrected gold pages (default: Yudale page_final/)")
    ap.add_argument("--no-dracor", action="store_true",
                    help="skip the DraCor overlay stage")
    ap.add_argument("--vocalize-speakers-and-stage", action="store_true",
                    help="vocalize speaker names and parenthesized stage directions "
                         "(use for editions where RA does so, e.g. Das Yudishe Kind)")
    args = ap.parse_args()
    GOLD_DIR = Path(args.gold_dir)

    indir = Path(args.indir)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    # Build RA gold dict
    gold_trees = [etree.parse(str(p)) for p in sorted(GOLD_DIR.glob("*.xml"))]
    vocab = vr.build_dictionary(gold_trees)
    vr.learn_yii_preferences(vocab.values())
    log.info(f"RA dictionary: {len(vocab)} entries from {len(gold_trees)} gold pages")

    # Build DraCor overlay dict (filtered for editor quirks)
    dracor_vocab = {}
    if not args.no_dracor:
        existing = [(p, uses_rafe) for p, uses_rafe in DRACOR_TEIS if p.exists()]
        dracor_vocab = dd.tei_to_dict(existing)
        log.info(f"DraCor dictionary: {len(dracor_vocab)} entries from {len(existing)} TEI(s) "
                 f"(filtered: final-letter sheva, double-dagesh, sin-dot-as-holam; "
                 f"rafe stripped per edition flag)")

    pages = sorted(indir.glob("[0-9]*.xml"))
    log.info(f"Processing {len(pages)} pages from {indir}")

    total_stats = Counter()
    total_filled = total_conflicts = 0
    for p in pages:
        tree = etree.parse(str(p))
        rule_violators = vr.find_preexisting_rule_violations(tree)
        stats: Counter = Counter()
        for el in vr.iter_unicode(tree):
            if el.text:
                trace: list = []
                el.text = vr.vocalize_line(el.text, vocab, stats, trace,
                                           vocalize_speakers_and_stage=args.vocalize_speakers_and_stage)
        unclear_toks = list(rule_violators)
        if dracor_vocab:
            filled, conflicts, conflict_toks = dd.overlay_tree(
                tree, dracor_vocab, PAGE_NS)
            total_filled += filled
            total_conflicts += conflicts
            unclear_toks.extend(conflict_toks)
        if unclear_toks:
            add_unclear_annotations(tree, unclear_toks)
        out_path = outdir / p.name
        tree.write(str(out_path), encoding="UTF-8",
                   xml_declaration=True, standalone=True)
        for k, v in stats.items():
            total_stats[k] += v
    log.info(f"Done. Rules+dict stats: {dict(total_stats)}")
    if dracor_vocab:
        log.info(f"DraCor overlay: filled={total_filled}, conflicts(tagged unclear)={total_conflicts}")


if __name__ == "__main__":
    main()
