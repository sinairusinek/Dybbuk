"""
End-to-end pipeline for one PAGE-XML file.

Stages:
  1. Rules + page-6 dictionary (vocalize_from_reference.run)
  2. Claude fill (claude_vocalize.run)
  3. Phonotactic check (phonotactic_check.run, default: Gemini only)
  4. Insert <unclear> annotations on TextLines for flagged tokens
  5. Write final XML to data/{project}/page_final/, ready for Transkribus upload

Usage:
  python pipeline.py                 # default: page 7, ref page 6
  python pipeline.py --page 8        # vocalize page 8 using page 6 as ref
  python pipeline.py --page 8 --ref 7
  python pipeline.py --no-llm        # rules+dict only, no API calls
  python pipeline.py --no-flags      # skip phonotactic check
"""

import argparse
import logging
from pathlib import Path
from lxml import etree

import vocalize_from_reference as v_rules
import claude_vocalize as v_llm
import phonotactic_check as v_phono
from unclear_tags import add_unclear_annotations
from ocr_flags import confusable_swap_scan
from rules import WORD_RE, strip_nikkud

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

repo_root = Path(__file__).resolve().parents[2]
DEFAULT_PROJECT = "Yudale_der_blinder,_Emkroyt1908"
# Yudale's page_final/ is the only fully hand-corrected gold; other plays use
# it as both the dictionary source and the Claude style reference.
GOLD_PROJECT = "Yudale_der_blinder,_Emkroyt1908"
GOLD_DIR = repo_root / "data" / GOLD_PROJECT / "page_final"
GOLD_STYLE_REF = "0006_OTgwNjYwMDE.111007894.xml"  # 97.7% gold page
artifacts_dir = Path(__file__).parent / "artifacts"


def project_dirs(project: str):
    return (
        repo_root / "data" / project / "page",
        repo_root / "data" / project / "page_final",
    )


def find_page(page_dir: Path, n: int) -> Path:
    """Locate the page XML for page number n by filename prefix '000N_'."""
    prefix = f"{n:04d}_"
    matches = sorted(page_dir.glob(f"{prefix}*.xml"))
    if not matches:
        raise FileNotFoundError(f"No page file matching {prefix}*.xml in {page_dir}")
    return matches[0]


def run_pipeline(target_n: int, ref_n: int, *,
                 project: str = DEFAULT_PROJECT,
                 use_llm: bool = True, use_flags: bool = True,
                 phono_mode: str = "gemini") -> Path:
    page_dir, final_dir = project_dirs(project)
    target_xml = find_page(page_dir, target_n)
    # For non-Yudale plays, no per-play reference exists — fall back to the
    # Yudale gold style page.
    if project == GOLD_PROJECT:
        ref_xml = find_page(page_dir, ref_n)
    else:
        ref_xml = GOLD_DIR / GOLD_STYLE_REF

    final_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    rules_xml = artifacts_dir / f"{target_xml.stem}_rules.xml"
    llm_xml   = artifacts_dir / f"{target_xml.stem}_llm.xml"
    final_xml = final_dir / target_xml.name
    flags_json = artifacts_dir / f"{target_xml.stem}_phono.json"
    suspect_json = artifacts_dir / f"{target_xml.stem}_claude_suspects.json"

    log.info(f"=== Stage 1: rules + dict (ref page {ref_n} → target page {target_n})")
    # Always source the dict from GOLD_DIR (Yudale's hand-corrected pages),
    # even when re-running on Yudale itself, since it's the only gold we have.
    v_rules.run(ref_xml, target_xml, rules_xml, gold_dir=GOLD_DIR)

    # Pre-existing nikkud that violates our rules: keep the marks (RA may
    # have a reason), but flag the token <unclear> for human review.
    rule_violators = v_rules.find_preexisting_rule_violations(
        etree.parse(str(target_xml)))
    if rule_violators:
        log.info(f"Pre-existing rule-violating tokens (will be tagged unclear): "
                 f"{len(rule_violators)} unique")
        for t in rule_violators[:10]:
            log.info(f"  {t}")

    if use_llm:
        log.info(f"=== Stage 2: Claude fill")
        v_llm.run(ref_xml, rules_xml, llm_xml, suspect_json)
        current_xml = llm_xml
    else:
        log.info(f"=== Stage 2: skipped (--no-llm)")
        current_xml = rules_xml

    flagged: list[str] = list(rule_violators)
    if use_flags:
        # 3a. Deterministic confusable-swap scan — free, catches recurring
        # OCR letter swaps (ה↔ח, נ↔ג etc.) by self-comparison within the page.
        log.info(f"=== Stage 3a: deterministic confusable-swap scan")
        target_tokens: list[str] = []
        for el in etree.parse(str(target_xml)).iter(
                f"{{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}}Unicode"):
            gp = el.getparent().getparent() if el.getparent() is not None else None
            if (gp is not None
                and gp.tag == f"{{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}}TextLine"
                and el.text):
                target_tokens.extend(WORD_RE.findall(el.text))
        det_flags = confusable_swap_scan(target_tokens)
        for f in det_flags:
            log.info(f"  {f['token']} ({f['rare_count']}×) → "
                     f"{f['suspected']} ({f['common_count']}×) "
                     f"swap {f['swap'][0]}↔{f['swap'][1]} @{f['swap_pos']}")
        flagged.extend(f["token"] for f in det_flags)

        # 3b. LLM phonotactic check on the residual.
        log.info(f"=== Stage 3b: phonotactic check ({phono_mode})")
        phono_out = v_phono.run(target_xml, flags_json, mode=phono_mode)
        if phono_mode == "both":
            flagged.extend(phono_out.get("summary", {}).get("both_flagged", []))
            flagged.extend(phono_out.get("summary", {}).get("only_claude", []))
            flagged.extend(phono_out.get("summary", {}).get("only_gemini", []))
        else:
            flagged.extend(phono_out.get("summary", {}).get("flagged", []))
        flagged = sorted(set(flagged))
        log.info(f"Total flags (deterministic + phonotactic): {len(flagged)}")
    else:
        log.info(f"=== Stage 3: skipped (--no-flags)")

    log.info(f"=== Stage 4: insert <unclear> annotations & write final XML")
    tree = etree.parse(str(current_xml))
    added = add_unclear_annotations(tree, flagged)
    log.info(f"Added {added} unclear annotation(s)")
    tree.write(str(final_xml), encoding="UTF-8",
               xml_declaration=True, standalone=True)
    log.info(f"Final XML: {final_xml}")
    return final_xml


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", default=DEFAULT_PROJECT,
                   help=f"Play directory under data/ (default: {DEFAULT_PROJECT})")
    p.add_argument("--page", type=int, default=None,
                   help="Page number to vocalize. Omit to process every page in the play.")
    p.add_argument("--ref", type=int, default=6,
                   help="Reference page (used only when --project is Yudale; "
                        "non-Yudale plays use the Yudale gold style page)")
    p.add_argument("--no-llm", action="store_true",
                   help="Skip the Claude vocalization step")
    p.add_argument("--no-flags", action="store_true",
                   help="Skip the phonotactic check / unclear-tag insertion")
    p.add_argument("--phono", choices=["claude", "gemini", "both"], default="both",
                   help="Model(s) for phonotactic check (default: both — Sonnet+Gemini3, union)")
    args = p.parse_args()

    page_dir, _ = project_dirs(args.project)
    if args.page is not None:
        pages = [args.page]
    else:
        pages = sorted(int(p.name[:4]) for p in page_dir.glob("[0-9][0-9][0-9][0-9]_*.xml"))
        log.info(f"Processing all {len(pages)} pages of {args.project}")

    for n in pages:
        try:
            run_pipeline(n, args.ref,
                         project=args.project,
                         use_llm=not args.no_llm,
                         use_flags=not args.no_flags,
                         phono_mode=args.phono)
        except Exception as e:
            log.error(f"Page {n} failed: {e}")


if __name__ == "__main__":
    main()
