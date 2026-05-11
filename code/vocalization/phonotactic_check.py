"""
Phonotactic / typo check on page 7. Identifies tokens whose consonants look
implausible for Yiddish — likely OCR errors the RA missed.

Default: Gemini 3 Pro alone (cheaper, tighter signal in our calibration run).
Use --both for the dual-model calibration mode (Claude + Gemini in parallel,
reporting agreement and disagreement separately).

Run:
  python phonotactic_check.py                  # gemini only (default)
  python phonotactic_check.py --single claude  # claude only
  python phonotactic_check.py --both           # calibration mode
"""

import argparse
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from lxml import etree
import anthropic
from google import genai
from google.genai import types as genai_types

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"

repo_root = Path(__file__).resolve().parents[2]
project = "Yudale_der_blinder,_Emkroyt1908"
page_dir = repo_root / "data" / project / "page"
target_path = page_dir / "0007_OTgwNjYwMDE.111007895.xml"
out_path = Path(__file__).parent / "page7_phonotactic_flags.json"

CLAUDE_MODEL = "claude-sonnet-4-6"
GEMINI_MODEL = "gemini-3-pro-preview"

PROMPT = """You are reviewing a page of Yiddish play text that has been
OCR'd from a 1908 print and partially corrected by a research assistant.
Your task: identify tokens whose CONSONANTS look implausible for Yiddish
— likely OCR errors that the RA missed. Do NOT flag tokens merely because
they lack nikkud or use unusual spelling — only flag tokens where the
letter sequence itself appears wrong (e.g. typos, scanning artifacts,
visually-confused letters, impossible Yiddish consonant clusters).

Output ONLY a JSON array. Each entry:
  {"token": "<as in input>",
   "suspected": "<your best guess at the intended word, or null>",
   "reason": "<short explanation>"}

If nothing looks wrong, return [].

TEXT:
"""


def page_text(tree) -> str:
    out = []
    for el in tree.iter(f"{{{PAGE_NS}}}Unicode"):
        gp = el.getparent().getparent() if el.getparent() is not None else None
        if gp is not None and gp.tag == LINE_TAG and el.text:
            out.append(el.text)
    return "\n".join(out)


def parse_json_array(text: str):
    t = text.strip()
    t = re.sub(r"^```(?:json)?\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    # Some models prepend prose; try to extract the first [...] block
    m = re.search(r"\[.*\]", t, flags=re.DOTALL)
    if m:
        t = m.group(0)
    return json.loads(t)


def ask_claude(text: str) -> list[dict]:
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": PROMPT + text}],
    )
    raw = resp.content[0].text
    log.info(f"claude usage: {resp.usage}")
    return parse_json_array(raw)


def ask_gemini(text: str) -> list[dict]:
    client = genai.Client()  # picks up GEMINI_API_KEY or GOOGLE_API_KEY
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=PROMPT + text,
        config=genai_types.GenerateContentConfig(
            response_mime_type="application/json",
        ),
    )
    log.info(f"gemini usage: {resp.usage_metadata}")
    return parse_json_array(resp.text)


def normalize_token(t: str) -> str:
    """Strip surrounding ASCII/Hebrew punctuation so that 'נאט'יניוּ' and
    'נאט'יניוּ.' compare equal."""
    return t.strip(".,;:!?׳״\"'") if isinstance(t, str) else t


def run(target_xml: Path, out_json: Path | None = None,
        mode: str = "gemini") -> dict:
    """mode: 'gemini', 'claude', or 'both'."""
    use_claude = mode in ("claude", "both")
    use_gemini = mode in ("gemini", "both")

    if use_claude and not os.environ.get("ANTHROPIC_API_KEY"):
        log.error("ANTHROPIC_API_KEY not set"); return {}
    if use_gemini and not (os.environ.get("GEMINI_API_KEY")
                           or os.environ.get("GOOGLE_API_KEY")):
        log.error("Neither GEMINI_API_KEY nor GOOGLE_API_KEY set"); return {}

    tree = etree.parse(str(target_xml))
    text = page_text(tree)

    claude_flags: list = []
    gemini_flags: list = []
    if use_claude and use_gemini:
        log.info(f"Sending {len(text)} chars to both models in parallel")
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_c = ex.submit(ask_claude, text)
            f_g = ex.submit(ask_gemini, text)
            try: claude_flags = f_c.result()
            except Exception as e: log.error(f"Claude failed: {e}")
            try: gemini_flags = f_g.result()
            except Exception as e: log.error(f"Gemini failed: {e}")
    elif use_claude:
        log.info(f"Sending {len(text)} chars to Claude")
        claude_flags = ask_claude(text)
    else:
        log.info(f"Sending {len(text)} chars to Gemini")
        gemini_flags = ask_gemini(text)

    def keys_of(flags):
        return {normalize_token(f["token"])
                for f in flags if isinstance(f, dict) and "token" in f}

    claude_keys = keys_of(claude_flags)
    gemini_keys = keys_of(gemini_flags)

    by_tok: dict = {}
    for f in claude_flags:
        if isinstance(f, dict) and "token" in f:
            by_tok.setdefault(normalize_token(f["token"]), {})["claude"] = f
    for f in gemini_flags:
        if isinstance(f, dict) and "token" in f:
            by_tok.setdefault(normalize_token(f["token"]), {})["gemini"] = f

    if use_claude and use_gemini:
        both = claude_keys & gemini_keys
        only_c = claude_keys - gemini_keys
        only_g = gemini_keys - claude_keys
        log.info(f"BOTH        ({len(both)}): {sorted(both)}")
        log.info(f"only Claude ({len(only_c)}): {sorted(only_c)}")
        log.info(f"only Gemini ({len(only_g)}): {sorted(only_g)}")
        summary = {"both_flagged": sorted(both),
                   "only_claude": sorted(only_c),
                   "only_gemini": sorted(only_g)}
    else:
        flagged = claude_keys | gemini_keys
        log.info(f"{len(flagged)} flag(s): {sorted(flagged)}")
        summary = {"flagged": sorted(flagged),
                   "model": "claude" if use_claude else "gemini"}

    output = {"summary": summary, "details": by_tok}
    if out_json is not None:
        out_json.write_text(json.dumps(output, ensure_ascii=False, indent=2),
                            encoding="utf-8")
        log.info(f"Wrote {out_json}")
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--single", choices=["claude", "gemini"],
                   help="Use only the named model (default: gemini)")
    g.add_argument("--both", action="store_true",
                   help="Calibration mode: Claude + Gemini in parallel")
    args = p.parse_args()
    mode = "both" if args.both else (args.single or "gemini")
    return run(target_path, out_path, mode=mode)


if __name__ == "__main__":
    main()
