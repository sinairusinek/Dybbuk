"""
Claude step: vocalize the tokens that the rule + dictionary pipeline left
untouched on page 7.

Inputs:
  - data/.../page/0007_..._vocalized.xml  (output of vocalize_from_reference.py)
  - page 6 (reference, cached in the system prompt)
  - rules inventory (in the system prompt)

Output:
  - data/.../page/0007_..._vocalized_llm.xml
  - per-token round-trip validation; tokens whose consonants change are
    NOT applied; they're written to a JSON sidecar as OCR-suspect candidates
    for human review.

Run:
  ANTHROPIC_API_KEY=... python claude_vocalize.py
"""

import json
import logging
import os
import re
from pathlib import Path

from lxml import etree
import anthropic

from rules import (
    WORD_RE, NIKKUD_RE, strip_nikkud, speaker_span,
    bracket_spans, in_any_span,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"

repo_root = Path(__file__).resolve().parents[2]
project = "Yudale_der_blinder,_Emkroyt1908"
page_dir = repo_root / "data" / project / "page"
ref_path = page_dir / "0006_OTgwNjYwMDE.111007894.xml"
in_path  = page_dir / "0007_OTgwNjYwMDE.111007895_vocalized.xml"
out_path = page_dir / "0007_OTgwNjYwMDE.111007895_vocalized_llm.xml"
suspect_path = Path(__file__).parent / "page7_ocr_suspects.json"

MODEL = "claude-opus-4-7"


def page_text(tree) -> str:
    """Concatenate all line-level Unicode texts as plain text."""
    out = []
    for el in tree.iter(f"{{{PAGE_NS}}}Unicode"):
        gp = el.getparent().getparent() if el.getparent() is not None else None
        if gp is not None and gp.tag == LINE_TAG and el.text:
            out.append(el.text)
    return "\n".join(out)


def collect_untouched(target_tree) -> list[dict]:
    """Find tokens still without nikkud (excluding speaker prefixes).
    Returns list of {line_index, line, token, span, key}."""
    items = []
    line_idx = 0
    for el in target_tree.iter(f"{{{PAGE_NS}}}Unicode"):
        gp = el.getparent().getparent() if el.getparent() is not None else None
        if gp is None or gp.tag != LINE_TAG or not el.text:
            continue
        line_idx += 1
        line = el.text
        sp = speaker_span(line)
        brackets = bracket_spans(line)
        for m in WORD_RE.finditer(line):
            tok = m.group(0)
            start, end = m.span()
            if sp and start >= sp[0] and end <= sp[1]:
                continue
            if in_any_span(start, end, brackets):
                continue  # stage directions: not vocalized
            if NIKKUD_RE.search(tok):
                continue
            # Hebrew letters only
            if not any("א" <= c <= "ת" for c in tok):
                continue
            items.append({
                "line_index": line_idx,
                "line": line,
                "token": tok,
                "span": (start, end),
                "key": strip_nikkud(tok),
            })
    return items


SYSTEM_INSTRUCTIONS = """You are a Yiddish vocalization assistant.
Your job: add nikkud (Hebrew vowel points) to bare Yiddish tokens, matching
the orthographic conventions of the supplied reference page.

Hard rules — never violate:
  • The output's consonantal letters MUST be identical to the input,
    letter-for-letter, in the same order. You add ONLY nikkud (U+05B0–U+05C7).
  • Speaker names are never vocalized.
  • Word-final ן and ם never carry nikkud.
  • Yods (single or in יי) never carry nikkud.
  • Second vav of a וו digraph is usually bare.

Strong defaults:
  • Consonant before ע → segol (ֶ).
  • Consonant before single י → hiriq (ִ); rarely holam for /oy/.
  • Consonant before יי → patah (ַ) for /ay/, tsere (ֵ) for /ey/ — pick by word.
  • Cluster-first sonorant (ר ל נ) → sheva (ְ).
  • ב in a vocalized word → dagesh (בּ); in loshn-koydesh /v/ contexts ב stays bare.
  • ש in a vocalized word → shin-dot (שׁ).

Output format: a JSON array of objects with keys "token" and "vocalized".
Output the array and nothing else.
"""


def build_messages(reference_text: str, items: list[dict]):
    """Build the system + user messages for Claude."""
    # Group items by line for context
    by_line = {}
    for it in items:
        by_line.setdefault(it["line_index"], (it["line"], []))[1].append(it["token"])
    payload_lines = []
    for ln, (line, toks) in sorted(by_line.items()):
        payload_lines.append({
            "line_index": ln,
            "line_context": line,
            "tokens_to_vocalize": toks,
        })

    system = [
        {"type": "text", "text": SYSTEM_INSTRUCTIONS},
        {
            "type": "text",
            "text": (
                "REFERENCE — page 6 of the same play, fully/partially vocalized "
                "by the same scribe. Use this as your style guide:\n\n"
                + reference_text
            ),
            "cache_control": {"type": "ephemeral"},
        },
    ]
    user = (
        "Vocalize the listed tokens. For each tokens_to_vocalize entry, "
        "output one JSON object {\"token\": <input>, \"vocalized\": <output>}. "
        "If you are uncertain about a token, return it unchanged.\n\n"
        + json.dumps(payload_lines, ensure_ascii=False, indent=2)
    )
    return system, user


def parse_claude_json(text: str) -> list[dict]:
    """Strip markdown code fences if present and parse JSON array."""
    t = text.strip()
    t = re.sub(r"^```(?:json)?\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    return json.loads(t)


def run(ref_xml: Path, in_xml: Path, out_xml: Path,
        suspect_json: Path | None = None) -> dict:
    """Run the Claude vocalization stage."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        log.error("ANTHROPIC_API_KEY not set; skipping Claude stage.")
        # Pass through unchanged so the pipeline can still complete.
        etree.parse(str(in_xml)).write(str(out_xml), encoding="UTF-8",
                                       xml_declaration=True, standalone=True)
        return {"applied": 0, "suspects": [], "skipped": True}

    ref_tree = etree.parse(str(ref_xml))
    target_tree = etree.parse(str(in_xml))

    reference_text = page_text(ref_tree)
    items = collect_untouched(target_tree)
    log.info(f"{len(items)} untouched token(s) to send to Claude")
    if not items:
        log.info("Nothing to do.")
        target_tree.write(str(out_xml), encoding="UTF-8",
                          xml_declaration=True, standalone=True)
        return {"applied": 0, "suspects": [], "skipped": False}

    system, user = build_messages(reference_text, items)
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    raw = resp.content[0].text
    log.info(f"usage: {resp.usage}")

    try:
        results = parse_claude_json(raw)
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse Claude output: {e}\n{raw[:500]}")
        return

    by_token = {r["token"]: r["vocalized"] for r in results if "token" in r}
    log.info(f"Claude returned {len(by_token)} mappings")

    applied = 0
    suspects = []
    for it in items:
        tok = it["token"]
        v = by_token.get(tok)
        if not v:
            continue
        # Round-trip check
        if strip_nikkud(v) != strip_nikkud(tok):
            suspects.append({
                "token": tok,
                "claude_output": v,
                "input_consonants": strip_nikkud(tok),
                "output_consonants": strip_nikkud(v),
                "line": it["line"],
                "reason": "consonant_change",
            })
            continue
        if v == tok:
            continue
        # Apply: replace this specific occurrence in the line
        for el in target_tree.iter(f"{{{PAGE_NS}}}Unicode"):
            gp = el.getparent().getparent() if el.getparent() is not None else None
            if gp is None or gp.tag != LINE_TAG or not el.text:
                continue
            # Replace by exact word-boundary safe regex
            new_text, n = re.subn(
                r"(?<![א-תְ-ׇ׳״'])" + re.escape(tok) + r"(?![א-תְ-ׇ׳״'])",
                v, el.text, count=1)
            if n:
                el.text = new_text
                applied += 1
                break

    log.info(f"Applied {applied} vocalizations; flagged {len(suspects)} OCR suspects")

    target_tree.write(str(out_xml), encoding="UTF-8",
                      xml_declaration=True, standalone=True)
    log.info(f"Wrote {out_xml}")
    if suspect_json is not None:
        suspect_json.write_text(
            json.dumps(suspects, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info(f"Wrote {suspect_json}")
    return {"applied": applied, "suspects": suspects, "skipped": False}


def main():
    return run(ref_path, in_path, out_path, suspect_path)


if __name__ == "__main__":
    main()
