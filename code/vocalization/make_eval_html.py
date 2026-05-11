"""
Build an evaluation HTML view of page 7's final vocalized output (rules +
dict + Claude). Each token is colored by which stage produced its current
form, so a reviewer can scan the page and quickly assess where to focus
attention — especially the Claude additions and the still-untouched tokens.
"""

import json
import logging
from pathlib import Path
from lxml import etree

from rules import WORD_RE, NIKKUD_RE, strip_nikkud, speaker_span

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"

repo_root = Path(__file__).resolve().parents[2]
project = "Yudale_der_blinder,_Emkroyt1908"
page_dir = repo_root / "data" / project / "page"
orig_path  = page_dir / "0007_OTgwNjYwMDE.111007895.xml"
rule_path  = page_dir / "0007_OTgwNjYwMDE.111007895_vocalized.xml"
llm_path   = page_dir / "0007_OTgwNjYwMDE.111007895_vocalized_llm.xml"
suspect_path = Path(__file__).parent / "page7_ocr_suspects.json"
out_path   = Path(__file__).parent / "page7_eval.html"


def line_texts(tree):
    out = []
    for el in tree.iter(f"{{{PAGE_NS}}}Unicode"):
        gp = el.getparent().getparent() if el.getparent() is not None else None
        if gp is not None and gp.tag == LINE_TAG:
            out.append(el.text or "")
    return out


def classify_token(orig_tok, rule_tok, llm_tok, in_speaker_span: bool) -> str:
    """Decide which stage owns the final form of this token."""
    if in_speaker_span:
        return "speaker"
    orig_has = bool(NIKKUD_RE.search(orig_tok))
    rule_has = bool(NIKKUD_RE.search(rule_tok))
    llm_has  = bool(NIKKUD_RE.search(llm_tok))
    if orig_has:
        return "source"
    if rule_has:
        # Rule stage added nikkud. Distinguish dict vs rule by checking
        # whether the rule output came verbatim from page 6 — we don't
        # have that info here, so fold them together as "rule_dict".
        return "rule_dict"
    if llm_has:
        return "claude"
    return "untouched"


def render_line(orig_line, rule_line, llm_line) -> tuple[str, dict]:
    """Walk tokens in parallel and produce HTML for the LLM line, plus a
    per-class count for stats. Assumes the three lines have identical token
    sequences (same consonants, possibly different nikkud)."""
    sp = speaker_span(orig_line)
    orig_tokens = list(WORD_RE.finditer(orig_line))
    rule_tokens = list(WORD_RE.finditer(rule_line))
    llm_tokens  = list(WORD_RE.finditer(llm_line))
    if not (len(orig_tokens) == len(rule_tokens) == len(llm_tokens)):
        # Token counts diverged — render LLM line bare.
        return f'<span class="warn">{llm_line}</span>', {"warn": 1}

    counts: dict = {}
    out: list[str] = []
    cursor = 0  # position in the LLM line
    for o_m, r_m, l_m in zip(orig_tokens, rule_tokens, llm_tokens):
        # Emit the inter-token slice from the LLM line as-is
        out.append(llm_line[cursor:l_m.start()])
        in_sp = bool(sp and o_m.start() >= sp[0] and o_m.end() <= sp[1])
        cls = classify_token(o_m.group(0), r_m.group(0), l_m.group(0), in_sp)
        counts[cls] = counts.get(cls, 0) + 1
        out.append(f'<span class="{cls}">{l_m.group(0)}</span>')
        cursor = l_m.end()
    out.append(llm_line[cursor:])
    return "".join(out), counts


def main():
    orig = line_texts(etree.parse(str(orig_path)))
    rule = line_texts(etree.parse(str(rule_path)))
    llm  = line_texts(etree.parse(str(llm_path)))
    assert len(orig) == len(rule) == len(llm), \
        f"line-count mismatch: {len(orig)} {len(rule)} {len(llm)}"

    rendered_lines = []
    totals: dict = {}
    for i, (o, r, l) in enumerate(zip(orig, rule, llm), 1):
        html, cnts = render_line(o, r, l)
        rendered_lines.append(f'<div class="line"><span class="lineno">{i}</span>{html}</div>')
        for k, v in cnts.items():
            totals[k] = totals.get(k, 0) + v

    suspects = []
    if suspect_path.exists():
        try:
            suspects = json.loads(suspect_path.read_text())
        except Exception:
            suspects = []

    css = """
    body { font-family: 'SBL Hebrew','Times New Roman',serif; padding:1em;
           background:#fafafa; max-width:920px; margin:0 auto; }
    .line { padding:8px 0; border-bottom:1px solid #eee; font-size:19px;
            line-height:1.7; direction:rtl; text-align:right; }
    .lineno { direction:ltr; color:#aaa; font-size:11px; font-family:monospace;
              float:left; width:30px; text-align:right; padding-top:4px; }
    .speaker  { color:#888; }
    .source   { color:#333; }
    .rule_dict{ color:#1a7f37; }
    .claude   { color:#8250df; font-weight:600;
                background:#f3e8ff; border-radius:2px; padding:0 3px; }
    .untouched{ color:#cf222e; background:#ffebe9; border-radius:2px; padding:0 3px; }
    .warn { color:#a40e26; background:#ffe9e9; }
    h2,h3,.legend,.summary,.suspects { font-family:sans-serif; direction:ltr;
                                        text-align:left; }
    .legend { font-size:14px; color:#555; margin-bottom:1em; }
    .summary { background:#fff; border:1px solid #ddd; padding:.8em 1em;
               margin-bottom:1em; font-size:14px; }
    .suspects { background:#fff8c5; border:1px solid #d4a72c; padding:1em;
                margin-top:2em; }
    .suspects pre { direction:rtl; font-family:'SBL Hebrew',serif;
                    white-space:pre-wrap; margin:.4em 0; }
    """
    legend = (
        '<span class="rule_dict">■ rules + page-6 dictionary</span> · '
        '<span class="claude">■ Claude (LLM)</span> · '
        '<span class="source">■ already vocalized in source</span> · '
        '<span class="speaker">■ speaker</span> · '
        '<span class="untouched">■ still bare</span>'
    )
    total = sum(totals.values())
    summary_rows = "".join(
        f'<tr><td>{k}</td><td style="text-align:right">{v}</td>'
        f'<td style="text-align:right">{v/total*100:.1f}%</td></tr>'
        for k, v in sorted(totals.items(), key=lambda kv: -kv[1])
    )
    summary_html = (
        f'<div class="summary"><b>Coverage on {total} tokens</b>'
        f'<table style="margin-top:.4em">'
        f'<tr><th align="left">stage</th><th>count</th><th>%</th></tr>'
        f'{summary_rows}</table></div>'
    )

    suspects_html = ""
    if suspects:
        items = []
        for s in suspects:
            items.append(
                f'<div><b>{s["token"]}</b> → <b>{s["claude_output"]}</b> '
                f'<span style="color:#666">({s.get("reason","")})</span>'
                f'<pre>{s["line"]}</pre></div>'
            )
        suspects_html = (
            '<div class="suspects"><h3>Claude round-trip flags '
            '(consonants changed — needs human review)</h3>'
            + "".join(items) + '</div>'
        )

    html = f"""<!doctype html><html lang="he"><head><meta charset="utf-8">
<title>Page 7 — evaluation</title><style>{css}</style></head><body>
<h2>Page 7 — vocalization evaluation</h2>
<div class="legend">{legend}</div>
{summary_html}
{''.join(rendered_lines)}
{suspects_html}
</body></html>"""
    out_path.write_text(html, encoding="utf-8")
    log.info(f"Wrote {out_path}")
    log.info(f"Totals: {totals}")


if __name__ == "__main__":
    main()
