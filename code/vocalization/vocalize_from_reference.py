"""
Vocalize page 7 of Yudale_der_blinder using:

  1. Rule A: skip the speaker prefix of each line (line-initial up to ':').
  2. Dictionary lookup from page 6 (most-frequent vocalization wins).
  3. Default rules from rules.py for tokens unseen on page 6.
  4. Validator pass to log any rule violations on the final output.

Input:  data/Yudale_der_blinder,_Emkroyt1908/page/0006_*.xml  (reference)
        data/Yudale_der_blinder,_Emkroyt1908/page/0007_*.xml  (target)
Output: …/0007_*_vocalized.xml
"""

import logging
from pathlib import Path
from collections import Counter, defaultdict
from lxml import etree

from rules import (
    WORD_RE, NIKKUD_RE, SPEAKER_RE,
    strip_nikkud, apply_default_rules, validate, speaker_span,
    bracket_spans, in_any_span,
    learn_yii_preferences, YII_PRECEDING,
)
from ocr_flags import find_ocr_candidates, find_intra_doc_variants

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

repo_root = Path(__file__).resolve().parents[2]
project = "Yudale_der_blinder,_Emkroyt1908"
page_dir = repo_root / "data" / project / "page"
ref_path = page_dir / "0006_OTgwNjYwMDE.111007894.xml"
target_path = page_dir / "0007_OTgwNjYwMDE.111007895.xml"
out_path = page_dir / "0007_OTgwNjYwMDE.111007895_vocalized.xml"
diff_html_path = repo_root / "code" / "vocalization" / "page7_diff.html"


def iter_unicode(tree):
    """Yield only line-level <Unicode> elements (parent is TextEquiv whose
    parent is TextLine). PAGE-XML also stores a redundant region-level copy
    that may differ in OCR; we skip it to avoid duplication and inconsistency."""
    line_tag = f"{{{PAGE_NS}}}TextLine"
    for el in tree.iter(f"{{{PAGE_NS}}}Unicode"):
        gp = el.getparent().getparent() if el.getparent() is not None else None
        if gp is not None and gp.tag == line_tag:
            yield el


def build_dictionary(ref_tree) -> dict[str, str]:
    """consonant-key → most common vocalized form on page 6."""
    cands: dict[str, Counter] = defaultdict(Counter)
    for el in iter_unicode(ref_tree):
        if not el.text:
            continue
        for tok in WORD_RE.findall(el.text):
            key = strip_nikkud(tok)
            if key == tok:
                continue
            cands[key][tok] += 1
    return {k: c.most_common(1)[0][0] for k, c in cands.items()}


def vocalize_line(line: str, vocab: dict[str, str], stats: Counter, trace: list) -> str:
    """Tokenize a line; per token, decide skip / lookup / rule.
    Appends (orig, new, source) tuples to `trace`."""
    sp = speaker_span(line)
    brackets = bracket_spans(line)

    def replace(m):
        tok = m.group(0)
        start, end = m.span()
        if sp and start >= sp[0] and end <= sp[1]:
            # Speaker names are bare in this corpus; strip any nikkud the
            # source happened to include (matches RA editorial convention).
            bare = NIKKUD_RE.sub("", tok)
            stats["skip_speaker"] += 1
            trace.append((tok, bare, "speaker"))
            return bare
        if in_any_span(start, end, brackets):
            # Stage directions / parentheticals: not vocalized in this corpus.
            stats["skip_stage"] += 1
            trace.append((tok, tok, "stage"))
            return tok
        if NIKKUD_RE.search(tok):
            stats["already_vocalized"] += 1
            trace.append((tok, tok, "source"))
            return tok
        key = strip_nikkud(tok)
        if key in vocab:
            stats["dict_hit"] += 1
            trace.append((tok, vocab[key], "dict"))
            return vocab[key]
        ruled = apply_default_rules(tok)
        if ruled != tok:
            stats["rule_partial"] += 1
            trace.append((tok, ruled, "rule"))
            return ruled
        stats["untouched"] += 1
        trace.append((tok, tok, "untouched"))
        return tok

    return WORD_RE.sub(replace, line)


def render_line_html(orig_line: str, trace: list) -> str:
    """Re-tokenize the original line with WORD_RE; for each match, wrap with a
    colored span using the next trace entry. Preserves all punctuation /
    whitespace exactly."""
    queue = list(trace)
    def repl(m):
        if not queue:
            return m.group(0)
        o, n, src = queue.pop(0)
        changed = " changed" if o != n else ""
        return f'<span class="{src}{changed}">{n}</span>'
    return WORD_RE.sub(repl, orig_line)


def write_html_diff(line_pairs, traces_by_line, violations,
                    ocr_suspects, intra_variants, path):
    css = """
    body { font-family: 'SBL Hebrew', 'Times New Roman', serif; padding: 1em;
           background:#fafafa; max-width: 900px; margin: 0 auto; }
    .line { padding: 8px 0; border-bottom: 1px solid #eee;
            font-size: 19px; line-height: 1.7; direction: rtl; text-align: right; }
    .lineno { direction: ltr; color:#aaa; font-size:11px;
              font-family: monospace; float: left; width: 30px;
              text-align: right; padding-top: 4px; }
    .speaker { color: #888; }
    .source  { color: #333; }
    .dict    { color: #1a7f37; }
    .rule    { color: #0969da; }
    .untouched { color: #cf222e; }
    .changed { background: #fff8c5; border-radius: 2px; padding: 0 2px; }
    h2, .legend, .violations h3 { font-family: sans-serif; direction: ltr; text-align: left; }
    .legend { font-size: 14px; color: #555; margin-bottom: 1em; }
    .violations { font-family: monospace; background:#fff; padding: 1em;
                  border:1px solid #ddd; direction: rtl; margin-top: 2em; }
    """
    rows = []
    for ln, ((orig, _new), trace) in enumerate(zip(line_pairs, traces_by_line), 1):
        rendered = render_line_html(orig, trace)
        rows.append(f'<div class="line"><span class="lineno">{ln}</span>{rendered}</div>')
    legend = ('<span class="dict">■ dict</span> · '
              '<span class="rule">■ rule</span> · '
              '<span class="source">■ source-vocalized</span> · '
              '<span class="speaker">■ speaker</span> · '
              '<span class="untouched">■ untouched</span>')
    viol_html = ""
    if violations:
        viol_html = "<div class='violations'><h3>Validator violations</h3>"
        viol_html += "<br>".join(f"{tok} — {v}" for tok, v in violations)
        viol_html += "</div>"
    iv_html = ""
    if intra_variants:
        rows_v = []
        for v in intra_variants:
            star = "★" if v["confusable_pair"] else ""
            rows_v.append(
                f"<tr><td>{star}</td>"
                f"<td>{v['rare']} ({v['rare_count']}×)</td>"
                f"<td>{v['common']} ({v['common_count']}×)</td>"
                f"<td>{v['diff_chars'][0]} ↔ {v['diff_chars'][1]}</td></tr>"
            )
        iv_html = (
            "<div class='violations'><h3>Intra-document spelling variants "
            "(rare form on left; suspect of uncaught error)</h3>"
            "<table><tr><th></th><th>rare</th><th>common</th><th>diff</th></tr>"
            + "".join(rows_v) + "</table></div>"
        )
    ocr_html = ""
    if ocr_suspects:
        rows_o = []
        for tok, c in ocr_suspects:
            star = "★" if c["confusable_pair"] else ""
            rows_o.append(
                f"<tr><td>{star}</td><td>{tok}</td><td>{c['ref']}</td>"
                f"<td>{c['diff_chars'][0]} ↔ {c['diff_chars'][1]}</td></tr>"
            )
        ocr_html = (
            "<div class='violations'><h3>Possible OCR/HTR errors "
            "(★ = confusable letter pair)</h3>"
            "<table><tr><th></th><th>token</th><th>page-6 word</th>"
            "<th>diff</th></tr>"
            + "".join(rows_o) + "</table></div>"
        )
    html = f"""<!doctype html><html lang="he"><head><meta charset="utf-8">
<title>Page 7 vocalization</title><style>{css}</style></head><body>
<h2>Page 7 — vocalization</h2>
<div class="legend">{legend}</div>
{''.join(rows)}
{viol_html}
{iv_html}
{ocr_html}
</body></html>"""
    path.write_text(html, encoding="utf-8")


def run(ref_xml: Path, target_xml: Path, out_xml: Path,
        diff_html: Path | None = None) -> dict:
    """Run the rule + dictionary stage. Returns stats and OCR signals."""
    log.info(f"Reference: {ref_xml.name}")
    log.info(f"Target:    {target_xml.name}")

    ref_tree = etree.parse(str(ref_xml))
    target_tree = etree.parse(str(target_xml))

    vocab = build_dictionary(ref_tree)
    log.info(f"Dictionary: {len(vocab)} entries from page 6")

    # Learn lexical patah-vs-tsere choice for consonants before יי
    learn_yii_preferences(vocab.values())
    log.info(f"YII_PRECEDING learned: {YII_PRECEDING}")

    stats = Counter()
    line_pairs = []
    traces_by_line = []
    for el in iter_unicode(target_tree):
        if el.text:
            orig = el.text
            trace: list = []
            new = vocalize_line(orig, vocab, stats, trace)
            el.text = new
            line_pairs.append((orig, new))
            traces_by_line.append(trace)

    total = sum(stats.values())
    log.info(f"Token outcomes on page 7 (total {total}):")
    for k in ("skip_speaker", "skip_stage", "dict_hit", "rule_partial", "already_vocalized", "untouched"):
        n = stats[k]
        pct = (n / total * 100) if total else 0
        log.info(f"  {k:18s}  {n:4d}  {pct:5.1f}%")

    # Validator pass
    violations = []
    for el in iter_unicode(target_tree):
        if not el.text:
            continue
        for tok in WORD_RE.findall(el.text):
            for v in validate(tok):
                violations.append((tok, v))
    log.info(f"Validator: {len(violations)} violation(s)")
    for tok, v in violations[:15]:
        log.info(f"  {tok}: {v}")

    # OCR-flag pass: scan untouched tokens for 1-edit matches against page-6 vocab.
    ocr_suspects = []
    seen = set()
    vocab_keys = set(vocab.keys())
    for trace in traces_by_line:
        for orig, new, src in trace:
            if src != "untouched":
                continue
            key = strip_nikkud(orig)
            if key in seen:
                continue
            seen.add(key)
            cands = find_ocr_candidates(orig, vocab_keys)
            if cands:
                ocr_suspects.append((orig, cands[0]))
    log.info(f"OCR suspects (untouched, 1-edit from page-6 word): {len(ocr_suspects)}")
    for tok, c in ocr_suspects:
        marker = "★" if c["confusable_pair"] else " "
        log.info(f"  {marker} {tok}  vs  {c['ref']}  "
                 f"(diff @{c['diff_pos']}: {c['diff_chars'][0]}↔{c['diff_chars'][1]})")

    # Intra-document variant detection: rare spelling vs. common spelling
    # in the same RA-corrected page. Targets errors the RA missed.
    all_target_tokens = []
    for el in iter_unicode(target_tree):
        if el.text:
            all_target_tokens.extend(WORD_RE.findall(el.text))
    intra_variants = find_intra_doc_variants(all_target_tokens)
    log.info(f"Intra-document variants on page 7: {len(intra_variants)}")
    for v in intra_variants:
        marker = "★" if v["confusable_pair"] else " "
        log.info(f"  {marker} {v['rare']} ({v['rare_count']}×) vs "
                 f"{v['common']} ({v['common_count']}×) "
                 f"@{v['diff_pos']}: {v['diff_chars'][0]}↔{v['diff_chars'][1]}")

    target_tree.write(str(out_xml), encoding="UTF-8",
                      xml_declaration=True, standalone=True)
    log.info(f"Wrote {out_xml}")

    if diff_html is not None:
        write_html_diff(line_pairs, traces_by_line, violations,
                        ocr_suspects, intra_variants, diff_html)
        log.info(f"Wrote diff viewer {diff_html}")

    return {
        "stats": dict(stats),
        "violations": violations,
        "ocr_suspects": ocr_suspects,
        "intra_variants": intra_variants,
    }


def main():
    return run(ref_path, target_path, out_path, diff_html_path)


if __name__ == "__main__":
    main()
