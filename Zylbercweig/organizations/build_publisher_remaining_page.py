"""Build the review surface for the publishers the audit has NOT yet decided.

The first publisher audit (publisher_audit.html) closed 176 decisions. This is
its sequel: the 192 publisher-like clusters in org_alignment_review.tsv that
still carry no decision, each with a recommendation and the reason for it.

Reads origin/main's data, not the working tree, because the deployed Zalmen app
pushes core_db and the review ledger continuously and a local checkout is
routinely hundreds of commits behind.

The finding that shapes the page: NONE of these 192 has a candidate above 0.75,
and only one scores >=0.80 against any live publisher row — the Riga/Warsaw
Arbeter Heim pair that this audit already rejected on evidence. The alignable
ones are gone. What remains is a minting job, and the page is organised to make
that fast rather than to re-litigate matches.

Usage: python3 build_publisher_remaining_page.py [--artifact]
"""
from __future__ import annotations

import argparse
import collections
import csv
import html
import re
import subprocess
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
RECS = HERE / "publisher_remaining_recommendations.tsv"
OUT = REPO / "docs" / "Visualizations" / "publisher_remaining.html"
OUT_ARTIFACT = (REPO / "docs" / "Visualizations" / "artifact"
                / "publisher_remaining.body.html")

HEBREW_RE = re.compile(r"[֐-׿יִ-ﭏ]")

# Each group says what the reviewer is being asked to do and why it is safe.
GROUPS = [
    ("MINT ONCE (pair)", "Two clusters, one firm", "pair",
     "Same name, same city, no existing row. Mint a single org and attach both "
     "clusters — the matcher kept them apart only because it compares strings, "
     "not businesses."),
    ("MINT SEPARATELY", "Same name, different cities", "split",
     "A _Q suffix marks a split the pipeline made deliberately, by place. The "
     "names match exactly and that is not evidence: this audit has already "
     "refused three such pairs on research (Kultur Minsk/Warsaw, Arbeter Heim "
     "Warsaw/Riga, Yavneh Warsaw/Tel Aviv). Mint each separately unless a "
     "source shows the firm moved."),
    ("RETYPE / NOT_AN_ORG", "Not a publishing house", "retype",
     "These read as something else — a record label, a work's title, a fund or "
     "a union. Give them the right org_type, or mark NOT_AN_ORG where the "
     "string names a book rather than a body. Minting them would repeat the "
     "mistake the first audit spent 24 decisions undoing."),
    ("MINT", "No row exists", "mint",
     "A publishing house attested in the corpus with nothing in core_db to "
     "align to, and no candidate worth weighing. These are the bulk of the "
     "work: mint, then attach."),
]

STATE = {"pair": "mint one, link two", "split": "mint each",
         "retype": "type, don't mint", "mint": "mint and attach"}


def esc(s: str) -> str:
    return html.escape(s or "", quote=True)


def yi(s: str) -> str:
    if not s:
        return ""
    return (f'<span class="yi">{esc(s)}</span>' if HEBREW_RE.search(s)
            else f'<span class="lat">{esc(s)}</span>')


def git_show(path: str) -> str:
    return subprocess.run(["git", "show", f"origin/main:{path}"], cwd=REPO,
                          capture_output=True, text=True, check=True).stdout


def main(artifact: bool = False) -> None:
    rows = list(csv.DictReader(RECS.open(encoding="utf-8"), delimiter="\t"))
    by_rec = collections.defaultdict(list)
    for r in rows:
        by_rec[r["recommendation"]].append(r)

    cities = collections.Counter(r["city"] for r in rows)
    multi = sum(1 for r in rows if (r["mentions"] or "0").isdigit()
                and int(r["mentions"]) > 1)

    # Corpus context, read from main so the counts are the live ones.
    import io
    db = [r for r in csv.DictReader(
        io.StringIO(git_show("Zylbercweig/organizations/core_db.tsv")),
        delimiter="\t")]
    PT = re.compile(r"publish|printer|print", re.I)
    livepub = [r for r in db if PT.search(r.get("org_type") or "")
               and (r.get("deprecated") or "").strip().lower() != "true"
               and (r.get("out_of_project") or "").strip().lower() != "true"]

    head = (f"""<title>Publishers still undecided</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>""" if artifact else """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Publishers still undecided</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>""")

    parts = [head, f"""
:root{{
  --paper:#faf7f0; --ink:#272219; --soft:#5c554a; --line:#e2dcce; --card:#f2eddf;
  --teal:#0e6f8a; --brick:#a1462e; --amber:#b07d2b; --green:#2f8f7f; --sunk:#f5f1e4;
}}
@media (prefers-color-scheme: dark){{
  :root:not([data-theme="light"]){{
    --paper:#1a1714; --ink:#ece5d6; --soft:#a79e8d; --line:#36302a; --card:#221e19;
    --teal:#5bb3cc; --brick:#dd8468; --amber:#d4a04a; --green:#5cbfa8; --sunk:#1f1b16;
  }}
}}
:root[data-theme="dark"]{{
  --paper:#1a1714; --ink:#ece5d6; --soft:#a79e8d; --line:#36302a; --card:#221e19;
  --teal:#5bb3cc; --brick:#dd8468; --amber:#d4a04a; --green:#5cbfa8; --sunk:#1f1b16;
}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--paper);color:var(--ink);
  font-family:"Spectral",Georgia,serif;font-size:15px;line-height:1.55}}
.wrap{{max-width:76rem;margin:0 auto;padding:0 1.6rem}}
header{{padding:2.2rem 0 .4rem}}
.eyebrow{{font-family:"IBM Plex Mono",monospace;font-size:.68rem;letter-spacing:.13em;
  text-transform:uppercase;color:var(--teal);margin:0}}
h1{{font-family:"Fraunces",serif;font-weight:600;font-size:2.3rem;margin:.1rem 0 .45rem;
  text-wrap:balance}}
.lede{{color:var(--soft);margin:.2rem 0 .5rem;max-width:44rem;font-size:1.02rem}}
.yi{{font-family:"Frank Ruhl Libre","Spectral",serif;unicode-bidi:isolate}}
.lat{{unicode-bidi:isolate}}
.band{{display:grid;grid-template-columns:repeat(auto-fit,minmax(11rem,1fr));
  gap:0;border:1px solid var(--line);border-radius:5px;overflow:hidden;
  margin:1.4rem 0 .6rem;background:var(--card)}}
.band div{{padding:.85rem 1rem;border-right:1px solid var(--line)}}
.band div:last-child{{border-right:0}}
.band .k{{font-family:"IBM Plex Mono",monospace;font-size:.63rem;letter-spacing:.09em;
  text-transform:uppercase;color:var(--soft);margin:0 0 .25rem}}
.band .v{{font-family:"Fraunces",serif;font-size:1.65rem;font-weight:600;line-height:1;
  font-variant-numeric:tabular-nums}}
.band .sub{{font-size:.78rem;color:var(--soft);margin-top:.2rem}}
.note{{font-size:.88rem;color:var(--soft);max-width:47rem;margin:.9rem 0 2rem}}
.note code{{font-family:"IBM Plex Mono",monospace;font-size:.78rem;
  background:var(--card);padding:.08rem .3rem;border-radius:3px}}
.note b{{color:var(--ink);font-weight:600}}
h2{{font-family:"Fraunces",serif;font-weight:600;font-size:1.2rem;
  margin:2.6rem 0 .2rem;padding-bottom:.4rem;border-bottom:1px solid var(--line);
  display:flex;align-items:baseline;gap:.6rem;flex-wrap:wrap}}
h2 .n{{font-family:"IBM Plex Mono",monospace;font-size:.68rem;color:var(--soft);
  font-weight:400}}
.state{{font-family:"IBM Plex Mono",monospace;font-size:.6rem;letter-spacing:.08em;
  text-transform:uppercase;border:1px solid currentColor;border-radius:3px;
  padding:.12rem .4rem;font-weight:500}}
.state.pair{{color:var(--green)}} .state.split{{color:var(--brick)}}
.state.retype{{color:var(--amber)}} .state.mint{{color:var(--teal)}}
.sect-note{{color:var(--soft);font-size:.88rem;margin:.6rem 0 1.1rem;max-width:47rem}}
.scroll{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:.9rem;
  border:1px solid var(--line);border-radius:4px;overflow:hidden}}
th{{font-family:"IBM Plex Mono",monospace;font-size:.62rem;letter-spacing:.09em;
  text-transform:uppercase;color:var(--soft);text-align:left;font-weight:500;
  padding:.5rem .8rem;background:var(--sunk);border-bottom:1px solid var(--line)}}
td{{padding:.42rem .8rem;border-bottom:1px solid var(--line);vertical-align:baseline}}
tr:last-child td{{border-bottom:0}}
tbody tr:nth-child(even){{background:var(--card)}}
.id{{font-family:"IBM Plex Mono",monospace;font-size:.7rem;color:var(--soft);
  white-space:nowrap}}
.nm{{overflow-wrap:anywhere;min-width:12rem}}
.city{{color:var(--soft);font-size:.84rem;white-space:nowrap}}
.n{{font-family:"IBM Plex Mono",monospace;font-variant-numeric:tabular-nums;
  font-size:.78rem;text-align:right}}
.why{{color:var(--soft);font-size:.82rem}}
footer{{margin:3rem 0 3rem;padding-top:1rem;border-top:1px solid var(--line);
  color:var(--soft);font-size:.82rem}}
footer code{{font-family:"IBM Plex Mono",monospace;font-size:.76rem;
  background:var(--card);padding:.08rem .3rem;border-radius:3px}}
a{{color:var(--teal)}}
</style>{'' if artifact else '</head><body>'}
<div class="wrap">
<header>
<p class="eyebrow">Dybbuk · Zylbercweig organizations</p>
<h1>Publishers still undecided</h1>
<p class="lede">The sequel to the publisher audit. Those 176 decisions are
merged; these {len(rows)} publisher-like clusters still carry none, and each one
here has a recommendation and the reason for it.</p>
</header>

<div class="band">
 <div><p class="k">undecided clusters</p><p class="v">{len(rows)}</p>
  <p class="sub">publisher-like, no decision</p></div>
 <div><p class="k">live publisher rows</p><p class="v">{len(livepub)}</p>
  <p class="sub">on origin/main today</p></div>
 <div><p class="k">alignable to a row</p><p class="v">1</p>
  <p class="sub">and it is already rejected</p></div>
 <div><p class="k">needing a new row</p><p class="v">{len(by_rec['MINT'])+len(by_rec['MINT SEPARATELY'])+len(by_rec['MINT ONCE (pair)'])}</p>
  <p class="sub">across {len([c for c in cities if c!='—'])} places</p></div>
 <div><p class="k">more than one mention</p><p class="v">{multi}</p>
  <p class="sub">the rest are singletons</p></div>
</div>
<p class="note"><b>The alignable publishers are gone.</b> Not one of these 192
has a candidate scoring above 0.75, and only one reaches 0.80 against any live
publisher row — <span class="yi">אַרבעטער היים</span>, the Riga cluster whose
Warsaw namesake this audit already refused on evidence. So this is a minting
job, not a matching one, and the page is ordered to make that quick. The
reasoning behind each group is the first audit's, carried forward: a shared name
across two cities is not evidence of one firm, and a string that names a book or
a fund is not an organization at all.</p>
"""]
    A = parts.append

    for rec, title, cls, blurb in GROUPS:
        g = by_rec.get(rec, [])
        if not g:
            continue
        A(f'<h2>{esc(title)} <span class="n">{len(g)}</span>'
          f'<span class="state {cls}">{STATE[cls]}</span></h2>\n')
        A(f'<p class="sect-note">{blurb}</p>\n')
        # The per-row "why" is only worth a column where it differs row to
        # row. In the _Q and MINT groups it restates the section note on every
        # line, which pushes the names a third of the way across the page.
        per_row_why = len({r["why"] for r in g}) > 1
        A('<div class="scroll"><table><thead><tr><th>cluster</th><th>name</th>'
          '<th>place</th><th>mentions</th><th>reads as</th>'
          + ('<th>why</th>' if per_row_why else '')
          + '</tr></thead><tbody>\n')
        for r in g:
            A(f'<tr><td class="id">{esc(r["cluster_id"])}</td>'
              f'<td class="nm">{yi(r["name"])}</td>'
              f'<td class="city">{yi(r["city"])}</td>'
              f'<td class="n">{esc(r["mentions"])}</td>'
              f'<td class="why">{esc(r["reads_as"])}</td>'
              + (f'<td class="why">{esc(r["why"])}</td>' if per_row_why else '')
              + '</tr>\n')
        A('</tbody></table></div>\n')

    A(f"""<footer>
Built by <code>build_publisher_remaining_page.py</code> from
<code>publisher_remaining_recommendations.tsv</code>, against
<code>origin/main</code>'s core_db ({len(db)} rows) and review ledger — not the
working tree, which the deployed app leaves behind continuously. Nothing here is
applied; every row is a recommendation awaiting a reviewer.
{'' if artifact else ' · <a href="gate.html">All visualizations</a>'}
</footer>
</div>{'' if artifact else '</body></html>'}
""")

    out = OUT_ARTIFACT if artifact else OUT
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(parts), encoding="utf-8")
    print(f"wrote {out.relative_to(REPO)} ({out.stat().st_size/1024:.0f} KB, "
          f"{len(rows)} clusters)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifact", action="store_true")
    main(ap.parse_args().artifact)
