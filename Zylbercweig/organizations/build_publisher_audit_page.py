"""Build docs/Visualizations/publisher_audit.html from the publisher audit.

The review surface for publisher_audit_decisions.tsv: 155 decisions over the
97 core_db publisher/printer rows and the 307 publisher-like clusters that were
never linked to any of them.

Reads core_db.tsv and organizations_clustered.tsv for the display evidence
(cluster mention counts, places, the db row's existing link count) so a reviewer
can judge each pair without opening either file. Nothing here writes a decision;
apply_publisher_audit.py applies the two mechanical classes, and everything else
is worked in the app.

Design follows docs/Visualizations/gate.html, which is the house system for this
directory: Fraunces display, Spectral body, Frank Ruhl Libre for Yiddish,
IBM Plex Mono for ids, on warm paper.

Usage: python3 build_publisher_audit_page.py [--artifact]
"""
from __future__ import annotations

import collections
import csv
import html
import re
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CORE_DB = HERE / "core_db.tsv"
CLUSTERS = HERE / "organizations_clustered.tsv"
DECISIONS = HERE / "publisher_audit_decisions.tsv"
OUT = REPO / "docs" / "Visualizations" / "publisher_audit.html"
# Body-only build for publishing the same page as a Claude Artifact, which
# wraps the content in its own doctype/head/body.
OUT_ARTIFACT = REPO / "docs" / "Visualizations" / "artifact" / "publisher_audit.body.html"

PRESS_TYPE_RE = re.compile(r"publish|printer|print", re.I)
HEBREW_RE = re.compile(r"[֐-׿יִ-ﭏ]")

# Cluster names that read as a publishing house without being typed as one.
PUB_HINT_RE = re.compile(
    r"פֿאַרלאַג|פֿאַרלאָג|פארלאג|פארלאָג|פֿארלאג|פערלאַג|פערלאג|דרוקער|דרוק |דרוק$|"
    r"דפוס|פּרינטינג|פרינטינג|publish|printing|press|verlag|farlag|drukeray|drukerei",
    re.I)

# Section order and copy. Each section says what the class of decision IS, so a
# reviewer opening the page cold knows what they are being asked to confirm.
SECTIONS = [
    ("out_of_project", "Flag out of project", "applied",
     "The modern Israeli trade-publisher block at the very start of core_db — "
     "Am Oved, Keter, Zmora Bitan, Modan, Schocken, Sifriyat Poalim, Yediot "
     "Aharonot. None belongs to Zylbercweig's world and no cluster will ever "
     "match one. Four of them (3, 4, 13, 14) were already flagged, which is "
     "how we know this pass was started deliberately and stopped halfway."),
    ("restore_type", "Restore a lost org_type", "applied",
     "These rows have an EMPTY org_type in core_db while "
     "org_type_canonical_mapping.tsv records Publisher for every one. The "
     "consequence is not cosmetic: an untyped row sits in no publisher queue, "
     "which is why db 54 was never seen to duplicate db 2332, and why the "
     "Amkroyt, Elizeum, Progres, Kunst, Munk and Roth clusters all read as "
     "unmatchable."),
    ("align", "Link a cluster to an existing row", "review",
     "Same publisher, two spellings. Each of these needs a reviewer to add the "
     "cluster to that row's linked_cluster_ids in the app — the links have no "
     "single repair authority, so they are not written by script."),
    ("duplicate", "Duplicate core_db rows", "review",
     "Two rows for one publisher. Both pairs were invisible to earlier dedup "
     "passes because one side of each had lost its org_type."),
    ("reject", "Scores high, is wrong", "rejected",
     "Each of these scores 1.000 on a consonant-skeleton match and must still "
     "be refused. The _Q suffixes in a cluster id mark a deliberate split by "
     "place: the same publisher name in two cities is two houses, so an exact "
     "name score across _Q siblings carries no evidence at all."),
    ("mint", "No core_db row exists", "review",
     "Well-attested publishers with no row at all. Absence was checked against "
     "live core_db over all four name fields with Hebrew points stripped, not "
     "by eye. Several clusters pair up as one firm under two spellings and "
     "should be minted once and linked twice."),
    ("retype", "Typed Publisher, isn't one", "review",
     "Publisher has been absorbing anything that put words on paper or wax. "
     "The DB already has the right labels for these — Media (Radio/Film/TV), "
     "Musical Organization, Trade Union, Library — they just were not applied."),
    ("not_an_org", "Not an organization", "review",
     "Book titles, almanacs, calendars and imprint formulas sitting in the org "
     "table. 'Published by the author's heirs' is a formula; 'Peretz's various "
     "editions' is a description. Neither is a body that can be linked to."),
    ("flag", "Needs a human", "research",
     "Close enough to be suspicious, not close enough to assert. Every one of "
     "these turns on a single disagreeing field — a given name, an initial, a "
     "continent."),
]

STATE_LABEL = {
    "applied": "applied by script",
    "review": "for review in the app",
    "rejected": "rejected",
    "research": "needs research",
}


def read_tsv(p: Path) -> list[dict]:
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def is_deprecated(r: dict) -> bool:
    return (r.get("deprecated") or "").strip().upper() in ("Y", "YES", "TRUE", "1")


def esc(s: str) -> str:
    return html.escape(s or "", quote=True)


def yi(s: str) -> str:
    """Wrap a string in the Yiddish face when it carries Hebrew script.

    unicode-bidi:isolate on .yi keeps a Yiddish name from reordering the Latin
    text around it — without it a name ending in a comma drags the punctuation
    to the wrong end of the line.
    """
    if not s:
        return ""
    cls = "yi" if HEBREW_RE.search(s) else "lat"
    return f'<span class="{cls}">{esc(s)}</span>'


def main(artifact: bool = False) -> None:
    db_rows = read_tsv(CORE_DB)
    db = {r["db_id"]: r for r in db_rows}
    decisions = read_tsv(DECISIONS)

    # Cluster evidence: names, mention count, places.
    agg: dict[str, dict] = {}
    linked: set[str] = set()
    for r in db_rows:
        for c in (r.get("linked_cluster_ids") or "").split("|"):
            if c.strip():
                linked.add(c.strip())
    with CLUSTERS.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            cid = (row.get("cluster_id") or "").strip()
            if not cid:
                continue
            a = agg.setdefault(cid, {"names": collections.Counter(), "n": 0,
                                     "cy": "", "places": collections.Counter(),
                                     "types": collections.Counter()})
            a["n"] += 1
            nm = (row.get("clustered organization") or "").strip()
            if nm:
                a["names"][nm] += 1
            cy = (row.get("canonical_yiddish") or "").strip()
            if cy:
                a["cy"] = cy
            s = (row.get("_ - organizations - _ - locations - _ - settlement") or "").strip()
            if s:
                a["places"][s] += 1
            ty = (row.get("_ - organizations - _ - org_type") or "").strip()
            if ty:
                a["types"][ty] += 1

    # Corpus-level counts for the summary band.
    live_pub = [r for r in db_rows
                if PRESS_TYPE_RE.search(r.get("org_type") or "")
                and not is_deprecated(r)]
    unlinked_pub = [r for r in live_pub
                    if not (r.get("linked_cluster_ids") or "").strip()]
    pub_clusters = [c for c, a in agg.items()
                    if PUB_HINT_RE.search(" ".join(list(a["names"]) + [a["cy"]]))
                    or any(re.search(r"publish|printer|print", t, re.I)
                           for t in a["types"])]
    unlinked_clusters = [c for c in pub_clusters if c not in linked]

    by_kind: dict[str, list[dict]] = collections.defaultdict(list)
    for d in decisions:
        by_kind[(d.get("kind") or "").strip()].append(d)

    conf = collections.Counter((d.get("confidence") or "").strip() for d in decisions)

    # The band reports core_db AS IT STANDS NOW. Once the two mechanical
    # sections are applied, that is no longer the count this audit started
    # from, and a reader comparing the band against the prose would otherwise
    # be left to guess why the numbers moved.
    restored = sum(1 for d in decisions
                   if d.get("kind") == "restore_type"
                   and (db.get((d.get("db_id") or "").strip(), {})
                        .get("org_type") or "").strip())
    flagged = sum(1 for d in decisions
                  if d.get("kind") == "out_of_project"
                  and (db.get((d.get("db_id") or "").strip(), {})
                       .get("out_of_project") or "").strip().lower() == "true")
    applied_note = (f" · includes {restored} whose type this audit restored"
                    if restored else "")
    gate_link = "" if artifact else ' · <a href="gate.html">All visualizations</a>'
    open_body = "" if artifact else "</head><body>"
    closing = "" if artifact else "</body></html>"
    state_line = (
        f"The two applied sections are written: {flagged} rows flagged out of "
        f"project and {restored} lost types restored, logged to "
        f"<code>activity_log.tsv</code>. The counts below are core_db as it "
        f"stands now, after those." if (restored or flagged) else
        "Neither applied section has been written yet; the counts below are "
        "core_db as the audit found it.")

    parts: list[str] = []
    A = parts.append

    # The Artifact host supplies the doctype/html/head/body skeleton and serves
    # the page on its own origin, so the standalone shell and the site-relative
    # gate link are both dropped from that build.
    if artifact:
        A("""<title>Publisher audit</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>""")
    else:
        A("""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Publisher audit</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600&family=Spectral:wght@400;600&family=Frank+Ruhl+Libre:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>""")
    A(f"""
:root{{
  --paper:#faf7f0; --ink:#272219; --soft:#5c554a; --line:#e2dcce; --card:#f2eddf;
  --teal:#0e6f8a; --brick:#a1462e; --amber:#b07d2b; --green:#2f8f7f;
  --violet:#6b5b95; --sunk:#f5f1e4;
}}
:root:not([data-theme="light"]){{}}
@media (prefers-color-scheme: dark){{
  :root:not([data-theme="light"]){{
    --paper:#1a1714; --ink:#ece5d6; --soft:#a79e8d; --line:#36302a; --card:#221e19;
    --teal:#5bb3cc; --brick:#dd8468; --amber:#d4a04a; --green:#5cbfa8;
    --violet:#a899cc; --sunk:#1f1b16;
  }}
}}
:root[data-theme="dark"]{{
  --paper:#1a1714; --ink:#ece5d6; --soft:#a79e8d; --line:#36302a; --card:#221e19;
  --teal:#5bb3cc; --brick:#dd8468; --amber:#d4a04a; --green:#5cbfa8;
  --violet:#a899cc; --sunk:#1f1b16;
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
.lede{{color:var(--soft);margin:.2rem 0 .5rem;max-width:42rem;font-size:1.02rem}}
.yi{{font-family:"Frank Ruhl Libre","Spectral",serif;unicode-bidi:isolate}}
.lat{{unicode-bidi:isolate}}

/* Summary band: the shape of the problem, before any single decision. */
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
.note{{font-size:.86rem;color:var(--soft);max-width:46rem;margin:.9rem 0 2rem}}
.note code{{font-family:"IBM Plex Mono",monospace;font-size:.78rem;
  background:var(--card);padding:.08rem .3rem;border-radius:3px}}

h2{{font-family:"Fraunces",serif;font-weight:600;font-size:1.2rem;
  margin:2.6rem 0 .2rem;padding-bottom:.4rem;border-bottom:1px solid var(--line);
  display:flex;align-items:baseline;gap:.6rem;flex-wrap:wrap}}
h2 .n{{font-family:"IBM Plex Mono",monospace;font-size:.68rem;color:var(--soft);
  font-weight:400;letter-spacing:.04em}}
.state{{font-family:"IBM Plex Mono",monospace;font-size:.6rem;letter-spacing:.08em;
  text-transform:uppercase;border:1px solid currentColor;border-radius:3px;
  padding:.12rem .4rem;font-weight:500}}
.state.applied{{color:var(--green)}}
.state.review{{color:var(--teal)}}
.state.rejected{{color:var(--brick)}}
.state.research{{color:var(--amber)}}
.sect-note{{color:var(--soft);font-size:.88rem;margin:.6rem 0 1.1rem;max-width:46rem}}

/* One decision = one record. A left stripe carries confidence, so a row that
   rests on thin evidence reads at a glance without hunting for the word. */
.rec{{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.15fr);
  gap:.2rem 1.4rem;padding:.7rem 1rem .75rem;border:1px solid var(--line);
  border-left:3px solid var(--green);border-radius:4px;background:var(--card);
  margin-bottom:.45rem;align-items:start}}
.rec.medium{{border-left-color:var(--amber)}}
.rec.low{{border-left-color:var(--brick)}}
.rec .id{{font-family:"IBM Plex Mono",monospace;font-size:.63rem;color:var(--soft);
  letter-spacing:.04em}}
.rec .pair{{min-width:0}}
.rec .why{{min-width:0;font-size:.89rem;color:var(--soft);grid-row:span 2}}
.names{{font-size:1rem;margin:.15rem 0 0;overflow-wrap:anywhere}}
.arrow{{color:var(--teal);font-family:"IBM Plex Mono",monospace;font-size:.78rem;
  padding:0 .15rem}}
.meta{{font-family:"IBM Plex Mono",monospace;font-size:.66rem;color:var(--soft);
  margin-top:.3rem;display:flex;gap:.55rem;flex-wrap:wrap;
  font-variant-numeric:tabular-nums}}
.meta b{{font-weight:500;color:var(--ink)}}
.val{{display:inline-block;font-family:"IBM Plex Mono",monospace;font-size:.68rem;
  border:1px solid var(--line);background:var(--sunk);border-radius:3px;
  padding:.05rem .35rem}}
.val.to{{color:var(--teal);border-color:var(--teal)}}
/* The two applied sections are one decision repeated over a block of ids, so
   they collapse to a table: a reviewer checks the membership of the list, not
   each row's reasoning. The judgement sections keep the full records. */
table.bulk{{width:100%;border-collapse:collapse;font-size:.88rem;
  border:1px solid var(--line);border-radius:4px;overflow:hidden}}
table.bulk th{{font-family:"IBM Plex Mono",monospace;font-size:.62rem;
  letter-spacing:.09em;text-transform:uppercase;color:var(--soft);
  text-align:left;font-weight:500;padding:.5rem .8rem;background:var(--sunk);
  border-bottom:1px solid var(--line)}}
table.bulk td{{padding:.4rem .8rem;border-bottom:1px solid var(--line);
  vertical-align:baseline}}
table.bulk tr:last-child td{{border-bottom:0}}
table.bulk tbody tr:nth-child(even){{background:var(--card)}}
table.bulk .num{{font-family:"IBM Plex Mono",monospace;font-size:.72rem;
  color:var(--soft);font-variant-numeric:tabular-nums;white-space:nowrap}}
table.bulk .nm{{overflow-wrap:anywhere}}
table.bulk .rat{{color:var(--soft);font-size:.82rem}}
.scroll{{overflow-x:auto}}
footer{{margin:3rem 0 3rem;padding-top:1rem;border-top:1px solid var(--line);
  color:var(--soft);font-size:.82rem}}
footer code{{font-family:"IBM Plex Mono",monospace;font-size:.76rem;
  background:var(--card);padding:.08rem .3rem;border-radius:3px}}
a{{color:var(--teal)}}
@media (max-width:50rem){{
  .rec{{grid-template-columns:minmax(0,1fr)}}
  .rec .why{{grid-row:auto}}
}}
</style>{open_body}
<div class="wrap">
<header>
<p class="eyebrow">Dybbuk · Zylbercweig organizations</p>
<h1>Publisher audit</h1>
<p class="lede">Every core_db row typed as a publisher or printer, read against
every cluster that looks like a publishing house and was never linked to one.
{len(decisions)} decisions, each with the evidence it rests on.</p>
</header>

<div class="band">
 <div><p class="k">live publisher rows</p><p class="v">{len(live_pub)}</p>
  <p class="sub">typed Publisher, Printer or Printer/Publisher{applied_note}</p></div>
 <div><p class="k">of those, unlinked</p><p class="v">{len(unlinked_pub)}</p>
  <p class="sub">no cluster at all — about half is the Israeli block</p></div>
 <div><p class="k">publisher-like clusters</p><p class="v">{len(pub_clusters)}</p>
  <p class="sub">by name shape or cluster type</p></div>
 <div><p class="k">of those, unlinked</p><p class="v">{len(unlinked_clusters)}</p>
  <p class="sub">attached to no core_db row</p></div>
 <div><p class="k">decisions here</p><p class="v">{len(decisions)}</p>
  <p class="sub">{conf['high']} firm · {conf['medium']} medium · {conf['low']} thin</p></div>
</div>
<p class="note">{state_line} The two sides are not the same shape, and that is
the first finding: the DB list is short, partly out-of-corpus and partly
untyped, while the cluster list is long and full of things that are not
publishers. The stripe on each record carries how far the evidence goes —
<span class="val" style="border-color:var(--green);color:var(--green)">firm</span>
<span class="val" style="border-color:var(--amber);color:var(--amber)">medium</span>
<span class="val" style="border-color:var(--brick);color:var(--brick)">thin</span>.
Only the first two sections are written by <code>apply_publisher_audit.py</code>;
every other decision is a link or a row that no script should write.</p>
""")

    for kind, title, state, blurb in SECTIONS:
        rows = by_kind.get(kind, [])
        if not rows:
            continue
        A(f'<h2>{esc(title)} <span class="n">{len(rows)}</span>'
          f'<span class="state {state}">{STATE_LABEL[state]}</span></h2>\n')
        A(f'<p class="sect-note">{blurb}</p>\n')

        if kind in ("out_of_project", "restore_type"):
            col = ("out_of_project" if kind == "out_of_project" else "org_type")
            A('<div class="scroll"><table class="bulk"><thead><tr>'
              '<th>id</th><th>db</th><th>name</th><th>now</th>'
              f'<th>{esc(col)} →</th><th>which publisher</th>'
              '</tr></thead><tbody>\n')
            for d in rows:
                did = (d.get("db_id") or "").strip()
                r = db.get(did)
                nm = (r.get("name") or r.get("name_yiddish") or "(unnamed)") if r else "—"
                now = (r.get(col) or "").strip() if r else ""
                # The rationale's distinguishing half is the parenthetical.
                rat = d.get("rationale", "")
                m = re.search(r"\(([^)]*)\)", rat)
                who = m.group(1) if m else rat
                A(f'<tr><td class="num">{esc(d.get("decision_id",""))}</td>'
                  f'<td class="num">{esc(did)}</td>'
                  f'<td class="nm">{yi(nm)}</td>'
                  f'<td class="num">{esc(now) if now else "— empty —"}</td>'
                  f'<td><span class="val to">{esc((d.get("value") or "").strip())}</span></td>'
                  f'<td class="rat">{esc(who)}</td></tr>\n')
            A('</tbody></table></div>\n')
            continue

        for d in rows:
            cid = (d.get("cluster_id") or "").strip()
            did = (d.get("db_id") or "").strip()
            cf = (d.get("confidence") or "high").strip()
            a = agg.get(cid)
            r = db.get(did)

            left: list[str] = []
            if r is not None:
                nm = r.get("name") or r.get("name_yiddish") or "(unnamed)"
                left.append(f'<span class="val">db {esc(did)}</span> {yi(nm)}')
            if a is not None:
                cl = list(a["names"])[0] if a["names"] else a["cy"]
                joiner = '<span class="arrow">←</span>' if r is not None else ""
                left.append(f'{joiner}<span class="val">{esc(cid)}</span> {yi(cl)}')
            elif cid:
                left.append(f'<span class="val">{esc(cid)}</span>')

            meta: list[str] = []
            if r is not None:
                t = (r.get("org_type") or "").strip()
                meta.append(f'type <b>{esc(t) if t else "— empty —"}</b>')
                meta.append(f'links <b>{len([x for x in (r.get("linked_cluster_ids") or "").split("|") if x.strip()])}</b>')
            if a is not None:
                meta.append(f'mentions <b>{a["n"]}</b>')
                pl = ", ".join(p for p, _ in a["places"].most_common(2))
                if pl:
                    meta.append(f'place {yi(pl)}')
                if len(a["names"]) > 1:
                    others = "; ".join(list(a["names"])[1:3])
                    meta.append(f'also {yi(others)}')
            val = (d.get("value") or "").strip()
            if val:
                meta.append(f'<span class="val to">{esc(val)}</span>')

            A(f'<div class="rec {esc(cf)}">'
              f'<div class="pair"><p class="id">{esc(d.get("decision_id",""))} · '
              f'{esc((d.get("action") or "").replace("_"," "))}</p>'
              f'<p class="names">{"".join(left)}</p>'
              f'<div class="meta">{"".join(f"<span>{m}</span>" for m in meta)}</div></div>'
              f'<div class="why">{esc(d.get("rationale",""))}</div>'
              f'</div>\n')

    A(f"""<footer>
Built by <code>build_publisher_audit_page.py</code> from
<code>publisher_audit_decisions.tsv</code>, <code>core_db.tsv</code>
({len(db_rows)} rows) and <code>organizations_clustered.tsv</code>
({len(agg)} clusters). The two applied sections are written by
<code>apply_publisher_audit.py --reviewer &lt;name&gt;</code>, which backs up
core_db, stamps every change into <code>activity_log.tsv</code> and is
idempotent. Everything else belongs in the app, where the decision gets a
reviewer.{gate_link}
</footer>
</div>{closing}
""")

    out = OUT_ARTIFACT if artifact else OUT
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(parts), encoding="utf-8")
    try:
        shown = out.relative_to(REPO)
    except ValueError:
        shown = out
    print(f"wrote {shown}  "
          f"({out.stat().st_size/1024:.0f} KB, {len(decisions)} decisions)")
    print(f"  live publisher rows {len(live_pub)} ({len(unlinked_pub)} unlinked); "
          f"publisher-like clusters {len(pub_clusters)} ({len(unlinked_clusters)} unlinked)")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifact", action="store_true",
                    help="emit the body-only build for a Claude Artifact")
    main(ap.parse_args().artifact)
