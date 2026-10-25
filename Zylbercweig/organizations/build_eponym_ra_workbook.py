#!/usr/bin/env python3
"""Export the still-open eponym questions as an Excel workbook for an RA.

The Zalmen "Eponym links" view is being retired: the high and medium tiers were
swept in-app on 2026-10-13 (127 CONFIRM / 21 REJECT), and what is left is the
part the app was worst at -- ambiguous surnames and spans with no candidate at
all, where the answer needs the lexicon entry, not a radio button.

Sheet 1 "troupes"  one row PER EPONYM, so a trio gets three rows. The reviewer
                   fills `person_name` from a dropdown; everything else is
                   context, read-only in practice.
Sheet 2 "persons"  every Zylbercweig person that HAS an id, as `heading [id]`
                   strings -- the list to point Excel's Data Validation at.

Writing the answers back: `ingest_eponym_ra_workbook.py` maps the chosen
`person_name` back to its hub_id and appends CONFIRM rows to
eponym_decisions.tsv, which apply_eponym_decisions.py already turns into
edge_source=eponym edges. Nothing here writes edges directly.

  python3 build_eponym_ra_workbook.py [-o OUT.xlsx]
"""
from __future__ import annotations

import argparse
import collections
import csv
import pathlib
import re
import xml.etree.ElementTree as ET

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
CANDIDATES = BASE / "organizations" / "eponym_candidates.tsv"
DECISIONS = BASE / "organizations" / "eponym_decisions.tsv"
VERDICTS = BASE / "organizations" / "eponym_llm_verdicts.tsv"
RA_COMMENTS = BASE / "organizations" / "ra_comment_identifications.tsv"
RECALL = BASE / "organizations" / "eponym_recall_candidates.tsv"

# db1482 יעקב און לאה ראַטבאום recovered to `יעקב און וויעראַ ווייס` -- a
# different couple, matched on the shared given name יעקב plus a token of the
# hub's own two-person heading. The surname-adjacent trap, and the target hub
# has no db_id. Excluded by id rather than by a rule, because the rest of the
# recall pass is sound: 43 of its 54 rows agree with a decision already made
# and none contradicts one.
RECALL_EXCLUDE = {("1482", "ראטבאום")}
EDGES = BASE / "organizations" / "graph" / "person_org_edges.tsv"
PERSON_HUB = BASE / "people" / "person_hub.tsv"
CORE_DB = BASE / "organizations" / "core_db.tsv"
LEXICON_DIR = BASE / "The Lexicon"

JSON_TO_XML = {
    "Volume5IIIorg.json": "Structured_Volume5III.xml",
    "Volume_3IIIorg.json": "Structured_Volume_3III.xml",
    "Volume_4IIIorg.json": "Structured_Volume_4III.xml",
    "volume6IIIorg.json": "Structured_volume6III.xml",
    "volume7IIIorg.json": "Structured_volume7III.xml",
    "volume_1IIIorg.json": "Structured_volume_1III.xml",
    "volume_2IIIorg.json": "Structured_volume_2III.xml",
}
TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_ID = "{http://www.w3.org/XML/1998/namespace}id"

SNIPPET_CHARS = 600   # per mention
CONTEXT_CHARS = 1800  # per cell, across mentions

# Common nouns the `namelist` sweep mistakes for surnames. db649
# דרוקעריי פון דער אלמנה און יתומים פּרופּס ("printing house of the widow and
# orphans Props") yielded four "eponyms", of which only פּרופּס is a name.
# Points are already stripped from the span before comparison.
NOT_A_NAME = {
    "דרוקעריי", "אלמנה", "יתומים", "יתומות", "ועד", "פאראיינ", "פארבאנד",
    "געזעלשאפט", "אגודה", "חברה", "קאסע", "שול", "שולע", "ישיבה", "תלמוד",
    "תורה", "בית", "הכנסת", "מדרש", "ביבליאטעק", "מוזיי", "קלוב", "ליגע",
    "צענטער", "צענטראל", "קאמיטעט", "אפטייל", "סעקציע", "גרופע", "קרייז",
    "אנשטאלט", "אינסטיטוט", "אקאדעמיע", "סעמינאר", "קורסנ", "אינטערנאט",
    "טעאטער", "אפערע", "אפערעטע", "סטודיע", "אנסאמבל", "כאר", "קאפעליע",
    "ארקעסטער", "צירק", "קינא", "פילם", "זאל", "הויז", "היים", "הויף",
    "גארטנ", "פארק", "ביהמד", "מלוכה", "שטאט", "שטעטל", "קהילה", "קאהאל",
    "יידישע", "יידישער", "ייִדישע", "נייע", "נייער", "אלטע", "גרויסע",
    "קליינע", "ערשטע", "צווייטע", "דריטע", "מענער", "פרויענ", "קינדער",
    "יוגנט", "ארבעטער", "פאלקס", "לאנד", "וועלט", "פרייע", "פרייער",
}


# Institution markers for the person list. A quoted heading is handled
# separately; these catch the unquoted ones.
INSTITUTION_WORDS = (
    "טעאַטער-פֿאַרבאַנד", "פֿאַרבאַנד", "פאַרבאַנד", "פֿאַראיין", "פאַראיין",
    "געזעלשאַפֿט", "געזעלשאפט", "אַקטיאָרן", "סטוּדיע", "סטודיע",
    "אַנסאַמבל", "קולטור-ליגע", "קאָמיטעט", "אָרגאַניזאַציע",
)


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


_index_cache: dict[str, dict[str, str]] = {}


def entry_index(xml_name: str) -> dict[str, str]:
    """{xml_id: flattened entry text} for one volume. Parsed once, tree freed."""
    if xml_name in _index_cache:
        return _index_cache[xml_name]
    out: dict[str, str] = {}
    path = LEXICON_DIR / xml_name
    try:
        tree = ET.parse(path)
        for div in tree.getroot().iter(f"{{{TEI_NS}}}div"):
            xid = div.get(XML_ID)
            if not xid:
                continue
            text = " ".join(t.strip() for t in div.itertext() if t and t.strip())
            out[xid] = re.sub(r"\s+", " ", text).strip()
        del tree
    except Exception:  # noqa: BLE001  one bad volume must not kill the export
        out = {}
    _index_cache[xml_name] = out
    return out


def entry_text(json_file: str, xml_id: str) -> str:
    xml_name = JSON_TO_XML.get(json_file)
    if not xml_name:
        return ""
    return entry_index(xml_name).get(xml_id, "")


def around(text: str, needle: str, width: int = SNIPPET_CHARS) -> str:
    """A window of `text` centred on `needle`; the head of it if not found."""
    if not text:
        return ""
    i = text.find(needle) if needle else -1
    if i < 0:
        return text[:width] + (" …" if len(text) > width else "")
    half = width // 2
    s = max(0, i - half)
    e = min(len(text), i + len(needle) + half)
    return ("… " if s else "") + text[s:e] + (" …" if e < len(text) else "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", default=str(
        BASE / "organizations" / "eponym_RA_workbook.xlsx"))
    args = ap.parse_args()

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
        from openpyxl.worksheet.datavalidation import DataValidation
    except ImportError:
        raise SystemExit("needs openpyxl: pip install openpyxl")

    cands = read_tsv(CANDIDATES)
    decisions = read_tsv(DECISIONS)
    # Machine proposals, if the judge has run. Absent file = empty columns,
    # so the workbook still builds without ever calling a model.
    verdicts = {(v["db_id"], v["span"]): v for v in read_tsv(VERDICTS)}
    # Her own earlier free-text identifications, harvested by
    # ingest_ra_comments.py. Carried back so a span she has already named is
    # not asked from scratch: the name she wrote goes in the comment column,
    # and where it resolved to a single hub it is offered as the proposal.
    ra_comments = {(c["db_id"], c["span"]): c for c in read_tsv(RA_COMMENTS)}
    # Names the judge recovered from its own free-text output, where the span's
    # own candidate list could not reach the person -- four of them were offered
    # NO candidate at all. Same evidence standard as any other proposal: a quote
    # she can check. `n_candidates_offered` rides along so she can see that
    # nothing was offered, rather than wondering why the dropdown disagrees.
    recall = {(r["db_id"], r["span"]): r for r in read_tsv(RECALL)
              if r.get("outcome") == "recovered"
              and (r["db_id"], r["span"]) not in RECALL_EXCLUDE}
    hubs = read_tsv(PERSON_HUB)
    orgs = {r["db_id"]: r for r in read_tsv(CORE_DB) if (r.get("db_id") or "").strip()}
    edges = read_tsv(EDGES)

    confirmed_spans = {(d["db_id"], d["span"]) for d in decisions
                       if (d.get("decision") or "").upper() == "CONFIRM"}
    # A REJECT naming a hub rules out THAT person and the span stays open for
    # another. A REJECT with no hub says the span is not a name at all
    # (מיט = "with", ערשטער = "first") -- there is no other answer to give, so
    # re-asking it wastes a reviewer's pass.
    confirmed_spans |= {(d["db_id"], d["span"]) for d in decisions
                        if (d.get("decision") or "").upper() == "REJECT"
                        and not (d.get("hub_id") or "").strip()}
    decided_pairs = {f"{d['db_id']}|{d['hub_id']}" for d in decisions}

    # Group candidates by (org, span) -- one eponym per group.
    spans: dict[tuple, list[dict]] = collections.defaultdict(list)
    for c in cands:
        spans[(c["db_id"], c["span"])].append(c)

    # Still open: no CONFIRM for that span, and not every candidate rejected.
    open_spans = []
    dropped_nouns = 0
    for k, group in sorted(spans.items(), key=lambda kv: (int(kv[0][0]), kv[0][1])):
        if k in confirmed_spans:
            continue
        if k[1] in NOT_A_NAME:
            dropped_nouns += 1
            continue
        real = [g for g in group if g.get("hub_id")]
        if real and all(f"{g['db_id']}|{g['hub_id']}" in decided_pairs for g in real):
            continue
        open_spans.append((k, group))

    # Mention context, per org, from the mention edges.
    by_org: dict[str, list[dict]] = collections.defaultdict(list)
    for e in edges:
        if e.get("edge_source") == "mention":
            by_org[e["db_id"]].append(e)

    wb = Workbook()
    ws = wb.active
    ws.title = "troupes"

    # "Ruthie's comments" sits beside person_name because that is how she works:
    # when the dropdown holds nobody who fits she writes the name in prose
    # instead, in Yiddish or in Latin ("Leo Largman", "חיים סאנדלער Chaim
    # Sandler"). The first version of this sheet had no such column, so 19 of
    # her 22 identifications had nowhere to go but a column no script read.
    # ingest_ra_comments.py harvests it; the dropdown stays on person_name.
    HEAD = ["db_id", "troupe (Yiddish)", "troupe (Latin)", "eponym #",
            "name in troupe", "LLM proposal", "LLM conf", "LLM evidence",
            "person_name", "Ruthie's comments", "suggested persons",
            "n_suggestions", "org_type", "mentioned by", "roles / dates",
            "mention context (lexicon)", "notes"]
    ws.append(HEAD)

    # Highest-confidence machine proposals first: the same pre-filled-first
    # ordering that let the high tier be cleared in one pass, while the bare
    # low tier stalled at 5 rows an hour.
    _conf_rank = {"high": 0, "medium": 1, "low": 2}

    def _sort_key(item):
        (db_id, span), _group = item
        v = verdicts.get((db_id, span)) or {}
        has = 1 if (v.get("proposal_hub_id") or "") else 2
        return (has, _conf_rank.get(v.get("confidence", ""), 3), int(db_id), span)

    open_spans.sort(key=_sort_key)

    rows_written = 0
    for (db_id, span), group in open_spans:
        head = group[0]
        org = orgs.get(db_id, {})
        n_for_org = sum(1 for (d, _s), _g in open_spans if d == db_id)
        idx = sum(1 for (d, s), _g in open_spans
                  if d == db_id and (int(db_id), s) <= (int(db_id), span))

        sugg = [g for g in group if g.get("hub_id")]
        sugg_txt = " | ".join(
            f"{g['person_heading']} [{(g.get('person_db_ids') or '').split(',')[0].strip() or g['hub_id']}]"
            for g in sugg[:12])

        # Who mentions this troupe, and the entry text around the troupe name.
        ments = by_org.get(db_id, [])
        who = "; ".join(dict.fromkeys(m["host_heading"] for m in ments if m.get("host_heading")))[:300]
        rd = "; ".join(dict.fromkeys(
            f"{m.get('rel_categories','')}{'/' + m['roles'] if m.get('roles') else ''}"
            f"{' ' + m['dates'] if m.get('dates') else ''}".strip()
            for m in ments if m.get("rel_categories") or m.get("roles")))[:300]

        org_name = (org.get("name_yiddish") or "").strip()
        ctx_parts, used = [], 0
        for m in ments[:6]:
            txt = entry_text(m.get("host_file", ""), m.get("host_xmlid", ""))
            if not txt:
                continue
            snip = around(txt, org_name or span)
            piece = f"[{m.get('host_heading','?')}] {snip}"
            if used + len(piece) > CONTEXT_CHARS:
                break
            ctx_parts.append(piece)
            used += len(piece)
        ctx = "\n\n".join(ctx_parts)

        # The judge's proposal, if one exists for this span. Offered as the
        # dropdown's own label so she can copy it into person_name verbatim --
        # and with its quote, so the proposal can be checked rather than
        # trusted. A proposal is never written into person_name itself: the
        # answer stays hers.
        v = verdicts.get((db_id, span)) or {}
        llm_label = ""
        if v.get("proposal_hub_id"):
            # 301 of 2,897 hubs are facsimile-derived (HUB-P-*) with db_ids
            # empty: a real lexicon entry that never got a people_db row. The
            # old `if pdb else ""` dropped the proposal for every one of them,
            # silently -- the summary line counts proposal_hub_id, so the loss
            # was invisible. Fall back to the hub_id, which ingest resolves via
            # by_label, so the proposal survives and stays selectable.
            pdb = (v.get("proposal_person_db_id") or "").strip()
            phead = (v.get("proposal_heading") or "").strip()
            tag = pdb or (v.get("proposal_hub_id") or "").strip()
            llm_label = f"{phead} [{tag}]" if phead else ""
        elif (v.get("choice") or "") in ("NONE", "ABSTAIN"):
            llm_label = v["choice"]
        llm_ev = v.get("quote") or ""
        if v.get("free_text_name"):
            llm_ev = f"names “{v['free_text_name']}” (not among the candidates) — {llm_ev}"

        # A recovered name fills the slot the span's own candidates could not:
        # the judge abstained there precisely BECAUSE the right person was not on
        # the list, so an ABSTAIN is not a verdict to preserve.
        rk = recall.get((db_id, span))
        if rk and rk.get("person_heading"):
            tag = (rk.get("person_db_id") or "").strip() or (rk.get("hub_id") or "").strip()
            llm_label = f"{rk['person_heading'].strip()} [{tag}]"
            offered = (rk.get("n_candidates_offered") or "").strip()
            llm_ev = (f"recovered from the judge's own reading “{rk['name_read_by_judge']}”"
                      f" — {'NO candidate was offered for this name' if offered in ('', '0') else f'not among the {offered} offered'}"
                      f" — {rk.get('judge_quote') or ''}")

        # Her own earlier answer wins the proposal slot: she named this person
        # from the lexicon, and a machine proposal is not better evidence than
        # that. Where her name resolved to one hub it becomes the pick; where
        # it did not, it still shows, so she sees what she wrote last time
        # instead of starting the span over.
        rc = ra_comments.get((db_id, span))
        if rc:
            if rc.get("outcome") == "resolved" and rc.get("person_heading"):
                pdb = (rc.get("person_db_id") or "").strip()
                tag = pdb or (rc.get("hub_id") or "").strip()
                llm_label = f"{rc['person_heading'].strip()} [{tag}]"
                llm_ev = f"your earlier note “{rc['comment']}” resolves here"
            else:
                llm_ev = (f"your earlier note: “{rc['comment']}”"
                          + (f" — {rc['n_mentions']} mentions in the corpus, no entry"
                             if (rc.get("n_mentions") or "").isdigit()
                             and int(rc["n_mentions"]) > 0 else " — no hub found"))

        ws.append([
            db_id, org_name, (org.get("name") or "").strip(),
            f"{idx} of {n_for_org}", span,
            llm_label, v.get("confidence", ""), llm_ev,
            "", "", sugg_txt, len(sugg),
            (org.get("org_type") or "").strip(), who, rd, ctx,
            head.get("notes") or "",
        ])
        rows_written += 1

    # ── links sheet: the eponym links already settled ──────────────────────
    # What the queue has produced so far, so the work is visible and checkable
    # rather than disappearing into a TSV: every CONFIRM, who decided it, and
    # the evidence. She asked for the links already given; this is also the
    # place an error gets spotted (db1047 זאָלאָטאַרעווסקי was caught exactly
    # by reading a settled row against its org name).
    #
    # One row per (org, person) -- the decision ledger records a row per SPAN,
    # so a troupe naming one person across several tokens appears once here,
    # with the spans joined, matching the single edge it produces.
    ls = wb.create_sheet("links")
    LINK_HEAD = ["db_id", "troupe (Yiddish)", "person", "person_db_id",
                 "hub_id", "spans", "org_type", "decided by", "decided",
                 "tier", "pattern", "notes"]
    ls.append(LINK_HEAD)

    by_pair: dict[tuple, list[dict]] = collections.defaultdict(list)
    for d in decisions:
        if (d.get("decision") or "").upper() == "CONFIRM":
            by_pair[(d["db_id"], d.get("hub_id") or "")].append(d)

    link_rows = 0
    for (db_id, hub_id), group in sorted(
            by_pair.items(), key=lambda kv: (int(kv[0][0]), kv[0][1])):
        org = orgs.get(db_id, {})
        first = group[0]
        spans = " | ".join(dict.fromkeys(
            (g.get("span") or "").strip() for g in group if g.get("span")))
        # Prefer the reviewer who actually settled it; several spans of one
        # person can carry different reviewers (Sinai in-app, Ruthie on the
        # workbook), so name them all rather than crediting only the first.
        who = " | ".join(dict.fromkeys(
            (g.get("reviewer") or "").strip() for g in group if g.get("reviewer")))
        when = sorted((g.get("reviewed_at") or "").strip()
                      for g in group if g.get("reviewed_at"))
        note = next((g.get("notes") or "").strip()
                    for g in group if (g.get("notes") or "").strip()) \
            if any((g.get("notes") or "").strip() for g in group) else ""
        ls.append([
            db_id,
            (org.get("name_yiddish") or "").strip() or (org.get("name") or "").strip(),
            (first.get("person_heading") or "").strip(),
            (first.get("person_db_ids") or "").split(",")[0].strip(),
            hub_id, spans,
            (org.get("org_type") or "").strip(),
            who, when[-1] if when else "",
            first.get("tier") or "", first.get("pattern") or "", note,
        ])
        link_rows += 1

    # ── persons sheet: every Zylbercweig person WITH an id ──────────────────
    ps = wb.create_sheet("persons")
    ps.append(["person_name", "heading", "person_db_id", "hub_id",
               "born", "died", "n_entries"])
    seen: set[str] = set()
    person_labels: list[str] = []
    dropped_orgs = 0
    # A db-less hub is normally not offered. But if the judge PROPOSED one, the
    # proposal has to be selectable or she is shown a name the dropdown refuses.
    # Only the proposed ones are admitted: the db-less class is mixed, holding
    # categories (אָדעסער פֿאָלקסזינגער) and play titles alongside real people,
    # so admitting all 301 would pollute the list she picks from.
    proposed_hub_ids = {
        (v.get("proposal_hub_id") or "").strip()
        for v in verdicts.values()
        if (v.get("proposal_hub_id") or "").strip()
        and not (v.get("proposal_person_db_id") or "").strip()
    }
    for h in sorted(hubs, key=lambda x: x.get("canonical_heading") or ""):
        heading = (h.get("canonical_heading") or "").strip()
        dbid = (h.get("db_ids") or "").split(",")[0].strip()
        if not dbid and (h.get("hub_id") or "").strip() in proposed_hub_ids:
            dbid = (h.get("hub_id") or "").strip()
        if not heading or not dbid:
            continue              # "those that have ID's"
        # person_hub.tsv's non_person flag is 0 for EVERY row, so it cannot be
        # used. Institutions are excluded by a POSITIVE test only: a quoted
        # name ("אַרטעף", „אונזער טעאַטער") or an institution word in the
        # heading. Absence of a comma is NOT a usable signal -- ~430 real
        # people are written "surname given" with no comma (אַדלער בעני,
        # אַנ-סקי ש.), and excluding them would silently hide them from the
        # dropdown she has to pick from.
        bare = heading.lstrip('"\'\u201c\u201e\u00ab ')
        if bare != heading:
            dropped_orgs += 1
            continue
        if any(w in heading for w in INSTITUTION_WORDS):
            dropped_orgs += 1
            continue
        label = f"{heading} [{dbid}]"
        if label in seen:
            continue
        seen.add(label)
        person_labels.append(label)
        ps.append([label, heading, dbid, h.get("hub_id") or "",
                   h.get("birth_date") or "", h.get("death_date") or "",
                   h.get("n_entries") or ""])

    # ── formatting ──────────────────────────────────────────────────────────
    bold = Font(bold=True)
    fill = PatternFill("solid", fgColor="DDEBF7")
    answer_fill = PatternFill("solid", fgColor="FFF2CC")
    for sheet in (ws, ls, ps):
        for c in sheet[1]:
            c.font = bold
            c.fill = fill
        sheet.freeze_panes = "A2"

    link_widths = {"db_id": 8, "troupe (Yiddish)": 34, "person": 38,
                   "person_db_id": 11, "hub_id": 14, "spans": 24,
                   "org_type": 18, "decided by": 16, "decided": 18,
                   "tier": 8, "pattern": 11, "notes": 46}
    for name, w in link_widths.items():
        ls.column_dimensions[get_column_letter(LINK_HEAD.index(name) + 1)].width = w
    for row in ls.iter_rows(min_row=2):
        for c in row:
            c.alignment = Alignment(vertical="top",
                                    wrap_text=c.column_letter in ("B", "C", "L"))
        for n in (LINK_HEAD.index("troupe (Yiddish)"),
                  LINK_HEAD.index("person"), LINK_HEAD.index("spans")):
            row[n].alignment = Alignment(vertical="top", wrap_text=True,
                                         readingOrder=2)

    # Keyed by HEADING, not by letter, so inserting a column cannot silently
    # hand one column's width to its neighbour.
    widths = {
        "db_id": 8, "troupe (Yiddish)": 30, "troupe (Latin)": 26,
        "eponym #": 9, "name in troupe": 16, "LLM proposal": 34,
        "LLM conf": 9, "LLM evidence": 56, "person_name": 34,
        "Ruthie's comments": 30, "suggested persons": 46, "n_suggestions": 7,
        "org_type": 18, "mentioned by": 30, "roles / dates": 28,
        "mention context (lexicon)": 70, "notes": 22,
    }
    for name, w in widths.items():
        ws.column_dimensions[get_column_letter(HEAD.index(name) + 1)].width = w
    for col, w in {"A": 38, "B": 30, "C": 12, "D": 16, "E": 10, "F": 10, "G": 9}.items():
        ps.column_dimensions[col].width = w

    proposal_fill = PatternFill("solid", fgColor="E2EFDA")
    # Positions are derived from HEAD, not hard-coded: inserting "Ruthie's
    # comments" shifted every column after person_name, and the literal
    # indices (row[8], "I2:I...") would have silently pointed one column left.
    ix = {name: n for n, name in enumerate(HEAD)}
    WRAP = {"troupe (Yiddish)", "LLM proposal", "LLM evidence",
            "Ruthie's comments", "suggested persons", "mentioned by",
            "roles / dates", "mention context (lexicon)"}
    wrap_letters = {get_column_letter(ix[h] + 1) for h in WRAP if h in ix}
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.alignment = Alignment(vertical="top",
                                    wrap_text=c.column_letter in wrap_letters)
        row[ix["person_name"]].fill = answer_fill        # the column she fills
        row[ix["Ruthie's comments"]].fill = answer_fill  # and the one beside it
        row[ix["LLM proposal"]].fill = proposal_fill

    # RTL on the Yiddish columns so Excel renders them properly.
    for row in ws.iter_rows(min_row=2):
        for i in (ix["troupe (Yiddish)"], ix["name in troupe"],
                  ix["LLM proposal"], ix["LLM evidence"],
                  ix["mention context (lexicon)"]):
            row[i].alignment = Alignment(vertical="top", wrap_text=True,
                                         readingOrder=2)

    # Data Validation: a dropdown on person_name over the persons sheet.
    # A formula range is used rather than an inline list -- an inline list is
    # capped at 255 chars and there are thousands of people.
    last = len(person_labels) + 1
    dv = DataValidation(type="list",
                        formula1=f"=persons!$A$2:$A${last}",
                        allow_blank=True, showDropDown=True)
    dv.error = ("Pick a person from the list, or leave blank / type NONE. "
                "If the right person is not on the list, write the name in "
                "the comments column instead.")
    dv.errorTitle = "Not in the person list"
    dv.prompt = ("Choose the person this troupe is named after. Nobody fits? "
                 "Name them in \"Ruthie's comments\".")
    dv.promptTitle = "Eponym"
    ws.add_data_validation(dv)
    answer_col = get_column_letter(ix["person_name"] + 1)
    dv.add(f"{answer_col}2:{answer_col}{rows_written + 1}")

    out = pathlib.Path(args.out)
    wb.save(out)

    multi = collections.Counter(d for (d, _s), _g in open_spans)
    print(f"sheet 'troupes': {rows_written} rows "
          f"({len(multi)} troupes; {sum(1 for v in multi.values() if v > 1)} multi-eponym)")
    print(f"  with suggestions : {sum(1 for _k, g in open_spans if any(x.get('hub_id') for x in g))}")
    print(f"  no suggestion    : {sum(1 for _k, g in open_spans if not any(x.get('hub_id') for x in g))}")
    print(f"  with lexicon ctx : (see column O)")
    print(f"  dropped common-noun spans: {dropped_nouns}")
    if verdicts:
        named = sum(1 for (d, s), _g in open_spans
                    if (verdicts.get((d, s)) or {}).get("proposal_hub_id"))
        hi = sum(1 for (d, s), _g in open_spans
                 if (verdicts.get((d, s)) or {}).get("proposal_hub_id")
                 and (verdicts.get((d, s)) or {}).get("confidence") == "high")
        print(f"  LLM proposals    : {named} ({hi} high confidence), "
              f"from {len(verdicts)} verdicts")
    else:
        print("  LLM proposals    : none (eponym_llm_verdicts.tsv absent)")
    print(f"sheet 'links':   {link_rows} settled links "
          f"({len({k[0] for k in by_pair})} troupes)")
    print(f"sheet 'persons': {len(person_labels)} people with ids "
          f"({dropped_orgs} org-like headings excluded)")
    print(f"dropdown on {answer_col}2:{answer_col}{rows_written + 1} "
          f"-> persons!$A$2:$A${last}")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
