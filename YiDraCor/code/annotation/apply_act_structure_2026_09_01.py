"""Stamp Judith's act/Bild structure onto the manuscript pages as heading spans.

Source: docs/handoff_2026-08-18_judith_act_structure.md — all eight questions
answered 2026-09-01. Judith settled WHERE the divisions are; this puts that
into the annotation layer so `structure.build_tei` can emit them.

WHY THIS IS NEEDED
------------------
`build_tei` derives `<div type="act">` / `<div type="scene">` from
`heading{type:act|scene; n:N}` spans. Across the nine notebooks there are 73
heading spans but only 15 carry a type, and `span_int(heading,"n",1)` defaults
an untyped one to act 1 — so Meshumed built six divs all claiming
`Meshumed_Act1` and the file was invalid XML. Yoysef has no heading spans at
all despite having five acts.

WHAT IT DOES
------------
For each (play, page, kind, n) in STRUCTURE it finds the line on that page
whose text carries the division's own heading (`ערשטער אקט`, `בילד`,
`פערוואנדלונג`, …), then either retypes the heading span already on that line
or mints one covering the heading text.

  kind "act"   -> heading {type:act;   n:N}
  kind "scene" -> heading {type:scene; n:N}   (a Bild; Sinai 2026-09-01)
  kind "epilog"-> heading {type:epilog}

Deliberately NOT touched:
  * `ענדע …` / act-end lines — they close a division, they do not open one,
    and build_tei has no use for them.
  * heading spans on lines that are not a division opening (Meshumed's
    `R/II Zingen` and `XX`): those are mis-tagged and are REPORTED, not
    silently retyped or dropped — an RA should look at them.

    python3.11 -m annotation.apply_act_structure_2026_09_01 --dry-run
    python3.11 -m annotation.apply_act_structure_2026_09_01 --only MS_BasKoyen
    python3.11 -m annotation.apply_act_structure_2026_09_01
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom, dedup_entries
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
COL = 2372172
NOTE = "act/Bild structure (Judith 2026-09-01)"

# An act-END line: closes a division, never opens one. Must never be stamped.
END_RE = re.compile(r"ענדע|ende|סוף|שלוסס", re.I)
# The opening of a division.
ACT_RE = re.compile(r"(ערשטער|צווייטער|דריטער|פיערטער|פֿערטער|פונפטער|"
                    r"\d+\s*[טד]?ער|[IVX]+\s*)?\s*(אקט|אַקט|Act|Akt)", re.I)
BILD_RE = re.compile(r"בילד|Bild|פערוואנדלונג|פֿערוואנדלונג|Verwandlung", re.I)
EPI_RE = re.compile(r"עפילאג|epilog", re.I)

# (play, page, kind, n) — from Judith's answers. `n` is None for an epilog.
# Page numbers are Transkribus page numbers, as in the handoff.
STRUCTURE = [
    # ---- Khurbn Yerusholaim: 5 acts, no Bilder -------------------------
    ("MS_KhurbnYerusholaim", 7,  "act", 1), ("MS_KhurbnYerusholaim", 14, "act", 2),
    ("MS_KhurbnYerusholaim", 19, "act", 3), ("MS_KhurbnYerusholaim", 30, "act", 4),
    ("MS_KhurbnYerusholaim", 33, "act", 5),
    # ---- Di Tsvey Tnoim (Q6) -------------------------------------------
    ("MS_DiTsveyTnoim", 5,  "act", 1),
    ("MS_DiTsveyTnoim", 12, "scene", 1), ("MS_DiTsveyTnoim", 16, "scene", 2),
    ("MS_DiTsveyTnoim", 18, "act", 2), ("MS_DiTsveyTnoim", 22, "scene", 1),
    ("MS_DiTsveyTnoim", 28, "act", 3),
    ("MS_DiTsveyTnoim", 40, "act", 4), ("MS_DiTsveyTnoim", 43, "scene", 1),
    # ---- Yoysef in Egipten (Q2) ----------------------------------------
    ("MS_YoysefInEgipten", 5,  "act", 1),
    ("MS_YoysefInEgipten", 19, "act", 2), ("MS_YoysefInEgipten", 21, "scene", 1),
    ("MS_YoysefInEgipten", 25, "act", 3), ("MS_YoysefInEgipten", 29, "scene", 1),
    ("MS_YoysefInEgipten", 32, "act", 4),
    ("MS_YoysefInEgipten", 41, "act", 5),
    ("MS_YoysefInEgipten", 54, "epilog", None),
    # ---- Tissa Essler (Q5) ---------------------------------------------
    ("MS_TissaEssler", 4,  "act", 1),
    ("MS_TissaEssler", 9,  "act", 2), ("MS_TissaEssler", 11, "scene", 2),
    ("MS_TissaEssler", 14, "act", 3),
    ("MS_TissaEssler", 20, "act", 4), ("MS_TissaEssler", 24, "scene", 2),
    ("MS_TissaEssler", 34, "act", 5), ("MS_TissaEssler", 34, "scene", 1),
    ("MS_TissaEssler", 35, "scene", 2),
    # ---- Ben HaDor (Q4): 4 acts, act 4 genuinely pp.35-36 ---------------
    ("MS_BenHaDor", 4,  "act", 1), ("MS_BenHaDor", 18, "act", 2),
    ("MS_BenHaDor", 29, "act", 3), ("MS_BenHaDor", 35, "act", 4),
    # ---- Yaakov-Esav (Q3) ----------------------------------------------
    # Judith wrote "Act 2: 31 - 31"; the notebook has צווייטער אקט at p.21 and
    # דריטער אקט at p.32, so the START was mistyped — act 2 opens at p.21.
    ("MS_YaakovEsav", 6,  "act", 1),
    ("MS_YaakovEsav", 21, "act", 2),
    ("MS_YaakovEsav", 32, "act", 3), ("MS_YaakovEsav", 40, "scene", 1),
    ("MS_YaakovEsav", 42, "act", 4), ("MS_YaakovEsav", 46, "scene", 1),
    # p.57 = a leaf of THE DYBBUK, skipped in build_tei's CONFIG. Not stamped.
    # ---- Bas Koyen (Q7): 3 acts ----------------------------------------
    ("MS_BasKoyen", 5,  "act", 1),
    ("MS_BasKoyen", 18, "scene", 2), ("MS_BasKoyen", 20, "scene", 3),
    ("MS_BasKoyen", 22, "act", 2),
    ("MS_BasKoyen", 31, "act", 3),
    # ---- Meshumed (Q8): FIVE acts, though the title page says four ------
    ("Lateiner_Meshumed", 4,  "act", 1), ("Lateiner_Meshumed", 16, "act", 2),
    ("Lateiner_Meshumed", 25, "act", 3), ("Lateiner_Meshumed", 33, "act", 4),
    ("Lateiner_Meshumed", 37, "act", 5),
    # ---- Emigration (Q1): 5 acts; only 4 of the declared 9 Bilder marked -
    ("MS_Emigration", 5,  "act", 1),
    ("MS_Emigration", 26, "act", 2), ("MS_Emigration", 30, "scene", 2),
    ("MS_Emigration", 42, "scene", 3),
    ("MS_Emigration", 48, "act", 3), ("MS_Emigration", 58, "scene", 2),
    ("MS_Emigration", 64, "act", 4), ("MS_Emigration", 80, "scene", 2),
    ("MS_Emigration", 94, "act", 5),
]


def line_text(el) -> str:
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def find_line(root, kind):
    """The line that OPENS this division, plus the (offset,length) of its head.

    Returns (element, offset, length) or None. Act-end lines are excluded:
    `ענדע ערשטער אקט` matches ACT_RE but closes act 1, it does not open one.
    """
    rx = {"act": ACT_RE, "scene": BILD_RE, "epilog": EPI_RE}[kind]
    for el in root.iter(f"{NS}TextLine"):
        txt = line_text(el).strip()
        if not txt or END_RE.search(txt):
            continue
        m = rx.search(txt)
        if not m:
            continue
        raw = line_text(el)
        start = raw.find(txt[m.start():m.end()])
        if start < 0:
            start = 0
        # take the heading to the end of the printed label, not the whole line
        return el, start, len(txt[m.start():m.end()])
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()

    client = TrpClient.from_env(); client.login()
    plays = sorted({p for p, *_ in STRUCTURE} if not a.only else {a.only})
    n_set = n_mint = n_miss = 0
    stray = []

    for play in plays:
        rows = [r for r in STRUCTURE if r[0] == play]
        if not rows:
            continue
        man = json.loads((REPO / "data" / play /
                          "_ms_pull_manifest.json").read_text(encoding="utf-8"))
        doc = man["doc_id"]
        pages_meta = {p["pageNr"]: p
                      for p in client.fulldoc(COL, doc)["pageList"]["pages"]}
        print(f"\n=== {play} (doc {doc})", flush=True)

        by_page: dict[int, list] = {}
        for _, pg, kind, n in rows:
            by_page.setdefault(pg, []).append((kind, n))

        for page_nr in sorted(by_page):
            meta = pages_meta.get(page_nr)
            if not meta or not meta.get("tsList", {}).get("transcripts"):
                print(f"  p{page_nr}: no transcript"); continue
            top = meta["tsList"]["transcripts"][0]
            root = etree.fromstring(
                client.fetch_transcript(top["url"]).encode("utf-8"))

            changed = False
            for kind, n in by_page[page_nr]:
                hit = find_line(root, kind)
                if hit is None:
                    n_miss += 1
                    print(f"  p{page_nr}: NO {kind} line found — skipped")
                    continue
                el, off, ln = hit
                entries = dedup_entries(parse_custom(el.get("custom") or ""))
                out, done = [], False
                for tag, at in entries:
                    if tag == "heading" and not done:
                        at = dict(at)
                        at["offset"], at["length"] = str(off), str(ln)
                        at["type"] = kind
                        if n is not None:
                            at["n"] = str(n)
                        out.append(("heading", at)); done = True; n_set += 1
                    else:
                        out.append((tag, at))
                if not done:
                    at = {"offset": str(off), "length": str(ln), "type": kind}
                    if n is not None:
                        at["n"] = str(n)
                    out.append(("heading", at)); n_mint += 1
                el.set("custom", serialize_custom(out))
                changed = True
                lbl = f"{kind}{'' if n is None else ' ' + str(n)}"
                print(f"  p{page_nr} {el.get('id')}: {lbl:<9} "
                      f"{line_text(el).strip()[:36]!r}")

            # report heading spans that open nothing — mis-tagged, for an RA
            for el in root.iter(f"{NS}TextLine"):
                txt = line_text(el).strip()
                cs = parse_custom(el.get("custom") or "")
                for tag, at in cs:
                    if tag == "heading" and not at.get("type"):
                        stray.append((play, page_nr, el.get("id"), txt[:34]))

            if changed and not a.dry_run:
                client.push_transcript(
                    COL, doc, page_nr, etree.tostring(root, encoding="unicode"),
                    parent_tsid=top["tsId"], status="IN_PROGRESS", note=NOTE,
                    tool_name="YiDraCor-annotation-pipeline")
                print(f"  p{page_nr}: → pushed", flush=True)

    print(f"\n{'DRY RUN — ' if a.dry_run else ''}{n_set} heading(s) retyped, "
          f"{n_mint} minted, {n_miss} not found")
    if stray:
        print(f"\n{len(stray)} untyped heading span(s) left — these open no "
              f"division and are probably mis-tagged; for an RA to check:")
        for pl, pg, lid, txt in stray:
            print(f"  {pl} p{pg} {lid}: {txt!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
