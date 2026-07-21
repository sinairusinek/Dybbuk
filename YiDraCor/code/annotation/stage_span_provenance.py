"""Extract every stage span with the provenance of its current @type.

For each stage span on the live top transcript, walk the page's layer history to
find the layer that INTRODUCED the current type (the oldest layer, contiguous
from the top, that already carries that exact type on that line). That layer's
note + userName tells us whether a deterministic rule, the first-pass
annotator, an HTR model, or a person set it. Columns:

  transkribus_url · current_type · value · value_in_context · type_source · flag

`flag` marks a type that still wants human eyes: `llm` (first-pass default that
no rule ever refined) or `untyped`. Multi-token types and @who are correct
states and are NOT flagged.
"""
from __future__ import annotations
import csv, json, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, STAGE_TYPES
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
COL = 18874
MAX_LAYERS = 8


def line_text(el):
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def parse_stage(root):
    """{line_id: [(offset, length, type)]} for stage spans."""
    out = {}
    for el in root.iter(f"{NS}TextLine"):
        got = [(int(a.get("offset", 0)), int(a.get("length", 0)), a.get("type", ""))
               for t, a in parse_custom(el.get("custom") or "") if t == "stage"]
        if got:
            out[el.get("id")] = got
    return out


# Layers that only carry TEXT/LAYOUT, never author annotation. An HTR run
# ("Model:") produces the Unicode transcription, not stage @types; the layout
# operator `uninecessity` reshaped lines before annotation existed. When one of
# these is the newest layer still carrying a type, the type was authored
# EARLIER and merely carried through — so the walk skips past them (see
# `is_carrier`), and they should never be reported as a tag's source.
_LAYOUT_USERS = {"uninecessity", "rabeliovi", "dmitritoperman", "abeliovich",
                 "avia.vaknin", "miriam.trinh", "hoshkinhoshk", "spam"}


def is_carrier(user: str, note: str) -> bool:
    u = user.split("@")[0]
    n = (note or "").strip()
    return n.startswith("Model:") or (not n and u in _LAYOUT_USERS)


def classify(user: str, note: str):
    """(category, human-readable source). category drives the flag.

    Distinguishes, for a stage @type:
      rule       — a targeted deterministic pipeline rule (auto-resolve lexicon,
                   opening-setting, act-heading, span-scope, …). Confident.
      heuristic  — the programmatic first-pass annotator (auto_annotate /
                   heuristic_annotate). No AI; coarse defaults (mostly business).
      llm        — the interactive Claude-session annotator (annotate_pages +
                   prompts.py; the module does NOT call an API, the model is
                   Claude in the Claude Code session). The one that reads a page
                   and judges the type.
      review     — a Sinai manual/proposal push.
      person     — an RA typing in the Transkribus web UI (Judith / Noa).
      carrier    — HTR/layout push (see is_carrier); NOT a tag source.
    Only `heuristic` and `llm` are flagged for review — a type no rule ever
    refined and a model/heuristic guessed.
    """
    u = user.split("@")[0]
    n = (note or "").strip()
    low = n.lower()
    if is_carrier(user, note):
        return "carrier", f"carried through non-annotating push ({n or u})"
    if not n:
        if u == "judithl1":  return "person", "person:Judith (web UI)"
        if u == "noashur":   return "person", "person:Noa (web UI)"
        if u == "sinai.rusinek": return "manual", "sinai (untooled push)"
        return "person", f"person:{u} (web UI)"
    # heuristic first-pass = auto_annotate / heuristic_annotate (deterministic).
    if any(k in low for k in ("auto annotations", "bootstrap",
                              "re-pull + auto_annotate", "auto songs")):
        return "heuristic", f"heuristic first-pass ({n[:34]})"
    # interactive Claude-session LLM annotator.
    if any(k in low for k in ("body annotation", "annotate pass",
                              "phase-4 annotation", "re-annotation",
                              "annotation push")):
        return "llm", f"Claude-session annotator ({n[:30]})"
    # targeted deterministic rules.
    if "act-opening setting sweep" in low:  return "rule", "rule:opening-setting (ST7b)"
    if "act-heading sweep" in low:          return "rule", "rule:act-heading (H1)"
    if "auto-resolve mechanical" in low:    return "rule", "rule:auto-resolve lexicon"
    if "retag" in low and "musical" in low: return "rule", "rule:musical (ביס→delivery)"
    if "repeat" in low and "delivery" in low: return "rule", "rule:repeat→delivery migration"
    if "pagenum" in low or "collective" in low: return "rule", "rule:pagenum/collective"
    if "span defects" in low:               return "rule", "rule:span-defect-fix"
    if "colon" in low or "l-span speaker" in low or "l spans" in low:
        return "rule", "rule:span-scope"
    if "multi-token stage retype" in low:   return "review", "Sinai option-C retype"
    if "stage-type proposals" in low or "defect fixes" in low:
        return "review", "Sinai proposal 2026-07-20"
    if "pi annotation-flag" in low or "manual q" in low:
        return "review", f"Sinai manual ({n[:32]})"
    if low.startswith("yidracor annotation"):   # generic pipeline annotation pass
        return "llm", f"Claude-session annotator ({n[:30]})"
    return "other", n[:44]


def main():
    client = TrpClient.from_env(); client.login()
    eds = {e["transkribus_doc_id"]: e for e in
           json.load((REPO / "data" / "editions.json").open())["editions"]}
    rows = []
    for doc, e in eds.items():
        if doc == 534187:            # Meshumed — manuscript, separate track
            continue
        fd = client.fulldoc(COL, doc)
        for p in fd["pageList"]["pages"]:
            nr = p["pageNr"]
            tr = p["tsList"]["transcripts"][:MAX_LAYERS]
            layers = []
            for t in tr:
                try:
                    root = etree.fromstring(client.fetch_transcript(t["url"]).encode("utf-8"))
                except Exception:
                    continue
                layers.append((t, parse_stage(root), root))
            if not layers:
                continue
            top_t, top_stage, top_root = layers[0]
            url = f"https://app.transkribus.org/collection/{COL}/doc/{doc}/edit?pageNr={nr}"
            # governing speaker per line, in document order
            gov = ""
            lines = list(top_root.iter(f"{NS}TextLine"))
            for el in lines:
                lid = el.get("id"); txt = line_text(el)
                ents = parse_custom(el.get("custom") or "")
                sp = [a for tag, a in ents if tag == "speaker"]
                if any(tag == "heading" for tag, _ in ents):
                    gov = ""
                if sp:
                    o = int(sp[0].get("offset", 0)); l = int(sp[0].get("length", 0))
                    gov = txt[o:o + l].strip()
                for tag, a in ents:
                    if tag != "stage":
                        continue
                    off = int(a.get("offset", 0)); ln = int(a.get("length", 0))
                    typ = a.get("type", "")
                    val = txt[off:off + ln]
                    ctx = txt.strip() if (sp or not gov) else f"{gov} → {txt.strip()}"
                    # Introducing layer = oldest layer, contiguous from top, that
                    # already carries this exact type on this line. Carrier layers
                    # (HTR/layout) don't author annotation, so the introducer is
                    # the oldest NON-carrier layer in that run.
                    intro = 0
                    for i in range(1, len(layers)):
                        cand = layers[i][1].get(lid, [])
                        m = min(cand, key=lambda s: abs(s[0] - off)) if cand else None
                        if m and abs(m[0] - off) <= 6 and m[2] == typ:
                            if not is_carrier(layers[i][0].get("userName", ""),
                                              layers[i][0].get("note", "")):
                                intro = i
                        else:
                            break
                    cat, src = classify(layers[intro][0].get("userName", ""),
                                        layers[intro][0].get("note", ""))
                    fl = []
                    if not typ:
                        fl.append("untyped")
                    elif not all(tok in STAGE_TYPES for tok in typ.split()):
                        fl.append("invalid-type")
                    if cat in ("llm", "heuristic"):
                        fl.append("review")
                    rows.append([url, typ, val, ctx, src, ";".join(fl)])
        print(f"  {e['title']}: cumulative {len(rows)} spans", flush=True)

    out = REPO / "data" / "review" / "stage_spans_provenance_2026-07-21.tsv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["transkribus_url", "current_type", "value",
                    "value_in_context", "type_source", "flag"])
        w.writerows(rows)
    print(f"\nwrote {len(rows)} stage spans → {out}")
    from collections import Counter
    print("source categories:", dict(Counter(r[4].split(":")[0].split(" ")[0] for r in rows)))
    print("flags:", dict(Counter(f for r in rows for f in (r[5].split(";") if r[5] else []))))


if __name__ == "__main__":
    main()
