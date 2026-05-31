"""Diff pipeline-vs-RA-final PAGE-XML pairs and emit categorized changes.

Run from inside `code/`:
    cd code && python3 -m transkribus.diff_ra_layers
"""
from __future__ import annotations

import csv
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET

def parse_custom(s: str):
    out = []
    for m in re.finditer(r"(\w+)\s*\{([^}]*)\}", s or ""):
        tag = m.group(1)
        attrs = {}
        for kv in m.group(2).split(";"):
            kv = kv.strip()
            if not kv:
                continue
            if ":" in kv:
                k, v = kv.split(":", 1)
                attrs[k.strip()] = v.strip()
        out.append((tag, attrs))
    return out

DATE = "2026-05-31"
ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data" / "review"
REVIEW.mkdir(parents=True, exist_ok=True)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NS = {"p": PAGE_NS}

NIKUD = re.compile(r"[֑-ׇ]")
HE_TOKEN = re.compile(r"[֐-׿א-ת'`װ-״]+")

PLAYS = [
    "MishkeMashke-Kultur1910",
    "DerManUnterTiff",
    "Yudale_der_blinder,_Emkroyt1908",
    "Di_seyder_nakht_Emkroyt_1908",
    "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete",
    "AlNaharotBavel-Amkreut&Freund1909",
    "KidushHashem",
]


def parse_lines(xml_path: Path) -> dict[str, dict]:
    """Return {line_id: {text, spans:[(tag, attrs)], region_id}}."""
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        print(f"  parse error {xml_path}: {e}", file=sys.stderr)
        return {}
    root = tree.getroot()
    out: dict[str, dict] = {}
    for region in root.iter(f"{{{PAGE_NS}}}TextRegion"):
        region_id = region.get("id")
        for line in region.findall(f"{{{PAGE_NS}}}TextLine"):
            lid = line.get("id") or ""
            custom = line.get("custom") or ""
            text_eq = line.find(f"{{{PAGE_NS}}}TextEquiv/{{{PAGE_NS}}}Unicode")
            text = (text_eq.text or "") if text_eq is not None else ""
            spans = parse_custom(custom)
            # only annotation spans (not readingOrder)
            ann = [(t, a) for (t, a) in spans if t in {"speaker", "stage", "heading", "trailer", "role", "roleDesc", "l", "head", "lg", "fw"}]
            out[lid] = {"text": text, "spans": ann, "region_id": region_id, "raw_custom": custom}
    return out


def strip_nikud(s: str) -> str:
    return NIKUD.sub("", s or "")


def span_substring(text: str, attrs: dict) -> str:
    try:
        off = int(attrs.get("offset", "0"))
        ln = int(attrs.get("length", "0"))
    except ValueError:
        return ""
    if off < 0 or ln <= 0 or off + ln > len(text):
        return ""
    return text[off:off+ln]


def span_key(tag: str, attrs: dict) -> tuple:
    """Identity key for matching same-position spans across versions."""
    try:
        off = int(attrs.get("offset", "0"))
        ln = int(attrs.get("length", "0"))
    except ValueError:
        off, ln = -1, -1
    return (tag, off, ln)


def overlap(a: dict, b: dict) -> float:
    try:
        ao, al = int(a.get("offset", 0)), int(a.get("length", 0))
        bo, bl = int(b.get("offset", 0)), int(b.get("length", 0))
    except ValueError:
        return 0.0
    if al <= 0 or bl <= 0:
        return 0.0
    lo, hi = max(ao, bo), min(ao+al, bo+bl)
    inter = max(0, hi-lo)
    union = max(ao+al, bo+bl) - min(ao, bo)
    return inter / union if union else 0.0


def diff_line_spans(tag: str, pipe: list, final: list, pipe_text: str, final_text: str):
    """Return list of (kind, old, new) for one tag on one line.

    kind in {added, deleted, type_changed, span_changed, xmlid_changed, unchanged}.
    Matches greedy by highest overlap.
    """
    pipe_spans = [(t, a) for (t, a) in pipe if t == tag]
    final_spans = [(t, a) for (t, a) in final if t == tag]
    used_p, used_f = set(), set()
    out = []
    # 1) exact span-key matches
    p_by_key = defaultdict(list)
    for i, (_, a) in enumerate(pipe_spans):
        p_by_key[span_key(tag, a)].append(i)
    for j, (_, b) in enumerate(final_spans):
        k = span_key(tag, b)
        if p_by_key[k]:
            i = p_by_key[k].pop(0)
            used_p.add(i); used_f.add(j)
            a = pipe_spans[i][1]
            change = []
            if a.get("type") != b.get("type"):
                change.append("type")
            if a.get("xmlid") != b.get("xmlid"):
                change.append("xmlid")
            if not change:
                out.append(("unchanged", a, b))
            else:
                kind = "type_changed" if "type" in change else "xmlid_changed"
                out.append((kind, a, b))
    # 2) overlap matches for remaining
    rem_p = [i for i in range(len(pipe_spans)) if i not in used_p]
    rem_f = [j for j in range(len(final_spans)) if j not in used_f]
    while rem_p and rem_f:
        best = (0.0, -1, -1)
        for i in rem_p:
            for j in rem_f:
                ov = overlap(pipe_spans[i][1], final_spans[j][1])
                if ov > best[0]:
                    best = (ov, i, j)
        if best[0] < 0.3:
            break
        _, i, j = best
        rem_p.remove(i); rem_f.remove(j)
        a, b = pipe_spans[i][1], final_spans[j][1]
        change = []
        if a.get("type") != b.get("type"):
            change.append("type")
        if a.get("xmlid") != b.get("xmlid"):
            change.append("xmlid")
        change.append("span")
        if "type" in change:
            kind = "type_changed"
        elif "xmlid" in change:
            kind = "xmlid_changed"
        else:
            kind = "span_changed"
        out.append((kind, a, b))
    for i in rem_p:
        out.append(("deleted", pipe_spans[i][1], None))
    for j in rem_f:
        out.append(("added", None, final_spans[j][1]))
    return out


def main() -> int:
    summary_rows = []
    stage_rows = []
    speaker_rows = []
    heading_rows = []
    type_lex: dict[str, Counter] = defaultdict(Counter)
    # Also collect all final-layer stage tags (across all plays) for rule precision evaluation
    final_stage_corpus: list[tuple[str, str]] = []  # (substring_no_nikud, type)

    for play in PLAYS:
        d = ROOT / "data" / play / f"layer_diff_{DATE}"
        if not d.exists():
            summary_rows.append({"play": play, "pages_with_ra": 0})
            continue
        manifest = {}
        mp = d / "_manifest.json"
        if mp.exists():
            for m in json.loads(mp.read_text()):
                manifest[m["pageNr"]] = m
        pairs = sorted(d.glob("*_pipeline.xml"))
        pages_with_ra = len(pairs)
        cat = Counter()
        edited_users: Counter = Counter()
        for pp in pairs:
            page_nr = int(pp.stem.split("_")[0])
            fp = d / f"{page_nr:04d}_final.xml"
            if not fp.exists():
                continue
            mi = manifest.get(page_nr, {})
            edited_users[mi.get("final_user", "?")] += 1
            pipe = parse_lines(pp)
            fin = parse_lines(fp)
            # also harvest ALL final-layer stage tags
            for lid, info in fin.items():
                for tag, attrs in info["spans"]:
                    if tag == "stage":
                        sub = strip_nikud(span_substring(info["text"], attrs))
                        final_stage_corpus.append((sub, attrs.get("type", "")))
            common = set(pipe) & set(fin)
            cat["lines_common"] += len(common)
            cat["lines_pipeline_only"] += len(set(pipe) - set(fin))
            cat["lines_final_only"] += len(set(fin) - set(pipe))
            for lid in common:
                a = pipe[lid]; b = fin[lid]
                if a["text"] != b["text"]:
                    cat["text_changed"] += 1
                # stage diffs
                for kind, ao, bo in diff_line_spans("stage", a["spans"], b["spans"], a["text"], b["text"]):
                    if kind == "unchanged":
                        continue
                    cat[f"stage_{kind}"] += 1
                    pipe_sub = span_substring(a["text"], ao) if ao else ""
                    fin_sub = span_substring(b["text"], bo) if bo else ""
                    new_type = (bo or {}).get("type", "") if bo else ""
                    old_type = (ao or {}).get("type", "") if ao else ""
                    stage_rows.append({
                        "play": play, "page": page_nr, "line_id": lid,
                        "kind": kind,
                        "old_type": old_type, "new_type": new_type,
                        "old_substring": pipe_sub, "new_substring": fin_sub,
                        "line_text": b["text"] or a["text"],
                    })
                    # lexicon: if added or type_changed, accumulate tokens from final substring under new_type
                    if kind in {"added", "type_changed"} and new_type:
                        for tok in HE_TOKEN.findall(strip_nikud(fin_sub)):
                            type_lex[new_type][tok] += 1
                # speaker diffs
                for kind, ao, bo in diff_line_spans("speaker", a["spans"], b["spans"], a["text"], b["text"]):
                    if kind == "unchanged":
                        continue
                    cat[f"speaker_{kind}"] += 1
                    pipe_sub = span_substring(a["text"], ao) if ao else ""
                    fin_sub = span_substring(b["text"], bo) if bo else ""
                    speaker_rows.append({
                        "play": play, "page": page_nr, "line_id": lid,
                        "kind": kind,
                        "old_xmlid": (ao or {}).get("xmlid", "") if ao else "",
                        "new_xmlid": (bo or {}).get("xmlid", "") if bo else "",
                        "old_substring": pipe_sub, "new_substring": fin_sub,
                        "line_text": b["text"] or a["text"],
                    })
                # heading diffs
                for kind, ao, bo in diff_line_spans("heading", a["spans"], b["spans"], a["text"], b["text"]):
                    if kind == "unchanged":
                        continue
                    cat[f"heading_{kind}"] += 1
                    heading_rows.append({
                        "play": play, "page": page_nr, "line_id": lid, "kind": kind,
                        "old_type": (ao or {}).get("type", "") if ao else "",
                        "new_type": (bo or {}).get("type", "") if bo else "",
                        "line_text": b["text"] or a["text"],
                    })
                # trailer diffs (mainly added when RA recognised "ענדע" as not a stage)
                for kind, ao, bo in diff_line_spans("trailer", a["spans"], b["spans"], a["text"], b["text"]):
                    if kind == "unchanged":
                        continue
                    cat[f"trailer_{kind}"] += 1
        row = {"play": play, "pages_with_ra": pages_with_ra}
        row.update({k: v for k, v in cat.items()})
        row["editors"] = ";".join(f"{u}:{n}" for u, n in edited_users.most_common())
        summary_rows.append(row)

    # Write summary
    keys = ["play", "pages_with_ra", "lines_common", "lines_pipeline_only", "lines_final_only",
            "text_changed",
            "stage_added", "stage_deleted", "stage_type_changed", "stage_span_changed", "stage_xmlid_changed",
            "speaker_added", "speaker_deleted", "speaker_type_changed", "speaker_span_changed", "speaker_xmlid_changed",
            "heading_added", "heading_deleted", "heading_type_changed", "heading_span_changed",
            "trailer_added", "trailer_deleted",
            "editors"]
    with (REVIEW / f"ra_corrections_summary_{DATE}.tsv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in summary_rows:
            w.writerow(r)

    # Stage detail
    with (REVIEW / f"ra_stage_changes_{DATE}.tsv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["play", "page", "line_id", "kind", "old_type", "new_type", "old_substring", "new_substring", "line_text"], delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in stage_rows:
            w.writerow(r)

    # Speaker detail
    with (REVIEW / f"ra_speaker_changes_{DATE}.tsv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["play", "page", "line_id", "kind", "old_xmlid", "new_xmlid", "old_substring", "new_substring", "line_text"], delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in speaker_rows:
            w.writerow(r)

    # Heading detail
    with (REVIEW / f"ra_heading_changes_{DATE}.tsv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["play", "page", "line_id", "kind", "old_type", "new_type", "line_text"], delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in heading_rows:
            w.writerow(r)

    # Lexicon
    with (REVIEW / f"ra_stage_type_lexicon_{DATE}.tsv").open("w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["new_type", "rank", "token", "count"])
        for typ in sorted(type_lex):
            for rank, (tok, c) in enumerate(type_lex[typ].most_common(50), 1):
                w.writerow([typ, rank, tok, c])

    # Final-layer corpus (for rule precision evaluation downstream)
    with (REVIEW / f"ra_final_stage_corpus_{DATE}.tsv").open("w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["substring_no_nikud", "type"])
        for sub, typ in final_stage_corpus:
            w.writerow([sub, typ])

    print(f"Wrote {len(stage_rows)} stage diffs, {len(speaker_rows)} speaker diffs, {len(heading_rows)} heading diffs.")
    print(f"Final-layer stage corpus: {len(final_stage_corpus)} tags.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
