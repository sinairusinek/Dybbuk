"""Tag the Regie cue notation of the manuscript track (annotation_conventions §11).

Finds the R/S/N cue marks in `page_annotated/`, writes them as `metamark`
spans, and **removes the RA `stage` annotation from the same characters**.
A cue is a sign about the performance, not a stage direction, so leaving both
on one span would publish two contradictory readings of the same ink
(Sinai 2026-08-18: "I prefer consistency").

Stage removal is by geometry, not by wholesale deletion:
  * a stage span lying entirely inside the cue is dropped;
  * a stage span overlapping the cue is trimmed back to the part that is still
    genuine stage text, on whichever side survives;
  * a stage span that would be split in two by the cue (real stage text on
    BOTH sides) is left alone and reported — that is an editorial call, not a
    mechanical one, and there should be none of them.

    python3.11 -m annotation.tag_ms_cues --dry-run
    python3.11 -m annotation.tag_ms_cues --only MS_BasKoyen --dry-run
    python3.11 -m annotation.tag_ms_cues --write
"""
import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lxml import etree
from annotation.schema import parse_custom, serialize_custom, dedup_entries

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"

# Separator between letter and numeral is a dot, a slash, a space, or nothing:
# `RII`, `R.I`, `R II`, `.R/II` (Meshumed p.11) are one notation.
RS   = re.compile(r'(?<![A-Za-z])(?:([RS])\s*[./]?\s*(I{1,3})|(I{1,3})\s*[./]?\s*([RS]))(?![A-Za-z])')
# The scribes abbreviate freely: Return, Returnell, Returnal, Retur:, Ret, Re.
# Anchored to an adjacent numeral or a colon so a stray `Re` in OCR noise
# (BenHaDor's cover carries `Ren VItand`) cannot match.
WORD = re.compile(r'(?<![A-Za-z])(?:(I{1,3}|\d)\s*\.?\s*(Re[a-z]*)\b\s*:?'
                  r'|(?:(I{1,3}|\d)\s*\.?\s*)?(Re[a-z]*)\s*:)', re.I)
NUM  = re.compile(r'(?<![A-Za-z])N[o°]?\s*[.=]?\s*(\d{1,2})(?![A-Za-z0-9])')
# NB Refrein/Refrain is deliberately absent: §M5 already assigns the refrain
# rubric to `head` as a structural part of the song, not a Regie cue.
GEN  = re.compile(r'Chor|Marsch|Tanz|Terzett|Quarted|Quartett|Duet\w*|arya|Arie', re.I)
HEBCHOR = re.compile(r'קאהר|כאר|כער')
ANYCUE  = re.compile(r'(?<![A-Za-z])(?:[RS]\s*\.?\s*I{1,3}|I{1,3}\s*\.?\s*[RS]|N[o°]?\s*[.=]?\s*\d{1,2}|Return\w*)', re.I)
BARE = re.compile(r'^(I{1,2})\.?$')
ROMAN = {"I": "in", "II": "out"}


ISOLATED_CUE_PLAYS = ("MS_BasKoyen",)   # Khurbn pending Judith's act-structure review


def isolated_numeral_cues(play, text, entries, found):
    """§C7b. A bare I/II that the RA has isolated as its own untyped `stage`
    span is a cue with the letter elided. Restricted to the plays where C7
    establishes the letter-elided form: elsewhere a bare numeral is an act
    number. The RA's span boundary is what distinguishes the two.
    """
    if play not in ISOLATED_CUE_PLAYS:
        return []
    covered = [(o, o + l) for o, l, *_ in found]
    out = []
    for tag, a in entries:
        if tag != "stage" or a.get("type"):
            continue
        try:
            s0 = int(a["offset"]); ln = int(a["length"])
        except (KeyError, ValueError):
            continue
        tok = text[s0:s0 + ln].strip()
        if tok not in ROMAN:
            continue
        if any(x <= s0 and s0 + ln <= y for x, y in covered):
            continue
        out.append((s0, ln, "musicCue", ROMAN[tok], None))
    return out


def cues(play: str, text: str):
    """Yield (offset, length, function, role, n) for every cue mark on a line."""
    out = []
    for m in RS.finditer(text):
        letter = m.group(1) or m.group(4)
        num = m.group(2) or m.group(3)
        if num not in ROMAN:          # §C2: III never occurs; if it does, leave it
            continue
        out.append((m.start(), len(m.group(0)),
                    "musicCue" if letter == "R" else "sceneCue", ROMAN[num], None))
    for m in WORD.finditer(text):
        n = (m.group(1) or m.group(3) or "").strip()
        # §C4: un-numbered Return* names the item, it does not bracket it
        role = {"I": "in", "1": "in", "II": "out", "2": "out"}.get(n, "genre")
        out.append((m.start(), len(m.group(0).rstrip()), "musicCue", role, None))
    for m in NUM.finditer(text):
        out.append((m.start(), len(m.group(0).rstrip()), "musicCue", "number", m.group(1)))
    for m in GEN.finditer(text):
        out.append((m.start(), len(m.group(0)), "musicCue", "genre", None))
    # §C6: the Hebrew chorus word is a cue only when a mark shares the line;
    # standalone it is a speaker label (148 standalone vs 3 co-occurring)
    if HEBCHOR.search(text) and ANYCUE.search(text):
        m = HEBCHOR.search(text)
        out.append((m.start(), len(m.group(0)), "musicCue", "genre", None))
    # §C7b: a bare numeral the RA has already isolated as its own `stage` span
    # is the letter-elided cue form — the RA drawing that boundary is the
    # evidence. Handled by the caller, which can see the existing spans.
    # §C7: BasKoyen alone drops the letter; Khurbn's bare numerals are act numbers
    if play == "MS_BasKoyen":
        m = BARE.match(text.strip())
        if m:
            out.append((0, len(text.strip()), "musicCue", ROMAN[m.group(1)], None))
    # A bare I/II standing beside another cue on the same line is itself a
    # cue with the letter elided — `R I II` marks both R I and R II, `N3  II`
    # is number 3 plus its out-bracket. Requiring another cue on the line is
    # what keeps this off MS_KhurbnYerusholaim's act numbers (§C7), which sit
    # alone on their line and go up to V.
    if out:
        covered = [(o, o + l) for o, l, *_ in out]
        for m in re.finditer(r'(?<![A-Za-z\u0590-\u05FF])(I{1,2})(?![A-Za-z\u0590-\u05FF])', text):
            a, b = m.start(), m.end()
            if any(x <= a and b <= y for x, y in covered):
                continue
            out.append((a, b - a, "musicCue", ROMAN[m.group(1)], None))
    # drop marks nested inside a longer one (e.g. the N of "No. 4 Returne N=")
    out.sort(key=lambda x: (x[0], -x[1]))
    kept, end = [], -1
    for c in out:
        if c[0] < end:
            continue
        kept.append(c); end = c[0] + c[1]
    return kept


CHORUS = re.compile(r'^(?:קאהר|כאר|כער|chor)$', re.I)
SUBSTANTIVE = re.compile(r'[^\s.,;:|!?\u00a0-]')


def demote_agent_cues(found, entries, text, report=None, where=""):
    """§C6 refinement (Sinai 2026-08-18). The chorus word is a *cue* only when
    it is not doing work inside a stage direction.

    `2 מערדער קאהר זינגט פריער מארש` and `אופטריט עלמת רחומה קאהר.` name the
    party that enters and sings — there the chorus word is the direction's
    agent, and tagging it `metamark` would strip the subject out of the RA's
    stage direction. The test is whether the enclosing `stage` span holds other
    substantive text once every cue is removed from it.
    """
    # A chorus word already tagged `speaker`/`role` IS a speaker label — the
    # RA has decided that, and it resolves to an xml:id. Never also a cue.
    labelled = []
    for tag, a in entries:
        if tag not in ("speaker", "role"):
            continue
        try:
            labelled.append((int(a["offset"]), int(a["offset"]) + int(a["length"])))
        except (KeyError, ValueError):
            pass
    stages = []
    for tag, a in entries:
        if tag != "stage":   # only a stage direction can have an agent
            continue
        try:
            stages.append((int(a["offset"]), int(a["offset"]) + int(a["length"])))
        except (KeyError, ValueError):
            pass
    cue_spans = [(o, o + l) for o, l, *_ in found]
    kept = []
    for c in found:
        off, ln = c[0], c[1]
        if any(a < off + ln and b > off for a, b in labelled):
            if report is not None:
                report["chorus_is_speaker"] += 1
                report.setdefault("_speakers", []).append(
                    (where, text[off:off + ln], text[:55]))
            continue
        if not CHORUS.match(text[off:off + ln].strip()):
            kept.append(c); continue
        host = next((st for st in stages if st[0] <= off and st[1] >= off + ln), None)
        if host is None:
            kept.append(c); continue
        rest = "".join(text[i] for i in range(*host)
                       if not any(a <= i < b for a, b in cue_spans))
        if SUBSTANTIVE.search(rest):
            if report is not None:
                report["agent_not_cue"] += 1
                report.setdefault("_agents", []).append(
                    (where, text[off:off + ln], text[host[0]:host[1]]))
            continue          # agent of a stage direction, not a cue
        kept.append(c)
    return kept


HOST_TAGS = ("stage", "heading")


def retag_stage(entries, found, text, report):
    """Subtract every cue on the line from the `stage` and `heading` spans.

    `heading` is here for the same reason `stage` is. The Meshumed cue marks
    were tagged `heading` by the RAs, so a line reading `R II S II` was being
    published as an act/scene heading — which is both wrong and the source of
    the corpus's `heading.type` violations. Where a line mixes the two
    (`R I II 1ter Act`) the subtraction leaves the genuine heading behind.

    Per-cue subtraction mangles a stage span that covers two cues (`II Return:
    N=3` is both a boundary and a number), so the union is removed at once. A
    residue with no substantive text is dropped rather than left as a stage
    direction consisting of a full stop; a residue on both sides becomes two
    stage spans, which §1 allows so long as they do not overlap.
    """
    cue_spans = sorted((o, o + l) for o, l, *_ in found)
    out = []
    for tag, a in entries:
        if tag not in HOST_TAGS:
            out.append((tag, a)); continue
        try:
            s0 = int(a["offset"]); e0 = s0 + int(a["length"])
        except (KeyError, ValueError):
            out.append((tag, a)); continue
        if not any(cs < e0 and ce > s0 for cs, ce in cue_spans):
            out.append((tag, a)); continue
        # walk the stage range, cutting out every cue
        parts, cur = [], s0
        for cs, ce in cue_spans:
            if ce <= s0 or cs >= e0:
                continue
            if cs > cur:
                parts.append((cur, min(cs, e0)))
            cur = max(cur, ce)
        if cur < e0:
            parts.append((cur, e0))
        parts = [(a1, b1) for a1, b1 in parts if SUBSTANTIVE.search(text[a1:b1])]
        if not parts:
            report[f"{tag}_dropped"] += 1
            continue
        if len(parts) > 1:
            report[f"{tag}_split"] += 1
        elif parts[0] != (s0, e0):
            report[f"{tag}_trimmed"] += 1
        for a1, b1 in parts:
            b = dict(a); b["offset"] = str(a1); b["length"] = str(b1 - a1)
            out.append((tag, b))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append", help="restrict to play folder(s)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--write", action="store_true")
    args = ap.parse_args()

    plays = sorted(m.parent.name
                   for m in (REPO / "data").glob("*/_ms_pull_manifest.json"))
    if args.only:
        plays = [p for p in plays if p in set(args.only)]

    report = Counter()  # also carries a '_agents' list for the §C6 report
    rows = []
    for play in plays:
        pa = REPO / "data" / play / "page_annotated"
        for f in sorted(pa.glob("*.xml")):
            tree = etree.parse(str(f))
            page_touched = False
            for tl in tree.iter(NS + "TextLine"):
                u = tl.find(f"./{NS}TextEquiv/{NS}Unicode")
                text = (u.text or "") if u is not None else ""
                if not text.strip():
                    continue
                entries = parse_custom(tl.get("custom") or "")
                found = cues(play, text)
                if not found and play not in ISOLATED_CUE_PLAYS:
                    continue
                line_touched = False
                found = found + isolated_numeral_cues(play, text, entries, found)
                found = demote_agent_cues(found, entries, text, report,
                                          f"{play} p{f.name.split('_')[0]}")
                new_found = [c for c in found
                             if not any(t == "metamark" and a.get("offset") == str(c[0])
                                        for t, a in entries)]
                report["already_tagged"] += len(found) - len(new_found)
                if new_found:
                    entries = retag_stage(entries, new_found, text, report)
                for off, ln, fn, role, n in new_found:
                    attrs = {"offset": str(off), "length": str(ln),
                             "function": fn, "role": role}
                    if n:
                        attrs["n"] = n
                    entries.append(("metamark", attrs))
                    report["metamark_added"] += 1
                    report[f"role:{role}"] += 1
                    rows.append([play, f.name.split("_")[0], tl.get("id"), off, ln,
                                 fn, role, n or "", text[off:off + ln], text[:100]])
                    line_touched = page_touched = True
                # only rewrite lines we actually changed — re-serialising an
                # untouched line churns the RAs' span order for no reason
                if line_touched:
                    tl.set("custom", serialize_custom(dedup_entries(entries)))
            if page_touched and args.write:
                tree.write(str(f), encoding="utf-8", xml_declaration=True)
                report["pages_written"] += 1

    out = REPO / "data" / "review" / "ms_cue_tagging_applied.tsv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["play", "page", "line_id", "offset", "length", "function",
                    "role", "n", "mark", "line_text"])
        w.writerows(rows)

    agents = report.pop("_agents", [])
    spk = report.pop("_speakers", [])
    for k in sorted(k for k in report if not k.startswith("_")):
        print(f"  {k:18} {report[k]}")
    if spk:
        print("\n  chorus word already tagged speaker/role — not a cue (§C6):")
        for w, mark, t in spk:
            print(f"    {w:22} {mark!r:12} in {t!r}")
    if agents:
        print("\n  demoted to stage-direction agent (§C6):")
        for w, mark, host in agents:
            print(f"    {w:22} {mark!r:10} in stage {host!r}")
    print(f"\n{'WOULD WRITE' if args.dry_run else 'WROTE'} {len(rows)} metamark spans -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
