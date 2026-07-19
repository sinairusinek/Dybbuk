"""Retag the printed musical markers `(ביס)` and `רעפריין` corpus-wide.

Implements docs/proposal_musical_directions_2026-07-19.md:
  - `(ביס)`   → stage {type:repeat}   (was: business, or untagged)
  - `רעפריין` → head   (was: speaker / l / stage), keeping any lg_id

Operates on the CURRENT TOP Transkribus transcript for each page, never on the
local mirror, so no unrelated human decision is reverted. Every other span on
the line is preserved byte-for-byte.

  python3.11 -m annotation.retag_musical_directions --dry-run
  python3.11 -m annotation.retag_musical_directions --push
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

# The repeat mark, in every attested spelling. Widened 2026-07-19 after an
# independent audit (matching the bare ב-י-ס skeleton rather than the tagging
# pattern) found the first pass had missed 29 instances:
#   - `(בּיס)` / `(בּיסס)` — dagesh. The first regex was nikud-blind, so pages
#     carrying ONLY these forms never even entered the fetch list.
#   - `(ביסס)` — doubled ס.
#   - `(ביס 2 מאל)` / `(ביס 4 מאהל)` — repeat WITH an explicit count. Sinai
#     2026-07-19: the count is not recorded; `repeat` alone is enough.
# Lesson: the first "zero missed" check used this same regex, so it could not
# possibly reveal a pattern gap. Verify coverage with an INDEPENDENT pattern.
_N = r"[֑-ׇ]*"                      # any nikud/cantillation run
BIS_RX = re.compile(
    rf"\(\s*ב{_N}י{_N}ס{_N}ס?{_N}"            # bis / biss, any pointing
    rf"(?:\s*\d+\s*מ{_N}א{_N}ה?{_N}ל{_N}\s*\.?)?"   # optional " N מאל" count
    rf"\s*\)")
# Deliberately NOT matched — compound voice-rubric + repeat, pending a decision:
# `(קאהר ביס)` ×9, `(קאהר - ביסס)`, `(אלע ביס)`, `(כער ביס)`. Nor the false
# friends `(אויפטריט ביסינג)` (enter Bising, a character) and
# `(ערוואכט צו ביסלעך)` ("bit by bit").
# line-initial rubric, optional colon; capture just the word
REFRAIN_RX = re.compile(r"^(\s*)(רעפריין)\s*[:׃]?")

# §G.4: a printed voice rubric before sung lines is a SPEAKER attribution, not
# a verse line and not a stage direction. These are abstract voices (no named
# singer identifiable on the page), so @who points at a `printed: false` person
# entry in particDesc — present for @who resolution, absent from the printed
# castList. `סאלא אלט` = "alto solo" → the same voice as a bare `אַלט`.
# The rubric must be the WHOLE line or be immediately followed by a colon.
# Without that boundary the alternation matched a PREFIX of longer words:
# `אַלטען גלויבּען` ("old belief") and, far worse, `אלטעריל:` — a real Yudale
# character — would have been retagged as the alto voice. Caught in dry-run
# 2026-07-19; keep the `(?:[:׃]|$)` anchor.
VOICE_RX = re.compile(r"^(\s*)(סאלא\s+אלט|סאָלאָ\s+אלט|אַלט|אלט|סאפראן|סאָפראן"
                      r"|טענאר|טענאָר|באס|באַס)\s*(?:[:׃]|$)")
VOICE_XMLID = {"סאלאאלט": "alt", "סאָלאָאלט": "alt", "אלט": "alt",
               "סאפראן": "sopran", "סאָפראן": "sopran",
               "טענאר": "tenor", "טענאָר": "tenor",
               "באס": "bas", "באַס": "bas"}
NIKUD_RX = re.compile(r"[֑-ֽֿ-ׇ]")


def line_text(el) -> str:
    u = el.find(f".//{UNICODE_TAG}")
    return (u.text or "") if u is not None else ""


def _covers(attrs: dict, lo: int, hi: int) -> bool:
    """True if the span overlaps [lo,hi)."""
    o = int(attrs.get("offset", 0)); l = int(attrs.get("length", 0))
    return o < hi and (o + l) > lo


def retag_line(el) -> list[str]:
    """Mutate one TextLine's custom attr. Returns list of human-readable changes."""
    txt = line_text(el)
    entries = parse_custom(el.get("custom", ""))
    changes = []

    # ---- 1. (ביס) → stage{type:repeat} -------------------------------------
    for m in BIS_RX.finditer(txt):
        lo, hi = m.start(), m.end()
        hit = None
        for i, (tag, attrs) in enumerate(entries):
            if tag == "stage" and _covers(attrs, lo, hi):
                hit = i
                break
        if hit is not None:
            old = entries[hit][1].get("type")
            if old == "repeat":
                continue
            entries[hit][1]["type"] = "repeat"
            changes.append(f"(ביס)@{lo}: stage type {old} → repeat")
        else:
            entries.append(("stage", {"offset": str(lo), "length": str(hi - lo),
                                      "type": "repeat"}))
            changes.append(f"(ביס)@{lo}: untagged → stage type:repeat")

    # ---- 2. רעפריין → head -------------------------------------------------
    m = REFRAIN_RX.match(txt)
    if m:
        lo, hi = m.start(2), m.end(2)          # the word only, no colon
        rest = m.end()                          # text after rubric+colon
        lg_id = None
        kept = []
        for tag, attrs in entries:
            if tag in ("speaker", "stage") and _covers(attrs, lo, hi):
                changes.append(f"רעפריין: dropped {tag} span on the rubric")
                continue
            if tag == "l" and _covers(attrs, lo, hi):
                lg_id = attrs.get("lg_id")
                o = int(attrs.get("offset", 0)); ln = int(attrs.get("length", 0))
                tail = o + ln - rest
                if tail > 0:
                    # sung text follows the rubric on the same line — keep it as
                    # `l`, but starting after the rubric.
                    attrs["offset"] = str(rest); attrs["length"] = str(tail)
                    kept.append((tag, attrs))
                    changes.append(f"רעפריין: l span shrunk to the sung tail @{rest}")
                else:
                    changes.append("רעפריין: dropped l span (rubric is not a verse line)")
                continue
            kept.append((tag, attrs))
        entries = kept
        if not any(t == "head" and _covers(a, lo, hi) for t, a in entries):
            head = {"offset": str(lo), "length": str(hi - lo)}
            if lg_id is not None:
                head["lg_id"] = lg_id
            entries.append(("head", head))
            changes.append(f"רעפריין: → head{' lg_id=' + lg_id if lg_id else ''}")

    # ---- 2b. a whole-line stage direction is not a verse line ---------------
    # Sinai 2026-07-19. NARROW BY DESIGN: only when a stage span covers the
    # entire line. Of 111 lines carrying both `stage` and `l`, 94 are genuine
    # sung lines with an inline `(ביס)` — dropping `l` there would destroy the
    # song encoding. Only the 17 whole-line cases (a bare `(ביסס)` or
    # `(טאנץ).` on its own line) are wrongly marked as verse.
    stripped = txt.strip()
    if stripped:
        widest = max((int(a.get("length", 0)) for t, a in entries if t == "stage"),
                     default=0)
        if widest >= len(stripped) - 1:
            before = len(entries)
            entries = [(t, a) for t, a in entries if t != "l"]
            if len(entries) != before:
                changes.append(f"whole-line stage: dropped {before - len(entries)} l span(s)")

    # ---- 3. voice rubric → speaker (§G.4) ----------------------------------
    m = VOICE_RX.match(txt)
    if m and not REFRAIN_RX.match(txt):
        lo, hi = m.start(2), m.end(2)
        rest = m.end()
        key = NIKUD_RX.sub("", m.group(2)).replace(" ", "")
        xmlid = VOICE_XMLID.get(key)
        if xmlid:
            kept = []
            for tag, attrs in entries:
                if tag == "l" and _covers(attrs, lo, hi):
                    o = int(attrs.get("offset", 0)); ln = int(attrs.get("length", 0))
                    tail = o + ln - rest
                    if tail > 0:
                        attrs["offset"] = str(rest); attrs["length"] = str(tail)
                        kept.append((tag, attrs))
                        changes.append(f"voice '{m.group(2)}': l span shrunk to sung tail @{rest}")
                    else:
                        changes.append(f"voice '{m.group(2)}': dropped l span (rubric is not a verse line)")
                    continue
                kept.append((tag, attrs))
            entries = kept
            if not any(t == "speaker" and _covers(a, lo, hi) for t, a in entries):
                entries.append(("speaker", {"offset": str(lo), "length": str(hi - lo),
                                            "xmlid": xmlid}))
                changes.append(f"voice '{m.group(2)}': → speaker xmlid={xmlid}")

    if changes:
        el.set("custom", serialize_custom(entries))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    args = ap.parse_args()

    plays = json.loads((REPO / "data" / "_musical_targets.json").read_text())
    client = TrpClient.from_env(); client.login()
    total_pages = total_changes = 0

    for play, info in plays.items():
        if args.only and args.only != play:
            continue
        col, doc = info["col"], info["doc"]
        fulldoc = client.fulldoc(col, doc)
        by_nr = {p["pageNr"]: p for p in fulldoc["pageList"]["pages"]}
        for page_nr in info["pages"]:
            p = by_nr.get(page_nr)
            if p is None:
                print(f"  !! {play} p{page_nr}: not found"); continue
            top = p["tsList"]["transcripts"][0]
            xml = client.fetch_transcript(top["url"])
            tree = etree.fromstring(xml.encode("utf-8"))
            changes = []
            for el in tree.iter(LINE_TAG):
                changes += retag_line(el)
            if not changes:
                continue
            total_pages += 1; total_changes += len(changes)
            who = top["userName"].split("@")[0]
            print(f"\n{play} p{page_nr}  (top layer: {who}, {top['status']})")
            for c in changes:
                print(f"    • {c}")
            if args.push:
                blob = etree.tostring(tree, encoding="UTF-8",
                                      xml_declaration=True, standalone=True)
                res = client.push_transcript(
                    col, doc, page_nr, blob.decode("utf-8"),
                    parent_tsid=top.get("tsId"), status=top["status"],
                    note="retag (ביס)/רעפריין as musical directions")
                print(f"    → pushed tsId={res.get('tsId')} status={top['status']}")
                # Mirror locally. Di Seder carries two filename families for the
                # SAME pageId, so write every mirror of this page, not just one.
                out = REPO / "data" / play / "page_annotated"
                for f in sorted(out.glob(f"{page_nr:04d}_*.xml")):
                    f.write_bytes(blob)
    print(f"\n{'DRY RUN — nothing written' if not args.push else 'PUSHED'}: "
          f"{total_changes} changes across {total_pages} pages")


if __name__ == "__main__":
    main()
