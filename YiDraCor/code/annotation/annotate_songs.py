"""Mark song lines with `l` tags and lg_id, across pages.

State machine (per play, walking pages in order):
  - Normal mode: scan lines for a stage span containing 'זינגט' or
    text containing 'געזאנגס' / 'Nr. N.'. On hit → enter song mode,
    open new lg.
  - Song mode: every non-empty line gets an `l` tag with the current
    lg_id covering the whole line text (minus trailing whitespace).
    EXCEPT: a line carrying a speaker span closes the current lg
    (start a new one if the speaker is a chorus marker, else exit
    song mode). A heading span exits song mode.

Usage:
  python -m annotation.annotate_songs --play <folder> [--dry-run]
"""
from __future__ import annotations
import argparse, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import PAGE_NS, parse_custom, append_custom

LINE_TAG = f"{{{PAGE_NS}}}TextLine"
REGION_TAG = f"{{{PAGE_NS}}}TextRegion"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

OPEN_RX_STAGE = re.compile(r"זינגט")  # must occur inside a stage span
OPEN_RX_TEXT  = re.compile(r"^\s*Nr\.\s*\d|^\s*געזאנגס[־⸗-]?טעקסט")  # header-style markers
# Noa 2026-06-14: standalone "(ביס)" (refrain/encore mark) appears at the end
# of song lines and is a robust "we are mid-song" signal. When seen and not
# already in song mode, treat as song-opener and backfill prior eligible
# lines on the same page (until a heading, folio, or non-chorus speaker line).
BIS_RX = re.compile(r"\(\s*ביס\s*\)")
CHORUS_RX = re.compile(r"^\s*(קאהר|כאר|רעפריין|אלע|ביידע|אלט|טענאר|סאלא)\s*[:׃]?")
REPO = Path(__file__).resolve().parents[2]


def has_tag(line_el, tag_name: str) -> bool:
    return any(t == tag_name for t, _ in parse_custom(line_el.get("custom", "")))


def line_text(line_el) -> str:
    u = line_el.find(f".//{UNICODE_TAG}")
    return (u.text or "") if u is not None else ""


def stage_text_in_line(line_el) -> str:
    parts = []
    txt = line_text(line_el)
    for tag, attrs in parse_custom(line_el.get("custom", "")):
        if tag == "stage":
            o = int(attrs.get("offset", 0)); l = int(attrs.get("length", 0))
            parts.append(txt[o:o + l])
    return " ".join(parts)


def iter_pages_in_order(play: str):
    d = REPO / "data" / play / "page_annotated"
    return sorted(d.glob("[0-9]*.xml"))


def process_play(play: str, dry_run: bool) -> dict:
    pages = iter_pages_in_order(play)
    in_song = False
    lg_id = 0
    stats = {"pages": 0, "song_lines": 0, "lg_opened": 0, "lg_closed": 0}

    page_trees = []
    for pf in pages:
        tree = etree.parse(str(pf))
        page_trees.append((pf, tree))

    # strip any pre-existing `l` / `head` / `lg` tags so re-runs are idempotent
    for pf, tree in page_trees:
        for line in tree.iter(LINE_TAG):
            cur = line.get("custom", "")
            cur = re.sub(r"\s*(?:l|head|lg) \{[^}]*\}", "", cur).strip()
            line.set("custom", cur)

    for pf, tree in page_trees:
        stats["pages"] += 1
        # collect lines on this page once so we can backfill on a (ביס) hit.
        page_lines = list(tree.iter(LINE_TAG))
        page_meta = []  # parallel: (text, has_speaker, has_heading, has_stage, chorus_speaker, has_bis)
        for line in page_lines:
            text = line_text(line).rstrip()
            hs = has_tag(line, "speaker")
            hh = has_tag(line, "heading")
            hst = has_tag(line, "stage")
            cs = hs and bool(CHORUS_RX.match(text))
            hb = bool(BIS_RX.search(text))
            page_meta.append((text, hs, hh, hst, cs, hb))

        for idx, line in enumerate(page_lines):
            text, has_speaker, has_heading, has_stage, chorus_speaker, has_bis = page_meta[idx]
            if not text:
                continue
            stage_blob = stage_text_in_line(line)

            opens_song = bool(OPEN_RX_STAGE.search(stage_blob) or OPEN_RX_TEXT.search(text) or has_bis)

            if in_song:
                if has_heading:
                    in_song = False
                    stats["lg_closed"] += 1
                    continue
                if has_speaker and not chorus_speaker:
                    in_song = False
                    stats["lg_closed"] += 1
                    # but still check if this line ITSELF opens a new song
                    if opens_song:
                        lg_id += 1
                        stats["lg_opened"] += 1
                        in_song = True
                    continue
                if has_stage and not chorus_speaker:
                    # stage direction inside a song run — don't tag as `l`
                    continue
                if OPEN_RX_TEXT.search(text):
                    # meta-marker line (e.g. "Nr. 2.", "געזאנגס-טעקסט") — not lyrics
                    continue
                if chorus_speaker:
                    # speaker change inside song → new lg
                    lg_id += 1
                    stats["lg_opened"] += 1
                # tag this line as `l`
                line.set("custom", append_custom(
                    line.get("custom", ""),
                    "l",
                    {"offset": 0, "length": len(text), "lg_id": str(lg_id)},
                ))
                stats["song_lines"] += 1
            else:
                if opens_song and not has_heading:
                    lg_id += 1
                    stats["lg_opened"] += 1
                    in_song = True
                    if has_bis:
                        # Backfill: walk backward on this page over lines that
                        # look like sung lyrics — no heading, no folio, no
                        # non-chorus speaker turn — and tag them under the same
                        # lg. Then tag the current (ביס) line itself.
                        backfill = []
                        j = idx - 1
                        while j >= 0:
                            t_j, hs_j, hh_j, hst_j, cs_j, _ = page_meta[j]
                            if not t_j:
                                j -= 1; continue
                            if hh_j:
                                break
                            if hs_j and not cs_j:
                                # speaker turn — chorus speaker label belongs
                                # to the song, others end the backfill.
                                break
                            if hst_j and not cs_j:
                                # pure stage direction inside a song run — skip
                                # (don't tag, but don't break)
                                j -= 1; continue
                            backfill.append((j, page_lines[j], t_j))
                            j -= 1
                        for _, bline, btext in reversed(backfill):
                            bline.set("custom", append_custom(
                                bline.get("custom", ""),
                                "l",
                                {"offset": 0, "length": len(btext), "lg_id": str(lg_id)},
                            ))
                            stats["song_lines"] += 1
                        line.set("custom", append_custom(
                            line.get("custom", ""),
                            "l",
                            {"offset": 0, "length": len(text), "lg_id": str(lg_id)},
                        ))
                        stats["song_lines"] += 1
                    # for the (זינגט) opener, do NOT tag this line — typically a
                    # stage direction; lyrics start on the next line.

    if in_song:
        stats["lg_closed"] += 1  # implicit close at end of play

    # Pre-pass: detect head lines and emit per-page lg boundaries.
    #   - A "head" is a line at the start of an lg that looks like a title /
    #     section heading (ends with '.' or ':', or is wholly a single character
    #     name like 'ראשעל'). At most 2 leading head lines per lg.
    #   - lg boundary: on each page that an lg appears on, the first line of
    #     that lg-on-page carries `lg {n:N; cont:no|yes}`. `cont:yes`
    #     means this is the same lg continuing from the previous page.
    from annotation.schema import serialize_custom

    META_WORDS = re.compile(r"\b(אקט|דועט|קופלעט|קופלעי|טריאָ|טריא|אַריע|ארע|רעפֿריין|רעפריין|פּראלאג|פראלאג)\b")

    def is_head_candidate(text: str) -> bool:
        t = text.strip()
        if not t: return False
        if t.endswith(":"): return True            # 'דועט:'
        if META_WORDS.search(t): return True       # title-style line
        if t.endswith(".") and len(t) <= 25: return True   # short title like 'קופלעי וואסיליע.'
        if len(t) <= 10 and " " not in t: return True       # single-word name like 'ראשעל'
        return False

    # Collect, per lg, the list of (page_num, line_el, text) in order.
    lg_streams: dict[str, list] = {}
    page_num_of = {}
    for pf, tree in page_trees:
        pnum = int(pf.name.split("_")[0])
        for line in tree.iter(LINE_TAG):
            for tag, attrs in parse_custom(line.get("custom", "")):
                if tag != "l": continue
                lg = attrs.get("lg_id")
                lg_streams.setdefault(lg, []).append((pnum, line, line_text(line).rstrip()))
                page_num_of[id(line)] = pnum

    # Convert leading head-candidate lines from `l` → `head`; mark first line
    # per page with `lg {n; cont}`.
    for lg, items in lg_streams.items():
        # head detection: scan first 3 leading lines; each independently checked
        for idx in range(min(3, len(items))):
            _, line, text = items[idx]
            if not is_head_candidate(text):
                continue
            cur = line.get("custom", "")
            kept = []
            for tag, attrs in parse_custom(cur):
                if tag == "l" and attrs.get("lg_id") == lg:
                    kept.append(("head", {"offset": attrs.get("offset", "0"),
                                          "length": attrs.get("length", "0"),
                                          "lg_id": lg}))
                else:
                    kept.append((tag, attrs))
            line.set("custom", serialize_custom(kept))

        # lg boundaries per page
        seen_pages = set()
        first_page = None
        for pnum, line, _ in items:
            if pnum in seen_pages: continue
            seen_pages.add(pnum)
            # `cont` (not `continued`): Transkribus rejects `continued` as a tag
            # property name. The TEI structurer expands cont:yes → continued="yes".
            cont = "no" if first_page is None else "yes"
            if first_page is None: first_page = pnum
            cur = line.get("custom", "")
            # drop any prior lg tag
            kept = [(t, a) for t, a in parse_custom(cur) if t != "lg"]
            kept.append(("lg", {"n": lg, "cont": cont}))
            line.set("custom", serialize_custom(kept))

    # Post-pass: drop lg's that look like prose (lines too long) or meta-marker
    # stubs (single-line lg with content like "פערוואַנדלונג", "סוף", etc.).
    META_MARKERS = {"פערוואַנדלונג", "פערוואנדלונג", "סוף", "אַ סוף", "ענדע"}
    lg_lines = {}  # lg_id -> list of (line_el, text_len, text_strip)
    for pf, tree in page_trees:
        for line in tree.iter(LINE_TAG):
            for tag, attrs in parse_custom(line.get("custom", "")):
                if tag == "l":
                    lg = attrs.get("lg_id")
                    t = line_text(line).rstrip()
                    lg_lines.setdefault(lg, []).append((line, len(t), t))

    drop = set()
    for lg, items in lg_lines.items():
        lens = [n for _, n, _ in items]
        if not lens: continue
        median = sorted(lens)[len(lens) // 2]
        if median > 45:
            drop.add(lg); continue
        if len(items) <= 2 and any(t.rstrip(".") in META_MARKERS for _, _, t in items):
            drop.add(lg); continue
    # Also drop lg's whose lines all got converted to `head` (no lyrics left).
    lg_has_l = set(lg_lines.keys())
    lg_seen = set()
    for pf, tree in page_trees:
        for line in tree.iter(LINE_TAG):
            for tag, attrs in parse_custom(line.get("custom", "")):
                if tag in ("head", "lg"):
                    # A `head` with no lg_id is not a song heading (castList
                    # "פערזאנען", act headings, …). Feeding None into lg_seen put
                    # None in `drop` and deleted every such head in the corpus.
                    lg = attrs.get("lg_id") or attrs.get("n")
                    if lg is not None:
                        lg_seen.add(lg)
    drop |= (lg_seen - lg_has_l)
    if drop:
        for pf, tree in page_trees:
            for line in tree.iter(LINE_TAG):
                cur = line.get("custom", "")
                kept = []
                for tag, attrs in parse_custom(cur):
                    if (tag in ("l", "head") and attrs.get("lg_id") is not None
                            and attrs.get("lg_id") in drop):
                        continue
                    if tag == "lg" and attrs.get("n") in drop:
                        continue
                    kept.append((tag, attrs))
                line.set("custom", serialize_custom(kept))
        stats["lg_dropped"] = len(drop)

    if not dry_run:
        for pf, tree in page_trees:
            tree.write(str(pf), encoding="UTF-8", xml_declaration=True, standalone=True)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    s = process_play(args.play, args.dry_run)
    print(f"{args.play}: {s}")


if __name__ == "__main__":
    main()
