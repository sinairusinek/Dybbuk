"""Convert the legacy per-line `lg` form into real stanzas: `lg {n}` + `l {lg_id}`.

THE PROBLEM (found 2026-08-16). RAs tag verse by putting a co-extensive
`lg {offset;length;continued:true;type:stanza}` on *every* verse line, rather
than one `lg` per stanza with `l {lg_id:N}` on its lines. `schema.py` accepts
this ("legacy RA form"), so it never linted — but `build_tei.new_lg` opens a
fresh <lg> on every line that carries an `lg` span, so each verse line becomes
its own one-line stanza. Measured across the 15 built print TEIs: 3,159 of
3,294 <lg> elements (96%) hold a single <l>. The externally-sourced Dybbuk
files sit at 3-4%, which is what correct output looks like.

This affects BOTH tracks — the print corpus carries the same legacy form
(2,994 of 3,120 `lg` spans have `continued`) — so this runs corpus-wide, not
just over the manuscript plays.

WHAT IT DOES, per page, in reading order:
  * a line is *verse* if it carries an `lg` or `LG` span
  * consecutive verse lines group into one stanza; the run BREAKS on a
    `speaker` span, a song `head`, an interrupting non-verse line, or page start
  * the run's first line keeps its `lg` span, rewritten to `{offset;length;n:N}`
    (plus `cont:yes` when the legacy `type` said the stanza carries over a page
    break — spelled `cont`, or the mojibake `con\\u003a yes` / `cont\\u003a yes`)
  * every line in the run gets `l {offset;length;lg_id:N}`, derived from the
    `lg` span's own offsets when no `l` span exists (Noa's Yoysef + Emigration
    have `lg` but no `l` at all)
  * per-line `lg`/`LG` spans on lines 2..k are dropped
  * uppercase `LG` is normalised to `lg` on the way through

ON SUB-STANZA STRUCTURE. A verse run is emitted as ONE <lg>. The RAs never
encoded stanza boundaries — in the original pull, 1,224 of 1,245 `lg` spans are
an identical `continued:true` and the 21 exceptions are malformed rather than
meaningful — so a contiguous run is all the evidence there is, and grouping it
whole is forced rather than chosen.

A line-pitch heuristic for splitting same-speaker runs was tried and REMOVED
(2026-08-16). Its ratios ran 1.51, 1.51, 1.52 … 1.96 in a continuous band with
no bimodal separation: ordinary line-spacing jitter, not blank lines. Any
threshold over that distribution invents findings. If sub-stanza structure is
wanted later it has to come from the page images or from the RAs annotating it,
not from a cutoff on noise.

Run (repo root, python3.11):
  python -m annotation.convert_legacy_lg --dry-run --only MS_BasKoyen
  python -m annotation.convert_legacy_lg --report /tmp/lg_review.tsv
  python -m annotation.convert_legacy_lg --apply --only MS_BasKoyen
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import PAGE_NS, parse_custom, serialize_custom  # noqa: E402
from annotation.review_links import page_url  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"

# A legacy `type` that means "this stanza continues from the previous page".
# Stored variously as `cont`, and — where the RA typed `cont: yes` into a field
# that then escaped the colon — as `con: yes` / `cont: yes`.
_CONT_RX = re.compile(r"^cont?\b", re.I)



def _is_cont(attrs: dict) -> bool:
    t = (attrs.get("type") or "").strip()
    if attrs.get("cont") == "yes":
        return True
    return bool(t) and bool(_CONT_RX.match(t)) and t.lower() != "stanza"


def _ytop(el) -> int | None:
    c = el.find(f"{NS}Coords")
    if c is None:
        return None
    pts = []
    for p in (c.get("points") or "").split():
        try:
            x, y = p.split(",")
            pts.append(int(y))
        except ValueError:
            continue
    return min(pts) if pts else None


def _ybot(el) -> int | None:
    c = el.find(f"{NS}Coords")
    if c is None:
        return None
    pts = []
    for p in (c.get("points") or "").split():
        try:
            x, y = p.split(",")
            pts.append(int(y))
        except ValueError:
            continue
    return max(pts) if pts else None


def _line_text(tl) -> str:
    """The line's Unicode text, taken from TextLine-level TextEquiv only.

    Word-level TextEquiv children would double-count; `custom` offsets are
    against the line-level string.
    """
    for te in tl.findall(f"{NS}TextEquiv"):
        u = te.find(f"{NS}Unicode")
        if u is not None:
            return u.text or ""
    return ""


def _reading_index(entries: list[tuple[str, dict]]) -> int:
    for tag, a in entries:
        if tag == "readingOrder":
            try:
                return int(a.get("index", "0"))
            except ValueError:
                return 0
    return 0


def convert_page(tree, start_n: int):
    """Rewrite one page's custom attributes. Returns (n_next, stats, flags).

    Mutates `tree` in place. `stats` counts what changed; `flags` are rows for
    the review sheet — decisions a human should confirm.
    """
    stats = {"stanzas": 0, "lines": 0, "lg_dropped": 0, "l_created": 0,
             "LG_normalised": 0, "cont": 0}
    flags: list[dict] = []

    # IDEMPOTENCE GUARD. After conversion only the stanza's FIRST line carries
    # an `lg` span, so a second pass would read every continuation line as
    # non-verse, close the run at once, and re-split each stanza back into
    # one-line stanzas — undoing the fix. The legacy form is recognisable by
    # `continued` (or uppercase `LG`); a page with neither is already converted
    # (or has no verse) and is left alone.
    legacy = False
    for tl in tree.iter(f"{NS}TextLine"):
        for tag, a in parse_custom(tl.get("custom") or ""):
            if tag == "LG" or (tag == "lg" and "continued" in a):
                legacy = True
                break
        if legacy:
            break
    if not legacy:
        return start_n, stats, flags

    for region in tree.iter(f"{NS}TextRegion"):
        lines = list(region.findall(f"{NS}TextLine"))
        if not lines:
            continue
        parsed = [(tl, parse_custom(tl.get("custom") or "")) for tl in lines]
        parsed.sort(key=lambda p: _reading_index(p[1]))

        run: list[tuple] = []   # (tl, entries, lg_attrs, text)

        def close_run():
            nonlocal run, start_n
            if not run:
                return
            n = start_n
            start_n += 1
            stats["stanzas"] += 1
            first_tl, first_ents, first_lg, first_text = run[0]
            cont = _is_cont(first_lg)
            if cont:
                stats["cont"] += 1
            for i, (tl, ents, lg_attrs, text) in enumerate(run):
                out: list[tuple[str, dict]] = []
                have_l = False
                for tag, a in ents:
                    if tag in ("lg", "LG"):
                        if tag == "LG":
                            stats["LG_normalised"] += 1
                        if i == 0:
                            new = {"offset": a.get("offset", "0"),
                                   "length": a.get("length", str(len(text))),
                                   "n": str(n)}
                            if cont:
                                new["cont"] = "yes"
                            out.append(("lg", new))
                        else:
                            stats["lg_dropped"] += 1
                        continue
                    if tag == "l":
                        a = dict(a)
                        a.pop("continued", None)
                        a["lg_id"] = str(n)
                        have_l = True
                    out.append((tag, a))
                if not have_l:
                    out.append(("l", {"offset": lg_attrs.get("offset", "0"),
                                      "length": lg_attrs.get("length", str(len(text))),
                                      "lg_id": str(n)}))
                    stats["l_created"] += 1
                stats["lines"] += 1
                tl.set("custom", serialize_custom(out))
            run = []

        run = []
        for tl, ents in parsed:
            lg_attrs = next((a for t, a in ents if t in ("lg", "LG")), None)
            text = _line_text(tl)
            if lg_attrs is None:
                close_run()
                continue
            has_speaker = any(t == "speaker" for t, _ in ents)
            has_head = any(t == "head" for t, _ in ents)
            if run and (has_speaker or has_head):
                close_run()
            run.append((tl, ents, lg_attrs, text))
        close_run()
    return start_n, stats, flags


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append", help="restrict to folder(s)")
    ap.add_argument("--apply", action="store_true",
                    help="write the converted XML back (default is dry-run)")
    ap.add_argument("--report", help="write the review TSV here")
    ap.add_argument("--dry-run", action="store_true", help="explicit no-op default")
    args = ap.parse_args()

    from xml.etree import ElementTree as ET
    ET.register_namespace("", PAGE_NS)

    folders = sorted(p for p in (REPO / "data").iterdir()
                     if (p / "page_annotated").is_dir())
    if args.only:
        want = set(args.only)
        folders = [f for f in folders if f.name in want]

    all_flags: list[dict] = []
    grand = {"stanzas": 0, "lines": 0, "lg_dropped": 0, "l_created": 0,
             "LG_normalised": 0, "cont": 0}
    print(f"{'play':34} {'stanzas':>8} {'lines':>7} {'lg dropped':>11} "
          f"{'l made':>7} {'LG→lg':>6} {'cont':>5} {'flags':>6}")
    for folder in folders:
        n = 1
        tot = {k: 0 for k in grand}
        nflag = 0
        for xf in sorted((folder / "page_annotated").glob("*.xml")):
            try:
                tree = ET.parse(xf)
            except ET.ParseError as e:
                print(f"  !! parse error {xf.name}: {e}")
                continue
            n, stats, flags = convert_page(tree, n)
            for k, v in stats.items():
                tot[k] += v
            for fl in flags:
                fl["play"] = folder.name
                fl["page"] = xf.name
                all_flags.append(fl)
            nflag += len(flags)
            if args.apply and (stats["lines"] or stats["lg_dropped"]):
                tree.write(xf, encoding="utf-8", xml_declaration=True)
        if not any(tot.values()) and not nflag:
            continue
        for k, v in tot.items():
            grand[k] += v
        print(f"{folder.name[:34]:34} {tot['stanzas']:8} {tot['lines']:7} "
              f"{tot['lg_dropped']:11} {tot['l_created']:7} "
              f"{tot['LG_normalised']:6} {tot['cont']:5} {nflag:6}")
    print(f"{'TOTAL':34} {grand['stanzas']:8} {grand['lines']:7} "
          f"{grand['lg_dropped']:11} {grand['l_created']:7} "
          f"{grand['LG_normalised']:6} {grand['cont']:5} {len(all_flags):6}")
    print(f"\n{'APPLIED — files rewritten' if args.apply else 'DRY RUN — nothing written'}")

    if args.report and all_flags:
        cols = ["play", "page", "transkribus_url", "kind", "reading_index",
                "text", "note"]
        for fl in all_flags:
            fl["transkribus_url"] = page_url(fl.get("play", ""), fl.get("page", ""))
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
            w.writeheader()
            for fl in all_flags:
                w.writerow({c: fl.get(c, "") for c in cols})
        print(f"review rows → {args.report} ({len(all_flags)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
