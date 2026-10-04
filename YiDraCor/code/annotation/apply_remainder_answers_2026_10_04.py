"""Apply the RAs' answers to the six labels held back from the 08-16 handoff.

Source: docs/handoff_2026-08-30_speaker_labels_REMAINDER.md — the returned copy
(Google Doc 1NaUlAfGE..., modified 2026-09-15). Five of the six are answered.

  1. THREE two-role spans, split (both halves already in the castList):
       MS_Emigration  p47  ירוחם שפרה     -> irukhm + shfrh
       MS_BenHaDor    p15  צורה לפידות    -> tsroyh + lfidus
       MS_DiTsveyTnoim p9  שמעון פדות     -> rbi_shmeun_bn_lkish + bn_fdus
                            ("Two speakers", overriding the single-role tick)

     On p9 the two speaker spans ALREADY exist and merely lack xmlids, so this
     only sets them. On p47 and p15 one span covers both names and is re-cut.

  2. BEN HADOR's duet pronouns, per scene -> speaker_overrides.json:
       Scene A, p.17 (lovers' duet)  ער = shlmun, זיא = sufur
       Scene B, pp.32-33 (comic trio) ער = lfidus
     Two scenes sharing the label `ער` is exactly what the override mechanism
     is for; nothing is retagged.

  3. STILL OPEN: MS_YoysefInEgipten p19 `נאר` — the RAs ticked "still unsure,
     leave it flagged". Untouched, deliberately.

    python3.11 -m annotation.apply_remainder_answers_2026_10_04 --dry-run
    python3.11 -m annotation.apply_remainder_answers_2026_10_04
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (parse_custom, serialize_custom, dedup_entries,
                               collective_skeleton as _skel)
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
COL = 2372172
NOTE = "RA answers 2026-09-15 (speaker-label remainder)"

# (play, page, line_id) -> [(offset, length, xmlid, why), ...]
# Offsets are against the LIVE line text and are re-checked against it before
# writing: the printed form must actually be at that offset or the edit aborts.
SPLITS = {
    ("MS_Emigration", 47, "r1l17"): [
        (0, 5, "irukhm", "ירוחם"),
        (6, 4, "shfrh", "שפרה"),
    ],
    ("MS_BenHaDor", 15, "r2l2"): [
        (0, 4, "tsroyh", "צורה"),
        (5, 6, "lfidus", "לפידות"),
    ],
    # Already two spans, no xmlids. Judith: two speakers, not one.
    ("MS_DiTsveyTnoim", 9, "r1l21"): [
        (0, 5, "rbi_shmeun_bn_lkish", "שמעון"),
        (6, 5, "bn_fdus", "פדות"),
    ],
    # Same line pattern one stanza earlier, same two singers.
    ("MS_DiTsveyTnoim", 9, "r1l18"): [
        (0, 5, "rbi_shmeun_bn_lkish", "שמעון"),
        (7, 5, "bn_fdus", "פדות"),
    ],
}

OVERRIDES = {
    "MS_BenHaDor": [
        {"pages": [17],
         "context": "RA answers 2026-09-15: the lovers' duet on p.17. "
                    "ער = shlmun (Shlmun), זיא = sufur (Sufura).",
         "labels": {"ער": "shlmun", "זיא": "sufur"}},
        {"pages": [32, 33],
         "context": "RA answers 2026-09-15: the comic trio on pp.32-33, where "
                    "ער sings alongside אלטע and יונגע. ער = lfidus. A second "
                    "scene sharing the label ער is why this needs an override "
                    "rather than one xmlid.",
         "labels": {"ער": "lfidus"}},
    ],
}

STILL_OPEN = {
    ("MS_YoysefInEgipten", 19, "נאר"):
        'RAs ticked "still unsure — leave it flagged" (2026-09-15).',
}


def line_text(el) -> str:
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    client = TrpClient.from_env(); client.login()
    by_play: dict[str, dict] = {}
    for (play, pg, lid), spans in SPLITS.items():
        by_play.setdefault(play, {}).setdefault(pg, []).append((lid, spans))

    n_set = n_cut = n_abort = 0
    for play in sorted(by_play):
        man = json.loads((REPO / "data" / play /
                          "_ms_pull_manifest.json").read_text(encoding="utf-8"))
        doc = man["doc_id"]
        pages_meta = {p["pageNr"]: p
                      for p in client.fulldoc(COL, doc)["pageList"]["pages"]}
        print(f"\n=== {play} (doc {doc})", flush=True)

        for pg in sorted(by_play[play]):
            meta = pages_meta.get(pg)
            if not meta or not meta.get("tsList", {}).get("transcripts"):
                print(f"  p{pg}: no transcript"); continue
            top = meta["tsList"]["transcripts"][0]
            root = etree.fromstring(
                client.fetch_transcript(top["url"]).encode("utf-8"))
            changed = False

            for lid, spans in by_play[play][pg]:
                el = next((e for e in root.iter(f"{NS}TextLine")
                           if e.get("id") == lid), None)
                if el is None:
                    print(f"  p{pg} {lid}: line gone — skipped"); n_abort += 1
                    continue
                txt = line_text(el)
                # Compare nikud-stripped: the printed `פּדות` carries a
                # dagesh that the expected form does not, and a raw ==
                # would read that as a stale offset.
                def same(got, want):
                    return _skel(got).strip(" .,|") == _skel(want).strip(" .,|")
                bad = [(o, l, x, w) for o, l, x, w in spans
                       if not same(txt[o:o + l], w)]
                if bad:
                    # The printed form is not where we expect: the line has
                    # been re-transcribed. Never write a guessed offset.
                    print(f"  p{pg} {lid}: OFFSETS STALE — skipped")
                    for o, l, x, w in bad:
                        print(f"      expected {w!r} at {o}+{l}, "
                              f"found {txt[o:o+l]!r}")
                    n_abort += 1
                    continue

                entries = dedup_entries(parse_custom(el.get("custom") or ""))
                others = [(t, at) for t, at in entries if t != "speaker"]
                new_sp = [("speaker", {"offset": str(o), "length": str(l),
                                       "xmlid": x}) for o, l, x, _ in spans]
                had = sum(1 for t, _ in entries if t == "speaker")
                merged = sorted(others + new_sp,
                                key=lambda e: (e[0] != "readingOrder",
                                               int(e[1].get("offset", -1))))
                el.set("custom", serialize_custom(merged))
                changed = True
                if had >= len(spans):
                    n_set += len(spans)
                    verb = "xmlid set on existing spans"
                else:
                    n_cut += len(spans)
                    verb = f"re-cut {had} span -> {len(spans)}"
                print(f"  p{pg} {lid}: {verb} — "
                      + ", ".join(f"{w}={x}" for _, _, x, w in spans))

            if changed and not a.dry_run:
                client.push_transcript(
                    COL, doc, pg, etree.tostring(root, encoding="unicode"),
                    parent_tsid=top["tsId"], status="IN_PROGRESS", note=NOTE,
                    tool_name="YiDraCor-annotation-pipeline")
                print(f"  p{pg}: → pushed", flush=True)

    # per-scene overrides
    for play, scenes in OVERRIDES.items():
        f = REPO / "data" / play / "speaker_overrides.json"
        cur = json.loads(f.read_text(encoding="utf-8")) if f.exists() \
            else {"scenes": []}
        have = {tuple(s["pages"]) for s in cur["scenes"]}
        new = [s for s in scenes if tuple(s["pages"]) not in have]
        if new:
            cur["scenes"].extend(new)
            print(f"\n{play} overrides: +{len(new)} scene(s) "
                  f"{[s['pages'] for s in new]}")
            if not a.dry_run:
                f.write_text(json.dumps(cur, ensure_ascii=False, indent=2)
                             + "\n", encoding="utf-8")
        else:
            print(f"\n{play} overrides: already present")

    print(f"\n{'DRY RUN — ' if a.dry_run else ''}{n_set} xmlid(s) set, "
          f"{n_cut} span(s) re-cut, {n_abort} aborted")
    print(f"{len(STILL_OPEN)} label still open:")
    for (pl, pg, lbl), why in STILL_OPEN.items():
        print(f"  {pl} p{pg} {lbl} — {why}")
    print("\nNext: auto_resolve_flags --only MS_BenHaDor (applies the "
          "overrides to the duet spans)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
