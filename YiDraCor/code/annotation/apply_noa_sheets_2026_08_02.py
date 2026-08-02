"""Apply Noa's 2026-08-02 sheet returns + two clarified stage-type rules.

Sources (saved in docs/):
  1. stage_spans_provenance_2026-07-21 — 11 marked rows (flag/comments cols).
  2. speaker_who_review "Mappings to check" — 3 corrections to earlier OKs.
  3. Rule clarifications relayed by Sinai 2026-08-02:
       R1. exit + entry of DIFFERENT persons = type `mixed` (compounds of one
           person's movement stay multi-token, e.g. `exit business`).
       R2. a sentence describing the location/situation directly after an ACT
           opening or a פערוואנדלונג = `setting`.

Already resolved live (verified, no-ops here): Yudale (קאהר ביז) → delivery
(the ביז regex was widened 2026-07-20); Ezra p13 exit business.
Not actionable: Mishke רוס "see note" — the sheet cell note doesn't export to
TSV; coining `rus` (רוס) as the plain reading, flagged for Sinai.
Left open (flag '?' with no comment): Mishke מאנקאדזשאו-whistles row.

R2 sweep = contiguous whole-line stage runs right after an act heading or a
פערוואנדלונג line, currently business/location → setting. Excluded: Di Seder
supplement (pages > 54 — song-context notes, not scenes) and music cues
(Ritt./Nr./No. patterns).

  python3.11 -m annotation.apply_noa_sheets_2026_08_02 --dry-run
  python3.11 -m annotation.apply_noa_sheets_2026_08_02 --push
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from annotation.lint_pages import REPO
from transkribus.client import TrpClient

NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
NOTE = "Noa sheets + stage-rule clarifications 2026-08-02"
VOWELS = re.compile(r"[֑-ׇ]")
def unvoc(s): return VOWELS.sub("", s)

DISEDER = "Di_seyder_nakht_Emkroyt_1908"
MUSIC_CUE = re.compile(r"Ritt|Nr\.|No\.")

# --- sheet 1: individual span retypes, located by (page, text-needle) -------
# (play, page, needle-in-unvocalized-line, from_type, to_type)
RETYPE = [
    ("MishkeMashke-Kultur1910", 14, "הער זיך איין", "delivery", "business"),
    (DISEDER, 8, "אויסער זיך", "business", "delivery"),
    ("BasSheva", 8, "ריטארנעל", "business", "entrance business"),
    ("DovidsFidele-1904", 24, "הייבט אויף דען קאפף", "location", "business"),
    ("Ezra-Emkroyt1908", 9, "ביז יעצט געלוישט", "entrance business", "mixed"),  # R1
    # R1 corpus scan results (exit+entrance tokens):
    ("DerManUnterTiff", 13, "יאכטשע אב, אויפטריט", "exit entrance", "mixed"),
    (DISEDER, 18, "קומט אריין, אין מיט'ן", "exit entrance", "mixed"),
    (DISEDER, 18, "קומט ער מיט דוד מרים", "exit entrance", "mixed"),
]

# --- sheet 2: speaker-who corrections ---------------------------------------
# AlNaharot p29: both דיך spans mis-ascribed to dovid — not a speaker at all.
# Mishke p10: רוס spans dos -> new role rus. SoreSheyndel p33: חנהלע -> khnhle.
DROP_SPEAKER = [("AlNaharotBavel-Amkreut&Freund1909", 29, "דיך", "dovid")]
REMAP_WHO = [("MishkeMashke-Kultur1910", 10, "רוס", "dos", "rus"),
             ("SoreSheyndel", 33, "חנהלע", "alle", "khnhle")]


def r2_targets(play, path):
    """Rule-2: (line_id, cur_type) list for one page file."""
    if play == DISEDER and int(path.name[:4]) > 54:
        return []
    lines = []
    for tl in etree.parse(str(path)).iter(NS + "TextLine"):
        u = tl.find(f".//{NS}Unicode")
        lines.append((tl.get("id"), tl.get("custom") or "",
                      (u.text or "") if u is not None else ""))
    out = []
    for i, (lid, c, t) in enumerate(lines):
        trigger = (("heading {" in c and "type:act" in c)
                   or ("פערוואנדלונג" in unvoc(t) and "stage" in c))
        if not trigger:
            continue
        j = i + 1
        while j < len(lines):
            lid2, c2, t2 = lines[j]
            st = re.search(r"stage \{([^}]*)\}", c2)
            if not st or "speaker {" in c2 or " l {" in c2:
                break
            off = re.search(r"offset:(\d+)", st.group(1))
            if off and int(off.group(1)) > 2:
                break
            mt = re.search(r"type:([^;]*)", st.group(1))
            ty = (mt.group(1).strip() if mt else "")
            if ty in ("business", "location") and not MUSIC_CUE.search(t2):
                out.append((lid2, ty))
            j += 1
    return out


def edit_page(root, play, page, r2):
    log = []
    for tl in root.iter(f"{NS}TextLine"):
        lid = tl.get("id")
        u = tl.find(f".//{NS}Unicode")
        txt = (u.text or "") if u is not None else ""
        bare = unvoc(txt)
        entries = parse_custom(tl.get("custom") or "")
        dirty = False

        for pl, pg, needle, frm, to in RETYPE:
            if (pl, pg) != (play, page) or needle not in bare:
                continue
            for tag, a in entries:
                if tag == "stage" and a.get("type") == frm:
                    a["type"] = to
                    log.append(f"{lid}: stage {frm} → {to}  [{bare[:34]!r}]")
                    dirty = True

        if lid in r2:
            for tag, a in entries:
                if tag == "stage" and a.get("type") == r2[lid]:
                    a["type"] = "setting"
                    log.append(f"{lid}: stage {r2[lid]} → setting (R2)  [{bare[:34]!r}]")
                    dirty = True

        for pl, pg, label, who in DROP_SPEAKER:
            if (pl, pg) != (play, page) or not bare.startswith(label):
                continue
            keep = [(t, a) for t, a in entries
                    if not (t == "speaker" and a.get("xmlid") == who)]
            if keep != entries:
                entries = keep
                log.append(f"{lid}: dropped speaker xmlid:{who} (not a speaker)  "
                           f"[{bare[:24]!r}]")
                dirty = True

        for pl, pg, label, frm, to in REMAP_WHO:
            if (pl, pg) != (play, page) or not bare.startswith(label):
                continue
            for tag, a in entries:
                if tag == "speaker" and a.get("xmlid") == frm:
                    a["xmlid"] = to
                    log.append(f"{lid}: speaker {frm} → {to}  [{bare[:24]!r}]")
                    dirty = True

        if dirty:
            tl.set("custom", serialize_custom(entries))
    return log


def update_cast_dicts(dry):
    p = REPO / "data" / "MishkeMashke-Kultur1910" / "cast_dict.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    if "rus" not in d["roles"]:
        d["roles"]["rus"] = {
            "form": "רוס", "bare": "רוס", "loc": None, "prefix_variants": [],
            "printed": False,
            "notes": [f"{NOTE}: Noa '++ new role — see note' on רוס→dos rows; "
                      "the sheet cell note did not export — id coined as rus, "
                      "confirm with Noa"]}
        print("  cast_dict Mishke: +role rus")
    if not dry:
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n",
                     encoding="utf-8")
    p = REPO / "data" / "AlNaharotBavel-Amkreut&Freund1909" / "cast_dict.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    ns = d.setdefault("non_speaker_labels", [])
    if "דיך" not in ns:
        ns.append("דיך")
        print("  cast_dict AlNaharot: non_speaker_labels += דיך")
    if not dry:
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n",
                     encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()
    dry = not args.push

    print("— cast_dict updates —")
    update_cast_dicts(dry)

    # page work-list: explicit edits + R2 scan over every play
    pages: dict[tuple[str, int], dict] = {}
    for pl, pg, *_ in RETYPE + DROP_SPEAKER + REMAP_WHO:
        pages.setdefault((pl, pg), {})
    import os
    for play in sorted(os.listdir(REPO / "data")):
        pa = REPO / "data" / play / "page_annotated"
        if not pa.is_dir():
            continue
        for f in sorted(pa.glob("0*.xml")):
            t = r2_targets(play, f)
            if t:
                pages.setdefault((play, int(f.name[:4])), {}).update(dict(t))

    ids = load_doc_ids()
    client = TrpClient.from_env(); client.login()
    pushed = 0
    for (play, page), r2 in sorted(pages.items()):
        tsid, owner, xml = top_transcript(client, ids[play], page)
        if xml is None:
            print(f"{play} p{page}: no transcript — SKIP"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        log = edit_page(root, play, page, r2)
        if not log:
            continue
        print(f"\n{play[:36]} p{page} (top: {(owner or '?').split('@')[0]})")
        for l in log:
            print(f"  {l}")
        if args.push:
            client.push_transcript(
                COL, ids[play], page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note=NOTE, tool_name="YiDraCor-annotation-pipeline")
            pushed += 1
            print("  → pushed")
    print(f"\n{'PUSHED' if args.push else 'DRY RUN'}: {pushed} pages")


if __name__ == "__main__":
    raise SystemExit(main())
