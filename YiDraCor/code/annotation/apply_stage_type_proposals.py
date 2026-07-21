"""Apply the 2026-07-20 stage-type proposals and defect fixes to the live top.

Companion to `data/review/stage_type_proposals_2026-07-20.csv`. Each edit is
addressed by (play, page, line_id, offset) and is a no-op if already applied.

Two proposals in that CSV are deliberately NOT applied here:
  * Ezra p30 `ביבס` — almost certainly OCR for `ביסס`. Per §7 the printed text
    is wrong, so it belongs in Judith's transcript queue; typing the span would
    bake in the misreading. Left bare on purpose.
  * the 3 rows marked ask-noa — 5.2a/b/c in her handoff.

  python3.11 -m annotation.apply_stage_type_proposals --dry-run
  python3.11 -m annotation.apply_stage_type_proposals --push
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from transkribus.client import TrpClient

NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"

# (play, page, line_id, offset) -> (action, payload)
#   settype  : set @type on the stage span at that offset
#   retag    : change the tag itself, keeping offset/length
#   tospeaker: stage → speaker{xmlid}
#   drop     : remove the stage span entirely
#   scope    : shrink the stage span to (new_offset, new_length) and set @type
EDITS = {
    # --- typed proposals -----------------------------------------------------
    ("AlNaharotBavel-Amkreut&Freund1909", 9, "r1l13", 8):  ("settype", "delivery"),
    ("AlNaharotBavel-Amkreut&Freund1909", 9, "r1l31", 41): ("settype", "business"),
    ("DerManUnterTiff", 10, "r_2_1l11", 59): ("settype", "exit"),
    ("DerManUnterTiff", 10, "r_2_1l14", 63): ("settype", "business"),
    ("Ezra-Emkroyt1908", 4, "line_1638791957851_2791", 0): ("settype", "setting"),
    ("SoreSheyndel", 31, "r1l30", 5): ("settype", "delivery"),
    ("דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete", 19, "r_3_1_tl_4", 14):
        ("settype", "entrance"),
    ("דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete", 21, "r_3_1_tl_11", 70):
        ("settype", "exit"),
    # --- B9 → option C: entrance+exit is enumerable, so not `mixed` ----------
    # Sinai 2026-07-20. `(יאָכטשע אָבּ, אויפטריט סאבּעלע…)` — Yokhtshe exits,
    # enter Sabele. The ONLY one of the 22 `mixed` spans in the corpus that is
    # genuinely entrance+exit; the other 21 are `mixed` standing in for other
    # combinations and need their own enumeration.
    ("DerManUnterTiff", 13, "r_2_1l18", 0): ("settype", "exit entrance"),
    # --- the other 21 `mixed` spans, enumerated per option C -----------------
    # `mixed` had been used as a catch-all for any compound direction. Under
    # ST3 it is reserved for functions that CANNOT be enumerated, and all of
    # these can. Continuation lines take the parent direction's type, the same
    # way apply_opening_setting carries a tableau across its lines (ST9).
    #
    # entrance + business — "enter X, who then does Y":
    ("Blimele-AhronFaust1903", 28, "TextRegion_1649021403901_561l3", 0):
        ("settype", "entrance business"),       # cont. of parent already so typed
    ("DerManUnterTiff", 7, "r_2_1l18", 0):  ("settype", "entrance business"),
    ("DerManUnterTiff", 7, "r_2_1l19", 0):  ("settype", "entrance business"),
    ("DerManUnterTiff", 12, "r_4_1l11", 0): ("settype", "entrance business"),
    ("DerManUnterTiff", 12, "r_4_1l12", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 17, "r_1_1l22", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 20, "r_2_1l3", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 20, "r_2_1l4", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 20, "r_2_1l5", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 20, "r_2_1l6", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 23, "r_4_1l7", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 23, "r_4_1l8", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 23, "r_4_1l24", 0): ("settype", "entrance business"),
    ("Di_seyder_nakht_Emkroyt_1908", 23, "r_4_1l25", 0): ("settype", "entrance business"),
    # exit + business — "X exits, and Y happens":
    ("Di_seyder_nakht_Emkroyt_1908", 6, "r_2_1l12", 11): ("settype", "exit business"),
    ("Di_seyder_nakht_Emkroyt_1908", 39, "TextRegion_1649446606027_461l35", 17):
        ("settype", "exit business"),
    ("Yudale_der_blinder,_Emkroyt1908", 48, "TextRegion_1648553814193_4366l22", 0):
        ("settype", "exit business"),          # "…he seizes her, leads her off (אב)"
    # exit + entrance — Meir Dreyer comes in, runs out mid-song, returns with
    # David and Miriam. The exit is `לויפט ער ארויס`, not `אב`, which is why the
    # cue test missed it; option C types by function, not by cue word.
    ("Di_seyder_nakht_Emkroyt_1908", 18, "TextRegion_1647706671180_1553l20", 0):
        ("settype", "exit entrance"),
    ("Di_seyder_nakht_Emkroyt_1908", 18, "TextRegion_1647706671180_1553l21", 0):
        ("settype", "exit entrance"),
    # business + delivery — a physical state colouring the speech that follows.
    ("Yudale_der_blinder,_Emkroyt1908", 39, "TextRegion_1648550742599_2532l10", 8):
        ("settype", "business delivery"),
    # Not compound at all: a scene-change tableau, and the line before it is
    # the `פערוואַנדלונג` cue (ST6).
    ("Yudale_der_blinder,_Emkroyt1908", 51, "TextRegion_1648548404769_1388l2", 0):
        ("settype", "setting"),
    # --- defect fixes --------------------------------------------------------
    # A song rubric ("Couplet of Vasilye", after `Nr. 4.`), not a direction.
    ("Di_seyder_nakht_Emkroyt_1908", 61, "TextRegion_1649533051421_1444l5", 0):
        ("retag", "head"),
    # The span sat over the speaker NAME; the direction on the same line is
    # already correctly typed business.
    ("Ezra-Emkroyt1908", 5, "line_1638824525750_1938", 0): ("tospeaker", "valentin"),
    # `ביסס יאַ ראכע` — only the first word is the repeat mark (now typed
    # `delivery` per the 2026-07-21 decision). Scope the span to it; the sung
    # remainder is left alone, song structure being deferred to Noa's report.
    ("Ezra-Emkroyt1908", 8, "line_1639248760763_356", 0): ("scope", (0, 4, "delivery")),
    # An act heading is not a stage direction; the line already carries
    # heading{type:act,n:2} from the 07-20 sweep.
    ("HinkePinke", 30, "TextRegion_1649691550551_2379l26", 0): ("drop", None),
}


def apply_edit(root, line_id: str, offset: int, action: str, payload) -> str | None:
    for el in root.iter(f"{NS}TextLine"):
        if el.get("id") != line_id:
            continue
        entries = parse_custom(el.get("custom") or "")
        out, done = [], None
        for tag, a in entries:
            if tag != "stage" or int(a.get("offset", -1)) != offset:
                out.append((tag, a)); continue
            if action == "settype":
                if a.get("type") == payload:
                    return None                      # already applied
                a = dict(a); a["type"] = payload
                out.append(("stage", a)); done = f"stage → type:{payload}"
            elif action == "retag":
                out.append((payload, {k: v for k, v in a.items() if k != "type"}))
                done = f"stage → <{payload}>"
            elif action == "tospeaker":
                out.append(("speaker", {"offset": a["offset"], "length": a["length"],
                                        "xmlid": payload}))
                done = f"stage → speaker xmlid:{payload}"
            elif action == "scope":
                o, ln, t = payload
                if (int(a.get("length", -1)) == ln and a.get("type") == t):
                    return None                      # already scoped and typed
                out.append(("stage", {"offset": str(o), "length": str(ln), "type": t}))
                done = f"stage scoped to ({o},{ln}) type:{t}"
            elif action == "drop":
                done = "stage span dropped"
                continue
        if done:
            el.set("custom", serialize_custom(out))
        return done
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()

    ids = load_doc_ids()
    client = TrpClient.from_env(); client.login()
    by_page: dict[tuple, list] = {}
    for (play, page, lid, off), (action, payload) in EDITS.items():
        by_page.setdefault((play, page), []).append((lid, off, action, payload))

    n = 0
    for (play, page), edits in sorted(by_page.items()):
        doc = ids.get(play)
        if doc is None:
            print(f"{play}: no doc id — skip"); continue
        tsid, owner, xml = top_transcript(client, doc, page)
        if xml is None:
            print(f"{play} p{page}: no transcript — skip"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        done = []
        for lid, off, action, payload in edits:
            r = apply_edit(root, lid, off, action, payload)
            if r:
                done.append(f"  {lid}@{off}: {r}")
        if not done:
            continue
        n += len(done)
        print(f"\n{play[:34]} p{page} (top: {owner.split('@')[0]})")
        for d in done:
            print(d)
        if args.push:
            client.push_transcript(
                COL, doc, page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note="stage-type proposals + defect fixes 2026-07-20",
                tool_name="YiDraCor-annotation-pipeline")
            print(f"  → pushed (parent {tsid})")
    print(f"\n{'PUSHED' if args.push else 'DRY RUN — nothing written'}: {n} edits")


if __name__ == "__main__":
    raise SystemExit(main())
