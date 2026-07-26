"""Follow-ups from the 2026-07-26 speaker-who session: Di Seder supplement + Blimele cast.

Di Seder pages 55-70 are Noa's new song-supplement annotation (pushed since
07-20); lint surfaced 25 flags. Rule-level fixes, all against live tops:

  - `heading` (untyped) on song numbers / strophe numerals / the supplement's
    act-group header -> `head`, matching in-play precedent: song heads carry
    lg_id (cf. the existing `רעפריין:` head), act-group headers carry
    unit-type (cf. p61 `צווייטער אקט.`). Noa's span offsets kept verbatim.
  - no-xmlid speaker rubrics -> resolved per §G: named rubric = the character
    (קופלעי וואסיליע -> vasile_talhar), named duet = space-sep ids (S4),
    voice/group rubrics = their §G.4 ids (kor/alle/mener/alt/duet).
  - `וואלצער:` / `דועט` section rubrics that are NOT turns -> head with lg_id.
  - untagged collective turns (ביידע p60, סאפראן p70) -> speaker spans.

cast_dict rides along:
  - Di Seder: + mener/damen/beyde collectives, + bas/tenor/duet §G.4 voices
    (all already used as span ids -- were dangling).
  - Blimele: + the 10 printed castList ensemble_* roles (were never
    extracted), + mener/damen collectives; castList role tag ensemble_chor
    -> chor (dual id: body's 28 chor spans + cast_dict entry are canonical).

  python3.11 -m annotation.fix_supplement_flags_2026_07_26 --dry-run
  python3.11 -m annotation.fix_supplement_flags_2026_07_26 --push
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from annotation.lint_pages import REPO
from transkribus.client import TrpClient

NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
_TRAIL = ":׃־ .,"
DISEDER = "Di_seyder_nakht_Emkroyt_1908"
BLIMELE = "Blimele-AhronFaust1903"
NOTE = "speaker-who follow-ups 2026-07-26"

# (page, line_id) -> xmlid for existing no-xmlid speaker spans (Di Seder)
SET_WHO = {
    (57, "line_1649532119485_953"): "kor",                    # קאָהר
    (57, "l_3"): "duet",                                      # דועט rubric (in-play precedent)
    (58, "TextRegion_1649532371978_1045l4"): "karl_rizvan rashel",  # דועט קארל און ראשעל
    (61, "TextRegion_1649533051421_1444l5"): "vasile_talhar",  # קופלעי וואסיליע (§G named rubric)
    (63, "TextRegion_1649533464042_1744l17"): "mener",
    (63, "TextRegion_1649533464042_1744l26"): "alle",
    (63, "line_1649533648504_1825"): "alle",
    (67, "TextRegion_1649534095026_2112l28"): "alt",          # סאלאָ אַלט
}

# (page, line_id) -> xmlid: lines with NO speaker span yet (collective turns)
ADD_SPEAKER = {
    (60, "region_1649532950552_1410l8"): "beyde",             # ביידע: געטריי צוּ זיין.
    (70, "TextRegion_1649421514322_415l20"): "sopran",        # סאפּראן: זה היום
}

# (page, line_id): untyped `heading` -> `head` + lg_id (derived from the next
# lg/l in document order; omitted when the following song is un-numbered).
# The act-group header instead stays `heading` and gets typed like p67's
# existing דריטער אקט: type:act n:N subtype:songGroup.
RETAG_HEAD = {
    (59, "TextRegion_1649532660213_1267l2"),   # Nr. 3.
    (61, "TextRegion_1649533051421_1444l4"),   # Nr. 4.
    (61, "r_7_1l1"),                           # Nr. 5.
    (62, "TextRegion_1649533285594_1649l14"),  # Nr. 6.
    (65, "line_1649533958065_1992"),           # I.
    (66, "r_1_1l17"),                          # II.
    (66, "r_1_1l30"),                          # III.
    (67, "l_1"),                               # .Nr. 7
    (68, "line_1649534215212_2211"),           # Nr. 8.
    (68, "line_1649448623554_1712"),           # I.
    (69, "line_1649448439192_1597"),           # II.
    (70, "line_1649422008641_462"),            # Nr. 9.
}
ACT_HEAD = {(70, "line_1649421995830_451"): "4"}  # פירטער אקט. → type:act n:4 subtype:songGroup

# (page, line_id): speaker span that is a section rubric, not a turn -> head
SPEAKER_TO_HEAD = {(57, "l")}                  # וואלצער:

# Blimele body flags (same conventions; S4 joints per mappings-tab precedent
# 'ליעפע דאניאל זעליקל' -> "liepe doktor_daniel zelikel_mnagen")
BLIMELE_SET_WHO = {
    (62, "TextRegion_1649023101499_2721l16"): "ensemble_fier_araber",  # אראבּער
    (62, "TextRegion_1649023101499_2721l21"): "chor",                  # קאר
    (66, "r_3_1l13"): "doktor_daniel blimele tsierele zelikel_mnagen",
    (66, "r_3_1l17"): "doktor_daniel blimele zelikel_mnagen tsierele",
}
BLIMELE_RETAG_HEAD = {
    (35, "r_2_1l12"),                          # No. 1
    (36, "line_1649026068853_4765"),           # No. 2
    (36, "TextRegion_1649021803322_1112l16"),  # No. 3
}


def doc_order_lg_id(root, line_id):
    """lg_id of the first lg/l span at-or-after line_id in document order."""
    seen = False
    for tl in root.iter(f"{NS}TextLine"):
        if tl.get("id") == line_id:
            seen = True
            continue
        if not seen:
            continue
        for tag, a in parse_custom(tl.get("custom") or ""):
            if tag == "lg" and a.get("n"):
                return a["n"]
            if tag in ("l", "lg") and a.get("lg_id"):
                return a["lg_id"]
    return None


def edit_page(root, page, SET_WHO=None, ADD_SPEAKER=None, RETAG_HEAD=None,
              ACT_HEAD=None, SPEAKER_TO_HEAD=None):
    g = globals()
    SET_WHO = SET_WHO if SET_WHO is not None else g["SET_WHO"]
    ADD_SPEAKER = ADD_SPEAKER if ADD_SPEAKER is not None else g["ADD_SPEAKER"]
    RETAG_HEAD = RETAG_HEAD if RETAG_HEAD is not None else g["RETAG_HEAD"]
    ACT_HEAD = ACT_HEAD if ACT_HEAD is not None else g["ACT_HEAD"]
    SPEAKER_TO_HEAD = (SPEAKER_TO_HEAD if SPEAKER_TO_HEAD is not None
                       else g["SPEAKER_TO_HEAD"])
    log = []
    for tl in root.iter(f"{NS}TextLine"):
        lid = tl.get("id")
        u = tl.find(f".//{NS}Unicode")
        txt = (u.text or "") if u is not None else ""
        entries = parse_custom(tl.get("custom") or "")
        dirty = False

        if (page, lid) in SET_WHO:
            want = SET_WHO[(page, lid)]
            for tag, a in entries:
                if tag == "speaker" and not a.get("xmlid"):
                    off, end = int(a.get("offset", 0)), int(a.get("offset", 0)) + int(a.get("length", 0))
                    while end > off and end <= len(txt) and txt[end - 1] in _TRAIL:
                        end -= 1
                    a["length"], a["xmlid"] = str(end - off), want
                    log.append(f"{lid}: +xmlid:{want}  [{txt[off:end]!r}]")
                    dirty = True

        if (page, lid) in ADD_SPEAKER:
            want = ADD_SPEAKER[(page, lid)]
            if not any(t == "speaker" for t, _ in entries):
                cut = txt.find(":")
                end = cut if cut != -1 else len(txt)
                while end > 0 and txt[end - 1] in _TRAIL:
                    end -= 1
                entries.append(("speaker", {"offset": "0", "length": str(end),
                                            "xmlid": want}))
                log.append(f"{lid}: +speaker xmlid:{want}  [{txt[:end]!r}]")
                dirty = True

        if (page, lid) in RETAG_HEAD or (page, lid) in ACT_HEAD:
            out = []
            for tag, a in entries:
                if tag == "heading":
                    a = {k: v for k, v in a.items() if k in ("offset", "length")}
                    if (page, lid) in ACT_HEAD:
                        a.update(type="act", n=ACT_HEAD[(page, lid)],
                                 subtype="songGroup")
                        log.append(f"{lid}: heading typed act n:{a['n']} "
                                   f"subtype:songGroup  [{txt[:16]!r}]")
                        out.append(("heading", a)); dirty = True
                        continue
                    lg = doc_order_lg_id(root, lid)
                    if lg:
                        a["lg_id"] = lg
                    log.append(f"{lid}: heading → head lg_id:{lg}  [{txt[:16]!r}]")
                    out.append(("head", a)); dirty = True
                else:
                    out.append((tag, a))
            entries = out

        if (page, lid) in SPEAKER_TO_HEAD:
            out = []
            for tag, a in entries:
                if tag == "speaker" and not a.get("xmlid"):
                    off, end = int(a.get("offset", 0)), int(a.get("offset", 0)) + int(a.get("length", 0))
                    while end > off and end <= len(txt) and txt[end - 1] in _TRAIL:
                        end -= 1
                    lg = doc_order_lg_id(root, lid)
                    h = {"offset": str(off), "length": str(end - off)}
                    if lg:
                        h["lg_id"] = lg
                    out.append(("head", h))
                    log.append(f"{lid}: speaker → head lg_id:{lg}  [{txt[off:end]!r}]")
                    dirty = True
                else:
                    out.append((tag, a))
            entries = out

        if dirty:
            tl.set("custom", serialize_custom(entries))
    return log


def rename_blimele_castlist_chor(root):
    log = []
    for tl in root.iter(f"{NS}TextLine"):
        entries, dirty = parse_custom(tl.get("custom") or ""), False
        for tag, a in entries:
            if tag == "role" and a.get("xmlid") == "ensemble_chor":
                a["xmlid"] = "chor"
                log.append(f"{tl.get('id')}: [role] ensemble_chor → chor")
                dirty = True
        if dirty:
            tl.set("custom", serialize_custom(entries))
    return log


BLIMELE_ENSEMBLE = [  # printed castList p6 ensemble roles (labels verified on page)
    ("ensemble_folk", "פֿאלק"), ("ensemble_yuden", "יודען"),
    ("ensemble_polen", "פאָלען"), ("ensemble_geste", "געסטע"),
    ("ensemble_maskirte_geste", "מאַסקירטע געסטע"),
    ("ensemble_fier_araber", "פֿיער אַראַבער"), ("ensemble_tsvey", "צוויי"),
    ("ensemble_toyben", "טויבען"),
    ("ensemble_eyn_tseyginer_paar", "איין ציגיינער פאַאַר"),
    ("ensemble_yeger", "יעגער"),
]


def update_cast_dicts(dry):
    def save(play, d):
        if not dry:
            (REPO / "data" / play / "cast_dict.json").write_text(
                json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    p = REPO / "data" / DISEDER / "cast_dict.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    for xid, form, kind in (("mener", "מענער", "coll"), ("damen", "דאמען", "coll"),
                            ("beyde", "ביידע", "coll"), ("bas", "באס", "voice"),
                            ("tenor", "טענאר", "voice"), ("duet", "דועט", "voice")):
        if xid in d["roles"]:
            continue
        if kind == "coll":
            d["roles"][xid] = {"form": form, "bare": form, "collective": True,
                               "source": NOTE}
        else:
            d["roles"][xid] = {"form": form, "bare": form, "loc": None,
                               "prefix_variants": [], "printed": False,
                               "note": "abstract song-supplement voice (§G.4); "
                                       f"not in printed castList ({NOTE})"}
        print(f"  cast_dict {DISEDER}: +{kind} {xid}")
    save(DISEDER, d)

    p = REPO / "data" / BLIMELE / "cast_dict.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    for xid, form in BLIMELE_ENSEMBLE:
        if xid not in d["roles"]:
            d["roles"][xid] = {"form": form, "bare": form, "collective": True,
                               "loc": {"page": 6}, "source":
                               f"printed castList ensemble section ({NOTE})"}
            print(f"  cast_dict {BLIMELE}: +ensemble {xid}")
    for xid, form in (("mener", "מענער"), ("damen", "דאמען")):
        if xid not in d["roles"]:
            d["roles"][xid] = {"form": form, "bare": form, "collective": True,
                               "source": NOTE}
            print(f"  cast_dict {BLIMELE}: +collective {xid}")
    if "ensemble_chor" in d["roles"]:   # merged into chor
        del d["roles"]["ensemble_chor"]
    save(BLIMELE, d)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()
    dry = not args.push

    print("— cast_dict updates —")
    update_cast_dicts(dry)

    ids = load_doc_ids()
    client = TrpClient.from_env(); client.login()
    n = 0
    diseder_pages = sorted({pg for pg, _ in list(SET_WHO) + list(ADD_SPEAKER)
                            + list(RETAG_HEAD) + list(ACT_HEAD) + list(SPEAKER_TO_HEAD)})
    blimele_pages = sorted({pg for pg, _ in
                            list(BLIMELE_SET_WHO) + list(BLIMELE_RETAG_HEAD)})
    work = ([(DISEDER, pg) for pg in diseder_pages] + [(BLIMELE, 6)]
            + [(BLIMELE, pg) for pg in blimele_pages])
    for play, page in work:
        tsid, owner, xml = top_transcript(client, ids[play], page)
        if xml is None:
            print(f"{play} p{page}: no transcript — SKIP"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        if play == BLIMELE and page == 6:
            log = rename_blimele_castlist_chor(root)
        elif play == BLIMELE:
            log = edit_page(root, page, SET_WHO=BLIMELE_SET_WHO, ADD_SPEAKER={},
                            RETAG_HEAD=BLIMELE_RETAG_HEAD, ACT_HEAD={},
                            SPEAKER_TO_HEAD=set())
        else:
            log = edit_page(root, page)
        if not log:
            continue
        print(f"\n{play[:36]} p{page} (top: {(owner or '?').split('@')[0]}, ts {tsid})")
        for l in log:
            print(f"  {l}")
        if args.push:
            client.push_transcript(
                COL, ids[play], page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note=NOTE, tool_name="YiDraCor-annotation-pipeline")
            n += 1
            print("  → pushed")
    print(f"\n{'PUSHED' if args.push else 'DRY RUN'}: {n} pages")


if __name__ == "__main__":
    raise SystemExit(main())
