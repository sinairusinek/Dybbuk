"""Apply Noa's speaker-who review answers (sheet of 2026-07-21, returned 2026-07-26).

Source: docs/speaker_who_review_2026-07-21.xlsx tabs "Missing who" (33 rows,
all answered) and "Mappings to check" (38 rows, all confirmed OK — no edits).

Three kinds of change, all against the LIVE top transcript per page:

1. DUAL-ID MERGE — Noa's "צריך לאחד עם העברית" rows exposed a systematic
   problem: DosYudisheHerts and HinkePinke carry TWO ids per character (the
   auto-generated castList skeleton id + a readable id used by later tagging).
   Per Sinai 2026-07-26 the READABLE id is canonical; every xmlid token is
   rewritten (speaker/role/stage alike), so the castList role tags migrate too.
   HinkePinke's dangling `kor` folds into its cast `chor` in the same pass.

2. WHO EDITS — set xmlid on the specific no-xmlid spans from "Missing who"
   (keys are (play, page, line_id) from speaker_who_occurrences_2026-07-21.tsv).
   Spans get the S1 trailing-colon trim, same as apply_speaker_xmlids.

3. TEXT FIX — Dos Yudishe Kind p.12 line `l`: Noa: the label was
   mis-transcribed זַיי; the print reads זי. Fix the transcript and re-anchor
   the spans on that line.

cast_dict.json updates ride along (local, idempotent): key renames for the
merge plays, new body-only role `shadkhn` (Di Seder §S6), new collectives
`froyen`/`andere` (Herts, Global E), variant ראזעלע → roza.

  python3.11 -m annotation.apply_speaker_who_answers --dry-run
  python3.11 -m annotation.apply_speaker_who_answers --push
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
KIND = "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete"
HERTS = "DosYudisheHerts-1910"
NOTE = "Noa speaker-who review answers 2026-07-26"

# 1. Canonical readable id per character (verified: identical surface labels
#    on both ids, id_labels scan 2026-07-26).
MERGE = {
    HERTS: {
        "rz": "roza", "hermn": "herman", "viktr": "viktor", "lid": "lida",
        "kmisr": "komisar", "lmkh": "lemekh_kretshmer",
        "iekb_shteren": "yankev_shtern",
    },
    "HinkePinke": {
        "khinke": "hinke", "finke": "pinke", "hertsg": "hertsog",
        "dinh": "dina", "gbril": "gavriel", "drin": "adryan",
        "dr_brhm": "dr_avrohom", "fzsh": "pazsh",
        "kor": "chor",   # dangling — chor is the cast id, kor never was
    },
}

# 2. (play, page, line_id) -> xmlid. All from the "Missing who" tab; values
#    already use post-merge canonical ids.
WHO_EDITS = {
    ("Blimele-AhronFaust1903", 27, "line_1649535651843_3345"): "chor",  # מענער קאר
    ("Blimele-AhronFaust1903", 27, "line_1649535558270_3263"): "chor",  # דאמען קאר
    ("Blimele-AhronFaust1903", 33, "r_1_1l14"): "chor",                 # יעגער קאר
    ("Blimele-AhronFaust1903", 41, "r_1_1l19"): "alle",                 # אלע צוזאמען
    ("Di_seyder_nakht_Emkroyt_1908", 65, "TextRegion_1649533891997_1934l27"):
        "shadkhn",                                                      # דער שדכן — new role
    (HERTS, 5, "r1l21"): "froyen",                                      # פֿרויען — new collective
    (HERTS, 5, "r1l23"): "andere",                                      # אַנדערע — new collective
    (HERTS, 65, "r1l10"): "roza",                                       # ראָזעלע
    (HERTS, 68, "r1l3"): "roza",                                        # ראָזעלע
    ("HinkePinke", 5, "r_3_1l4"): "chor",                               # יעגער-כאר
    ("KidushHashem", 8, "r1l2"): "tobyas",   # solo Tobias, NOT the duet id
    ("KidushHashem", 8, "r1l4"): "tobyas",
    (KIND, 12, "r_3_1_tl_19"): "rov",                                   # רב זינגט
    (KIND, 12, "l"): "henele",               # זַיי — see TEXT_FIXES
    (KIND, 32, "l_10"): "henele", (KIND, 32, "l_8"): "vladislav",
    (KIND, 32, "l_4"): "vladislav", (KIND, 32, "l_2"): "henele",
    (KIND, 33, "l_3"): "henele", (KIND, 33, "r_4_1_tl_4"): "henele",
    (KIND, 33, "l_5"): "henele",
    (KIND, 50, "l_1"): "shmerl", (KIND, 50, "l_2"): "shprintse",
    (KIND, 51, "l_3"): "shmerl", (KIND, 51, "l_2"): "shprintse",
    (KIND, 51, "l_1"): "shmerl", (KIND, 51, "l"): "shprintse",
    (KIND, 52, "l_6"): "shmerl", (KIND, 52, "l_3"): "shprintse",
    (KIND, 52, "l_2"): "shmerl", (KIND, 52, "l_1"): "shprintse",
    (KIND, 52, "l"): "shmerl",
}

# 3. (play, page, line_id) -> (wrong, right): fix the transcript label.
TEXT_FIXES = {(KIND, 12, "l"): ("זַיי", "זי")}


def rewrite_xmlids(root, id_map):
    """Map every whole xmlid token on every custom entry. Returns change log."""
    changed = []
    for tl in root.iter(f"{NS}TextLine"):
        entries, dirty = parse_custom(tl.get("custom") or ""), False
        for tag, a in entries:
            if "xmlid" not in a:
                continue
            toks = a["xmlid"].split()
            new = [id_map.get(t, t) for t in toks]
            if new != toks:
                changed.append(f"{tl.get('id')}: [{tag}] "
                               f"{' '.join(toks)} → {' '.join(new)}")
                a["xmlid"] = " ".join(new)
                dirty = True
        if dirty:
            tl.set("custom", serialize_custom(entries))
    return changed


def fix_text(root, line_id, wrong, right):
    """Replace `wrong`→`right` once at the head of the line; shift spans."""
    for tl in root.iter(f"{NS}TextLine"):
        if tl.get("id") != line_id:
            continue
        u = tl.find(f".//{NS}Unicode")
        txt = u.text or ""
        pos = txt.find(wrong)
        if pos == -1:
            return None if right in txt else f"!! {line_id}: {wrong!r} not in {txt[:24]!r}"
        u.text = txt[:pos] + right + txt[pos + len(wrong):]
        delta = len(right) - len(wrong)
        entries = parse_custom(tl.get("custom") or "")
        for tag, a in entries:
            if "offset" not in a:
                continue
            off, ln = int(a["offset"]), int(a.get("length", 0))
            if off > pos:
                a["offset"] = str(off + delta)
            elif off <= pos < off + ln:            # span covers the fix
                a["length"] = str(ln + delta)
        tl.set("custom", serialize_custom(entries))
        return f"{line_id}: text {wrong!r} → {right!r} (spans shifted {delta:+d})"
    return f"!! {line_id}: line not found"


def set_xmlid(root, line_id, xmlid):
    """Set xmlid on the line's no-xmlid speaker span; S1-trim the trailing colon."""
    for tl in root.iter(f"{NS}TextLine"):
        if tl.get("id") != line_id:
            continue
        u = tl.find(f".//{NS}Unicode")
        txt = u.text if u is not None and u.text else ""
        entries = parse_custom(tl.get("custom") or "")
        for tag, a in entries:
            if tag != "speaker":
                continue
            if a.get("xmlid"):
                if a["xmlid"] == xmlid:
                    return None                     # already applied
                continue
            off, end = int(a.get("offset", 0)), int(a.get("offset", 0)) + int(a.get("length", 0))
            while end > off and end <= len(txt) and txt[end - 1] in _TRAIL:
                end -= 1
            a["length"], a["xmlid"] = str(end - off), xmlid
            tl.set("custom", serialize_custom(entries))
            return f"{line_id}: +xmlid:{xmlid}  [{txt[off:end]!r}]"
        return f"!! {line_id}: no un-id'd speaker span (custom={tl.get('custom')!r:.80})"
    return f"!! {line_id}: line not found"


def update_cast_dicts(dry):
    """Local cast_dict.json edits: renames + new roles/collectives + variant."""
    def save(play, d):
        if not dry:
            p = REPO / "data" / play / "cast_dict.json"
            p.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")

    for play, id_map in MERGE.items():
        p = REPO / "data" / play / "cast_dict.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        renamed = []
        d["roles"] = {id_map.get(k, k): v for k, v in d["roles"].items()}
        for old, new in id_map.items():
            if old != new and new in d["roles"] and old not in d["roles"]:
                renamed.append(f"{old}→{new}")
        for old, new in id_map.items():
            if new in d["roles"]:
                d["roles"][new].setdefault("notes", []).append(
                    f"{NOTE}: canonical id (merged duplicate {old})")
        print(f"  cast_dict {play}: renamed {', '.join(renamed) or '(none)'}")
        save(play, d)

    p = REPO / "data" / HERTS / "cast_dict.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    if "roza" in d["roles"] and "ראזעלע" not in d["roles"]["roza"].setdefault("prefix_variants", []):
        d["roles"]["roza"]["prefix_variants"].append("ראזעלע")
        d["roles"]["roza"].setdefault("notes", []).append(
            f"{NOTE}: ראָזעלע (pp. 65, 68) is Roza — diminutive variant")
    for xid, form, bare in (("froyen", "פֿרויען", "פרויען"),
                            ("andere", "אַנדערע", "אנדערע")):
        if xid not in d["roles"]:
            d["roles"][xid] = {"form": form, "bare": bare, "collective": True,
                               "source": f"{NOTE} (p.5, Global E)"}
            print(f"  cast_dict {HERTS}: +collective {xid}")
    save(HERTS, d)

    p = REPO / "data" / "Di_seyder_nakht_Emkroyt_1908" / "cast_dict.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    if "shadkhn" not in d["roles"]:
        d["roles"]["shadkhn"] = {
            "form": "דער שדכן", "bare": "דער שדכן",
            "prefix_variants": ["שדכן"], "source": "body",
            "notes": [f"{NOTE}: p.65 body-only role per §S6"]}
        print("  cast_dict Di_seyder: +role shadkhn")
    save("Di_seyder_nakht_Emkroyt_1908", d)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    args = ap.parse_args()
    dry = not args.push

    print("— cast_dict updates —")
    update_cast_dicts(dry)

    # page work-list: every page of a merge play + the WHO_EDITS/TEXT_FIXES pages
    ids = load_doc_ids()
    pages: dict[tuple[str, int], None] = {}
    for play in MERGE:
        pa = REPO / "data" / play / "page_annotated"
        for f in sorted(pa.glob("0*.xml")):
            pages[(play, int(f.name[:4]))] = None
    for (play, page, _lid) in list(WHO_EDITS) + list(TEXT_FIXES):
        pages[(play, page)] = None

    client = TrpClient.from_env(); client.login()
    pushed = skipped = 0
    problems = []
    for play, page in sorted(pages):
        doc = ids.get(play)
        if args.only and play != args.only:
            continue
        if doc is None:
            problems.append(f"{play}: no doc id"); continue
        tsid, owner, xml = top_transcript(client, doc, page)
        if xml is None:
            problems.append(f"{play} p{page}: no transcript"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        log = []
        if play in MERGE:
            log += rewrite_xmlids(root, MERGE[play])
        for (p_, pg_, lid), (wrong, right) in TEXT_FIXES.items():
            if (p_, pg_) == (play, page):
                r = fix_text(root, lid, wrong, right)
                if r: log.append(r)
        for (p_, pg_, lid), xmlid in WHO_EDITS.items():
            if (p_, pg_) == (play, page):
                r = set_xmlid(root, lid, xmlid)
                if r: log.append(r)
        if not log:
            skipped += 1; continue
        bad = [l for l in log if l.startswith("!!")]
        problems += [f"{play} p{page} {b}" for b in bad]
        print(f"\n{play[:40]} p{page} (top: {(owner or '?').split('@')[0]}, ts {tsid})")
        for l in log:
            print(f"  {l}")
        if args.push and not bad:
            client.push_transcript(
                COL, doc, page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note="speaker-who answers + dual-id merge 2026-07-26",
                tool_name="YiDraCor-annotation-pipeline")
            pushed += 1
            print("  → pushed")

    print(f"\n{'PUSHED' if args.push else 'DRY RUN'}: {pushed} pages"
          f" ({skipped} unchanged)")
    if problems:
        print("\nPROBLEMS — resolve before/instead of pushing these:")
        for p_ in problems:
            print(f"  {p_}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
