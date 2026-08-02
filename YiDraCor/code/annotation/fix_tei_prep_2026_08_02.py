"""Pre-TEI-build fixes surfaced by the first corpus-wide build_tei run.

1. SoreSheyndel is the last dual-id play (castList skeleton ids vs readable
   ids in later tagging). Readable canonical per the standing 2026-07-26
   policy: brhmele→avromele, bbele→babele, gimfel→gimpel, shfse→shabse,
   shrh_sheyndel→sore_sheyndel — cast_dict keys + every span rewritten.
2. Collectives used by spans but never declared (build_tei bad-@who list):
   AlNaharot/Yudale alle+beyde; Mishke alle+beyde+chor+eyner; Sore mener+chor.
3. Orphan lines the builder would drop, tagged live per existing rules:
   - multi-line act/scene-opening descriptions missing their stage span →
     stage{type:setting} (ST7/ST7b; continuation lines of the same
     parenthesis get continued:true per ST9).
   - Sore running page-headers 'שרה שיינדעל' → fw{type:header}.
   - HinkePinke p63 appended-couplet intro note → stage{type:novelistic}
     (the couplet itself is already l/lg-tagged).
   Cover stamps, back-catalog ads and Dovid's p71 promotional page stay
   untagged by design (Noa 06-28: non-theatrical) and are dropped from TEI.

  python3.11 -m annotation.fix_tei_prep_2026_08_02 --dry-run
  python3.11 -m annotation.fix_tei_prep_2026_08_02 --push
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
NOTE = "pre-TEI fixes 2026-08-02 (Sore id merge, collectives, opening blocks)"
VOWELS = re.compile(r"[֑-ׇ]")
def unvoc(s): return VOWELS.sub("", s)

SORE = "SoreSheyndel"
KIND = "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete"
MERGE = {SORE: {"brhmele": "avromele", "bbele": "babele", "gimfel": "gimpel",
                "shfse": "shabse", "shrh_sheyndel": "sore_sheyndel"}}

NEW_COLLECTIVES = {
    "AlNaharotBavel-Amkreut&Freund1909": [("alle", "אלע"), ("beyde", "ביידע")],
    "Yudale_der_blinder,_Emkroyt1908": [("alle", "אלע"), ("beyde", "ביידע")],
    "MishkeMashke-Kultur1910": [("alle", "אלע"), ("beyde", "ביידע"),
                                ("chor", "כאר"), ("eyner", "איינער")],
    SORE: [("mener", "מענער"), ("chor", "כאר")],
}

# (play, page) -> [(needle-in-unvocalized-text, tag, attrs_type, continued)]
# One entry per orphan line; `continued` marks non-first lines of a block.
SETTING = "setting"
TAG_LINES = {
    ("Di_seyder_nakht_Emkroyt_1908", 5): [
        ("שעהן מעבליערט, פערשיעדענע", "stage", SETTING, True),
        ("ביים אויפגעהן דעם פארהאנגס", "stage", SETTING, True),
        ("אנשטענדיג געקליידעט", "stage", SETTING, True),
        ("שיעדענע גאסען, איינער זיצט", "stage", SETTING, True),
        ("בלעטער נאטען אין די הענד", "stage", SETTING, True),
    ],
    ("DosYudisheHerts-1910", 5): [
        ("(אגארטען. בענק. טישלעך", "stage", SETTING, False),
        ("אריינגאנג צו דער קרעטשמע", "stage", SETTING, True),
        ("זיצען ביי די טישלעך", "stage", SETTING, True),
    ],
    ("DosYudisheHerts-1910", 28): [
        ("ביי למך'ן אין שטוב", "stage", SETTING, False),
        ("זיצען ביי א טישעל", "stage", SETTING, True),
    ],
    ("DosYudisheHerts-1910", 45): [
        ("(דאס זעלבע צימער וואס אין", "stage", SETTING, False),
    ],
    ("DosYudisheHerts-1910", 63): [
        ("(א צימער ביי למך'ן", "stage", SETTING, False),
        ("משה שטעהט און פאקט", "stage", SETTING, True),
    ],
    ("HinkePinke", 5): [
        ("(א גארטען, הערען אונד דאמען", "stage", SETTING, False),
        ("רעכטס איינגאנג צום הערצאג'ס", "stage", SETTING, True),
    ],
    ("HinkePinke", 30): [
        ("(איין קערקער, אויף דער ערד", "stage", SETTING, False),
    ],
    ("HinkePinke", 58): [
        ("(א פרייער פלאץ, ביים געריכט", "stage", SETTING, False),
        ("קייטען, סאלדאטען, פאלק", "stage", SETTING, True),
    ],
    ("HinkePinke", 63): [
        ("(דיעזער צוגעבענע קיפלעט", "stage", "novelistic", False),
        ("זייטע 50 ווען דיינקע", "stage", "novelistic", True),
    ],
    (SORE, 41): [
        ("(עס קומט פאר א צימער ביי גימפעל", "stage", SETTING, False),
        ("פארהאנג געהט אויף האלט", "stage", SETTING, True),
        ("עפעס, מיט זיינע משוררים", "stage", SETTING, True),
        ("סעודה).", "stage", SETTING, True),
    ],
    (SORE, 53): [
        ("(ביי רב יוחנצי)", "stage", SETTING, False),
    ],
    (SORE, 25): [("שרה שיינדעל", "fw", "header", False)],
    (KIND, 34): [
        ("הענעלע, וועכטער.", "stage", SETTING, False),
    ],
}
# Sore p41+p53 ALSO carry the running header line, same text as p25.
TAG_LINES[(SORE, 41)].append(("שרה שיינדעל", "fw", "header", False))
TAG_LINES[(SORE, 53)].append(("שרה שיינדעל", "fw", "header", False))


def edit_page(root, play, page):
    log = []
    jobs = list(TAG_LINES.get((play, page), []))
    for tl in root.iter(f"{NS}TextLine"):
        lid = tl.get("id")
        u = tl.find(f".//{NS}Unicode")
        txt = (u.text or "") if u is not None else ""
        bare = unvoc(txt)
        entries = parse_custom(tl.get("custom") or "")
        dirty = False

        if play in MERGE:
            for tag, a in entries:
                if "xmlid" in a:
                    toks = a["xmlid"].split()
                    new = [MERGE[play].get(t, t) for t in toks]
                    if new != toks:
                        log.append(f"{lid}: [{tag}] {a['xmlid']} → {' '.join(new)}")
                        a["xmlid"] = " ".join(new); dirty = True

        for job in list(jobs):
            needle, tag, ty, cont = job
            if needle not in bare.replace("  ", " "):
                continue
            jobs.remove(job)
            if any(t == tag for t, _ in entries):
                log.append(f"{lid}: already has {tag} — skip  [{bare[:24]!r}]")
                break
            attrs = {"offset": "0", "length": str(len(txt.rstrip()))}
            if tag == "stage":
                attrs["type"] = ty
                if cont:
                    attrs["continued"] = "true"
            else:
                attrs["type"] = ty
            entries.append((tag, attrs))
            log.append(f"{lid}: +{tag} type:{ty}"
                       f"{' continued' if cont and tag == 'stage' else ''}"
                       f"  [{bare[:30]!r}]")
            dirty = True
            break

        if dirty:
            tl.set("custom", serialize_custom(entries))
    for needle, tag, ty, _ in jobs:
        log.append(f"!! NOT FOUND on p{page}: {needle[:30]!r}")
    return log


def update_cast_dicts(dry):
    def save(play, d):
        if not dry:
            (REPO / "data" / play / "cast_dict.json").write_text(
                json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for play, id_map in MERGE.items():
        p = REPO / "data" / play / "cast_dict.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        d["roles"] = {id_map.get(k, k): v for k, v in d["roles"].items()}
        for old, new in id_map.items():
            if new in d["roles"]:
                d["roles"][new].setdefault("notes", []).append(
                    f"{NOTE}: canonical id (merged duplicate {old})")
        print(f"  cast_dict {play}: renamed {', '.join(f'{o}→{n}' for o, n in id_map.items())}")
        save(play, d)
    for play, pairs in NEW_COLLECTIVES.items():
        p = REPO / "data" / play / "cast_dict.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        added = []
        for xid, form in pairs:
            if xid not in d["roles"]:
                d["roles"][xid] = {"form": form, "bare": form,
                                   "collective": True, "source": NOTE}
                added.append(xid)
        if added:
            print(f"  cast_dict {play}: +collectives {', '.join(added)}")
        save(play, d)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()
    dry = not args.push

    print("— cast_dict updates —")
    update_cast_dicts(dry)

    pages: set[tuple[str, int]] = set(TAG_LINES)
    for play in MERGE:
        pa = REPO / "data" / play / "page_annotated"
        for f in sorted(pa.glob("0*.xml")):
            pages.add((play, int(f.name[:4])))

    ids = load_doc_ids()
    client = TrpClient.from_env(); client.login()
    pushed = 0
    problems = []
    for play, page in sorted(pages):
        tsid, owner, xml = top_transcript(client, ids[play], page)
        if xml is None:
            problems.append(f"{play} p{page}: no transcript"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        log = edit_page(root, play, page)
        if not log:
            continue
        bad = [l for l in log if l.startswith("!!")]
        problems += [f"{play} p{page} {b}" for b in bad]
        print(f"\n{play[:36]} p{page} (top: {(owner or '?').split('@')[0]})")
        for l in log:
            print(f"  {l}")
        if args.push and not bad:
            client.push_transcript(
                COL, ids[play], page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note=NOTE, tool_name="YiDraCor-annotation-pipeline")
            pushed += 1
            print("  → pushed")
    print(f"\n{'PUSHED' if args.push else 'DRY RUN'}: {pushed} pages")
    if problems:
        print("PROBLEMS:")
        for p_ in problems:
            print(f"  {p_}")


if __name__ == "__main__":
    raise SystemExit(main())
