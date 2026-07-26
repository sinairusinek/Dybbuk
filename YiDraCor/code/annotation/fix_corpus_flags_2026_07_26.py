"""Corpus lint-flag fixes, 2026-07-26 (after the legacy scan-set prune).

Driven by data/review/lint_flags_2026-07-26.csv plus Noa's answered handoff
(docs/handoff_noa_2026-06-28_flags.md). Runs BEFORE auto_resolve_flags:
converting Ezra's three head{unit-type}-only act headings to real typed
headings defuses auto_resolve's unconditional head{unit-type} drop, which
would otherwise erase the only act markers those pages have.

What it does (all against live tops, idempotent):
  - CSV-driven: untyped `heading` on song/section numerals -> `head`
    (+lg_id when a numbered song follows); stage.type typos busines/seting
    -> business/setting; bare `fw` in Ezra/Mishke/Yudale -> type:pageNum.
  - Ezra p15/21/29: head{unit-type:act} -> heading{type:act; n:2/3/4}.
  - Dovid's p62: heading n:6 -> n:4 ('.VI אַקט' is Roman IV mirrored in the
    RTL line; the play's acts run I..IV).
  - Noa 06-28 answers finally applied: Kind p45 `זעזעמיר)` + Herts p16
    `(שרייט)` -> stage{type:delivery}; Dovid's p71 promotional labels ->
    cast_dict non_speaker_labels.
  - IshaRaa p6 `אללע` -> meydkhen (Noa: the maidens, NOT alle — must land
    before apply_collective_speakers); p65 `פון:` -> perets (OCR of פרץ,
    dialogue alternation with חנוך matches his question two lines above).
  - Yudale p65 `דבורה` -> dvorele; Kind p15 `שפּריצע` -> shprintse; BasSheva
    p15 `אבנר, בנימין` -> one span, `bnr bnimin` (S4, per Noa).
  - Kind p23 `רעפריין:` speaker spans -> head (musical rubric); p12 `זינגט:`
    stage busines -> delivery (singing = performance instruction, same call
    as the ביס migration); Kidush p24 `קאָהר דאָס זעלבע` -> delivery
    xmlid:chor (M4b); DerMann p14 `(קלערט אביסיל)` -> business.
  - Role xmlids: BasSheva `עטצ.` -> etts; Mishke p4 castList quartet ->
    diener/matrozen/chinezer/pasazhiren; Kind p2 one role span over
    `יודען, גראפען` split into role:yudn + role:grafn.
  - Id hygiene (same policy as the morning's merges): kor -> chor in
    IshaRaa/Ezra/Kidush (chor is the in-play majority everywhere), Kidush
    fused `tobyas_yulye` -> `tobyas yulye` (S4); cast_dict declarations for
    every id this touches.

  python3.11 -m annotation.fix_corpus_flags_2026_07_26 --dry-run
  python3.11 -m annotation.fix_corpus_flags_2026_07_26 --push
"""
from __future__ import annotations
import argparse, csv, json, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from annotation.lint_pages import REPO
from transkribus.client import TrpClient

NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
_TRAIL = ":׃־ .,"
NOTE = "corpus lint fixes 2026-07-26"
CSV = REPO / "data" / "review" / "lint_flags_2026-07-26.csv"
KIND = "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete"

FOLDERS = {
    "Bas Sheva": "BasSheva", "Der Mann untern Tisch": "DerManUnterTiff",
    "Ezra": "Ezra-Emkroyt1908", "Isha Raa": "IshahRaah",
    "Kidush Hashem": "KidushHashem", "Mishke Mashke": "MishkeMashke-Kultur1910",
    "Yudale der Blinder": "Yudale_der_blinder,_Emkroyt1908",
    "Das Yudishe Kind": KIND, "Dos Yudishe Herts": "DosYudisheHerts-1910",
    "Dovid's Fidele": "DovidsFidele-1904",
}
FW_PLAYS = {"Ezra-Emkroyt1908", "MishkeMashke-Kultur1910",
            "Yudale_der_blinder,_Emkroyt1908"}
TYPO = {"busines": "business", "seting": "setting"}

# (play, page): head{unit-type} -> heading{type:act; n}
EZRA_ACTS = {("Ezra-Emkroyt1908", 15): "2", ("Ezra-Emkroyt1908", 21): "3",
             ("Ezra-Emkroyt1908", 29): "4"}

MERGE_IDS = {
    "IshahRaah": {"kor": "chor"},
    "Ezra-Emkroyt1908": {"kor": "chor"},
    "KidushHashem": {"kor": "chor", "tobyas_yulye": "tobyas yulye"},
}

# (play, page, line_id) -> xmlid on the existing no-xmlid speaker span
SET_WHO = {
    (KIND, 15, "r_3_1_tl_16"): "shprintse",        # שפּריצע
}
# (play, page, line_id) -> xmlid: no speaker span yet, add one over the label
ADD_SPEAKER = {
    ("IshahRaah", 65, "tr_2_l27"): "perets",       # פון: = OCR פרץ
    ("IshahRaah", 6, "tr_1_tl_9"): "meydkhen",     # Noa: מעדכען, not alle
    ("Yudale_der_blinder,_Emkroyt1908", 65, "line_1647441957184_1889"): "dvorele",
    ("BasSheva", 15, "r_2_1l17"): "bnr bnimin",    # S4 (07-20 edit used a stale id)
}
# (play, page, line_id) -> stage span to ADD over the head of the line.
# (Herts (שרייט) and Kind זעזעמיר) turned out already stage-covered live —
# their lint flags were the stage-covered-label gap, patched in lint_pages.)
ADD_STAGE = {}
# (play, page, line_id) -> (set type, optional xmlid) on existing stage span
STAGE_TYPE = {
    ("KidushHashem", 24, "r1l19"): ("delivery", "chor"),  # קאהר דאס זעלבע (M4b)
    ("DerManUnterTiff", 14, "r_1_1l26"): ("business", None),  # (קלערט אביסיל)
    (KIND, 12, "r_3_1_tl_19"): ("delivery", None),        # זינגט: busines -> delivery
}
# (play, page, line_id) -> ordered xmlids for the role spans on that line
ROLE_WHO = {
    ("BasSheva", 6, "r_5_1l11"): ["etts"],
    ("MishkeMashke-Kultur1910", 4, "line_1638100149342_810"):
        ["diener", "matrozen", "chinezer", "pasazhiren"],
}
DOVID = "DovidsFidele-1904"


def load_csv_targets():
    """(retag_head, typo_pages) from the lint CSV."""
    retag, typo_pages = {}, set()
    for r in csv.DictReader(CSV.open(encoding="utf-8")):
        play = FOLDERS.get(r["edition"])
        if not play or not r["page(s)"].isdigit():
            continue
        pg, lid = int(r["page(s)"]), r["line_id / count"]
        d = r["issue/detail"]
        if "heading.type must be" in d and play != "Ezra-Emkroyt1908":
            retag[(play, pg, lid)] = True
        if "tokens not in vocab: ['busines']" in d or \
           "tokens not in vocab: ['seting']" in d:
            typo_pages.add((play, pg))
    return retag, typo_pages


def doc_order_lg_id(root, line_id, horizon=6):
    seen, left = False, horizon
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
        left -= 1
        if left <= 0:
            return None
    return None


def edit_page(root, play, page, retag_head):
    log = []
    for tl in root.iter(f"{NS}TextLine"):
        lid = tl.get("id")
        key = (play, page, lid)
        u = tl.find(f".//{NS}Unicode")
        txt = (u.text or "") if u is not None else ""
        entries = parse_custom(tl.get("custom") or "")
        dirty = False
        out = []
        for tag, a in entries:
            # fw type backfill
            if play in FW_PLAYS and tag == "fw" and "type" not in a:
                a["type"] = "pageNum"
                log.append(f"{lid}: fw +type:pageNum"); dirty = True
            # stage.type typo tokens
            if tag == "stage" and a.get("type"):
                toks = [TYPO.get(t, t) for t in a["type"].split()]
                if toks != a["type"].split():
                    log.append(f"{lid}: stage type {a['type']!r} → {' '.join(toks)!r}")
                    a["type"] = " ".join(toks); dirty = True
            # Ezra act heads
            if (play, page) in EZRA_ACTS and tag == "head" and "unit-type" in a:
                n = EZRA_ACTS[(play, page)]
                a = {"offset": a["offset"], "length": a["length"],
                     "type": "act", "n": n}
                log.append(f"{lid}: head{{unit-type}} → heading type:act n:{n}  [{txt[:14]!r}]")
                out.append(("heading", a)); dirty = True
                continue
            # Dovid act 6 -> 4
            if play == DOVID and tag == "heading" and a.get("n") == "6":
                a["n"] = "4"
                log.append(f"{lid}: heading n:6 → n:4  [{txt[:12]!r}]"); dirty = True
            # numeral headings -> head
            if key in retag_head and tag == "heading" and not a.get("type"):
                a = {k: v for k, v in a.items() if k in ("offset", "length")}
                lg = doc_order_lg_id(root, lid)
                if lg:
                    a["lg_id"] = lg
                log.append(f"{lid}: heading → head lg_id:{lg}  [{txt[:12]!r}]")
                out.append(("head", a)); dirty = True
                continue
            # רעפריין speaker rubrics -> head (Kind p23)
            if key in {(KIND, 23, "l_8"), (KIND, 23, "l_24")} and tag == "speaker":
                end = int(a["offset"]) + int(a["length"])
                while end > 0 and end <= len(txt) and txt[end - 1] in _TRAIL:
                    end -= 1
                h = {"offset": a["offset"], "length": str(end - int(a["offset"]))}
                lg = doc_order_lg_id(root, lid)
                if lg:
                    h["lg_id"] = lg
                log.append(f"{lid}: speaker → head  [{txt[:10]!r}]")
                out.append(("head", h)); dirty = True
                continue
            # stage type overrides (+ optional xmlid)
            if key in STAGE_TYPE and tag == "stage":
                t, xid = STAGE_TYPE[key]
                if a.get("type") != t or (xid and a.get("xmlid") != xid):
                    a["type"] = t
                    if xid:
                        a["xmlid"] = xid
                    log.append(f"{lid}: stage → type:{t}"
                               f"{' xmlid:' + xid if xid else ''}  [{txt[:16]!r}]")
                    dirty = True
            # speaker xmlid sets
            if key in SET_WHO and tag == "speaker" and not a.get("xmlid"):
                off, end = int(a.get("offset", 0)), int(a.get("offset", 0)) + int(a.get("length", 0))
                while end > off and end <= len(txt) and txt[end - 1] in _TRAIL:
                    end -= 1
                a["length"], a["xmlid"] = str(end - off), SET_WHO[key]
                log.append(f"{lid}: +xmlid:{SET_WHO[key]}  [{txt[off:end]!r}]")
                dirty = True
            # id merges
            if "xmlid" in a and play in MERGE_IDS:
                toks = a["xmlid"].split()
                new = " ".join(MERGE_IDS[play].get(t, t) for t in toks)
                if new != a["xmlid"]:
                    log.append(f"{lid}: [{tag}] {a['xmlid']} → {new}")
                    a["xmlid"] = new; dirty = True
            out.append((tag, a))
        entries = out

        # role xmlids in printed order
        if key in ROLE_WHO:
            want = list(ROLE_WHO[key])
            for tag, a in entries:
                if tag == "role" and not a.get("xmlid") and want:
                    a["xmlid"] = want.pop(0)
                    log.append(f"{lid}: role +xmlid:{a['xmlid']}"); dirty = True
        # Kind p2: split the double role span into yudn + grafn
        if key == (KIND, 2, "line_1694599194469_485"):
            roles = [(t, a) for t, a in entries if t == "role"]
            if len(roles) == 1 and not roles[0][1].get("xmlid"):
                cut = txt.find(",")
                sp2 = txt.find("גרא")
                end2 = txt.find(" א.", sp2)
                new = [(t, a) for t, a in entries if t != "role"]
                new.append(("role", {"offset": "0", "length": str(cut),
                                     "xmlid": "yudn"}))
                new.append(("role", {"offset": str(sp2),
                                     "length": str(end2 - sp2), "xmlid": "grafn"}))
                entries = new
                log.append(f"{lid}: role split → yudn[{txt[:cut]!r}] "
                           f"+ grafn[{txt[sp2:end2]!r}]")
                dirty = True
        # add speaker spans
        if key in ADD_SPEAKER and not any(t == "speaker" for t, _ in entries):
            cut = txt.find(":")
            end = cut if cut != -1 else len(txt)
            while end > 0 and txt[end - 1] in _TRAIL:
                end -= 1
            entries.append(("speaker", {"offset": "0", "length": str(end),
                                        "xmlid": ADD_SPEAKER[key]}))
            log.append(f"{lid}: +speaker xmlid:{ADD_SPEAKER[key]}  [{txt[:end]!r}]")
            dirty = True
        # add stage spans over the leading parenthetical label
        if key in ADD_STAGE and not any(t == "stage" for t, _ in entries):
            cut = txt.find(":")
            end = cut if cut != -1 else len(txt)
            while end > 0 and txt[end - 1] in " ．.,":
                end -= 1
            entries.append(("stage", {"offset": "0", "length": str(end),
                                      "type": ADD_STAGE[key]}))
            log.append(f"{lid}: +stage type:{ADD_STAGE[key]}  [{txt[:end]!r}]")
            dirty = True

        if dirty:
            tl.set("custom", serialize_custom(entries))
    return log


def update_cast_dicts(dry):
    def load(play):
        return json.loads((REPO / "data" / play / "cast_dict.json").read_text(encoding="utf-8"))
    def save(play, d):
        if not dry:
            (REPO / "data" / play / "cast_dict.json").write_text(
                json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    def coll(form):
        return {"form": form, "bare": form, "collective": True, "source": NOTE}

    d = load("BasSheva")
    d["roles"].setdefault("beyde", coll("ביידע")); save("BasSheva", d)
    d = load("IshahRaah")
    d["roles"].setdefault("meydkhen", coll("מעדכען"))
    d["roles"].setdefault("chor", coll("כאר")); save("IshahRaah", d)
    d = load("Ezra-Emkroyt1908")
    d["roles"].setdefault("chor", coll("כאר")); save("Ezra-Emkroyt1908", d)
    d = load("KidushHashem")
    for xid, form in (("alle", "אלע"), ("chor", "כאר"), ("eyner", "איינער"),
                      ("menner", "מענער"), ("meydkhen", "מעדכען"),
                      ("pazh", "פאזש"), ("vekhter", "וועכטער")):
        d["roles"].setdefault(xid, coll(form))
    d["roles"].setdefault("duet", {"form": "דועט", "bare": "דועט", "loc": None,
                                   "printed": False,
                                   "note": f"abstract voice rubric ({NOTE})"})
    save("KidushHashem", d)
    d = load("MishkeMashke-Kultur1910")
    for xid, form in (("diener", "דיענער"), ("matrozen", "מאטראזען"),
                      ("pasazhiren", "פאסאזשירען")):
        d["roles"].setdefault(xid, {**coll(form),
                                    "source": f"printed castList p4 ({NOTE})"})
    save("MishkeMashke-Kultur1910", d)
    d = load("Yudale_der_blinder,_Emkroyt1908")
    v = d["roles"]["dvorele"].setdefault("prefix_variants", [])
    if "דבורה" not in v:
        v.append("דבורה")
    save("Yudale_der_blinder,_Emkroyt1908", d)
    d = load(KIND)
    v = d["roles"]["shprintse"].setdefault("prefix_variants", [])
    if "שפריצע" not in v:
        v.append("שפריצע")
    save(KIND, d)
    d = load(DOVID)
    ns = d.setdefault("non_speaker_labels", [])
    for lab in ("בר ככבא", "בר ככנא", "טהיילע)", "בריינדיל קאזאק"):
        if lab not in ns:
            ns.append(lab)   # Noa 06-28: p71 promotional material, non-theatrical
    save(DOVID, d)
    print("  cast_dicts updated (idempotent)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()
    dry = not args.push

    retag_head, typo_pages = load_csv_targets()
    print(f"— {len(retag_head)} numeral headings, {len(typo_pages)} typo pages from CSV —")
    update_cast_dicts(dry)

    pages: dict[tuple[str, int], None] = {}
    for play in FW_PLAYS | set(MERGE_IDS):
        pa = REPO / "data" / play / "page_annotated"
        for f in sorted(pa.glob("0*.xml")):
            pages[(play, int(f.name[:4]))] = None
    for src in (retag_head, SET_WHO, ADD_SPEAKER, ADD_STAGE, STAGE_TYPE, ROLE_WHO):
        for (play, pg, _l) in src:
            pages[(play, pg)] = None
    for (play, pg) in typo_pages:
        pages[(play, pg)] = None
    for (play, pg) in list(EZRA_ACTS) + [(DOVID, 62), (KIND, 2), (KIND, 23)]:
        pages[(play, pg)] = None

    ids = load_doc_ids()
    client = TrpClient.from_env(); client.login()
    pushed = 0
    problems = []
    for play, page in sorted(pages):
        tsid, owner, xml = top_transcript(client, ids[play], page)
        if xml is None:
            problems.append(f"{play} p{page}: no transcript"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        log = edit_page(root, play, page, retag_head)
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
    if problems:
        print("PROBLEMS:")
        for p in problems:
            print(f"  {p}")


if __name__ == "__main__":
    raise SystemExit(main())
