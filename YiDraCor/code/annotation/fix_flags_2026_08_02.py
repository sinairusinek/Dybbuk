"""Clear the 2026-08-02 lint wave (Judith's heading sweep + stage-type typos).

Driven by data/review/lint_flags_2026-08-02.csv. Rules, all established:
  - stage.type typos -> vocab: seting/busines/busunes and the Hebrew-layout
    artifact נודןמקד (= 'busines' typed with a Hebrew keyboard) -> business,
    seting -> setting.
  - untyped `heading` spans:
      * worded act headings (contain אקט/אַקט) -> heading{type:act; n} with n
        parsed from the Roman numeral or ordinal word;
      * everything else (numerals, No./Nr., Ritt. cues, song titles, דועט
        rubrics) -> `head` (+lg_id when the following song is numbered);
      * a line already carrying a speaker span keeps it (§G duet rubrics) —
        the redundant untyped heading is dropped instead.
  - bare fw -> type:pageNum.

  python3.11 -m annotation.fix_flags_2026_08_02 --dry-run
  python3.11 -m annotation.fix_flags_2026_08_02 --push
"""
from __future__ import annotations
import argparse, csv, re, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from annotation.lint_pages import REPO
from transkribus.client import TrpClient

NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
NOTE = "lint wave fixes 2026-08-02 (heading types + stage typos)"
CSV = REPO / "data" / "review" / "lint_flags_2026-08-02.csv"
VOWELS = re.compile(r"[֑-ׇ]")
def unvoc(s): return VOWELS.sub("", s)

FOLDERS = {
    "Al Naharot Bavel": "AlNaharotBavel-Amkreut&Freund1909",
    "Bas Sheva": "BasSheva", "Blimele (di Perle von Warsha)": "Blimele-AhronFaust1903",
    "Der Mann untern Tisch": "DerManUnterTiff", "Di Seder Nakht": "Di_seyder_nakht_Emkroyt_1908",
    "Dos Yudishe Herts": "DosYudisheHerts-1910", "Dovid's Fidele": "DovidsFidele-1904",
    "Ezra": "Ezra-Emkroyt1908", "Hinke Pinke": "HinkePinke", "Isha Raa": "IshahRaah",
    "Kidush Hashem": "KidushHashem", "Mishke Mashke": "MishkeMashke-Kultur1910",
    "Sore Sheyndel": "SoreSheyndel",
    "Yudale der Blinder": "Yudale_der_blinder,_Emkroyt1908",
    "Das Yudishe Kind": "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete",
}
TYPO = {"seting": "setting", "busines": "business", "busunes": "business",
        "נודןמקד": "business"}
ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6}
ORDINAL = {"ערשטער": 1, "צווייטער": 2, "דריטער": 3, "פירטער": 4,
           "פינפטער": 5, "זעקסטער": 6}


def act_n(bare):
    if "אקט" not in bare:
        return None
    m = re.search(r"\b(I{1,3}|IV|V|VI)\b", bare)
    if m:
        return ROMAN[m.group(1)]
    for w, n in ORDINAL.items():
        if w in bare:
            return n
    return None


def load_targets():
    """(play, page) -> set of flagged line_ids per kind."""
    heads, typos, fws = {}, set(), set()
    for r in csv.DictReader(CSV.open(encoding="utf-8")):
        play = FOLDERS.get(r["edition"])
        if not play or not r["page(s)"].isdigit():
            continue
        key = (play, int(r["page(s)"]))
        d = r["issue/detail"]
        if "heading.type must be" in d:
            heads.setdefault(key, set()).add(r["line_id / count"])
        elif "tokens not in vocab" in d:
            typos.add(key)
        elif "fw span requires" in d:
            fws.add(key)
    return heads, typos, fws


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


def edit_page(root, play, page, head_lids):
    log = []
    for tl in root.iter(f"{NS}TextLine"):
        lid = tl.get("id")
        u = tl.find(f".//{NS}Unicode")
        txt = (u.text or "") if u is not None else ""
        bare = unvoc(txt)
        entries = parse_custom(tl.get("custom") or "")
        dirty = False
        out = []
        has_speaker = any(t == "speaker" for t, _ in entries)
        for tag, a in entries:
            if tag == "stage" and a.get("type"):
                toks = [TYPO.get(t, t) for t in a["type"].split()]
                if toks != a["type"].split():
                    log.append(f"{lid}: stage {a['type']!r} → {' '.join(toks)!r}")
                    a["type"] = " ".join(toks); dirty = True
            if tag == "fw" and "type" not in a:
                a["type"] = "pageNum"
                log.append(f"{lid}: fw +type:pageNum"); dirty = True
            if tag == "heading" and not a.get("type") and lid in head_lids:
                n = act_n(bare)
                if n is not None:
                    a["type"], a["n"] = "act", str(n)
                    log.append(f"{lid}: heading typed act n:{n}  [{bare[:16]!r}]")
                    out.append((tag, a)); dirty = True
                    continue
                if int(a.get("offset", 0)) > 0:
                    # sub-line music cue ((N. Ritt…) inside a dialogue line):
                    # a head at its own offsets, not a heading and not dropped
                    a = {k: v for k, v in a.items() if k in ("offset", "length")}
                    log.append(f"{lid}: inline heading → head  "
                               f"[{bare[int(a['offset']):int(a['offset'])+int(a['length'])][:18]!r}]")
                    out.append(("head", a)); dirty = True
                    continue
                if has_speaker:
                    log.append(f"{lid}: dropped untyped whole-line heading "
                               f"(speaker span stands per §G)  [{bare[:18]!r}]")
                    dirty = True
                    continue          # drop it
                a = {k: v for k, v in a.items() if k in ("offset", "length")}
                lg = doc_order_lg_id(root, lid)
                if lg:
                    a["lg_id"] = lg
                log.append(f"{lid}: heading → head lg_id:{lg}  [{bare[:16]!r}]")
                out.append(("head", a)); dirty = True
                continue
            out.append((tag, a))
        if dirty:
            tl.set("custom", serialize_custom(out))
    return log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()

    heads, typos, fws = load_targets()
    pages = sorted(set(heads) | typos | fws)
    print(f"{sum(len(v) for v in heads.values())} headings, "
          f"{len(typos)} typo pages, {len(fws)} fw pages → {len(pages)} pages")

    ids = load_doc_ids()
    client = TrpClient.from_env(); client.login()
    pushed = 0
    for play, page in pages:
        tsid, owner, xml = top_transcript(client, ids[play], page)
        if xml is None:
            print(f"{play} p{page}: no transcript — SKIP"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        log = edit_page(root, play, page, heads.get((play, page), set()))
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
