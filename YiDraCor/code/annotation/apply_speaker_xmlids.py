"""Resolve speaker spans that carry no xmlid, where the answer is determinate.

Sinai 2026-07-20. 59 speaker spans corpus-wide had no xmlid. These are the ones
where the play's own cast settles it; the rest are in Noa's handoff.

  joint    — S4: one span, space-separated xmlids
  xmlid    — the label is a known role under a shorter/longer surface form
  rescope  — the span itself is mis-cut; fix offsets, then set xmlid

Every entry also re-applies S1 (the span covers the NAME only, never the
trailing colon), because several of these carried the colon.

  python3.11 -m annotation.apply_speaker_xmlids --dry-run
  python3.11 -m annotation.apply_speaker_xmlids --push
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
_TRAIL = ":׃־ .,"

# (play, page, line_id) -> (xmlid, note, optional forced offset)
EDITS = {
    # S4 joint turns — one span, space-separated ids (Sinai 2026-07-20).
    ("IshahRaah", 23, "tr_1_tl_43"):
        ("shlomit talmai chor", "joint: Shlomit + Talmai + chorus", None),
    ("BasSheva", 16, "TextRegion_1648725342005_1510l3"):
        ("bnr bnimin", "joint: Avner + Benyamin (Noa: tag each separately → "
                       "one span, two ids per S4)", None),
    # The label is the surname of a titled role already in cast_dict.
    ("דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete", 28, "l_4"):
        ("graf_zezemir", "זעזעמיר = graf_zezemir without the title; the line "
                         "before addresses him as פֿרייַנד זֶעזֶעמִיר", None),
    # Mis-cut span: the line reads קעניג: but the span started at offset 1,
    # clipping the ק. Ten other קעניג spans on this same page are (0,6).
    ("IshahRaah", 70, "tr_1_l14"):
        ("kenig_hurknos", "span clipped the leading ק — rescoped to (0,·)", 0),
}


def apply_edit(root, line_id, xmlid, forced_offset):
    for el in root.iter(f"{NS}TextLine"):
        if el.get("id") != line_id:
            continue
        u = el.find(f".//{NS}Unicode")
        txt = (u.text or "") if u is not None else ""
        entries, out, done = parse_custom(el.get("custom") or ""), [], None
        for tag, a in entries:
            if tag != "speaker":
                out.append((tag, a)); continue
            a = dict(a)
            off = forced_offset if forced_offset is not None else int(a.get("offset", 0))
            end = off + int(a.get("length", 0))
            if forced_offset is not None:
                end = max(end, off + 1)
                # re-derive the end from the text: extend to the label's colon
                cut = txt.find(":", off)
                if cut != -1:
                    end = cut
            while end > off and txt[end - 1] in _TRAIL:   # S1: name only
                end -= 1
            was = (a.get("offset"), a.get("length"), a.get("xmlid"))
            a["offset"], a["length"], a["xmlid"] = str(off), str(end - off), xmlid
            if (a["offset"], a["length"], a["xmlid"]) == tuple(map(str, was)):
                return None                                # already applied
            out.append(("speaker", a))
            done = (f"speaker({was[0]},{was[1]}) → ({a['offset']},{a['length']}) "
                    f"xmlid:{xmlid}  [{txt[off:end]!r}]")
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
    n = 0
    for (play, page, lid), (xmlid, note, off) in sorted(EDITS.items()):
        doc = ids.get(play)
        if doc is None:
            print(f"{play}: no doc id — skip"); continue
        tsid, owner, xml = top_transcript(client, doc, page)
        if xml is None:
            print(f"{play} p{page}: no transcript — skip"); continue
        root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
        r = apply_edit(root, lid, xmlid, off)
        if not r:
            continue
        n += 1
        print(f"\n{play[:34]} p{page} (top: {owner.split('@')[0]})")
        print(f"  {r}")
        print(f"  — {note}")
        if args.push:
            client.push_transcript(
                COL, doc, page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note="resolve speaker xmlids 2026-07-20",
                tool_name="YiDraCor-annotation-pipeline")
            print(f"  → pushed (parent {tsid})")
    print(f"\n{'PUSHED' if args.push else 'DRY RUN — nothing written'}: {n} spans")


if __name__ == "__main__":
    raise SystemExit(main())
