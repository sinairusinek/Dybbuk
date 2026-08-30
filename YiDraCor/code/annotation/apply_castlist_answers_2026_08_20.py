"""Apply Noa's answers to the castList half of the speaker-label handoff.

Source: docs/handoff_2026-08-20_speaker_labels_ADDENDUM.md — the returned copy
of the 2026-08-16 addendum, which carried the eight `role`-span questions that
were dropped from the main questionnaire. The main handoff (128 speech-prefix
labels) is still out with Noa and Judith; nothing here touches it.

The addendum's diagnosis held: none of the eight was a failure to identify the
character. Six are one span covering several names, one is a clipped span, one
is a role the castList never listed.

  xmlid    — the span is right, the id was simply never written
  rescope  — the span covers the name PLUS something else (a period, `אונד`)
  add      — the line names a role the RAs never tagged (Meshumed's מעכעל, Di
             Tsvey Tnoim's חגלה and הילנא), so the span itself is minted
  cast     — a role the castList does not have: `flismn`, `kinder`, `chor`

HELD BACK — Di Tsvey Tnoim p.18 `כאָהר. רויבער.` (the first two spans of r2l5).
Noa ticked *one span, several roles* but the comment on that entry is a
copy-paste of the previous answer ("Split into two roles: bn_fdus and izrelis"),
which cannot be right for this line. The open question is whether `כאהר` +
`רויבער` are one role — the castList's `khr_fun_royber` ("קאהר פֿון רויבער") —
or a collective `chor` followed by a separate band-of-robbers entry. The other
four names on that line are unambiguous and are applied.

  python3.11 -m annotation.apply_castlist_answers_2026_08_20 --dry-run
  python3.11 -m annotation.apply_castlist_answers_2026_08_20 --local --push
  python3.11 -m annotation.apply_castlist_answers_2026_08_20 --push
"""
from __future__ import annotations
import argparse, glob, json, sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, serialize_custom
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, COL
from transkribus.client import TrpClient

REPO = Path(__file__).resolve().parents[2]
NS = "{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}"
NOTE = "Noa 2026-08-20 (castList addendum)"
MESH, TNOIM = "Lateiner_Meshumed", "MS_DiTsveyTnoim"

# (play, page, line_id, offset) -> (new_offset, new_length, xmlid, why)
# `offset` is the span's CURRENT offset and identifies it on the line; for an
# `add` the span does not exist yet and the key offset is where it will start.
EDITS = {
    # ── Meshumed ────────────────────────────────────────────────────────────
    # "The span only caught 'א'. The full role is 'א ריכטיר פֿון געהיימס
    # געריכט' (rikhtir_fun_geheyms_gerikht)". The re-cut already landed in the
    # mirror; what is still missing is the id.
    (MESH, 4, "line_1604766338196_300", 0):
        (0, 28, "rikhtir_fun_geheyms_gerikht", "clipped span — full role"),
    # "Split into shrhke and kinder (collective)" — both spans already exist.
    (MESH, 21, "line_1606075239239_2248", 0):
        (0, 3, "shrhke", "שרה = שרהקע"),
    (MESH, 21, "line_1606075239239_2248", 4):
        (4, 6, "kinder", "קינדער — new collective"),
    # "Split into three roles: iekb_eyzenshteyn, mekhil, and shrhke" — only
    # יעקב and שרה were tagged; מעכעל between them was never spanned.
    (MESH, 25, "line_1607702913666_1210", 0):
        (0, 4, "iekb_eyzenshteyn", "יעקב"),
    (MESH, 25, "line_1607702913666_1210", 5):
        (5, 5, "mekhil", "מעכעל — span minted, Noa's third role"),
    (MESH, 25, "line_1607702913666_1210", 11):
        (11, 3, "shrhke", "שרה"),
    # "A speaking character (Policeman/מוף אן) in the dialogue, missing from
    # the initial cast list." Trailing period dropped, per S1.
    (MESH, 38, "r2l1", 0):
        (0, 9, "flismn", "פֿאליסמאן — new body-only role"),

    # ── Di Tsvey Tnoim ──────────────────────────────────────────────────────
    # "Split into two roles: bn_fdus and izrelis". The second span opened on
    # `אונד` ("and"), which is not part of the name.
    (TNOIM, 14, "r2l4", 0):
        (0, 8, "bn_fdus", "בן פּדות"),
    (TNOIM, 14, "r2l4", 10):
        (15, 9, "izrelis", "rescoped off `אונד` onto איזראעלית"),
    # "Split into two roles: rbi_shmeun_bn_lkish and tsenbi".
    (TNOIM, 17, "r2l18", 5):
        (5, 5, "rbi_shmeun_bn_lkish", "שמעון — trailing space trimmed"),
    (TNOIM, 17, "r2l18", 13):
        (13, 9, "tsenbi", "צענאַביאַ"),
    # "Collective / group" — `chor` is already the xmlid of every כאהר speaker
    # span in this play; it was simply never a castList entry.
    (TNOIM, 18, "r2l2", 0):
        (0, 4, "chor", "Chor — new collective"),
    # r2l5: the four unambiguous names. כאָהר/רויבער held back, see docstring.
    (TNOIM, 18, "r2l5", 15):
        (15, 4, "khgle", "חגלה — span minted"),
    (TNOIM, 18, "r2l5", 21):
        (21, 5, "fldsh", "פּלדש"),
    (TNOIM, 18, "r2l5", 28):
        (28, 4, "nkhmn", "נחמן"),
    (TNOIM, 18, "r2l5", 34):
        (34, 5, "hiln", "הילנא — span minted"),
}

# Roles the castLists do not have. Applied to the local cast_dict.json only.
CAST_ADD = {
    MESH: {
        "flismn": {"form": "פֿאליסמאן", "bare": "פאליסמאן", "source": "body",
                   "prefix_variants": ["פאליסמ", "פֿאליסמ", "פֿאליס"],
                   "notes": [f"{NOTE}: p.38 scene heading; speaks but is not "
                             f"in the castList"]},
        "kinder": {"form": "קינדער", "bare": "קינדער", "collective": True,
                   "source": "body",
                   "notes": [f"{NOTE}: p.21 — 'שרה קינדער' splits into "
                             f"shrhke + this collective"]},
    },
    TNOIM: {
        "chor": {"form": "כאָהר", "bare": "כאהר", "collective": True,
                 "source": "body", "prefix_variants": ["Chor", "קאהר"],
                 "notes": [f"{NOTE}: p.18 castList span; already the xmlid of "
                           f"the כאהר speaker spans"]},
    },
}


def line_text(el) -> str:
    u = el.find(f".//{NS}Unicode")
    return (u.text or "") if u is not None else ""


def apply_page(root, page_edits):
    """page_edits: {(line_id, offset): (off, ln, xmlid, why)}. -> [report]"""
    done = []
    for el in root.iter(f"{NS}TextLine"):
        lid = el.get("id")
        mine = {k: v for k, v in page_edits.items() if k[0] == lid}
        if not mine:
            continue
        txt = line_text(el)
        entries = parse_custom(el.get("custom") or "")
        # A rescope moves the span off its key offset, so an already-applied
        # edit must be recognised by where it LANDED too — otherwise a second
        # run mints a duplicate span beside it.
        landed = {(o, l, x): k for k, (o, l, x, _) in mine.items()}
        out, seen = [], set()
        for tag, a in entries:
            key = None
            if tag == "role":
                key = (lid, int(a.get("offset", -1)))
                if key not in mine:
                    key = landed.get((int(a.get("offset", -1)),
                                      int(a.get("length", -1)),
                                      a.get("xmlid")))
            if key not in mine:
                out.append((tag, a)); continue
            off, ln, xmlid, why = mine[key]
            seen.add(key)
            a = dict(a)
            was = (a.get("offset"), a.get("length"), a.get("xmlid"))
            a["offset"], a["length"], a["xmlid"] = str(off), str(ln), xmlid
            out.append(("role", a))
            if (a["offset"], a["length"], a["xmlid"]) != tuple(map(str, was)):
                done.append(f"  role({was[0]},{was[1]}) → ({off},{ln}) "
                            f"xmlid:{xmlid:28} {txt[off:off+ln]!r}  — {why}")
        added = []
        for key in sorted(mine.keys() - seen, key=lambda k: k[1]):
            off, ln, xmlid, why = mine[key]
            if txt[off:off + ln].strip() == "":
                print(f"  !! {lid} @{off}: text does not cover a name — skip")
                continue
            added.append(("role", {"offset": str(off), "length": str(ln),
                                   "xmlid": xmlid}))
            done.append(f"  role ADD      ({off},{ln}) xmlid:{xmlid:28} "
                        f"{txt[off:off+ln]!r}  — {why}")
        if added:
            # Slot the minted spans in among the role spans and leave every
            # other entry — readingOrder first of all — exactly where it was.
            roles = sorted([e for e in out if e[0] == "role"] + added,
                           key=lambda e: int(e[1].get("offset", 0)))
            merged, it = [], iter(roles)
            for e in out:
                merged.append(next(it) if e[0] == "role" else e)
            merged.extend(it)
            out = merged
        if done:
            el.set("custom", serialize_custom(out))
    return done


def update_cast_dicts(dry):
    for play, adds in CAST_ADD.items():
        p = REPO / "data" / play / "cast_dict.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        new = [x for x in adds if x not in d["roles"]]
        for xid in new:
            d["roles"][xid] = adds[xid]
        if new and not dry:
            p.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")
        print(f"  cast_dict {play}: +{', '.join(new) or '(nothing new)'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--local", action="store_true",
                    help="read/write data/*/page_annotated instead of the "
                         "live top transcript")
    args = ap.parse_args()
    dry = not args.push

    by_page = {}
    for (play, page, lid, off), v in EDITS.items():
        by_page.setdefault((play, page), {})[(lid, off)] = v

    client = ids = None
    if not args.local:
        ids = load_doc_ids()
        client = TrpClient.from_env(); client.login()

    n = 0
    for (play, page), page_edits in sorted(by_page.items()):
        if args.local:
            files = glob.glob(str(REPO / "data" / play / "page_annotated"
                                  / f"{page:04d}_*.xml"))
            if not files:
                print(f"{play} p{page}: no local page — skip"); continue
            root = etree.parse(files[0]).getroot()
            where = Path(files[0]).name
        else:
            doc = ids.get(play)
            if doc is None:
                print(f"{play}: no doc id — skip"); continue
            tsid, owner, xml = top_transcript(client, doc, page)
            if xml is None:
                print(f"{play} p{page}: no transcript — skip"); continue
            root = etree.fromstring(xml.encode("utf-8")
                                    if isinstance(xml, str) else xml)
            where = f"top: {owner.split('@')[0]}"
        rep = apply_page(root, page_edits)
        if not rep:
            continue
        n += len(rep)
        print(f"\n{play} p{page} ({where})")
        print("\n".join(rep))
        if args.push and args.local:
            # Keep the file's own XML declaration — lxml would rewrite the
            # quoting and drop `standalone`, which is diff noise, not a change.
            f = Path(files[0])
            orig = f.read_bytes()
            head = orig.split(b"\n", 1)[0]
            body = etree.tostring(root.getroottree(), encoding="UTF-8")
            if body.startswith(b"<?xml"):
                body = body.split(b"\n", 1)[1]
            if orig.endswith(b"\n") and not body.endswith(b"\n"):
                body += b"\n"
            f.write_bytes(head + b"\n" + body)
            print("  → written to the local mirror")
        elif args.push:
            client.push_transcript(
                COL, doc, page, etree.tostring(root, encoding="unicode"),
                parent_tsid=tsid, status="IN_PROGRESS",
                note="castList answers 2026-08-20",
                tool_name="YiDraCor-annotation-pipeline")
            print(f"  → pushed (parent {tsid})")

    print("\n— cast_dict updates —")
    update_cast_dicts(dry)
    print(f"\n{'APPLIED' if args.push else 'DRY RUN — nothing written'}: "
          f"{n} role spans")


if __name__ == "__main__":
    raise SystemExit(main())
