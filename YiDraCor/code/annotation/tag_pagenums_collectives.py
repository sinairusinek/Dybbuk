"""Combined live tagger: fw{type:pageNum} + collective speaker spans, one
layered push per page. Corpus-wide (all plays with page_annotated/).

Page-number selection per play:
  candidates = whole lines of only digits (1-3) + dash-ish punctuation/space
  framed     = contains a real dash/=/~/_/* (shelfmark guard: no quotes; 4+
               digit runs never match)
  model      = printed value ~= a*scanPage + c, a in (1,2) (2 = spread scans),
               c = mode over framed candidates; best (a,c) by consistency
  tag: framed always (wrong digits -> audit list, text fix is Judith's);
       bare only if sequence-consistent (SoreSheyndel / Dos Yudishe Kind).
Collective turns: same rule as annotation.apply_collective_speakers (skip
titlePage/castList + Di Seder supplement pages > 54); re-verified on the live
line before tagging.

Run (repo root, python3.11):
  python -m annotation.tag_pagenums_collectives [--dry-run] [--only FOLDER]
Writes the audit CSV to data/review/pagenum_audit_<DATE>.csv.
"""
import argparse, csv, datetime as _dt, re, sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
AUDIT_DIR = REPO / "data" / "review"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lxml import etree
from annotation.schema import parse_custom, serialize_custom, is_collective_label
from annotation.lint_pages import (NS, TURN_RE, COLLECTIVE_XMLID, skel,
                                   line_text, page_type)
from transkribus.client import TrpClient

COL = 2372172
SUPPLEMENT_FROM = {"Di_seyder_nakht_Emkroyt_1908": 54}
DASHY = "—–־‒―=~_.·*-"
SHAPE_RE = re.compile(rf"^[\s{DASHY}]*(\d{{1,3}})[\s{DASHY}]*$")
FRAME_RE = re.compile(r"[—–־‒―=~_*-]")
HEADING_RE = re.compile(r"אַ?קט|פערוואנ|פֶערוואנ|עפילאג|עֶפִּילָאג|געזאנג")


def load_doc_ids():
    out = {}
    with open(REPO / "data" / "editions.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if (r.get("folder") or "").strip() and (r.get("transkribus_doc_id") or "").strip():
                out[r["folder"].strip()] = int(r["transkribus_doc_id"])
    return out


def play_pages(play_dir):
    best = {}
    for p in play_dir.glob("*.xml"):
        try:
            nr = int(p.name.split("_")[0])
        except ValueError:
            continue
        if nr not in best or p.stat().st_mtime > best[nr].stat().st_mtime:
            best[nr] = p
    return sorted(best.items())


def trimmed_span(txt):
    m = re.search(r"\S.*\S|\S", txt)
    return (m.start(), m.end()) if m else None


def scan_play(play):
    """Return (fw_targets, coll_targets, audit) from local refreshed pages."""
    pdir = REPO / "data" / play / "page_annotated"
    supp = SUPPLEMENT_FROM.get(play)
    cands = defaultdict(list)   # nr -> [(val, framed, has_fw, line_id, txt)]
    coll, audit = [], []
    ptypes, has_text, first_line = {}, {}, {}
    pages = play_pages(pdir)
    for nr, path in pages:
        tree = etree.parse(str(path))
        ptypes[nr] = page_type(tree)
        n_lines = 0
        for tl in tree.iter(NS + "TextLine"):
            txt = line_text(tl)
            if txt.strip():
                n_lines += 1
                first_line.setdefault(nr, txt.strip()[:44])
            spans = parse_custom(tl.get("custom") or "")
            m = SHAPE_RE.match(txt)
            if m:
                cands[nr].append((int(m.group(1)), bool(FRAME_RE.search(txt)),
                                  any(t == "fw" for t, _ in spans),
                                  tl.get("id"), txt))
            if ptypes[nr] in ("titlePage", "castList"):
                continue
            if supp is not None and nr > supp:
                continue
            if any(t == "speaker" for t, _ in spans):
                continue
            mt = TURN_RE.match(txt)
            if mt and is_collective_label(mt.group(1)):
                coll.append((nr, tl.get("id"),
                             COLLECTIVE_XMLID.get(skel(mt.group(1)), skel(mt.group(1)))))
        has_text[nr] = n_lines > 0

    # fit printed = a*nr + c
    fit_pool = [(nr, v) for nr, cs in cands.items() for v, fr, *_ in cs if fr] \
        or [(nr, v) for nr, cs in cands.items() for v, fr, *_ in cs]
    best_a, best_c, best_n = 1, None, -1
    for a in (1, 2):
        cnt = Counter(v - a * nr for nr, v in fit_pool)
        if not cnt:
            continue
        c, n = cnt.most_common(1)[0]
        if n > best_n:
            best_a, best_c, best_n = a, c, n

    fw_targets = []
    body = [nr for nr, _ in pages if ptypes.get(nr) not in ("titlePage", "castList")]
    for nr in body:
        exp = best_a * nr + best_c if best_c is not None else None
        cs = cands.get(nr, [])
        pick = None
        for want_framed, want_consistent in ((True, True), (True, False), (False, True)):
            for val, fr, has_fw, lid, txt in cs:
                if fr == want_framed and (not want_consistent or exp is None or val == exp):
                    pick = (val, fr, has_fw, lid, txt)
                    break
            if pick:
                break
        if pick:
            val, fr, has_fw, lid, txt = pick
            if exp is not None and val != exp:
                audit.append((play, nr, "value-suspect",
                              f"reads {val}, expected {exp}: {txt.strip()!r}"))
            if not has_fw:
                fw_targets.append((nr, lid))
        else:
            if not has_text.get(nr):
                continue
            fl = first_line.get(nr, "")
            kind = "unnumbered-heading-page" if HEADING_RE.search(fl) else "missing-pagenum"
            if exp is not None:
                audit.append((play, nr, kind, f"expected {exp}; first line: {fl!r}"))
            for val, fr, has_fw, lid, txt in cs:
                audit.append((play, nr, "bare-number-inconsistent",
                              f"{txt.strip()!r} (expected {exp}) — NOT tagged"))
    return fw_targets, coll, audit, (best_a, best_c)


def doc_tops(client, doc):
    """page -> (tsId, userName, url) of the top transcript, one fulldoc call."""
    fd = client.fulldoc(COL, doc)
    out = {}
    for p in fd.get("pageList", {}).get("pages", []):
        tss = p.get("tsList", {}).get("transcripts") or []
        if tss:
            out[int(p.get("pageNr"))] = (tss[0].get("tsId"), tss[0].get("userName"),
                                         tss[0]["url"])
    return out


def find_line(root, line_id):
    for tl in root.iter(NS + "TextLine"):
        if tl.get("id") == line_id:
            return tl
    return None


def apply_fw(root, line_id):
    tl = find_line(root, line_id)
    if tl is None:
        return False, "line not found on server"
    entries = parse_custom(tl.get("custom") or "")
    if any(t == "fw" for t, _ in entries):
        return False, "already has fw"
    txt = line_text(tl)
    if not SHAPE_RE.match(txt):
        return False, f"live text no longer a page number ({txt[:16]!r})"
    sp = trimmed_span(txt)
    if not sp:
        return False, "empty live line"
    entries.append(("fw", {"offset": str(sp[0]), "length": str(sp[1] - sp[0]),
                           "type": "pageNum"}))
    tl.set("custom", serialize_custom(entries))
    return True, f"+fw pageNum {txt.strip()!r}"


def apply_coll(root, line_id, xmlid):
    tl = find_line(root, line_id)
    if tl is None:
        return False, "line not found on server"
    entries = parse_custom(tl.get("custom") or "")
    if any(t == "speaker" for t, _ in entries):
        return False, "already has speaker"
    txt = line_text(tl)
    m = TURN_RE.match(txt)
    if not m or not is_collective_label(m.group(1)):
        return False, f"live text no longer a collective turn ({txt[:16]!r})"
    entries.append(("speaker", {"offset": "0", "length": str(len(m.group(1))),
                                "xmlid": xmlid}))
    tl.set("custom", serialize_custom(entries))
    return True, f"+speaker xmlid:{xmlid}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only")
    args = ap.parse_args()

    doc_ids = load_doc_ids()
    plays = [args.only] if args.only else \
        sorted(d.parent.name for d in REPO.glob("data/*/page_annotated"))
    client = None if args.dry_run else TrpClient.from_env()
    note = f"YiDraCor pageNum+collective tagging {_dt.date.today().isoformat()}"

    all_audit = []
    used_xmlids = defaultdict(set)
    tot_fw = tot_sp = tot_pages = tot_skip = tot_fail = 0
    for play in plays:
        doc = doc_ids.get(play)
        if doc is None:
            print(f"[{play}] no transkribus_doc_id — skip", flush=True)
            continue
        fw_targets, coll_targets, audit, (a, c) = scan_play(play)
        all_audit.extend(audit)
        per_page = defaultdict(lambda: {"fw": [], "coll": []})
        for nr, lid in fw_targets:
            per_page[nr]["fw"].append(lid)
        for nr, lid, xmlid in coll_targets:
            per_page[nr]["coll"].append((lid, xmlid))
            used_xmlids[play].add(xmlid)
        print(f"\n=== {play} (doc {doc}, model printed={a}*page{c:+d}) — "
              f"{len(fw_targets)} fw + {len(coll_targets)} collective "
              f"on {len(per_page)} pages ===", flush=True)
        tops = None if args.dry_run else doc_tops(client, doc)
        for nr in sorted(per_page):
            jobs = per_page[nr]
            if args.dry_run:
                print(f"  p{nr}: [dry-run] {len(jobs['fw'])} fw, "
                      f"{len(jobs['coll'])} coll "
                      f"{[x for _, x in jobs['coll']]}", flush=True)
                tot_pages += 1
                tot_fw += len(jobs["fw"]); tot_sp += len(jobs["coll"])
                continue
            if nr not in tops:
                print(f"  p{nr}: no server transcript — skip", flush=True)
                tot_skip += 1
                continue
            tsid, owner, url = tops[nr]
            xml = client.fetch_transcript(url)
            root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
            changed = False
            for lid in jobs["fw"]:
                ok, st = apply_fw(root, lid)
                if ok:
                    changed = True; tot_fw += 1
                elif "not found" in st or "no longer" in st:
                    tot_fail += 1; print(f"  p{nr} fw {lid}: ✗ {st}", flush=True)
            for lid, xmlid in jobs["coll"]:
                ok, st = apply_coll(root, lid, xmlid)
                if ok:
                    changed = True; tot_sp += 1
                elif "not found" in st or "no longer" in st:
                    tot_fail += 1; print(f"  p{nr} coll {lid}: ✗ {st}", flush=True)
            if not changed:
                tot_skip += 1
                continue
            client.push_transcript(COL, doc, nr, etree.tostring(root, encoding="unicode"),
                                   parent_tsid=tsid, status="IN_PROGRESS", note=note,
                                   tool_name="YiDraCor-annotation-pipeline")
            tot_pages += 1
            print(f"  p{nr}: → pushed (parent {tsid}, top by {owner})", flush=True)

    out = AUDIT_DIR / f"pagenum_audit_{_dt.date.today().isoformat()}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["play", "page", "kind", "detail"])
        w.writerows(all_audit)
    print(f"\n{'DRY-RUN ' if args.dry_run else ''}SUMMARY: {tot_fw} fw spans, "
          f"{tot_sp} speaker spans, {tot_pages} pages "
          f"{'would push' if args.dry_run else 'pushed'}, {tot_skip} skipped, "
          f"{tot_fail} live-verify failures", flush=True)
    print(f"audit rows: {len(all_audit)} -> {out}", flush=True)
    print("\nCollective xml:ids used (declare in cast_dict):", flush=True)
    for play, ids in sorted(used_xmlids.items()):
        print(f"  {play}: {sorted(ids)}", flush=True)


if __name__ == "__main__":
    main()
