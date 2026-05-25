"""Auto-resolve the mechanical lint flags on Transkribus; leave only the flags
that genuinely need a human in the review list.

For each annotated line it computes the confidently-mechanical fixes and applies
them to the live top transcript (idempotent, parent-layered push). What it
resolves:

  drop legacy tags        : `unclear`, `textStyle`, `Header`, `head{unit-type}`
                            (front-matter cruft / superseded by a real `heading`)
  stage type by lexicon   : פערוואנדלונג / פאָרהאַנג[ פאלט] → type:setting;
                            ענדע … → retag `trailer`; עפילאג → heading{type:epilog}
  stage type typo         : e.g. `settingָ` (stray nikud) → `setting`
  untagged named speaker  : turn label resolvable via cast_dict → add `speaker`
  speaker missing xmlid   : existing speaker span, label resolvable → set xmlid

What it deliberately LEAVES for a human (written to the trimmed CSV):
  untagged speaker (unknown)        — OCR-mangled or not in cast (needs a person)
  untyped/invalid stage, no lexicon — parenthesised action dirs need a type call
  span out of range / unknown 'add' — anchoring / schema-gap, manual
  unreferenced cast / act numbering — informational

Collective speakers are handled by apply_collective_speakers.py and are skipped
here. Runs over `page_annotated/` to find candidate pages, then operates on the
live transcript.

Run:
  python -m annotation.auto_resolve_flags --dry-run
  python -m annotation.auto_resolve_flags --only KidushHashem
  python -m annotation.auto_resolve_flags --out data/review/needs_human_2026-05-25.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import re
import sys
from collections import defaultdict
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (
    parse_custom, serialize_custom, is_collective_label, validate_span,
    STAGE_TYPES, _NIKUD,
)
from annotation.lint_pages import (
    NS, REPO, TURN_RE, skel, has_nikud, line_text, page_type, page_files,
    load_cast, load_editions, FLAG_COLUMNS,
)
from annotation.apply_collective_speakers import load_doc_ids, top_transcript, find_line, COL


def stage_lexicon(text: str):
    """Return ('setting'|'trailer'|'epilog') for a known scene-boundary cue, else None."""
    sk = _NIKUD.sub("", text or "")
    sk = re.sub(r"[()\s.,׃:‐-―\-]", "", sk)
    if "פערוואנדלונג" in sk or "פארהאנג" in sk:
        return "setting"
    if sk.startswith("ענדע"):
        return "trailer"
    if "עפילאג" in sk or "עפּילאג" in sk:
        return "epilog"
    return None


def fix_stage_type_typo(t: str):
    """Map a near-miss stage @type (e.g. 'settingָ') to a valid one, else None."""
    clean = _NIKUD.sub("", t or "")
    clean = "".join(c for c in clean if c.isascii()).strip()
    return clean if clean in STAGE_TYPES else None


def resolve_line(text: str, entries, cast_index):
    """Return (new_entries, [auto_descriptions], [human_issues]).

    new_entries is None if nothing auto-changed. Operates on a single line's
    parsed custom entries; safe to run on the live transcript (idempotent).
    """
    auto, human = [], []
    out = []
    lex = stage_lexicon(text)

    for tag, a in entries:
        # 1. drop legacy / editorial cruft
        if tag in ("unclear", "textStyle", "Header"):
            auto.append(f"drop {tag}")
            continue
        if tag == "head" and "unit-type" in a:
            auto.append("drop head{unit-type}")
            continue
        # 2. stage type
        if tag == "stage":
            t = a.get("type")
            if t in STAGE_TYPES:
                out.append((tag, a))
                continue
            if lex == "setting":
                a = dict(a); a["type"] = "setting"; auto.append("stage→setting")
                out.append((tag, a)); continue
            if lex == "trailer":
                auto.append("stage→trailer")
                out.append(("trailer", {k: v for k, v in a.items() if k != "type"})); continue
            if lex == "epilog":
                ha = {k: v for k, v in a.items() if k in ("offset", "length")}
                ha["type"] = "epilog"; auto.append("stage→heading:epilog")
                out.append(("heading", ha)); continue
            fixed = fix_stage_type_typo(t) if t else None
            if fixed:
                a = dict(a); a["type"] = fixed; auto.append(f"stage type {t!r}→{fixed}")
                out.append((tag, a)); continue
            human.append("untyped stage (no lexicon)" if not t
                         else f"invalid stage.type {t!r}")
            out.append((tag, a)); continue
        # 3. speaker missing xmlid
        if tag == "speaker" and not a.get("xmlid"):
            try:
                off = int(a.get("offset", 0)); ln = int(a.get("length", 0))
                label = skel(text[off:off + ln])
            except ValueError:
                label = ""
            if label in cast_index:
                a = dict(a); a["xmlid"] = cast_index[label]
                auto.append(f"speaker xmlid:{cast_index[label]}")
            else:
                human.append("speaker missing xmlid (unresolved)")
            out.append((tag, a)); continue
        out.append((tag, a))

    # 4. untagged named speaker turn → add speaker span
    if not any(t == "speaker" for t, _ in out):
        m = TURN_RE.match(text)
        if m:
            label = m.group(1); k = skel(label)
            if is_collective_label(label):
                pass  # handled by apply_collective_speakers.py
            elif k in cast_index:
                out.append(("speaker", {"offset": "0", "length": str(len(label)),
                                        "xmlid": cast_index[k]}))
                auto.append(f"+speaker xmlid:{cast_index[k]}")
            elif (not has_nikud(label)) and has_nikud(text[m.end():]):
                human.append(f"untagged speaker (unknown) '{k}'")

    changed = bool(auto)
    return (out if changed else None), auto, human


def recheck_live(csv_path: Path):
    """Re-validate each row of a needs-human CSV against the LIVE transcript and
    drop rows whose issue has since been fixed on the server. Rewrites in place."""
    editions = load_editions()
    label_to_folder = {v: k for k, v in editions.items()}
    doc_ids = load_doc_ids()
    from transkribus.client import TrpClient
    client = TrpClient.from_env()
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    # group by (folder, page) to fetch each page once
    by_page: dict[tuple[str, int], list] = defaultdict(list)
    cast_cache: dict[str, dict] = {}
    for r in rows:
        folder = label_to_folder.get(r["edition"])
        if folder is None:
            r["_keep"] = True; continue  # unknown edition — keep to be safe
        by_page[(folder, int(r["page(s)"]))].append(r)
    kept = []
    for (folder, page), group in by_page.items():
        doc = doc_ids.get(folder)
        cast = cast_cache.setdefault(folder, load_cast(folder)[0])
        if doc is None:
            kept.extend(group); continue
        _, _, xml = top_transcript(client, doc, page)
        root = etree.fromstring(xml.encode("utf-8")) if isinstance(xml, str) else (
            etree.fromstring(xml) if xml else None)
        for r in group:
            tl = find_line(root, r["line_id / count"]) if root is not None else None
            if tl is None:
                kept.append(r); continue  # can't verify — keep
            body = [e for e in parse_custom(tl.get("custom") or "") if e[0] != "readingOrder"]
            _, _, human = resolve_line(line_text(tl), body, cast)
            cat = r["category"]
            still = any(h.split(" (")[0] == cat or h.startswith(cat) for h in human)
            if still:
                r["text"] = line_text(tl)[:50]
                kept.append(r)
    out_rows = [r for r in rows if r.get("_keep")] + [r for r in kept]
    # de-dup preserve order
    seen = set(); final = []
    for r in out_rows:
        k = (r["edition"], r["page(s)"], r["line_id / count"], r["category"])
        if k in seen:
            continue
        seen.add(k); final.append({c: r.get(c, "") for c in FLAG_COLUMNS})
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FLAG_COLUMNS)
        w.writeheader(); w.writerows(final)
    print(f"recheck-live: {len(rows)} → {len(final)} rows still open on Transkribus "
          f"→ {csv_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    ap.add_argument("--out", help="trimmed needs-human CSV (default data/review/needs_human_<date>.csv)")
    ap.add_argument("--recheck", metavar="CSV",
                    help="re-validate an existing needs-human CSV against live Transkribus and rewrite it")
    args = ap.parse_args()

    if args.recheck:
        recheck_live(Path(args.recheck) if Path(args.recheck).is_absolute()
                     else REPO / args.recheck)
        return 0

    editions = load_editions()
    doc_ids = load_doc_ids()
    plays = [args.only] if args.only else sorted(
        p.name for p in (REPO / "data").iterdir() if (p / "page_annotated").is_dir())

    client = None  # lazy
    note = f"YiDraCor auto-resolve mechanical flags {_dt.date.today().isoformat()}"
    human_rows: list[dict] = []
    n_auto = n_push = n_human = 0

    for play in plays:
        cast_index, _ = load_cast(play)
        label = editions.get(play, play)
        doc = doc_ids.get(play)
        # Phase 1: local scan → candidate pages (auto edits) + human flags
        candidate_pages = set()
        for page, path in page_files(play):
            tree = etree.parse(str(path))
            ptype = page_type(tree)
            for tl in tree.iter(NS + "TextLine"):
                txt = line_text(tl)
                entries = parse_custom(tl.get("custom") or "")
                # don't raise untagged-speaker on title/cast pages
                scan = resolve_line(txt, [e for e in entries if e[0] != "readingOrder"], cast_index)
                _, auto, human = scan
                if ptype in ("titlePage", "castList"):
                    human = [h for h in human if "speaker" not in h]
                if auto:
                    candidate_pages.add(page)
                for h in human:
                    human_rows.append({
                        "edition": label, "page(s)": str(page),
                        "line_id / count": tl.get("id"), "category": h.split(" (")[0],
                        "owner": "NOA", "issue/detail": h, "text": txt[:50],
                        "suggested_action": "manual",
                    })
                    n_human += 1
        if not candidate_pages or doc is None:
            continue
        if client is None:
            from transkribus.client import TrpClient
            client = TrpClient.from_env()
        print(f"\n=== {label} (doc {doc}) — {len(candidate_pages)} candidate pages ===")
        for page in sorted(candidate_pages):
            tsid, owner, xml = top_transcript(client, doc, page)
            if xml is None:
                print(f"  p{page}: no server transcript — skip"); continue
            root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
            page_changed = False
            for tl in root.iter(NS + "TextLine"):
                entries = parse_custom(tl.get("custom") or "")
                ro = [e for e in entries if e[0] == "readingOrder"]
                body = [e for e in entries if e[0] != "readingOrder"]
                new, auto, _ = resolve_line(line_text(tl), body, cast_index)
                if new is not None:
                    tl.set("custom", serialize_custom(ro + new))
                    page_changed = True; n_auto += len(auto)
                    print(f"  p{page} {tl.get('id')}: " + ", ".join(auto))
            if not page_changed:
                continue
            if args.dry_run:
                n_push += 1; print(f"  p{page}: [dry-run] would push (parent {tsid}, top {owner})")
                continue
            client.push_transcript(COL, doc, page, etree.tostring(root, encoding="unicode"),
                                   parent_tsid=tsid, status="IN_PROGRESS", note=note,
                                   tool_name="YiDraCor-annotation-pipeline")
            n_push += 1; print(f"  p{page}: → pushed (parent {tsid})")

    out = Path(args.out) if args.out else REPO / "data" / "review" / f"needs_human_{_dt.date.today()}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FLAG_COLUMNS)
        w.writeheader(); w.writerows(human_rows)
    print(f"\n{'DRY-RUN ' if args.dry_run else ''}SUMMARY: {n_auto} auto-edits on "
          f"{n_push} pages {'to push' if args.dry_run else 'pushed'}; "
          f"{n_human} flags left for humans → {out.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
