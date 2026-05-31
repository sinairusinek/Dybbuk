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

import json


# RA-corroborated single-token manner/emotion adverbs (precision 4/4 in
# 2026-05-31 RA-edit corpus; backlog of 16 pipeline `business` tags this flips
# to the correct `delivery`).
EMOTION_ADVERBS = {
    "בייז", "שרייט", "ברוגז", "שפעטיש", "זיגנענד", "אפארט",
    "בעגייסטערט", "פערקלעהרט", "פערקלערט", "פערשעמט",
    "לאכענד", "ערנסט", "שטיל",
}

# Compound-action verbs that, if present alongside an entrance cue, mean the
# direction is doing more than entering — punt to `business`/`mixed` rather than
# auto-typing `entrance`. Guard surfaced by DerManUnterTiff p11.
_COMPOUND_ACTION = {"זעצט", "גיט", "נעמט", "קושט", "שאקעלט", "שטעלט",
                    "הויבט", "דרעהט", "ציהט", "פאלט"}

_HEB_TOKEN = re.compile(r"[א-תװ-ײ']+")


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


def stage_lexicon_span(span_text: str):
    """Return a stage type for a high-precision span-level cue, else None.

    Calibrated against RA edits 2026-05-31 — see
    `data/review/ra_corrections_analysis_2026-05-31.md`. All multi-word rules
    are gated on span length (≤ 6 Hebrew tokens) and absence of a sentence-
    boundary period — compound directions stay typed as the LLM had them,
    since rules-of-thumb don't generalise across multi-clause spans.
    """
    sk = _NIKUD.sub("", span_text or "")
    tokens = _HEB_TOKEN.findall(sk)
    if not tokens:
        return None
    token_set = set(tokens)
    short = len(tokens) <= 6
    no_period = "." not in span_text

    # Single-token emotion/manner adverb → delivery. Strictly single-token
    # (excluding the surrounding parens already stripped by _HEB_TOKEN).
    if len(tokens) == 1 and tokens[0] in EMOTION_ADVERBS:
        return "delivery"

    # 'אב' = off-stage / exit. Only when אב is the LAST Hebrew token of the
    # span (the trailing convention "<actor> אב.") and the span is short.
    # Modal/auxiliary guard: "<actor> וויל/זאל/מוז/דארף/קען אב" = intention
    # to leave, not an actual exit — this is `business`, not `exit`. The PI
    # flagged this 2026-05-31 (4 LLM-typed exits in the corpus to undo).
    _MODAL_BEFORE_AB = {"וויל", "ויל", "וויעל",
                        "זאל", "זאָל",
                        "מוז", "מוּז",
                        "דארף", "דאַרף", "דארפט", "דאַרפט",
                        "קען", "קעהן",
                        "געהט", "גייט", "גיט"}
    if short and no_period and tokens[-1] in {"אב", "אבּ"}:
        prev = tokens[-2] if len(tokens) >= 2 else ""
        if prev in _MODAL_BEFORE_AB:
            return "business"
        return "exit"

    # Beyond this point, compound directions are filtered out.
    if not (short and no_period):
        return None

    has_action = bool(token_set & _COMPOUND_ACTION)
    if not has_action:
        if tokens[0] in {"אויפטריט", "אויפטרעטען", "אויפטרעטן"} and len(tokens) <= 5:
            return "entrance"
        if "אריין" in token_set and (token_set & {"קומט", "קומען"}):
            return "entrance"
    if "צימער" in token_set and len(tokens) <= 5:
        return "setting"
    return None


def load_cast_bares(play: str) -> dict[str, str]:
    """{xmlid -> bare} restricted to roles whose bare form is multi-word —
    used by the speaker span re-anchor (P1)."""
    f = REPO / "data" / play / "cast_dict.json"
    if not f.exists():
        return {}
    d = json.loads(f.read_text(encoding="utf-8"))
    out = {}
    for xmlid, info in d.get("roles", {}).items():
        bare = (info.get("bare") or "").strip()
        if bare and len(bare.split()) > 1:
            out[xmlid] = bare
    return out


def _nikud_tolerant(bare: str) -> re.Pattern:
    """Compile a regex that matches `bare` allowing arbitrary nikud after each
    consonant and any whitespace between tokens."""
    parts = []
    for ch in bare:
        if ch.isspace():
            parts.append(r"\s+")
        else:
            parts.append(re.escape(ch) + r"[֑-ׇ]*")
    return re.compile("".join(parts))


def fix_stage_type_typo(t: str):
    """Map a near-miss stage @type (e.g. 'settingָ') to a valid one, else None."""
    clean = _NIKUD.sub("", t or "")
    clean = "".join(c for c in clean if c.isascii()).strip()
    return clean if clean in STAGE_TYPES else None


def resolve_line(text: str, entries, cast_index, cast_bares=None):
    """Return (new_entries, [auto_descriptions], [human_issues]).

    new_entries is None if nothing auto-changed. Operates on a single line's
    parsed custom entries; safe to run on the live transcript (idempotent).

    cast_bares is {xmlid -> multi-word bare form} used to re-anchor a speaker
    span the LLM truncated to the first token (P1, ben_kaspi case).
    """
    cast_bares = cast_bares or {}
    auto, human = [], []
    out = []

    # Pre-pass: drop untyped `stage` spans that overlap a typed `stage` span on
    # the same line. Happens when a programmatic annotator (or LLM pass)
    # emits a stage tag without inspecting existing custom — both end up in
    # the live transcript and the schema "no same-tag overlap" rule fires.
    typed_stage_ranges = []
    for tag, a in entries:
        if tag == "stage" and a.get("type"):
            try:
                off = int(a.get("offset", 0)); ln = int(a.get("length", 0))
                typed_stage_ranges.append((off, off + ln))
            except (ValueError, TypeError):
                pass
    filtered_entries = []
    for tag, a in entries:
        if tag == "stage" and not a.get("type"):
            try:
                off = int(a.get("offset", 0)); ln = int(a.get("length", 0))
                end = off + ln
                if any(not (end <= ts or off >= te)
                       for ts, te in typed_stage_ranges):
                    auto.append("drop overlapping untyped stage")
                    continue
            except (ValueError, TypeError):
                pass
        filtered_entries.append((tag, a))
    entries = filtered_entries

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
            # span content for span-level cues
            try:
                off = int(a.get("offset", 0)); ln = int(a.get("length", 0))
                span_text = text[off:off + ln]
            except (ValueError, TypeError):
                span_text = ""
            # 2a. trailer/setting/epilog cues — applied to SPAN content (not
            #     full line) so a delivery direction sharing a line with a
            #     setting-cue isn't stomped. Overrides LLM-set type (P0).
            #     Gated on ≤ 5 Hebrew tokens so the cue must dominate the
            #     span (vs. occur incidentally in a 10-token song framing).
            span_tokens = _HEB_TOKEN.findall(_NIKUD.sub("", span_text))
            span_lex = stage_lexicon(span_text) if len(span_tokens) <= 5 else None
            if span_lex == "trailer":
                if t != "trailer":
                    auto.append(f"stage{{type:{t or '∅'}}}→trailer (span-cue)")
                out.append(("trailer", {k: v for k, v in a.items() if k != "type"}))
                continue
            if span_lex == "epilog":
                ha = {k: v for k, v in a.items() if k in ("offset", "length")}
                ha["type"] = "epilog"
                if t != "epilog":
                    auto.append(f"stage{{type:{t or '∅'}}}→heading:epilog (span-cue)")
                out.append(("heading", ha)); continue
            if span_lex == "setting":
                if t != "setting":
                    a = dict(a); a["type"] = "setting"
                    auto.append(f"stage{{type:{t or '∅'}}}→setting (span-cue)")
                out.append((tag, a)); continue
            # 2b. other span-level cues
            span_type = stage_lexicon_span(span_text)
            if span_type and span_type != t:
                a = dict(a); a["type"] = span_type
                auto.append(f"stage{{type:{t or '∅'}}}→{span_type} (span-cue)")
                out.append((tag, a)); continue
            # 2c. accept valid LLM type
            if t in STAGE_TYPES:
                out.append((tag, a)); continue
            # 2d. typo repair / human flag
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
        # 4. speaker span re-anchor — LLM truncated a multi-word cast name to
        #    its first token (P1, ben_kaspi case)
        if tag == "speaker" and a.get("xmlid") in cast_bares:
            try:
                off = int(a.get("offset", 0)); ln = int(a.get("length", 0))
            except (ValueError, TypeError):
                off, ln = 0, 0
            bare = cast_bares[a["xmlid"]]
            m = _nikud_tolerant(bare).match(text, off) if off + ln <= len(text) else None
            if m and (m.end() - off) > ln:
                new_ln = m.end() - off
                a = dict(a); a["length"] = str(new_ln)
                auto.append(f"speaker span {ln}→{new_ln} (re-anchor {bare!r})")
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
    bares_cache: dict[str, dict] = {}
    for r in rows:
        folder = label_to_folder.get(r["edition"])
        if folder is None:
            r["_keep"] = True; continue  # unknown edition — keep to be safe
        by_page[(folder, int(r["page(s)"]))].append(r)
    kept = []
    for (folder, page), group in by_page.items():
        doc = doc_ids.get(folder)
        cast = cast_cache.setdefault(folder, load_cast(folder)[0])
        bares = bares_cache.setdefault(folder, load_cast_bares(folder))
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
            _, _, human = resolve_line(line_text(tl), body, cast, bares)
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
        cast_bares = load_cast_bares(play)
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
                scan = resolve_line(txt, [e for e in entries if e[0] != "readingOrder"], cast_index, cast_bares)
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
                new, auto, _ = resolve_line(line_text(tl), body, cast_index, cast_bares)
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
