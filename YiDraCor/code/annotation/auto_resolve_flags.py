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
    parse_custom, serialize_custom, dedup_entries, is_collective_label, validate_span,
    STAGE_TYPES, _NIKUD, parse_act_heading, parse_scene_heading,
    COLLECTIVE_XMLID as _COLLECTIVE_XMLID,
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
    # 2026-06-24 (Sinai general correction #5): vocal-performance verbs
    # in 3sg/participle. וויינט/לאכט were misclassified as `business` in
    # the Ezra+Blimele pull; וויינענד matches existing -ענד participle pattern.
    "וויינט", "לאכט", "וויינענד",
}

# Compound-action verbs that, if present alongside an entrance cue, mean the
# direction is doing more than entering — punt to `business`/`mixed` rather than
# auto-typing `entrance`. Guard surfaced by DerManUnterTiff p11.
_COMPOUND_ACTION = {"זעצט", "גיט", "נעמט", "קושט", "שאקעלט", "שטעלט",
                    "הויבט", "דרעהט", "ציהט", "פאלט"}

_HEB_TOKEN = re.compile(r"[א-תװ-ײ']+")

# Collective xmlid map now lives in annotation.schema (imported above) —
# it was an identical hand-maintained duplicate of that one.


def _is_global_a(text: str) -> bool:
    """True for a castList closing line that announces the locus/time of action."""
    sk = _NIKUD.sub("", text or "")
    sk = re.sub(r"[()\s.,׃:‐-―\-]", "", sk)
    return (sk.startswith("ארטדערהאנדלונג") or sk.startswith("ארטהאנדלונג")
            or sk.startswith("דיגעשיכטעהאנדעלטזיך")
            or sk.startswith("דיאגעשיכטעהאנדעלטזיך"))


def apply_global_a(root) -> list[str]:
    """Global A (Noa 2026-06-14) as a PAGE-level pass on castList pages.

    A closing line announcing where/when the action happens is not a role — it
    is a whole-line stage{type:setting}. On Al Naharot p.6 both such lines were
    tagged `roleDesc`, which attaches them to the preceding castItem (Delilah)
    in the TEI and is silently invisible to every existing check:
      * lint only flags a castList line carrying NO role/roleDesc span, so a
        MIS-tagged line reads as tagged;
      * `stage_lexicon` answers "what type should this stage span be?", never
        "should this roleDesc have been a stage span?".

    Continuation matters: the second line (`נאך חרבן בית ראשון.`) carries no cue
    of its own, so we carry through following lines until one bears a `role`
    span. Global-C brace labels (`זיינע קינדער`) never match a Global-A prefix,
    so shared roleDescs are untouched.
    """
    if page_type(root) != "castList":
        return []
    changed, carrying = [], False
    for tl in root.iter(NS + "TextLine"):
        txt = line_text(tl)
        if not txt.strip():
            continue
        entries = parse_custom(tl.get("custom") or "")
        tags = {t for t, _ in entries}
        if "role" in tags:
            carrying = False
            continue
        if _is_global_a(txt):
            carrying = True
        elif not carrying:
            continue
        out, hit = [], False
        for tag, a in entries:
            if tag == "roleDesc":
                out.append(("stage", {"offset": a.get("offset", "0"),
                                      "length": a.get("length", str(len(txt.rstrip()))),
                                      "type": "setting"}))
                hit = True
            else:
                out.append((tag, a))
        if not hit and "stage" not in tags:
            out.append(("stage", {"offset": "0",
                                  "length": str(len(txt.rstrip())),
                                  "type": "setting"}))
            hit = True
        if hit:
            tl.set("custom", serialize_custom(out))
            changed.append(f"Global-A: → stage{{type:setting}} {txt.strip()[:34]!r}")
    return changed


_HEADING_RX = re.compile(r"heading\s*\{[^}]*type:(act|scene)")


def apply_opening_setting(root) -> list[str]:
    """The parenthesised direction opening an act/scene is a `setting`.

    Sinai 2026-07-20 (BasSheva p7). `stage_lexicon` is purely lexical — it types
    a direction `setting` only on a cue word (פערוואנדלונג / פאָרהאַנג / אָרט
    דער האַנדלונג). An act-opening tableau names no cue; it just describes the
    stage. So the corpus split by accident of wording: 22 openers came out
    `setting`, 15 `business`. Position, not vocabulary, is what identifies these.

    The whole opening parenthesis is ONE `setting`, even where it describes
    people in motion — that is the established corpus reading, not a new call:
    Al Naharot p51 `(דער קעניג זיצט אויף דעם טראהן…)` and p38 `(… מעדכען
    זינגענדיג)` are both plain `setting` today. Hence no compound
    `type="setting business"` here; compound typing stays for directions that
    genuinely do two jobs mid-scene.

    Deliberately conservative:
      * only retypes `business` or an untyped stage span. `entrance` and
        `delivery` openers are left alone — `(קאהר ווי אנפאנג פונ'ם צווייטען
        אקט)` on Kidush ha-Shem p47 sits in opening position but is a musical
        instruction, and only a human should overrule those five.
      * requires the direction to open with `(`, so an unparenthesised stray
        line after a heading is not swept up.
      * carries across continuation lines only while the parenthesis is still
        unclosed — BasSheva p7's opener runs over two printed lines.
    """
    changed, armed, carrying = [], False, False
    for tl in root.iter(NS + "TextLine"):
        txt = line_text(tl)
        if not txt.strip():
            continue
        custom = tl.get("custom") or ""
        if _HEADING_RX.search(custom):
            armed, carrying = True, False
            continue
        if not (armed or carrying):
            continue
        entries = parse_custom(custom)
        tags = {t for t, _ in entries}
        if "speaker" in tags or "stage" not in tags:
            armed = carrying = False
            continue
        if not carrying and not txt.lstrip().startswith("("):
            armed = False
            continue
        out, hit = [], False
        for tag, a in entries:
            if tag == "stage" and (a.get("type") or "business") == "business":
                a = dict(a); a["type"] = "setting"
                hit = True
            out.append((tag, a))
        if hit:
            tl.set("custom", serialize_custom(out))
            changed.append(f"opening: stage→setting {txt.strip()[:40]!r}")
        # Keep going only while the parenthesis stays open.
        carrying = txt.count("(") > txt.count(")")
        armed = False
    return changed


def stage_lexicon(text: str):
    """Return ('setting'|'trailer'|'epilog') for a known scene-boundary cue, else None."""
    sk = _NIKUD.sub("", text or "")
    sk = re.sub(r"[()\s.,׃:‐-―\-]", "", sk)
    if ("פערוואנדלונג" in sk or "פערווענלונג" in sk or "פערוואנדעלונג" in sk
            or "פערענדלונג" in sk  # Sinai 2026-06-24 general correction #2
            or "פארהאנג" in sk):
        return "setting"
    # Global-A (Noa 2026-06-14): castList-page closing lines that announce
    # the locus/time of action — "אָרט דער האַנדלונג…" / "דיא געשיכטע האנדעלט זיך…"
    # Match on de-nikud'd, de-punctuated prefix to tolerate vocalization +
    # spelling variants (האנדעלונג / האנדלונג, דיא / די).
    if sk.startswith("ארטדערהאנדלונג") or sk.startswith("ארטהאנדלונג"):
        return "setting"
    if sk.startswith("דיגעשיכטעהאנדעלטזיך") or sk.startswith("דיאגעשיכטעהאנדעלטזיך"):
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
    # Entrance/exit cues — TEI-principled multi-token typing (Noa 2026-06-18):
    # when an entrance OR exit cue co-occurs with another action verb in the
    # same direction, emit space-separated `@type` (e.g. "entrance business",
    # "exit business"); pure entrance/exit stays single-token.
    # Per TEI P5 spec, the literal value `mixed` is reserved as a single-value
    # fallback; we no longer emit it from this lexicon.
    has_action = bool(token_set & _COMPOUND_ACTION)
    # 2026-06-24 (Sinai general correction #3): include the infinitive
    # `ערשיינען` alongside present-tense `ערשיינט` so castList ensemble
    # entrance lines ("…ערשיינען") are typed correctly.
    has_ersheynt = bool(token_set & {"ערשיינט", "ערשײנט", "ערשיינען", "ערשײנען"})
    has_oyftrit = tokens and tokens[0] in {"אויפטריט", "אויפטרעטען", "אויפטרעטן"}
    has_arayn_kumt = "אריין" in token_set and bool(token_set & {"קומט", "קומען"})
    has_entrance_cue = has_ersheynt or has_oyftrit or has_arayn_kumt
    has_ab = "אב" in token_set or "אבּ" in token_set

    # Entrance cue AND exit cue in one direction ("Yokhtshe exits, enter
    # Sabele") → `exit entrance`. Sinai 2026-07-20, resolving the B9-vs-ST3
    # collision: B9 (2026-06-24) said this MUST be literal `mixed`, but option C
    # (2026-06-18) reserves `mixed` for functions that CANNOT be enumerated, and
    # entrance+exit plainly can — the option-C document even lists
    # `exit entrance` as its example. Option C wins.
    # This must short-circuit BEFORE the per-cue branches below.
    if short and no_period and has_ersheynt and has_ab:
        return "exit entrance"
    if short and no_period and tokens[-1] in {"אב", "אבּ"}:
        prev = tokens[-2] if len(tokens) >= 2 else ""
        if prev in _MODAL_BEFORE_AB:
            return "business"
        # exit + extra non-modal action in same direction → "exit business"
        # (e.g. "<actor> X אב" where X is another action verb).
        if len(tokens) > 2:
            return "exit business"
        return "exit"
    # exit cue NOT at end-of-span but present elsewhere together with another
    # token → "exit business" (e.g. "(אב, שטורם)" = exits + storming).
    if short and no_period and has_ab and len(tokens) >= 2:
        return "exit business"

    # Beyond this point, compound directions are filtered out.
    if not (short and no_period):
        # …with one exception: ערשיינט inside a longer/punctuated span
        # is still entrance / entrance+business (e.g.
        # "(לעגט וועג דיא האַרפֿע— ערשיינט).").
        if has_ersheynt:
            if has_action or len(tokens) > 3:
                return "entrance business"
            return "entrance"
        return None

    # Compound entrance: pure "<actor> ערשיינט" is 1-2 tokens; once we have
    # ≥3 surface tokens before/around the entrance cue, treat as compound.
    if has_ersheynt and (has_action or len(tokens) >= 4):
        return "entrance business"
    if has_entrance_cue and has_action:
        return "entrance business"
    if has_entrance_cue:
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


def load_non_speaker_labels(play: str) -> set[str]:
    """Skeletons of labels Noa marked 'part of the text' / 'do not tag as role'
    (cast_dict.json `non_speaker_labels`). resolve_line uses this to suppress the
    'untagged speaker (unknown)' flag for surface strings that look like a
    `label:` turn but are really running text (e.g. בראווא, געאנטווארטעט)."""
    f = REPO / "data" / play / "cast_dict.json"
    if not f.exists():
        return set()
    d = json.loads(f.read_text(encoding="utf-8"))
    return {skel(x) for x in (d.get("non_speaker_labels") or [])}


def load_speaker_overrides(play: str) -> dict[int, list[dict]]:
    """Per-scene speaker label overrides — for scenes where the same surface
    label maps to different cast members in different scenes (the classic
    case is the gendered duet pronouns ער/זיא, which on one scene mean
    Zelikel+Tsierele and on another mean Daniel+Blimele).

    File: data/<play>/speaker_overrides.json
    Schema: {"scenes": [
        {"pages": [13, 14], "labels": {"ער": "zelikel_mnagen", ...}},
        {"pages": [61], "lines": [3, 4, 5], "labels": {...}}   # optional `lines`
                                                                 # narrows to those line indices
    ]}

    Returns: {page_num -> list of scope-rules}. Each rule is a dict with keys:
      - `labels`: {label_skeleton -> xmlid}
      - `lines`: optional set of int line-indices; if absent, applies to all
        lines on the page.
    `resolve_line` consults the matching rule(s) for the current page+line.
    """
    f = REPO / "data" / play / "speaker_overrides.json"
    if not f.exists():
        return {}
    d = json.loads(f.read_text(encoding="utf-8"))
    out: dict[int, list[dict]] = {}
    for scene in d.get("scenes", []):
        labels = {skel(k): v for k, v in (scene.get("labels") or {}).items()}
        lines_field = scene.get("lines")
        lines = set(int(x) for x in lines_field) if lines_field else None
        for page in scene.get("pages") or []:
            try:
                p = int(page)
            except (TypeError, ValueError):
                continue
            out.setdefault(p, []).append({"labels": labels, "lines": lines})
    return out


def overrides_for(page_rules: list[dict] | None, line_idx: int | None) -> dict[str, str]:
    """Resolve a page's override rules to a flat {label -> xmlid} dict for
    one specific line index. A rule applies if either it has no `lines`
    field, or `line_idx` is in its `lines` set."""
    if not page_rules:
        return {}
    out: dict[str, str] = {}
    for rule in page_rules:
        if rule.get("lines") is not None and line_idx not in rule["lines"]:
            continue
        out.update(rule["labels"])
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


def resolve_line(text: str, entries, cast_index, cast_bares=None, page_overrides=None,
                 non_speakers=None, allow_speaker=True):
    """Return (new_entries, [auto_descriptions], [human_issues]).

    new_entries is None if nothing auto-changed. Operates on a single line's
    parsed custom entries; safe to run on the live transcript (idempotent).

    cast_bares is {xmlid -> multi-word bare form} used to re-anchor a speaker
    span the LLM truncated to the first token (P1, ben_kaspi case).

    allow_speaker=False suppresses the "untagged turn -> add speaker span" rule.
    Set it for castList/titlePage pages, where `ד"ר אברהם:` is a ROLE entry, not
    a speech turn. Until 2026-07-19 the page-type check filtered only the
    human-facing flags and left the auto-edit unfiltered, so every sweep
    re-added spurious speaker spans to castList pages.

    page_overrides is {label_skeleton -> xmlid} for THIS page, from
    `load_speaker_overrides(play)[page_num]`. Used for per-scene speaker
    label remapping (e.g. duet pronouns ער/זיא that mean different
    characters in different scenes). Wins over cast_index.
    """
    cast_bares = cast_bares or {}
    non_speakers = non_speakers or set()
    if page_overrides:
        cast_index = {**cast_index, **page_overrides}
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
            # 2c. accept valid LLM type — includes TEI multi-token types
            # (e.g. "entrance business", "exit business") per the
            # 2026-06-18 ratification. Use the full schema validator,
            # NOT a bare set-membership check which only handles single tokens.
            from annotation.schema import _validate_stage_type
            if _validate_stage_type(t) is None:
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
            # Fallback: speaker label includes a parenthesized stage cue
            # (e.g. `שמואל (לויפט)`). Strip parens and retry the lookup.
            if label and label not in cast_index and label not in _COLLECTIVE_XMLID \
                    and "(" in label:
                label = skel(re.sub(r"\s*\([^)]*\)\s*", " ", label))
            if label in cast_index:
                a = dict(a); a["xmlid"] = cast_index[label]
                auto.append(f"speaker xmlid:{cast_index[label]}")
            elif label in _COLLECTIVE_XMLID:
                # Collective speaker (קאר / מענער / דאמען / אלע …):
                # Noa-tagged speaker spans on body pages legitimately lack a
                # cast_dict entry; auto-fill with the canonical collective xmlid
                # (same mapping used by apply_collective_speakers.py).
                a = dict(a); a["xmlid"] = _COLLECTIVE_XMLID[label]
                auto.append(f"speaker xmlid:{_COLLECTIVE_XMLID[label]} (collective)")
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
    if allow_speaker and not any(t == "speaker" for t, _ in out):
        m = TURN_RE.match(text)
        if m:
            label = m.group(1); k = skel(label)
            if is_collective_label(label):
                pass  # handled by apply_collective_speakers.py
            elif k in cast_index:
                out.append(("speaker", {"offset": "0", "length": str(len(label)),
                                        "xmlid": cast_index[k]}))
                auto.append(f"+speaker xmlid:{cast_index[k]}")
            elif "(" in label or ")" in label:
                # Speaker label includes a parenthesized stage cue
                # (e.g. `שמואל (לויפט):`). Walk the label, skipping `(...)`
                # groups, and emit a speaker span scoped to the name run
                # whose nikud-stripped form is in cast_index. The paren
                # itself is left to the existing stage-tagging pass.
                lo = m.start(1); hi = m.end(1)
                prefix = text[lo:hi]
                placed = False
                i = 0; nP = len(prefix)
                while i < nP and not placed:
                    if prefix[i] == "(":
                        j = prefix.find(")", i + 1)
                        i = (j + 1) if j >= 0 else nP
                        continue
                    if prefix[i].isspace():
                        i += 1; continue
                    j = i
                    while j < nP and prefix[j] != "(":
                        j += 1
                    seg = prefix[i:j].rstrip()
                    if seg:
                        kk = skel(seg)
                        if kk in cast_index:
                            out.append(("speaker", {"offset": str(lo + i),
                                                    "length": str(len(seg)),
                                                    "xmlid": cast_index[kk]}))
                            auto.append(f"+speaker xmlid:{cast_index[kk]} "
                                        f"(stripped paren cue)")
                            placed = True
                        elif kk in _COLLECTIVE_XMLID:
                            out.append(("speaker", {"offset": str(lo + i),
                                                    "length": str(len(seg)),
                                                    "xmlid": _COLLECTIVE_XMLID[kk]}))
                            auto.append(f"+speaker xmlid:{_COLLECTIVE_XMLID[kk]} "
                                        f"(collective, stripped paren cue)")
                            placed = True
                    i = j
                # Standalone parenthesized cue as a "turn" — `(שרייט): …` — no
                # name outside the parens. Noa 2026-06-28: tag as a stage
                # direction, not a speaker. Emit a stage span over the paren
                # group and let the span-level lexicon type it (שרייט→delivery).
                # Only fires when the lexicon resolves a type, so unknown cues
                # still fall through to the human flag below.
                if not placed and re.sub(r"\([^)]*\)", "", prefix).strip() == "" \
                        and "(" in prefix:
                    po = prefix.find("("); pc = prefix.find(")", po + 1)
                    if pc > po:
                        cue = prefix[po:pc + 1]
                        stype = stage_lexicon_span(cue)
                        if stype:
                            out.append(("stage", {"offset": str(lo + po),
                                                  "length": str(pc - po + 1),
                                                  "type": stype}))
                            auto.append(f"+stage{{type:{stype}}} (standalone paren cue)")
                            placed = True
                if not placed and (not has_nikud(label)) and has_nikud(text[m.end():]) \
                        and k not in non_speakers:
                    human.append(f"untagged speaker (unknown) '{k}'")
            elif (not has_nikud(label)) and has_nikud(text[m.end():]) \
                    and k not in non_speakers:
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
    overrides_cache: dict[str, dict] = {}
    nonspk_cache: dict[str, set] = {}
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
        overrides = overrides_cache.setdefault(folder, load_speaker_overrides(folder))
        nonspk = nonspk_cache.setdefault(folder, load_non_speaker_labels(folder))
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
            _, _, human = resolve_line(line_text(tl), body, cast, bares,
                                        page_overrides=overrides_for(overrides.get(page), None),
                                        non_speakers=nonspk)
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


def apply_missing_headings(root) -> list[str]:
    """Tag act/scene heading lines that carry no `heading` span.

    Sinai 2026-07-20. The heading matcher only learned Roman numerals today
    (schema.parse_act_heading), and it runs at ANNOTATE time — so every page
    annotated before today still has its `I. אַקט` / `אַקט .II` lines untagged.
    Corpus audit before the fix: 26 tagged, 25 untagged.

    Only ADDS a heading span to a line that has none. A line already carrying
    `heading` is left alone, as is one carrying `trailer` — `ענדע פון דעם
    צווייטען אקט.` is an act-END and must never become an act heading.
    """
    changed = []
    for tl in root.iter(NS + "TextLine"):
        txt = line_text(tl)
        entries = parse_custom(tl.get("custom") or "")
        tags = {t for t, _ in entries}
        if "heading" in tags or "trailer" in tags:
            continue
        n = parse_act_heading(txt)
        kind, val = ("act", str(n)) if n else (None, None)
        if not kind:
            s = parse_scene_heading(txt)
            kind, val = ("scene", s) if s else (None, None)
        if not kind:
            continue
        entries.append(("heading", {"offset": "0", "length": str(len(txt.rstrip())),
                                    "type": kind, "n": val}))
        tl.set("custom", serialize_custom(entries))
        changed.append(f"heading{{type:{kind},n:{val}}} ← {txt.strip()[:32]!r}")
    return changed


_L_LEAD_RX = re.compile(r"^\s*[:׃־\-–—]?\s*")


def apply_l_speaker_scope(root) -> list[str]:
    """An `l` must cover the spoken text only, never the speaker label.

    Sinai 2026-07-20. In TEI `<speaker>` and `<l>` are siblings inside `<sp>`;
    the printed label is not part of the verse line. 119 `l` spans corpus-wide
    began at offset 0 on a line whose speaker label also starts there, so the
    span swallowed the label; 139 were already correct.

    The published TEI was never wrong — `build_tei.speaker_slice` re-splits the
    line by the SPEAKER span's length and puts only the remainder in `<l>`, so
    it ignores the `l` offsets for content. This fixes the annotation layer so
    the offsets mean what they say, for any consumer that reads the PAGE-XML
    directly rather than re-deriving.

    Precedent: §G.4/M6 already shrinks `l` to the sung tail after a voice
    rubric. Same rule, never applied to named speakers.

    Trims the same leading punctuation `speaker_slice` does, so the two agree.
    A line that is nothing but a label loses its `l` entirely.
    """
    changed = []
    for tl in root.iter(NS + "TextLine"):
        txt = line_text(tl)
        entries = parse_custom(tl.get("custom") or "")
        speakers = [a for t, a in entries if t == "speaker"]
        if not speakers or not any(t == "l" for t, _ in entries):
            continue
        out, hit = [], False
        for tag, a in entries:
            if tag != "l":
                out.append((tag, a)); continue
            try:
                lo, ln = int(a.get("offset", 0)), int(a.get("length", 0))
            except (TypeError, ValueError):
                out.append((tag, a)); continue
            end = lo + ln
            # widest speaker span overlapping this l
            ov = [(int(s.get("offset", 0)), int(s.get("length", 0))) for s in speakers]
            ov = [(so, sl) for so, sl in ov if lo < so + sl and end > so]
            if not ov:
                out.append((tag, a)); continue
            new_lo = max(so + sl for so, sl in ov)
            new_lo += len(_L_LEAD_RX.match(txt[new_lo:]).group(0))
            new_ln = end - new_lo
            if new_ln <= 0:
                changed.append(f"dropped l (label-only line) {txt.strip()[:30]!r}")
                hit = True
                continue
            a = dict(a); a["offset"] = str(new_lo); a["length"] = str(new_ln)
            out.append(("l", a)); hit = True
            changed.append(f"l({lo},{ln})→({new_lo},{new_ln}) {txt.strip()[:34]!r}")
        if hit:
            tl.set("custom", serialize_custom(out))
    return changed


_SPEAKER_TRAIL = ":׃־ .,"


def apply_speaker_colon_scope(root) -> list[str]:
    """S1: a speaker span covers the NAME only — never the trailing colon.

    Sinai 2026-07-20. 156 of 9,509 spans ended in a colon or other punctuation,
    concentrated in Das Yudishe Kind (44), Isha Raa (37) and Blimele (26). On
    Isha Raa p70 ten `קעניג:` spans were (0,6) — colon included — beside one at
    (0,5) without it, with no editorial logic behind the difference.

    Consequence is cosmetic but real: `build_tei.speaker_slice` puts
    `text[:length]` in `<speaker>`, so the published TEI carries a trailing
    colon on some speakers and not others. `<l>` content is unaffected either
    way, because the leading-colon strip on the remainder catches it — which is
    why this survived unnoticed, the same way the M9 `l` overlap did.
    """
    changed = []
    for tl in root.iter(NS + "TextLine"):
        txt = line_text(tl)
        entries = parse_custom(tl.get("custom") or "")
        out, hit = [], False
        for tag, a in entries:
            if tag != "speaker":
                out.append((tag, a)); continue
            try:
                lo, ln = int(a.get("offset", 0)), int(a.get("length", 0))
            except (TypeError, ValueError):
                out.append((tag, a)); continue
            end = lo + ln
            while end > lo and end <= len(txt) and txt[end - 1] in _SPEAKER_TRAIL:
                end -= 1
            if end == lo or end - lo == ln:      # nothing to trim, or all punct
                out.append((tag, a)); continue
            a = dict(a); a["length"] = str(end - lo)
            out.append(("speaker", a)); hit = True
            changed.append(f"speaker({lo},{ln})→({lo},{end - lo}) "
                           f"{txt[lo:end]!r} was {txt[lo:lo + ln]!r}")
        if hit:
            tl.set("custom", serialize_custom(out))
    return changed


def sweep_speaker_colon(only: str | None, dry_run: bool) -> int:
    """Corpus sweep for `apply_speaker_colon_scope`."""
    return _sweep(only, dry_run, apply_speaker_colon_scope,
                  f"YiDraCor speaker-span colon trim {_dt.date.today().isoformat()}",
                  "speaker spans trimmed", mirror_rx=re.compile(r"speaker\s*\{"))


def sweep_l_scope(only: str | None, dry_run: bool) -> int:
    """Corpus sweep for `apply_l_speaker_scope`."""
    return _sweep(only, dry_run, apply_l_speaker_scope,
                  f"YiDraCor l-span speaker scoping {_dt.date.today().isoformat()}",
                  "l spans rescoped", mirror_rx=re.compile(r"\bl\s*\{offset"))


def sweep_headings(only: str | None, dry_run: bool) -> int:
    """Corpus sweep for `apply_missing_headings` — same contract as sweep_openings."""
    return _sweep(only, dry_run, apply_missing_headings,
                  f"YiDraCor act-heading sweep {_dt.date.today().isoformat()}",
                  "heading spans added")


def sweep_openings(only: str | None, dry_run: bool) -> int:
    """Corpus sweep for `apply_opening_setting`, independent of the flag queue.

    The normal run only visits pages that raised a flag, and a `business` opener
    raises none — it is a perfectly well-formed span, just the wrong type. So
    the rule would otherwise reach an act-opening only by coincidence. This mode
    selects candidates from the local mirror (cheap) but edits the LIVE top
    transcript (correct), the same contract as the main path.
    """
    return _sweep(only, dry_run, apply_opening_setting,
                  f"YiDraCor act-opening setting sweep {_dt.date.today().isoformat()}",
                  "opening spans retyped", mirror_rx=_HEADING_RX)


def _sweep(only, dry_run, fn, note, unit, mirror_rx=None) -> int:
    """Shared harness: pick candidate pages from the mirror, edit the LIVE top.

    `mirror_rx` is a cheap pre-filter over the mirror's serialized XML; None
    means visit every page. The mirror decides only WHICH pages to look at —
    every edit is made against the live transcript, so a stale mirror can cost
    coverage but can never clobber live work.
    """
    editions, doc_ids = load_editions(), load_doc_ids()
    plays = [only] if only else sorted(
        p.name for p in (REPO / "data").iterdir() if (p / "page_annotated").is_dir())
    client = None
    n_edit = n_push = 0

    for play in plays:
        doc = doc_ids.get(play)
        if doc is None:
            continue
        pages = sorted({page for page, path in page_files(play)
                        if mirror_rx is None or mirror_rx.search(
                            etree.tostring(etree.parse(str(path)), encoding="unicode"))})
        if not pages:
            continue
        if client is None:
            from transkribus.client import TrpClient
            client = TrpClient.from_env()
        print(f"\n=== {editions.get(play, play)} (doc {doc}) — {len(pages)} candidate pages ===")
        for page in pages:
            tsid, owner, xml = top_transcript(client, doc, page)
            if xml is None:
                print(f"  p{page}: no server transcript — skip"); continue
            root = etree.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
            changes = fn(root)
            if not changes:
                continue
            n_edit += len(changes)
            for c in changes:
                print(f"  p{page}: {c}")
            if dry_run:
                n_push += 1; print(f"  p{page}: [dry-run] would push (parent {tsid}, top {owner})")
                continue
            client.push_transcript(COL, doc, page, etree.tostring(root, encoding="unicode"),
                                   parent_tsid=tsid, status="IN_PROGRESS", note=note,
                                   tool_name="YiDraCor-annotation-pipeline")
            n_push += 1; print(f"  p{page}: → pushed (parent {tsid})")

    print(f"\n{'DRY-RUN ' if dry_run else ''}SUMMARY: {n_edit} {unit} "
          f"on {n_push} pages {'to push' if dry_run else 'pushed'}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="restrict to one play folder")
    ap.add_argument("--out", help="trimmed needs-human CSV (default data/review/needs_human_<date>.csv)")
    ap.add_argument("--recheck", metavar="CSV",
                    help="re-validate an existing needs-human CSV against live Transkribus and rewrite it")
    ap.add_argument("--sweep-openings", action="store_true",
                    help="apply ONLY apply_opening_setting(), to every page carrying an "
                         "act/scene heading (not just flag-candidate pages), then exit")
    ap.add_argument("--sweep-headings", action="store_true",
                    help="tag act/scene headings that carry no heading span "
                         "(Roman numerals were only learned 2026-07-20), then exit")
    ap.add_argument("--sweep-l-scope", action="store_true",
                    help="shrink `l` spans that swallow the speaker label, then exit")
    ap.add_argument("--sweep-speaker-colon", action="store_true",
                    help="trim the trailing colon from speaker spans (S1), then exit")
    args = ap.parse_args()

    if args.sweep_openings:
        return sweep_openings(args.only, args.dry_run)

    if args.sweep_headings:
        return sweep_headings(args.only, args.dry_run)

    if args.sweep_l_scope:
        return sweep_l_scope(args.only, args.dry_run)

    if args.sweep_speaker_colon:
        return sweep_speaker_colon(args.only, args.dry_run)

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
        cast_index, _, _ = load_cast(play)
        cast_bares = load_cast_bares(play)
        speaker_overrides = load_speaker_overrides(play)
        non_speakers = load_non_speaker_labels(play)
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
                scan = resolve_line(txt, [e for e in entries if e[0] != "readingOrder"],
                                     cast_index, cast_bares,
                                     page_overrides=overrides_for(speaker_overrides.get(page), None),
                                     non_speakers=non_speakers,
                                     allow_speaker=ptype not in ("titlePage", "castList"))
                _, auto, human = scan
                # A Global-A defect produces no per-line auto edit, so without
                # this the page never becomes a candidate and the page-level
                # pass never runs (Al Naharot p.6).
                if ptype == "castList" and _is_global_a(txt) and not any(
                        t == "stage" and "setting" in (a.get("type") or "")
                        for t, a in entries):
                    candidate_pages.add(page)
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
            live_ptype = page_type(root)
            page_changed = False
            for c in apply_global_a(root):
                print(f"  p{page}: {c}")
                page_changed = True
            for tl in root.iter(NS + "TextLine"):
                entries = parse_custom(tl.get("custom") or "")
                ro = [e for e in entries if e[0] == "readingOrder"]
                body = [e for e in entries if e[0] != "readingOrder"]
                # Dedup before resolving — prior layered pushes from Transkribus
                # often carry duplicate (tag, offset, length) spans. Latest wins.
                body_dd = dedup_entries(body)
                deduped_count = len(body) - len(body_dd)
                new, auto, _ = resolve_line(line_text(tl), body_dd, cast_index, cast_bares,
                                              page_overrides=overrides_for(speaker_overrides.get(page), None),
                                              non_speakers=non_speakers,
                                              allow_speaker=live_ptype not in ("titlePage", "castList"))
                if new is not None or deduped_count:
                    final_entries = dedup_entries(ro + (new if new is not None else body_dd))
                    tl.set("custom", serialize_custom(final_entries))
                    page_changed = True
                    if new is not None:
                        n_auto += len(auto)
                        print(f"  p{page} {tl.get('id')}: " + ", ".join(auto))
                    if deduped_count:
                        print(f"  p{page} {tl.get('id')}: dropped {deduped_count} duplicate span(s)")
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
