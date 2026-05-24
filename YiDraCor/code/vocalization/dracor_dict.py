"""
Build a vocabulary dictionary from DraCor TEI files, with filters that drop
known editor-specific quirks and obvious typos:

  • word-final ך/ם/ן/ף/ץ sheva-nakh    (we don't adopt this convention)
  • double-dagesh on a single letter    (Yudale TEI typo: נוּּר)
  • U+05C2 sin-dot on a vav            (DerMann editor's holam encoding mistake)
  • rafe (U+05BF) on any letter        (stripped unless edition_uses_rafe=True;
                                        DerMann marks soft כ/פ with rafe)

Speakers and stage directions are excluded.

Also exposes `merge_token` for per-letter merging: keep pipeline marks where
present, adopt DraCor marks where pipeline has none, flag contradiction.
"""

import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
NIK = re.compile(r"[֑-ׇ]")
TOKEN = re.compile(r"[֐-׿]+")

SHEVA = "ְ"
DAGESH = "ּ"
SHIN_DOT = "ׁ"
SIN_DOT = "ׂ"
HOLAM = "ֹ"
RAFE = "ֿ"
FINAL_LETTERS = set("ךםןףץ")
VOWELS_PLUS_SHEVA = set("ְֱֲֳִֵֶַ"
                        "ָׇֹֺֻ")


# Real syllable vowels (excluding sheva). Sheva is a syllable-boundary marker,
# not a nucleus, so the merge treats it as additive: DraCor adding sheva where
# we don't is informative, not a placement conflict.
SYLLABLE_VOWELS = VOWELS_PLUS_SHEVA - {SHEVA}

def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def parse(tok: str):
    out = []
    for ch in tok:
        if "א" <= ch <= "ת":
            out.append([ch, []])
        elif "֑" <= ch <= "ׇ" and out:
            out[-1][1].append(ch)
        else:
            out.append([ch, []])
    return out


def reassemble(letters) -> str:
    return "".join(l + "".join(m) for l, m in letters)


def apply_dracor_filters(tok: str, *, edition_uses_rafe: bool = False) -> str:
    """Drop editor-quirk marks before importing a DraCor token.

    edition_uses_rafe: when True, preserve U+05BF rafe on soft כ/פ (DerMann
    convention). Default False keeps Yudale-style behavior.
    """
    letters = parse(tok)
    n = len(letters)
    for i, (ltr, marks) in enumerate(letters):
        if not edition_uses_rafe:
            marks[:] = [m for m in marks if m != RAFE]
        # Sin-dot on vav: encoding mistake for holam. Convert.
        if ltr == "ו":  # vav
            marks[:] = [HOLAM if m == SIN_DOT else m for m in marks]
        # Double-dagesh: dedupe (keep one)
        if marks.count(DAGESH) > 1:
            marks[:] = [m for m in marks if m != DAGESH] + [DAGESH]
        # Drop sheva from word-final ך/ם/ן/ף/ץ
        if i == n - 1 and ltr in FINAL_LETTERS:
            marks[:] = [m for m in marks if m != SHEVA]
    return reassemble(letters)


def _iter_text_skip_speakers_stages(el):
    skip = {f"{{{TEI_NS}}}speaker", f"{{{TEI_NS}}}stage"}
    def walk(node):
        if node.tag in skip:
            if node.tail:
                yield node.tail
            return
        if node.text:
            yield node.text
        for child in node:
            yield from walk(child)
        if node.tail:
            yield node.tail
    yield from walk(el)


def tei_to_dict(tei_paths, *, edition_uses_rafe: bool = False) -> dict[str, str]:
    """Build a {bare → most-common-filtered-vocalized} dict from one or more
    DraCor TEI files. Drops speakers/stages; applies all editor-quirk filters.
    Bare-only tokens don't enter the dict.

    tei_paths: iterable of either Path/str (uses the default rafe behavior) or
    (path, edition_uses_rafe) tuples for per-edition control. The keyword
    edition_uses_rafe sets the default applied to plain-path entries.
    """
    counts: dict[str, Counter] = defaultdict(Counter)
    for entry in tei_paths:
        if isinstance(entry, tuple):
            path, uses_rafe = entry
        else:
            path, uses_rafe = entry, edition_uses_rafe
        data = Path(path).read_bytes()
        if not data.lstrip().startswith(b"<"):
            data = data[data.find(b"<"):]
        root = etree.fromstring(data)
        body = root.find(f".//{{{TEI_NS}}}body")
        if body is None:
            body = root
        text = "".join(_iter_text_skip_speakers_stages(body))
        for tok in TOKEN.findall(nfc(text)):
            filtered = apply_dracor_filters(tok, edition_uses_rafe=uses_rafe)
            key = NIK.sub("", filtered)
            if key == filtered:  # ended up bare after filtering
                continue
            counts[key][filtered] += 1
    return {k: c.most_common(1)[0][0] for k, c in counts.items()}


def _is_known_category_conflict(p, d, p_v, d_v) -> bool:
    """Classify a syllable-vowel conflict against the four documented policy
    splits (see [[vocalization-dracor-overlay]] memory):
      CAT1 וו digraph vowel placement
      CAT2 vowel on consonant vs on the matres aleph
      CAT3 vowel quality at a shared letter (same skeleton)
      CAT4 holam-on-vav for /oy/

    If a conflict fits one of those, the caller treats it as silent
    (no <unclear> tag) — those are RA-confirmed convention choices, not
    errors. Returns True iff the conflict fits CAT1/2/3/4.
    """
    bare = "".join(l for l, _ in p)
    same_skel = [bool(p_v[i]) == bool(d_v[i]) for i in range(len(p))]

    # CAT3: same syllable-vowel skeleton, vowel quality differs at some letter.
    if all(same_skel):
        return True

    diffs = [i for i in range(len(p)) if not same_skel[i]]
    diff_letters = {p[i][0] for i in diffs}

    if "וו" in bare:                          # CAT1
        return True
    if "א" in diff_letters:                    # CAT2
        return True
    for i in diffs:                            # CAT4
        if p[i][0] == "ו" and HOLAM in d[i][1]:
            return True
    return False


def overlay_tree(tree, dracor_dict, page_ns: str) -> tuple[int, int, list[str]]:
    """Apply the DraCor dict overlay to every line-level <Unicode> in `tree`,
    in place. Skips speaker prefixes and bracketed stage directions.

    Returns (filled_count, conflict_count, conflict_tokens) — conflict_tokens
    is deduped; pass to add_unclear_annotations.
    """
    from rules import WORD_RE, speaker_span, bracket_spans, NIKKUD_RE
    line_tag = f"{{{page_ns}}}TextLine"
    filled = conflicts = 0
    conflict_toks: list[str] = []
    seen_conflicts: set[str] = set()

    for tl in tree.iter(line_tag):
        for te in tl.findall(f"{{{page_ns}}}TextEquiv"):
            u = te.find(f"{{{page_ns}}}Unicode")
            if u is None or not u.text:
                continue
            line = u.text
            sp = speaker_span(line)
            brackets = bracket_spans(line)

            def replace(m):
                nonlocal filled, conflicts
                tok = m.group(0)
                s, e = m.span()
                if sp and s >= sp[0] and e <= sp[1]:
                    return tok
                if any(bs <= s and e <= be for bs, be in brackets):
                    return tok
                key = NIKKUD_RE.sub("", tok)
                d = dracor_dict.get(key)
                if not d:
                    return tok
                merged, conflict = merge_token(tok, d)
                if conflict:
                    conflicts += 1
                    if tok not in seen_conflicts:
                        seen_conflicts.add(tok)
                        conflict_toks.append(tok)
                    return tok
                if merged != tok:
                    filled += 1
                return merged

            u.text = WORD_RE.sub(replace, line)
            break

    return filled, conflicts, conflict_toks


def merge_token(pipeline_tok: str, dracor_tok: str) -> tuple[str, bool]:
    """Per-letter merge with placement-conflict detection.

    Returns (merged_token, conflict). `conflict=True` means the two sides
    disagree on either (a) which letter carries the vowel (placement) or
    (b) the vowel quality at a shared letter. In that case we keep the
    pipeline token unchanged and the caller tags it <unclear>.

    DraCor strictly adding marks (filling letters pipeline left bare without
    moving any existing vowel) is NOT a conflict — those marks are adopted.
    DraCor lacking marks pipeline has (e.g. final-ך sheva) is NOT a conflict
    either — pipeline's extras are kept.
    """
    if NIK.sub("", pipeline_tok) != NIK.sub("", dracor_tok):
        return pipeline_tok, False
    p = parse(pipeline_tok)
    d = parse(dracor_tok)
    if len(p) != len(d):
        return pipeline_tok, False

    # Conflict detection runs on syllable vowels only (not sheva), so DraCor's
    # heavier cluster-sheva marking is treated as additive, not as relocation.
    p_v = [[m for m in marks if m in SYLLABLE_VOWELS] for _, marks in p]
    d_v = [[m for m in marks if m in SYLLABLE_VOWELS] for _, marks in d]

    is_conflict = False
    for i in range(len(p)):
        if p_v[i] and d_v[i] and set(p_v[i]) != set(d_v[i]):
            is_conflict = True
            break
        if d_v[i] and not p_v[i]:
            if sum(map(bool, p_v)) and sum(map(bool, d_v)) <= sum(map(bool, p_v)):
                is_conflict = True
                break

    # Auto-resolve conflicts in the four documented policy categories: keep
    # pipeline form silently (no <unclear> tag) since the RA has decided
    # those are conventions, not errors. Only novel disagreements get flagged.
    flag = is_conflict and not _is_known_category_conflict(p, d, p_v, d_v)

    merged = []
    for i, ((pl, pm), (dl, dm)) in enumerate(zip(p, d)):
        if is_conflict:
            # Keep pipeline syllable vowels; still adopt non-syllable marks
            # (sheva, dagesh, shin-dot, sin-dot) DraCor offers that we lack.
            extras = [m for m in dm if m not in pm and m not in SYLLABLE_VOWELS]
            merged.append((pl, pm + extras))
        else:
            if p_v[i]:
                extras = [m for m in dm if m not in pm]
                merged.append((pl, pm + extras))
            elif d_v[i]:
                extras = [m for m in pm if m not in dm]
                merged.append((pl, list(dm) + extras))
            else:
                extras = [m for m in dm if m not in pm]
                merged.append((pl, pm + extras))
    return reassemble(merged), flag
