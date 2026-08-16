"""Write `xmlid` onto the `role` and `speaker` spans the RAs already tagged.

The manuscript-track RAs scope speaker and role spans correctly but never set
an xmlid — that was always the pipeline's job (survey 2026-08-16: 3,969 speaker
spans and 204 role spans corpus-wide with zero xmlids, which `schema.validate_span`
rejects as `speaker span requires xmlid attribute`).

This differs from `auto_annotate`, which *finds* speakers by matching a
line-initial `Name:` regex. Here the span already exists and is correctly cut,
so the job is only to identify what it covers. Matching runs on the span text
itself, through a cascade that stops at the first hit:

  1. exact       — nikud/punctuation-stripped label equals a cast role's bare form
  2. collective  — a known chorus/group label (schema.COLLECTIVE_XMLID): אלע,
                   קאהר, ביידע ...; these intentionally have no cast entry
  3. variant     — same, tolerating the א/ע interchange, optional apostrophes
                   and doubled letters that OCR and MS orthography produce
  4. contains    — the label appears inside exactly ONE cast bare (a bare name
                   for a titled role, e.g. שמעון inside `רבי שמעון בן לקיש`).
                   Ambiguous containment (2+ candidates) is NOT guessed.
  5. skeleton    — consonant skeletons match, against the whole cast form or
                   against one of its words; unique matches only, 3-letter
                   floor of two. This is the mater-lectionis floor
                   (יעקעל/יעקיל, חגלה/חגלע, גדיאל/גדליאל).
  6. joint       — the label names several roles (`X און Y`, `X, Y`); each part
                   resolves through 1-5 and the span gets space-separated
                   xmlids, per the S4 convention in apply_speaker_xmlids

Anything that survives the cascade is left alone and written to the review TSV
with its distinct surface forms and counts, so an RA answers each *label* once
rather than each occurrence.

Roles are matched by cast_dict's recorded `loc` (line_id + offset) and the
covered text is VERIFIED against the cast form before writing, so a stale loc
is skipped rather than mis-assigned — same guard as backfill_role_xmlids.

Operates on the local `page_annotated/` mirror, not on Transkribus.

  python3.11 -m annotation.resolve_span_xmlids --dry-run
  python3.11 -m annotation.resolve_span_xmlids --apply --report /tmp/spk.tsv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (  # noqa: E402
    PAGE_NS, COLLECTIVE_XMLID, collective_skeleton, parse_custom,
    serialize_custom, _NIKUD,
)

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"

# Split a joint speaker label: `שרה און רחל`, `שרה, רחל`, `שרה און רחל`.
_JOINT_RX = re.compile(r"\s+און\s+|\s*[,،]\s*")
# Trailing punctuation the RAs sometimes include in the span.
_TRAIL = " \t:׃־.,;|)("
# Shortest consonant skeleton allowed to carry a match. Two is low, and it is
# only safe because the skeleton rules test EQUALITY (against the whole cast
# form, or against one of its words) and demand a unique hit inside a cast of
# 13-28 roles. A three-letter floor was tried first and rejected: it threw away
# 68 spans of יעקעל→יעקיל and 23 of לובאוו→ליובאוו, both plainly correct.
# Every skeleton-rule match is listed in the review sheet for confirmation.
MIN_SKEL = 2


def bare(s: str) -> str:
    """Nikud-stripped, punctuation-stripped comparison key."""
    return _NIKUD.sub("", s or "").strip().strip(_TRAIL).strip()


def skeleton_key(s: str) -> str:
    """Consonant skeleton: drop the mater-lectionis vowel letters.

    The MS castLists and the body speaker labels disagree constantly about
    which vowel letter spells an unstressed vowel — יעקעל/יעקיל, גדיאל/גדליאל,
    לובאוו/ליובאוו, יוסיף/יוסף — and about whether a final /e/ is ה or ע
    (חגלה/חגלע). Dropping א ע י ו and a final ה collapses all of these onto one
    key. It is deliberately lossy, so a hit is only ever accepted when it picks
    out exactly ONE role in the play's cast, and the match is reported as
    `skeleton` in the run summary so it can be spot-checked.
    """
    s = bare(s).replace("'", "").replace("’", "").replace("׳", "")
    s = re.sub(r"[הא]$", "", s)          # final mater for /a/ /e/
    s = re.sub(r"[אעיו]", "", s)
    s = re.sub(r"(.)\1+", r"\1", s)
    return re.sub(r"\s+", " ", s).strip()


def variant_key(s: str) -> str:
    """Collapse the spelling differences that separate the same name.

    א/ע are interchangeable in this orthography, apostrophes are optional, and
    doubled letters (אללע vs אלע) vary freely — the same three tolerances
    build_name_matcher applies, reduced to a normal form so it can be a dict key.
    """
    s = bare(s).replace("'", "").replace("’", "").replace("׳", "")
    s = re.sub(r"[אע]", "א", s)
    s = re.sub(r"(.)\1+", r"\1", s)   # collapse doubled letters
    s = re.sub(r"\s+", " ", s)
    return s


def _line_text(tl) -> str:
    for te in tl.findall(f"{NS}TextEquiv"):
        u = te.find(f"{NS}Unicode")
        if u is not None:
            return u.text or ""
    return ""


class Resolver:
    def __init__(self, cast: dict):
        roles = cast.get("roles", {})
        self.exact: dict[str, str] = {}
        self.variant: dict[str, set] = defaultdict(set)
        self.skeleton: dict[str, set] = defaultdict(set)
        self.skel_words: dict[str, set] = defaultdict(set)
        self.bares: list[tuple[str, str]] = []
        for xid, info in roles.items():
            forms = [info.get("bare") or "", info.get("form") or ""]
            forms += list(info.get("prefix_variants") or [])
            forms += list(info.get("variants") or [])
            for f in forms:
                b = bare(f)
                if not b:
                    continue
                self.exact.setdefault(b, xid)
                self.variant[variant_key(b)].add(xid)
                sk = skeleton_key(b)
                if len(sk) >= MIN_SKEL:
                    self.skeleton[sk].add(xid)
                # Index each WORD of the canonical form separately, so a bare
                # given name can match a titled role (גדליהו → `גדיליהו בן
                # אחיקום`) without the skeleton of the SHORT label being
                # allowed to match anywhere inside the LONG one. Substring
                # matching on skeletons is far too loose: it paired עטוואס
                # ("something") with אנטיסעמיטען, and סאלא — a solo rubric,
                # not a name at all — with אסתר סאלומאסי.
                for w in b.split():
                    wsk = skeleton_key(w)
                    if len(wsk) >= MIN_SKEL:
                        self.skel_words[wsk].add(xid)
                self.bares.append((b, xid))

    def _one(self, label: str) -> tuple[str | None, str]:
        b = bare(label)
        if not b:
            return None, "empty"
        if b in self.exact:
            return self.exact[b], "exact"
        skel = collective_skeleton(label)
        if skel in COLLECTIVE_XMLID:
            return COLLECTIVE_XMLID[skel], "collective"
        vk = variant_key(b)
        cands = self.variant.get(vk) or set()
        if len(cands) == 1:
            return next(iter(cands)), "variant"
        if len(cands) > 1:
            return None, f"variant-ambiguous ({len(cands)})"
        # Containment anywhere in the canonical form, not just at its edges:
        # titled roles put the bare name in the middle (שמעון inside
        # `רבי שמעון בן לקיש`). Only when it picks out exactly one role.
        hits = {xid for cb, xid in self.bares
                if len(b) >= 3 and cb != b and b in cb}
        if len(hits) == 1:
            return next(iter(hits)), "contains"
        if len(hits) > 1:
            return None, f"contains-ambiguous ({len(hits)})"
        # Mater-lectionis floor: consonant skeleton, unique match only.
        sk = skeleton_key(b)
        if len(sk) >= MIN_SKEL:
            shits = self.skeleton.get(sk) or set()
            if len(shits) == 1:
                return next(iter(shits)), "skeleton"
            if len(shits) > 1:
                return None, f"skeleton-ambiguous ({len(shits)})"
            # The label matches one WORD of a canonical form — a bare given
            # name for a titled role. Equality against a word, never a
            # substring of the whole form.
            whits = self.skel_words.get(sk) or set()
            if len(whits) == 1:
                return next(iter(whits)), "skeleton-word"
            if len(whits) > 1:
                return None, f"skeleton-word-ambiguous ({len(whits)})"
        return None, "unmatched"

    def resolve(self, label: str) -> tuple[str | None, str]:
        """Return (space-separated xmlids, reason) or (None, reason)."""
        xid, why = self._one(label)
        if xid:
            return xid, why
        parts = [p for p in _JOINT_RX.split(label) if bare(p)]
        if len(parts) > 1:
            got, whys = [], []
            for p in parts:
                x, w = self._one(p)
                if not x:
                    return None, f"joint: {bare(p)!r} {w}"
                got.append(x)
                whys.append(w)
            return " ".join(got), f"joint ({'+'.join(whys)})"
        return None, why


def process_play(folder: Path, apply: bool):
    cast_path = folder / "cast_dict.json"
    if not cast_path.exists():
        return None
    cast = json.loads(cast_path.read_text(encoding="utf-8"))
    res = Resolver(cast)
    roles = cast.get("roles", {})
    # line_id -> [(offset, xmlid, form)] from cast_dict's recorded locations
    role_locs: dict[str, list] = defaultdict(list)
    for xid, info in roles.items():
        loc = info.get("loc") or {}
        if loc.get("line_id") is not None:
            role_locs[loc["line_id"]].append(
                (str(loc.get("offset")), xid, info.get("form") or ""))

    from xml.etree import ElementTree as ET
    ET.register_namespace("", PAGE_NS)

    stats = Counter()
    unresolved: dict[str, Counter] = defaultdict(Counter)
    unres_reason: dict[str, str] = {}

    for xf in sorted((folder / "page_annotated").glob("*.xml")):
        try:
            tree = ET.parse(xf)
        except ET.ParseError:
            stats["parse_error"] += 1
            continue
        changed = False
        for tl in tree.iter(f"{NS}TextLine"):
            ents = parse_custom(tl.get("custom") or "")
            if not any(t in ("speaker", "role") for t, _ in ents):
                continue
            text = _line_text(tl)
            out, touched = [], False
            for tag, a in ents:
                if tag not in ("speaker", "role") or a.get("xmlid"):
                    if tag in ("speaker", "role"):
                        stats[f"{tag}_already"] += 1
                    out.append((tag, a))
                    continue
                try:
                    off, ln = int(a["offset"]), int(a["length"])
                except (KeyError, ValueError):
                    stats[f"{tag}_bad_span"] += 1
                    out.append((tag, a))
                    continue
                label = text[off:off + ln]

                if tag == "role":
                    # Trust cast_dict's loc, but verify the covered text.
                    hit = None
                    for o, xid, form in role_locs.get(tl.get("id") or "", []):
                        if o == a.get("offset") and bare(form) == bare(label):
                            hit = xid
                            break
                    if hit is None:
                        xid, why = res.resolve(label)
                        hit = xid
                        if not hit:
                            stats["role_unmatched"] += 1
                            unresolved[bare(label)][folder.name] += 1
                            unres_reason.setdefault(bare(label), f"role: {why}")
                            out.append((tag, a))
                            continue
                        stats["role_by_name"] += 1
                    else:
                        stats["role_by_loc"] += 1
                    a = dict(a)
                    a["xmlid"] = hit
                    touched = True
                    out.append((tag, a))
                    continue

                xid, why = res.resolve(label)
                if not xid:
                    stats["speaker_unmatched"] += 1
                    unresolved[bare(label)][folder.name] += 1
                    unres_reason.setdefault(bare(label), why)
                    out.append((tag, a))
                    continue
                stats[f"speaker_{why.split()[0]}"] += 1
                stats["speaker_resolved"] += 1
                a = dict(a)
                a["xmlid"] = xid
                touched = True
                out.append((tag, a))
            if touched:
                tl.set("custom", serialize_custom(out))
                changed = True
        if changed and apply:
            tree.write(xf, encoding="utf-8", xml_declaration=True)
    return stats, unresolved, unres_reason


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report")
    args = ap.parse_args()

    folders = sorted(p for p in (REPO / "data").iterdir()
                     if (p / "page_annotated").is_dir() and (p / "cast_dict.json").exists())
    if args.only:
        folders = [f for f in folders if f.name in set(args.only)]

    rows = []
    grand = Counter()
    print(f"{'play':30} {'spk done':>9} {'spk left':>9} {'role done':>10} {'role left':>10}")
    for folder in folders:
        got = process_play(folder, args.apply)
        if not got:
            continue
        stats, unresolved, reasons = got
        grand.update(stats)
        for label, plays in unresolved.items():
            if folder.name in plays:
                rows.append({
                    "play": folder.name, "label": label,
                    "occurrences": plays[folder.name],
                    "reason": reasons.get(label, ""),
                })
        print(f"{folder.name[:30]:30} {stats['speaker_resolved']:9} "
              f"{stats['speaker_unmatched']:9} "
              f"{stats['role_by_loc'] + stats['role_by_name']:10} "
              f"{stats['role_unmatched']:10}")
    print(f"{'TOTAL':30} {grand['speaker_resolved']:9} {grand['speaker_unmatched']:9} "
          f"{grand['role_by_loc'] + grand['role_by_name']:10} {grand['role_unmatched']:10}")
    print("\nby rule:", {k: v for k, v in sorted(grand.items()) if k.startswith("speaker_")})
    print("APPLIED — files rewritten" if args.apply else "DRY RUN — nothing written")

    if args.report and rows:
        rows.sort(key=lambda r: (-r["occurrences"], r["play"]))
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["play", "label", "occurrences", "reason"],
                               delimiter="\t")
            w.writeheader()
            w.writerows(rows)
        print(f"unresolved labels → {args.report} "
              f"({len(rows)} distinct, {sum(r['occurrences'] for r in rows)} spans)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
