"""Suggest cast_id mappings for unmatched speaker prefixes.

For each play:
  1. Scan all body pages for "Name:" prefixes that the name-matcher misses
  2. For each unmatched prefix, find the best-fitting cast_id by substring
     containment (prefix is suffix or substring of canonical bare).
  3. Emit a JSON suggestion file the user can review and edit.

After review, the cast_dict.json `roles[xmlid]` entries get an extra
`prefix_variants: [...]` list, which build_name_matcher will pick up.
"""
from __future__ import annotations
import json, re, sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.auto_annotate import build_name_matcher, find_speaker, strip_nikud
from annotation.annotate_pages import list_pages, dump_lines, REPO_ROOT


SPEAKER_RX = re.compile(r"^\s*([א-ת][֐-ת ⸝]{0,25}?[א-ת])\s*[:׃]")
NON_ROLE_MARKERS = {
    "אלע", "ביידע", "פערזאנען", "סאלא", "סאלא אלט",
    "טענאר", "אלט", "באס", "רעפריין", "סאפראן",
    "קאהר",  # chorus — depending on edition, may want its own xml:id
}


def best_match(bare: str, cast_bares: dict[str, str]) -> tuple[str, str] | None:
    """Return (xmlid, reason) or None."""
    bare = bare.strip()
    if bare in NON_ROLE_MARKERS:
        return ("__NON_ROLE__", f"common non-role marker")
    for xmlid, cb in cast_bares.items():
        if bare == cb:
            return (xmlid, "exact match (matcher bug?)")
    # suffix / prefix in canonical bare
    for xmlid, cb in cast_bares.items():
        if bare and (cb.endswith(bare) or cb.startswith(bare)) and len(bare) >= 3:
            return (xmlid, f"substring of {cb!r}")
    # char-overlap heuristic
    best = None; best_score = 0
    for xmlid, cb in cast_bares.items():
        if not cb: continue
        # ratio of bare chars present in cb
        common = sum(1 for c in bare if c in cb)
        score = common / max(len(bare), len(cb))
        if score > best_score:
            best, best_score = (xmlid, f"~{int(score*100)}% char overlap with {cb!r}"), score
    if best and best_score >= 0.5:
        return best
    return None


def scan_play(play: str) -> dict:
    cast_path = REPO_ROOT / "data" / play / "cast_dict.json"
    d = json.loads(cast_path.read_text())
    cast_bares = {xid: info["bare"] for xid, info in d["roles"].items()}
    matchers = build_name_matcher(cast_path)

    unmatched = Counter()
    for n, src in list_pages(play):
        try:
            payload = dump_lines(play, n)
        except Exception:
            continue
        for ln in payload["lines"]:
            m = SPEAKER_RX.match(ln["text"])
            if not m: continue
            if find_speaker(ln["text"], matchers): continue
            bare = strip_nikud(m.group(1)).strip().replace("'", "")
            unmatched[bare] += 1

    suggestions = []
    for prefix, count in unmatched.most_common():
        if count < 2 and len(prefix) < 4:
            continue  # too noisy
        match = best_match(prefix, cast_bares)
        suggestions.append({
            "prefix": prefix,
            "count": count,
            "suggested_xmlid": match[0] if match else None,
            "reason": match[1] if match else "no match — possibly a missing role",
        })
    return {"play": play, "cast_bares": cast_bares, "suggestions": suggestions}


def main():
    plays = [
        "Yudale_der_blinder,_Emkroyt1908",
        "Di_seyder_nakht_Emkroyt_1908",
        "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete",
    ]
    for p in plays:
        out = scan_play(p)
        path = REPO_ROOT / "data" / p / "speaker_variants_suggested.json"
        path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
        print(f"{p}: {len(out['suggestions'])} suggestions → {path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
