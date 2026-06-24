"""Harvest the vocalized cast forms from an annotated castList PAGE-XML page.

Outputs a per-edition JSON sidecar that downstream stages (the vocalizer, the
DraCor overlay) can load to override bare-letter filling for the play's
character names and the words that appear in their role descriptions.

Output schema:
{
  "play": "<edition folder name>",
  "source_pages": [...],
  "roles": {
    "<xmlid>": {
      "form":     "יוּדַאלֶע",     # vocalized name as printed in the cast list
      "bare":     "יודאלע",          # niqqud-stripped key
      "lines": [ {"page": 4, "line_id": "...", "offset": 0, "length": 7} ]
    },
    ...
  },
  "desc_tokens": {
    "<bare-token>": "<vocalized-token>",
    ...
  }
}

`form` is the source of truth for inline mentions of the character in body
pages — when the vocalizer sees a bare occurrence of the name, it should prefer
this form over rules+dict guesses.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from lxml import etree

from annotation.schema import PAGE_NS, parse_custom

LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"

COMBINING = set(chr(c) for c in range(0x0591, 0x05C8))
TOKEN_RE = re.compile(r"[֐-׿']+")


def strip_niqqud(s: str) -> str:
    return "".join(c for c in s if c not in COMBINING)


def has_niqqud(s: str) -> bool:
    return any(c in COMBINING for c in s)


def _line_unicode(line_el) -> str:
    for u in line_el.iter(UNICODE_TAG):
        if u.getparent().getparent() is line_el:
            return u.text or ""
    return ""


def _page_num_from_filename(name: str) -> int | None:
    m = re.match(r"^(\d+)_", name)
    return int(m.group(1)) if m else None


_TRANSLIT_DIGRAPHS = [
    ("וו", "v"), ("וי", "oy"), ("ױ", "oy"), ("ײ", "ey"), ("יי", "ey"),
    ("אַ", "a"), ("אָ", "o"), ("בּ", "b"), ("בֿ", "v"), ("כּ", "k"),
    ("פּ", "p"), ("פֿ", "f"), ("שׂ", "s"), ("תּ", "t"), ("וּ", "u"), ("יִ", "i"),
]
_TRANSLIT_SINGLE = {
    "א": "", "ב": "b", "ג": "g", "ד": "d", "ה": "h", "ו": "u", "ז": "z",
    "ח": "kh", "ט": "t", "י": "i", "כ": "kh", "ך": "kh", "ל": "l", "מ": "m",
    "ם": "m", "נ": "n", "ן": "n", "ס": "s", "ע": "e", "פ": "f", "ף": "f",
    "צ": "ts", "ץ": "ts", "ק": "k", "ר": "r", "ש": "sh", "ת": "s",
}


def _auto_xmlid(text: str) -> str:
    """YIVO-ish translit → safe xmlid token (a-z, 0-9, underscore)."""
    if not text:
        return ""
    s = text.strip().rstrip(",.;:'\"")
    s = s.replace("'", "").replace("’", "").replace("‘", "")
    s = re.sub(r"[֑-ׇ]", "", s)  # strip nikud
    for src, dst in _TRANSLIT_DIGRAPHS:
        s = s.replace(src, dst)
    out = "".join(_TRANSLIT_SINGLE.get(ch, ch) for ch in s)
    out = re.sub(r"\s+", "_", out.strip())
    out = re.sub(r"[^a-z0-9_]", "", out.lower())
    out = re.sub(r"_+", "_", out).strip("_")
    return out


def extract_one(path: Path) -> tuple[dict, dict]:
    """Return (roles_for_page, desc_tokens_for_page).

    roles_for_page: {xmlid: {"form": ..., "bare": ..., "loc": {...}}}
    desc_tokens_for_page: {bare: Counter({vocalized: count})}
    """
    tree = etree.parse(str(path))
    page_num = _page_num_from_filename(path.name)

    roles: dict = {}
    desc_tokens: dict[str, Counter] = defaultdict(Counter)

    for line in tree.iter(LINE_TAG):
        text = _line_unicode(line)
        custom = line.get("custom") or ""
        spans = parse_custom(custom)
        for tag, attrs in spans:
            if tag not in {"role", "roleDesc"}:
                continue
            try:
                off = int(attrs["offset"])
                ln = int(attrs["length"])
            except (KeyError, ValueError):
                continue
            sub = text[off:off + ln]
            if not sub.strip():
                continue
            if tag == "role":
                xid = attrs.get("xmlid")
                if not xid:
                    # Auto-assign xmlid by transliterating bare consonant skeleton
                    # (Noa marks role spans on Transkribus without xmlids; this
                    # turns them into a usable cast_dict.json — 2026-06-24).
                    bare = strip_niqqud(sub).strip()
                    xid = _auto_xmlid(bare)
                    if not xid:
                        continue
                    # ensure uniqueness inside the page
                    base = xid; n = 2
                    while xid in roles:
                        xid = f"{base}_{n}"; n += 1
                roles[xid] = {
                    "form": sub,
                    "bare": strip_niqqud(sub),
                    "loc": {"page": page_num, "line_id": line.get("id"),
                            "offset": off, "length": ln},
                }
            elif tag == "roleDesc":
                for tok in TOKEN_RE.findall(sub):
                    bare = strip_niqqud(tok)
                    if bare and has_niqqud(tok):
                        desc_tokens[bare][tok] += 1

    return roles, desc_tokens


def extract_cast(paths: list[Path], *, play: str | None = None) -> dict:
    all_roles: dict = {}
    all_desc: dict[str, Counter] = defaultdict(Counter)
    source_pages = []

    for p in paths:
        roles, desc = extract_one(p)
        source_pages.append({"file": p.name,
                             "page": _page_num_from_filename(p.name),
                             "roles": len(roles)})
        for xid, info in roles.items():
            existing = all_roles.get(xid)
            if existing is None:
                all_roles[xid] = info
            elif existing["form"] != info["form"]:
                existing.setdefault("variants", []).append(info["form"])
        for bare, ctr in desc.items():
            all_desc[bare].update(ctr)

    desc_out = {bare: ctr.most_common(1)[0][0] for bare, ctr in all_desc.items()}

    return {
        "play": play,
        "source_pages": source_pages,
        "roles": all_roles,
        "desc_tokens": desc_out,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--play", help="edition folder name (just for the output 'play' field)")
    ap.add_argument("--in", dest="inputs", nargs="+", required=True,
                    help="one or more annotated castList PAGE-XML files")
    ap.add_argument("--out", required=True, help="output JSON path")
    args = ap.parse_args(argv)

    paths = [Path(p) for p in args.inputs]
    result = extract_cast(paths, play=args.play)
    Path(args.out).write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    n_roles = len(result["roles"])
    n_desc = len(result["desc_tokens"])
    print(f"Wrote {n_roles} roles + {n_desc} desc-tokens to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
