"""Cross-check @who / particDesc consistency in a generated TEI edition.

DraCor's Schematron makes three demands we verify here:
  1. Every person / personGrp in particDesc SHOULD be referenced by at least
     one @who in the body (unreferenced entries are usually non-speaking cast,
     but may also be dead xml:ids or typos).
  2. Every <sp> SHOULD carry a @who (a speaker with no @who cannot enter the
     character network — typically an un-resolved song voice).
  3. Every @who token MUST resolve to a person / personGrp in particDesc.

Usage:
    python3.11 -m structure.check_who tei/Di-Seder-Nakht.xml [more.xml ...]
    python3.11 -m structure.check_who            # all tei/*.xml built by us
"""
from __future__ import annotations

import sys
from pathlib import Path

from lxml import etree

TEI = "http://www.tei-c.org/ns/1.0"
REPO_ROOT = Path(__file__).resolve().parents[2]


def q(tag: str) -> str:
    return f"{{{TEI}}}{tag}"


def who_tokens(val: str) -> list[str]:
    """Split a @who value into bare xml:ids ('#a #b' -> ['a', 'b'])."""
    return [t.lstrip("#") for t in (val or "").split() if t.strip()]


def check_file(path: Path) -> int:
    tree = etree.parse(str(path))
    root = tree.getroot()

    # particDesc master list: person + personGrp xml:ids
    declared: dict[str, str] = {}
    for tag in ("person", "personGrp"):
        for el in root.iter(q(tag)):
            xid = el.get(f"{{http://www.w3.org/XML/1998/namespace}}id")
            if xid:
                declared[xid] = tag

    # every @who token used in the body (sp/stage/anything)
    used: set[str] = set()
    sp_no_who: list[str] = []
    for sp in root.iter(q("sp")):
        toks = who_tokens(sp.get("who"))
        if not toks:
            spk = sp.findtext(q("speaker")) or ""
            xid = sp.get(f"{{http://www.w3.org/XML/1998/namespace}}id") or "?"
            sp_no_who.append(f"{xid}: {spk.strip()[:30]!r}")
        used.update(toks)
    # @who may also live on <stage> etc.
    for el in root.iter():
        if el.get("who") and el.tag != q("sp"):
            used.update(who_tokens(el.get("who")))

    unreferenced = sorted(x for x in declared if x not in used)
    unresolved = sorted(t for t in used if t not in declared)

    try:
        shown = path.relative_to(REPO_ROOT)
    except ValueError:
        shown = path
    print(f"\n=== {shown} ===")
    print(f"  particDesc entries: {len(declared)}  "
          f"(person={sum(v=='person' for v in declared.values())}, "
          f"personGrp={sum(v=='personGrp' for v in declared.values())})")
    print(f"  distinct @who tokens used: {len(used)}")

    print(f"\n  [1] particDesc entries with NO @who reference: {len(unreferenced)}")
    for x in unreferenced:
        print(f"        {x}  ({declared[x]})")

    print(f"\n  [2] <sp> with NO @who: {len(sp_no_who)}")
    for s in sp_no_who:
        print(f"        {s}")

    print(f"\n  [3] @who tokens NOT in particDesc: {len(unresolved)}")
    for t in unresolved:
        print(f"        #{t}")

    return len(unreferenced) + len(sp_no_who) + len(unresolved)


def main() -> None:
    args = sys.argv[1:]
    if args:
        paths = [Path(a) if Path(a).is_absolute() else REPO_ROOT / a for a in args]
    else:
        paths = sorted((REPO_ROOT / "tei").glob("*.xml"))
    total = 0
    for p in paths:
        # skip non-TEI files (some tei/ files are non-edition exports)
        try:
            root = etree.parse(str(p)).getroot()
        except etree.XMLSyntaxError:
            continue
        if root.tag != q("TEI"):
            continue
        total += check_file(p)
    print(f"\nTotal issues across {len(paths)} file(s): {total}")


if __name__ == "__main__":
    main()
