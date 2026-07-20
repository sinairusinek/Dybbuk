"""PAGE-XML coverage linter for YiDraCor annotated pages.

`schema.validate_span` is a POSITIVE validator — it only inspects spans that
exist (bad @type, unknown attr, xmlid-not-in-cast). It structurally cannot see
an *absent* span: a line that reads like a speech turn but carries no `speaker`,
a body line the structurer will silently drop, a cast member never referenced.
Those are exactly the issues that only surfaced when reading the structured TEI
for Di Seder.

This linter closes that gap. It runs over `data/<play>/page_annotated/` and
emits flags in the same CSV schema the Zalmen `yidracor_flags` review consumes
(via `make_flag_crops.py`). Detectors:

  untagged speaker (collective)  — `אלע:` / `קאהר:` etc. with no speaker span.
                                   Auto-fixable (PI-confirmed collective). owner=AUTO.
  untagged speaker (named)        — `<label>:` resolvable to a cast xmlid via
                                   cast_dict bare/prefix_variants. suggested xmlid.
  untagged speaker (unknown)      — turn-like line, label neither collective nor
                                   in cast (often OCR, e.g. `ולמן`). owner=NOA.
  schema violation                — re-run validate_span over existing spans.
  unreferenced cast               — a declared role never used by any speaker.
  act numbering                   — act @n not 1..k contiguous across the play.

Usage:
  python -m annotation.lint_pages --all
  python -m annotation.lint_pages --play Di_seyder_nakht_Emkroyt_1908
  python -m annotation.lint_pages --all --out data/review/lint_flags_2026-05-25.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (
    PAGE_NS, parse_custom, validate_span, is_collective_label, _NIKUD,
)

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"
EDITIONS_CSV = REPO / "data" / "editions.csv"

# A speech-turn opener: a short label (<=18 non-colon chars) then ':' / '׃'.
TURN_RE = re.compile(r"^\s*([^\s:׃]{1,18}(?:\s+[^\s:׃]{1,18}){0,2})\s*[:׃]")
# Canonical xmlid for each collective surface skeleton (consistent across plays).
COLLECTIVE_XMLID = {
    "אלע": "alle", "שטימען": "shtimen", "ביידע": "beyde", "מענער": "mener",
    "מעדכען": "meydkhen", "מ_דכען": "meydkhen", "קאהר": "chor", "כאר": "chor", "קאר": "chor",
    "דועט": "duet", "איינער": "eyner", "דאמען": "damen", "קינדער": "kinder",
    "סאפראן": "sopran", "אלט": "alt", "באס": "bas", "טענאר": "tenor",
}

FLAG_COLUMNS = ["edition", "page(s)", "line_id / count", "category",
                "owner", "issue/detail", "text", "suggested_action"]


def skel(s: str) -> str:
    return _NIKUD.sub("", (s or "").strip()).strip(":׃־ .")


def has_nikud(s: str) -> bool:
    return bool(_NIKUD.search(s or ""))


def line_text(tl) -> str:
    for u in tl.iter(NS + "Unicode"):
        if u.getparent().getparent() is tl:
            return u.text or ""
    return ""


_GLOBAL_A_PREFIXES = ("ארטדערהאנדלונג", "ארטהאנדלונג",
                      "דיגעשיכטעהאנדעלטזיך", "דיאגעשיכטעהאנדעלטזיך")


def _is_global_a_line(text: str) -> bool:
    sk = re.sub(r"[()\s.,׃:‐-―\-]", "", _NIKUD.sub("", text or ""))
    return sk.startswith(_GLOBAL_A_PREFIXES)


def _has_setting(tl) -> bool:
    return any(t == "stage" and "setting" in (a.get("type") or "")
               for t, a in parse_custom(tl.get("custom") or ""))


def page_type(tree) -> str | None:
    """Read the region structure{type:...} marker (titlePage/castList/body)."""
    for reg in tree.iter(NS + "TextRegion"):
        for tag, a in parse_custom(reg.get("custom") or ""):
            if tag == "structure" and a.get("type"):
                return a["type"]
    return None


def load_editions() -> dict[str, str]:
    """folder -> human edition label."""
    out = {}
    with open(EDITIONS_CSV, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("folder"):
                out[r["folder"]] = r.get("title", r["folder"])
    return out


def load_cast(play: str) -> tuple[dict[str, str], set[str]]:
    """Return (label_skeleton -> xmlid index, set of declared xmlids)."""
    f = REPO / "data" / play / "cast_dict.json"
    if not f.exists():
        return {}, set()
    d = json.loads(f.read_text(encoding="utf-8"))
    roles = d.get("roles", {})
    index: dict[str, str] = {}
    for xmlid, info in roles.items():
        surfaces = [info.get("bare", ""), info.get("form", "")]
        surfaces += info.get("prefix_variants", []) or []
        # also the first word of the bare name (speech labels are usually first name)
        if info.get("bare"):
            surfaces.append(info["bare"].split()[0])
        for s in surfaces:
            k = skel(s)
            if k and k not in index:
                index[k] = xmlid
    return index, set(roles.keys())


def page_files(play: str):
    d = REPO / "data" / play / "page_annotated"
    if not d.is_dir():
        return []
    out = []
    for p in sorted(d.glob("[0-9]" * 4 + "_*.xml")):
        out.append((int(p.name[:4]), p))
    return out


def lint_play(play: str, label: str) -> list[dict]:
    cast_index, declared = load_cast(play)
    flags: list[dict] = []
    referenced: set[str] = set()
    act_seq: list[tuple[int, int]] = []  # (page, n)
    # de-dup collective/coverage rows that recur many times
    collective_pages: dict[str, list[int]] = defaultdict(list)

    def add(page, line_id, category, owner, detail, text, suggested):
        flags.append({
            "edition": label, "page(s)": str(page), "line_id / count": line_id,
            "category": category, "owner": owner, "issue/detail": detail,
            "text": text, "suggested_action": suggested,
        })

    for page, path in page_files(play):
        tree = etree.parse(str(path))
        ptype = page_type(tree)
        for tl in tree.iter(NS + "TextLine"):
            txt = line_text(tl)
            spans = parse_custom(tl.get("custom") or "")
            tags = {t for t, _ in spans}
            # track referenced + schema-validate existing spans
            for t, a in spans:
                if t in ("readingOrder", "structure"):
                    continue
                if t in ("speaker", "role") and a.get("xmlid"):
                    referenced.add(a["xmlid"])
                if t == "heading" and a.get("type") == "act":
                    try:
                        act_seq.append((page, int(a.get("n", "0"))))
                    except ValueError:
                        pass
                try:
                    off = int(a.get("offset")); ln = int(a.get("length"))
                except (TypeError, ValueError):
                    continue
                err = validate_span(txt, {"tag": t, "offset": off, "length": ln,
                                          "attrs": {k: v for k, v in a.items()
                                                    if k not in ("offset", "length")}})
                if err:
                    add(page, tl.get("id"), "schema violation", "NOA",
                        err, txt[off:off + ln], "fix per schema")

            # ---- untagged speaker / cast-entry turn ----
            if "speaker" in tags:
                continue
            m = TURN_RE.match(txt)
            if not m:
                continue
            label_txt = m.group(1)
            k = skel(label_txt)
            rest = txt[m.end():]
            # signal that this is really a turn (not e.g. "אז:" mid-sentence):
            # label unvocalized while the rest of the line is vocalized.
            turn_signal = (not has_nikud(label_txt)) and has_nikud(rest)
            # titlePage: "Name: description"-shaped lines aren't speech turns.
            if ptype == "titlePage":
                continue
            # castList: a "<name>: <role-desc>" line lacking a role span is an
            # untagged cast entry, not an untagged speaker.
            if ptype == "castList":
                if skel(label_txt).startswith("פערזאנען") or skel(label_txt).startswith("פערזאן"):
                    continue  # the "פּערזאָנען:" dramatis-personae header, not an entry
                # Global A: a locus-of-action closing line is a setting, not a
                # role. Until 2026-07-20 this was invisible: the check below
                # only fires on a line with NO role/roleDesc, so a line
                # MIS-tagged `roleDesc` (Al Naharot p.6) read as tagged.
                if _is_global_a_line(txt) and not _has_setting(tl):
                    add(page, tl.get("id"), "mis-tagged setting line", "AUTO",
                        "Global-A locus-of-action line not tagged "
                        "stage{type:setting}", txt[:40],
                        "retag whole line stage{type:setting}")
                    continue
                if not ({"role", "roleDesc"} & tags):
                    add(page, tl.get("id"), "untagged cast entry", "NOA",
                        f"castList line '{k}' has no role/roleDesc span", txt[:40],
                        "tag role + roleDesc")
                continue
            if is_collective_label(label_txt):
                collective_pages[COLLECTIVE_XMLID.get(k, k)].append(page)
                # individual rows still emitted (auto owner) for the applier
                add(page, tl.get("id"), "untagged speaker (collective)", "AUTO",
                    f"collective '{k}' — no speaker span",
                    txt[:40], f"tag speaker xmlid:{COLLECTIVE_XMLID.get(k, k)}")
            elif k in cast_index:
                add(page, tl.get("id"), "untagged speaker (named)", "NOA",
                    f"label '{k}' matches cast role", txt[:40],
                    f"tag speaker xmlid:{cast_index[k]}")
            elif turn_signal:
                add(page, tl.get("id"), "untagged speaker (unknown)", "NOA",
                    f"turn-like '{k}' not in cast (OCR?)", txt[:40],
                    "fix OCR / link to a cast xmlid")

    # ---- cast coverage ----
    for xmlid in sorted(declared - referenced):
        add("—", f"role:{xmlid}", "unreferenced cast", "NOA",
            "declared in cast_dict but never used by any speaker", xmlid,
            "verify the role is real / find its speeches")

    # ---- act numbering ----
    nums = sorted({n for _, n in act_seq})
    if nums and nums != list(range(1, len(nums) + 1)):
        add("—", "acts", "act numbering", "NOA",
            f"act @n not contiguous 1..k: saw {nums}", "", "check for missing/extra act heading")

    return flags


def discover_plays() -> list[str]:
    base = REPO / "data"
    return sorted(p.name for p in base.iterdir()
                  if (p / "page_annotated").is_dir())


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--play", help="data/<folder> name")
    g.add_argument("--all", action="store_true", help="all plays with page_annotated/")
    ap.add_argument("--out", help="flags CSV path (default data/review/lint_flags_<date>.csv)")
    args = ap.parse_args()

    editions = load_editions()
    plays = [args.play] if args.play else discover_plays()
    all_flags: list[dict] = []
    for play in plays:
        label = editions.get(play, play)
        fl = lint_play(play, label)
        all_flags.extend(fl)
        by_cat = defaultdict(int)
        for f in fl:
            by_cat[f["category"]] += 1
        print(f"{label:28} {len(fl):4} flags  " +
              "  ".join(f"{c}={n}" for c, n in sorted(by_cat.items())))

    out = Path(args.out) if args.out else REPO / "data" / "review" / f"lint_flags_{_dt.date.today()}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FLAG_COLUMNS)
        w.writeheader(); w.writerows(all_flags)
    print(f"\n{len(all_flags)} flags → {out.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
