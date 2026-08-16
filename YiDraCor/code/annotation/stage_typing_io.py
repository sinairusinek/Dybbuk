"""Export untyped `stage` spans for in-session typing, and apply the answers.

`annotate_pages` establishes the pattern: the LLM step runs in the Claude Code
session, not through an API call, and the module's job is to hand the model
clean input and write its output back safely. That is what the print track did
to reach 3,114 typed stage spans with none left over; this is the same loop
scoped to spans rather than whole pages.

`export` writes a numbered batch. Each row is the span, the line it sits in
(the surrounding text is usually what decides `delivery` vs `business`), and
the play — enough to type without opening the page, with a Transkribus link
for when it isn't.

`apply` takes `<n> <type>` lines back. Every span is addressed by
(play, page, line_id, offset), so an answer lands on the span it was asked
about even if another pass has since edited the line. It is a no-op if the
span already has a type, and it validates every value against
`schema._validate_stage_type` before writing — a typo or an invented token is
rejected loudly rather than baked into the corpus.

  python3.11 -m annotation.stage_typing_io export --out /tmp/b --size 150
  python3.11 -m annotation.stage_typing_io apply --batch /tmp/b_001.tsv --answers /tmp/a_001.txt
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import (  # noqa: E402
    PAGE_NS, parse_custom, serialize_custom, _validate_stage_type,
)
from annotation.review_links import page_url  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"
MS_FOLDERS = ["Lateiner_Meshumed"]


def _line_text(tl) -> str:
    for te in tl.findall(f"{NS}TextEquiv"):
        u = te.find(f"{NS}Unicode")
        if u is not None:
            return u.text or ""
    return ""


def _folders(only=None):
    data = REPO / "data"
    out = [p for p in sorted(data.glob("MS_*")) if (p / "page_annotated").is_dir()]
    out += [data / n for n in MS_FOLDERS if (data / n / "page_annotated").is_dir()]
    if only:
        out = [f for f in out if f.name in set(only)]
    return sorted(set(out))


# Prompt-book structural markers: Auftritt/Aufzug (scene/act), the S· and R·
# forms, and bare numerals. These are historical division markings mis-tagged
# as `stage`, not directions — they belong to the heading-notation question and
# should be retagged `heading` once the RAs settle what they mean, so they are
# not offered for stage typing.
_STRUCTURAL = [
    re.compile(r'^(auf|auft|auftr|auftri|auftritt|aufz|aufzug)\.?', re.I),
    re.compile(r'^[SR][\s.=/]*[IVX0-9]*\.?$', re.I),
    re.compile(r'^[IVX]+\.?$'),
    re.compile(r'^[\W\d]+$'),
    # Numerals mixed with Roman numerals and punctuation only: `2. (II)`,
    # `3. (III)`, `.II A` — the same division numbering as `ער אקט III (ער3)`.
    re.compile(r'^[IVXA\d\s.()/=-]+$'),
]


def is_structural(span: str) -> bool:
    t = re.sub(r"[\u0591-\u05C7]", "", span or "").strip()
    return not t or any(rx.match(t) for rx in _STRUCTURAL)


def collect(only=None, skip_structural=False):
    """Every untyped stage span, in a stable order."""
    rows = []
    for folder in _folders(only):
        for xf in sorted((folder / "page_annotated").glob("*.xml")):
            try:
                tree = etree.parse(str(xf))
            except etree.XMLSyntaxError:
                continue
            for tl in tree.getroot().iter(f"{NS}TextLine"):
                ents = parse_custom(tl.get("custom") or "")
                if not any(t == "stage" for t, _ in ents):
                    continue
                text = _line_text(tl)
                for tag, a in ents:
                    if tag != "stage" or a.get("type"):
                        continue
                    try:
                        off, ln = int(a["offset"]), int(a["length"])
                    except (KeyError, ValueError):
                        continue
                    if skip_structural and is_structural(text[off:off + ln]):
                        continue
                    rows.append({
                        "play": folder.name,
                        "page": xf.name,
                        "line_id": tl.get("id") or "",
                        "offset": str(off),
                        "span": re.sub(r"\s+", " ", text[off:off + ln]).strip(),
                        "line": re.sub(r"\s+", " ", text).strip(),
                        "url": page_url(folder.name, xf.name),
                    })
    return rows


def cmd_export(args) -> int:
    rows = collect(args.only, skip_structural=args.skip_structural)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = ["n", "play", "page", "line_id", "offset", "span", "line", "url"]
    nbatch = 0
    for i in range(0, len(rows), args.size):
        nbatch += 1
        chunk = rows[i:i + args.size]
        p = out.parent / f"{out.name}_{nbatch:03d}.tsv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
            w.writeheader()
            for j, r in enumerate(chunk, start=i + 1):
                w.writerow({"n": j, **{c: r.get(c, "") for c in cols[1:]}})
        print(f"{p}  ({len(chunk)} spans, n={i+1}..{i+len(chunk)})")
    print(f"total {len(rows)} untyped spans in {nbatch} batches")
    return 0


def _read_answers(path: Path) -> dict[int, str]:
    """`<n> <type tokens>` per line; blanks and #comments ignored."""
    out: dict[int, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(\d+)[\s:.\t]+(.+)$", line)
        if not m:
            raise SystemExit(f"unparseable answer line: {raw!r}")
        out[int(m.group(1))] = " ".join(m.group(2).split())
    return out


def cmd_apply(args) -> int:
    batch = {int(r["n"]): r for r in csv.DictReader(
        open(args.batch, encoding="utf-8"), delimiter="\t")}
    answers = _read_answers(Path(args.answers))

    missing = sorted(set(batch) - set(answers))
    extra = sorted(set(answers) - set(batch))
    if extra:
        raise SystemExit(f"answers for rows not in this batch: {extra[:10]}")
    for n, t in answers.items():
        err = _validate_stage_type(t)
        if err:
            raise SystemExit(f"row {n}: {err}")

    # Group by page so each file is parsed and written once.
    by_page: dict[tuple[str, str], list] = {}
    for n, t in answers.items():
        r = batch[n]
        by_page.setdefault((r["play"], r["page"]), []).append((r, t))

    applied = skipped = notfound = 0
    for (play, page), items in sorted(by_page.items()):
        xf = REPO / "data" / play / "page_annotated" / page
        tree = etree.parse(str(xf))
        changed = False
        index = {tl.get("id"): tl for tl in tree.getroot().iter(f"{NS}TextLine")}
        for r, t in items:
            tl = index.get(r["line_id"])
            if tl is None:
                notfound += 1
                continue
            ents = parse_custom(tl.get("custom") or "")
            out, hit = [], False
            for tag, a in ents:
                if (tag == "stage" and a.get("offset") == r["offset"]
                        and not a.get("type")):
                    a = dict(a)
                    a["type"] = t
                    hit = True
                out.append((tag, a))
            if hit:
                tl.set("custom", serialize_custom(out))
                changed = True
                applied += 1
            else:
                skipped += 1
        if changed and not args.dry_run:
            tree.write(str(xf), encoding="utf-8", xml_declaration=True)

    print(f"applied {applied}, already-typed/not-matched {skipped}, "
          f"line not found {notfound}, unanswered {len(missing)}")
    if missing:
        print(f"  unanswered rows: {missing[:20]}{' …' if len(missing) > 20 else ''}")
    print("DRY RUN — nothing written" if args.dry_run else "written")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("export")
    e.add_argument("--out", required=True, help="path prefix; _001.tsv etc. appended")
    e.add_argument("--size", type=int, default=150)
    e.add_argument("--only", action="append")
    e.add_argument("--skip-structural", action="store_true",
                   help="omit prompt-book division markers (heading question)")
    e.set_defaults(func=cmd_export)

    a = sub.add_parser("apply")
    a.add_argument("--batch", required=True)
    a.add_argument("--answers", required=True)
    a.add_argument("--dry-run", action="store_true")
    a.set_defaults(func=cmd_apply)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
