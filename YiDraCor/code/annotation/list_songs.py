"""List all <lg> (song) blocks per play, with first-line incipits."""
import argparse, json, re, sys
from collections import defaultdict
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import PAGE_NS, parse_custom

LINE_TAG = f"{{{PAGE_NS}}}TextLine"
UNICODE_TAG = f"{{{PAGE_NS}}}Unicode"
REPO = Path(__file__).resolve().parents[2]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()

    d = REPO / "data" / args.play / "page_annotated"
    pages = sorted(d.glob("[0-9]*.xml"))

    by_lg = defaultdict(lambda: {"head": [], "lines": [], "lg_marks": []})
    for pf in pages:
        page_num = int(pf.name.split("_")[0])
        tree = etree.parse(str(pf))
        for line in tree.iter(LINE_TAG):
            tags = parse_custom(line.get("custom", ""))
            u = line.find(f".//{UNICODE_TAG}")
            txt = (u.text or "").rstrip() if u is not None else ""
            for tag, attrs in tags:
                if tag == "l":
                    by_lg[attrs.get("lg_id", "?")]["lines"].append({"page": page_num, "text": txt})
                elif tag == "head":
                    by_lg[attrs.get("lg_id", "?")]["head"].append({"page": page_num, "text": txt})
                elif tag == "lg":
                    by_lg[attrs.get("n", "?")]["lg_marks"].append({"page": page_num, "cont": attrs.get("cont", "?")})

    songs = []
    for lg_id in sorted(by_lg, key=lambda k: int(k) if str(k).isdigit() else 1e9):
        info = by_lg[lg_id]
        lines = info["lines"]
        if not lines and not info["head"]:
            continue
        songs.append({
            "lg_id": lg_id,
            "pages": sorted({ln["page"] for ln in lines + info["head"]}),
            "n_lines": len(lines),
            "head": [h["text"] for h in info["head"]],
            "lg_marks": info["lg_marks"],
            "incipit": lines[0]["text"][:60] if lines else (info["head"][0]["text"][:60] if info["head"] else ""),
            "lines": [ln["text"] for ln in lines],
        })

    out_path = Path(args.out) if args.out else REPO / "data" / args.play / "songs.json"
    out_path.write_text(json.dumps({"play": args.play, "songs": songs},
                                   ensure_ascii=False, indent=2))
    print(f"{args.play}: {len(songs)} songs, {sum(s['n_lines'] for s in songs)} lines")
    for s in songs:
        pages = ",".join(map(str, s["pages"]))
        head_txt = f"  head={s['head'][0]!r}" if s["head"] else ""
        cont_pgs = [m["page"] for m in s["lg_marks"] if m["cont"] == "yes"]
        cont = f"  continued_on={cont_pgs}" if cont_pgs else ""
        print(f"  lg{s['lg_id']:>3}  pp.{pages:<10}  {s['n_lines']:>3} lines{head_txt}{cont}  → {s['incipit']!r}")


if __name__ == "__main__":
    main()
