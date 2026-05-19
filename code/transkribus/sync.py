"""Sync PAGE-XML between Transkribus and local data/ tree.

Subcommands:
  collections                       list collections the user can see
  docs --col CID                    list documents in a collection
  pages --col CID --doc DID         list pages + latest transcript URL
  pull  --col CID --doc DID --out DIR
                                    download latest PAGE-XML per page to DIR
  push  --file PAGE_XML [--col CID --doc DID --page-nr N] [--dry-run]
                                    upload one PAGE-XML as a new transcript
                                    layer. col/doc/pageNr default to the values
                                    in the file's TranskribusMetadata.
  push-dir --dir DIR [--col CID --doc DID] [--dry-run]
                                    upload every *.xml in DIR (skips files
                                    without a TranskribusMetadata pageNr).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys
from pathlib import Path

from .client import TrpClient


PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"


def _print_table(rows: list[dict], cols: list[tuple[str, str]]) -> None:
    """cols = [(key, header), ...]; prints a fixed-width table."""
    widths = {k: max(len(h), *(len(str(r.get(k, ""))) for r in rows)) for k, h in cols}
    print("  ".join(h.ljust(widths[k]) for k, h in cols))
    print("  ".join("-" * widths[k] for k, _ in cols))
    for r in rows:
        print("  ".join(str(r.get(k, "")).ljust(widths[k]) for k, _ in cols))


def cmd_collections(args: argparse.Namespace) -> int:
    c = TrpClient.from_env()
    cols = c.list_collections()
    _print_table(cols, [("colId", "colId"), ("colName", "name"), ("nrOfDocuments", "#docs")])
    return 0


def cmd_docs(args: argparse.Namespace) -> int:
    c = TrpClient.from_env()
    docs = c.list_docs(args.col)
    _print_table(docs, [("docId", "docId"), ("title", "title"), ("nrOfPages", "#pages")])
    return 0


def cmd_pages(args: argparse.Namespace) -> int:
    c = TrpClient.from_env()
    doc = c.fulldoc(args.col, args.doc)
    pages = doc.get("pageList", {}).get("pages", [])
    rows = []
    for p in pages:
        ts = (p.get("tsList", {}).get("transcripts") or [{}])[0]
        rows.append({
            "pageNr": p.get("pageNr"),
            "pageId": p.get("pageId"),
            "tsId": ts.get("tsId"),
            "status": ts.get("status"),
            "toolName": ts.get("toolName"),
        })
    _print_table(rows, [
        ("pageNr", "pageNr"),
        ("pageId", "pageId"),
        ("tsId", "tsId"),
        ("status", "status"),
        ("toolName", "toolName"),
    ])
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    c = TrpClient.from_env()
    doc = c.fulldoc(args.col, args.doc)
    pages = doc.get("pageList", {}).get("pages", [])
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    manifest = {
        "colId": args.col, "docId": args.doc,
        "docTitle": doc.get("md", {}).get("title"),
        "pages": [],
    }
    for p in pages:
        page_nr = p.get("pageNr")
        transcripts = p.get("tsList", {}).get("transcripts") or []
        if not transcripts:
            print(f"[skip] page {page_nr}: no transcript", file=sys.stderr)
            continue
        ts = transcripts[0]  # latest
        url = ts.get("url")
        if not url:
            print(f"[skip] page {page_nr}: no transcript URL", file=sys.stderr)
            continue
        page_xml = c.fetch_transcript(url)
        fname = f"{page_nr:04d}_{p.get('pageId')}.xml"
        (out / fname).write_text(page_xml, encoding="utf-8")
        manifest["pages"].append({
            "pageNr": page_nr, "pageId": p.get("pageId"),
            "tsId": ts.get("tsId"), "file": fname, "status": ts.get("status"),
        })
        print(f"[pulled] {fname} (tsId={ts.get('tsId')}, status={ts.get('status')})")

    (out / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\nWrote {len(manifest['pages'])} pages + _manifest.json to {out}")
    return 0


def _extract_trp_metadata(page_xml: str) -> dict:
    """Pull docId / pageId / pageNr / tsid from <TranskribusMetadata> in a PAGE-XML."""
    m = re.search(r"<TranskribusMetadata\b(.*?)/?>", page_xml, re.DOTALL)
    if not m:
        return {}
    attrs = {}
    for k, v in re.findall(r'(\w+)="([^"]*)"', m.group(1)):
        attrs[k] = v
    out: dict = {}
    for k in ("docId", "pageId", "pageNr", "tsid"):
        if k in attrs:
            try:
                out[k] = int(attrs[k])
            except ValueError:
                out[k] = attrs[k]
    out["status"] = attrs.get("status")
    return out


def cmd_push(args: argparse.Namespace) -> int:
    path = Path(args.file)
    page_xml = path.read_text(encoding="utf-8")
    meta = _extract_trp_metadata(page_xml)
    col_id = args.col if args.col is not None else meta.get("docId") and args.col  # placeholder
    # We can derive doc/pageNr from metadata, but col_id has to come from caller
    # (collections are not stamped into the PAGE-XML).
    doc_id = args.doc if args.doc is not None else meta.get("docId")
    page_nr = args.page_nr if args.page_nr is not None else meta.get("pageNr")
    parent = args.parent if args.parent is not None else meta.get("tsid")
    if args.col is None:
        print("ERROR: --col is required (not stored in PAGE-XML)", file=sys.stderr)
        return 2
    if doc_id is None or page_nr is None:
        print("ERROR: doc/pageNr missing — file has no TranskribusMetadata. "
              "Pass --doc and --page-nr explicitly.", file=sys.stderr)
        return 2

    note = args.note or f"YiDraCor annotation push {_dt.date.today().isoformat()}"

    print(f"[push] file={path.name}")
    print(f"       col={args.col} doc={doc_id} pageNr={page_nr} parent={parent}")
    print(f"       status={args.status} toolName={args.tool_name} note={note!r}")
    print(f"       size={len(page_xml)} bytes")
    if args.dry_run:
        print("[dry-run] not sending")
        return 0

    c = TrpClient.from_env()
    resp = c.push_transcript(
        args.col, int(doc_id), int(page_nr), page_xml,
        parent_tsid=int(parent) if parent else None,
        status=args.status, note=note, tool_name=args.tool_name,
    )
    print("[ok]", json.dumps(resp, ensure_ascii=False)[:400])
    return 0


def cmd_push_dir(args: argparse.Namespace) -> int:
    if args.col is None:
        print("ERROR: --col is required", file=sys.stderr)
        return 2
    files = sorted(Path(args.dir).glob("*.xml"))
    files = [f for f in files if not f.name.startswith("_")]
    if not files:
        print(f"No .xml files in {args.dir}", file=sys.stderr)
        return 1

    c = None if args.dry_run else TrpClient.from_env()
    note = args.note or f"YiDraCor annotation push {_dt.date.today().isoformat()}"
    n_ok = n_skip = n_err = 0
    for f in files:
        xml = f.read_text(encoding="utf-8")
        meta = _extract_trp_metadata(xml)
        doc_id = args.doc if args.doc is not None else meta.get("docId")
        page_nr = meta.get("pageNr")
        parent = meta.get("tsid")
        if doc_id is None or page_nr is None:
            print(f"[skip] {f.name}: no TranskribusMetadata")
            n_skip += 1
            continue
        print(f"[push] {f.name} → doc={doc_id} pageNr={page_nr} parent={parent}", end="")
        if args.dry_run:
            print("  (dry-run)")
            n_ok += 1
            continue
        try:
            c.push_transcript(args.col, int(doc_id), int(page_nr), xml,
                              parent_tsid=int(parent) if parent else None,
                              status=args.status, note=note, tool_name=args.tool_name)
            print("  ok")
            n_ok += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            n_err += 1
    print(f"\nDone: ok={n_ok} skip={n_skip} err={n_err}")
    return 0 if n_err == 0 else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="transkribus.sync")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("collections").set_defaults(func=cmd_collections)

    sp = sub.add_parser("docs"); sp.add_argument("--col", type=int, required=True)
    sp.set_defaults(func=cmd_docs)

    sp = sub.add_parser("pages")
    sp.add_argument("--col", type=int, required=True)
    sp.add_argument("--doc", type=int, required=True)
    sp.set_defaults(func=cmd_pages)

    sp = sub.add_parser("pull")
    sp.add_argument("--col", type=int, required=True)
    sp.add_argument("--doc", type=int, required=True)
    sp.add_argument("--out", required=True, help="output directory for PAGE-XML files")
    sp.set_defaults(func=cmd_pull)

    sp = sub.add_parser("push", help="upload one PAGE-XML as a new transcript layer")
    sp.add_argument("--col", type=int, required=True)
    sp.add_argument("--doc", type=int, help="doc id (default: from TranskribusMetadata)")
    sp.add_argument("--page-nr", type=int, dest="page_nr",
                    help="page number (default: from TranskribusMetadata)")
    sp.add_argument("--parent", type=int, help="parent tsId (default: tsid in metadata)")
    sp.add_argument("--file", required=True, help="path to PAGE-XML file")
    sp.add_argument("--status", default="IN_PROGRESS",
                    help="transcript status (NEW, IN_PROGRESS, DONE, FINAL)")
    sp.add_argument("--tool-name", default="YiDraCor-annotation-pipeline")
    sp.add_argument("--note", default=None)
    sp.add_argument("--dry-run", action="store_true",
                    help="print what would be sent, don't POST")
    sp.set_defaults(func=cmd_push)

    sp = sub.add_parser("push-dir", help="upload every PAGE-XML in a directory")
    sp.add_argument("--col", type=int, required=True)
    sp.add_argument("--doc", type=int, help="override docId from metadata")
    sp.add_argument("--dir", required=True, help="directory of PAGE-XML files")
    sp.add_argument("--status", default="IN_PROGRESS")
    sp.add_argument("--tool-name", default="YiDraCor-annotation-pipeline")
    sp.add_argument("--note", default=None)
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_push_dir)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
