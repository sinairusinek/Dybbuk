"""Pull RA-edit layer + matching pipeline layer per page, for diff analysis.

For each play in editions.csv with a local `page_annotated/` dir AND a non-empty
`transkribus_doc_id`, fetch per page:
  - `final`   : the most recent transcript that looks like an RA edit
  - `pipeline`: the most recent ancestor that is one of OUR pipeline pushes

If the page's most recent transcript IS already a pipeline push (i.e. no RA
layer on top), skip the page.

Outputs:
  data/<folder>/layer_diff_2026-05-31/<pageNr:04>_pipeline.xml
  data/<folder>/layer_diff_2026-05-31/<pageNr:04>_final.xml
  data/<folder>/layer_diff_2026-05-31/_manifest.json

Run from inside `code/` to avoid stdlib `code` shadowing:
    cd code && python3 -m transkribus.pull_layers
"""
from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path
from typing import Optional

from transkribus.client import TrpClient

DATE = "2026-06-14"
COLLECTION_ID = 2372172

# Tool-name prefixes that identify OUR pipeline pushes (not RA edits).
PIPELINE_TOOL_PREFIXES = (
    "YiDraCor",
    "apply_pi_decisions",
    "apply_collective_speakers",
    "auto_resolve",
)

# Anything that starts with these is definitely an RA edit (Transkribus client).
RA_TOOL_PREFIXES = ("App_", "TranskribusLite", "Transkribus")

# Account that pushes the pipeline layers (Sinai). Anyone else editing under
# *our* tool name (e.g. judith re-running apply_pi_decisions in TranskribusLite)
# counts as an RA edit for our purposes.
PIPELINE_USER = "sinai.rusinek@mail.huji.ac.il"


def is_pipeline(t: dict) -> bool:
    name = (t.get("toolName") or "")
    user = (t.get("userName") or "")
    if any(name.startswith(p) for p in PIPELINE_TOOL_PREFIXES):
        # pipeline-tool-named, but only really ours if pushed by Sinai
        return user == PIPELINE_USER
    return False


def is_ra(t: dict) -> bool:
    name = (t.get("toolName") or "")
    user = (t.get("userName") or "")
    if any(name.startswith(p) for p in RA_TOOL_PREFIXES):
        return True
    # pipeline-tool-named but pushed by someone other than Sinai = RA
    if any(name.startswith(p) for p in PIPELINE_TOOL_PREFIXES) and user != PIPELINE_USER:
        return True
    return False


def find_pair(transcripts: list[dict]) -> Optional[tuple[dict, dict]]:
    """transcripts are newest-first. Return (final RA, pipeline ancestor) or None."""
    if not transcripts:
        return None
    # Walk from newest; find newest RA, then newest pipeline at or below it.
    final = None
    for t in transcripts:
        if is_ra(t):
            final = t
            break
    if final is None:
        return None
    # Walk from final's parent down until we hit a pipeline layer (any depth).
    by_ts = {t["tsId"]: t for t in transcripts}
    cur_parent = final.get("parentTsId")
    while cur_parent and cur_parent != -1:
        p = by_ts.get(cur_parent)
        if p is None:
            break
        if is_pipeline(p):
            return (final, p)
        cur_parent = p.get("parentTsId")
    # If parent-chain didn't yield one (e.g. doc was re-uploaded), fall back to
    # the most recent pipeline anywhere below `final` in the time order.
    final_ts = final.get("timestamp", 0)
    for t in transcripts:
        if t is final:
            continue
        if t.get("timestamp", 0) >= final_ts:
            continue
        if is_pipeline(t):
            return (final, t)
    return None


def load_plays() -> list[tuple[str, int]]:
    root = Path(__file__).resolve().parents[2]
    editions = root / "data" / "editions.csv"
    out: list[tuple[str, int]] = []
    target_folders = {
        "AlNaharotBavel-Amkreut&Freund1909",
        "DerManUnterTiff",
        "Di_seyder_nakht_Emkroyt_1908",
        "KidushHashem",
        "MishkeMashke-Kultur1910",
        "Yudale_der_blinder,_Emkroyt1908",
        "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete",
        "Ezra-Emkroyt1908",
        "Blimele-AhronFaust1903",
        "IshahRaah",
        "Lateiner_Meshumed",
    }
    with editions.open() as f:
        for r in csv.DictReader(f):
            folder = r.get("folder") or ""
            doc_id = (r.get("transkribus_doc_id") or "").strip()
            if folder in target_folders and doc_id:
                out.append((folder, int(doc_id)))
    return out


def pull_play(client: TrpClient, folder: str, doc_id: int) -> dict:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "data" / folder / f"layer_diff_{DATE}"
    out_dir.mkdir(parents=True, exist_ok=True)
    fd = client.fulldoc(COLLECTION_ID, doc_id)
    pages = fd.get("pageList", {}).get("pages", [])
    manifest = []
    n_ra = 0
    for p in pages:
        page_nr = int(p["pageNr"])
        page_id = p["pageId"]
        ts = p.get("tsList", {}).get("transcripts", [])
        pair = find_pair(ts)
        if pair is None:
            continue
        final, pipeline = pair
        n_ra += 1
        # Fetch both transcripts (sequential to be polite)
        try:
            final_xml = client.fetch_transcript(final["url"])
            pipeline_xml = client.fetch_transcript(pipeline["url"])
        except Exception as e:
            print(f"  page {page_nr}: fetch failed: {e}", file=sys.stderr)
            continue
        (out_dir / f"{page_nr:04d}_pipeline.xml").write_text(pipeline_xml, encoding="utf-8")
        (out_dir / f"{page_nr:04d}_final.xml").write_text(final_xml, encoding="utf-8")
        manifest.append({
            "pageNr": page_nr,
            "pageId": page_id,
            "pipeline_tsId": pipeline.get("tsId"),
            "pipeline_tool": pipeline.get("toolName"),
            "pipeline_user": pipeline.get("userName"),
            "pipeline_ts": pipeline.get("timestamp"),
            "final_tsId": final.get("tsId"),
            "final_tool": final.get("toolName"),
            "final_user": final.get("userName"),
            "final_ts": final.get("timestamp"),
            "final_status": final.get("status"),
        })
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"folder": folder, "doc_id": doc_id, "n_pages": len(pages), "n_ra_pages": n_ra}


def main() -> int:
    client = TrpClient.from_env()
    plays = load_plays()
    print(f"Plays: {len(plays)}")
    results = []
    for folder, doc_id in plays:
        print(f"--- {folder} (doc {doc_id}) ---")
        try:
            r = pull_play(client, folder, doc_id)
        except Exception as e:
            # Re-login & retry once on auth blips
            print(f"  error: {e}; relogin+retry", file=sys.stderr)
            time.sleep(2)
            client.login()
            r = pull_play(client, folder, doc_id)
        print(f"  {r['n_ra_pages']}/{r['n_pages']} pages with RA layer")
        results.append(r)
    print("=== summary ===")
    for r in results:
        print(f"{r['folder']:60s}  {r['n_ra_pages']:>3}/{r['n_pages']:>3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
