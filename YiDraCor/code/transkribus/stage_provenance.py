"""Build a per-stage-tag provenance TSV across the 7 reviewed plays.

For each `stage` custom-tag span in the *latest* (top-of-stack) PAGE-XML on
Transkribus, walk the full transcript timeline for that page and attribute the
*current* type to one of: rule_based / LLM / RA.

Span identity = (line_id, offset, length). If a layer changed offset/length,
that is a new span and shows up as its own row.

Run from inside `code/`:
    cd code && python3.11 -m transkribus.stage_provenance

Reads/writes:
  - cache: data/<folder>/layer_diff_2026-05-31/all_layers/<pageNr>/<tsId>.xml
  - out  : data/review/stage_provenance_2026-05-31.tsv
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

from transkribus.client import TrpClient

DATE = "2026-05-31"
COLLECTION_ID = 2372172
TODAY_TOOL = "YiDraCor-annotation-pipeline"
TODAY_USER_SUBSTR = "sinai"

PIPELINE_TOOL_PREFIXES = (
    "YiDraCor",
    "apply_pi_decisions",
    "apply_collective_speakers",
    "auto_resolve",
)

ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data" / "review"
REVIEW.mkdir(parents=True, exist_ok=True)
OUT_TSV = REVIEW / f"stage_provenance_{DATE}.tsv"

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

PLAYS = [
    ("AlNaharotBavel-Amkreut&Freund1909", 820975),
    ("DerManUnterTiff", 817462),
    ("Di_seyder_nakht_Emkroyt_1908", 828503),
    ("KidushHashem", 820939),
    ("MishkeMashke-Kultur1910", 828537),
    ("Yudale_der_blinder,_Emkroyt1908", 828539),
    ("דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete", 828424),
]

CUSTOM_RE = re.compile(r"(\w+)\s*\{([^}]*)\}")


def parse_custom_stage(custom: str) -> list[tuple[int, int, str]]:
    """Return list of (offset, length, type) for stage tags only."""
    out: list[tuple[int, int, str]] = []
    for m in CUSTOM_RE.finditer(custom or ""):
        tag = m.group(1)
        if tag != "stage":
            continue
        attrs: dict[str, str] = {}
        for kv in m.group(2).split(";"):
            kv = kv.strip()
            if not kv or ":" not in kv:
                continue
            k, v = kv.split(":", 1)
            attrs[k.strip()] = v.strip()
        try:
            off = int(attrs.get("offset", "0"))
            ln = int(attrs.get("length", "0"))
        except ValueError:
            continue
        typ = attrs.get("type", "")
        out.append((off, ln, typ))
    return out


def parse_page(xml_text: str) -> dict[str, dict]:
    """Return {line_id: {text, stages:[(off,len,type)]}}."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}
    out: dict[str, dict] = {}
    for line in root.iter(f"{{{PAGE_NS}}}TextLine"):
        lid = line.get("id") or ""
        custom = line.get("custom") or ""
        text_eq = line.find(f"{{{PAGE_NS}}}TextEquiv/{{{PAGE_NS}}}Unicode")
        text = (text_eq.text or "") if text_eq is not None else ""
        out[lid] = {"text": text, "stages": parse_custom_stage(custom)}
    return out


def ts_to_date(ts) -> str:
    if ts is None:
        return ""
    try:
        if isinstance(ts, str):
            ts = int(ts)
        # Transkribus timestamps are ms epoch
        if ts > 10_000_000_000:
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


def classify_layer(tool: str, user: str, date_str: str) -> str:
    """Returns 'today_pipeline' | 'pipeline' | 'ra'."""
    tool = tool or ""
    user = user or ""
    is_pipeline_tool = any(tool.startswith(p) for p in PIPELINE_TOOL_PREFIXES)
    if (
        is_pipeline_tool
        and tool == TODAY_TOOL
        and TODAY_USER_SUBSTR in user.lower()
        and date_str == DATE
    ):
        return "today_pipeline"
    if is_pipeline_tool:
        return "pipeline"
    return "ra"


def fetch_with_cache(client: TrpClient, url: str, cache_path: Path) -> Optional[str]:
    if cache_path.exists():
        try:
            return cache_path.read_text(encoding="utf-8")
        except Exception:
            pass
    last_err = None
    for attempt in range(3):
        try:
            xml = client.fetch_transcript(url)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(xml, encoding="utf-8")
            return xml
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
            # try relogin on auth blips
            if "401" in str(e) or "403" in str(e):
                try:
                    client.login()
                except Exception:
                    pass
    print(f"  fetch failed after retries: {url}: {last_err}", file=sys.stderr)
    return None


def process_play(client: TrpClient, folder: str, doc_id: int, out_rows: list) -> dict:
    play_dir = ROOT / "data" / folder
    if not (play_dir / "page_annotated").exists():
        print(f"  skip {folder}: no page_annotated/", file=sys.stderr)
        return {"folder": folder, "skipped": True}
    cache_root = play_dir / f"layer_diff_{DATE}" / "all_layers"
    cache_root.mkdir(parents=True, exist_ok=True)

    fd = client.fulldoc(COLLECTION_ID, doc_id)
    pages = fd.get("pageList", {}).get("pages", [])
    n_pages = 0
    n_ra_above_today_pages = 0
    ra_above_pages: list[int] = []

    for p in pages:
        page_nr = int(p["pageNr"])
        ts_list = p.get("tsList", {}).get("transcripts", []) or []
        if not ts_list:
            continue
        # sort ascending by timestamp
        ts_sorted = sorted(ts_list, key=lambda t: t.get("timestamp", 0))
        page_cache = cache_root / f"{page_nr:04d}"
        # batched fetch with parallelism=4
        to_fetch = []
        for t in ts_sorted:
            tsid = t.get("tsId")
            url = t.get("url")
            if not tsid or not url:
                continue
            cp = page_cache / f"{tsid}.xml"
            if not cp.exists():
                to_fetch.append((t, cp))
        # fetch in batches of 4 with small backoff
        for i in range(0, len(to_fetch), 4):
            batch = to_fetch[i:i+4]
            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = {ex.submit(fetch_with_cache, client, t["url"], cp): (t, cp) for (t, cp) in batch}
                for fu in as_completed(futs):
                    fu.result()
            time.sleep(0.4)

        # parse each cached xml in chronological order
        per_layer: list[tuple[dict, dict[str, dict]]] = []  # (transcript meta, parsed)
        for t in ts_sorted:
            tsid = t.get("tsId")
            cp = page_cache / f"{tsid}.xml"
            if not cp.exists():
                continue
            try:
                parsed = parse_page(cp.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"  parse fail {cp}: {e}", file=sys.stderr)
                continue
            per_layer.append((t, parsed))

        if not per_layer:
            continue

        # last layer = current state
        last_t, last_parsed = per_layer[-1]
        last_date = ts_to_date(last_t.get("timestamp"))
        last_class = classify_layer(last_t.get("toolName", ""), last_t.get("userName", ""), last_date)
        # Was today's pipeline push the top? If top is RA and there exists a today_pipeline layer below, mark parent_check
        ra_above_today = False
        if last_class == "ra":
            for tm, _ in per_layer[:-1]:
                d = ts_to_date(tm.get("timestamp"))
                if classify_layer(tm.get("toolName", ""), tm.get("userName", ""), d) == "today_pipeline":
                    ra_above_today = True
                    break
        if ra_above_today:
            n_ra_above_today_pages += 1
            ra_above_pages.append(page_nr)

        # Build timeline per span identity (line_id, off, len)
        # For each layer, collect map { (lid, off, len): type }
        layer_maps: list[tuple[dict, dict[tuple[str, int, int], str]]] = []
        for tm, parsed in per_layer:
            m: dict[tuple[str, int, int], str] = {}
            for lid, info in parsed.items():
                for (off, ln, typ) in info["stages"]:
                    m[(lid, off, ln)] = typ
            layer_maps.append((tm, m))

        # current spans = those present in last layer
        cur_map = layer_maps[-1][1]
        for (lid, off, ln), cur_type in cur_map.items():
            # build timeline of this span across layers (where present), compact consecutive same-type
            timeline: list[tuple[dict, str]] = []
            for tm, m in layer_maps:
                if (lid, off, ln) in m:
                    typ = m[(lid, off, ln)]
                    if not timeline or timeline[-1][1] != typ:
                        timeline.append((tm, typ))
            if not timeline:
                continue
            setter_t, _ = timeline[-1]
            prior_type = timeline[-2][1] if len(timeline) >= 2 else ""
            prior_tool = timeline[-2][0].get("toolName", "") if len(timeline) >= 2 else ""
            setter_date = ts_to_date(setter_t.get("timestamp"))
            setter_class = classify_layer(setter_t.get("toolName", ""), setter_t.get("userName", ""), setter_date)
            if setter_class == "today_pipeline":
                source = "rule_based"
            elif setter_class == "pipeline":
                source = "LLM"
            else:
                source = "RA"

            line_info = last_parsed.get(lid, {})
            text = line_info.get("text", "") or ""
            text_span = text[off:off+ln] if (0 <= off and ln > 0 and off + ln <= len(text)) else ""
            full_line = text[:200]

            out_rows.append({
                "play": folder,
                "page": page_nr,
                "line_id": lid,
                "offset": off,
                "length": ln,
                "current_type": cur_type,
                "text_span": text_span,
                "full_line": full_line,
                "source": source,
                "source_tool": setter_t.get("toolName", ""),
                "source_user": setter_t.get("userName", ""),
                "source_tsid": setter_t.get("tsId", ""),
                "source_date": setter_date,
                "prior_type": prior_type,
                "prior_source_tool": prior_tool,
                "n_layers_touched": len(timeline),
                "parent_check": "RA_layer_above_today" if ra_above_today else "",
            })
        n_pages += 1

    return {
        "folder": folder,
        "pages": n_pages,
        "ra_above_today_pages": n_ra_above_today_pages,
        "ra_above_today_page_nrs": ra_above_pages,
    }


def main() -> int:
    client = TrpClient.from_env()
    out_rows: list[dict] = []
    summaries = []
    for folder, doc_id in PLAYS:
        print(f"--- {folder} (doc {doc_id}) ---")
        try:
            s = process_play(client, folder, doc_id, out_rows)
        except Exception as e:
            print(f"  ERROR processing {folder}: {e}; relogin+retry", file=sys.stderr)
            time.sleep(2)
            client.login()
            s = process_play(client, folder, doc_id, out_rows)
        summaries.append(s)
        if not s.get("skipped"):
            print(f"  pages={s['pages']}  ra_above_today={s['ra_above_today_pages']}")

    fieldnames = [
        "play", "page", "line_id", "offset", "length", "current_type",
        "text_span", "full_line",
        "source", "source_tool", "source_user", "source_tsid", "source_date",
        "prior_type", "prior_source_tool",
        "n_layers_touched", "parent_check",
    ]
    with OUT_TSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    # console summary
    print("\n=== TOTALS ===")
    print(f"Total stage tags: {len(out_rows)}")
    by_source = Counter(r["source"] for r in out_rows)
    for s, n in by_source.most_common():
        print(f"  {s}: {n}")
    by_src_type = Counter((r["source"], r["current_type"]) for r in out_rows)
    print("\nTop (source, current_type) combos:")
    for (s, t), n in by_src_type.most_common(20):
        print(f"  {s:12s} {t:24s}  {n}")

    print("\nPages where RA edited on top of today's pipeline push:")
    any_ra = False
    for s in summaries:
        if s.get("skipped"):
            continue
        if s["ra_above_today_pages"]:
            any_ra = True
            print(f"  {s['folder']}: pages={s['ra_above_today_page_nrs']}")
    if not any_ra:
        print("  (none)")

    print(f"\nWrote {OUT_TSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
