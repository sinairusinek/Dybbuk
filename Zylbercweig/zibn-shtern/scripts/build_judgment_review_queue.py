#!/usr/bin/env python3
"""
Append the two "workable-now" judgment queues to the Kimatch Zylbercweig
review queue, baking the full Yiddish lexicon entry text into each row's
`contexts` so the reviewer sees the highlighted attestation in the deployed
app (no runtime access to the Dybbuk Lexicon XML required).

Sources (Dybbuk / zibn-shtern):
  data/working/kima/review_disambiguation.tsv          — corpus disambiguation (per record)
  data/working/kima/audit_translit_review_punchlist.tsv — historical-identity calls (per spelling)

Context resolution:
  - org records  (ORG-…):  cluster_id → member xml:id(s) via organizations_clustered.tsv,
                           keeping entries whose text contains the spelling.
  - person records (N-facs…): strip the volume prefix → bare xml:id.
  - punchlist (no record id): look up source_record_id(s) in toponyms_attestations.csv.
  Entry text comes from a one-pass index over The Lexicon/*.xml.

Both sets are judgment calls → match_status="ambiguous" (lands in the page's
Ambiguous filter). Disambiguation rows collapse per spelling (the backend keys
one decision per source_value). fuzzy_candidates is empty — the reviewer uses
Kima search or the "Map to a Wikidata entity" action.

Idempotent: spellings already present in the queue are skipped.
"""
from __future__ import annotations

import csv
import glob
import json
import re
import xml.etree.ElementTree as ET
from collections import OrderedDict, defaultdict
from pathlib import Path

csv.field_size_limit(10**7)

# ── paths ──────────────────────────────────────────────────────────────────────
_ZIBN     = Path(__file__).resolve().parents[1]
_ZYLB     = _ZIBN.parent
_WORKING  = _ZIBN / "data" / "working"
_DISAMB   = _WORKING / "kima" / "review_disambiguation.tsv"
_PUNCH    = _WORKING / "kima" / "audit_translit_review_punchlist.tsv"
_ATTEST   = _WORKING / "toponyms_attestations.csv"
_CLUSTERS = _ZYLB / "organizations" / "organizations_clustered.tsv"
_LEXICON  = _ZYLB / "The Lexicon"

_KIMATCH_LOCAL = Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
_KIMATCH  = _KIMATCH_LOCAL if _KIMATCH_LOCAL.exists() else Path.cwd()
_QUEUE    = _KIMATCH / "data" / "zylbercweig" / "kimatch_review_full.tsv"
_KIMA_CSV = _KIMATCH / "20250126KimaPlacesCSVx.csv"
_MAX_CANDIDATES = 8

_COLS = [
    "source_value", "wikidata_yi", "english_name", "wikidata_qid",
    "resolved_category", "match_status", "fuzzy_candidates",
    "fuzzy_confidence", "entry_ids", "contexts",
]

_TEI   = "http://www.tei-c.org/ns/1.0"
_XMLID = "{http://www.w3.org/XML/1998/namespace}id"
_QID_RE = re.compile(r"\bQ\d+\b")
_VOL_PREFIX = re.compile(r"^\d+-")          # "2-facs_…" → "facs_…"
_MAX_CTX = 3                                 # page renders the first 3


def _qid(text: str) -> str:
    m = _QID_RE.search(text or "")
    return m.group(0) if m else ""


def _read_tsv(path: Path, delim: str = "\t") -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh, delimiter=delim))


# ── indexes ─────────────────────────────────────────────────────────────────────
def build_lexicon_index() -> dict[str, str]:
    """xml:id → full entry text (whitespace-collapsed; longest wins on dup)."""
    idx: dict[str, str] = {}
    for fn in glob.glob(str(_LEXICON / "*.xml")):
        try:
            tree = ET.parse(fn)
        except ET.ParseError:
            continue
        for el in tree.getroot().iter(f"{{{_TEI}}}div"):
            xid = el.get(_XMLID)
            if not xid:
                continue
            txt = re.sub(r"\s+", " ", " ".join(el.itertext())).strip()
            if len(txt) > len(idx.get(xid, "")):
                idx[xid] = txt
    return idx


def build_cluster_index() -> dict[str, list[str]]:
    """cluster_id → [member xml:id]."""
    out: dict[str, list[str]] = defaultdict(list)
    with _CLUSTERS.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            cid = (row.get("cluster_id") or "").strip()
            xid = (row.get("_ - xml:id") or "").strip()
            if cid and xid:
                out[cid].append(xid)
    return out


def build_attestation_index() -> dict[str, list[str]]:
    """source_value → [source_record_id] (from the attestation spine)."""
    out: dict[str, list[str]] = defaultdict(list)
    for row in _read_tsv(_ATTEST, delim=","):
        sv  = (row.get("source_value") or "").strip()
        rid = (row.get("source_record_id") or "").strip()
        if sv and rid and rid not in out[sv]:
            out[sv].append(rid)
    return out


# ── Kima candidates ──────────────────────────────────────────────────────────────
def build_kima_places() -> list[dict]:
    """Minimal Kima place records: id, rom, heb, qid (same CSV the backend loads)."""
    out = []
    for row in _read_tsv(_KIMA_CSV, delim=","):
        out.append({
            "kima_id": (row.get("id") or "").strip(),
            "rom":     (row.get("primary_rom_full") or "").strip(),
            "heb":     (row.get("primary_heb_full") or "").strip(),
            "qid":     (row.get("WikiData_Id") or "").strip(),
        })
    return out


_WORD_RE = re.compile(r"[A-Za-z]+")


def _english_base(current_link: str, question: str) -> str:
    """Head place-name token, e.g. 'Q771572/Williamsburg (Brooklyn)' → 'Williamsburg',
    '(unlinked)' + 'Troy (which?)…' → 'Troy'."""
    label = current_link.split("/", 1)[1] if "/" in current_link else ""
    src = label if (label and "unlinked" not in label.lower()) else question
    m = _WORD_RE.search(src)
    return m.group(0) if m else ""


def kima_candidates(base: str, current_qid: str, places: list[dict]) -> list[dict]:
    """Kima records whose romanized name contains `base` as a whole word.
    Currently-linked QID first; capped. Returns [{kima_id, rom, heb}]."""
    if len(base) < 3:          # too short / diacritic-truncated to search safely
        return []
    pat = re.compile(rf"\b{re.escape(base)}\b", re.IGNORECASE)
    hits = [p for p in places if p["kima_id"] and pat.search(p["rom"])]
    hits.sort(key=lambda p: (current_qid == "" or p["qid"] != current_qid))
    seen, out = set(), []
    for p in hits:
        if p["kima_id"] in seen:
            continue
        seen.add(p["kima_id"])
        out.append({"kima_id": p["kima_id"], "rom": p["rom"], "heb": p["heb"]})
        if len(out) >= _MAX_CANDIDATES:
            break
    return out


# ── context resolution ───────────────────────────────────────────────────────────
def _entry_ids_for_record(rid: str, clusters: dict[str, list[str]]) -> list[str]:
    """Map a source_record_id to one or more lexicon xml:ids."""
    if rid.startswith("ORG-"):
        return clusters.get(rid, [])
    return [_VOL_PREFIX.sub("", rid)]          # person record


def resolve_contexts(
    record_ids: list[str],
    spelling: str,
    clusters: dict[str, list[str]],
    lex: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Return (entry_ids_used, context_texts) — entries containing the spelling
    first, capped to _MAX_CTX."""
    seen: set[str] = set()
    with_spelling: list[tuple[str, str]] = []
    without: list[tuple[str, str]] = []
    for rid in record_ids:
        for xid in _entry_ids_for_record(rid, clusters):
            if xid in seen:
                continue
            seen.add(xid)
            txt = lex.get(xid, "")
            if not txt:
                continue
            (with_spelling if spelling in txt else without).append((xid, txt))
    chosen = (with_spelling or without)[:_MAX_CTX]
    return [x for x, _ in chosen], [t for _, t in chosen]


# ── builders ─────────────────────────────────────────────────────────────────────
def build_disambiguation_rows(clusters, lex, places) -> list[dict]:
    grouped: "OrderedDict[str, dict]" = OrderedDict()
    for r in _read_tsv(_DISAMB):
        yi = r["yiddish"].strip()
        g = grouped.setdefault(yi, {
            "question": r["question"].strip(),
            "current_link": r["current_link"].strip(),
            "qid": _qid(r["current_link"]),
            "fields": set(),
            "records": [],
        })
        if r["source_field"].strip():
            g["fields"].add(r["source_field"].strip())
        rid = r["source_record_id"].strip()
        if rid and rid not in g["records"]:
            g["records"].append(rid)

    rows = []
    for yi, g in grouped.items():
        entry_ids, ctxs = resolve_contexts(g["records"], yi, clusters, lex)
        base = _english_base(g["current_link"], g["question"])
        cands = kima_candidates(base, g["qid"], places)
        rows.append({
            "source_value":      yi,
            "wikidata_yi":        "",
            "english_name":       g["question"],
            "wikidata_qid":       g["qid"],
            "resolved_category":  "country" if g["fields"] & {"countries"} else "settlement",
            "match_status":       "ambiguous",
            "fuzzy_candidates":   json.dumps(cands, ensure_ascii=False),
            "fuzzy_confidence":   "",
            "entry_ids":          "|".join(entry_ids),
            "contexts":           "|".join(ctxs),
        })
    return rows


def build_punchlist_rows(clusters, lex, attest, places) -> list[dict]:
    rows = []
    for r in _read_tsv(_PUNCH):
        yi = r["yiddish"].strip()
        record_ids = attest.get(yi, [])
        entry_ids, ctxs = resolve_contexts(record_ids, yi, clusters, lex)
        note = (f"{yi}: {r['note'].strip()} "
                f"(grade {r['grade'].strip()}; hypothesis: {r['hypothesis'].strip()})")
        base = _english_base("", r["linked_label"].strip())
        cands = kima_candidates(base, r["linked_qid"].strip(), places)
        rows.append({
            "source_value":      yi,
            "wikidata_yi":        "",
            "english_name":       f"{r['linked_label'].strip()} — {r['hypothesis'].strip()}",
            "wikidata_qid":       r["linked_qid"].strip(),
            "resolved_category":  "settlement",
            "match_status":       "ambiguous",
            "fuzzy_candidates":   json.dumps(cands, ensure_ascii=False),
            "fuzzy_confidence":   "",
            "entry_ids":          "|".join(entry_ids),
            "contexts":           "|".join([note, *ctxs]),
        })
    return rows


def main() -> None:
    print("Indexing Lexicon XML…")
    lex      = build_lexicon_index()
    clusters = build_cluster_index()
    attest   = build_attestation_index()
    places   = build_kima_places()
    print(f"  lexicon entries: {len(lex)}  clusters: {len(clusters)}  "
          f"attested spellings: {len(attest)}  kima places: {len(places)}")

    new_rows = (build_disambiguation_rows(clusters, lex, places)
                + build_punchlist_rows(clusters, lex, attest, places))
    managed = {r["source_value"] for r in new_rows}

    # Replace any previously-written managed rows (idempotent); keep everything else.
    existing = [r for r in _read_tsv(_QUEUE) if r["source_value"].strip() not in managed]
    kept = len(existing)
    existing.extend(new_rows)

    with _QUEUE.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=_COLS, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(existing)

    no_ctx   = [r["source_value"] for r in new_rows if not r["contexts"].strip()]
    no_cands = [r["source_value"] for r in new_rows if r["fuzzy_candidates"] == "[]"]
    print(f"\nQueue now has {len(existing)} rows ({kept} kept + {len(new_rows)} managed).")
    print(f"Managed rows ({len(new_rows)}): {', '.join(managed)}")
    if no_ctx:
        print(f"  ⚠ no context resolved for: {', '.join(no_ctx)}")
    if no_cands:
        print(f"  ⓘ no Kima candidates surfaced for: {', '.join(no_cands)}")


if __name__ == "__main__":
    main()
