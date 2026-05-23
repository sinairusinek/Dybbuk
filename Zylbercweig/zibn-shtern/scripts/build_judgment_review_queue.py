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
_NAMEAMB  = _WORKING / "kima" / "audit_name_exact_ambiguous.tsv"
_AMBALL   = _WORKING / "kima" / "audit_ambiguity_all.tsv"
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
    "fuzzy_confidence", "entry_ids", "contexts", "mentions",
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


def build_attestation_records() -> dict[str, list[dict]]:
    """source_value → [full attestation rows] (for per-mention breakdown)."""
    out: dict[str, list[dict]] = defaultdict(list)
    for row in _read_tsv(_ATTEST, delim=","):
        sv = (row.get("source_value") or "").strip()
        if sv:
            out[sv].append(row)
    return out


_MAX_MENTIONS = 20


def _window(text: str, needle: str, w: int = 180) -> str:
    """A ±w-char window around `needle` in `text` (whole text if not found)."""
    i = text.find(needle)
    if i < 0:
        return text[: 2 * w] + ("…" if len(text) > 2 * w else "")
    a, b = max(0, i - w), min(len(text), i + len(needle) + w)
    return ("…" if a > 0 else "") + text[a:b] + ("…" if b < len(text) else "")


def build_mentions(sv: str, recs_by_sv, clusters, lex) -> list[dict]:
    """Per-mention breakdown for a spelling, grouped by source_record_id (= org
    location-variant or person entry). Each mention carries its own windowed
    Yiddish context + the QID it is currently linked to."""
    grouped: "OrderedDict[str, dict]" = OrderedDict()
    for r in recs_by_sv.get(sv, []):
        rid = (r.get("source_record_id") or "").strip()
        if not rid:
            continue
        g = grouped.setdefault(rid, {
            "rid": rid,
            "corpus": (r.get("source_corpus") or "").strip(),
            "fields": [],
            "qid": (r.get("qid") or "").strip(),
            "n": 0,
        })
        g["n"] += 1
        fld = (r.get("source_field") or "").strip()
        if fld and fld not in g["fields"]:
            g["fields"].append(fld)
        if not g["qid"] and r.get("qid"):
            g["qid"] = r["qid"].strip()

    mentions = []
    for rid, g in list(grouped.items())[:_MAX_MENTIONS]:
        ctx = ""
        for xid in _entry_ids_for_record(rid, clusters):
            txt = lex.get(xid, "")
            if txt and (sv in txt or not ctx):
                ctx = _window(txt, sv)
                if sv in txt:
                    break
        mentions.append({
            "rid": rid,
            "corpus": g["corpus"],
            "field": "/".join(g["fields"]),
            "n": g["n"],
            "qid": g["qid"],
            "ctx": ctx,
        })
    return mentions


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


# Candidate option in the ambiguity audits, e.g. "Lublin (Poland)[174→Q37333]"
# or "Międzyrzec Podlaski (Poland)[223->Q34267]". Arrow may be → or -> or =.
_OPT_RE = re.compile(r"\[(\d+)\s*[-=>→]+\s*(Q\d+|-)\]")


def _parse_options(cell: str) -> list[dict]:
    cands = []
    for part in (cell or "").split("|"):
        part = part.strip()
        m = _OPT_RE.search(part)
        if not m:
            continue
        cands.append({"kima_id": m.group(1), "rom": part[:m.start()].strip(), "heb": ""})
    return cands


def build_ambiguity_rows(path, options_col, clusters, lex, attest, places) -> list[dict]:
    """Rows from an ambiguity audit: one Yiddish spelling linked to a name that
    several Kima places share. Candidates come from the audit's options column."""
    rows = []
    for r in _read_tsv(path):
        yi = r["yiddish"].strip()
        cands = _parse_options(r.get(options_col, ""))
        label = r.get("linked_label", "").strip()
        lqid  = r.get("linked_qid", "").strip()
        entry_ids, ctxs = resolve_contexts(attest.get(yi, []), yi, clusters, lex)
        note = (f"{yi}: currently linked to {lqid}/{label}; "
                f"{len(cands)} Kima places share this name — pick the right one.")
        rows.append({
            "source_value":      yi,
            "wikidata_yi":        "",
            "english_name":       f"{label} — ambiguous: pick the right Kima place",
            "wikidata_qid":       lqid,
            "resolved_category":  "settlement",
            "match_status":       "ambiguous",
            "fuzzy_candidates":   json.dumps(cands, ensure_ascii=False),
            "fuzzy_confidence":   "",
            "entry_ids":          "|".join(entry_ids),
            "contexts":           "|".join([note, *ctxs]),
        })
    return rows


def merge_by_source_value(source_rows: list[dict]) -> list[dict]:
    """One row per spelling. Scalars: first non-empty wins (source order =
    priority). Lists (candidates / contexts / entry_ids): unioned."""
    merged: "OrderedDict[str, dict]" = OrderedDict()
    for r in source_rows:
        sv = r["source_value"]
        if sv not in merged:
            merged[sv] = {**r, "fuzzy_candidates": json.loads(r["fuzzy_candidates"])}
            continue
        m = merged[sv]
        for col in ("english_name", "wikidata_qid", "wikidata_yi", "resolved_category"):
            if not m.get(col) and r.get(col):
                m[col] = r[col]
        # union candidates by kima_id
        seen = {c["kima_id"] for c in m["fuzzy_candidates"]}
        for c in json.loads(r["fuzzy_candidates"]):
            if c["kima_id"] not in seen:
                m["fuzzy_candidates"].append(c)
                seen.add(c["kima_id"])
        # union contexts / entry_ids (preserve order, dedup)
        m["contexts"] = "|".join(dict.fromkeys(
            [c for c in (m["contexts"].split("|") + r["contexts"].split("|")) if c]))
        m["entry_ids"] = "|".join(dict.fromkeys(
            [e for e in (m["entry_ids"].split("|") + r["entry_ids"].split("|")) if e]))

    out = []
    for m in merged.values():
        m["fuzzy_candidates"] = json.dumps(m["fuzzy_candidates"][:_MAX_CANDIDATES],
                                           ensure_ascii=False)
        # cap contexts to what the page renders plus a little headroom
        m["contexts"] = "|".join(m["contexts"].split("|")[:4])
        out.append(m)
    return out


def main() -> None:
    print("Indexing Lexicon XML…")
    lex      = build_lexicon_index()
    clusters = build_cluster_index()
    attest   = build_attestation_index()
    recs     = build_attestation_records()
    places   = build_kima_places()
    print(f"  lexicon entries: {len(lex)}  clusters: {len(clusters)}  "
          f"attested spellings: {len(attest)}  kima places: {len(places)}")

    # Source order = merge priority (disambiguation richest, then punchlist,
    # then the two ambiguity audits). Overlapping spellings are merged, not
    # duplicated (union candidates + contexts).
    source_rows = (
        build_disambiguation_rows(clusters, lex, places)
        + build_punchlist_rows(clusters, lex, attest, places)
        + build_ambiguity_rows(_NAMEAMB, "kima_options", clusters, lex, attest, places)
        + build_ambiguity_rows(_AMBALL, "options", clusters, lex, attest, places)
    )
    new_rows = merge_by_source_value(source_rows)
    # Bake per-mention breakdown (feature A) — grouped by source_record_id.
    for r in new_rows:
        r["mentions"] = json.dumps(
            build_mentions(r["source_value"], recs, clusters, lex), ensure_ascii=False)
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
    multi    = [r["source_value"] for r in new_rows if len(json.loads(r["mentions"])) > 1]
    print(f"\nQueue now has {len(existing)} rows ({kept} kept + {len(new_rows)} managed).")
    print(f"Managed rows ({len(new_rows)}): {', '.join(managed)}")
    print(f"  per-mention (>1 mention): {len(multi)} → {', '.join(multi)}")
    if no_ctx:
        print(f"  ⚠ no context resolved for: {', '.join(no_ctx)}")
    if no_cands:
        print(f"  ⓘ no Kima candidates surfaced for: {', '.join(no_cands)}")


if __name__ == "__main__":
    main()
