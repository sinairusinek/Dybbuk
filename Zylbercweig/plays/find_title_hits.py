"""A2 — Deterministic sweep for play-title mentions across the lexicon.

Matches the normalized אָדער-split title segments from plays_db.tsv against
(a) all entry texts in people/entry_texts.tsv and (b) all mention relation
sentences in people/mentions_all.tsv (read via load_mentions_with_host).

Three tiers, no LLM:
  A   quoted span, exact normalized match
  A'  quoted span, fuzzy (rapidfuzz ratio>=88 or token_set>=92 on loose keys)
  B   unquoted substring (multi-token segments only, word-boundary checked)

Precision guards: corpus document frequency per segment; generic segments
(high DF / single-token / short / stoplisted) disable tier B and downgrade
tier A to `candidate` unless the author's surname co-occurs in the entry.

Outputs (on --execute): play_title_hits.tsv; also fills df_corpus /
ambiguity_flag back into plays_db.tsv. Dry-run prints tier/DF stats for
threshold tuning.

Usage:
    python3.11 find_title_hits.py                 # dry-run stats
    python3.11 find_title_hits.py --execute
    python3.11 find_title_hits.py --df-threshold 40 --execute
"""
from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from collections import defaultdict

from rapidfuzz import fuzz

import plays_common as pc

sys.path.insert(0, str(pc.PEOPLE_DIR))
import people_common  # noqa: E402

HITS_FIELDS = [
    "hit_id", "play_id", "author_db_id", "matched_segment", "matched_surface",
    "tier", "source", "person_id", "volume", "heading", "mention_id",
    "char_start", "char_end", "context_before", "context_after",
    "author_comention", "ambiguity_flag", "hit_status",
]

QUOTE_OPEN = "„«"
QUOTE_ANY = "„“”«»\""
MAX_QUOTED_SPAN = 120
CONTEXT_CHARS = 400
NEAR_CHARS = 600

# Hand stoplist of segments too generic to trust even below the DF threshold.
STOPLIST: set[str] = set()


def norm_with_map(raw: str) -> tuple[str, list[int]]:
    """plays_common.norm_yiddish, but returns (norm, map norm_idx -> raw_idx)."""
    chars: list[str] = []
    idx: list[int] = []
    for i, ch in enumerate(raw):
        d = unicodedata.normalize("NFD", ch)
        d = "".join(c for c in d if unicodedata.category(c) != "Mn")
        for k, v in {"װ": "וו", "ױ": "וי", "ײ": "יי"}.items():
            d = d.replace(k, v)
        d = d.translate(str.maketrans("ךםןףץ", "כמנפצ"))
        d = re.sub(r"[^0-9א-ת]", " ", d)
        for c in d:
            chars.append(c)
            idx.append(i)
    # squeeze whitespace, keep map
    norm_chars: list[str] = []
    norm_idx: list[int] = []
    for c, i in zip(chars, idx):
        if c == " " and (not norm_chars or norm_chars[-1] == " "):
            continue
        norm_chars.append(c)
        norm_idx.append(i)
    while norm_chars and norm_chars[-1] == " ":
        norm_chars.pop()
        norm_idx.pop()
    return "".join(norm_chars), norm_idx


def quoted_spans(raw: str) -> list[tuple[int, int]]:
    """(start, end) raw-offset spans of quoted text. Handles „…“ pairs and
    plain-" toggling; spans longer than MAX_QUOTED_SPAN are discarded."""
    spans = []
    open_pos = -1
    for m in re.finditer(f"[{QUOTE_ANY}]", raw):
        pos, ch = m.start(), m.group()
        if open_pos < 0:
            if ch in QUOTE_OPEN or ch == '"':
                open_pos = pos + 1
        else:
            if pos - open_pos <= MAX_QUOTED_SPAN:
                spans.append((open_pos, pos))
                open_pos = -1
            else:
                # runaway quote: treat this char as a fresh opener
                open_pos = pos + 1 if (ch in QUOTE_OPEN or ch == '"') else -1
    return spans


def word_bounded(hay_padded: str, seg: str) -> list[int]:
    """Start offsets (in the unpadded string) of word-boundary matches."""
    out, start = [], 0
    needle = f" {seg} "
    while True:
        i = hay_padded.find(needle, start)
        if i < 0:
            return out
        out.append(i)  # hay_padded has 1 leading pad char, so i == unpadded start of seg... adjusted by caller
        start = i + 1


class SegmentIndex:
    def __init__(self, plays: list[dict]):
        self.by_seg: dict[str, list[dict]] = defaultdict(list)
        for p in plays:
            for seg in (p["title_segments_norm"] or "").split("|"):
                if seg:
                    self.by_seg[seg].append(p)
        self.segments = sorted(self.by_seg)
        self.loose = {s: pc.loose_key(s) for s in self.segments}

    def match_span(self, span_raw: str) -> list[tuple[str, str]]:
        """Match one quoted span. Returns [(segment, tier)] where tier is
        A (exact, incl. loose-article and span-side אדער-split), A' (fuzzy
        ratio>=88), or P (span is a token-prefix of a longer title — the
        short-title citation style; inherently ambiguous, candidate-only)."""
        span_norm = pc.norm_yiddish(span_raw)
        if len(span_norm) < 4:
            return []
        if span_norm in self.by_seg:
            return [(span_norm, "A")]
        loose = pc.loose_key(span_norm)
        if loose in self.by_seg:
            return [(loose, "A")]
        part_hits = [(s, "A") for s in pc.title_segments(span_raw)
                     if s != span_norm and s in self.by_seg]
        if part_hits:
            return part_hits
        best, best_r = None, 0.0
        for seg in self.segments:
            r = max(fuzz.ratio(span_norm, seg), fuzz.ratio(loose, self.loose[seg]))
            if r >= 88 and r > best_r:
                best, best_r = seg, r
        if best:
            return [(best, "A'")]
        stoks = loose.split()
        out = []
        if stoks and len(loose) >= 4:
            for seg in self.segments:
                ltoks = self.loose[seg].split()
                if len(ltoks) > len(stoks) and ltoks[:len(stoks)] == stoks:
                    out.append((seg, "P"))
        return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--df-threshold", type=int, default=40,
                    help="segment document frequency above which it is generic")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    plays = pc.load_plays_db()
    if not plays:
        raise SystemExit("plays_db.tsv missing — run build_plays_db.py --execute first")
    index = SegmentIndex(plays)
    print(f"segments indexed: {len(index.segments)} from {len(plays)} plays")

    entries = pc.read_tsv(pc.ENTRY_TEXTS_TSV)
    print(f"entries: {len(entries)}")

    # Pass 1: per-entry normalization, DF, raw hit collection.
    df: dict[str, int] = defaultdict(int)
    raw_hits: list[dict] = []
    entry_norm: dict[str, tuple[str, list[int], str]] = {}  # person_id -> (norm, map, raw)

    for e in entries:
        raw = e["entry_text"] or ""
        norm, nmap = norm_with_map(raw)
        entry_norm[e["person_id"]] = (norm, nmap, raw)
        padded = f" {norm} "
        seen_segs = set()
        for seg in index.segments:
            if f" {seg} " in padded:
                seen_segs.add(seg)
        for seg in seen_segs:
            df[seg] += 1

    def comention_flag(norm: str, author_db_id: str, norm_pos: int | None) -> str:
        pat = pc.AUTHOR_SURNAME_PATTERNS.get(author_db_id)
        if not pat:
            return "none"
        if norm_pos is not None:
            lo, hi = max(0, norm_pos - NEAR_CHARS), norm_pos + NEAR_CHARS
            if pat.search(norm[lo:hi]):
                return "near"
        return "entry" if pat.search(norm) else "none"

    def add_hit(play: dict, seg: str, tier: str, source: str, person_id: str,
                volume: str, heading: str, mention_id: str, raw: str,
                start: int, end: int, norm: str, norm_pos: int | None) -> None:
        raw_hits.append({
            "play_id": play["play_id"],
            "author_db_id": play["author_db_id"],
            "matched_segment": seg,
            "matched_surface": raw[start:end],
            "tier": tier,
            "source": source,
            "person_id": person_id,
            "volume": volume,
            "heading": heading,
            "mention_id": mention_id,
            "char_start": start,
            "char_end": end,
            "context_before": raw[max(0, start - CONTEXT_CHARS):start],
            "context_after": raw[end:end + CONTEXT_CHARS],
            "author_comention": comention_flag(norm, play["author_db_id"], norm_pos),
        })

    # Pass 2: entry texts — quoted tiers + unquoted tier B.
    n_spans = 0
    for e in entries:
        norm, nmap, raw = entry_norm[e["person_id"]]
        pid, vol, head = e["person_id"], e["volume"], e["heading"]
        covered: set[tuple[str, int]] = set()  # (play_id, approx raw start)

        for s_raw, e_raw in quoted_spans(raw):
            n_spans += 1
            for seg, tier in index.match_span(raw[s_raw:e_raw]):
                for p in index.by_seg[seg]:
                    add_hit(p, seg, tier, "entry_text", pid, vol, head, "",
                            raw, s_raw, e_raw, norm, None)
                    covered.add((p["play_id"], s_raw // 40))

        padded = f" {norm} "
        for seg in index.segments:
            if " " not in seg or len(seg) < 6:
                continue
            start = 0
            needle = f" {seg} "
            while True:
                i = padded.find(needle, start)
                if i < 0:
                    break
                start = i + 1
                norm_start = i  # padded has 1 leading char; i == norm offset of seg start
                if norm_start >= len(nmap):
                    break
                raw_start = nmap[norm_start]
                raw_end = nmap[min(norm_start + len(seg) - 1, len(nmap) - 1)] + 1
                for p in index.by_seg[seg]:
                    if (p["play_id"], raw_start // 40) in covered:
                        continue
                    add_hit(p, seg, "B", "entry_text", pid, vol, head, "",
                            raw, raw_start, raw_end, norm, norm_start)

    print(f"quoted spans examined: {n_spans}")

    # Pass 3: mention relation sentences.
    entry_flag_cache: dict[tuple[str, str], str] = {}
    mrows = people_common.load_mentions_with_host()
    n_sent = 0
    for m in mrows:
        sent = (m.get("relation_sentence") or "").strip()
        if len(sent) < 6:
            continue
        n_sent += 1
        sent_norm = pc.norm_yiddish(sent)
        host_pid = m.get("host_person_id", "")
        for s_raw, e_raw in quoted_spans(sent):
            hits = [(seg, tier, p) for seg, tier in index.match_span(sent[s_raw:e_raw])
                    for p in index.by_seg[seg]]
            for seg, tier, p in hits:
                key = (host_pid, p["author_db_id"])
                if key not in entry_flag_cache:
                    en = entry_norm.get(host_pid)
                    entry_flag_cache[key] = (
                        "entry" if en and pc.AUTHOR_SURNAME_PATTERNS[p["author_db_id"]].search(en[0])
                        else "none")
                flag = ("near" if pc.AUTHOR_SURNAME_PATTERNS[p["author_db_id"]].search(sent_norm)
                        else entry_flag_cache[key])
                raw_hits.append({
                    "play_id": p["play_id"], "author_db_id": p["author_db_id"],
                    "matched_segment": seg, "matched_surface": sent[s_raw:e_raw],
                    "tier": tier, "source": "mention_sentence",
                    "person_id": host_pid, "volume": m.get("host_volume", ""),
                    "heading": m.get("host_heading_filled", ""),
                    "mention_id": m.get("mention_id", ""),
                    "char_start": s_raw, "char_end": e_raw,
                    "context_before": sent[:s_raw][-CONTEXT_CHARS:],
                    "context_after": sent[e_raw:][:CONTEXT_CHARS],
                    "author_comention": flag,
                })
    print(f"mention sentences examined: {n_sent}")

    # Statuses via DF-informed generic flag.
    def is_generic(seg: str) -> bool:
        return (df.get(seg, 0) > args.df_threshold or " " not in seg
                or len(seg) < 5 or seg in STOPLIST)

    # Homonym risk: a segment hit often across the corpus but rarely with the
    # author co-mentioned is likely a famous same-titled play by ANOTHER
    # playwright (e.g. „דער דיבוק" = Ansky, not Hurwitz's homonym).
    seg_hits: dict[str, int] = defaultdict(int)
    seg_com: dict[str, int] = defaultdict(int)
    for h in raw_hits:
        seg_hits[h["matched_segment"]] += 1
        if h["author_comention"] != "none":
            seg_com[h["matched_segment"]] += 1

    def homonym_risk(seg: str) -> bool:
        n = seg_hits[seg]
        return n >= 8 and seg_com[seg] / n < 0.25

    final_hits: list[dict] = []
    for h in raw_hits:
        seg = h["matched_segment"]
        generic = is_generic(seg)
        risky = homonym_risk(seg)
        if generic and h["tier"] == "B":
            continue
        if h["tier"] == "P":
            status = "accept" if h["author_comention"] == "near" else "candidate"
        elif h["tier"] == "B" or generic or risky:
            status = "accept" if h["author_comention"] != "none" else "candidate"
        else:
            status = "accept"
        flags = [f for f, on in (("generic", generic), ("homonym_risk", risky),
                                 ("prefix", h["tier"] == "P")) if on]
        h["ambiguity_flag"] = "|".join(flags)
        h["hit_status"] = status
        final_hits.append(h)

    final_hits.sort(key=lambda h: (h["source"], h["person_id"], int(h["char_start"])))
    for i, h in enumerate(final_hits, 1):
        h["hit_id"] = f"HIT-{i:05d}"

    # Stats.
    by_tier = defaultdict(int)
    by_status = defaultdict(int)
    entries_with = set()
    for h in final_hits:
        by_tier[h["tier"]] += 1
        by_status[h["hit_status"]] += 1
        if h["source"] == "entry_text":
            entries_with.add(h["person_id"])
    print(f"\nhits: {len(final_hits)}  tiers: {dict(by_tier)}  status: {dict(by_status)}")
    print(f"entries with >=1 hit: {len(entries_with)}")
    top_df = sorted(df.items(), key=lambda kv: -kv[1])[:15]
    print("top-DF segments:")
    for seg, n in top_df:
        mark = " GENERIC" if is_generic(seg) else ""
        print(f"  {n:4d}  {seg}{mark}")
    n_generic = sum(1 for s in index.segments if is_generic(s))
    print(f"generic segments: {n_generic}/{len(index.segments)}")

    if not args.execute:
        print("\ndry-run — pass --execute to write play_title_hits.tsv and update plays_db")
        return

    pc.write_tsv(pc.TITLE_HITS_TSV, final_hits, HITS_FIELDS)
    for p in plays:
        segs = [s for s in (p["title_segments_norm"] or "").split("|") if s]
        p["df_corpus"] = str(max((df.get(s, 0) for s in segs), default=0))
        p["ambiguity_flag"] = "generic" if segs and is_generic(segs[0]) else ""
    from build_plays_db import PLAYS_FIELDS
    pc.write_tsv(pc.PLAYS_DB_TSV, plays, PLAYS_FIELDS)
    print(f"wrote {pc.TITLE_HITS_TSV} ({len(final_hits)}); updated df/ambiguity in plays_db.tsv")


if __name__ == "__main__":
    main()
