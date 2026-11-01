#!/usr/bin/env python3
"""Per-fact play attribution guard for the Lateiner/Hurwitz plays KG.

Play links are adjudicated per *surface* (kg_link_review.tsv), so one decision
for „דיבוק" used to send every Lexicon Dybbuk — Ansky's 1920 play, Shomer's
Goldfadn pastiche — to Lateiner's comedy PL-0133. And surfaces adjudicated
GEMINI_NEW_PLAY / REJECT (other authors' plays: Hirshbein, Pinski, Ansky …)
were minted into the graph as play:NEW-* nodes. Both are wrong for an L/H
graph: plays by other authors belong to the future general Zylbercweig graph.

This module decides, for one fact row, whether its play is an L/H play:

  1. play_fact_attribution.tsv — curated per-fact verdicts (wins outright)
  2. an author named next to the title in the evidence sentence:
       L/H author ("לאַטיינערס „דבוק"")            -> LH
       another author known to have written a work with this title
       (works_full2.tsv from the editions extraction, or play_homonyms.tsv)
                                                    -> OTHER
  2b. the title stands in a list of the entry subject's own plays
       („פֿירט אויף זיינע פּיעסן: … „דאָס יידישע האַרץ"" in Gebil's entry is
       Gebil's play) — unless the subject is Lateiner/Hurwitz or one of them
       is named in the sentence                     -> OTHER
  3. play_homonyms.tsv rules for titles shared with another author's play:
       a character of the other play / of the L/H play, a date on or after
       the other play's first year, else the row's default verdict
  4. otherwise KEEP (no evidence against the L/H link)

Facts on surfaces that resolved to no L/H play are not minted any more; when
the evidence names the L/H author next to a title that fuzzy-matches one of
that author's plays they are rescued onto it (Lager as „פאלעק" in
„לאַטיינערס „דבוק"" -> PL-0133). Everything excluded is written to
kg/excluded_play_facts.tsv by build_kg.py so it is kept for the general graph.

Run directly for a dry diagnostic:  python3.11 play_attribution.py
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict

from rapidfuzz import fuzz

import plays_common as pc

FACT_ATTRIBUTION_TSV = pc.HERE / "play_fact_attribution.tsv"
HOMONYMS_TSV = pc.HERE / "play_homonyms.tsv"
WORKS_TSV = pc.HERE / "editions" / "works_full2.tsv"

_QUOTE_OPEN = "„\"“״'‘"
_TOKEN = re.compile(r"\S+")
_POSSESSIVE = re.compile(r"(?:ס|עס)$")
RESCUE_FUZZY = 85
# "his/her/their (new) plays / one-acters" — plural list openers only, the
# title must follow. Singular "זיין פּיעסע" is too ambiguous: in Rosenfeld's
# entry "זיין פּיעסע „גלות רוסלאַנד"" is Lateiner's play.
_OWN_PLAYS = re.compile(r"\b(?:זיינע|אירע|זייערע)\s+(?:ניוע\s+|נייע\s+|אייגענע\s+)?"
                        r"(?:פיעסנ|פיעסע|איינאקטערס|אפערעטעס|שטיקער)\b")
_STOP = {"דער", "די", "דאס", "דעם", "פון", "אינ", "און", "מיט", "ביי"}


def _surname_keys(heading: str) -> set[str]:
    """Normalized surname key of a surname-first lexicon heading ('פּינסקי, דוד',
    'מאָזער בנציון [יעקב הערצאָג]'). Bracketed civil names are skipped: ליבּין's
    bracket says הוּרוויטש, which would read as a Hurwitz cue."""
    if "—" in heading or len(heading) > 60:
        return set()  # a description the extractor put in the author slot
    toks = pc.norm_yiddish(heading.partition("[")[0].split(",")[0]).split()
    return {toks[0]} if toks and len(toks[0]) >= 3 and toks[0] not in _STOP else set()


def _split(cell: str) -> list[str]:
    return [x.strip() for x in (cell or "").split("|") if x.strip()]


class Attribution:
    def __init__(self) -> None:
        self.plays = {p["play_id"]: p for p in pc.load_plays_db()}
        self.lh_by_author: dict[str, list[dict]] = defaultdict(list)
        self.seg_to_play: dict[str, set[str]] = defaultdict(set)
        for p in self.plays.values():
            self.lh_by_author[p["author_db_id"]].append(p)
            for s in _split(p["title_segments_norm"]):
                self.seg_to_play[s].add(p["play_id"])
        self.overrides = {r["fact_id"]: r for r in pc.read_tsv(FACT_ATTRIBUTION_TSV)}
        self.homonyms: dict[str, list[dict]] = defaultdict(list)
        for r in pc.read_tsv(HOMONYMS_TSV):
            self.homonyms[r["play_id"]].append(r)
        # other authors per L/H play: works sharing a title segment
        self.other_authors: dict[str, dict[str, str]] = defaultdict(dict)
        for w in pc.read_tsv(WORKS_TSV) if WORKS_TSV.exists() else []:
            if w.get("lateiner_hurwitz") or not w.get("author_heading"):
                continue
            titles = [w["title"]] + _split(w.get("titles_printed", ""))
            pids = {pid for t in titles for s in pc.title_segments(t)
                    for pid in self.seg_to_play.get(s, ())}
            for pid in pids:
                if w.get("plays_db_id") == pid:
                    continue
                for k in _surname_keys(w["author_heading"]):
                    if not pc.author_comention(k, self.plays[pid]["author_db_id"]):
                        self.other_authors[pid][k] = w["author_heading"]
        for pid, rows in self.homonyms.items():
            for r in rows:
                for v in _split(r["other_author_forms"]):
                    self.other_authors[pid][pc.norm_yiddish(v)] = r["other_author"]
        self._headings: dict[str, str] | None = None

    def _host_is_lh(self, person_id: str) -> bool:
        if self._headings is None:
            self._headings = {r["person_id"]: pc.norm_yiddish(r["heading"])
                              for r in pc.read_tsv(pc.ENTRY_TEXTS_TSV)}
        h = self._headings.get(person_id, "")
        return any(pc.author_comention(h, a) for a in pc.DEFAULT_AUTHOR_DB_IDS)

    @staticmethod
    def _own_plays_cue(quote: str, surface: str) -> bool:
        """The title follows a 'his plays …' list opener in the sentence."""
        nq = pc.norm_yiddish(quote)
        m = _OWN_PLAYS.search(nq)
        if not m:
            return False
        tail = nq[m.end():]
        return any(seg in tail for seg in pc.title_segments(surface)
                   or [pc.norm_yiddish(surface)] if seg)

    # -- evidence cues -------------------------------------------------------
    @staticmethod
    def _title_windows(quote: str, surface: str) -> list[list[str]]:
        """Token windows (3 before, 4 after) around each occurrence of the
        play title in the evidence sentence."""
        toks = pc.norm_yiddish(quote).split()
        segs = [s.split() for s in pc.title_segments(surface)] or \
               ([pc.norm_yiddish(surface).split()] if surface.strip() else [])
        wins = []
        for seg in segs:
            n = len(seg)
            for i in range(len(toks) - n + 1):
                if toks[i:i + n] == seg:
                    wins.append(toks[max(0, i - 3):i] + toks[i + n:i + n + 4])
        return wins

    def _author_cue(self, tokens: list[str], pid: str) -> str:
        """'LH' / 'OTHER:<author>' / '' for a token list."""
        lh = self.plays[pid]["author_db_id"]
        for t in tokens:
            stem = _POSSESSIVE.sub("", t)
            if pc.author_comention(t, lh):
                return "LH"
            for key in (t, stem):
                if key in self.other_authors.get(pid, {}):
                    return f"OTHER:{self.other_authors[pid][key]}"
        return ""

    def decide(self, r: dict, pid: str) -> tuple[str, str]:
        """(verdict, reason) for a fact linked to L/H play `pid`.
        verdict: LH (confirmed), KEEP (no evidence either way), OTHER."""
        ov = self.overrides.get(r.get("fact_id", ""))
        if ov:
            return ov["verdict"], f"override:{ov.get('note', '')}"
        quote, surface = r.get("evidence_quote", ""), r.get("play_title_surface", "")
        for win in self._title_windows(quote, surface):
            cue = self._author_cue(win, pid)
            if cue:
                return ("LH", "author_next_to_title") if cue == "LH" \
                    else ("OTHER", f"author_next_to_title:{cue[6:]}")
        cue = self._author_cue(pc.norm_yiddish(quote).split(), pid)
        if cue.startswith("OTHER"):
            return "OTHER", f"author_in_sentence:{cue[6:]}"
        lh_in_sentence = cue == "LH"
        any_lh = any(pc.author_comention(pc.norm_yiddish(quote), a)
                     for a in pc.DEFAULT_AUTHOR_DB_IDS)
        if not any_lh and self._own_plays_cue(quote, surface) \
                and not self._host_is_lh(r.get("person_id", "")):
            return "OTHER", "host_own_play"
        for h in self.homonyms.get(pid, []):
            char = pc.norm_yiddish(r.get("character", ""))
            text = pc.norm_yiddish(quote + " " + r.get("character", ""))
            if char and any(pc.norm_yiddish(c) in char for c in _split(h["lh_characters"])):
                return "LH", "lh_character"
            if any(pc.norm_yiddish(c) in text for c in _split(h["other_characters"])):
                return "OTHER", f"character_of:{h['other_author']}"
            year = (r.get("date_start") or "")[:4]
            if not lh_in_sentence and year.isdigit() and h["other_first_year"] \
                    and int(year) >= int(h["other_first_year"]):
                return "OTHER", f"date_after:{h['other_author']}"
        if lh_in_sentence:
            return "LH", "author_in_sentence"
        for h in self.homonyms.get(pid, []):
            if h.get("default_verdict") == "OTHER":
                return "OTHER", f"homonym_default:{h['other_author']}"
        return "KEEP", ""

    def rescue(self, r: dict) -> str:
        """play_id when an unlinked surface's evidence names an L/H author next
        to a title fuzzy-matching one of that author's plays, else ''."""
        ov = self.overrides.get(r.get("fact_id", ""))
        if ov:
            return ov["play_id"] if ov["verdict"] == "LH" else ""
        surface = r.get("play_title_surface", "")
        segs = pc.title_segments(surface) or [pc.norm_yiddish(surface)]
        for win in self._title_windows(r.get("evidence_quote", ""), surface):
            for author, plays in self.lh_by_author.items():
                if not any(pc.author_comention(t, author) for t in win):
                    continue
                best, best_r = "", 0.0
                for p in plays:
                    for ps in _split(p["title_segments_norm"]):
                        sc = max(fuzz.ratio(s, ps) for s in segs if s) if any(segs) else 0
                        if sc > best_r:
                            best, best_r = p["play_id"], sc
                if best_r >= RESCUE_FUZZY:
                    return best
        return ""

    def character_ref(self, r: dict) -> str:
        ov = self.overrides.get(r.get("fact_id", ""))
        return (ov or {}).get("character_ref", "")


def main() -> None:
    """Dry diagnostic over kg_facts_linked.tsv (+ surface adjudication)."""
    import build_kg as bk
    att = Attribution()
    review = {(r["slot"], r["surface"]): r for r in pc.read_tsv(pc.LINK_REVIEW_TSV)}
    out, rescued, excluded_new = Counter(), Counter(), Counter()
    for r in pc.read_tsv(bk.LINKED_TSV):
        link, st = bk.resolve_with_adjudication(r, "play", review)
        if link.startswith("play:PL-") and "|" not in link and st in ("matched", "candidate"):
            v, why = att.decide(r, link[5:])
            out[(v, why.split(":")[0])] += 1
            if v == "OTHER":
                print(f"OTHER {link} {r['play_title_surface']!r} {why} | "
                      f"{r['evidence_quote'][:110]}")
        elif st != "not_entity" and r.get("play_title_surface", "").strip():
            pid = att.rescue(r)
            if pid:
                rescued[pid] += 1
                print(f"RESCUE {r['play_title_surface']!r} -> {pid} | "
                      f"{r['evidence_quote'][:110]}")
            else:
                excluded_new[r["play_title_surface"]] += 1
    print("\nlinked-to-L/H verdicts:", dict(out))
    print("rescued:", dict(rescued))
    print(f"excluded non-L/H surfaces: {len(excluded_new)} surfaces, "
          f"{sum(excluded_new.values())} facts")


if __name__ == "__main__":
    main()
