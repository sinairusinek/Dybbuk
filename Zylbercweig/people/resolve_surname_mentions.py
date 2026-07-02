"""Resolve bare-surname mentions to person hubs (Phase C, C3a — job 3).

Design principles (PI-ratified 2026-07-02):
  1. Resolution unit = (surname × host entry), NEVER a global surname→person
     rule. Within-entry coreference is tried FIRST: a bare surname expands to
     the fullest same-surname mention earlier/elsewhere in the SAME entry.
  2. Residual scoring: host-subject career overlap + co-mentions + gender
     cues + fame prior. The fame prior is DISABLED inside family clusters
     (≥2 candidates who are each corpus-famous — Adler/Kaminski/…).
  3. Abstain is first-class: verdict AMBIGUOUS carries the surviving
     candidate set; family clusters pre-suggest a family-level abstain.

Inputs: mentions_all.tsv, person_hub.tsv (+members), people_extracted.tsv,
people_db.tsv, derived_mention_alignments.tsv.
Outputs:
  mention_surname_resolutions.tsv  one row per bare-surname mention
  surname_groups.tsv               per-surname rollup driving the B3 GUI

Run: python3.11 Zylbercweig/people/resolve_surname_mentions.py
"""
from __future__ import annotations

import pathlib
import re
import sys
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from people_common import (  # noqa: E402
    load_db_rows,
    load_extracted,
    mention_volume,
    read_tsv,
    write_tsv,
)
from people_similarity import (  # noqa: E402
    expand_name_variants,
    extract_year,
    normalize_person_name,
    set_given_name_counts,
    split_name_tokens,
    variant_pair_score,
)

MENTIONS_TSV = HERE / "mentions_all.tsv"
HUB_TSV = HERE / "person_hub.tsv"
DERIVED_TSV = HERE / "derived_mention_alignments.tsv"
OUT_TSV = HERE / "mention_surname_resolutions.tsv"
GROUPS_TSV = HERE / "surname_groups.tsv"

# scoring weights / thresholds
W_WITHIN_ENTRY = 0.40
W_CO_MENTION = 0.25
W_FAME = 0.30
W_CAREER = 0.10
BASE = 0.30
RESOLVE_MARGIN = 0.20          # top-vs-runner-up margin to auto-resolve
EXPANSION_MIN = 0.75           # fuller-form ↔ candidate variant score floor
FAME_DOMINANCE = 4.0           # top surface-prior must be ≥4× runner-up
FAMILY_FAME_FLOOR = 20         # ≥2 candidates this corpus-famous → family cluster

OUT_FIELDS = [
    "mention_id", "volume", "host_xml_id", "host_person_id", "host_heading",
    "surname_token", "mention_name", "verdict", "method",
    "resolved_hub_id", "resolved_heading",
    "candidate_hub_ids", "candidate_headings", "candidate_scores",
    "family_cluster", "chips", "gold_hub_id", "gold_agrees", "sentence",
]


def build_given_counts(extracted: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for p in extracted:
        h = p.get("heading") or ""
        if "," in h:
            for t in split_name_tokens(h.split(",", 1)[1]):
                counts[t] += 1
    return dict(counts)


def heading_surname_tokens(heading: str, given_counts: dict[str, int]) -> set[str]:
    """Surname tokens of a HEADING (not a variant/DB form).

    people_similarity._surname_tokens always keeps the LAST token of comma-less
    forms — right for names_variants/DB hebnames (given-first), wrong for
    headings: comma-less headings mix surname-first (בלום אַברהם) and
    given-first (אַברהם עדעלמאַן) orders. For headings the reliable rule is to
    drop common given names from BOTH ends; the survivors are the surname.
    Without this, bare given names (אברהם…) flood the surname index and the
    candidate sets are garbage.
    """
    h = re.sub(r"[\[\(].*?[\]\)]", " ", heading or "").strip()
    if "," in h:
        return {t for t in split_name_tokens(h.split(",", 1)[0]) if len(t) >= 3}
    toks = [t for t in split_name_tokens(h) if len(t) >= 3]
    if not toks:
        return set()
    if len(toks) == 1:
        return set(toks) if given_counts.get(toks[0], 0) < 3 else set()
    kept = {t for t in toks if given_counts.get(t, 0) < 3}
    return kept or {toks[0]}


class HubIndex:
    def __init__(self):
        extracted = load_extracted()
        given_counts = build_given_counts(extracted)
        set_given_name_counts(given_counts)
        self.entry_by_pid = {r["person_id"]: r for r in extracted}
        db_gender = {d["db_id"]: (d.get("gender") or "").strip().lower()
                     for d in load_db_rows()}

        self.hubs = {h["hub_id"]: h for h in read_tsv(HUB_TSV)}
        self.hub_of_pid: dict[str, str] = {}
        self.surname_to_hubs: dict[str, set[str]] = defaultdict(set)
        self.hub_gender: dict[str, str] = {}
        self.hub_birth: dict[str, int | None] = {}
        self.hub_death: dict[str, int | None] = {}
        self._variants_cache: dict[str, set[str]] = {}

        for h in self.hubs.values():
            hid = h["hub_id"]
            if h.get("non_person") == "1":
                continue
            pids = [p for p in h["entry_person_ids"].split("|") if p]
            for p in pids:
                self.hub_of_pid[p] = hid
                e = self.entry_by_pid.get(p, {})
                for t in heading_surname_tokens(e.get("heading", ""), given_counts):
                    self.surname_to_hubs[t].add(hid)
            genders = {db_gender.get(db, "") for db in h["db_ids"].split("|") if db}
            genders.discard("")
            self.hub_gender[hid] = next(iter(genders)) if len(genders) == 1 else ""
            self.hub_birth[hid] = extract_year(h.get("birth_date", ""))
            self.hub_death[hid] = extract_year(h.get("death_date", ""))

        # fame prior + full-form lexicon from validated surfaces
        self.surface_prior: dict[str, dict[str, float]] = defaultdict(dict)
        self.full_lexicon: dict[str, str] = {}   # normalized full surface → hub
        self.hub_total_occ: dict[str, float] = defaultdict(float)
        for r in read_tsv(DERIVED_TSV):
            pids = [p for p in (r.get("matched_person_ids") or "").split("|") if p]
            hids = {self.hub_of_pid.get(p) for p in pids} - {None}
            if len(hids) != 1:
                continue
            hid = next(iter(hids))
            occ = float(r.get("occurrences") or 0)
            norm = normalize_person_name(r["mention_surface"])
            if not norm:
                continue
            self.hub_total_occ[hid] += occ
            if r["source_sheet"] == "surnames":
                prev = self.surface_prior[norm].get(hid, 0.0)
                self.surface_prior[norm][hid] = prev + occ
                # RA-validated referents extend the candidate universe: the
                # surface may address a person by given name / mononym /
                # pseudonym (לייוויק → כאַנוקאָוו, לייוויק), which the
                # heading-surname index alone can never surface.
                if len(norm.split()) == 1 and len(norm) >= 3:
                    self.surname_to_hubs[norm].add(hid)
            elif len(norm.split()) >= 2:
                self.full_lexicon.setdefault(norm, hid)

    def hub_variants(self, hub_id: str) -> set[str]:
        if hub_id not in self._variants_cache:
            out: set[str] = set()
            for p in self.hubs[hub_id]["entry_person_ids"].split("|"):
                e = self.entry_by_pid.get(p)
                if e:
                    out |= expand_name_variants(
                        e.get("heading", ""), e.get("names_variants", ""),
                        e.get("subheading", ""))
            self._variants_cache[hub_id] = out
        return self._variants_cache[hub_id]

    def heading(self, hub_id: str) -> str:
        return self.hubs.get(hub_id, {}).get("canonical_heading", "")


def mention_year(m: dict) -> int | None:
    for k in ("rel_date_start", "rel_date_end"):
        y = extract_year(m.get(k) or "")
        if y:
            return y
    return None


def main() -> None:
    idx = HubIndex()
    mentions = read_tsv(MENTIONS_TSV)

    # group mentions per host entry — the resolution unit is (surname × entry)
    by_host: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for m in mentions:
        by_host[(mention_volume(m.get("src_file", "")),
                 (m.get("host_xml_id") or "").strip())].append(m)

    out: list[dict] = []
    n_gold = n_gold_agree = 0

    for (vol, host_xml), group in by_host.items():
        host_pid = f"P-{vol}-{host_xml}" if host_xml else ""
        host_entry = idx.entry_by_pid.get(host_pid, {})
        host_by = extract_year(host_entry.get("birth_date", ""))
        host_dy = extract_year(host_entry.get("death_date", ""))

        # multi-token mentions in this entry = expansion material + co-mention hubs
        fuller: list[str] = []
        co_hubs: set[str] = set()
        for m in group:
            toks = split_name_tokens(m.get("mention_name", ""))
            if len(toks) >= 2:
                surface = m["mention_name"]
                fuller.append(surface)
                hid = idx.full_lexicon.get(normalize_person_name(surface))
                if hid:
                    co_hubs.add(hid)

        for m in group:
            name = (m.get("mention_name") or "").strip()
            if not name or "*" in name or (m.get("relational_placeholder") or "").strip() == "1":
                continue
            toks = split_name_tokens(name)
            if len(toks) != 1 or len(toks[0]) < 3:
                continue
            token = toks[0]
            cands = sorted(idx.surname_to_hubs.get(token, set()))
            if not cands:
                continue  # not a known surname — out of this job's universe

            chips: list[str] = []
            mgender = (m.get("gender") or "").strip().lower()
            myear = mention_year(m)

            # hard gates: gender + birth-impossibility
            survivors = []
            for hid in cands:
                hg = idx.hub_gender.get(hid, "")
                if mgender and hg and mgender != hg:
                    continue
                hb = idx.hub_birth.get(hid)
                if myear and hb and myear < hb + 10:
                    continue
                survivors.append(hid)
            if mgender:
                chips.append(f"gender:{mgender}")
            if myear:
                chips.append(f"year:{myear}")

            # family-cluster detection (among gate survivors)
            famous = [h for h in survivors
                      if idx.hub_total_occ.get(h, 0) >= FAMILY_FAME_FLOOR]
            family = len(famous) >= 2
            if family:
                chips.append("family-cluster")

            # 1. within-entry expansion
            expansion_hits: set[str] = set()
            for surface in fuller:
                if token not in split_name_tokens(surface):
                    continue
                scored = []
                for hid in survivors:
                    best = max((variant_pair_score(surface, v)
                                for v in idx.hub_variants(hid)), default=0.0)
                    scored.append((best, hid))
                scored.sort(reverse=True)
                if scored and scored[0][0] >= EXPANSION_MIN and (
                        len(scored) == 1 or scored[0][0] - scored[1][0] >= 0.1):
                    expansion_hits.add(scored[0][1])
            if expansion_hits:
                chips.append("within-entry")

            # 2. fame prior (surname-sheet surface priors; disabled in families)
            norm_token = normalize_person_name(token)
            priors = {h: p for h, p in idx.surface_prior.get(norm_token, {}).items()
                      if h in survivors}
            prior_pick = ""
            if priors and not family:
                ranked = sorted(priors.items(), key=lambda kv: -kv[1])
                top_p = ranked[0][1]
                second_p = ranked[1][1] if len(ranked) > 1 else 0.0
                if top_p >= FAME_DOMINANCE * max(second_p, 1.0):
                    prior_pick = ranked[0][0]
                    chips.append("fame-prior")

            # 3. composite scores
            scores: dict[str, float] = {}
            for hid in survivors:
                s = BASE
                if hid in expansion_hits:
                    s += W_WITHIN_ENTRY
                if hid in co_hubs:
                    s += W_CO_MENTION
                    if "co-mention" not in chips:
                        chips.append("co-mention")
                if hid == prior_pick:
                    s += W_FAME
                hb, hd = idx.hub_birth.get(hid), idx.hub_death.get(hid)
                anchor_lo = host_by or (myear and myear - 5)
                anchor_hi = host_dy or (myear and myear + 5)
                if hb and hd and anchor_lo and anchor_hi and not (
                        hd < anchor_lo or hb > anchor_hi):
                    s += W_CAREER
                    if "career-overlap" not in chips:
                        chips.append("career-overlap")
                scores[hid] = round(s, 3)

            ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
            if not ranked:
                verdict, method, top = "UNKNOWN", "all_gated_out", ""
            elif len(expansion_hits) == 1:
                verdict, method, top = "RESOLVED", "within_entry", next(iter(expansion_hits))
            elif len(ranked) == 1:
                verdict, method, top = "RESOLVED", "unique_candidate", ranked[0][0]
            elif ranked[0][1] - ranked[1][1] >= RESOLVE_MARGIN:
                verdict, method, top = "RESOLVED", "scored", ranked[0][0]
            else:
                verdict = "AMBIGUOUS"
                method = "family_abstain_suggested" if family else "low_margin"
                top = ""

            # gold comparison (surname sheet: dominant referent of this surface)
            gold_hubs = idx.surface_prior.get(norm_token, {})
            gold_hub = max(gold_hubs, key=gold_hubs.get) if gold_hubs else ""
            gold_agrees = ""
            if gold_hub and verdict == "RESOLVED":
                n_gold += 1
                gold_agrees = "1" if top == gold_hub else "0"
                n_gold_agree += int(gold_agrees == "1")

            out.append({
                "mention_id": m.get("mention_id", ""),
                "volume": vol,
                "host_xml_id": host_xml,
                "host_person_id": host_pid if host_entry else "",
                "host_heading": m.get("host_heading", ""),
                "surname_token": token,
                "mention_name": name,
                "verdict": verdict,
                "method": method,
                "resolved_hub_id": top,
                "resolved_heading": idx.heading(top) if top else "",
                "candidate_hub_ids": "|".join(h for h, _ in ranked[:6]),
                "candidate_headings": "|".join(idx.heading(h) for h, _ in ranked[:6]),
                "candidate_scores": "|".join(str(s) for _, s in ranked[:6]),
                "family_cluster": "1" if family else "0",
                "chips": "|".join(chips),
                "gold_hub_id": gold_hub,
                "gold_agrees": gold_agrees,
                "sentence": (m.get("relation_sentence") or "")[:400],
            })

    out.sort(key=lambda r: (r["surname_token"], r["volume"], r["host_xml_id"],
                            r["mention_id"]))
    write_tsv(OUT_TSV, out, OUT_FIELDS)

    # per-surname rollup for the GUI
    groups: dict[str, dict] = {}
    for r in out:
        g = groups.setdefault(r["surname_token"], {
            "surname": r["surname_token"], "n_mentions": 0, "n_resolved": 0,
            "n_ambiguous": 0, "n_unknown": 0, "family_cluster": "0",
            "hub_ids": set(), "headings": set()})
        g["n_mentions"] += 1
        key = {"RESOLVED": "n_resolved", "AMBIGUOUS": "n_ambiguous",
               "UNKNOWN": "n_unknown"}[r["verdict"]]
        g[key] += 1
        if r["family_cluster"] == "1":
            g["family_cluster"] = "1"
        for h, head in zip(r["candidate_hub_ids"].split("|"),
                           r["candidate_headings"].split("|")):
            if h:
                g["hub_ids"].add(h)
                g["headings"].add(head)
    group_rows = []
    for g in groups.values():
        group_rows.append({**g, "n_candidates": len(g["hub_ids"]),
                           "hub_ids": "|".join(sorted(g["hub_ids"])),
                           "headings": "|".join(sorted(g["headings"]))})
    group_rows.sort(key=lambda g: -g["n_ambiguous"])
    write_tsv(GROUPS_TSV, group_rows, [
        "surname", "n_mentions", "n_resolved", "n_ambiguous", "n_unknown",
        "n_candidates", "family_cluster", "hub_ids", "headings"])

    n = len(out)
    nv = {v: sum(1 for r in out if r["verdict"] == v)
          for v in ("RESOLVED", "AMBIGUOUS", "UNKNOWN")}
    nm = defaultdict(int)
    for r in out:
        if r["verdict"] == "RESOLVED":
            nm[r["method"]] += 1
    print(f"bare-surname mentions in universe: {n}")
    print(f"verdicts: {nv}")
    print(f"resolved by method: {dict(nm)}")
    print(f"family-cluster mentions: {sum(1 for r in out if r['family_cluster'] == '1')}")
    print(f"surname groups: {len(group_rows)} "
          f"(with ambiguity: {sum(1 for g in group_rows if g['n_ambiguous'])})")
    if n_gold:
        print(f"gold check (dominant-referent surfaces): "
              f"{n_gold_agree}/{n_gold} = {100 * n_gold_agree / n_gold:.1f}% "
              f"of RESOLVED verdicts agree with the RA surname sheet")


if __name__ == "__main__":
    main()
