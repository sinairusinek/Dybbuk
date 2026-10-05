"""Reconcile the editions dataset against the project databases.

Reads data/editions.json (built by build_editions_dataset.py) and resolves every
real-world entity it names — playwrights, credited people, venues, places,
publishers — against the canonical databases:

  people  → Zylbercweig/people/people_db.tsv            (db_id)
  orgs    → Zylbercweig/organizations/core_db.tsv        (db_id)
  places  → Zylbercweig/zibn-shtern/.../toponyms_gazetteer.csv  (qid)

Every entity lands in exactly one bucket:

  LINKED    an exact match on a normalised name or variant → carries a db id
  PROPOSED  a high-confidence automatic candidate, NOT a decision; needs a human
            to confirm. Order-insensitive token matching, which is what the
            catalogue's "Surname, Given" vs people_db's "Given Surname" needs.
  GAP       no candidate, or several equally good ones → an open question

Outputs:
  data/entity_graph.json   nodes + edges, every node carrying its link status
  data/entity_gaps.tsv     the ledger: one row per open question

Nothing here writes a decision to any database. PROPOSED rows are proposals.
"""
from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(10 ** 7)

ROOT = Path(__file__).resolve().parent.parent          # YiDraCor/
REPO = ROOT.parent                                      # Dybbuk/
EDITIONS = ROOT / "data" / "editions.json"
PEOPLE_DB = REPO / "Zylbercweig" / "people" / "people_db.tsv"
CORE_DB = REPO / "Zylbercweig" / "organizations" / "core_db.tsv"
GAZETTEER = (REPO / "Zylbercweig" / "zibn-shtern" / "data" / "working"
             / "toponyms_gazetteer.csv")
OUT_GRAPH = ROOT / "data" / "entity_graph.json"
OUT_GAPS = ROOT / "data" / "entity_gaps.tsv"

# People we know the catalogue names by surname alone in the author column.
AUTHOR_DB_ID = {683: "Joseph Lateiner", 684: "Moyshe (Ish Halevi) Hurwitz"}

# ---------------------------------------------------------------------------
# Confirmed human decisions. Everything here was reviewed and accepted by a
# person; the reviewer and date are recorded so a later reader can tell these
# apart from the matcher's own automatic PROPOSED candidates.
#
# reviewer: Sinai · 2026-10-05
# Accepted the ten org spelling variants surfaced as near-spelling hints, and
# identified 'מאדאם ליפצין' (Madame Lipzin) as Keni Lipzin.
REVIEWED_BY = "Sinai 2026-10-05"

ORG_DECISION = {
    "windsortheater": ("134", "Windsor Theatre"),
    "poolstheatre": ("123", "Poole's Theatre"),
    "tsentraltheatre": ("151", "Tsentral Theater"),
    "peoplestheatre": ("120", "People's Theatre NY"),
    "folkstheatre": ("365", "Public Theatre (Folks Theatre)"),
    "kesslersthaliatheatre": ("130", "Thalia Theatre|Bowery Theatre"),
    "thomashefskyspeopletheatre": ("156", "Thomashefsky Theatre"),
    "amkroytetfreundbuchhandlung": ("63", "אַמקרויט עט פריינד| Amkroyt un Fraynd"),
    "amkrautfreundbuchhandlung": ("63", "אַמקרויט עט פריינד| Amkroyt un Fraynd"),
    "diyudishebihne": ("1849", "די אידישע ביהנע"),
}

PERSON_DECISION = {
    "מאדאםליפצין": ("762", "קעני ליפצין (סאַכאַר קריינע סאָניעס)"),
}

# Sub-city districts the gazetteer does not hold as settlements. Token matching
# would bind these to the parent city (Lower East Side → New York City), which
# silently loses the distinction, so they are raised as questions instead:
# the premiere venues are all Manhattan, and whether the corpus wants a
# district tier or should resolve to the city is a modelling decision.
DISTRICT_NOT_SETTLEMENT = {
    "lowereastsidenewyorkny": "New York City (Q60) — district, not a settlement",
}

# Latin-script historical names the gazetteer lacks. It DOES carry these
# exonyms, but only in Yiddish (לעמבערג for Lviv, ווילנע/ווילנא for Vilnius)
# — the romanised forms a title page or an English catalogue prints are
# absent, and there are no German forms at all (no Pressburg, Breslau,
# Danzig). Checked against the gazetteer 2026-10-05.
# `Vilna` alone would otherwise match Vilna Governorate / Vilna Ghetto, i.e.
# the region and the WWII ghetto rather than the city.
PLACE_EXONYM = {
    "lemberg": ("Q36036", "Lviv"),
    "vilna": ("Q216", "Vilnius"),
    "vilne": ("Q216", "Vilnius"),
    "wilno": ("Q216", "Vilnius"),
    # source typos, confirmed by Sinai 2026-10-05
    "clevelandohaio": ("Q37320", "Cleveland"),
    "clevelandohio": ("Q37320", "Cleveland"),
}


# ---------------------------------------------------------------- normalisation

def strip_points(s: str) -> str:
    """NFKD then drop combining marks — Yiddish points must go before any
    comparison or ostensibly identical names will not match."""
    return "".join(c for c in unicodedata.normalize("NFKD", str(s or ""))
                   if not unicodedata.combining(c))


def sm(s) -> str:
    """Squash to a comparable key: unpointed, alphanumerics only, lowercase."""
    return re.sub(r"[^\w]+", "", strip_points(s)).lower()


# Imprint boilerplate around a publisher's actual name. The catalogue writes
# 'Farlag "Kultur" (Dzika 13)' and 'Verlag von Benjamin Munk Buchhandlung'
# where core_db holds the bare 'Kultur' / 'Benjamin Munk'.
_IMPRINT_NOISE = re.compile(
    r"""\b(farlag|verlag(\s+von)?|buchhandlung|buchdruckerei|druck(\s+von)?|
         drukeray|press|printing|publishers?|publishing|et|und|and)\b""",
    re.I | re.X)


def imprint_core(s: str) -> str:
    """A publisher string reduced to its distinguishing name.

    Drops imprint boilerplate, a parenthesised street address, and quotes, so
    'Farlag "Kultur" (Dzika 13)' and 'Kultur' compare equal.
    """
    s = re.sub(r"\([^)]*\)", " ", str(s or ""))     # (Dzika 13)
    s = s.replace("&", " ")
    s = _IMPRINT_NOISE.sub(" ", s)
    return s


def toks(s) -> frozenset:
    """Order-insensitive token set, locators and punctuation removed.

    The catalogue stores people as "Surname, Given (Alias) 936:1" while
    people_db stores "Given Surname (Alias)" — only a set comparison matches
    those, and the trailing `vol:col` lexicon locator has to come out first.
    """
    s = strip_points(s)
    s = re.sub(r"\d+:\d+", " ", s)       # lexicon locator
    s = re.sub(r"[^\w\s]+", " ", s)      # incl. the RTL-mangled parens
    return frozenset(t for t in s.split() if len(t) > 1 and not t.isdigit())


# ---------------------------------------------------------------- db loading

def load_people() -> tuple[dict, dict]:
    """Return (exact index, token index) over people_db."""
    exact, tokidx = {}, defaultdict(list)
    with PEOPLE_DB.open(encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            names = [r.get("hebname"), r.get("english"), r.get("alternative_name")]
            names += (r.get("name_variants") or "").split("|")
            label = (r.get("hebname") or r.get("english") or "").strip()
            for n in names:
                if not (n or "").strip():
                    continue
                exact.setdefault(sm(n), (r["db_id"], label))
                tokidx[toks(n)].append((r["db_id"], label))
    return exact, dict(tokidx)


def load_orgs() -> tuple[dict, dict, dict]:
    """Return (exact index, composite index, token index) over core_db.

    core_db stores some venues as a single `A|B` row (two names for one
    building), so index the halves too — otherwise "Bowery Theatre" misses
    the row called "Thalia Theatre|Bowery Theatre".
    """
    exact, parts, tokidx = {}, {}, defaultdict(list)
    with CORE_DB.open(encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if (r.get("deprecated") or "").strip().lower() in ("y", "yes", "true", "1"):
                continue
            label = (r.get("name") or r.get("name_yiddish") or "").strip()
            fields = [r.get("name"), r.get("name_yiddish"),
                      r.get("name_yiddish_translit")]
            fields += (r.get("name_variants") or "").split("|")
            for n in fields:
                if not (n or "").strip():
                    continue
                exact.setdefault(sm(n), (r["db_id"], label))
                # `A|B` rows name one body two ways; index each half, and each
                # half's imprint core, so a publisher written out in full still
                # finds the bare name core_db stores.
                for half in str(n).split("|"):
                    if half.strip():
                        parts.setdefault(sm(half), (r["db_id"], label))
                        core = sm(imprint_core(half))
                        if core:
                            parts.setdefault(core, (r["db_id"], label))
                        tokidx[toks(half)].append((r["db_id"], label))
    return exact, parts, dict(tokidx)


def bare_place(s: str) -> str:
    """A place name without its disambiguating qualifier.

    534 of the gazetteer's 1018 labels carry one — "Iași (Romania)",
    "Podgorze (Krakow, Poland)" — and the corpus writes its own
    ("Iași (Rumenia)"), so neither side matches until both are stripped.
    """
    return re.sub(r"\([^)]*\)", " ", str(s or ""))


def load_places() -> tuple[dict, dict]:
    """Return (exact index, token index) over the toponym gazetteer."""
    exact, tokidx = {}, defaultdict(list)
    with GAZETTEER.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            label = (r.get("label_en") or r.get("kima_rom") or "").strip()
            names = [r.get("label_en"), r.get("label_yi"),
                     r.get("kima_rom"), r.get("kima_heb")]
            # the gazetteer delimits variants with ';', not '|' — splitting on
            # the wrong character indexed all of them as one unusable string,
            # which is why the Yiddish exonyms (ווילנע, לעמבערג) never matched
            names += re.split(r"[;|]", r.get("variants") or "")
            for n in names:
                if not (n or "").strip():
                    continue
                exact.setdefault(sm(n), (r["qid"], label))
                # also index the name without its qualifier, so a corpus
                # string that carries none (or a different one) still lands
                bare = sm(bare_place(n))
                if bare:
                    exact.setdefault(bare, (r["qid"], label))
                    tokidx[toks(bare_place(n))].append((r["qid"], label))
                tokidx[toks(n)].append((r["qid"], label))
    return exact, dict(tokidx)


# ---------------------------------------------------------------- resolution

def nearest_spellings(raw: str, exact: dict, limit: int = 3,
                      floor: float = 0.86) -> list[tuple]:
    """Closest db names by character similarity — a hint, never a link.

    Catches one-character orthographic variance ("Theater"/"Theatre",
    "Pool's"/"Poole's") that token matching treats as a miss.
    """
    from difflib import SequenceMatcher
    key = sm(raw)
    if len(key) < 5:
        return []
    scored = []
    for k, (db_id, label) in exact.items():
        if abs(len(k) - len(key)) > 4:
            continue
        r = SequenceMatcher(None, key, k).ratio()
        if r >= floor:
            scored.append((r, db_id, label))
    scored.sort(key=lambda x: -x[0])
    out, seen = [], set()
    for _, db_id, label in scored:
        if db_id in seen:
            continue
        seen.add(db_id)
        out.append((db_id, label))
        if len(out) >= limit:
            break
    return out


def propose(raw: str, tokidx: dict) -> list[tuple]:
    """Order-insensitive candidates for `raw`, best first.

    A candidate qualifies when one token set contains the other (so
    "Karp" ⊂ "Sophie Karp"), scored by overlap over the longer set. Several
    equal-scoring candidates means ambiguous, which is a GAP, not a proposal.
    """
    t = toks(raw)
    if not t:
        return []
    scored = []
    for k, vals in tokidx.items():
        if not k:
            continue
        inter = len(k & t)
        if inter and (k <= t or t <= k):
            scored.append((inter / max(len(k), len(t)), k, vals))
    scored.sort(key=lambda x: -x[0])
    return scored


def resolve(raw: str, exact: dict, tokidx: dict | None = None,
            parts: dict | None = None, decisions: dict | None = None) -> dict:
    """Resolve one name string to LINKED / PROPOSED / GAP.

    `decisions` holds reviewer-confirmed links, which win over every automatic
    rule below and are stamped with the reviewer so they read as decisions
    rather than as the matcher's own guesses.
    """
    key = sm(raw)
    if decisions and key in decisions:
        db_id, label = decisions[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "reviewed", "reviewer": REVIEWED_BY}
    if key in DISTRICT_NOT_SETTLEMENT:
        return {"status": "GAP", "reason": "district, not a gazetteer settlement",
                "note": DISTRICT_NOT_SETTLEMENT[key]}
    if key in exact:
        db_id, label = exact[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "exact"}

    # a historical German/Yiddish name the gazetteer does not carry
    if key in PLACE_EXONYM:
        db_id, label = PLACE_EXONYM[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "exonym"}

    # the incoming string carries a qualifier the db label does not, or vice
    # versa ("Iași (Rumenia)" vs "Iași (Romania)")
    bare = sm(bare_place(raw))
    if bare and bare != key and bare in exact:
        db_id, label = exact[bare]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "qualifier-stripped"}

    # a half of a composite `A|B` db row
    if parts and key in parts:
        db_id, label = parts[key]
        return {"status": "LINKED", "db_id": db_id, "matched": label,
                "method": "composite-half"}

    # the same, after stripping imprint boilerplate off a publisher string
    if parts:
        core = sm(imprint_core(raw))
        if core and core in parts:
            db_id, label = parts[core]
            return {"status": "LINKED", "db_id": db_id, "matched": label,
                    "method": "imprint-core"}

    if tokidx:
        scored = propose(raw, tokidx)
        if scored:
            top = scored[0]
            # several distinct ids tie at the top score → genuinely ambiguous
            tied = [s for s in scored if abs(s[0] - top[0]) < 1e-9]
            ids = {v[0] for s in tied for v in s[2]}
            if len(ids) > 1:
                return {"status": "GAP", "reason": "ambiguous",
                        "candidates": sorted(
                            {(v[0], v[1]) for s in tied for v in s[2]})[:6]}
            # Guard against a proposal resting on a single shared token:
            # "Rozenfeld, Moris" → a row called just "Moris", or a
            # neighbourhood → its city. Worse than admitting a gap. A
            # one-token name on BOTH sides (a mononym) is still allowed.
            score, k, vals = top
            t = toks(raw)
            if len(k & t) < 2 and max(len(k), len(t)) > 1:
                return {"status": "GAP", "reason": "weak match (one token only)",
                        "candidates": [(v[0], v[1]) for v in vals][:6]}
            db_id, label = vals[0]
            return {"status": "PROPOSED", "db_id": db_id, "matched": label,
                    "method": "token-set", "score": round(score, 3)}

    # Nothing matched. Offer the closest spellings as a hint for the reviewer:
    # "Windsor Theater" vs core_db's "Windsor Theatre" differ by one character,
    # which token matching cannot bridge but a human resolves instantly.
    near = nearest_spellings(raw, exact)
    if near:
        return {"status": "GAP", "reason": "no candidate (near spellings offered)",
                "candidates": near}
    return {"status": "GAP", "reason": "no candidate"}


# ---------------------------------------------------------------- graph build

def person_raw(role_row: dict) -> str:
    return (role_row.get("PersonKey")
            or role_row.get("PersonName as appears in Source if there is no key")
            or "").strip()


def split_venues(v) -> list[str]:
    """Venue cells hold `A|B` for one building under two names."""
    return [p.strip() for p in str(v or "").split("|") if p.strip()]


def main() -> int:
    editions = json.loads(EDITIONS.read_text(encoding="utf-8"))["editions"]
    ppl_exact, ppl_tok = load_people()
    org_exact, org_parts, org_tok = load_orgs()
    plc_exact, plc_tok = load_places()
    print(f"indexes: people={len(ppl_exact)} orgs={len(org_exact)} "
          f"places={len(plc_exact)}")

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    gaps: list[dict] = []

    def node(kind: str, raw: str, res: dict, **extra) -> str:
        nid = f"{kind}:{sm(raw) or 'blank'}"
        if nid not in nodes:
            nodes[nid] = {"id": nid, "kind": kind, "label": raw, **res, **extra}
            if res.get("status") == "GAP":
                cands = "; ".join(f"{i} {l}" for i, l in res.get("candidates", []))
                gaps.append({"kind": kind, "label": raw,
                             "reason": res.get("reason", ""),
                             "candidates": cands or res.get("note", "")})
        return nid

    for e in editions:
        folder = e.get("folder") or e.get("title")
        eid = f"edition:{folder}"
        expr = e.get("expression") or {}
        nodes[eid] = {
            "id": eid, "kind": "edition", "label": e.get("title"),
            "status": "LINKED" if e.get("expression_id") else "GAP",
            "expression_id": e.get("expression_id"),
            "yiddish_title": e.get("catalogue_yiddish_name"),
            "year_printed": e.get("year_printed"),
            "transkribus_doc_id": e.get("transkribus_doc_id"),
            "folder": folder,
            "counts": {k: len(e.get(k) or []) for k in
                       ("productions", "roles", "songs", "performance_events")},
        }

        # ---- playwright: already an id in the data, so LINKED by construction
        aid = expr.get("author_id")
        if aid:
            pid = f"person:dbid{aid}"
            nodes.setdefault(pid, {
                "id": pid, "kind": "person", "label": e.get("author"),
                "status": "LINKED", "db_id": aid,
                "matched": AUTHOR_DB_ID.get(aid, ""), "method": "author_id",
                "role": "playwright"})
            edges.append({"src": pid, "dst": eid, "rel": "wrote"})
        else:
            gaps.append({"kind": "person", "label": e.get("author") or "(blank)",
                         "reason": "edition has no author_id", "candidates": ""})

        # ---- credited people
        for r in e.get("roles") or []:
            raw = person_raw(r)
            if not raw:
                continue
            res = resolve(raw, ppl_exact, ppl_tok, decisions=PERSON_DECISION)
            # Role cells sometimes carry the character too ("actor: Lemekh");
            # keep the relation clean and move the character to its own field.
            role_raw = str(r.get("Role") or "credited").strip()
            rel, _, character = role_raw.partition(":")
            nid = node("person", raw, res, role=rel.strip().lower())
            edges.append({"src": nid, "dst": eid, "rel": rel.strip().lower(),
                          "character": character.strip() or None,
                          "source": r.get("source"), "context": r.get("context")})

        # ---- venues + premiere places, from productions
        for p in e.get("productions") or []:
            for v in split_venues(p.get("Theatre")):
                res = resolve(v, org_exact, org_tok, org_parts, decisions=ORG_DECISION)
                nid = node("org", v, res, org_role="venue")
                edges.append({"src": eid, "dst": nid, "rel": "performed_at",
                              "year": p.get("Year"), "type": p.get("Type")})
            if p.get("PremierePlace"):
                res = resolve(p["PremierePlace"], plc_exact, plc_tok)
                nid = node("place", p["PremierePlace"], res)
                edges.append({"src": eid, "dst": nid, "rel": "premiered_in",
                              "year": p.get("Year")})

        # ---- venues from the performance-events report
        for ev in e.get("performance_events") or []:
            for v in split_venues(ev.get("venue")) + split_venues(ev.get("venue_alt")):
                res = resolve(v, org_exact, org_tok, org_parts, decisions=ORG_DECISION)
                nid = node("org", v, res, org_role="venue")
                edges.append({"src": eid, "dst": nid, "rel": "performed_at",
                              "date": ev.get("date"), "type": ev.get("event_type")})

        # ---- imprint: publisher org + publication place
        if e.get("publisher"):
            res = resolve(e["publisher"], org_exact, org_tok, org_parts, decisions=ORG_DECISION)
            nid = node("org", e["publisher"], res, org_role="publisher")
            edges.append({"src": nid, "dst": eid, "rel": "published"})
        if e.get("publication_place"):
            res = resolve(e["publication_place"], plc_exact, plc_tok)
            nid = node("place", e["publication_place"], res)
            edges.append({"src": eid, "dst": nid, "rel": "printed_in"})

        # ---- field-level completeness questions.
        # Imprint fields are meaningless for the manuscript track (no publisher,
        # no print year), so only ask about them for printed editions.
        is_ms = str(e.get("folder") or "").startswith(("MS_", "YIVO_"))
        imprint_fields = () if is_ms else (
            ("year_printed", "no print year"),
            ("publisher", "no publisher"),
            ("publication_place", "no publication place"),
        )
        for field, q in imprint_fields:
            if not e.get(field):
                gaps.append({"kind": "edition-field", "label": f"{folder} · {field}",
                             "reason": q, "candidates": ""})

        # The manuscripts' holding shelfmark is often recorded in free-text
        # `notes` ("YIVO rg8-1-f4179") while the `library` column sits empty.
        # Say so, and quote the folio, rather than reporting it simply absent.
        if not e.get("library"):
            folio = re.search(r"YIVO[\s,]*(?:RG\s*8|rg8)[\w\-.:]*",
                              str(e.get("notes") or ""), re.I)
            gaps.append({
                "kind": "edition-field", "label": f"{folder} · library",
                "reason": ("holding library recorded only in notes"
                           if folio else "no holding library"),
                "candidates": folio.group(0).strip() if folio else ""})
        if not (e.get("performance_events") or []):
            gaps.append({"kind": "edition-field",
                         "label": f"{folder} · performance_events",
                         "reason": "no performance events in the DB report",
                         "candidates": ""})

    # editions.csv lists Lateiner_Meshumed twice, so dedupe the ledger.
    seen_gap, uniq_gaps = set(), []
    for g in sorted(gaps, key=lambda g: (g["kind"], g["label"])):
        sig = (g["kind"], g["label"], g["reason"])
        if sig in seen_gap:
            continue
        seen_gap.add(sig)
        uniq_gaps.append(g)

    with OUT_GAPS.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, delimiter="\t",
                           fieldnames=["kind", "label", "reason", "candidates"])
        w.writeheader()
        w.writerows(uniq_gaps)

    graph = {
        "nodes": list(nodes.values()),
        "edges": edges,
        "gaps": uniq_gaps,
        "summary": {
            "editions": sum(1 for n in nodes.values() if n["kind"] == "edition"),
            "by_kind": dict(Counter(n["kind"] for n in nodes.values())),
            "by_status": dict(Counter(n.get("status") for n in nodes.values())),
            "edges": len(edges),
            "gaps": len(uniq_gaps),
            "gaps_by_reason": dict(Counter(g["reason"] for g in uniq_gaps)),
        },
    }
    OUT_GRAPH.write_text(json.dumps(graph, ensure_ascii=False, indent=2),
                         encoding="utf-8")

    print(f"wrote {OUT_GRAPH.relative_to(REPO)}")
    print(f"wrote {OUT_GAPS.relative_to(REPO)}")
    print()
    for k, v in graph["summary"].items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
