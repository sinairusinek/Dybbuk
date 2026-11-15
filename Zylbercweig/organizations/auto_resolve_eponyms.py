#!/usr/bin/env python3
"""Resolve the eponym questions a machine can settle safely; leave the rest.

Two rules, both calibrated against the 450 human rows in eponym_decisions.tsv
(2026-11-15) before being trusted -- see CALIBRATION below.

FULLNAME  The org name spells the person's full name -- given name + surname,
          or initials + surname -- inside ONE name segment (segments split on
          `,` `/` `&` `און`, so "קאָניעווסקא, ברייטמאַן" is never read as one
          person), the form belongs to exactly one hub, AND the given name agrees
          with that hub's own heading (compared on consonant skeletons, so
          `מאָריס`/`מאריס`, `אייווען`/`אייוואָן` agree). The heading check matters:
          validated surfaces carry relatives' names -- `לואיס קרעמער` sits on Ida
          Kremer's hub and `בער האַרט` (Bernard) on Berta Hart's.
          Guard: if another hub with the same surname has a given name that ALSO
          appears in the org name (Jacob P. and Sara Adler), it abstains.

PAIR      The question's span names a hub that a human already CONFIRMED for
          this org through a different span (`מאַלווינע` confirmed -> the
          `לאָבעל` question is redundant). Only when exactly one confirmed hub of
          the org fits the span.

Rules that were tested and REJECTED -- do not re-add them:
  * unique surname (one hub with that surname): humans rejected ~10% (23/239).
  * self-mention (the candidate's own entry mentions the troupe): 9 of 18
    wrong -- it is the eponym's wife/sister/brother who performed in it
    (Berta for Ber Hart, Regina for A. Y. Kaminski, Anna for Adolf Freiman).
  * initials alone against a surface (`מ. וואָלקענשטיין` is a surface of David).

CALIBRATION (FULLNAME on decided rows): 137 agree, 3 disagree -- all three are
ledger slips, not rule errors (db841 Ruthie `אננא` -> A. L. Yakubovitch;
db1356 Ruthie `קארל` -> Spivakovsky; db1426 Ruthie REJECT `לאבעל` while
confirming Malvina Lobel on `מאלווינע`).

Writes (only with --write), reviewer `auto`:
  * eponym_decisions.tsv     CONFIRM rows for open questions it settles
  * eponym_queue_answers.tsv `OK` rows (sheet=spot-check) for spot-check rows
Never edits or removes an existing row. Afterwards run
apply_eponym_decisions.py (dry-run first) and build_eponym_ra_workbook.py.

  python3.11 auto_resolve_eponyms.py            # report
  python3.11 auto_resolve_eponyms.py --write
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import pathlib
import re
import unicodedata

csv.field_size_limit(10 ** 9)

HERE = pathlib.Path(__file__).resolve().parent
BASE = HERE.parent
DECISIONS = HERE / "eponym_decisions.tsv"
QUESTIONS = HERE / "eponym_open_questions.tsv"
SPOTCHECK = HERE / "eponym_spotcheck_queue.tsv"
QUEUE_ANSWERS = HERE / "eponym_queue_answers.tsv"
PERSON_HUB = BASE / "people" / "person_hub.tsv"
HUB_MEMBERS = BASE / "people" / "person_hub_members.tsv"
REPORT = HERE / "eponym_auto_resolution_report.tsv"

REVIEWER = "auto"
SEG_SPLIT = re.compile(r"[,/&;]|\sאון\s|\sאָון\s")
BRACKETS = re.compile(r"\[[^\]]*\]|\([^)]*\]?\)?")


def fold(s: str) -> str:
    if not s:
        return ""
    s = s.replace("ײ", "יי").replace("װ", "וו").replace("ױ", "וי")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    for a, b in (("ם", "מ"), ("ן", "נ"), ("ך", "כ"), ("ף", "פ"), ("ץ", "צ")):
        s = s.replace(a, b)
    return re.sub(r"[^\w]+", " ", s).lower().strip()


def skel(t: str) -> str:
    """Consonant skeleton: drop the Yiddish vowel letters and a possessive -ס."""
    t = re.sub(r"[אעויה]", "", t)
    return t


def compat(a: str, b: str) -> bool:
    if a == b:
        return True
    sa, sb = skel(a), skel(b)
    if len(sa) < 2 or len(sb) < 2:
        return False
    return sa == sb or (sa.endswith("ס") and sa[:-1] == sb)


def read(p):
    with open(p, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


# ── hub index ────────────────────────────────────────────────────────────────
def split_heading(h: str):
    """-> (surname tokens, given tokens incl. bracketed alternates)."""
    alts = re.findall(r"\[([^\]]*)\]|\(([^)\]]*)[)\]]", h)
    core = re.sub(r"\[[^\]]*\]|\([^)\]]*[)\]]", "", h)
    if "," in core:
        sur, giv = core.split(",", 1)
    else:  # `גאָלדענבערג איציקל`, `שלום-עליכם`, `מענדעלע מוכר ספרים`
        parts = core.split()
        sur, giv = (parts[0], " ".join(parts[1:])) if parts else ("", "")
    giv = re.sub(r"\bדר\b\.?", "", giv)
    gt = set(fold(giv).split())
    for a in alts:
        gt |= set(fold(" ".join(x for x in a if x)).split())
    return set(fold(sur).split()), {t for t in gt if t}


class Index:
    def __init__(self):
        self.head = {r["hub_id"]: r["canonical_heading"] for r in read(PERSON_HUB)}
        self.split = {h: split_heading(v) for h, v in self.head.items()}
        self.by_sur = collections.defaultdict(set)
        for h, (sur, _) in self.split.items():
            for t in sur:
                if len(t) >= 2:
                    self.by_sur[t].add(h)
        self.forms = collections.defaultdict(set)
        for h, (sur, giv) in self.split.items():
            if sur and giv and "," in self.head[h]:
                s = " ".join(sorted(sur, key=self.head[h].find))
                g = fold(BRACKETS.sub("", self.head[h].split(",", 1)[1]))
                g = re.sub(r"\bדר\b", "", g).strip()
                if g:
                    self.forms[f"{g} {s}"].add(h)
                    self.forms[f"{g.split()[0]} {s}"].add(h)
        for r in read(HUB_MEMBERS):
            if r["member_kind"] in ("surface", "db"):
                f = fold(r["label"])
                if len(f.split()) >= 2:
                    self.forms[f].add(r["hub_id"])
        self.by_len = sorted(self.forms, key=len, reverse=True)

    def name_ok(self, hub: str, form: str) -> bool:
        """Every token of the matched form agrees with the hub's OWN heading."""
        sur, giv = self.split.get(hub, (set(), set()))
        names = sur | giv
        for t in form.split():
            if len(t) == 1:  # an initial: must open some given name
                if not any(g.startswith(t) for g in giv):
                    return False
                continue
            if not any(compat(t, n) for n in names):
                return False
        return True

    def surname_of(self, hub, form):
        sur = self.split.get(hub, (set(), set()))[0]
        return [t for t in form.split() if any(compat(t, s) for s in sur)]


# Function words that precede a surname in org names. `פֿון` has the skeleton
# of `פעני`, so without this "טרופּע פֿון שוואַרץ" read as Feni Schwartz.
STOP = {fold(w) for w in """פֿון פון פאָן מיט און ביי בײַ דער די דאָס דעם א אַ
    טרופּע טרופע אויפֿן נאָמען פֿאַרלאַג קאָלעקטיוו א'א א'אַ""".split()}


def compat_any(t: str, names) -> bool:
    return len(t) >= 2 and t not in STOP and any(compat(t, n) for n in names)


def span_hits(span_f: str, tok: str) -> bool:
    return len(tok) >= 2 and (span_f.startswith(tok) or tok.startswith(span_f)
                              or compat(span_f, tok))


def rule_fullname(ix: Index, org: str, span: str):
    sf = fold(span)
    if not sf:
        return None, "no_span"
    org_toks = set(fold(org).split())
    found = {}
    for seg in SEG_SPLIT.split(org or ""):
        o = " " + fold(seg) + " "
        if not any(span_hits(sf, t) for t in o.split()):
            continue
        for f in ix.by_len:
            if (" " + f + " ") not in o and (" " + f + "ס ") not in o:
                continue
            if not any(span_hits(sf, t) for t in f.split()):
                continue
            # initials-only given part needs at least one full token besides surname
            for h in ix.forms[f]:
                if ix.name_ok(h, f):
                    found.setdefault(h, f)
        # Heading pairs: `<given> <surname>` where the SURNAME matches a hub's
        # heading exactly (a possessive -ס allowed) and the given name agrees by
        # skeleton (`מישע`/`מישאַ`). Surnames are never skeleton-matched:
        # `גאָלדענבערג` and `גאָלדענבורג` share one and are different people.
        toks = fold(seg).split()
        for i in range(1, len(toks)):
            t = toks[i]
            for sur in {t, t[:-1] if t.endswith("ס") else t}:
                for h in ix.by_sur.get(sur, ()):
                    giv = ix.split[h][1]
                    if compat_any(toks[i - 1], giv) and (
                            span_hits(sf, sur) or span_hits(sf, toks[i - 1])):
                        found.setdefault(h, f"{toks[i - 1]} {sur}")
    if not found:
        return None, "no_fullname"
    if len(found) > 1:
        return None, "fullname_several_hubs:" + "|".join(sorted(found))
    hub, form = next(iter(found.items()))
    own = ix.split[hub][1]
    inits = [t for t in form.split() if len(t) == 1]
    for s in ix.surname_of(hub, form):
        for r in ix.by_sur.get(s, set()) - {hub}:
            # `מ. גאָלדבערג` / `ה. ווייסמאַן`: an initial picks nobody when a
            # rival with the same surname has given names opening the same way.
            if inits:
                rg = fold(BRACKETS.sub("", ix.head[r].split(",", 1)[-1])).split()
                if [g[0] for g in rg[:len(inits)]] == inits:
                    return None, f"initials_shared_with:{r}"
            clash = [g for g in ix.split[r][1] - own
                     if len(g) >= 2 and g in org_toks]
            if clash:
                return None, f"rival_given_in_org:{r}:{clash[0]}"
    return (hub, form), "fullname"


def rule_pair(ix: Index, db: str, span: str, confirmed: dict):
    sf = fold(span)
    fits = [h for h in confirmed.get(db, ())
            if any(span_hits(sf, t) and compat(sf.rstrip("ס") or sf, t) or sf == t
                   for t in ix.split.get(h, (set(), set()))[0] | ix.split.get(h, (set(), set()))[1])]
    if len(fits) == 1:
        return fits[0], "pair_already_confirmed"
    return None, ("pair_ambiguous" if fits else "no_pair")


# ── main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()
    ix = Index()
    dec = read(DECISIONS)
    dec_fields = list(dec[0].keys())
    confirmed = collections.defaultdict(set)
    # A span is settled by a CONFIRM, or by a hub-less REJECT ("not a name").
    # A REJECT naming a hub only rules out that person -- the span stays open,
    # exactly as build_eponym_ra_workbook.py treats it.
    decided_span, rejected = set(), set()
    for d in dec:
        if d["decision"] == "CONFIRM" or not d["hub_id"]:
            decided_span.add((d["db_id"], d["span"]))
        else:
            rejected.add((d["db_id"], d["span"], d["hub_id"]))
        if d["decision"] == "CONFIRM" and d["hub_id"] and d["reviewer"] != REVIEWER:
            confirmed[d["db_id"]].add(d["hub_id"])
    now = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    report, new_dec, new_ok = [], [], []

    # 1. open troupe questions
    for q in read(QUESTIONS):
        db, span = q["db_id"], q["span"]
        if (db, span) in decided_span:
            report.append(dict(surface="open_q", db_id=db, org=q["troupe_yiddish"], span=span,
                               hub="", heading="", outcome="already_decided", why="stale question"))
            continue
        hit, why = rule_fullname(ix, q["troupe_yiddish"], span)
        hub = hit[0] if hit else None
        if not hub:
            hub, why2 = rule_pair(ix, db, span, confirmed)
            why = why2 if hub else why
        if hub and (db, span, hub) in rejected:
            why, hub = f"would confirm {hub}, which a human REJECTED for this span", None
        if hub:
            new_dec.append({**{k: "" for k in dec_fields}, "db_id": db, "hub_id": hub, "span": span,
                            "decision": "CONFIRM", "tier": q["tier"], "pattern": q["pattern"],
                            "org_name_yiddish": q["troupe_yiddish"], "person_heading": ix.head.get(hub, ""),
                            "person_db_ids": hub[5:] if hub.startswith("HUB-D") else "",
                            "reviewer": REVIEWER, "reviewed_at": now,
                            "notes": f"auto_resolve_eponyms: {why}" + (f" `{hit[1]}`" if hit else "")})
        report.append(dict(surface="open_q", db_id=db, org=q["troupe_yiddish"], span=span,
                           hub=hub or "", heading=ix.head.get(hub, "") if hub else "",
                           outcome="AUTO_CONFIRM" if hub else "human", why=why))

    # 2. spot-check rows not yet answered
    answered = {(r["db_id"], r["span"], r["hub_id"]) for r in read(QUEUE_ANSWERS)
                if r["sheet"] == "spot-check"}
    sc = read(SPOTCHECK)
    flagged = {(r["db_id"], r["span"], r["hub_id"]) for r in sc}
    span_of = collections.defaultdict(set)
    for d in dec:
        if d["decision"] == "CONFIRM" and d["reviewer"] != REVIEWER:
            span_of[(d["db_id"], d["hub_id"])].add(d["span"])
    for r in sc:
        k = (r["db_id"], r["span"], r["hub_id"])
        if k in answered:
            continue
        hit, why = rule_fullname(ix, r["org_name"], r["span"])
        if hit and hit[0] != r["hub_id"]:
            outcome = "CONTRADICTS"
            why = f"org name spells {ix.head.get(hit[0])} [{hit[0]}]"
        elif hit:
            outcome = "AUTO_OK"
        else:
            sib = [s for s in span_of[(r["db_id"], r["hub_id"])] if s != r["span"]
                   and ((r["db_id"], s, r["hub_id"]) not in flagged
                        or (r["db_id"], s, r["hub_id"]) in answered)]
            if sib:
                outcome, why = "AUTO_OK", f"pair corroborated by span `{sib[0]}`"
            else:
                outcome = "human"
        if outcome == "AUTO_OK":
            new_ok.append({"sheet": "spot-check", "db_id": r["db_id"], "span": r["span"],
                           "hub_id": r["hub_id"], "answer": "OK",
                           "comment": f"auto_resolve_eponyms: {why}",
                           "reviewer": REVIEWER, "reviewed_at": now})
        report.append(dict(surface="spot_check", db_id=r["db_id"], org=r["org_name"], span=r["span"],
                           hub=r["hub_id"], heading=r["person_heading"], outcome=outcome, why=why))

    # 3. human decisions the rule contradicts -- REPORTED, never changed
    for d in dec:
        if d["reviewer"] == REVIEWER:
            continue
        hit, _ = rule_fullname(ix, d["org_name_yiddish"], d["span"])
        if not hit:
            continue
        bad = (d["decision"] == "CONFIRM" and d["hub_id"] != hit[0]) or \
              (d["decision"] == "REJECT" and d["hub_id"] == hit[0])
        if bad:
            report.append(dict(surface="ledger", db_id=d["db_id"], org=d["org_name_yiddish"],
                               span=d["span"], hub=d["hub_id"], heading=d["person_heading"],
                               outcome="SUSPECT_HUMAN_ROW",
                               why=f"{d['reviewer']} {d['decision']}; org name spells "
                                   f"{ix.head.get(hit[0])} [{hit[0]}] `{hit[1]}`"))

    c = collections.Counter((r["surface"], r["outcome"]) for r in report)
    for k in sorted(c):
        print(f"  {k[0]:11} {k[1]:18} {c[k]}")
    for r in report:
        if r["outcome"] not in ("human",):
            print(f"  [{r['surface']}] {r['outcome']:17} db{r['db_id']} {r['org']} | {r['span']} -> "
                  f"{r['heading']} {r['hub']} :: {r['why']}")

    with open(REPORT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(report[0]), delimiter="\t", lineterminator="\n")
        w.writeheader(); w.writerows(report)
    print(f"\nreport -> {REPORT.name}")
    if not a.write:
        print(f"(dry run) would add {len(new_dec)} decisions, {len(new_ok)} spot-check OKs")
        return 0
    with open(DECISIONS, "a", encoding="utf-8", newline="") as f:
        csv.DictWriter(f, fieldnames=dec_fields, delimiter="\t", lineterminator="\n").writerows(new_dec)
    qa_fields = list(read(QUEUE_ANSWERS)[0].keys())
    with open(QUEUE_ANSWERS, "a", encoding="utf-8", newline="") as f:
        csv.DictWriter(f, fieldnames=qa_fields, delimiter="\t", lineterminator="\n").writerows(new_ok)
    print(f"wrote {len(new_dec)} decisions, {len(new_ok)} spot-check OKs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
