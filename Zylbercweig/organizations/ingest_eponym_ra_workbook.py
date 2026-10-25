#!/usr/bin/env python3
"""Read a filled eponym RA workbook back into eponym_decisions.tsv.

The reviewer fills column `person_name` on the "troupes" sheet from the
dropdown (values look like `גראָדנער, רייטשעל [ריינאלדס] [1883]` -- heading
plus the person's db id in trailing brackets). This maps each answer back to a
hub_id and appends a CONFIRM row; `NONE` / `none` / `-` records a REJECT so the
span is not asked again.

Decisions are keyed (db_id, hub_id) exactly as the retired Streamlit view wrote
them, so apply_eponym_decisions.py consumes them unchanged.

  --dry-run (default)   report what would be added
  --execute             append to eponym_decisions.tsv
  --reviewer NAME       stamped on every row (required for --execute)
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import pathlib
import re
import shutil
import unicodedata

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
DECISIONS = BASE / "organizations" / "eponym_decisions.tsv"
CANDIDATES = BASE / "organizations" / "eponym_candidates.tsv"
PERSON_HUB = BASE / "people" / "person_hub.tsv"
BLOCKED = BASE / "organizations" / "eponym_blocked_answers.tsv"
QUEUE_ANSWERS = BASE / "organizations" / "eponym_queue_answers.tsv"
QUEUE_FIELDS = ["sheet", "db_id", "span", "hub_id", "answer", "comment",
                "reviewer", "reviewed_at"]

FIELDS = ["db_id", "hub_id", "span", "decision", "tier", "pattern",
          "org_name_yiddish", "person_heading", "person_db_ids",
          "reviewer", "reviewed_at", "notes"]

NONE_TOKENS = {"none", "NONE", "-", "—", "n/a", "na", "no", "nobody", "אין",
               # Ruthie's own vocabulary on the Drive workbook: "garbage" for a
               # span that is not a name at all (מיט = "with"), and "not
               # relevant" in the comments column for the same thing.
               "garbage", "not relevant", "irrelevant",
               # Added 2026-10-25 from the 10-21 workbook revision. Without
               # these, "Troupe (not person)" read as a PERSON NAME and the
               # span was silently confirmed against a candidate hub -- the
               # exact opposite of her answer (db1614 פאקט, db1624, db1630).
               "troupe (not person)", "troupe(not person)", "not a person",
               "not person"}

# Out-of-scope rather than not-a-person: the span IS someone's name, but the
# person is outside this corpus. Recorded as REJECT so the span stops being
# asked, with the reason preserved -- never folded into "no person", which
# would assert something false about the name (db1031 גארסקי "Non Jewish").
OUT_OF_SCOPE_TOKENS = {"non jewish", "non-jewish", "not jewish"}


def _name_key(s: str) -> frozenset:
    """Order-insensitive token key for a person name.

    Headings read "surname, given"; reviewers write "given surname". Comparing
    the SET of folded tokens makes those equal without inviting a substring
    match. Bracketed alternates are dropped (they are editorial), Latin is
    dropped when Hebrew is present (she writes both), single letters are
    dropped (initials must never corroborate -- `ה. ב. עפשטיין` matched
    `עפּשטיין, מ. ב.` until they were skipped).
    """
    if not s:
        return frozenset()
    s = re.sub(r"\[[^\]]*\]", " ", s)              # editorial brackets
    s = re.sub(r"\([^)]*\)", " ", s)                # parenthetical alternates
    nfd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    if re.search(r"[\u0590-\u05FF]", s):            # Hebrew present -> drop Latin
        s = re.sub(r"[A-Za-z]+", " ", s)
    for a, b in (("ם", "מ"), ("ן", "נ"), ("ך", "כ"), ("ף", "פ"), ("ץ", "צ")):
        s = s.replace(a, b)
    toks = [t for t in re.split(r"[^\w]+", s.lower()) if len(t) > 1]
    return frozenset(toks)


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workbook")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--reviewer", default="")
    args = ap.parse_args()

    if args.execute and not args.reviewer:
        raise SystemExit("--execute needs --reviewer NAME "
                         "(every decision is reviewer-stamped)")

    from openpyxl import load_workbook

    wb = load_workbook(args.workbook, data_only=True)
    ws = wb["troupes"]
    head = [c.value for c in ws[1]]
    col = {name: i for i, name in enumerate(head)}

    # people_db by folded name, so a blocked answer can say whether the person
    # merely lacks a HUB (fixable now) or lacks a db row too (needs minting).
    db_by_name: dict[frozenset, str] = {}
    for r in read_tsv(BASE / "people" / "people_db.tsv"):
        for field in ("hebname", "english", "name_variants"):
            for v in (r.get(field) or "").split("|"):
                k = _name_key(v)
                if k:
                    db_by_name.setdefault(k, r["db_id"].strip())

    hubs = read_tsv(PERSON_HUB)
    # heading+id -> hub, and db_id -> hub, for mapping the answer back.
    by_label, by_dbid = {}, {}
    # EXACT full-heading index, for a comments-column answer written as a plain
    # name rather than picked from the dropdown ("חיים סאנדלער Chaim Sandler").
    # Keyed on the FULL heading with points, finals and punctuation folded and
    # the token ORDER normalised, because headings are "surname given" while she
    # writes "given surname". Deliberately NOT a surname or substring index: a
    # partial match on קאמינסקי would hit Regina (HUB-D3612, d.1913) for an
    # answer naming Ida, which is the misattribution 9a4927395 removed.
    by_heading_exact: dict[frozenset, list] = {}
    for h in hubs:
        hd = (h.get("canonical_heading") or "").strip()
        db = (h.get("db_ids") or "").split(",")[0].strip()
        if db:
            by_dbid.setdefault(db, h)
            by_label[f"{hd} [{db}]"] = h
        else:
            # Facsimile-derived hub (HUB-P-*) with no people_db row. The builder
            # labels a proposed one `heading [hub_id]`, which matches neither the
            # bracketed db form nor the trailing-[digits] regex below, so index
            # that spelling explicitly.
            hub_id = (h.get("hub_id") or "").strip()
            if hub_id:
                by_label[f"{hd} [{hub_id}]"] = h
        key = _name_key(hd)
        if key:
            by_heading_exact.setdefault(key, []).append(h)
        by_label.setdefault(hd, h)

    cands = read_tsv(CANDIDATES)
    cand_by = {}
    for c in cands:
        cand_by.setdefault((c["db_id"], c["span"]), []).append(c)

    existing = read_tsv(DECISIONS)
    have = {f"{d['db_id']}|{d['hub_id']}" for d in existing}
    # Decisions are keyed (db_id, hub_id) because that is what an edge is, but
    # the WORKBOOK is keyed (db_id, span) -- and one org routinely names one
    # person across several spans: db1335 אַסתר-רחל קאַמינסקי fills four
    # (אברהם, אסתר, יצחק, רחל). Deduping on the pair alone recorded only the
    # first and left the other spans undecided, so the next build re-asked 31
    # questions Ruthie had already answered. Dedup on the triple instead; the
    # extra rows are harmless because apply_eponym_decisions collapses them to
    # one edge per (org, person).
    have_triples = {(d["db_id"], d["hub_id"], d.get("span") or "")
                    for d in existing}

    now = _dt.datetime.now().isoformat(timespec="seconds")
    new, unmatched, blank = [], [], 0
    unresolved_named: list[tuple] = []   # named a person we cannot map to a hub

    # She also rejects a span from the comments column, writing "not relevant"
    # there rather than in person_name (11 spans on the Drive workbook). Read
    # that as the same answer: it is a rejection either way, and ignoring it
    # left those spans in the queue to be asked again.
    cmt_col = next((h for h in head
                    if h and "comment" in str(h).lower()), None)

    for r in range(2, ws.max_row + 1):
        answer = (ws.cell(r, col["person_name"] + 1).value or "")
        answer = str(answer).strip()
        from_comment = False
        if not answer and cmt_col:
            # The comments column is a FIRST-CLASS answer source, not only a
            # place for reject words. Until 2026-10-25 it was consulted only
            # when it matched a reject token, so every free-text
            # IDENTIFICATION there was dropped -- 28 of Ruthie's answers on the
            # 10-21 workbook, and the run still reported "0 unmatched".
            note = str(ws.cell(r, col[cmt_col] + 1).value or "").strip()
            if note:
                answer = note
                from_comment = True
        if not answer:
            blank += 1
            continue
        db_id = str(ws.cell(r, col["db_id"] + 1).value or "").strip()
        span = str(ws.cell(r, col["name in troupe"] + 1).value or "").strip()
        group = cand_by.get((db_id, span)) or []
        tier = group[0]["tier"] if group else ""
        pattern = group[0]["pattern"] if group else ""
        org_y = str(ws.cell(r, col["troupe (Yiddish)"] + 1).value or "")

        out_of_scope = answer.lower() in OUT_OF_SCOPE_TOKENS
        if out_of_scope or answer in NONE_TOKENS or answer.lower() in NONE_TOKENS:
            # No person: record against the first candidate hub if there is
            # one, else a bare marker row so the span is not re-asked.
            hub_id = group[0]["hub_id"] if group and group[0].get("hub_id") else ""
            # Same triple guard as the CONFIRM path: re-running the ingest on a
            # workbook whose REJECTs are already recorded must not append them
            # twice (db1426 לאבעל got two identical REJECT rows this way).
            if (db_id, hub_id, span) in have_triples:
                continue
            have_triples.add((db_id, hub_id, span))
            new.append({
                "db_id": db_id, "hub_id": hub_id, "span": span,
                "decision": "REJECT", "tier": tier, "pattern": pattern,
                "org_name_yiddish": org_y, "person_heading": "",
                "person_db_ids": "", "reviewer": args.reviewer,
                "reviewed_at": now,
                "notes": (f"RA workbook: out of scope ({answer})"
                          if out_of_scope else "RA workbook: no person"),
            })
            continue

        hub = by_label.get(answer)
        if not hub:
            m = re.search(r"\[(\d+)\]\s*$", answer)   # trailing [id]
            if m:
                hub = by_dbid.get(m.group(1))
        if not hub and from_comment:
            # Exact full-name match only, and only when it is unambiguous.
            hits = by_heading_exact.get(_name_key(answer)) or []
            if len(hits) == 1:
                hub = hits[0]
        if not hub and from_comment:
            # A free-text identification we could not map to a hub. This is NOT
            # the same as an unparseable dropdown value: she named a real person
            # the generator never offered. Resolving it by name similarity is
            # UNSAFE -- db864 "אידא קאמינסקי" has no hub (Ida Kaminska is
            # people_db 3832, no lexicon entry), and the candidates for that
            # span are Esther-Rokhl, REGINA (HUB-D3612, d.1913) and
            # Avrom-Yitskhok. A surname match would re-create the exact
            # misattribution 9a4927395 removed. Report, never guess.
            unresolved_named.append((r, db_id, span, answer))
            continue
        if not hub:
            unmatched.append((r, db_id, span, answer))
            continue

        # Decisions are keyed (db_id, hub_id) because that is what an edge is,
        # but the WORKBOOK is keyed (db_id, span) -- and one org routinely
        # names one person across several spans: db1335 אַסתר-רחל קאַמינסקי
        # fills four (אברהם, אסתר, יצחק, רחל). Skipping on the pair alone
        # recorded only the first and left the other spans with no decision, so
        # the next build re-asked 31 questions Ruthie had already answered.
        # Skip only when this exact SPAN is already decided (see have_triples).
        if (db_id, hub["hub_id"], span) in have_triples:
            continue
        new.append({
            "db_id": db_id, "hub_id": hub["hub_id"], "span": span,
            "decision": "CONFIRM", "tier": tier, "pattern": pattern,
            "org_name_yiddish": org_y,
            "person_heading": hub.get("canonical_heading") or "",
            "person_db_ids": hub.get("db_ids") or "",
            "reviewer": args.reviewer, "reviewed_at": now,
            "notes": "RA workbook",
        })
        have_triples.add((db_id, hub["hub_id"], span))

    print(f"workbook rows:      {ws.max_row - 1}")
    print(f"  blank (unanswered): {blank}")
    print(f"  new decisions:      {len(new)} "
          f"(CONFIRM {sum(1 for d in new if d['decision'] == 'CONFIRM')}, "
          f"REJECT {sum(1 for d in new if d['decision'] == 'REJECT')})")
    # An answer that was SEEN but not turned into a decision must be counted
    # here. Five of the six bugs on this track were a reviewer's answer being
    # discarded while the summary line said success; this is the counter that
    # would have surfaced every one of them on its first run.
    print(f"  named, NO hub:      {len(unresolved_named)}"
          f"{'  <-- answers dropped; see below' if unresolved_named else ''}")
    if unresolved_named:
        print("     these name a person with no hub -- they need a hub or a mint,")
        print("     and must NOT be resolved by name similarity:")
        for row, db_id, span, answer in unresolved_named:
            print(f"     row {row}: db{db_id} span={span} -> {answer!r}")
    # Written on EVERY run, dry or not: a list that exists only in console
    # output is a list nobody can review. A UNION with the rows already there,
    # less any span since decided: a rebuilt workbook no longer carries her
    # old answers in `person_name`, so a run over it sees none of them, and a
    # rewrite from this run alone would silently drop all 18.
    blocked_rows = []
    for row, db_id, span, answer in unresolved_named:
        pdb = db_by_name.get(_name_key(answer), "")
        blocked_rows.append({
            "workbook_row": row, "org_db_id": db_id,
            # Read BOTH name columns: 4 of these rows (db545, 559,
            # 563, 596) have an empty 'troupe (Yiddish)' and carry the
            # Yiddish name in 'troupe (Latin)' instead -- the same
            # split that hides db641 לערמאַנס טרופּע from any matcher
            # reading one field.
            "org_name_yiddish": str(
                ws.cell(row, col["troupe (Yiddish)"] + 1).value
                or ws.cell(row, col["troupe (Latin)"] + 1).value or ""),
            "span": span, "reviewer_answer": answer,
            "has_people_db_row": pdb,
            # A person WITH a db row needs only add_dbonly_hubs.py; one
            # without needs a minting decision from Sinai first.
            "needs": "hub (add_dbonly_hubs.py)" if pdb else "mint, then hub",
        })
    decided = {(d["db_id"], d.get("span") or "") for d in existing + new}
    seen = {(b["org_db_id"], b["span"]) for b in blocked_rows}
    carried = [b for b in read_tsv(BLOCKED)
               if (b["org_db_id"], b["span"]) not in seen | decided]
    blocked_rows += carried
    BLOCKED.parent.mkdir(parents=True, exist_ok=True)
    with BLOCKED.open("w", encoding="utf-8", newline="") as bf:
        bw = csv.DictWriter(
            bf, fieldnames=["workbook_row", "org_db_id", "org_name_yiddish",
                            "span", "reviewer_answer", "has_people_db_row",
                            "needs"],
            delimiter="\t", lineterminator="\n")
        bw.writeheader()
        bw.writerows(blocked_rows)
    print(f"     -> {BLOCKED.name}: {len(blocked_rows)} rows "
          f"({len(carried)} carried from earlier runs)")
    print(f"  unmatched answers:  {len(unmatched)}")
    for r, db, sp, a in unmatched[:15]:
        print(f"     row {r}: db{db} {sp} -> {a!r}")

    # The "spot-check" and "blocked" sheets (added 2026-10-25) carry answers
    # that are not decisions: a verdict on a settled link, or a note toward a
    # mint. Harvested into their own ledger, keyed (sheet, db_id, span); the
    # builder writes them back into the cells, so a rebuild keeps them. Older
    # workbooks without these sheets contribute nothing.
    q_prior = {(a["sheet"], a["db_id"], a["span"]): a
               for a in read_tsv(QUEUE_ANSWERS)}
    q_new = []
    for sheet, answer_col in (("spot-check", "verdict"), ("blocked", None)):
        if sheet not in wb.sheetnames:
            continue
        qs = wb[sheet]
        qh = [c.value for c in qs[1]]
        for vals in qs.iter_rows(min_row=2, values_only=True):
            rec = dict(zip(qh, vals))
            ans = str(rec.get(answer_col) or "").strip() if answer_col else ""
            com = str(rec.get("comments") or "").strip()
            key = (sheet, str(rec.get("db_id") or "").strip(),
                   str(rec.get("span") or "").strip())
            old = q_prior.get(key, {})
            if (ans or com) and (ans, com) != (old.get("answer", ""),
                                                old.get("comment", "")):
                q_new.append({"sheet": key[0], "db_id": key[1], "span": key[2],
                              "hub_id": str(rec.get("hub_id") or ""),
                              "answer": ans, "comment": com,
                              "reviewer": args.reviewer, "reviewed_at": now})
    print(f"  queue-sheet answers: {len(q_new)} new or changed "
          f"(spot-check {sum(1 for a in q_new if a['sheet'] == 'spot-check')}, "
          f"blocked {sum(1 for a in q_new if a['sheet'] == 'blocked')})")
    for a in q_new[:15]:
        print(f"     {a['sheet']}: db{a['db_id']} {a['span']} -> "
              f"{a['answer']!r} {a['comment']!r}")

    if not args.execute:
        print("\nDRY RUN -- nothing written. Re-run with --execute --reviewer NAME.")
        return
    if q_new:
        for a in q_new:
            q_prior[(a["sheet"], a["db_id"], a["span"])] = a
        with QUEUE_ANSWERS.open("w", encoding="utf-8", newline="") as qf:
            qw = csv.DictWriter(qf, fieldnames=QUEUE_FIELDS, delimiter="\t",
                                lineterminator="\n")
            qw.writeheader()
            qw.writerows(q_prior.values())
        print(f"\nwrote {QUEUE_ANSWERS.relative_to(BASE)}  ({len(q_prior)} rows)")
    if not new:
        print("\nnothing to add.")
        return

    if DECISIONS.exists():
        stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(DECISIONS, DECISIONS.with_suffix(f".tsv.pre_ingest_{stamp}"))
    rows = existing + new
    with DECISIONS.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for d in rows:
            w.writerow({**{k: "" for k in FIELDS}, **d})
    print(f"\nwrote {DECISIONS.relative_to(BASE)}  ({len(rows)} rows total)")


if __name__ == "__main__":
    main()
