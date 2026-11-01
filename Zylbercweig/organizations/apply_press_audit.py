#!/usr/bin/env python3
"""Apply the periodical (press) audit: merges, alignments, mints, rulings, comments.

Source of truth: press_audit_decisions.tsv (one row per decision, written
BEFORE anything is applied, so a lost core_db can always be rebuilt by
re-running this script — feedback_zalmen_stale_checkout_overwrites_core_db).

Built 2026-11-01 for the Galron-press follow-up (stages 1–3 of
reference/HANDOFF.md): every Zylbercweig periodical gets exactly one live
core_db row, every cluster naming it is linked to that row on BOTH sides, and
every row carries a comment in press_comments.tsv.

Kinds, applied in this order:

  merge       loser db -> keeper db. Same procedure as
              apply_dup_merges_2026_10_31.py: clusters, name variant,
              merged_into + deprecated, alignments re-pointed, address card
              carried or re-keyed, person_org_edges re-pointed.
  unlink      cluster removed from a db row it was wrongly filed under (a
              troupe under a newspaper). The row is never deleted.
  align       cluster -> existing live db row: ALIGN + aligned_db_id on the
              cluster, and the cluster added to the row's linked_cluster_ids.
              A cluster already ALIGNed to a DIFFERENT row is reported, never
              moved (no single authority — feedback_core_db_link_authority).
  mint        one new core_db row per `group_id`; every row of the group with
              a cluster_id is aligned to it. Ids come from the HIGHER of
              origin's and the local high-water mark, read at apply time
              (feedback_core_db_mint_from_origin_high_water). A name that
              already exists among live periodical rows is refused unless the
              decision says `distinct` in its confidence.
  retype_db   a core_db row that is not a periodical gets its real org_type.
  generic     cluster decision GENERIC (no proper title; nothing minted).
  not_an_org  cluster decision NOT_AN_ORG (a book, a column, a phrase).
  retype      cluster org_type changed (e.g. a farlag typed as a newspaper);
              no decision is stamped, so it re-enters its own type's queue.
  comment     press_comments.tsv row for a db_id, or for a mint's group_id
              (resolved to the minted id).

  parent      core_db parent_db_id set (an edition under its main paper).

Clusters a human has already decided are never overwritten: an existing
decision other than the one being applied is reported and skipped -- unless
the decision says `override` in its confidence, which is reserved for a human
choosing the audit's proposal over the earlier decision (the open-items form,
2026-11-01). An overriding align also takes the cluster off the row it was on.

After merges, every core_db parent_db_id that names a merged row is re-pointed
to its keeper (a parent equal to the row itself is cleared): the 10-31 merge
of 249 into 692 had left 692's editions, and 692 itself, under 249.

Idempotent: a second --apply changes and logs nothing. Backups are written as
<file>.pre_press_audit_<TS>. Dry-run by default.

Usage: python3 apply_press_audit.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import io
import json
import re
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
ALIGN = HERE / "org_alignment_review.tsv"
ADDR = HERE / "org_addresses_review.tsv"
EDGES = HERE / "graph" / "person_org_edges.tsv"
ACTIVITY_LOG = HERE / "activity_log.tsv"
DECISIONS = HERE / "press_audit_decisions.tsv"
COMMENTS = HERE / "press_comments.tsv"

PRESS_TYPE = "Journals/ Newspapers"
COMMENT_COLUMNS = ["db_id", "name_yiddish", "name_latin", "settlement",
                   "years_attested", "language", "periodicity", "wikidata_qid",
                   "comment", "links", "galron_row_ids", "decision_id", "updated"]
LOG_COLUMNS = ("ts", "reviewer", "view", "action", "target_id", "decision",
               "note", "extra")
POINTS = re.compile(r"[֑-ׇ]")
NOW = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TODAY = _dt.date.today().isoformat()


def norm(s: str) -> str:
    return POINTS.sub("", unicodedata.normalize("NFKD", s or "")).strip()


def pipe_list(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def add_pipe(row: dict, col: str, vals: list[str]) -> bool:
    cur = pipe_list(row.get(col, ""))
    new = [v for v in vals if v and v not in cur]
    if new:
        row[col] = " | ".join(cur + new)
    return bool(new)


def live(r: dict) -> bool:
    return (r.get("deprecated", "").strip().lower() not in ("true", "1")
            and not r.get("merged_into", "").strip())


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    # keep each file's own terminator: core_db/addresses/alignment are CRLF
    term = "\r\n" if (not p.exists() or b"\r\n" in p.read_bytes()[:4096]) else "\n"
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator=term)
        w.writeheader()
        w.writerows(rows)


def origin_high_water() -> int:
    try:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=HERE,
                       capture_output=True, timeout=120, check=False)
        out = subprocess.run(
            ["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"],
            cwd=HERE, capture_output=True, text=True, timeout=120, check=True).stdout
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"cannot read origin's core_db to mint safely: {e}")
    ids = [int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t")
           if r["db_id"].strip().isdigit()]
    if not ids:
        raise SystemExit("origin core_db has no numeric db_id; refusing to mint")
    return max(ids)


def log_rows(entries: list[dict]) -> int:
    if not entries:
        return 0
    existing = ACTIVITY_LOG.read_bytes()
    header = existing.split(b"\n", 1)[0].rstrip(b"\r").decode("utf-8").split("\t")
    if tuple(header) != LOG_COLUMNS:
        raise SystemExit(f"{ACTIVITY_LOG.name} header is {header}")
    buf = io.StringIO()
    csv.DictWriter(buf, fieldnames=list(LOG_COLUMNS), delimiter="\t",
                   lineterminator="\r\n").writerows(entries)
    with ACTIVITY_LOG.open("ab") as f:
        if not existing.endswith(b"\n"):
            f.write(b"\r\n")
        f.write(buf.getvalue().encode("utf-8"))
    return len(entries)


def entry(reviewer: str, view: str, action: str, target: str, decision: str,
          note: str, **extra) -> dict:
    return {"ts": NOW, "reviewer": reviewer, "view": view, "action": action,
            "target_id": str(target), "decision": decision,
            "note": note.replace("\n", " ").replace("\t", " ").strip()[:500],
            "extra": json.dumps({"source": DECISIONS.name, **extra}, ensure_ascii=False)}


def stamp(row: dict, reviewer: str, note: str) -> None:
    row["reviewer"] = reviewer
    row["reviewed_at"] = NOW
    prev = (row.get("reviewer_notes") or "").strip()
    if note and note not in prev:
        row["reviewer_notes"] = f"{prev} | {note}" if prev else note


def main(apply: bool, reviewer: str) -> int:
    core_f, core = _load(CORE_DB)
    by_id = {r["db_id"].strip(): r for r in core}
    al_f, al = _load(ALIGN)
    rv = {r["cluster_id"].strip(): r for r in al}
    _, decisions = _load(DECISIONS)
    log: list[dict] = []
    report: dict[str, list[str]] = {}
    touched = {"core": False, "align": False, "addr": False, "edges": False}

    def say(k: str, msg: str) -> None:
        report.setdefault(k, []).append(msg)

    # ---------------- 1. merges ----------------
    merges = {d["db_id"].strip(): (d["value"].strip(), d) for d in decisions
              if d["kind"] == "merge"}
    for lo, (kp, d) in merges.items():
        if lo not in by_id or kp not in by_id:
            raise SystemExit(f"{d['decision_id']}: missing db_id {lo} or {kp}")
        if lo == kp or merges.get(kp):
            raise SystemExit(f"{d['decision_id']}: merge chain/self-merge {lo}->{kp}")
    for lo, (kp, d) in merges.items():
        lrow, krow = by_id[lo], by_id[kp]
        if lrow.get("merged_into", "").strip() == kp:
            continue
        if not live(krow):
            say("refused", f"{d['decision_id']}: keeper {kp} is not live")
            continue
        moved = pipe_list(lrow["linked_cluster_ids"])
        add_pipe(krow, "linked_cluster_ids", moved)
        lname = (lrow["name_yiddish"] or lrow["name"]).strip()
        known = [krow["name"], krow["name_yiddish"]] + pipe_list(krow.get("name_variants", ""))
        if lname and not any(norm(v) == norm(lname) for v in known):
            add_pipe(krow, "name_variants", [lname])
        lrow["merged_into"], lrow["deprecated"], lrow["linked_cluster_ids"] = kp, "true", ""
        touched["core"] = True
        say("merge", f"{d['decision_id']}  db {lo} -> {kp}  clusters {moved}")
        log.append(entry(reviewer, "org_addresses", "db_merge", kp, "MERGE_INTO",
                         f"{d['decision_id']}: merged db {lo} into {kp}: {d['rationale']}",
                         loser_id=lo, survivor_id=kp, clusters_moved=moved))
    if merges:
        for r in al:
            lo = r.get("aligned_db_id", "").strip()
            if lo in merges and by_id[lo].get("merged_into", "").strip() == merges[lo][0]:
                kp = merges[lo][0]
                r["aligned_db_id"] = kp
                add_pipe(by_id[kp], "linked_cluster_ids", [r["cluster_id"]])
                touched["align"] = touched["core"] = True
        ad_f, ad = _load(ADDR)
        ad_by = {r["db_id"].strip(): r for r in ad}
        owner = {c: r["db_id"].strip() for r in core if live(r)
                 for c in pipe_list(r["linked_cluster_ids"])}
        drop = set()
        for lo, (kp, _) in merges.items():
            lcard, kcard = ad_by.get(lo), ad_by.get(kp)
            if lcard is None:
                continue
            carried = [c for c in pipe_list(lcard["linked_cluster_ids"])
                       if owner.get(c, kp) == kp]
            if kcard is None:
                lcard["db_id"] = kp
                lcard["linked_cluster_ids"] = " | ".join(pipe_list(by_id[kp]["linked_cluster_ids"]))
                lcard["canonical_yiddish"] = by_id[kp]["name_yiddish"] or by_id[kp]["name"]
                ad_by[kp] = lcard
            else:
                add_pipe(kcard, "linked_cluster_ids", carried)
                for c in ("confirmed_settlement", "confirmed_settlement_yiddish",
                          "confirmed_address", "confirmed_address_romanized", "lat", "lon"):
                    if not (kcard.get(c) or "").strip() and (lcard.get(c) or "").strip():
                        kcard[c] = lcard[c]
                lnote = (lcard.get("reviewer_notes") or "").strip()
                if lnote:
                    tag = f"[merged from db_id {lo}] {lnote}"
                    knote = (kcard.get("reviewer_notes") or "").strip()
                    kcard["reviewer_notes"] = f"{knote}\n---\n{tag}" if knote else tag
                drop.add(id(lcard))
            touched["addr"] = True
        if touched["addr"]:
            ad = [r for r in ad if id(r) not in drop]
            for r in ad:
                if r.get("parent_db_id", "").strip() in merges:
                    r["parent_db_id"] = merges[r["parent_db_id"].strip()][0]
        ed_f, ed = _load(EDGES)
        for r in ed:
            lo = r.get("db_id", "").strip()
            if lo in merges and by_id[lo].get("merged_into", "").strip() == merges[lo][0]:
                r["db_id"] = merges[lo][0]
                touched["edges"] = True

    def resolve(db: str) -> str:
        seen = set()
        while db in by_id and by_id[db].get("merged_into", "").strip() and db not in seen:
            seen.add(db)
            db = by_id[db]["merged_into"].strip()
        return db

    def link(cid: str, db: str, d: dict, why: str) -> None:
        cl = rv.get(cid)
        if cl is None:
            say("refused", f"{d['decision_id']}: cluster {cid} not in {ALIGN.name}")
            return
        cur, cur_db = (cl.get("decision") or "").strip(), (cl.get("aligned_db_id") or "").strip()
        if "override" in d["confidence"].lower() and cur and not (cur in ("ALIGN", "NEW") and resolve(cur_db) == db):
            for r in core:
                if r["db_id"] != db and cid in pipe_list(r["linked_cluster_ids"]):
                    r["linked_cluster_ids"] = " | ".join(c for c in pipe_list(r["linked_cluster_ids"]) if c != cid)
                    touched["core"] = True
                    say("override", f"{d['decision_id']}: {cid} taken off db {r['db_id']}")
            say("override", f"{d['decision_id']}: {cid} {cur} {cur_db} -> ALIGN {db}")
            cur, cur_db = "", ""
        if cur in ("ALIGN", "NEW") and cur_db and resolve(cur_db) != db:
            say("conflict", f"{d['decision_id']}: {cid} already ALIGNed to {cur_db}; not moved to {db}")
            return
        if cur and cur not in ("ALIGN", "NEW"):
            say("conflict", f"{d['decision_id']}: {cid} already decided {cur}; linked from core_db only")
            if add_pipe(by_id[db], "linked_cluster_ids", [cid]):
                touched["core"] = True
            return
        changed = False
        # a NEW cluster already on this row is the app's own mint: leave its decision
        if not (cur == "NEW" and cur_db == db) and (cur != "ALIGN" or cur_db != db):
            cl["decision"], cl["aligned_db_id"] = "ALIGN", db
            stamp(cl, reviewer, f"press audit {TODAY} ({d['decision_id']}): {why}")
            changed = touched["align"] = True
        if add_pipe(by_id[db], "linked_cluster_ids", [cid]):
            changed = touched["core"] = True
        if changed:
            log.append(entry(reviewer, "org_review", "alignment", cid, "ALIGN",
                             f"{d['decision_id']}: {why}", aligned_db_id=db))

    # ---------------- 1b. parents ----------------
    for r in core:
        p = r.get("parent_db_id", "").strip()
        if p and p in by_id and not live(by_id[p]):
            q = resolve(p)
            r["parent_db_id"] = "" if q == r["db_id"] else q
            touched["core"] = True
            say("parent", f"db {r['db_id']}: parent {p} (merged) -> {r['parent_db_id'] or '(none)'}")
    for d in decisions:
        if d["kind"] != "parent":
            continue
        row, par = by_id.get(d["db_id"].strip()), d["value"].strip()
        if row is None or par not in by_id or not live(by_id[par]):
            say("refused", f"{d['decision_id']}: db {d['db_id']} or parent {par} missing/not live")
            continue
        if row.get("parent_db_id", "").strip() != par:
            log.append(entry(reviewer, "org_addresses", "set_parent", row["db_id"], par,
                             f"{d['decision_id']}: {d['rationale']}", was=row.get("parent_db_id", "")))
            row["parent_db_id"] = par
            touched["core"] = True
            say("parent", f"{d['decision_id']}  db {row['db_id']} parent -> {par}")

    # ---------------- 2a. unlinks (a cluster filed under the wrong row) ----------------
    # Never deletes the row, unlike apply_db_audit_decisions.py: a row left
    # empty here usually receives its real clusters in step 2b. The REMOVE is
    # also recorded in db_audit_decisions.tsv by the ledger builder, so repair
    # passes honour it (feedback_core_db_link_authority).
    for d in decisions:
        if d["kind"] != "unlink":
            continue
        db, cid = d["db_id"].strip(), d["cluster_id"].strip()
        row = by_id.get(db)
        if row is None:
            say("refused", f"{d['decision_id']}: no db {db}")
            continue
        ids = pipe_list(row["linked_cluster_ids"])
        cl = rv.get(cid)
        hit = False
        if cid in ids:
            row["linked_cluster_ids"] = " | ".join(c for c in ids if c != cid)
            touched["core"] = hit = True
        if cl is not None and (cl.get("aligned_db_id") or "").strip() == db:
            cl["aligned_db_id"] = ""
            if (cl.get("decision") or "").strip() in ("ALIGN", "NEW"):
                cl["decision"] = ""
            stamp(cl, reviewer, f"press audit {TODAY} ({d['decision_id']}): unlinked from db {db} — {d['rationale']}")
            touched["align"] = hit = True
        if hit:
            say("unlink", f"{d['decision_id']}  {cid} x db {db}")
            log.append(entry(reviewer, "org_review", "alignment", cid, "REMOVE",
                             f"{d['decision_id']}: {d['rationale']}", db_id=db))

    # ---------------- 2b. aligns ----------------
    for d in decisions:
        if d["kind"] != "align":
            continue
        db = resolve(d["db_id"].strip())
        if db not in by_id or not live(by_id[db]):
            say("refused", f"{d['decision_id']}: db {d['db_id']} is not a live row")
            continue
        link(d["cluster_id"].strip(), db, d, f"aligned to db {db}")

    # ---------------- 3. mints ----------------
    groups: dict[str, list[dict]] = {}
    for d in decisions:
        if d["kind"] == "mint":
            groups.setdefault(d["group_id"].strip(), []).append(d)
    minted: dict[str, str] = {}
    live_press = {norm(r["name_yiddish"]): r["db_id"] for r in core
                  if live(r) and r["name_yiddish"].strip()
                  and re.search("journal|newspaper", r["org_type"], re.I)}
    next_id = None
    for gid, ds in groups.items():
        head = ds[0]
        cids = [d["cluster_id"].strip() for d in ds if d["cluster_id"].strip()]
        name = head["name_yiddish"].strip()
        # already applied: a live row created by this ledger for this group
        prior = [r for r in core if live(r) and f"press-audit {gid}" in (r.get("name_variants") or "")]
        if not prior and cids:
            prior = [r for r in core if live(r)
                     and set(cids) <= set(pipe_list(r["linked_cluster_ids"]))]
        if prior:
            minted[gid] = prior[0]["db_id"]
            for d in ds:
                if d["cluster_id"].strip():
                    link(d["cluster_id"].strip(), minted[gid], d, f"minted db {minted[gid]} {name}")
            continue
        if not name:
            say("refused", f"{head['decision_id']}: no name_yiddish; nothing minted")
            continue
        clash = live_press.get(norm(name))
        if clash and "distinct" not in head["confidence"].lower():
            say("refused", f"{head['decision_id']}: {name} already names live db {clash}")
            continue
        if next_id is None:
            local_max = max((int(r["db_id"]) for r in core if r["db_id"].strip().isdigit()), default=0)
            next_id = max(origin_high_water(), local_max) + 1
        db = str(next_id)
        next_id += 1
        row = {f: "" for f in core_f}
        row.update(db_id=db, name=head["name_latin"].strip(), name_yiddish=name,
                   org_type=PRESS_TYPE, address=head["settlement"].strip(),
                   name_variants=f"press-audit {gid}")
        core.append(row)
        by_id[db] = row
        live_press[norm(name)] = db
        minted[gid] = db
        touched["core"] = True
        say("mint", f"{head['decision_id']}  db {db}  {name} / {row['name']} ({row['address']})  <- {', '.join(cids) or '(no cluster)'}")
        log.append(entry(reviewer, "org_review", "mint", db, "NEW",
                         f"{head['decision_id']}: {head['rationale']}",
                         name_yiddish=name, clusters=cids, org_type=PRESS_TYPE))
        for d in ds:
            if d["cluster_id"].strip():
                link(d["cluster_id"].strip(), db, d, f"minted db {db} {name}")

    # ---------------- 4a. rows that are not periodicals ----------------
    for d in decisions:
        if d["kind"] != "retype_db":
            continue
        row = by_id.get(d["db_id"].strip())
        if row is None or not live(row):
            say("refused", f"{d['decision_id']}: db {d['db_id']} not live")
            continue
        if row["org_type"].strip() != d["value"].strip():
            say("retype", f"{d['decision_id']}  db {row['db_id']} {row['org_type']} -> {d['value']}")
            log.append(entry(reviewer, "org_review", "retype_db", row["db_id"], d["value"],
                             f"{d['decision_id']}: {d['rationale']}", was=row["org_type"]))
            row["org_type"] = d["value"].strip()
            touched["core"] = True

    # ---------------- 4b. rulings on clusters ----------------
    for d in decisions:
        k = d["kind"]
        if k not in ("generic", "not_an_org", "retype"):
            continue
        cid = d["cluster_id"].strip()
        cl = rv.get(cid)
        if cl is None:
            say("refused", f"{d['decision_id']}: cluster {cid} not in {ALIGN.name}")
            continue
        if k == "retype":
            if (cl.get("org_type") or "").strip() != d["value"].strip():
                cl["org_type"] = d["value"].strip()
                prev = (cl.get("reviewer_notes") or "").strip()
                note = f"press audit {TODAY} ({d['decision_id']}): retyped {d['value']} — {d['rationale']}"
                cl["reviewer_notes"] = f"{prev} | {note}" if prev else note
                touched["align"] = True
                say("retype", f"{d['decision_id']}  {cid} -> {d['value']}")
                log.append(entry(reviewer, "org_review", "retype", cid, d["value"], d["rationale"]))
            continue
        want = "GENERIC" if k == "generic" else "NOT_AN_ORG"
        cur = (cl.get("decision") or "").strip()
        if cur == want:
            continue
        if cur:
            say("conflict", f"{d['decision_id']}: {cid} already decided {cur}; {want} not applied")
            continue
        cl["decision"] = want
        stamp(cl, reviewer, f"press audit {TODAY} ({d['decision_id']}): {d['rationale']}")
        touched["align"] = True
        say(k, f"{d['decision_id']}  {cid} {want}")
        log.append(entry(reviewer, "org_review", "alignment", cid, want, d["rationale"]))

    # ---------------- 5. comments ----------------
    cm_rows: list[dict] = []
    if COMMENTS.exists():
        _, cm_rows = _load(COMMENTS)
    cm = {r["db_id"]: r for r in cm_rows}
    n_cm = 0
    # the first comment decision for a row wins, unless a later one says
    # `override` (a rewrite after a merge or split): the last override wins
    chosen: dict[str, dict] = {}
    for d in decisions:
        if d["kind"] != "comment":
            continue
        db = minted.get(d["group_id"].strip()) if not d["db_id"].strip() else resolve(d["db_id"].strip())
        if not db or db not in by_id:
            say("refused", f"{d['decision_id']}: comment target unresolved ({d['group_id']}/{d['db_id']})")
            continue
        if db not in chosen or "override" in d["confidence"].lower():
            chosen[db] = d
    for db, d in chosen.items():
        payload = json.loads(d["value"])
        new = {c: "" for c in COMMENT_COLUMNS}
        new.update({k: v for k, v in payload.items() if k in new})
        new.update(db_id=db, name_yiddish=by_id[db]["name_yiddish"], decision_id=d["decision_id"])
        old = cm.get(db)
        if old and all(old.get(c, "") == new[c] for c in COMMENT_COLUMNS if c != "updated"):
            continue
        new["updated"] = TODAY
        cm[db] = new
        n_cm += 1

    for k in ("merge", "parent", "override", "unlink", "mint", "generic", "not_an_org", "retype", "conflict", "refused"):
        if report.get(k):
            print(f"\n== {k} ({len(report[k])})")
            for m in report[k][:400]:
                print("  " + m)
    n_link = sum(1 for e in log if e["action"] == "alignment" and e["decision"] == "ALIGN")
    print(f"\nlinks written: {n_link}   comments new/changed: {n_cm}   log rows: {len(log)}")
    if not apply:
        print("\nDRY-RUN — nothing written. Re-run with --apply --reviewer NAME.")
        return 0
    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    for flag, path, fields, rows in (("core", CORE_DB, core_f, core), ("align", ALIGN, al_f, al)):
        if touched[flag]:
            shutil.copy2(path, path.with_suffix(f".tsv.pre_press_audit_{ts}"))
            _dump(path, fields, rows)
    if touched["addr"]:
        shutil.copy2(ADDR, ADDR.with_suffix(f".tsv.pre_press_audit_{ts}"))
        _dump(ADDR, ad_f, ad)
    if touched["edges"]:
        _dump(EDGES, ed_f, ed)
    if n_cm:
        _dump(COMMENTS, COMMENT_COLUMNS, sorted(cm.values(), key=lambda r: int(r["db_id"])))
    print(f"wrote {[k for k, v in touched.items() if v]}{' + comments' if n_cm else ''}; "
          f"logged {log_rows(log)} rows as {reviewer!r}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    raise SystemExit(main(a.apply, a.reviewer.strip()))
