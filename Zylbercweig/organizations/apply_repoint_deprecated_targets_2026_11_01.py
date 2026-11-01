"""Re-point alignments that still target a deprecated core_db row (2026-11-01).

Earlier merges deprecated the loser row (deprecated=true, merged_into=keeper)
but left some review rows with aligned_db_id = the loser. Readers that do not
follow merged_into treat those clusters as linked to a dead row.

For every review row whose aligned_db_id is deprecated:
  - follow merged_into to the live end of the chain;
  - set aligned_db_id to it; the decision is kept (NEW stays NEW, ALIGN stays
    ALIGN), as settlement_audit's merge cascade does;
  - add the cluster to the keeper's linked_cluster_ids in core_db and on its
    address card, and drop it from the dead row's.
A cluster that core_db links to some OTHER live row is reported, not moved.

Deprecated rows with no merged_into are Ruthie's 2026-10-04 troupe answers
(apply_ruthie_doc_answers.py), which left aligned_db_id alone on purpose:
  - a dispatched BUNDLE ("troupes of X, Y and Z") — its one mention is now in
    the linked_cluster_ids of every part's live row. aligned_db_id takes the
    lowest of them, the row kg_orgrel / build_person_org_graph already pick;
    the full set stays in core_db and is named in reviewer_notes.
  - a REMOVE — the cluster gets the decision in REMOVED, link cleared.

OVERRIDES: merges that followed a wrong name. db 727/728 were coined as
Balagan-yidn and merged into 497, but their clusters are the 1921 Moving Yiddish
Dramatic Theatre, db 725 (Danzig dossier, problem 1).

Last, a deprecated row keeps no linked_cluster_ids of its own. kg_orgrel follows
a dead row's links to its keeper, so 691 -> 689 sent ORG-C04456 and
ORG-C02591_Q03 to 689 although they align to 3796 / 3797. Every cluster such a
row still listed either has its live owner already, is GENERIC, or no longer
exists in organizations_clustered.tsv.

Idempotent: a second --apply changes and logs nothing.

Usage:
    python3.11 Zylbercweig/organizations/apply_repoint_deprecated_targets_2026_11_01.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
from pathlib import Path

from apply_dup_merges_2026_10_31 import (ADDR, ALIGN, CORE_DB, _dump, _load,
                                         add_pipe, log_rows, pipe_list)

DEAD = ("true", "1")

OVERRIDES = {"ORG-C01787": "725", "ORG-C02772_Q02": "725"}
REMOVED = {
    "1354": ("NOT_AN_ORG", "two people, not a troupe (Ruthie 2026-10-04)"),
    "1717": ("NOT_AN_ORG", "פלייסיגן is the adjective 'diligent' (Ruthie 2026-10-04)"),
    "1735": ("GENERIC", "כאָפּ-שפּילן is a genre term (Ruthie 2026-10-04)"),
}


def live_end(db_id: str, by_id: dict[str, dict]) -> str:
    """Follow merged_into to a live row; '' for a cancellation or a cycle."""
    seen = set()
    while db_id in by_id and by_id[db_id].get("deprecated", "").strip().lower() in DEAD:
        if db_id in seen:
            return ""
        seen.add(db_id)
        db_id = by_id[db_id].get("merged_into", "").strip()
        if not db_id:
            return ""
    return db_id if db_id in by_id else ""


def drop_pipe(row: dict, col: str, val: str) -> bool:
    cur = pipe_list(row.get(col, ""))
    if val not in cur:
        return False
    row[col] = " | ".join(c for c in cur if c != val)
    return True


def main(apply: bool, reviewer: str) -> None:
    core_f, core = _load(CORE_DB)
    by_id = {r["db_id"].strip(): r for r in core}
    is_dead = lambda d: by_id[d].get("deprecated", "").strip().lower() in DEAD
    owner = {c: r["db_id"].strip() for r in core
             if r.get("deprecated", "").strip().lower() not in DEAD
             for c in pipe_list(r["linked_cluster_ids"])}
    owner_sets = {r["db_id"].strip(): set(pipe_list(r["linked_cluster_ids"])) for r in core
                  if r.get("deprecated", "").strip().lower() not in DEAD}
    al_f, al = _load(ALIGN)
    ad_f, ad = _load(ADDR)
    card = {r["db_id"].strip(): r for r in ad}

    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log, conflicts, dangling = [], [], []
    n_al = n_core = n_card = 0
    for r in al:
        cid, tid = r["cluster_id"].strip(), r.get("aligned_db_id", "").strip()
        if not tid:
            continue
        if tid not in by_id:
            dangling.append((cid, r.get("decision", ""), tid))
            continue
        if not is_dead(tid):
            continue
        old_dec = r.get("decision", "")
        owners = sorted((d for d, c in owner_sets.items() if cid in c),
                        key=lambda x: int(x) if x.isdigit() else 10**9)
        if cid in OVERRIDES:
            keep, why = OVERRIDES[cid], "override"
        elif tid in REMOVED:
            keep, why = "", "removed"
        elif by_id[tid].get("merged_into", "").strip():
            keep, why = live_end(tid, by_id), "merged"
        elif owners:
            keep, why = owners[0], "bundle"
        else:
            raise SystemExit(f"{cid}: cancelled target {tid} not in REMOVED and no live owner")
        if why == "merged" and keep and owner.get(cid, keep) != keep:
            conflicts.append((cid, tid, keep, owner[cid]))
            continue
        if why in ("merged", "override", "removed"):
            # the dead chain and any live row wrongly holding it give it up
            stale = {tid, by_id[tid].get("merged_into", "").strip()}
            if why == "override":
                stale |= {d for d in owners if d != keep}
            for d in stale:
                if d in by_id:
                    n_core += drop_pipe(by_id[d], "linked_cluster_ids", cid)
                if d in card:
                    n_card += drop_pipe(card[d], "linked_cluster_ids", cid)
        else:
            n_core += drop_pipe(by_id[tid], "linked_cluster_ids", cid)
            if tid in card:
                n_card += drop_pipe(card[tid], "linked_cluster_ids", cid)
        if keep:
            r["aligned_db_id"] = keep
            n_core += add_pipe(by_id[keep], "linked_cluster_ids", [cid])
            if keep in card:
                n_card += add_pipe(card[keep], "linked_cluster_ids", [cid])
            note = {"merged": f"re-pointed {tid} (deprecated, merged into {keep}) -> {keep}",
                    "override": f"re-pointed {tid} -> {keep}: merge followed a wrong name",
                    "bundle": f"bundle {tid} dispatched to db {' | '.join(owners)}; "
                              f"aligned_db_id = lowest"}[why]
            if why == "bundle":
                r["reviewer_notes"] = (r.get("reviewer_notes", "").strip() + "\n" + note).strip()
        else:
            r["aligned_db_id"] = ""
            r["decision"], reason = REMOVED[tid]
            note = f"target {tid} removed: {reason}"
        r["reviewer"], r["reviewed_at"] = reviewer or r.get("reviewer", ""), now
        n_al += 1
        print(f"  {cid:16} {old_dec:6} {tid:>5} -> {keep or r['decision']:10} {why}")
        log.append({
            "ts": now, "reviewer": reviewer, "view": "org_alignment",
            "action": "repoint_deprecated_target", "target_id": cid,
            "decision": r["decision"], "note": note,
            "extra": json.dumps({"from_db_id": tid, "to_db_id": keep, "kind": why,
                                 "source": Path(__file__).name}, ensure_ascii=False),
        })

    n_dead = 0
    for d, row in by_id.items():
        if not is_dead(d) or not pipe_list(row["linked_cluster_ids"]):
            continue
        cleared = pipe_list(row["linked_cluster_ids"])
        row["linked_cluster_ids"] = ""
        if d in card:
            card[d]["linked_cluster_ids"] = ""
        n_dead += 1
        print(f"  dead row {d} (merged into {row.get('merged_into', '').strip() or '-'}) "
              f"drops its links {cleared}")
        log.append({
            "ts": now, "reviewer": reviewer, "view": "org_addresses",
            "action": "clear_deprecated_links", "target_id": d, "decision": "",
            "note": f"deprecated db {d} drops its own cluster links",
            "extra": json.dumps({"cleared": cleared, "source": Path(__file__).name},
                                ensure_ascii=False),
        })

    for c in conflicts:
        print(f"  CONFLICT {c[0]}: aligned {c[1]} -> {c[2]}, but core_db links it to live {c[3]}")
    for d in dangling:
        print(f"  DANGLING {d[0]} ({d[1]}) -> {d[2]} not in core_db")
    print(f"\nre-pointed {n_al}  core_db link edits {n_core}  card link edits {n_card}"
          f"  dead rows cleared {n_dead}  conflicts {len(conflicts)}  dangling {len(dangling)}")
    if not apply:
        print("(dry-run; pass --apply --reviewer NAME to write)")
        return
    if n_al or n_dead:
        _dump(ALIGN, al_f, al)
        _dump(CORE_DB, core_f, core)
        _dump(ADDR, ad_f, ad)
    print(f"logged {log_rows(log)} rows as {reviewer!r}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
