"""Surgically repair a subset of the empty-linked_cluster_ids orphans in
core_db.tsv (the settlement-audit re-align path strands rows; see the
investigation notes). SAFE SUBSET ONLY:

  RE-LINK   — orphan whose name_yiddish exactly matches a cluster's
              canonical_yiddish, and that cluster has NO active owner. Add the
              cluster to the orphan's linked_cluster_ids.
  DEPRECATE — orphan whose matched cluster is already owned by exactly ONE
              other active DB → the orphan is a duplicate leftover. Set
              deprecated='true', merged_into=<owner db_id>.

Everything else is left untouched: blank-name rows, modern Hebrew publishers,
and any match whose cluster is owned by 2+ DBs (a double-alignment that needs
human review, not an auto-merge).

Two things a name match alone must never override:

  REMOVE decisions — a reviewer who unlinks a cluster in the DB Audit view
      leaves the pair in db_audit_decisions.tsv, and the cluster keeps naming
      the row in org_alignment_review.aligned_db_id. A name match will happily
      re-link it. It re-linked db427 אונזער ווינקל ← ORG-C00752 against
      Ruthie's 2026-08-09 REMOVE; that pair is now blocked here.
  umbrella parents — a row whose clusters were QID-exploded onto child rows
      (parent_db_id) is *supposed* to end up with no links of its own; db427's
      ORG-C00752 lives on as _Q01/_Q02/_Q03 on db1785/1786/1787. Such a row is
      not an orphan and must be neither re-linked nor deprecated.

In place, reversible (one/two columns per affected row). NEVER regenerates
core_db (build_core_db.py is non-idempotent). Dry-run by default; pass --apply
to write.
"""
from __future__ import annotations
import csv, sys, pathlib

csv.field_size_limit(10**9)
HERE = pathlib.Path(__file__).resolve().parent
CORE = HERE / "core_db.tsv"
CLUSTERED = HERE / "organizations_clustered.tsv"
DECISIONS = HERE / "db_audit_decisions.tsv"
APPLY = "--apply" in sys.argv


def load(p):
    with open(p, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd), rd.fieldnames


def is_active(r):
    return not any((r.get(k) or "").strip()
                   for k in ("deprecated", "merged_into", "out_of_project"))


def split_links(s):
    return [x.strip() for x in (s or "").split("|") if x.strip()]


core, headers = load(CORE)
active = [r for r in core if is_active(r)]

# (db_id, cluster_id) pairs a reviewer explicitly unlinked — never re-link.
removed = set()
if DECISIONS.exists():
    for r in load(DECISIONS)[0]:
        if (r.get("decision") or "").strip().upper() == "REMOVE":
            removed.add(((r.get("db_id") or "").strip(),
                         (r.get("cluster_id") or "").strip()))

# db_ids that are some other row's parent: umbrella rows, empty by design.
parents = {(r.get("parent_db_id") or "").strip()
           for r in core if (r.get("parent_db_id") or "").strip()}

# cluster_id -> set of active db_ids owning it
owner = {}
for r in active:
    for cid in split_links(r.get("linked_cluster_ids", "")):
        owner.setdefault(cid, set()).add(r["db_id"])

# canonical_yiddish -> set of cluster_ids, and mention counts
SENT = "_ - organizations - _ - relations - _ - original_sentence"
canon2cids, cid_mentions = {}, {}
with open(CLUSTERED, newline="", encoding="utf-8") as f:
    rd = csv.DictReader(f, delimiter="\t")
    cidcol = next(c for c in rd.fieldnames if c.strip().lower() == "cluster_id")
    for row in rd:
        cid = (row.get(cidcol) or "").strip()
        cy = (row.get("canonical_yiddish") or "").strip()
        if cy and cid:
            canon2cids.setdefault(cy, set()).add(cid)
        if cid and (row.get(SENT) or "").strip():
            cid_mentions[cid] = cid_mentions.get(cid, 0) + 1

orphans = [r for r in active if not split_links(r.get("linked_cluster_ids", ""))]

relink, deprecate, skip_multi = [], [], []
skip_parent, skip_removed = [], []
for r in orphans:
    yi = (r.get("name_yiddish") or "").strip()
    if not yi or yi not in canon2cids:
        continue
    if r["db_id"].strip() in parents:
        skip_parent.append(r)      # umbrella: its clusters live on the children
        continue
    cids = sorted(canon2cids[yi])
    # A reviewer's REMOVE outranks a name match.
    blocked = [c for c in cids if (r["db_id"].strip(), c) in removed]
    if blocked:
        skip_removed.append((r, blocked))
        cids = [c for c in cids if c not in blocked]
        if not cids:
            continue
    # partition matched clusters by ownership
    unowned = [c for c in cids if not owner.get(c)]
    owners = {o for c in cids for o in owner.get(c, set())}
    if owners:
        if len(owners) == 1:
            deprecate.append((r, next(iter(owners)), cids))
        else:
            skip_multi.append((r, owners, cids))
    elif unowned:
        relink.append((r, unowned))

# ── report ────────────────────────────────────────────────────────────────────
print(f"orphans (active, empty links): {len(orphans)}")
print(f"RE-LINK candidates : {len(relink)}")
print(f"DEPRECATE (dup)    : {len(deprecate)}")
print(f"SKIP (multi-owner) : {len(skip_multi)}")
print(f"SKIP (umbrella parent, empty by design) : {len(skip_parent)}")
for r in skip_parent:
    print(f"    db{r['db_id']:>5} {(r.get('name_yiddish') or r.get('name') or '')[:30]}")
print(f"SKIP (reviewer filed REMOVE) : {len(skip_removed)}")
for r, b in skip_removed:
    print(f"    db{r['db_id']:>5} ⊅ {' | '.join(b)}")
print()

print("── RE-LINK ─────────────────────────────────────────────")
for r, cids in relink:
    nm = (r.get("name") or r.get("name_yiddish"))[:30]
    m = sum(cid_mentions.get(c, 0) for c in cids)
    print(f"  db{r['db_id']:>5} {nm:30} += {' | '.join(cids)}  ({m} mentions)")

print("\n── DEPRECATE (merged_into owner) ───────────────────────")
for r, own, cids in deprecate:
    nm = (r.get("name") or r.get("name_yiddish"))[:30]
    print(f"  db{r['db_id']:>5} {nm:30} → merged_into {own}  (cluster {'|'.join(cids)})")

if skip_multi:
    print("\n── SKIPPED (cluster owned by 2+ DBs — needs review) ────")
    for r, owners, cids in skip_multi:
        nm = (r.get("name") or r.get("name_yiddish"))[:30]
        print(f"  db{r['db_id']:>5} {nm:30} owners={sorted(owners)} cids={cids}")

# ── apply ─────────────────────────────────────────────────────────────────────
if not APPLY:
    print("\n(dry-run — pass --apply to write core_db.tsv)")
    sys.exit(0)

by_id = {r["db_id"]: r for r in core}
for r, cids in relink:
    by_id[r["db_id"]]["linked_cluster_ids"] = " | ".join(cids)
for r, own, cids in deprecate:
    row = by_id[r["db_id"]]
    row["deprecated"] = "true"
    row["merged_into"] = own
with open(CORE, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
    w.writeheader()
    w.writerows(core)
print(f"\nAPPLIED: {len(relink)} re-linked, {len(deprecate)} deprecated.")
