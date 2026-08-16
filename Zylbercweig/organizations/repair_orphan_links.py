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
for r in orphans:
    yi = (r.get("name_yiddish") or "").strip()
    if not yi or yi not in canon2cids:
        continue
    cids = sorted(canon2cids[yi])
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
print(f"SKIP (multi-owner) : {len(skip_multi)}\n")

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
