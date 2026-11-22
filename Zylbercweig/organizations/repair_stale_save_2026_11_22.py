"""Repair the 14:18 stale app save of 2026-11-22 (21a7274c8 + 22b9389c5).

21a7274c8 "chore: save alignment decisions" wrote org_alignment_review.tsv from a checkout
older than 81d133137 (SPLIT/OTHER review, 13:56) and 629a80ea9 (network parents, 13:57):
  - 59 SPLIT/OTHER decisions + 14 network-parent decisions reverted to their old values,
  - 54 rows minted by the SPLIT/OTHER review (partition / name-split children) deleted,
    while organizations_clustered.tsv (which the app never writes) still carries their ids.
22b9389c5 "chore: save core DB" re-used db 5040: the SPLIT review had minted it as
Dimitrovsky's music school, Kiev (ORG-C00242_P01); Bella's app session minted its own 5040
(ORG-C04274, choir of Nozhik's school, 12:18Z) on top of it. Bella's is live, so she keeps
5040, and Dimitrovsky's school is re-minted from origin's high-water mark.

Restore rule, row by row from the pre-save state (21a7274c8~1): a reverted row is restored
only if it is still exactly as the stale save left it (no one touched it since); a deleted row
is re-inserted after its old predecessor if its cluster_id is absent. ORG-C04274 (Bella's real
decision) is never touched. Any restored row pointing at 5040 is re-pointed to the new id.

Idempotent. Usage: python3.11 repair_stale_save_2026_11_22.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, csv, datetime as _dt, io, json, subprocess, sys
from pathlib import Path
import apply_network_parents_2026_11_22 as m

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
TAG = "repair stale save 2026-11-22"
BAD = "21a7274c8"
REV = "Zylbercweig/organizations/org_alignment_review.tsv"
CORE = "Zylbercweig/organizations/core_db.tsv"
KEEP = {"ORG-C04274"}
COLLIDED = "5040"


def at(commit: str, path: str) -> list[dict]:
    out = subprocess.run(["git", "show", f"{commit}:{path}"], cwd=HERE, capture_output=True,
                         text=True, check=True).stdout
    return list(csv.DictReader(io.StringIO(out), delimiter="\t"))


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    before = at(BAD + "~1", REV)
    after = {r["cluster_id"]: r for r in at(BAD, REV)}
    old5040 = next(r for r in at("22b9389c5~1", CORE) if r["db_id"] == COLLIDED)

    af, al, at_ = m._rw(HERE / "org_alignment_review.tsv")
    kf, core, kt = m._rw(HERE / "core_db.tsv")
    by = {r["db_id"]: r for r in core}
    cur = {r["cluster_id"]: r for r in al}
    log: list[list[str]] = []

    def L(action, target, decision, text, extra):
        log.append([now, reviewer, "org_review", action, target, decision, f"{TAG}: {text}",
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    # ---- re-mint Dimitrovsky's school (idempotent on name_yiddish among non-5040 rows)
    new_id = next((r["db_id"] for r in core if r["db_id"] != COLLIDED
                   and r["name_yiddish"] == old5040["name_yiddish"] and not r["deprecated"].strip()), "")
    if not new_id:
        new_id = str(max(m.origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1)
        row = {f: old5040.get(f, "") for f in kf}
        row["db_id"] = new_id
        core.append(row); by[new_id] = row
        L("mint", new_id, "NEW", f"re-mint of {old5040['name']} (its db {COLLIDED} was re-used by an app mint)",
          {"name_yiddish": old5040["name_yiddish"], "was_db_id": COLLIDED})
        print(f"  re-mint {old5040['name']} as db {new_id} (links {old5040['linked_cluster_ids']})")
    for cid in m.pipes(old5040["linked_cluster_ids"]):
        if cid in m.pipes(by[COLLIDED]["linked_cluster_ids"]) and cid not in KEEP:
            by[COLLIDED]["linked_cluster_ids"] = " | ".join(x for x in m.pipes(by[COLLIDED]["linked_cluster_ids"]) if x != cid)
        if cid not in m.pipes(by[new_id]["linked_cluster_ids"]):
            by[new_id]["linked_cluster_ids"] = " | ".join(m.pipes(by[new_id]["linked_cluster_ids"]) + [cid])

    def fixed(r: dict) -> dict:
        r = dict(r)
        if r["aligned_db_id"].strip() == COLLIDED and r["cluster_id"] not in KEEP:
            r["aligned_db_id"] = new_id
        return r

    restored = reinserted = 0
    prev_cid = None
    for b in before:
        cid = b["cluster_id"]
        a = after.get(cid)
        if cid in KEEP:
            prev_cid = cid; continue
        if a is None:  # deleted by the stale save
            if cid not in cur:
                row = fixed(b)
                idx = next((i for i, x in enumerate(al) if x["cluster_id"] == prev_cid), len(al) - 1) + 1
                al.insert(idx, row); cur[cid] = row
                L("alignment", cid, row["decision"], "re-inserted (deleted by stale save 21a7274c8)",
                  {"aligned_db_id": row["aligned_db_id"]})
                reinserted += 1
        elif a != b:
            c = cur.get(cid)
            if c is not None and c == a:  # untouched since the stale save
                row = fixed(b)
                c.update(row)
                L("alignment", cid, row["decision"], "restored (reverted by stale save 21a7274c8)",
                  {"aligned_db_id": row["aligned_db_id"], "was": f"{a['decision']} {a['aligned_db_id']}".strip()})
                restored += 1
            elif c is not None and c != fixed(b):
                print(f"  SKIP {cid}: changed since the stale save ({c['decision']} {c['aligned_db_id']}, {c['reviewer']})")
        prev_cid = cid
    print(f"  restored {restored}, re-inserted {reinserted}; {len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    if not log:
        return
    m._dump(HERE / "org_alignment_review.tsv", af, al, at_)
    m._dump(HERE / "core_db.tsv", kf, core, kt)
    p = HERE / "activity_log.tsv"
    b_ = p.read_bytes()
    term = "\r\n" if b"\r\n" in b_[:4096] else "\n"
    with p.open("ab") as f:
        if not b_.endswith(b"\n"):
            f.write(term.encode())
        for row in log:
            f.write(("\t".join(row) + term).encode("utf-8"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
