"""Apply Arne's clear Vienna+Berlin audit decisions directly to Zalmen data files,
as if he had taken them in the app.

Three passes, in order:
  1. COLLAPSE DUPLICATES — for each QID dup-group, pick/mint one canonical core_db
     row and align every clear group member to it.
  2. MINT SINGLETONS — for each clear non-dup cluster with no aligned_db_id, mint
     a new core_db row and align the cluster to it.
  3. ENRICH EXISTING — write name / address / QID note onto every clear row's
     target core_db + addresses-review entry.

"Clear" excludes rows whose comment contains a question marker, rows flagged for
typology review, and the one row Arne column-shifted (ORG-C04853 Kammerspiele).
Those go to vienna_berlin_arne_open_questions.tsv.
"""
from __future__ import annotations
import csv
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
DELTA = HERE / "vienna_berlin_audit_arne_delta_2026-06-14.tsv"
HANDOFF = HERE / "vienna_berlin_audit_for_RA.tsv"
CORE = HERE / "core_db.tsv"
ALIGN = HERE / "org_alignment_review.tsv"
ADDR = HERE / "org_addresses_review.tsv"
OPEN_OUT = HERE / "vienna_berlin_arne_open_questions.tsv"

REVIEWER = "Arne"
NOW = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
QUESTION_RE = re.compile(r"\?|Maybe|Probably|Probaly|Perhaps|unassignable", re.IGNORECASE)


def has_question(s: str) -> bool:
    return bool(s) and bool(QUESTION_RE.search(s))

# Manual extras: typology-only flags (no question mark) — we still mint/enrich
# but we surface them in open_questions so org_type can be revisited.
TYPOLOGY_FLAG_COMMENTS = {
    "Company for theatre sets, stage design and costumes",
    "Art Exhibition",
    "Zion Association",
}
COL_SHIFT_IDS = {"ORG-C04853"}


def load(path, quote_none=False):
    """Default reader honors quoted multi-line cells (existing TSVs).
    For files with unescaped " in cells (e.g. Arne's handoff with Yiddish), pass quote_none=True."""
    with open(path, encoding="utf-8") as f:
        if quote_none:
            rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            raw = list(rd)
            if not raw: return []
            header = raw[0]
            return [dict(zip(header, r + [""] * (len(header) - len(r)))) for r in raw[1:]]
        return list(csv.DictReader(f, delimiter="\t"))


def write(path, rows, fields):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        for r in rows: w.writerow({k: r.get(k, "") for k in fields})


def strip_translit(s: str) -> str:
    return re.sub(r"\s*\[[^\]]*\]\s*", "", s or "").strip()


def first_yiddish(h: dict) -> str:
    return strip_translit(h.get("name_yiddish_with_translit", ""))


def main():
    delta = load(DELTA)
    handoff_idx = {(r["kind"], r["id"]): r for r in load(HANDOFF, quote_none=True)}
    core = load(CORE)
    core_fields = list(core[0].keys()) if core else []
    core_by_id = {r["db_id"]: r for r in core}
    align = load(ALIGN)
    align_fields = list(align[0].keys())
    align_by_id = {r["cluster_id"]: r for r in align}
    addr = load(ADDR)
    addr_fields = list(addr[0].keys())
    addr_by_id = {r["db_id"]: r for r in addr}

    next_db = max(int(r["db_id"]) for r in core if r["db_id"].isdigit()) + 1

    # Classify
    open_rows = []  # rows we will NOT apply
    clear_delta = []
    for d in delta:
        comment = (d.get("comments") or "").strip()
        name = (d.get("name_latin") or "").strip()
        qid = (d.get("QID") or "").strip()
        addr_now = (d.get("current_address") or "").strip()
        addr_hist = (d.get("historic_address") or "").strip()
        reason = []
        if has_question(comment) or has_question(name):
            reason.append("question")
        if comment in TYPOLOGY_FLAG_COMMENTS:
            reason.append("typology")
        if d["id"] in COL_SHIFT_IDS:
            reason.append("col-shift")
        # Comment-only rows (no QID, no address, no real Latin name) aren't
        # actionable decisions — Arne just left an explanatory note.
        if not reason:
            has_action = qid.startswith("Q") or addr_now or addr_hist or (
                name and not has_question(name))
            if not has_action:
                reason.append("comment-only")
        if reason:
            open_rows.append({**d, "open_reason": "|".join(reason)})
            # Pure typology rows still get applied (only org_type is in question).
            if reason == ["typology"]:
                clear_delta.append(d)
        else:
            clear_delta.append(d)

    print(f"clear rows to apply: {len(clear_delta)}")
    print(f"open rows skipped:   {len(open_rows)}")

    def enrich_core(db_id: str, *, name_latin: str, address: str, yiddish: str = ""):
        r = core_by_id.get(db_id)
        if not r:
            return
        if name_latin and not (r.get("name") or "").strip():
            r["name"] = name_latin
        elif name_latin and name_latin != r["name"]:
            existing = r.get("name_variants", "")
            if name_latin not in existing:
                r["name_variants"] = (existing + " | " if existing else "") + name_latin
        if yiddish and not (r.get("name_yiddish") or "").strip():
            r["name_yiddish"] = yiddish
        if address and not (r.get("address") or "").strip():
            r["address"] = address

    def enrich_addr(db_id: str, *, qid: str, name_latin: str, current: str,
                    historic: str, settlement: str, yiddish: str):
        r = addr_by_id.get(db_id)
        if not r:
            r = {k: "" for k in addr_fields}
            r["db_id"] = db_id
            addr.append(r)
            addr_by_id[db_id] = r
        if current and not (r.get("confirmed_address") or "").strip():
            r["confirmed_address"] = current
        if settlement and not (r.get("confirmed_settlement") or "").strip():
            r["confirmed_settlement"] = settlement
        if yiddish and not (r.get("confirmed_settlement_yiddish") or "").strip():
            # leave settlement_yiddish; we have org-yiddish not settlement-yiddish
            pass
        notes_existing = r.get("reviewer_notes") or ""
        new_notes = []
        if name_latin and f"name_latin: {name_latin}" not in notes_existing:
            new_notes.append(f"[arne] name_latin: {name_latin}")
        if qid.startswith("Q") and f"WD:{qid}" not in notes_existing:
            new_notes.append(f"[arne] WD:{qid}")
        if historic and f"historic_address: {historic}" not in notes_existing:
            new_notes.append(f"[arne] historic_address: {historic}")
        if new_notes:
            r["reviewer_notes"] = "\n".join(new_notes + ([notes_existing] if notes_existing else []))
        r["reviewer"] = REVIEWER
        r["reviewed_at"] = NOW

    def mint(*, name_latin: str, yiddish: str, address: str, settlement: str,
             org_type: str, cluster_id: str = "", source_id: str = "") -> str:
        nonlocal next_db
        new_id = str(next_db); next_db += 1
        row = {k: "" for k in core_fields}
        row["db_id"] = new_id
        row["name"] = name_latin
        row["name_yiddish"] = yiddish
        row["org_type"] = org_type or "Theatre"
        row["address"] = address
        row["linked_cluster_ids"] = cluster_id
        core.append(row)
        core_by_id[new_id] = row
        print(f"  mint db {new_id:>4} <- {source_id:25} {name_latin[:40]!r}")
        return new_id

    def set_alignment(cluster_id: str, db_id: str, decision: str):
        a = align_by_id.get(cluster_id)
        if not a:
            a = {k: "" for k in align_fields}
            a["cluster_id"] = cluster_id
            align.append(a)
            align_by_id[cluster_id] = a
        a["decision"] = decision
        a["aligned_db_id"] = db_id
        a["reviewer"] = REVIEWER
        a["reviewed_at"] = NOW
        # Push cluster id into core_db.linked_cluster_ids for the canonical row.
        r = core_by_id.get(db_id)
        if r is not None:
            existing = [x for x in (r.get("linked_cluster_ids") or "").split("|") if x.strip()]
            if cluster_id not in existing:
                existing.append(cluster_id)
                r["linked_cluster_ids"] = "|".join(existing)

    # --- STEP 1: COLLAPSE DUPLICATES ---
    from collections import defaultdict
    dup_groups = defaultdict(list)
    for d in clear_delta:
        if (d.get("duplicate") or "").strip() != "yes":
            continue
        qid = (d.get("QID") or "").strip()
        if qid.startswith("Q"):
            key = qid
        else:
            # Fallback: group by (city, name_latin) — Arne marked these as duplicate
            # but couldn't find a QID.
            name = (d.get("name_latin") or "").strip().lower()
            if not name:
                continue
            key = f"name::{d.get('city','')}::{name}"
        dup_groups[key].append(d)

    canonical_for_qid: dict[str, str] = {}  # maps both QID and name-key -> canonical db_id
    handled_ids: set[str] = set()
    print("\n=== STEP 1: COLLAPSE DUPLICATES ===")
    for qid, members in sorted(dup_groups.items()):
        if len(members) < 2:
            continue
        # Pick canonical: any db-kind member, or any cluster member with existing alignment
        cand = None
        for m in members:
            if m["kind"] == "db":
                cand = m["id"]; break
        if not cand:
            for m in members:
                h = handoff_idx.get((m["kind"], m["id"])) or {}
                if (h.get("aligned_db_id") or "").strip():
                    cand = h["aligned_db_id"]; break
        if not cand:
            # Canonical name = shortest clean Latin name across the group.
            clean_names = sorted(
                {m["name_latin"].strip() for m in members
                 if m.get("name_latin") and not has_question(m["name_latin"])},
                key=len,
            )
            canon_name = clean_names[0] if clean_names else ""
            # Take address from any member that has one.
            addrs = [m["current_address"] for m in members if m.get("current_address")]
            canon_addr = addrs[0] if addrs else ""
            # Pick a representative for yiddish lookup.
            rep = next((m for m in members if m["kind"] == "cluster"), members[0])
            h = handoff_idx.get((rep["kind"], rep["id"])) or {}
            cand = mint(
                name_latin=canon_name,
                yiddish=first_yiddish(h),
                address=canon_addr,
                settlement=rep.get("city", ""),
                org_type=h.get("org_type", "Theatre"),
                cluster_id="",  # filled below via linked_cluster_ids accumulation
                source_id=f"dup/{qid}",
            )
        canonical_for_qid[qid] = cand
        print(f"  {qid:15} -> db {cand:>4}  ({len(members)} members)")
        # Align every cluster member to canonical; db members are already the target
        for m in members:
            if m["kind"] == "cluster":
                decision = "ALIGN" if cand != m.get("id") else ""
                set_alignment(m["id"], cand, decision)
            handled_ids.add(m["id"])

    # --- STEP 2: MINT SINGLETONS ---
    print("\n=== STEP 2: MINT SINGLETONS ===")
    minted_singletons = 0
    for d in clear_delta:
        if d["kind"] != "cluster":
            continue
        if d["id"] in handled_ids:
            continue
        cid = d["id"]
        h = handoff_idx.get(("cluster", cid)) or {}
        if (h.get("aligned_db_id") or "").strip():
            continue  # already aligned -> enrich pass only
        # Mint
        new_id = mint(
            name_latin=d.get("name_latin", ""),
            yiddish=first_yiddish(h),
            address=d.get("current_address", ""),
            settlement=d.get("city", ""),
            org_type=h.get("org_type", "Theatre"),
            cluster_id=cid,
            source_id=cid,
        )
        set_alignment(cid, new_id, "NEW")
        minted_singletons += 1
    print(f"  minted {minted_singletons} singletons")

    # --- STEP 3: ENRICH EXISTING ---
    print("\n=== STEP 3: ENRICH EXISTING ===")
    enriched = 0
    for d in clear_delta:
        h = handoff_idx.get((d["kind"], d["id"])) or {}
        if d["kind"] == "db":
            target = d["id"]
        else:
            qid = d.get("QID", "")
            target = canonical_for_qid.get(qid) if qid in canonical_for_qid else (
                (h.get("aligned_db_id") or "").strip() or align_by_id.get(d["id"], {}).get("aligned_db_id", "")
            )
        if not target:
            continue
        enrich_core(target,
                    name_latin=d.get("name_latin", ""),
                    address=d.get("current_address", ""),
                    yiddish=first_yiddish(h))
        enrich_addr(target,
                    qid=d.get("QID", ""),
                    name_latin=d.get("name_latin", ""),
                    current=d.get("current_address", ""),
                    historic=d.get("historic_address", ""),
                    settlement=d.get("city", ""),
                    yiddish=first_yiddish(h))
        enriched += 1
    print(f"  enriched {enriched} rows")

    # Write all three files
    write(CORE, core, core_fields)
    write(ALIGN, align, align_fields)
    write(ADDR, addr, addr_fields)

    # Open questions
    open_fields = ["kind", "id", "city", "name_latin", "QID", "duplicate",
                   "current_address", "historic_address", "comments", "open_reason"]
    write(OPEN_OUT, open_rows, open_fields)
    print(f"\nwrote {len(open_rows)} rows -> {OPEN_OUT.name}")
    print(f"core_db now: {len(core)} rows  (added {len(core) - sum(1 for _ in open(CORE.with_name('core_db.tsv.pre_arne_2026-06-28')) ) + 1})")


if __name__ == "__main__":
    main()
