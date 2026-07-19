"""Ingest Arne's Vienna+Berlin audit deltas.

Treats Arne's name_latin / QID / historic_address / current_address / duplicate
as FINAL. Only typology and explicit questions remain for second review.

Outputs:
  - Updates org_addresses_review.tsv in-place (DB-row writes + cluster->aligned_db_id writes)
  - vienna_berlin_arne_unaligned_for_mint.tsv  -- QID-anchored mint candidates
  - vienna_berlin_arne_dup_merge.tsv           -- duplicate-merge action list (final)
  - vienna_berlin_arne_typology_review.tsv     -- non-theatre entities tagged Theatre cluster_type
  - vienna_berlin_arne_questions.tsv           -- rows with ?/Maybe/Probably/Perhaps comments
"""
from __future__ import annotations
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
DELTA = HERE / "vienna_berlin_audit_arne_delta_2026-06-14.tsv"
HANDOFF = HERE / "vienna_berlin_audit_for_RA.tsv"
ADDR = HERE / "org_addresses_review.tsv"

REVIEWER = "Arne (via Sinai 2026-06-14)"
NOW = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

QUESTION_MARKERS = ("?", "Maybe", "Probably", "Perhaps", "probably")


def load(path, key=None):
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return {key(r): r for r in rows} if key else rows


def is_question(comment: str) -> bool:
    return bool(comment) and any(m in comment for m in QUESTION_MARKERS)


def main():
    delta = load(DELTA)
    handoff = load(HANDOFF, key=lambda r: (r["kind"], r["id"]))
    addr_rows = load(ADDR)
    addr_idx = {r["db_id"]: r for r in addr_rows}

    dup_merge = []
    mint_candidates = []
    typology_review = []
    questions = []

    # Group duplicates by QID for the merge action list.
    dup_groups: dict[str, list[dict]] = defaultdict(list)

    for d in delta:
        kind, oid = d["kind"], d["id"]
        h = handoff.get((kind, oid))
        if not h:
            print(f"  no handoff row for {kind}/{oid}")
            continue

        name_latin = (d.get("name_latin") or "").strip()
        qid = (d.get("QID") or "").strip()
        hist = (d.get("historic_address") or "").strip()
        curr = (d.get("current_address") or "").strip()
        comment = (d.get("comments") or "").strip()
        dup_flag = (d.get("duplicate") or "").strip() == "yes"

        if dup_flag and qid.startswith("Q"):
            dup_groups[qid].append({"kind": kind, "id": oid, "name_latin": name_latin})

        # Target DB id: direct (kind=db) or cluster.aligned_db_id
        target_db = oid if kind == "db" else (h.get("aligned_db_id") or "").strip()

        if target_db and target_db in addr_idx:
            r = addr_idx[target_db]
            if name_latin:
                r["reviewer_notes"] = (r.get("reviewer_notes") or "")
                r["reviewer_notes"] = f"[arne] name_latin: {name_latin}\n" + r["reviewer_notes"]
            if qid.startswith("Q"):
                tag = f"[arne] WD:{qid}"
                if tag not in (r.get("reviewer_notes") or ""):
                    r["reviewer_notes"] = tag + "\n" + (r.get("reviewer_notes") or "")
            if curr and not (r.get("confirmed_address") or "").strip():
                r["confirmed_address"] = curr
            if hist:
                r["reviewer_notes"] = f"[arne] historic_address: {hist}\n" + (r.get("reviewer_notes") or "")
            if comment:
                r["reviewer_notes"] = f"[arne] comment: {comment}\n" + (r.get("reviewer_notes") or "")
            r["reviewer"] = REVIEWER
            r["reviewed_at"] = NOW
        elif target_db:
            # DB row exists but not in addresses_review yet — append a new row.
            new = {k: "" for k in addr_rows[0].keys()}
            new["db_id"] = target_db
            new["confirmed_settlement"] = d.get("city", "")
            new["confirmed_address"] = curr
            notes = []
            if name_latin: notes.append(f"name_latin: {name_latin}")
            if qid.startswith("Q"): notes.append(f"WD:{qid}")
            if hist: notes.append(f"historic_address: {hist}")
            if comment: notes.append(f"comment: {comment}")
            new["reviewer_notes"] = "\n".join(f"[arne] {n}" for n in notes)
            new["reviewer"] = REVIEWER
            new["reviewed_at"] = NOW
            addr_rows.append(new)
            addr_idx[target_db] = new
        else:
            # Unaligned cluster — emit mint candidate.
            mint_candidates.append({
                "cluster_id": oid,
                "city": d.get("city", ""),
                "name_latin": name_latin,
                "QID": qid,
                "historic_address": hist,
                "current_address": curr,
                "comment": comment,
                "cluster_size": h.get("cluster_size", ""),
                "org_type_current": h.get("org_type", ""),
                "duplicate": "yes" if dup_flag else "",
            })

        # Typology review: comments imply non-theatre entity but org_type=Theatre
        ot = h.get("org_type", "")
        non_theatre_signals = (
            "Company for theatre sets",
            "Art Exhibition",
            "Zion Association",
            "General Jewish Workers",
            "(actor",
            "Max Reinhardt",
            "(director)",
        )
        if ot == "Theatre" and any(s in comment for s in non_theatre_signals):
            typology_review.append({
                "kind": kind, "id": oid, "city": d.get("city", ""),
                "current_org_type": ot, "name_latin": name_latin,
                "comment": comment,
            })

        # Questions: any "?/Maybe/Probably/Perhaps" in comment
        if is_question(comment):
            questions.append({
                "kind": kind, "id": oid, "city": d.get("city", ""),
                "name_latin": name_latin, "QID": qid, "comment": comment,
            })

    # Build dup-merge action list (one row per cluster member of each QID group, keyed by QID)
    for qid, members in sorted(dup_groups.items()):
        if len(members) < 2:
            continue
        canonical_name = next((m["name_latin"] for m in members if m["name_latin"]), "")
        for m in members:
            dup_merge.append({
                "QID": qid,
                "canonical_name": canonical_name,
                "kind": m["kind"],
                "id": m["id"],
                "name_latin": m["name_latin"],
                "group_size": len(members),
            })

    # Write outputs.
    with open(ADDR, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(addr_rows[0].keys()), delimiter="\t")
        w.writeheader()
        for r in addr_rows:
            w.writerow(r)

    def dump(name, rows, fields):
        path = HERE / name
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
            w.writeheader()
            for r in rows: w.writerow(r)
        print(f"  wrote {len(rows):>4} rows -> {name}")

    dump("vienna_berlin_arne_dup_merge.tsv", dup_merge,
         ["QID", "canonical_name", "kind", "id", "name_latin", "group_size"])
    dump("vienna_berlin_arne_unaligned_for_mint.tsv", mint_candidates,
         ["cluster_id", "city", "name_latin", "QID", "historic_address",
          "current_address", "comment", "cluster_size", "org_type_current", "duplicate"])
    dump("vienna_berlin_arne_typology_review.tsv", typology_review,
         ["kind", "id", "city", "current_org_type", "name_latin", "comment"])
    dump("vienna_berlin_arne_questions.tsv", questions,
         ["kind", "id", "city", "name_latin", "QID", "comment"])

    print(f"\norg_addresses_review.tsv now: {len(addr_rows)} rows")


if __name__ == "__main__":
    main()
