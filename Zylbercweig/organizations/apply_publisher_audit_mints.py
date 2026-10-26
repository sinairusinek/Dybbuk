"""Mint the core_db rows the publisher audit established, and attach clusters.

Source of truth: publisher_audit_decisions.tsv rows of kind `mint`, each
approved by Sinai. A mint coins a permanent id, so this is deliberately the
narrowest of the three apply scripts: it writes only rows whose `value` column
carries the agreed name, and it refuses to run if a name it is about to mint
already exists in core_db.

Ids are minted from the HIGH-WATER MARK OF ORIGIN, not of the local file.
The deployed Zalmen app pushes core_db on every save, so a local maximum can be
hundreds of ids behind and minting from it would collide with rows reviewers
have already created (feedback_core_db_mint_from_origin_high_water).

Grouping: several clusters can belong to one firm. A `mint_new_org` row starts
a firm and names it; `add_to_mint` rows name the starter's decision_id in their
`value` and attach to the same new row rather than coining a second.

Every mint writes: the new core_db row (name_yiddish, org_type, address where
known), its linked_cluster_ids, and ALIGN + aligned_db_id on each cluster in
org_alignment_review.tsv, so neither side carries a one-way link.

Idempotent: a decision whose name already resolves to a row minted by this
script is skipped and not logged. Re-running appends nothing.

Backups to <file>.pre_publisher_mints_<TS>. Dry-run by default.

Usage: python3 apply_publisher_audit_mints.py --reviewer Sinai [--apply]
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
import unicodedata
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
REVIEW = HERE / "org_alignment_review.tsv"
DECISIONS = HERE / "publisher_audit_decisions.tsv"
ACTIVITY_LOG = HERE / "activity_log.tsv"

LOG_COLUMNS = ("ts", "reviewer", "view", "action", "target_id", "decision",
               "note", "extra")
LOG_VIEW = "org_review"
SOURCE = DECISIONS.name
TODAY = _dt.date.today().isoformat()

POINTS = re.compile(r"[֑-ׇ]")


def norm(s: str) -> str:
    """NFKD then strip points, for comparing Yiddish names.

    The corpus uses the precomposed ligature U+FB4E (פֿ) where a typed string
    uses פ + rafe; stripping points alone cannot match them.
    """
    return POINTS.sub("", unicodedata.normalize("NFKD", s or "")).strip()


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def origin_high_water() -> int:
    """Highest db_id on origin/main, so a mint cannot collide with the app's."""
    try:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=HERE,
                       capture_output=True, timeout=120, check=False)
        out = subprocess.run(
            ["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"],
            cwd=HERE, capture_output=True, text=True, timeout=120, check=True).stdout
    except Exception as e:
        raise SystemExit(f"cannot read origin's core_db to mint safely: {e}")
    ids = [int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t")
           if r["db_id"].isdigit()]
    if not ids:
        raise SystemExit("origin core_db has no numeric db_id; refusing to mint")
    return max(ids)


def log_rows(entries: list[dict]) -> int:
    if not entries:
        return 0
    if not ACTIVITY_LOG.exists():
        raise SystemExit(f"missing {ACTIVITY_LOG.name}; refusing to create it")
    existing = ACTIVITY_LOG.read_bytes()
    header = existing.split(b"\n", 1)[0].rstrip(b"\r").decode("utf-8").split("\t")
    if tuple(header) != LOG_COLUMNS:
        raise SystemExit(f"{ACTIVITY_LOG.name} header is {header}")
    buf = io.StringIO()
    csv.DictWriter(buf, fieldnames=list(LOG_COLUMNS), delimiter="\t",
                   lineterminator="\r\n").writerows(entries)
    with ACTIVITY_LOG.open("ab") as f:
        if not existing.endswith((b"\n", b"\r\n")):
            f.write(b"\r\n")
        f.write(buf.getvalue().encode("utf-8"))
    return len(entries)


def log_entry(reviewer: str, action: str, target_id: str, decision: str,
              note: str, **extra) -> dict:
    return {
        "ts": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reviewer": reviewer, "view": LOG_VIEW, "action": action,
        "target_id": str(target_id), "decision": decision,
        "note": note.replace("\n", " ").replace("\t", " ").strip()[:500],
        "extra": json.dumps({"source": SOURCE, **extra}, ensure_ascii=False),
    }


def stamp(row: dict, reviewer: str) -> None:
    row["reviewer"] = reviewer
    row["reviewed_at"] = _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def main(apply: bool, reviewer: str, only: set[str] | None = None) -> None:
    db_fields, db_rows = _load(CORE_DB)
    rv_fields, rv_rows = _load(REVIEW)
    rv = {r["cluster_id"]: r for r in rv_rows}
    _, decisions = _load(DECISIONS)

    existing = {norm(r.get("name_yiddish") or ""): r for r in db_rows
                if (r.get("name_yiddish") or "").strip()}
    existing_lat = {(r.get("name") or "").strip().lower(): r for r in db_rows
                    if (r.get("name") or "").strip()}

    # Group: a mint_new_org row, plus any add_to_mint rows naming it.
    starters = {d["decision_id"]: d for d in decisions
                if d.get("kind") == "mint" and d.get("action") == "mint_new_org"
                and (not only or d["decision_id"] in only)}
    if only:
        unknown = only - set(starters)
        if unknown:
            raise SystemExit(f"--only names no mint_new_org row: {sorted(unknown)}")
    extras: dict[str, list[dict]] = {}
    for d in decisions:
        if d.get("kind") == "mint" and d.get("action") == "add_to_mint":
            extras.setdefault((d.get("value") or "").strip(), []).append(d)

    # The floor is the HIGHER of origin's mark and the local file's. Origin
    # guards against colliding with ids the app has minted; the local maximum
    # guards against colliding with ids THIS session minted but has not pushed
    # yet. Taking origin alone re-issued 2661, which Bletnitski already held.
    local_max = max((int(r["db_id"]) for r in db_rows if r["db_id"].isdigit()),
                    default=0)
    next_id = max(origin_high_water(), local_max) + 1
    minted, skipped, entries, notes = [], [], [], []

    for pid, d in starters.items():
        name = (d.get("value") or "").strip()
        if not name:
            skipped.append(f"{pid}: no name in `value`; nothing to mint")
            continue
        # The name guard stops a second row for a firm already in core_db. It
        # cannot tell a duplicate from a DELIBERATE place-split sibling: the
        # pipeline splits one name across cities as _Q01/_Q02, and those are
        # different firms that share a name (Kultur Minsk/Warsaw, Yavneh
        # Warsaw/Tel Aviv). Such a decision says so in `confidence` as
        # `high|distinct` and is minted anyway, with the clash reported.
        force = "distinct" in (d.get("confidence") or "").lower()

        # Idempotency must hold for forced rows too. `force` waives the NAME
        # collision (a deliberate place split legitimately repeats a name), but
        # a decision whose own clusters are already linked to a row has been
        # applied already and must never mint a second time.
        own = [c for c in [(d.get("cluster_id") or "").strip()] if c]
        for x in extras.get(pid, []):
            c = (x.get("cluster_id") or "").strip()
            if c and c not in own:
                own.append(c)
        if own:
            already = [r for r in db_rows
                       if set(own) <= {t.strip() for t in
                                       (r.get("linked_cluster_ids") or "").split("|")}]
            if already:
                skipped.append(f"{pid}: already applied as db {already[0]['db_id']} "
                               f"(its cluster(s) {' + '.join(own)} are linked there)")
                continue

        hit = existing.get(norm(name)) or existing_lat.get(name.lower())
        if hit is not None and not force:
            skipped.append(f"{pid}: {name!r} already exists as db {hit['db_id']}")
            continue
        if hit is not None and force:
            notes.append(f"{pid}: {name!r} also names db {hit['db_id']} — minted "
                         f"separately as a deliberate place split")

        cids = [c for c in [(d.get("cluster_id") or "").strip()] if c]
        for e in extras.get(pid, []):
            c = (e.get("cluster_id") or "").strip()
            if c and c not in cids:
                cids.append(c)

        # A printing house is minted as Printer, not Publisher: the decision
        # records that by retyping its cluster first (kind=retype), and the
        # new row takes the first cluster's type. Untyped clusters stay
        # Publisher, which is what every earlier mint got.
        org_type = next(((rv[c].get("org_type") or "").strip() for c in cids
                         if c in rv and (rv[c].get("org_type") or "").strip()),
                        "") or "Publisher"

        db_id = str(next_id)
        next_id += 1
        row = {f: "" for f in db_fields}
        row["db_id"] = db_id
        row["name_yiddish"] = name
        row["org_type"] = org_type
        row["linked_cluster_ids"] = " | ".join(cids)
        if apply:
            db_rows.append(row)
            existing[norm(name)] = row

        for c in cids:
            cl = rv.get(c)
            if cl is None:
                skipped.append(f"{pid}: cluster {c} not in {REVIEW.name}")
                continue
            cur = (cl.get("decision") or "").strip()
            if cur and cur != "ALIGN":
                skipped.append(f"{pid}: cluster {c} already decided {cur}; "
                               f"linked from core_db only")
                continue
            if apply:
                cl["decision"] = "ALIGN"
                cl["aligned_db_id"] = db_id
                note = (f"publisher audit {TODAY} ({pid}): minted db {db_id} "
                        f"{name} and aligned here. {d.get('rationale','')}")
                cl["reviewer_notes"] = ((cl["reviewer_notes"] + " | " + note)
                                        if cl["reviewer_notes"].strip() else note)
                stamp(cl, reviewer)

        minted.append(f"{pid}  db {db_id}  {name}  <- {', '.join(cids) or '(no cluster)'}")
        entries.append(log_entry(
            reviewer, "mint", db_id, "NEW",
            f"{pid}: {d.get('rationale','')}", name_yiddish=name,
            clusters=cids, org_type=org_type))
        for c in cids:
            entries.append(log_entry(
                reviewer, "alignment", c, "ALIGN",
                f"{pid}: aligned to newly minted db {db_id}", aligned_db_id=db_id))

    print(f"{'APPLY' if apply else 'DRY-RUN'}  {len(minted)} mint(s), "
          f"{len(skipped)} skipped   (ids from origin high-water mark)\n")
    for m in minted:
        print("  " + m)
    if notes:
        print()
        for n in notes:
            print("  note " + n)
    if skipped:
        print()
        for s in skipped:
            print("  skip " + s)

    if not apply:
        print("\nNothing written. Re-run with --apply to write.")
        return
    if not minted:
        print("\nNothing to write; files untouched.")
        return

    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    for path, fields, rows in ((CORE_DB, db_fields, db_rows),
                               (REVIEW, rv_fields, rv_rows)):
        shutil.copy2(path, path.with_suffix(f".tsv.pre_publisher_mints_{ts}"))
        _dump(path, fields, rows)
    n = log_rows(entries)
    print(f"\nwrote {CORE_DB.name} and {REVIEW.name} "
          f"(backups .pre_publisher_mints_{ts}); logged {n} row(s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", required=True)
    ap.add_argument("--only", default="",
                    help="comma-separated decision_ids to mint (default: all "
                         "approved mint rows). A mint coins a permanent id, so "
                         "mint only what has actually been approved.")
    a = ap.parse_args()
    main(a.apply, a.reviewer,
         {x.strip() for x in a.only.split(",") if x.strip()})
