"""ORG-C01582 -> db 4736, Poale Zion singing society, New York (Sinai, 2026-11-22).

Brunov organised "די פּועלי-ציון געזאַנג סאָסייעטי" in 1911 and led it 2 years; Lekovich
became conductor of "פּועלי ציון געזאַנג-פֿאַראיין" in 1911 after Brunov resigned: one
society. C01582 aligned to 4736; 4736 gets parent Poale Zion 362 and address New York.
Idempotent. Usage: python3.11 apply_poale_zion_choir_2026_11_22.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, datetime as _dt, json
from pathlib import Path
from apply_education_q2_2026_11_22 import read, write

HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
CID, DB, PARENT, ADDR = "ORG-C01582", "4736", "362", "ניו יאָרק"


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kf, core, knl = read(HERE / "core_db.tsv")
    af, al, anl = read(HERE / "org_alignment_review.tsv")
    df, cards, dnl = read(HERE / "org_addresses_review.tsv")
    k = next(r for r in core if r["db_id"] == DB)
    a = next(r for r in al if r["cluster_id"].strip() == CID)
    card = next((c for c in cards if c["db_id"] == DB), None)
    log = []
    if (a["decision"], a["aligned_db_id"]) != ("ALIGN", DB):
        was = (a["decision"], a["aligned_db_id"])
        prev = a["reviewer_notes"].strip()
        a.update(decision="ALIGN", aligned_db_id=DB, reviewer=reviewer, reviewed_at=now,
                 reviewer_notes=(prev + " | " if prev else "") + "11-22 Sinai: same society as 4736 (Brunov 1911, succeeded by Lekovich)")
        log.append([now, reviewer, "org_review", "alignment", CID, "ALIGN", "Poale Zion singing society NY = 4736 (Brunov / Lekovich, 1911)",
                    json.dumps({"aligned_db_id": DB, "was": list(was), "source": SRC})])
        print(f"  {CID} -> ALIGN {DB} (was {was})")
    links = [x.strip() for x in k["linked_cluster_ids"].split("|") if x.strip()]
    if CID not in links:
        k["linked_cluster_ids"] = " | ".join(links + [CID])
        if card is not None:
            cl = [x.strip() for x in card["linked_cluster_ids"].split("|") if x.strip()]
            card["linked_cluster_ids"] = " | ".join(cl + [CID]) if CID not in cl else card["linked_cluster_ids"]
    if k["parent_db_id"] != PARENT:
        k["parent_db_id"] = PARENT
        log.append([now, reviewer, "org_addresses", "set_parent", DB, f"parent_db_id={PARENT}",
                    "Poale Zion singing society under Poale Zion", json.dumps({"source": SRC})])
        print(f"  {DB} parent -> {PARENT}")
    if k["address"] != ADDR:
        k["address"] = ADDR
        log.append([now, reviewer, "org_addresses", "set_address", DB, f"address={ADDR}", "Brunov: New York, 1911",
                    json.dumps({"source": SRC}, ensure_ascii=False)])
        print(f"  {DB} address -> {ADDR}")
    if card is not None:
        card["parent_db_id"] = PARENT
    print(f"{len(log)} log row(s); card {'found' if card else 'MISSING'}")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    write(HERE / "core_db.tsv", kf, core, knl); write(HERE / "org_alignment_review.tsv", af, al, anl)
    write(HERE / "org_addresses_review.tsv", df, cards, dnl)
    p = HERE / "activity_log.tsv"
    b = p.read_bytes()
    with p.open("ab") as f:
        if not b.endswith(b"\n"):
            f.write(b"\r\n")
        for row in log:
            f.write(("\t".join(row) + "\r\n").encode("utf-8"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    x = ap.parse_args()
    if x.apply and not x.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(x.apply, x.reviewer.strip())
