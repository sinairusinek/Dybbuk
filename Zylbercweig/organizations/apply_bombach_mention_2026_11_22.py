"""Bombach's inn: add Podzamche's missed mention, record the street (Sinai, 2026-11-22).

Shalom Podzamche's entry (Volume_3, facs_12_tr_1752652594) says he played
"אין לעמבערג אין באָמבאַך'ס שענק אויף דער ברייטער גאַס"; the extraction produced no
row for it. Added as a mention of cluster ORG-C01804 (db 4992, after
apply_dup_merges_2026_11_22.py merged Hotel Bombach 4997 into it), copied from his
existing Lemberg row so the entry metadata is identical. 4992 gets the address
ברייטע גאַס (Szeroka).

Run after apply_dup_merges_2026_11_22.py. Idempotent.
Usage: python3.11 apply_bombach_mention_2026_11_22.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, datetime as _dt, json
from pathlib import Path
from apply_education_q2_2026_11_22 import read, write

HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
CL, AL, CORE = HERE / "organizations_clustered.tsv", HERE / "org_alignment_review.tsv", HERE / "core_db.tsv"
CID, DB, ADDR = "ORG-C01804", "4992", "ברייטע גאַס"
NAME = "באָמבאַך'ס שענק"
SENT = "טלום פּאָדזאַמטשע האָט אויך געשפּילט אין לעמבערג אין באָמבאַך'ס שענק אויף דער בּרייטער גאַס"
XMLID, TEMPLATE_CID = "facs_12_tr_1752652594", "ORG-C00482"
P = "_ - organizations - _ - "


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, cl, cnl = read(CL)
    af, al, anl = read(AL)
    kf, core, knl = read(CORE)
    log = []
    mine = [r for r in cl if r["cluster_id"].strip() == CID]
    if not any(r["_ - xml:id"] == XMLID for r in mine):
        tpl = next(r for r in cl if r["_ - xml:id"] == XMLID and r["cluster_id"].strip() == TEMPLATE_CID)
        new = {k: "" for k in cf}
        for k in cf:   # entry-level metadata only
            if not k.startswith(("_ - organizations", "_ - family_background", "_ - endnotes")) and k not in (
                    "clustered organization", "name_type", "confidence", "review_flag", "cluster_id",
                    "canonical_yiddish", "cluster_size", "auto_or_review", "_ - entry"):
                new[k] = tpl[k]
        new.update({P + "org_type": "Hospitality", P + "title": NAME, "clustered organization": NAME,
                    P + "relations - _ - category": "Employment_Performance",
                    P + "relations - _ - specific_relation": "performer",
                    P + "relations - _ - original_sentence": SENT,
                    P + "locations - _ - settlement": "לעמבערג", P + "locations - _ - Venue": NAME,
                    P + "locations - _ - address": ADDR, "name_type": "proper_name", "confidence": "manual",
                    "review_flag": f"manual_add 11-22 {reviewer}: missed by extraction",
                    "cluster_id": CID, "canonical_yiddish": mine[0]["canonical_yiddish"],
                    "auto_or_review": "manual"})
        cl.insert(cl.index(mine[-1]) + 1, new)
        mine.append(new)
        log.append([now, reviewer, "org_review", "add_mention", CID, "", "Podzamche: Bombach's tavern on the Broad Street, Lemberg (missed by extraction)",
                    json.dumps({"xml_id": XMLID, "db_id": DB, "source": SRC}, ensure_ascii=False)])
        print(f"  + mention {XMLID} -> {CID}")
    for r in mine:
        r["cluster_size"] = str(len(mine))
    a = next(r for r in al if r["cluster_id"].strip() == CID)
    a["cluster_size"] = str(len(mine))
    if ADDR not in a["extracted_addresses"]:
        a["extracted_addresses"] = " | ".join(x for x in [a["extracted_addresses"].strip(), ADDR] if x)
    k = next(r for r in core if r["db_id"] == DB)
    if k["address"] != ADDR:
        k["address"] = ADDR
        log.append([now, reviewer, "org_addresses", "set_address", DB, f"address={ADDR}",
                    "Bombach's inn: Szeroka / Broad Street (Podzamche)", json.dumps({"source": SRC})])
        print(f"  {DB} address -> {ADDR}")
    print(f"{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    write(CL, cf, cl, cnl); write(AL, af, al, anl); write(CORE, kf, core, knl)
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
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
