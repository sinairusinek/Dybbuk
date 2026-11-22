"""School-system networks: their generic sub-units align to the network parent (Sinai, 2026-11-22).

The 10-04 generic sweep (rule_2026_10_04) treated Arbeter Ring, Tarbut and TsIShO as
benefactor networks, like Baron Hirsch. They are not. Each was a membership or school
organization that RAN its own schools, so "teacher at the Arbeter Ring school in Boston"
or "graduated a TsIShO school" does relate the person to an organization: the network.
Only Hirsch, ICA and ORT are funder networks; those rows stay GENERIC.

Rule: a network sub-unit that does not individuate (bare or city + school) -> ALIGN to the
network parent. One that does (a small town with one such school, a branch number, a
named camp) -> its own row, with parent_db_id = the network.

1. ALIGN (were GENERIC by rule_2026_10_04, or swept with them):
     Arbeter Ring 189: C00272_Q02 (Boston school, teacher 1921-26), C01520 (alumni clubs),
       C01704 (teachers' courses; possibly = seminary 1982, not asserted), C01705
       ("non-partisan AR school"), C02444 ("an AR school"), C05136 (Chicago, added same day)
     TsIShO 1111: C01477, C03665 (Warsaw TsIShO school, graduated)
     Tarbut (minted here): C03388 (Warsaw Tarbut school), C07185 (Tarbut gymnasium, no city)
2. MINT: Tarbut (network parent); Tarbut elementary school, Otaci (C07186: a small town's
   one Tarbut school individuates), parent = Tarbut.
3. The Arbeter Ring main cluster C00198_Q01 (26 mentions: membership, choir, the AR
   cemeteries, concert tours) was place-split on 10-04 into P01 (Waldheim) and P02
   (New York), then both children were swept GENERIC as a "movement network". Since the
   11-01 KG rule, a SPLIT_DONE parent's mentions go to the children (GENERIC -> dropped) or
   are dropped when placeless, so all 26 left the graph. The split was wrong: the remainder
   after Q02-Q07 split off the local branches IS the national organization (Q04 Los
   Angeles is already ALIGN 189). Parent SPLIT_DONE -> ALIGN 189; P01, P02 -> ALIGN 189.
4. set_parent on existing sub-units that lacked one:
     -> 189: 1982 seminary, 1994 Pittsburgh school, 2175 Williamsburg school 1, 2343
        sanatorium, 2527 branch 6, 2536 education committee, 2537 youth center, 2596
        branch 324, 2644 branch 597, 2656 division, 2959 children's library, 3073 + 3184
        old-age home
     -> 2478 (Arbeter-ring London): 1978 radical school, branch 3, London 1923
     -> 1111 TsIShO: 701 Nesvizh TsIShO school
   Not parented: 2610 'Hamler socialist AR of Harlem' (a co-founding; may predate or not
   be the Workmen's Circle).

Flagged, not changed: C00069 'צווישאָ' (Zhitnitsky, president of it and the Sholem
Aleichem schools) is likely the Argentine TsVISHO, not 1111; C04158 'Jewish school
organization' too vague to place.
Also flagged: 3073 and 3184 look like the same old-age home (both Troy family
mentions); 1994 Pittsburgh also holds Koplan's indefinite "an AR school" mention.

Idempotent. Usage: python3.11 apply_network_parents_2026_11_22.py [--apply --reviewer Sinai]
"""
from __future__ import annotations
import argparse, csv, datetime as _dt, io, json, subprocess, sys, unicodedata
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).name
TAG = "network parents 2026-11-22"

AR, AR_LONDON, TSISHO = "189", "2478", "1111"
MINTS = {  # key -> (name, name_yiddish, address, parent key or db_id)
    "tarbut": ("Tarbut (Hebrew-language school network)", "תרבות", "", ""),
    "tarbut_otaci": ("Tarbut elementary school, Otaci", "תרבות עלעמענטאַר שול (אָטאַקיי)",
                     "אָטאַקיי", "MINT:tarbut"),
}
ALIGN = {  # cluster_id -> (target, why)
    "ORG-C00272_Q02": (AR, "teacher at the Arbeter Ring school in Boston 1921-26; a city had several, so the network"),
    "ORG-C01520": (AR, "Arbeter Ring alumni clubs (plural): the network's youth body"),
    "ORG-C01704": (AR, "Arbeter Ring teachers' courses; perhaps = the seminary 1982, not asserted"),
    "ORG-C01705": (AR, "'non-partisan Arbeter Ring school', no place: the network"),
    "ORG-C02444": (AR, "'an Arbeter Ring school' (indefinite): the network"),
    "ORG-C05136": (AR, "manager of the local Arbeter Ring school, Chicago (spelled ארבייטער): the network"),
    "ORG-C01477": (TSISHO, "graduated the TsIShO school, Warsaw: TsIShO ran many there"),
    "ORG-C03665": (TSISHO, "graduated a TsIShO school, Warsaw: TsIShO ran many there"),
    "ORG-C03388": ("MINT:tarbut", "rhythmics lessons at the Tarbut school, Warsaw: Tarbut ran several there"),
    "ORG-C07185": ("MINT:tarbut", "graduated the Tarbut gymnasium 1927, no place: the network"),
    "ORG-C07186": ("MINT:tarbut_otaci", "teacher at the Tarbut elementary school in Otaci 1930: one in a small town"),
    "ORG-C00198_Q01": (AR, "un-split: the remainder after the local branches split off is the national "
                           "Workmen's Circle (Q04 LA already ALIGN 189); the 10-04 place split + generic "
                           "sweep dropped all 26 mentions from the KG"),
    "ORG-C00198_Q01_P01": (AR, "child of the un-split C00198_Q01 (Waldheim cemetery mention)"),
    "ORG-C00198_Q01_P02": (AR, "child of the un-split C00198_Q01 (New York)"),
}
PARENTS = {d: AR for d in "1982 1994 2175 2343 2527 2536 2537 2596 2644 2656 2959 3073 3184".split()}
PARENTS.update({"1978": AR_LONDON, "701": TSISHO})


def sk(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).translate(str.maketrans("ךםןףץ", "כמנפצ"))


def _rw(p: Path):
    term = "\r\n" if b"\r\n" in p.read_bytes()[:4096] else "\n"
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd), term


def _dump(p: Path, fields, rows, term) -> None:
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator=term)
        w.writeheader()
        w.writerows(rows)


def pipes(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def origin_high_water() -> int:
    subprocess.run(["git", "fetch", "origin", "main"], cwd=HERE, capture_output=True, timeout=120)
    out = subprocess.run(["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"],
                         cwd=HERE, capture_output=True, text=True, timeout=120, check=True).stdout
    return max(int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t") if r["db_id"].isdigit())


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kf, core, kt = _rw(HERE / "core_db.tsv")
    af, al, at = _rw(HERE / "org_alignment_review.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    cards_by = {c["db_id"]: c for c in cards}
    log: list[list[str]] = []

    def L(view, action, target, decision, text, extra):
        log.append([now, reviewer, view, action, target, decision, f"{TAG}: {text}",
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    next_id = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    have = {sk(r["name_yiddish"]): r["db_id"] for r in core if not r["deprecated"].strip()}

    def resolve(t: str) -> str:
        nonlocal next_id
        if not t.startswith("MINT:"):
            return t
        name, yid, addr, parent = MINTS[t[5:]]
        if sk(yid) in have:
            return have[sk(yid)]
        parent = resolve(parent) if parent else ""
        d = str(next_id); next_id += 1
        row = {f: "" for f in kf}
        row.update(db_id=d, name=name, name_yiddish=yid, address=addr, parent_db_id=parent, org_type="Education")
        core.append(row); by[d] = row; have[sk(yid)] = d
        L("org_review", "mint", d, "NEW", name, {"name_yiddish": yid, "parent_db_id": parent})
        print(f"  mint db {d}  {name}  parent={parent or '-'}")
        return d

    for cid, (t, why) in ALIGN.items():
        r = arow.get(cid)
        if r is None:
            print(f"  ERROR missing {cid}"); continue
        d = resolve(t)
        if r["decision"].strip() == "ALIGN" and r["aligned_db_id"].strip() == d:
            continue
        was = f"{r['decision']} {r['aligned_db_id']}".strip()
        # unlink from any other db row; link to the target
        for c in core:
            if c["db_id"] != d and cid in pipes(c["linked_cluster_ids"]):
                c["linked_cluster_ids"] = " | ".join(x for x in pipes(c["linked_cluster_ids"]) if x != cid)
                L("org_review", "unlink", c["db_id"], "", f"cluster {cid} unlinked", {"cluster": cid})
                print(f"  unlink {cid} from {c['db_id']}")
        if cid not in pipes(by[d]["linked_cluster_ids"]):
            by[d]["linked_cluster_ids"] = " | ".join(pipes(by[d]["linked_cluster_ids"]) + [cid])
        prev = r["reviewer_notes"].strip()
        r["decision"], r["aligned_db_id"] = "ALIGN", d
        r["reviewer_notes"] = f"{TAG}: {why} (was {was})" + (" | " + prev if prev else "")
        r["reviewer"], r["reviewed_at"] = reviewer, now
        L("org_review", "alignment", cid, "ALIGN", why, {"aligned_db_id": d, "was": was})
        print(f"  {cid:20s} {was:12s} -> ALIGN {d}   {r['canonical_yiddish']}")

    for d, p in PARENTS.items():
        r = by[d]
        if r["parent_db_id"].strip() == p:
            continue
        if r["parent_db_id"].strip():
            print(f"  CHECK {d} already has parent {r['parent_db_id']}, not overwritten"); continue
        r["parent_db_id"] = p
        if d in cards_by:
            cards_by[d]["parent_db_id"] = p
        L("org_addresses", "set_parent", d, f"parent_db_id={p}", f"{r['name_yiddish'] or r['name']} under {p}", {"was": ""})
        print(f"  set_parent {d} -> {p}   {r['name_yiddish'] or r['name']}")

    print(f"\n{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME"); return
    if not log:
        return
    _dump(HERE / "core_db.tsv", kf, core, kt)
    _dump(HERE / "org_alignment_review.tsv", af, al, at)
    _dump(HERE / "org_addresses_review.tsv", df, cards, dt)
    p = HERE / "activity_log.tsv"
    b = p.read_bytes()
    term = "\r\n" if b"\r\n" in b[:4096] else "\n"
    with p.open("ab") as f:
        if not b.endswith(b"\n"):
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
