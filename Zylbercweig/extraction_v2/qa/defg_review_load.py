"""How much human review do the D–G place and organization names add? No LLM.

Places: every place surface in stations / events / performance facts (and mentions of kind
place) goes through the itinerary resolver (toponym gazetteer + confirmed Kima rows + plays-KG
places). Unresolved = candidates for the place-review queue.
Organizations: every org surface is matched by letter skeleton against core_db (name,
name_yiddish, name_variants) and the org-alignment clusters (canonical_yiddish, name_variants).
Unmatched = candidates for the Zalmen org queue; eponymous ("X's troupe") are counted apart,
because they resolve through the person, not the name.

  python3.11 qa/defg_review_load.py  -> qa/review_load_places.tsv, qa/review_load_orgs.tsv
"""
import csv, json, re, sys, collections, unicodedata
from pathlib import Path

HERE = Path(__file__).parent
V2 = HERE.parent
Z = V2.parent
sys.path.insert(0, str(Z / "plays" / "itinerary"))
import build_itinerary_data as bid  # noqa: E402
csv.field_size_limit(10**9)


def skel(t):
    t = unicodedata.normalize("NFD", t or "")
    t = re.sub(r"[֑-ׇ]", "", t)
    return re.sub(r"[^\w]|_", "", t)


def tsv(p):
    return csv.DictReader(p.open(encoding="utf-8"), delimiter="\t")


def main():
    D = V2 / "defg"
    # ---- places
    lut = bid.load_gazetteer()
    use = collections.Counter(); ents = collections.defaultdict(set); kind = {}
    for tab in ("stations", "events", "performance_facts"):
        for r in tsv(D / f"{tab}.tsv"):
            p = r.get("place", "").strip()
            if p:
                use[p] += 1; ents[p].add(r["entry_id"].split("~")[0])
    for r in tsv(D / "mentions.tsv"):
        if r["kind"] == "place":
            try:
                kind.setdefault(r["surface"].strip(), json.loads(r["attrs"] or "{}").get("place_kind", ""))
            except Exception:
                pass
    P = []
    st_count = collections.Counter()
    for p, n in use.items():
        st, q, en, *_ = bid.resolve(p, lut)
        status = "resolved" if st.startswith("settlement") else ("region" if st.startswith("region") else "unresolved")
        st_count[(status, "surfaces")] += 1; st_count[(status, "uses")] += n
        if status == "unresolved":
            P.append({"surface": p, "uses": n, "entries": len(ents[p]), "place_kind": kind.get(p, ""),
                      "example_entry": sorted(ents[p])[0]})
    P.sort(key=lambda r: -r["uses"])
    with (HERE / "review_load_places.tsv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(P[0]), delimiter="\t"); w.writeheader(); w.writerows(P)

    # ---- organizations
    known = set()
    for r in tsv(Z / "organizations/core_db.tsv"):
        if r.get("deprecated", "").lower() == "true":
            continue
        for v in [r.get("name", ""), r.get("name_yiddish", "")] + (r.get("name_variants", "") or "").split("|"):
            for x in re.split(r"[;|]", v):
                if skel(x):
                    known.add(skel(x))
    for r in tsv(Z / "organizations/org_alignment_review.tsv"):
        for v in [r.get("canonical_yiddish", "")] + re.split(r"[;|]", r.get("name_variants", "") or ""):
            if skel(v):
                known.add(skel(v))
    ouse = collections.Counter(); oents = collections.defaultdict(set); epon = collections.Counter()
    for r in tsv(D / "mentions.tsv"):
        if r["kind"] != "org":
            continue
        s = r["surface"].strip()
        if not s:
            continue
        ouse[s] += 1; oents[s].add(r["entry_id"].split("~")[0])
        try:
            if json.loads(r["attrs"] or "{}").get("named_after"):
                epon[s] += 1
        except Exception:
            pass
    O = []; oc = collections.Counter()
    for s, n in ouse.items():
        k = skel(s)
        status = "matched" if k in known else ("eponymous" if epon[s] else "unmatched")
        oc[(status, "surfaces")] += 1; oc[(status, "uses")] += n
        if status != "matched":
            O.append({"surface": s, "status": status, "uses": n, "entries": len(oents[s]),
                      "example_entry": sorted(oents[s])[0]})
    O.sort(key=lambda r: (r["status"], -r["uses"]))
    with (HERE / "review_load_orgs.tsv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(O[0]), delimiter="\t"); w.writeheader(); w.writerows(O)

    def line(c, keys):
        ts = sum(c[(k, "surfaces")] for k in keys); tu = sum(c[(k, "uses")] for k in keys)
        return "; ".join(f"{k} {c[(k, 'surfaces')]:,} names ({c[(k, 'surfaces')] / ts:.0%}) / {c[(k, 'uses')]:,} uses ({c[(k, 'uses')] / tu:.0%})" for k in keys)
    print("PLACES:", line(st_count, ("resolved", "region", "unresolved")))
    un = [r for r in P]
    for t in (10, 5, 2):
        print(f"  unresolved names used >= {t} times: {sum(1 for r in un if r['uses'] >= t):,} (cover {sum(r['uses'] for r in un if r['uses'] >= t):,} uses)")
    print("ORGS:", line(oc, ("matched", "eponymous", "unmatched")))
    um = [r for r in O if r["status"] == "unmatched"]
    for t in (10, 5, 2):
        print(f"  unmatched org names used >= {t} times: {sum(1 for r in um if r['uses'] >= t):,} (cover {sum(r['uses'] for r in um if r['uses'] >= t):,} uses)")


if __name__ == "__main__":
    main()
