"""Report why each person sits in a surname's candidate set, weakest first.

A candidate whose heading shows a different surname is usually CORRECT: married
women appear under the married name with the maiden name bracketed (פינקעל, עמאַ
[טאָמאַשעווסקי]), and pseudonyms do the same (כאַנוקאָוו, לייוויק). Flagging every
cross-surname member therefore produces ~165 hits that are nearly all right, and
flagging none is equally useless. What actually separates a real error from a
legitimate maiden name is the STRENGTH of the link:

  heading    the hub's own heading carries this surname            (strongest)
  variant    a bracketed maiden name / alias / spelling variant does
  validated  an RA-validated bare-surname surface points here
  fuzzy      nothing above — it joined only by spelling similarity  (weakest)

`fuzzy` members are the ones worth a human look: that is how קעסלער (Kessler)
acquired זאַרזשעווסקאַ, פאַלינאַ [קעלער], whose bracket reads Keler — one letter
apart, so the fuzzy pass accepted it.

Confirmed errors go in surname_group_overrides.tsv, which the resolver honours:

    surname_token   hub_id      action   reason
    קעסלער          HUB-D3353   exclude  bracket reads קעלער, a different surname

Re-run resolve_surname_mentions.py afterwards to apply them.

Run: python3.11 Zylbercweig/people/audit_surname_memberships.py
"""
from __future__ import annotations

import csv
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from people_common import load_extracted, read_tsv, write_tsv  # noqa: E402
from people_similarity import (  # noqa: E402
    expand_name_variants,
    normalize_person_name,
    split_name_tokens,
    token_variant_similarity,
)
from resolve_surname_mentions import (  # noqa: E402
    HubIndex,
    build_given_counts,
    heading_surname_tokens,
)

GROUPS_TSV = HERE / "surname_groups.tsv"
OUT_TSV = HERE / "surname_membership_audit.tsv"

OUT_FIELDS = ["evidence", "best_sim", "surname", "n_mentions", "family_cluster",
              "n_candidates", "hub_id", "canonical_heading", "hub_occurrences",
              "matched_on"]


def main() -> None:
    idx = HubIndex()
    extracted = load_extracted()
    given_counts = build_given_counts(extracted)
    by_pid = {r["person_id"]: r for r in extracted}

    def entry_tokens(hub_id: str) -> tuple[set[str], set[str]]:
        """(heading surname tokens, all variant tokens incl. bracketed aliases)."""
        head: set[str] = set()
        var: set[str] = set()
        for p in (idx.hubs.get(hub_id, {}).get("entry_person_ids") or "").split("|"):
            e = by_pid.get(p)
            if not e:
                continue
            head |= heading_surname_tokens(e.get("heading", ""), given_counts)
            for v in expand_name_variants(e.get("heading", ""),
                                          e.get("names_variants", ""),
                                          e.get("subheading", "")):
                var |= set(split_name_tokens(v))
        return head, var

    rows = []
    for g in read_tsv(GROUPS_TSV):
        token = normalize_person_name(g["surname"])
        hub_ids = [h for h in g["hub_ids"].split("|") if h]
        if len(hub_ids) < 2:
            continue                      # a lone candidate is not a grouping
        for hid in hub_ids:
            head, var = entry_tokens(hid)
            if token in head:
                ev, on = "heading", "own heading"
            elif token in var:
                ev, on = "variant", "bracketed alias / maiden name / variant"
            elif hid in idx.surface_prior.get(token, {}):
                ev, on = "validated", "RA-validated bare-surname surface"
            else:
                ev, on = "fuzzy", "spelling similarity only"
            best = max((token_variant_similarity(token, t) for t in (head | var)),
                       default=0.0)
            rows.append({
                "evidence": ev,
                "best_sim": round(best, 3),
                "surname": g["surname"],
                "n_mentions": g["n_mentions"],
                "family_cluster": g["family_cluster"],
                "n_candidates": len(hub_ids),
                "hub_id": hid,
                "canonical_heading": idx.heading(hid),
                "hub_occurrences": int(idx.hub_total_occ.get(hid, 0)),
                "matched_on": on,
            })

    order = {"fuzzy": 0, "validated": 1, "variant": 2, "heading": 3}
    rows.sort(key=lambda r: (order[r["evidence"]], r["best_sim"],
                             -int(r["n_mentions"])))
    write_tsv(OUT_TSV, rows, OUT_FIELDS)

    tally: dict[str, int] = {}
    for r in rows:
        tally[r["evidence"]] = tally.get(r["evidence"], 0) + 1
    print(f"memberships audited: {len(rows)} across "
          f"{len({r['surname'] for r in rows})} multi-candidate groups")
    for ev in ("heading", "variant", "validated", "fuzzy"):
        print(f"  {ev:10s} {tally.get(ev, 0)}")
    weak = [r for r in rows if r["evidence"] == "fuzzy"]
    print(f"\nwrote {OUT_TSV.name} — {len(weak)} membership(s) rest on spelling "
          f"similarity alone and are worth a human check:")
    for r in weak[:22]:
        fam = "👪" if r["family_cluster"] == "1" else "  "
        print(f"  {fam} sim={r['best_sim']:.2f}  {r['surname']}  ←  "
              f"{r['hub_id']}  {r['canonical_heading']}")


if __name__ == "__main__":
    main()
