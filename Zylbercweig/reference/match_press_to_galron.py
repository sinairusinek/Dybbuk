"""Match core_db press organizations (and org clusters) to Galron's union list.

Produces a REVIEW TSV, never a write: this proposes digitisation links, and a
wrong link sends a researcher to the wrong newspaper's scans.

Two stages, both emitted into one file with a `stage` column:

  db       -- core_db.tsv rows whose org_type is a press type (the 182 live
              newspaper/journal/publisher rows)
  cluster  -- organizations_clustered.tsv clusters that look press-like but
              were never typed, which is where unlinked periodicals hide

Matching crosses scripts by necessity: core_db records most periodicals only in
Hebrew script, Galron's Latin sheet only in Latin letters. The comparable form
is the consonant skeleton from press_match_key -- see that module for why the
existing YIVO transliterator cannot do this job on unpointed titles.

Place is a confirmation signal, not a filter. Galron's places are unnormalised
and differ by sheet (`תל־אביב (ישראל)` vs `Warsaw (Poland)`), so a place
agreement raises confidence while a disagreement never rejects a pair -- many
papers moved, and core_db's address field is frequently empty.
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from press_match_key import (HEBREW_RE, PHONETIC_AVAILABLE,  # noqa: E402
                             phonetic_agreement, similarity, skeleton)

HERE = Path(__file__).resolve().parent
ORGS = HERE.parent / "organizations"
GALRON = HERE / "galron_union_list.tsv"
CORE_DB = ORGS / "core_db.tsv"
CLUSTERS = ORGS / "organizations_clustered.tsv"
OUT = HERE / "galron_press_match_review.tsv"

# NOTHING here auto-accepts. An exact skeleton agreement is not sufficient
# evidence on its own: dropping every vowel makes short titles collide outright
# (בבל/יובל -> "bl", רוס/אורות -> "rs", אויפקום/אופקים -> "fkm",
# דער האמער/המעורר -> "hmr"). All four of those scored 1.000 on a first run.
#
# So the top band is STRONG -- "a human should be able to confirm this at a
# glance" -- and it additionally requires the skeleton to carry enough
# consonants to mean something, plus a second independent signal (place
# agreement, or the Hebrew-script forms agreeing directly).
T_STRONG = 1.00
T_REVIEW = 0.60

# Below this many consonants a skeleton is too lossy to stand alone. בבל and
# יובל both reduce to "bl"; so do dozens of unrelated two-consonant titles.
MIN_SKELETON_CONSONANTS = 4

PRESS_TYPE_RE = re.compile(
    r"newspaper|journal|periodical|publish|printer|press", re.I)

# Cluster names that look like periodicals without being typed as such.
PRESS_HINT_RE = re.compile(
    r"צייטונג|צײטונג|טאגבלאט|טאָגבלאַט|וואכנבלאט|וואָכנבלאַט|בלאט|בלאַט|"
    r"בלעטער|שטימע|זשורנאל|זשורנאַל|עקספרעס|פאסט|פּאָסט|נייעס|"
    r"tsaytung|tseytung|zeitung|cajtung|togblat|tagblat|vokhnblat|blat|"
    r"bleter|shtime|zhurnal|journal|gazet|press|news|herald|tribune",
    re.I)


def read_tsv(path: Path) -> list[dict]:
    csv.field_size_limit(10 ** 9)
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def is_deprecated(row: dict) -> bool:
    return (row.get("deprecated") or "").strip().upper() in ("Y", "YES", "TRUE", "1")


QUALIFIER_RE = re.compile(r"\(([^)]*)\)|<([^>]*)>|\[([^\]]*)\]")


def qualifier_places(text: str) -> set[str]:
    """Place skeletons from a title's own disambiguating qualifier.

    Both sources disambiguate homonymous titles inline -- core_db writes
    "Forverts (Detroit)" and "די אידישע וועלט (פֿילאדעלפֿיע)", Galron writes
    "די אידישע וועלט <סט. פטרבורג>". The title keys are then identical while
    the papers are different, so this qualifier is the only thing separating
    them and has to be read, not discarded.
    """
    out = set()
    for match in QUALIFIER_RE.finditer(text or ""):
        inner = next((g for g in match.groups() if g), "")
        for part in re.split(r"[,;/]", inner):
            key = skeleton(part)
            if key:
                out.add(key)
    return out


def place_tokens(text: str) -> set[str]:
    """Crude place key: Galron's places are unnormalised, so compare skeletons."""
    if not text:
        return set()
        # Drop the country parenthetical Galron appends.
    head = re.split(r"[(<]", text)[0]
    return {skeleton(t) for t in re.split(r"[,/;]| - ", head) if skeleton(t)}


def galron_index(rows: list[dict]) -> dict[str, list[dict]]:
    """Bucket Galron rows by skeleton, and by each token, for blocking."""
    index: dict[str, list[dict]] = {}
    for row in rows:
        keys = set()
        for field in ("title", "title_hebrew", "title_latin"):
            key = skeleton(row.get(field, ""))
            if key:
                keys.add(key)
                for token in key.split():
                    if len(token) >= 3:
                        keys.add(token)
        for key in keys:
            index.setdefault(key, []).append(row)
    return index


def candidates(name: str, index: dict[str, list[dict]]) -> list[dict]:
    key = skeleton(name)
    if not key:
        return []
    seen, out = set(), []
    for probe in [key] + [t for t in key.split() if len(t) >= 3]:
        for row in index.get(probe, ()):
            if row["row_id"] not in seen:
                seen.add(row["row_id"])
                out.append(row)
    return out


def best_match(names: list[str], place: str,
               index: dict[str, list[dict]]) -> dict | None:
    best = None
    for name in [n for n in names if n and n.strip()]:
        for cand in candidates(name, index):
            for field in ("title", "title_hebrew", "title_latin"):
                other = cand.get(field, "")
                if not other.strip():
                    continue
                score, how = similarity(name, other)
                phon = phonetic_agreement(name, other)
                # Keep a pair the skeleton would drop if phonetics vouches for
                # it: that is how אונזער וואָרט / Undzer Vort survives.
                if score < T_REVIEW and phon < 1.0:
                    continue
                qual_a = qualifier_places(name)
                qual_b = (qualifier_places(other)
                          | place_tokens(cand["place_of_publication"]))
                # Both sides named a place and they disagree -> different paper.
                qualifier_clash = bool(qual_a and qual_b and not (qual_a & qual_b))

                shared = place_tokens(place) & place_tokens(
                    cand["place_of_publication"])
                if qual_a and qual_b and (qual_a & qual_b):
                    shared = shared | (qual_a & qual_b)
                # A place agreement is worth a nudge, never a veto.
                adjusted = min(1.0, score + 0.03) if shared else score
                same_script = bool(HEBREW_RE.search(name)) == bool(
                    HEBREW_RE.search(other))
                combined = adjusted + phon * 0.25
                if best is None or combined > best["combined"]:
                    best = {
                        "score": adjusted, "raw": score, "how": how,
                        "combined": combined,
                        "matched_on": name, "galron_field": field,
                        "cand": cand, "place_agree": "Y" if shared else "",
                        "same_script": same_script, "phonetic": phon,
                        "qualifier_clash": qualifier_clash,
                    }
    return best


def band(hit: dict) -> str:
    """STRONG / REVIEW / WEAK. Never "accepted" -- a human decides every row.

    Two independent channels vote: the orthographic skeleton (how it is
    spelled, vowels dropped) and Daitch-Mokotoff phonetics (how it sounds,
    vowels kept). Their failure modes do not overlap, which is the whole point
    of running both -- דער האמער/HaMeorer is a perfect skeleton match and a
    phonetic non-match, while אונזער/Undzer is the reverse.

    Neither channel is trusted alone: phonetics over-matches once generic type
    words are stripped (די אידישע וועלט vs Di Yidishe Shtime both reduce to
    "yidishe"), and the skeleton collides on short titles.
    """
    key = skeleton(hit["matched_on"]).replace(" ", "")
    phon = hit["phonetic"]

    # The two sources named different cities for the same title: homonymous
    # papers (Di Yidishe Velt ran in both Philadelphia and St. Petersburg).
    if hit["qualifier_clash"]:
        return "WEAK"

    # Short keys collide too readily to assert anything, however they scored.
    if len(key) < MIN_SKELETON_CONSONANTS:
        return "WEAK"
    # A perfect spelling match that sounds nothing alike is the signature of a
    # vowel-collision, not a match.
    if hit["raw"] >= T_STRONG and phon == 0.0 and PHONETIC_AVAILABLE:
        return "WEAK"
    # Both channels agree outright.
    if hit["raw"] >= T_STRONG and phon >= 1.0:
        return "STRONG"
    # One channel is perfect and the other corroborates at all.
    if hit["raw"] >= T_STRONG and (phon >= 0.5 or hit["place_agree"]):
        return "REVIEW"
    return "REVIEW" if hit["raw"] >= T_REVIEW or phon >= 1.0 else "WEAK"


COLUMNS = [
    "stage", "band", "score", "phonetic", "signal", "place_agree",
    "same_script", "qualifier_clash",
    "db_id", "cluster_id", "org_type", "db_name", "db_name_yiddish",
    "db_address", "matched_on", "galron_row_id", "galron_title",
    "galron_field", "galron_place", "galron_years", "galron_language",
    "galron_depository", "galron_gated", "galron_scan_url",
    "db_skeleton", "galron_skeleton", "decision", "reviewer", "note",
]


def emit(stage: str, hit: dict, *, db_id="", cluster_id="", org_type="",
         name="", name_yiddish="", address="") -> dict:
    cand = hit["cand"]
    return {
        "stage": stage, "band": band(hit),
        "score": f"{hit['score']:.3f}",
        "phonetic": f"{hit['phonetic']:.2f}", "signal": hit["how"],
        "place_agree": hit["place_agree"],
        "same_script": "Y" if hit["same_script"] else "",
        "qualifier_clash": "Y" if hit["qualifier_clash"] else "",
        "db_id": db_id, "cluster_id": cluster_id, "org_type": org_type,
        "db_name": name, "db_name_yiddish": name_yiddish, "db_address": address,
        "matched_on": hit["matched_on"],
        "galron_row_id": cand["row_id"], "galron_title": cand["title"],
        "galron_field": hit["galron_field"],
        "galron_place": cand["place_of_publication"],
        "galron_years": cand["years_covered"],
        "galron_language": cand["language"],
        "galron_depository": cand["depository"], "galron_gated": cand["gated"],
        "galron_scan_url": cand["scan_url"],
        "db_skeleton": skeleton(hit["matched_on"]),
        "galron_skeleton": skeleton(cand.get(hit["galron_field"], "")),
        "decision": "", "reviewer": "", "note": "",
    }


def main() -> None:
    galron = read_tsv(GALRON)
    index = galron_index(galron)
    print(f"galron: {len(galron)} rows, {len(index)} blocking keys",
          file=sys.stderr)

    out_rows = []

    # --- stage 1: core_db press rows -------------------------------------
    db_rows = [r for r in read_tsv(CORE_DB)
               if PRESS_TYPE_RE.search(r.get("org_type") or "")
               and not is_deprecated(r)]
    print(f"core_db press rows: {len(db_rows)}", file=sys.stderr)
    db_hits = 0
    for row in db_rows:
        names = [row.get("name_yiddish", ""), row.get("name", ""),
                 row.get("name_yiddish_translit", "")]
        names += [v for v in (row.get("name_variants") or "").split("|") if v]
        hit = best_match(names, row.get("address", ""), index)
        if hit:
            db_hits += 1
            out_rows.append(emit(
                "db", hit, db_id=row.get("db_id", ""),
                org_type=row.get("org_type", ""), name=row.get("name", ""),
                name_yiddish=row.get("name_yiddish", ""),
                address=row.get("address", "")))
    print(f"  matched: {db_hits}/{len(db_rows)}", file=sys.stderr)

    # --- stage 2: press-like clusters -------------------------------------
    if CLUSTERS.exists():
        cluster_rows = read_tsv(CLUSTERS)
        # The clustered export carries flattened XML paths as headers; the
        # usable name fields are the cluster's canonical form and the raw
        # clustered mention, with the org title as a third fallback.
        name_cols = [c for c in ("canonical_yiddish", "clustered organization",
                                 "_ - organizations - _ - title")
                     if c in cluster_rows[0]]
        name_col = name_cols[0] if name_cols else None
        id_col = "cluster_id" if "cluster_id" in cluster_rows[0] else None
        type_col = "_ - organizations - _ - org_type"
        place_col = "_ - organizations - _ - locations - _ - settlement"
        print(f"clusters: {len(cluster_rows)} rows "
              f"(names={name_cols}, id={id_col})", file=sys.stderr)
        if name_cols and id_col:
            seen_clusters = set()
            press_like = 0
            cl_hits = 0
            for row in cluster_rows:
                cid = row.get(id_col, "")
                if not cid or cid in seen_clusters:
                    continue
                names = [row.get(c, "") for c in name_cols]
                org_type = row.get(type_col, "")
                # A cluster qualifies either by an explicit press org_type or
                # by a press word in one of its names: the untyped ones are the
                # whole reason this stage exists.
                if not (PRESS_TYPE_RE.search(org_type)
                        or any(PRESS_HINT_RE.search(n) for n in names if n)):
                    continue
                seen_clusters.add(cid)
                press_like += 1
                hit = best_match(names, row.get(place_col, ""), index)
                if hit:
                    cl_hits += 1
                    out_rows.append(emit(
                        "cluster", hit, cluster_id=cid, org_type=org_type,
                        name=names[0] if names else "",
                        address=row.get(place_col, "")))
            print(f"  press-like clusters: {press_like}, matched: {cl_hits}",
                  file=sys.stderr)

    out_rows.sort(key=lambda r: (r["stage"], -float(r["score"])))
    with OUT.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, delimiter="\t",
                                lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out_rows)
    counts: dict[str, int] = {}
    for row in out_rows:
        counts[row["band"]] = counts.get(row["band"], 0) + 1
    summary = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    print(f"wrote {OUT.name}: {len(out_rows)} rows ({summary})",
          file=sys.stderr)


if __name__ == "__main__":
    main()
