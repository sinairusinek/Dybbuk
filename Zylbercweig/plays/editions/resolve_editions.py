#!/usr/bin/env python3.11
"""Step 4: resolve the extracted publication records. Deterministic, no LLM.

  1. DEDUP   the same printing reported twice in one entry (narrative + the
             end-of-entry bibliography) -> one edition, ED-nnnnn.
  2. WORKS   union-find over (author, title) nodes. Edges come ONLY from what
             the Lexicon states: a record's alt_titles, a double title
             "X אָדער Y", and the model's same_work groups. Identical
             (author, title) nodes in different entries meet automatically.
             Lateiner/Hurwitz works are then pointed at plays_db play_ids --
             nothing is minted there (Sinai 2026-11-01).
  3. ORGS    publisher and periodical/collection strings -> core_db db_id by
             exact normalised name (name / name_yiddish / name_variants),
             place-disambiguated; periodicals -> Galron row via CONFIRMed
             press decisions.
  4. PEOPLE  "SUBJECT" -> the entry's hub (person_hub.entry_person_ids); other
             names -> hub when one hub owns that surface form, else left as
             text.

Every link carries its basis (`*_match`), so a reviewer can see why.

    python3.11 resolve_editions.py [--tag full2]
Outputs: editions_resolved_<tag>.tsv, works_<tag>.tsv
"""
from __future__ import annotations

import argparse
import collections
import csv
import pathlib
import re
import sys
import unicodedata

HERE = pathlib.Path(__file__).resolve().parent
ZYL = HERE.parent.parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(ZYL / "reference"))
import plays_common as pc  # noqa: E402
from press_match_key import place_agree, skeleton  # noqa: E402  (cross-script)

csv.field_size_limit(10**9)

PLAY_KINDS = {"play", "play_translation", "play_adaptation", "libretto"}

# Descriptor printed after the title ("בלינדע ליבע, דראַמע אין 4 אקטן"): it is
# not part of the name and splits otherwise-identical titles.
GENRE_TAIL = re.compile(
    r"\s(?:דראמ|דראמא|קאמעדי|טראגעדי|טראגיקאמעדי|מעלאדראמ|אפערעט|אפערע|"
    r"שפיל|שפאס|סצענע|בילד|לעבנסבילד|לעבענס|פאלקסשטיק|פארס|עטיוד|מאנאלאג|"
    r"איינאקטער|פיעסע|ראמאנ|פאעמע|געזאנג|א שפיל|אינ \d|\d+ אקט)")

PUB_NOISE = re.compile(
    r"\b(?:פארלאג|פערלאג|פארלאגס|פערלאגס|פ ג|פיג|פג|הוצאת|הוצאה|ביכער פארלאג|"
    r"געזעלשאפט|פאבלישינג|פאבלישינג קאמפאני|קאמפאני|קא|פאבל|"
    r"publishing|publishers?|company|co|inc|verlag)\b", re.I)


def tnorm_all(title: str) -> list[str]:
    """Every key a title answers to: whole (descriptor cut) + אָדער halves.
    Latin titles keep a lowercase key (norm_yiddish drops Latin letters)."""
    t = title or ""
    if not re.search(r"[א-ת]", t):
        k = " ".join(re.sub(r"[^0-9a-z ]", " ", t.lower()).split())
        return [k] if k else []
    n = pc.norm_yiddish(t)
    m = GENRE_TAIL.search(" " + n)
    if m and m.start() > 3:
        n = n[:m.start() - 1].strip()
    keys = [n] if len(n) >= 2 else []
    # "אשה רעה (די שלעכטע פרויא)": the bracketed part is a second title
    for inner in re.findall(r"\(([^()]{3,})\)", t):
        keys += [k for k in tnorm_all(inner) if k not in keys]
        bare = pc.norm_yiddish(re.sub(r"\([^()]*\)", " ", t))
        if len(bare) >= 2 and bare not in keys:
            keys.append(bare)
    for seg in pc.title_segments(n):
        if seg not in keys:
            keys.append(seg)
    return keys


ARTICLE = re.compile(r"^(?:דער|די|דאס|דעם|the|der|di|dos|das)\s+")


def okeys(s: str) -> list[str]:
    """Org match keys. core_db mixes forms -- "Di tsukunft- די צוקונפט",
    'פֿאַרלאַג „ביכער" (וואַרשע)' -- so the Hebrew part and the Latin part are
    keyed separately, the bracketed city dropped, the article stripped, and
    publisher noise words ("פֿאַרלאַג", "Publishing Co") cut."""
    s = re.sub(r"\([^()]*\)", " ", s or "")
    heb = PUB_NOISE.sub(" ", pc.norm_yiddish(s))
    lat = PUB_NOISE.sub(" ", " ".join(re.findall(r"[A-Za-z]+", s)).lower())
    out = []
    for k in (heb, lat):
        k = ARTICLE.sub("", " ".join(k.split()))
        if len(k) >= 3 and k not in out:
            out.append(k)
    return out


def onorm(s: str) -> str:
    k = okeys(s)
    return k[0] if k else ""


PLACE_ABBR = {"נ י": "ניו יארק", "ב א": "בוענאס איירעס", "ל א": "לאס אנדזשעלעס",
              "שיקאגא": "Chicago", "שיקאגאָ": "Chicago"}


PLACE_ABBR_LATIN = {"n y": "New York", "ny": "New York", "nyc": "New York",
                    "l a": "Los Angeles", "b a": "Buenos Aires"}


def place_full(p: str) -> str:
    n = pc.norm_yiddish(p).replace("ניויארק", "ניו יארק")
    if n:
        return PLACE_ABBR.get(n, n)
    lat = " ".join(re.sub(r"[^a-z ]", " ", (p or "").lower()).split())
    return PLACE_ABBR_LATIN.get(lat, p or "")


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[max(ra, rb)] = min(ra, rb)


# ── people ──────────────────────────────────────────────────────────────────
def load_people():
    hubs = pc.read_tsv(ZYL / "people" / "person_hub.tsv")
    by_entry, by_surface, by_dbid, heading = {}, collections.defaultdict(set), {}, {}
    for h in hubs:
        heading[h["hub_id"]] = h["canonical_heading"]
        for pid in filter(None, h["entry_person_ids"].split("|")):
            by_entry[pid] = h["hub_id"]
        for d in filter(None, re.split(r"[|,]", h["db_ids"])):
            by_dbid[d.strip()] = h["hub_id"]
        forms = [h["canonical_heading"]] + h["top_surfaces"].split("|")
        if "," in h["canonical_heading"]:      # "קעסלער, דוד" -> "דוד קעסלער"
            sur, _, given = h["canonical_heading"].partition(",")
            forms.append(f"{given} {sur}")
        for f in forms:
            k = pc.norm_yiddish(re.sub(r"\[.*?\]", "", f))
            if len(k.split()) >= 2:            # never resolve a bare surname
                by_surface[k].add(h["hub_id"])
    return by_entry, by_surface, by_dbid, heading


def resolve_person(name, subject_hub, by_surface):
    if not name or name.strip().upper() == "SUBJECT":
        return (subject_hub, "subject") if subject_hub else ("", "")
    k = pc.norm_yiddish(name)
    hubs = by_surface.get(k, set())
    if len(hubs) == 1:
        return next(iter(hubs)), "surface_unique"
    return "", ("surface_ambiguous" if hubs else "")


# ── orgs ────────────────────────────────────────────────────────────────────
PUBLISHER_TYPES = {"Publisher", "Printer", "Printer/Publisher"}
PRESS_TYPES = {"Journals/ Newspapers", "Non-Jewish Journals/ Newspapers"}


def load_orgs():
    rows = pc.read_tsv(ZYL / "organizations" / "core_db.tsv")
    live = {}
    for r in rows:
        if r["out_of_project"] or r["deprecated"] in ("true", "1"):
            continue
        live[r["db_id"]] = r
    idx = collections.defaultdict(set)
    for d, r in live.items():
        places = [r["address"]]
        for f in [r["name"], r["name_yiddish"]] + r["name_variants"].split("|"):
            places += re.findall(r"\(([^()]+)\)", f or "")
            for k in okeys(f):
                idx[k].add(d)
        r["_places"] = [x for x in places if x]
        r["_weight"] = len([c for c in r["linked_cluster_ids"].split("|") if c])
    galron = {}
    for g in pc.read_tsv(ZYL / "reference" / "galron_press_match_review.tsv"):
        if g["decision"] == "CONFIRM" and g["db_id"]:
            galron.setdefault(g["db_id"], g["galron_row_id"])
    return live, idx, galron


def resolve_org(name, place, want, live, idx):
    """-> (db_id, basis). `want` = preferred org_type set."""
    keys = okeys(name)
    if not keys:
        return "", ""
    cands = set().union(*(idx.get(k, set()) for k in keys))
    if not cands:
        return "", "no_match"
    typed = {d for d in cands if live[d]["org_type"] in want} or cands
    pf = place_full(place)
    if len(typed) == 1:
        d = next(iter(typed))
        # both sides placed and the places disagree: a namesake, not a match
        # (Warsaw Kultur-lige publishing house vs the "קולטור-ליגע (קיעוו)" row)
        if pf and live[d]["_places"] and not any(
                place_agree(pf, x) for x in live[d]["_places"]):
            return "", f"place_conflict:{d}"
        return d, "exact" + ("" if live[d]["org_type"] in want else "_othertype")
    if pf:
        here = {d for d in typed if any(place_agree(pf, x) for x in live[d]["_places"])}
        if len(here) == 1:
            return next(iter(here)), "exact+place"
        if here:
            typed = here
    # same name, several city editions (פֿאַרווערטס NY/Chicago/Philadelphia):
    # without a place, take the clearly most-attested one -- marked, reviewable
    ranked = sorted(typed, key=lambda d: -live[d]["_weight"])
    if live[ranked[0]]["_weight"] > live[ranked[1]]["_weight"]:
        return ranked[0], "exact+prominence"
    return "", "ambiguous:" + ",".join(sorted(typed)[:6])


# ── main ────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="full2")
    a = ap.parse_args()

    recs = pc.read_tsv(HERE / f"editions_{a.tag}.tsv")
    groups = pc.read_tsv(HERE / f"same_work_{a.tag}.tsv")
    by_entry, by_surface, by_dbid, heading = load_people()
    live, oidx, galron = load_orgs()
    pid_of = {r["xml_id"]: r["person_id"] for r in recs}
    pid_of.update({g["xml_id"]: g["person_id"] for g in groups})

    # 1. dedup ---------------------------------------------------------------
    seen, eds, n_dup = {}, [], 0
    for r in recs:
        keys = tnorm_all(r["title"])
        key = (r["xml_id"], keys[0] if keys else r["evidence"][:60],
               r["pub_kind"], onorm(r["container"]), onorm(r["publisher"]),
               re.sub(r"\D", "", r["year"])[:4])
        if key in seen:
            seen[key]["dup_count"] += 1
            n_dup += 1
            continue
        r = dict(r, dup_count=1)
        seen[key] = r
        eds.append(r)
    for i, r in enumerate(eds, 1):
        r["edition_id"] = f"ED-{i:05d}"

    # 2. works ---------------------------------------------------------------
    uf = UF()

    # Hand identifications of names the matcher cannot place (surname-only
    # Yiddish authors, non-Yiddish playwrights, translators): author_identifications.tsv,
    # keyed by the exact extracted string. Read by a person, not inferred here.
    # NFKD both sides: the extraction keeps precomposed פֿ (U+FB4E) where a
    # typed table row has פ + rafe, and an exact-string key silently misses.
    def ckey(x):
        return unicodedata.normalize("NFKD", (x or "").strip())
    curated = {ckey(c["author_string"]): c
               for c in pc.read_tsv(HERE / "author_identifications.tsv")}

    def author_key(r, name):
        c = curated.get(ckey(name))
        if c and (c["hub_id"] or c["wikidata"]):
            r["author_qid"], r["author_label"] = c["wikidata"], c["person_label"]
            r["author_role"] = c["role"]
            key = c["hub_id"] or "wd:" + c["wikidata"]
            return key, c["hub_id"], "curated"
        if c:
            r["author_label"], r["author_role"] = c["person_label"], c["role"]
        subj = by_entry.get(r["person_id"] or pid_of.get(r["xml_id"], ""), "")
        hub, basis = resolve_person(name, subj, by_surface)
        return (hub or "txt:" + pc.norm_yiddish(name or "?")), hub, basis

    def original_author(r):
        n = (r["original_author"] or "").strip()
        if not n:
            return
        if n.upper() == "SUBJECT":
            hub = by_entry.get(r["person_id"], "")
            r["original_author_hub"] = hub
            return
        c = curated.get(ckey(n))
        if c:
            r["original_author_hub"], r["original_author_qid"] = c["hub_id"], c["wikidata"]
            return
        hub, _ = resolve_person(n, "", by_surface)
        r["original_author_hub"] = hub

    for r in eds:
        # whose work is it: the subject when they wrote it, or the translator/
        # adapter (a translation is its own Yiddish play; the original is kept
        # as original_title/original_author, not merged into it)
        if r["subject_role"] in ("author", "co_author", "translator", "adapter") \
                or r["author"].strip().upper() == "SUBJECT":
            name = "SUBJECT"
        else:
            name = r["author"]
        ak, hub, basis = author_key(r, name)
        r["author_hub"], r["author_match"] = hub, basis
        r["author_hub_heading"] = heading.get(hub, "")
        if r["subject_role"] in ("translator", "adapter") and r["author"] \
                and r["author"].strip().upper() != "SUBJECT":
            # the model put the ORIGINAL author in `author`: keep it visible
            r["original_author"] = r["original_author"] or r["author"]
        original_author(r)
        nodes = [(ak, k) for t in [r["title"]] + r["alt_titles"].split(" | ")
                 for k in tnorm_all(t)]
        r["_node"] = nodes[0] if nodes else (ak, "∅" + r["edition_id"])
        uf.find(r["_node"])
        for n in nodes[1:]:
            uf.union(r["_node"], n)
    n_groups_used = 0
    for g in groups:
        ak, _, _ = author_key(g, g["author"] or "SUBJECT")
        nodes = [(ak, k) for t in g["titles"].split(" | ") for k in tnorm_all(t)]
        for n in nodes[1:]:
            uf.union(nodes[0], n)
        n_groups_used += len(nodes) > 1

    # plays_db (Lateiner/Hurwitz only) -> node index
    play_of = {}
    for p in pc.load_plays_db():
        hub = by_dbid.get(p["author_db_id"], "")
        if not hub:
            continue
        for t in [p["title_yiddish"]] + p["alt_titles"].split("|"):
            for k in tnorm_all(t):
                play_of.setdefault((hub, k), p["play_id"])

    # looser fallback, same author only: the editions print the older
    # daytshmerish spellings (בליהמעלע, דיא, חרבן, עגיפּטען) that plays_db
    # normalised away. Skeleton = consonant spine, so vowel letters and a
    # silent ה stop mattering.
    def skel(k):
        k = re.sub(r"^(?:דער|די|דיא|דאס|א)\s+", "", k)
        k = re.sub(r"(?<=[יע])ה", "", k)
        return skeleton(k)
    play_skel = {}
    for (hub, k), pid in play_of.items():
        sk = skel(k)
        if len(sk.replace("*", "")) >= 3:
            play_skel.setdefault((hub, sk), set()).add(pid)

    members = collections.defaultdict(list)
    for r in eds:
        members[uf.find(r["_node"])].append(r)
    all_nodes = collections.defaultdict(set)
    for n in list(uf.p):
        all_nodes[uf.find(n)].add(n)
    works = []
    for wi, (root, rs) in enumerate(sorted(members.items(),
                                           key=lambda kv: kv[1][0]["edition_id"]), 1):
        wid = f"WK-{wi:05d}"
        play_ids = sorted({play_of[n] for n in all_nodes[root] if n in play_of})
        play_basis = "exact" if play_ids else ""
        if not play_ids:
            hits = set()
            for hub, k in all_nodes[root]:
                got = play_skel.get((hub, skel(k)), set())
                if len(got) == 1:
                    hits |= got
            play_ids, play_basis = sorted(hits), ("skeleton" if hits else "")
        titles = collections.Counter(r["title"] for r in rs if r["title"])
        alts = sorted({k for _, k in all_nodes[root]} - {pc.norm_yiddish(t) for t in titles})
        for r in rs:
            r["work_id"] = wid
            r["plays_db_id"] = "|".join(play_ids)
        kinds = collections.Counter(r["work_kind"] for r in rs)
        works.append({
            "work_id": wid,
            "title": titles.most_common(1)[0][0] if titles else "",
            "titles_printed": " | ".join(t for t, _ in titles.most_common()),
            "other_keys": " | ".join(alts),
            "work_kind": kinds.most_common(1)[0][0],
            "author_hub": rs[0]["author_hub"],
            "author_heading": rs[0]["author_hub_heading"] or rs[0].get("author_label") or rs[0]["author"],
            "author_qid": rs[0].get("author_qid", ""),
            "original_title": next((r["original_title"] for r in rs if r["original_title"]), ""),
            "original_author": next((r["original_author"] for r in rs if r["original_author"]), ""),
            "n_editions": len(rs),
            "n_entries": len({r["xml_id"] for r in rs}),
            "years": " ".join(sorted({re.sub(r"\D", "", r["year"])[:4] for r in rs} - {""})),
            "plays_db_id": "|".join(play_ids),
            "plays_db_match": play_basis,
            "lateiner_hurwitz": "yes" if play_ids else "",
        })

    # 3. orgs ----------------------------------------------------------------
    for r in eds:
        d, b = resolve_org(r["publisher"], r["place"], PUBLISHER_TYPES, live, oidx)
        r["publisher_db_id"], r["publisher_match"] = d, b
        # only a periodical's container is an organisation; a collection's is a
        # book title ("געזאַמלטע ווערק"), which no org record should claim
        if r["pub_kind"] == "periodical":
            d, b = resolve_org(r["container"], r["place"], PRESS_TYPES, live, oidx)
        else:
            d, b = "", ("n/a_collection" if r["container"] else "")
        r["container_db_id"], r["container_match"] = d, b
        r["container_galron_id"] = galron.get(d, "") if d else ""

    # write ------------------------------------------------------------------
    efields = ["edition_id", "work_id", "plays_db_id", "xml_id", "person_id",
               "volume", "heading", "title", "genre_phrase", "alt_titles",
               "work_kind", "author", "subject_role", "author_hub",
               "author_hub_heading", "author_qid", "author_label",
               "author_role", "author_match", "original_title",
               "original_author", "original_author_hub", "original_author_qid",
               "pub_kind", "container", "container_db_id",
               "container_match", "container_galron_id", "publisher",
               "publisher_db_id", "publisher_match", "place", "year",
               "date_detail", "edition_note", "dup_count", "confidence",
               "evidence"]
    for r in eds:
        for k in ("author_qid", "author_label", "author_role",
                  "original_author_hub", "original_author_qid"):
            r.setdefault(k, "")
    pc.write_tsv(HERE / f"editions_resolved_{a.tag}.tsv", eds, efields)
    pc.write_tsv(HERE / f"works_{a.tag}.tsv", works, list(works[0].keys()))

    # report -----------------------------------------------------------------
    pe = [r for r in eds if r["work_kind"] in PLAY_KINDS]
    pw = [w for w in works if w["work_kind"] in PLAY_KINDS]
    def pct(n, d): return f"{n} ({100 * n / max(d, 1):.0f}%)"
    print(f"records {len(recs)} -> editions {len(eds)} ({n_dup} duplicates merged)")
    print(f"works {len(works)}; play works {len(pw)}; multi-edition play works "
          f"{sum(w['n_editions'] > 1 for w in pw)}; seen in >1 entry "
          f"{sum(w['n_entries'] > 1 for w in pw)}; same_work groups used {n_groups_used}")
    print(f"play works linked to plays_db (L/H): {sum(bool(w['plays_db_id']) for w in pw)} "
          f"-> distinct play_ids {len({p for w in pw for p in w['plays_db_id'].split('|') if p})}")
    print(f"play editions: {len(pe)}")
    print("  author identified (hub or Wikidata):",
          pct(sum(bool(r['author_hub'] or r['author_qid']) for r in pe), len(pe)))
    print("  author hub:", pct(sum(bool(r['author_hub']) for r in pe), len(pe)),
          collections.Counter(r["author_match"] for r in pe).most_common())
    hp = [r for r in pe if r["publisher"]]
    print("  publisher resolved:", pct(sum(bool(r['publisher_db_id']) for r in hp), len(hp)),
          collections.Counter(r["publisher_match"].split(":")[0] for r in hp).most_common())
    hc = [r for r in pe if r["container"] and r["pub_kind"] == "periodical"]
    print("  periodical container resolved:", pct(sum(bool(r['container_db_id']) for r in hc), len(hc)),
          collections.Counter(r["container_match"].split(":")[0] for r in hc).most_common())
    print("  periodical with Galron row:",
          sum(bool(r["container_galron_id"]) for r in pe if r["pub_kind"] == "periodical"))


if __name__ == "__main__":
    main()
