"""Job A (v2 delta): in-entry full form for surname-only and initials mentions.

Input: people/mentions_all.tsv (already has every person mention, all 7 volumes).
Step 1, deterministic: match the bare surname (or initials + surname) against
  (a) the entry's subject (heading = "Surname Firstname [...]") and
  (b) multi-word mentions in the same entry whose last word is the same surname
      (allowing the Yiddish possessive/plural endings -ס, -ן, -ען, -ס׳).
  A unique match -> full_form. Several different full names -> ambiguous.
Step 2, Gemini, only for the rest (none / ambiguous): a text window plus every
  place the surname occurs in the entry, with the candidate full names.
Output: a_full_form.tsv, one row per targeted mention.

  python3.11 a_full_form.py --deterministic-only     # step 1 only
  GOOGLE_API_KEY=... python3.11 a_full_form.py        # both steps
"""
import argparse, csv, json, re, sys, unicodedata
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE.parent / "people" / "mentions_all.tsv"
OUT = HERE / "a_full_form.tsv"
LLM_OUT = HERE / "a_full_form_llm.jsonl"
csv.field_size_limit(10**9)

HONORIFICS = {"מר.", "מר", "מרס.", "מרס", "מיס", "מיסיס", "הער", "פֿרוי", "פרוי", "ד״ר", 'ד"ר', "דר.",
              "מאַדאַם", "מאַדאם", "פּראָפ.", "ר׳", "ר'", "רב", "חזן", "פֿרײַלין", "פרײלין"}
ENDINGS = ("ס׳", "ס'", "ס", "ען", "ן")


def key(s):
    """Skeleton for comparison: NFD, drop combining marks and punctuation."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("״", "").replace("׳", "").replace("'", "").replace('"', "")
    return unicodedata.normalize("NFC", s).strip(" .,;:()[]-–")


def forms(sur):
    k = key(sur)
    out = {k}
    for e in ENDINGS:
        e = key(e)
        if e and k.endswith(e) and len(k) - len(e) >= 3:
            out.add(k[: -len(e)])
    return out


def skel(s):
    """Looser key for merging spelling variants of ONE name: drop vowel letters
    that Yiddish spelling varies (א/ע) and collapse doubled letters."""
    k = key(s).replace("ײ", "יי").replace("װ", "וו").replace("ױ", "וי").replace("פֿ", "פ")
    k = k.replace("-", "").replace(" ", "")
    k = re.sub(r"(.)\1+", r"\1", k)
    return k


def cand_key(full):
    toks = full.split()
    return skel(toks[0]) + "|" + min(skel(x) for x in forms(toks[-1]))


def parse_heading(h):
    h = re.sub(r"\[.*?\]|\(.*?\)", "", h).strip()
    toks = h.replace(",", " ").split()
    if len(toks) < 2:
        return None, None
    return toks[0], " ".join(toks[1:] + toks[:1])  # surname, "First Surname"


def split_mention(name, issue):
    toks = [t for t in name.split() if t not in HONORIFICS]
    if not toks:
        return "", []
    sur = toks[-1]
    initials = [t for t in toks[:-1] if t.endswith(".") or len(key(t)) == 1]
    return sur, initials


def windows(text, sur, width=250, maxn=4):
    out, k = [], re.escape(sur.strip(" .,"))
    for m in re.finditer(k, text):
        a, b = max(0, m.start() - width), min(len(text), m.end() + width)
        out.append(text[a:b].replace("\n", " "))
        if len(out) >= maxn:
            break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deterministic-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()

    sys.path.insert(0, str(HERE))
    from bc_jobs import load  # fills entry_key / entry_text / entry_xml_id (host_* is first-row only)
    rows = load()
    by_host = defaultdict(list)
    for r in rows:
        by_host[r["entry_key"]].append(r)

    # index once per host: surname form -> {full-name key: full name}
    idx = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        otoks = [t for t in r["mention_name"].split() if t not in HONORIFICS]
        if len(otoks) < 2 or any(t.endswith(".") for t in otoks[:-1]):
            continue
        full = " ".join(otoks)
        for fm in forms(otoks[-1]):
            idx[r["entry_key"]][fm][cand_key(full)] = (full, key(otoks[0])[:1], r["mention_id"])

    # 5,931 rows flagged "single-word name" have an EMPTY mention_name (entry-level artifacts) -> not targets
    targets = [r for r in rows if r["extraction_issue"].startswith(("single-word", "initials"))
               and r["mention_name"].strip()]
    results, residue = {}, []
    for r in targets:
        sur, inits = split_mention(r["mention_name"], r["extraction_issue"])
        f = forms(sur) if sur else set()
        cands = {}
        hs, hfull = parse_heading(r["host_heading_fill"])
        if hs and f & forms(hs):
            cands[cand_key(hfull)] = (hfull, "subject")
        hidx = idx.get(r["entry_key"], {})
        for fm in f:
            for k, (full, ini, mid) in hidx.get(fm, {}).items():
                if mid == r["mention_id"]:
                    continue
                if inits and key(inits[0])[:1] != ini:
                    continue
                cands.setdefault(k, (full, "same_entry_mention"))
        base = {"mention_id": r["mention_id"], "host_xml_id": r["entry_xml_id"],
                "host_heading": r["host_heading_fill"], "mention_name": r["mention_name"],
                "extraction_issue": r["extraction_issue"], "relation_category": r["relation_category"],
                "relation_specific": r["relation_specific"]}
        only_subject = len(cands) == 1 and next(iter(cands.values()))[1] == "subject"
        if len(cands) == 1 and not only_subject:  # a bare surname in one's own entry is often a relative
            full, how = next(iter(cands.values()))
            results[r["mention_id"]] = {**base, "full_form": full, "method": f"det:{how}",
                                        "status": "resolved", "n_candidates": 1, "evidence": ""}
        else:
            status = "subject_candidate" if only_subject else ("ambiguous" if cands else "none")
            results[r["mention_id"]] = {**base, "full_form": "", "method": f"det:{status}",
                                        "status": status, "n_candidates": len(cands), "evidence": ""}
            residue.append({"id": r["mention_id"], "mention": r["mention_name"],
                            "entry_subject": r["host_heading_fill"],
                            "relation_sentence": r["relation_sentence"][:400],
                            "candidate_full_names": [v[0] for v in cands.values()],
                            "occurrences": windows(r["entry_text"], sur or r["mention_name"])})

    det = sum(1 for v in results.values() if v["status"] == "resolved")
    print(f"targets {len(targets)}; deterministic resolved {det}; residue {len(residue)} "
          f"(ambiguous {sum(1 for v in results.values() if v['status']=='ambiguous')})")

    if not args.deterministic_only:
        sys.path.insert(0, str(HERE))
        import gemini_batch
        prompt = (HERE / "prompts" / "a_full_form.txt").read_text(encoding="utf-8")
        work = residue[: args.limit] if args.limit else residue
        gemini_batch.run(work, prompt, LLM_OUT, batch_size=20, workers=args.workers)

    if LLM_OUT.exists():
        for line in LLM_OUT.open(encoding="utf-8"):
            rec = json.loads(line)
            if "error" in rec or rec["id"] not in results:
                continue
            v = results[rec["id"]]
            ff = (rec.get("full_form") or "").strip()
            fft = [t for t in ff.split() if t not in HONORIFICS]
            # accept only a full form that adds a real (non-initial) given name to the mention
            if rec.get("basis") == "not_in_entry":
                ff = ""
            # initials in the mention must agree with the full form's given names
            mt = [t for t in v["mention_name"].split() if t not in HONORIFICS]
            ini = [key(t)[:1] for t in mt[:-1] if t.endswith(".") or len(key(t)) == 1]
            if ff and (len(key(v["mention_name"])) < 2 or
                       (ini and [key(t)[:1] for t in ff.split()[:len(ini)]] != ini)):
                ff = ""
            if ff and (len(fft) < 2 or all(t.endswith(".") or len(key(t)) <= 1 for t in fft[:-1])
                       or skel(ff) == skel(v["mention_name"])):
                ff = ""
            v.update(full_form=ff, method="llm:" + (rec.get("basis") or ""),
                     status="resolved" if ff else "unresolved_in_entry",
                     evidence=(rec.get("evidence") or "")[:300])

    cols = ["mention_id", "host_xml_id", "host_heading", "mention_name", "extraction_issue",
            "full_form", "status", "method", "n_candidates", "evidence",
            "relation_category", "relation_specific"]
    with OUT.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for v in results.values():
            w.writerow(v)
    from collections import Counter
    print(Counter(v["method"] for v in results.values()).most_common())
    print("wrote", OUT)


if __name__ == "__main__":
    main()
