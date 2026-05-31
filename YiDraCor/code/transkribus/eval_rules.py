"""Evaluate rule-candidate precision against the RA-final stage corpus."""
from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path

DATE = "2026-05-31"
ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data" / "review"

# Each rule: (name, predicate(substring_no_nikud_no_parens) -> bool, expected_type)
def strip_parens(s: str) -> str:
    # remove ALL paren/bracket content's enclosing punctuation but keep words.
    # Per PI: rules should fire on the stripped-paren content of the direction.
    return re.sub(r"[()\[\].,:;׃׀־,־ \t]+", " ", s).strip()


def has_word(s: str, words) -> bool:
    toks = strip_parens(s).split()
    return any(w in toks for w in words)


def has_substr(s: str, subs) -> bool:
    return any(sub in s for sub in subs)


# Rules ordered roughly by specificity
RULES = [
    ("trailer:ende-fun-akt",       lambda s: re.search(r"\bענדע\b.*\bאקט\b", strip_parens(s)) is not None,      "trailer"),
    ("trailer:ende",               lambda s: re.search(r"^\s*\(?\s*ענדע", s) is not None,                        "trailer"),
    ("setting:pervandlung",        lambda s: has_substr(strip_parens(s), ["פערוואנדלונג", "פאָרהאַנג", "פארהאנג", "פאלט דער פארהאנג"]), "setting"),
    ("setting:tsimer",             lambda s: has_word(s, ["צימער"]),                                            "setting"),
    ("exit:ab",                    lambda s: has_word(s, ["אב", "אבּ", "אָבּ"]) ,                                "exit"),
    ("entrance:oyftrit",           lambda s: has_substr(strip_parens(s), ["אויפטריט", "אויפטרעטען", "אויפטרעטן", "אויפטרעטענד"]), "entrance"),
    ("entrance:kumt-arayn",        lambda s: re.search(r"קומ[טען].*אריין", strip_parens(s)) is not None,        "entrance"),
    ("entrance:falt-arayn",        lambda s: re.search(r"\bפאלט\b.*\bאריין\b", strip_parens(s)) is not None,    "entrance"),
    ("delivery:emotion",           lambda s: has_word(s, ["בייז", "שרייט", "ברוגז", "שפעטיש", "שטיל", "זיגנענד",
                                                            "פערקלעהרט", "פערקלערט", "ווייטיגדיג", "בעגייסטערט",
                                                            "פערשעמט", "לאכענדיק", "לאכענד", "אפארט", "פערזיכט",
                                                            "ערנסט", "פערשטערט"]),                              "delivery"),
]

NIKUD = re.compile(r"[֑-ׇ]")


def main():
    corpus = []  # full final-layer (noisy: includes untouched lines)
    with (REVIEW / f"ra_final_stage_corpus_{DATE}.tsv").open() as f:
        next(f)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2: continue
            sub, typ = parts[0], parts[1]
            corpus.append((sub, typ))
    # clean corpus: only stage tags that the RA explicitly added or retyped
    clean = []
    with (REVIEW / f"ra_stage_changes_{DATE}.tsv").open() as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for r in rdr:
            if r["kind"] in {"added", "type_changed"} and r["new_type"]:
                sub = NIKUD.sub("", r["new_substring"] or "")
                clean.append((sub, r["new_type"]))
    print(f"Full final-layer corpus: {len(corpus)} tags (noisy)")
    print(f"RA-touched corpus: {len(clean)} tags (decisive)")
    # Type distribution
    td = Counter(t for _, t in corpus)
    print("Type distribution:", td.most_common())
    print()
    out_rows = []
    for name, pred, exp in RULES:
        hits = [(s, t) for s, t in clean if pred(s)]
        if not hits:
            print(f"{name:30s}  expected={exp:9s}  NO HITS")
            out_rows.append({"rule": name, "expected_type": exp, "hits": 0, "correct": 0, "precision": "n/a", "wrong_types": ""})
            continue
        correct = sum(1 for _, t in hits if t == exp)
        wrong = Counter(t for _, t in hits if t != exp)
        prec = correct / len(hits)
        wrong_str = "; ".join(f"{t}:{c}" for t, c in wrong.most_common())
        print(f"{name:30s}  expected={exp:9s}  hits={len(hits):3d}  correct={correct:3d}  precision={prec:.0%}  wrong={wrong_str}")
        # show examples of wrong
        for s, t in hits:
            if t != exp:
                print(f"    WRONG ({t}): {s[:80]}")
        out_rows.append({"rule": name, "expected_type": exp, "hits": len(hits), "correct": correct,
                         "precision": f"{prec:.0%}", "wrong_types": wrong_str})
    with (REVIEW / f"ra_rule_precision_{DATE}.tsv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["rule", "expected_type", "hits", "correct", "precision", "wrong_types"], delimiter="\t")
        w.writeheader(); w.writerows(out_rows)
    print(f"\nWrote {REVIEW / f'ra_rule_precision_{DATE}.tsv'}")


if __name__ == "__main__":
    main()
