"""Build the RA handoff for speaker labels the resolver could not place.

`resolve_span_xmlids` settled 3,583 of 3,969 speaker spans. The 386 it left are
not mechanical failures — they are editorial questions: characters who speak but
are absent from the castList (אסיפ, 65 spans in Meshumed), unnamed functional
roles (קעלנער, דיענער, שפיאן), and the ער/זיא duet pronouns. Each needs a
person to decide, and each label needs deciding ONCE rather than per occurrence.

The output is a markdown questionnaire: one entry per label, ordered by how much
it matters (occurrences), carrying

  * where it appears — Transkribus deep links, per page
  * what it looks like in context — real lines, so the answer can be given
    without opening the page
  * tick-box candidates drawn from THAT PLAY's castList, ranked by similarity,
    plus `mint a new role` with a suggested xml:id, plus `not a speaker`
  * a comment line

Candidate ranking is deliberately loose here — looser than the resolver, which
must not guess. A candidate list is a prompt for a human, so a near-miss costs
nothing; a wrong automatic assignment costs a great deal.

  python3.11 -m annotation.build_speaker_questions --out ../docs/handoff.md
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import PAGE_NS, parse_custom, _NIKUD  # noqa: E402
from annotation.resolve_span_xmlids import bare, skeleton_key, Resolver  # noqa: E402
from annotation.review_links import page_url  # noqa: E402
from annotation.extract_cast_dict import _auto_xmlid  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
NS = f"{{{PAGE_NS}}}"
MAX_CANDIDATES = 5
# Below this similarity a cast role is not a candidate, it is noise. Offering
# `fedlers` and `iekil` as answers for קעלנער ("waiter") does not help someone
# decide — it invites a wrong tick. When nothing clears the bar the entry says
# so, which is itself the useful signal: no close match means a new role or a
# collective.
MIN_CANDIDATE_SCORE = 0.45
# Above this, the closest name is a genuine candidate rather than the accident
# of two Yiddish words sharing letters.
CONFIDENT_SCORE = 0.62
MAX_CONTEXT = 3
MAX_PAGES = 10

# Evidence found in the text while working through it, offered as a
# recommendation rather than applied silently — the RA still ticks the box.
# (play, label) -> (recommended xmlid or None, note in markdown)
NOTES = {
    ("Lateiner_Meshumed", "אסיפ"): (
        "iusf",
        "**Likely already answered by the text.** On p.26 this speaker says, "
        "unmasking: *`אסיפ. |(דעסמארקירט זיך) היער בין איך דיין יוסיף דיין זאהן.`* "
        "— \u201cHere I am, your Yosif, your son.\u201d Osip is the Russian name "
        "Yosef takes as the *meshumed*, so `אסיפ` and `יוסף` look like one "
        "character under two names. If that is right, tick `iusf` and these 62 "
        "spans are settled at once. The question is really whether you want "
        "them merged onto one role or kept as two."),
    ("MS_Emigration", "ער"): (
        None,
        "**Duet pronoun.** `ער` / `זיא` name whoever is singing in that "
        "particular scene, so one answer will not cover all of them. The "
        "pipeline has per-scene speaker overrides for exactly this "
        "(`speaker_overrides`); if you give the singers scene by scene — or "
        "just say which duet each page belongs to — they can be applied "
        "without retagging anything."),
    ("MS_Emigration", "זיא"): (
        None,
        "**Duet pronoun** — see `ער` above; the same per-scene answer settles "
        "both."),
    ("MS_BenHaDor", "ער"): (
        None,
        "**Duet pronoun** — same as in Emigration; needs the scene, not the "
        "label."),
}

PLAY_TITLES = {
    "MS_YoysefInEgipten": "Yoysef in Egipten",
    "MS_DiTsveyTnoim": "Di Tsvey Tnoim",
    "MS_TissaEssler": "Tissa-Essler",
    "MS_Emigration": "Emigration",
    "MS_BenHaDor": "Ben HaDor",
    "MS_BasKoyen": "Bas Koyen",
    "MS_KhurbnYerusholaim": "Khurbn Yerusholaim",
    "MS_YaakovEsav": "Yaakov-Esav",
    "Lateiner_Meshumed": "Meshumed",
}


def _line_text(tl) -> str:
    for te in tl.findall(f"{NS}TextEquiv"):
        u = te.find(f"{NS}Unicode")
        if u is not None:
            return u.text or ""
    return ""


def score(label: str, cast_bare: str) -> float:
    """How plausible is `cast_bare` as the role behind `label`?"""
    a, b = bare(label), bare(cast_bare)
    if not a or not b:
        return 0.0
    best = difflib.SequenceMatcher(None, a, b).ratio()
    # Also compare against each word of a multi-word cast form: a bare given
    # name should rank against `רבי שמעון בן לקיש` on `שמעון`, not on the whole.
    for w in b.split():
        best = max(best, difflib.SequenceMatcher(None, a, w).ratio())
    sa, sb = skeleton_key(a), skeleton_key(b)
    if sa and sb:
        best = max(best, 0.9 * difflib.SequenceMatcher(None, sa, sb).ratio())
    return best


def collect(only=None):
    """{play: {label: {"n":int, "pages":[...], "context":[...]}}}"""
    data = REPO / "data"
    folders = [p for p in sorted(data.glob("MS_*")) if (p / "page_annotated").is_dir()]
    folders += [data / "Lateiner_Meshumed"]
    if only:
        folders = [f for f in folders if f.name in set(only)]

    out: dict[str, dict] = {}
    for folder in folders:
        cast_path = folder / "cast_dict.json"
        cast = json.loads(cast_path.read_text(encoding="utf-8")) \
            if cast_path.exists() else {}
        resolver = Resolver(cast)
        labels: dict[str, dict] = defaultdict(
            lambda: {"n": 0, "pages": [], "context": [], "kind": "speaker",
                     "why": ""})
        for xf in sorted((folder / "page_annotated").glob("*.xml")):
            try:
                tree = etree.parse(str(xf))
            except etree.XMLSyntaxError:
                continue
            for tl in tree.getroot().iter(f"{NS}TextLine"):
                ents = parse_custom(tl.get("custom") or "")
                if not any(t in ("speaker", "role") and not a.get("xmlid")
                           for t, a in ents):
                    continue
                text = _line_text(tl)
                for tag, a in ents:
                    # `role` spans are castList entries and fail for their own
                    # reasons — usually because one span covers several names.
                    # They were in the TSV and must not be lost here.
                    if tag not in ("speaker", "role") or a.get("xmlid"):
                        continue
                    try:
                        off, ln = int(a["offset"]), int(a["length"])
                    except (KeyError, ValueError):
                        continue
                    key = bare(text[off:off + ln])
                    if not key:
                        continue
                    rec = labels[key]
                    rec["kind"] = tag
                    if not rec["why"]:
                        rec["why"] = resolver.resolve(text[off:off + ln])[1]
                    rec["n"] += 1
                    if xf.name not in rec["pages"]:
                        rec["pages"].append(xf.name)
                    line = re.sub(r"\s+", " ", text).strip()
                    if len(rec["context"]) < MAX_CONTEXT and len(line) > 12:
                        rec["context"].append(line[:110])
        if labels:
            out[folder.name] = labels
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--only", action="append")
    ap.add_argument("--min-occurrences", type=int, default=1)
    args = ap.parse_args()

    found = collect(args.only)
    L: list[str] = []
    A = L.append

    total = sum(r["n"] for p in found.values() for r in p.values())
    distinct = sum(len(p) for p in found.values())

    A("# Speaker labels needing a decision — manuscript track")
    A("")
    A("**For Noa and Judith. 2026-08-16.**")
    A("")
    A(f"The pipeline resolved 3,583 of 3,969 speaker labels automatically "
      f"(90%). The remaining **{total} spans across {distinct} labels** could "
      f"not be settled mechanically, and shouldn't be — each is an editorial "
      f"question rather than a spelling problem.")
    A("")
    A("Three kinds turn up:")
    A("")
    A("* **Characters who speak but are not in the castList.** `אסיפ` speaks "
      "65 times in Meshumed and appears nowhere in its cast list.")
    A("* **Unnamed functional roles** — `קעלנער` (waiter), `דיענער` (servant), "
      "`שפיאן` (spy). These may deserve their own entries, or may be "
      "collectives.")
    A("* **The duet pronouns** `ער` / `זיא`, which refer to different people "
      "scene by scene and need a per-scene answer.")
    A("* **Cast-list entries** (marked *castList `role` span*) rather than "
      "speech prefixes. These usually failed because one span covers several "
      "names, or because the span is clipped — Meshumed's `א` covers only the "
      "article of `א ריכטיר פֿון געהיימס געריכט`, a role the list already has.")
    A("")
    A("**How to answer:** tick one box per label. Every candidate is a real "
      "role from that play's own cast list, with its `xml:id` in code font. "
      "If none fits, tick *mint a new role* — a suggested id is given, change "
      "it if you prefer. The last boxes cover the cases where the tagging "
      "itself is wrong rather than the identification. The comment line is for "
      "anything else, including \"depends on the scene\".")
    A("")
    A("Each label is asked **once**, however many times it occurs. Page links "
      "go straight to Transkribus.")
    A("")
    A("### The ones that matter most")
    A("")
    A("| Play | Label | Spans | |")
    A("|---|---|---:|---|")
    big = sorted(((p, lab, r["n"]) for p, labs in found.items()
                  for lab, r in labs.items()), key=lambda t: -t[2])[:15]
    for p, lab, n in big:
        anchor = PLAY_TITLES.get(p, p)
        hint = NOTES.get((p, lab))
        flag = " *(suggestion below)*" if hint and hint[0] else (
            " *(needs a per-scene answer)*" if hint else "")
        A(f"| {anchor} | {lab} | {n} |{flag} |")
    A("")
    A(f"Those {sum(n for _, _, n in big)} spans are "
      f"{100 * sum(n for _, _, n in big) // max(total, 1)}% of the total. The "
      f"remaining labels occur only a handful of times each.")
    A("")
    A("---")
    A("")

    for play in sorted(found, key=lambda p: -sum(r["n"] for r in found[p].values())):
        labels = found[play]
        cast_path = REPO / "data" / play / "cast_dict.json"
        cast = json.loads(cast_path.read_text(encoding="utf-8")) if cast_path.exists() else {}
        roles = cast.get("roles", {})

        items = sorted(labels.items(), key=lambda kv: (-kv[1]["n"], kv[0]))
        items = [i for i in items if i[1]["n"] >= args.min_occurrences]
        if not items:
            continue
        nspans = sum(r["n"] for _, r in items)

        A(f"## {PLAY_TITLES.get(play, play)}")
        A("")
        A(f"*{len(items)} labels, {nspans} spans.* "
          f"Cast list has {len(roles)} roles: "
          + ", ".join(f"`{x}`" for x in list(roles)[:12])
          + (" …" if len(roles) > 12 else ""))
        A("")

        for label, rec in items:
            kind = rec.get("kind", "speaker")
            tagnote = "" if kind == "speaker" else "  *(castList `role` span)*"
            A(f"### {label} — {rec['n']} occurrence"
              f"{'s' if rec['n'] != 1 else ''}{tagnote}")
            A("")
            if kind == "role":
                A("This is an entry in the **cast list**, not a speech prefix. "
                  "It could not be given an `xml:id` — usually because one span "
                  "covers several names, or names someone the list does not "
                  "otherwise have. Should it be split, or is it one role?")
                A("")
            links = ", ".join(
                f"[p.{p.split('_')[0].lstrip('0') or '0'}]({page_url(play, p)})"
                for p in rec["pages"][:MAX_PAGES])
            more = "" if len(rec["pages"]) <= MAX_PAGES else \
                f" …and {len(rec['pages']) - MAX_PAGES} more"
            A(f"**Pages:** {links}{more}")
            A("")
            if rec["context"]:
                A("**In context:**")
                A("")
                for c in rec["context"]:
                    A(f"> {c}")
                    A(">")
                A("")

            note = NOTES.get((play, label))
            if note:
                A(note[1])
                A("")

            ranked = sorted(
                ((score(label, info.get("bare") or ""), xid, info.get("bare") or "")
                 for xid, info in roles.items()),
                reverse=True)[:MAX_CANDIDATES]
            A("**Which role is this?**")
            A("")
            rec_id = note[0] if note else None
            if rec_id and rec_id not in {x for _, x, _ in ranked}:
                info = roles.get(rec_id, {})
                ranked = [(1.0, rec_id, info.get("bare") or "")] + ranked[:-1]
            shown = [(sc, xid, b) for sc, xid, b in ranked
                     if sc >= MIN_CANDIDATE_SCORE or xid == rec_id]
            best = max((sc for sc, _, _ in shown), default=0.0)
            if not shown:
                A("*Nothing in this play's cast list resembles this label, so "
                  "it is most likely a role that was never listed, or a "
                  "collective.*")
                A("")
            elif best < CONFIDENT_SCORE and not rec_id:
                # Letter overlap alone will always rank something. Say plainly
                # that the list is proximity, not plausibility — קעלנער
                # ("waiter") scores against פעדלערס on shared letters, and
                # presenting that as a candidate without comment is misleading.
                A("*No close match — the names below are simply the nearest in "
                  "the cast list and may well be none of them. For a role like "
                  "this, `mint a new role` or `collective` is often the answer.*")
                A("")
            for sc, xid, b in shown:
                mark = "  ← **suggested**" if xid == rec_id else ""
                A(f"- [ ] `{xid}` — {b}{mark}")
            suggested = _auto_xmlid(label) or "new_role"
            A(f"- [ ] **Mint a new role** — suggested `xml:id`: `{suggested}` "
              f"(name as printed: {label})")
            if kind == "role":
                # The commonest role-span failure is not identification but a
                # clipped span: Meshumed's `א` covers only the article of
                # `א ריכטיר פֿון געהיימס געריכט`, a role the cast list already
                # has. Offer that as an answer rather than forcing a choice
                # between names.
                A("- [ ] **The span is mis-cut** — it covers the wrong text; "
                  "the role itself is fine")
                A("- [ ] **One span, several roles** — split it (say which "
                  "in the comment)")
                A("- [ ] **Not a role** — this span is mis-tagged")
            else:
                A("- [ ] **Collective / group**, no individual cast entry "
                  "(like `אלע`, `קאהר`)")
                A("- [ ] **Not a speaker** — this span is mis-tagged")
            A("")
            why = (rec.get("why") or "").strip()
            if why and not why.startswith("unmatched"):
                A(f"<sub>Resolver: {why}. Where it says *ambiguous*, that is "
                  f"the number of cast names it found equally plausible — it "
                  f"declined to choose rather than guess.</sub>")
                A("")
            A("**Comment:**")
            A("")
            A("---")
            A("")

    Path(args.out).write_text("\n".join(L), encoding="utf-8")
    print(f"wrote {args.out}: {distinct} labels, {total} spans, "
          f"{len(found)} plays")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
