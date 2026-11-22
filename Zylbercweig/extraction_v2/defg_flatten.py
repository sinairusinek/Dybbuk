"""Flatten the D–G output (defg_full.jsonl) into TSV tables in defg/ (no LLM).

Every row keeps entry_id, part_id and verbatim evidence; mentions get a global key
`{part_id}:{local_id}` (local ids are valid only inside one part). Nothing is dropped:
off-vocabulary values are mapped to the vocabulary and the original goes into `flags`
(norm:<field>=<value>), so every repair is visible and reversible.

  python3.11 defg_flatten.py            # -> defg/*.tsv + defg/README.md counts
"""
import csv, json, re, sys, collections, unicodedata
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
import defg_job as dj

SRC = HERE / "defg_full.jsonl"
OUT = HERE / "defg"

VERB = {"born", "settled", "stay", "pass_through", "tour", "flight", "emigration", "return", "visit", "death",
        "deportation", "evacuation", "expulsion", "conscription", "unknown"}
VERB_MAP = {"work_engagement": "stay", "settlement": "settled", "imprisonment": "stay", "dissolution": "unknown",
            "planned": "unknown"}
REASON = {"work_engagement", "tour_or_guest_appearance", "family", "education", "emigration", "forced", "health",
          "other", "unstated"}
REASON_MAP = {"military_service": "forced", "conscription": "forced", "flight": "forced"}
ETYPE = {"performance", "premiere", "debut", "guest_appearance", "season", "benefit", "jubilee", "honor", "memorial",
         "founding", "dissolution", "congress", "election", "strike", "conflict", "marriage", "divorce",
         "birth_of_child", "arrest", "imprisonment", "military_service", "retirement", "illness", "death", "burial",
         "other"}
ETYPE_STATUS = {"planned": ("performance", "planned"), "cancelled": ("performance", "cancelled")}
ETYPE_MAP = {"tour": "guest_appearance", "birth": "other"}
STATUS = {"performed", "planned", "banned", "cancelled", "unstated"}
STATUS_MAP = {"unstaged": "planned", "forced": "unstated"}
FUNC = {"author", "translator", "adapter", "composer", "lyricist", "librettist", "director", "designer",
        "choreographer", "performer", "producer", "prompter", "conductor", "staged", "other"}
FUNC_MAP = {"playwright": "author", "costume_designer": "designer", "stage_manager": "director"}
KIND_SPELL = {"comedy drama": "comedy_drama", "comedy-drama": "comedy_drama", "tragi-comedy": "tragicomedy",
              "tragi_comedy": "tragicomedy", "tragedy_comedy": "tragicomedy", "vodevil": "vaudeville",
              "vodvil": "vaudeville", "dramatic poem": "dramatic_poem", "lebensbild": "lebnsbild",
              "קאָמעדיע": "comedy", "comic opera": "comic_opera", "operette": "operetta",
              "historical operetta": "historical_operetta", "musical burlesque": "musical_burlesque",
              "comedy-vaudeville": "comedy_vaudeville", "grotesque-comedy": "grotesque_comedy",
              "farce-comedy": "farce_comedy", "tragedy-comedy": "tragicomedy",
              "מוזיקאַלישער קאָמעדיע": "musical_comedy", "מוזיקאַלישע פֿאָלקסשפּיל": "musical_folk_play",
              "메לאָדראַמע": "melodrama",  # garbled script in one output
              "part_of": "other", "adaptation": "other", "fragment": "other"}  # last 3: not genres
# spelling only; genres stay as stated (decision 10). All other labels that came up were added
# to the vocabulary by Sinai 11-22 (comedy_drama and tragicomedy kept as merged spellings).
# Sinai 11-22: new work kinds; earlier parts wrote them as `other` -> recover from the words next to the title
NEW_KINDS = [("lebnsbild", re.compile("לעב[ע]?נס[-]?בילד")), ("dramatic_poem", re.compile("דראַמאַטיש[ע]? פּאָעמע")),
             ("dialogue", re.compile("דיאַלאָג")), ("shadow_play", re.compile("שאָטן[-]?(טעאַטער|שפּיל)"))]
RETIRE = re.compile(r"retir|left the stage|פֿאַרלאָזט די בינע|אָפּגעטראָטן פֿון דער בינע|אַוועק פֿון דער בינע", re.I)
ILL = re.compile(r"ill|paraly|sick|stroke|קראַנק|פּאַראַליז", re.I)
DATE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")


def as_dict(x):
    return next((y for y in x if isinstance(y, dict)), {}) if isinstance(x, list) else (x or {})


_SKEL = {}


def skel(t):
    """Letters only: no niqqud/rafe/dagesh (U+0591-U+05C7), quotes or punctuation; final forms kept."""
    t = unicodedata.normalize("NFD", t)
    t = re.sub(r"[\u0591-\u05C7]", "", t)
    return re.sub(r"[^\w]|_", "", t)


def verbatim(ev, text):
    """1 = exact substring(s); near = same letters once diacritics, quotes and punctuation are
    ignored (פֿ/פ, an added closing quote...); 0 = not in the text (paraphrase or wrong)."""
    if not ev:
        return ""
    parts = [p.strip() for p in re.split(r"\.\.\.|…", ev) if p.strip()]
    if parts and all(p in text for p in parts):
        return "1"
    k = id(text)
    if k not in _SKEL:
        _SKEL.clear(); _SKEL[k] = skel(text)
    return "near" if parts and all(skel(p) in _SKEL[k] for p in parts) else "0"


def date_flags(a, b):
    f = []
    for k, v in (("date_start", a), ("date_end", b)):
        if v and not DATE.match(str(v)):
            f.append(f"bad_{k}={v}")
        elif v and not (1700 <= int(str(v)[:4]) <= 1975):
            f.append(f"implausible_{k}={v}")
    if a and b and DATE.match(str(a)) and DATE.match(str(b)) and str(b) < str(a):
        f.append("end_before_start")
    return f


def main():
    ents = dj.entries()
    texts = {}
    for it in dj.items(sorted(ents), ents):
        texts[it["id"]] = it["text"]
    OUT.mkdir(exist_ok=True)
    T = collections.defaultdict(list)
    C = collections.Counter()

    for line in SRC.open(encoding="utf-8"):
        r = json.loads(line)
        if "error" in r:
            continue
        pid, eid, x = r["id"], r["entry_id"], as_dict(r["result"])
        text = texts.get(pid, "")
        M = {}  # local id -> (kind, surface)

        def key(v, kind_hint):
            """id string -> global key; an inline dict becomes a new mention."""
            if isinstance(v, dict):
                lid = f"inline{len(M)}"
                M[lid] = (kind_hint, v.get("surface") or v.get("name") or "")
                T["mentions"].append({"mkey": f"{pid}:{lid}", "entry_id": eid, "part_id": pid, "kind": kind_hint,
                                      "local_id": lid, "surface": M[lid][1], "full_form": v.get("full_form", ""),
                                      "is_subject": "", "attrs": json.dumps(v, ensure_ascii=False), "flags": "inline"})
                C["inline_mention"] += 1
                return f"{pid}:{lid}", M[lid][1], ""
            v = str(v or "")
            if not v:
                return "", "", ""
            if v not in M:
                C["undefined_id"] += 1
                return f"{pid}:{v}", "", "undefined_id"
            return f"{pid}:{v}", M[v][1], ""

        mens = x.get("mentions") or {}
        extra = {"works": x.get("works"), "sources": x.get("mentions_sources")}
        for kind, lst in list(mens.items()) + [(k, v) for k, v in extra.items() if v]:
            kind = {"persons": "person", "organizations": "org", "places": "place", "works": "work",
                    "sources": "source", "persons_extra": "person", "mentions_works": "work",
                    "mentions_sources": "source"}.get(kind, kind)
            for m in lst or []:
                if not isinstance(m, dict) or not m.get("id"):
                    continue
                M[str(m["id"])] = (kind, m.get("surface", ""))
        for kind, lst in list(mens.items()) + [(k, v) for k, v in extra.items() if v]:
            kind = {"persons": "person", "organizations": "org", "places": "place", "works": "work",
                    "sources": "source", "persons_extra": "person"}.get(kind, kind)
            for m in lst or []:
                if not isinstance(m, dict) or not m.get("id"):
                    continue
                flags, attrs = [], {k: v for k, v in m.items() if k not in ("id", "surface", "full_form", "is_subject")}
                if kind == "work":
                    wk = str(attrs.get("work_kind", "") or "")
                    if wk in KIND_SPELL:
                        flags.append(f"norm:work_kind={wk}"); wk = KIND_SPELL[wk]
                    if wk in ("", "other") and m.get("surface"):
                        i = text.find(m["surface"])
                        win = text[max(0, i - 60): i + len(m["surface"]) + 80] if i >= 0 else ""
                        for nk, rx in NEW_KINDS:
                            if rx.search(win):
                                flags.append(f"norm:work_kind={wk or 'blank'}->{nk}"); wk = nk; C["new_kind_" + nk] += 1
                                break
                    attrs["work_kind"] = wk
                if kind == "org" and attrs.get("named_after"):
                    attrs["named_after_key"] = f"{pid}:{attrs['named_after']}" if isinstance(attrs["named_after"], str) else ""
                T["mentions"].append({"mkey": f"{pid}:{m['id']}", "entry_id": eid, "part_id": pid, "kind": kind,
                                      "local_id": m["id"], "surface": m.get("surface", ""),
                                      "full_form": m.get("full_form", ""), "is_subject": "1" if m.get("is_subject") else "",
                                      "attrs": json.dumps(attrs, ensure_ascii=False) if attrs else "",
                                      "flags": " ".join(flags)})

        for e in x.get("events") or []:
            if isinstance(e, dict) and e.get("id"):
                M.setdefault(str(e["id"]), ("event", e.get("event_type", "")))

        for s in x.get("stations") or []:
            if not isinstance(s, dict):
                continue
            fl = []
            vc = str(s.get("verb_class", "") or "")
            if not vc:
                vc = "stay"; fl.append("norm:verb_class=missing"); C["verb_missing"] += 1
            elif vc not in VERB:
                fl.append(f"norm:verb_class={vc}"); vc = VERB_MAP.get(vc, "unknown")
            rs = str(s.get("reason", "") or "unstated")
            if rs not in REASON:
                fl.append(f"norm:reason={rs}"); rs = REASON_MAP.get(rs, "other")
            sk, ss, f1 = key(s.get("subject"), "person")
            lk, ls, f2 = key(s.get("place"), "place")
            ok_, os_, f3 = key(s.get("org"), "org")
            fl += [f for f in (f1, f2, f3) if f] + date_flags(s.get("date_start", ""), s.get("date_end", ""))
            T["stations"].append({"part_id": pid, "entry_id": eid, "seq": s.get("seq", ""), "subject_key": sk,
                                  "subject": ss, "place_key": lk, "place": ls, "org_key": ok_, "org": os_,
                                  "role": s.get("role", ""), "verb_class": vc,
                                  "date_start": s.get("date_start", ""), "date_end": s.get("date_end", ""),
                                  "date_certainty": s.get("date_certainty", s.get("date_certain", "")) or "none",
                                  "reason": rs, "reason_stated": "1" if s.get("reason_stated") else "0",
                                  "event_keys": " ".join(f"{pid}:{v}" for v in s.get("events") or [] if isinstance(v, str)),
                                  "in_quote": "1" if s.get("in_quote") else "", "evidence": s.get("evidence", ""),
                                  "verbatim": verbatim(s.get("evidence", ""), text), "flags": " ".join(fl)})

        for e in x.get("events") or []:
            if not isinstance(e, dict):
                continue
            fl, et, st = [], str(e.get("event_type", "") or "other"), str(e.get("status", "") or "performed")
            if et in ETYPE_STATUS:
                fl.append(f"norm:event_type={et}"); et, st = ETYPE_STATUS[et]
            elif et not in ETYPE:
                fl.append(f"norm:event_type={et}"); et = ETYPE_MAP.get(et, "other")
            if et == "other":
                txt = f"{e.get('event_text', '')} {e.get('evidence', '')}"
                if RETIRE.search(txt):
                    fl.append("norm:event_type=other->retirement"); et = "retirement"; C["new_retirement"] += 1
                elif ILL.search(str(e.get("event_text", ""))):
                    fl.append("norm:event_type=other->illness"); et = "illness"; C["new_illness"] += 1
            if st not in STATUS:
                fl.append(f"norm:status={st}"); st = STATUS_MAP.get(st, "unstated")
            ek = f"{pid}:{e.get('id', '')}"
            lk, ls, f1 = key(e.get("place"), "place")
            vk, vs, f2 = key(e.get("venue"), "org")
            wk, ws, f3 = key(e.get("work"), "work")
            fl += [f for f in (f1, f2, f3) if f] + date_flags(e.get("date_start", ""), e.get("date_end", ""))
            T["events"].append({"ekey": ek, "part_id": pid, "entry_id": eid, "event_type": et,
                                "event_text": e.get("event_text", ""), "status": st,
                                "date_start": e.get("date_start", ""), "date_end": e.get("date_end", ""),
                                "date_certainty": e.get("date_certainty", e.get("date_certain", "")) or "none",
                                "place_key": lk, "place": ls, "venue_key": vk, "venue": vs, "work_key": wk, "work": ws,
                                "language": e.get("language", ""), "in_quote": "1" if e.get("in_quote") else "",
                                "evidence": e.get("evidence", ""), "verbatim": verbatim(e.get("evidence", ""), text),
                                "flags": " ".join(fl)})
            for p in e.get("participants") or []:
                if isinstance(p, dict):
                    k_, s_, f_ = key(p.get("person"), "person")
                    T["event_participants"].append({"ekey": ek, "person_key": k_, "person": s_,
                                                    "role": p.get("role", ""), "character": p.get("character", ""),
                                                    "flags": f_})
            for o in e.get("orgs") or []:
                if isinstance(o, dict):
                    k_, s_, f_ = key(o.get("org"), "org")
                    T["event_orgs"].append({"ekey": ek, "org_key": k_, "org": s_, "org_role": o.get("org_role", ""),
                                            "flags": f_})

        for f in x.get("performance_facts") or []:
            if not isinstance(f, dict):
                continue
            fl, fn = [], str(f.get("function", "") or "other")
            ftext = f.get("function_text", "")
            if fn not in FUNC:
                fl.append(f"norm:function={fn}")
                ftext = ftext or fn
                fn = FUNC_MAP.get(fn, "other")
            st = str(f.get("status", "") or "performed")
            if st not in STATUS:
                fl.append(f"norm:status={st}"); st = STATUS_MAP.get(st, "unstated")
            pk, ps, f1 = key(f.get("person"), "person")
            wk, ws, f2 = key(f.get("work"), "work")
            ok_, os_, f3 = key(f.get("org"), "org")
            vk, vs, f4 = key(f.get("venue"), "org")
            lk, ls, f5 = key(f.get("place"), "place")
            if not wk and not f.get("character") and not ftext:
                fl.append("no_work_no_character")
            fl += [x_ for x_ in (f1, f2, f3, f4, f5) if x_] + date_flags(f.get("date_start", ""), f.get("date_end", ""))
            T["performance_facts"].append({
                "part_id": pid, "entry_id": eid, "person_key": pk, "person": ps, "function": fn,
                "function_text": ftext, "character": f.get("character", ""), "work_key": wk, "work": ws,
                "org_key": ok_, "org": os_, "venue_key": vk, "venue": vs, "place_key": lk, "place": ls,
                "date_start": f.get("date_start", ""), "date_end": f.get("date_end", ""),
                "language": f.get("language", ""), "status": st,
                "event_key": f"{pid}:{f['event']}" if isinstance(f.get("event"), str) and f.get("event") else "",
                "in_quote": "1" if f.get("in_quote") else "", "evidence": f.get("evidence", ""),
                "verbatim": verbatim(f.get("evidence", ""), text), "flags": " ".join(fl)})

        for w in x.get("work_relations") or []:
            if isinstance(w, dict):
                a, sa, f1 = key(w.get("from"), "work"); b, sb, f2 = key(w.get("to"), "work")
                T["work_relations"].append({"part_id": pid, "entry_id": eid, "from_key": a, "from": sa, "to_key": b,
                                            "to": sb, "type": w.get("type", ""), "evidence": w.get("evidence", ""),
                                            "verbatim": verbatim(w.get("evidence", ""), text),
                                            "flags": " ".join(x_ for x_ in (f1, f2) if x_)})
        for u_ in x.get("source_uses") or []:
            if isinstance(u_, dict):
                k_, s_, f_ = key(u_.get("source"), "source")
                T["source_uses"].append({"part_id": pid, "entry_id": eid, "source_key": k_, "source": s_,
                                         "use": u_.get("use", ""), "about": u_.get("about", ""),
                                         "evidence": u_.get("evidence", ""),
                                         "verbatim": verbatim(u_.get("evidence", ""), text), "flags": f_})
        for s in x.get("schema_issues") or []:
            if isinstance(s, dict):
                T["schema_issues"].append({"part_id": pid, "entry_id": eid, "issue": s.get("issue", ""),
                                           "question": s.get("question", ""), "suggested": s.get("suggested", ""),
                                           "evidence": s.get("evidence", "")})

    qf = {}
    qpath = HERE / "qa" / "entry_quality_flags.tsv"
    if qpath.exists():
        qf = {r["person_id"]: r["flag"] for r in csv.DictReader(qpath.open(encoding="utf-8"), delimiter="\t")}
    for rows in T.values():
        for r_ in rows:
            f_ = qf.get(str(r_.get("entry_id", "")).split("~")[0])
            if f_ and "flags" in r_:
                r_["flags"] = " ".join(x_ for x_ in (r_["flags"], f_) if x_)

    lines = ["# D–G flattened tables (defg_flatten.py)", "", f"Source: `defg_full.jsonl`. Keys: `{{part_id}}:{{local_id}}`.", ""]
    for name, rows in T.items():
        cols = list(rows[0]) if rows else []
        with (OUT / f"{name}.tsv").open("w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t", extrasaction="ignore")
            w.writeheader(); w.writerows(rows)
        vb = [r_["verbatim"] for r_ in rows if r_.get("verbatim") in ("0", "1", "near")]
        nf = sum(1 for r_ in rows if r_.get("flags"))
        msg = f"- `{name}.tsv`: {len(rows):,} rows" + (f"; evidence exact {vb.count('1') / len(vb):.1%}, near {vb.count('near') / len(vb):.1%}, not found {vb.count('0') / len(vb):.1%}" if vb else "") \
              + (f"; {nf:,} with flags" if nf else "")
        print(msg); lines.append(msg)
    lines += ["", "Repairs: " + ", ".join(f"{k} {v:,}" for k, v in sorted(C.items()))]
    print(lines[-1])
    (OUT / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
