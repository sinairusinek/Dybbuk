"""Extract first N items of lp_review_queue into JSON for a standalone review page.
Ports the evidence logic from Zylbercweig/zalmen/views/lp_review.py.
BLIND: never reads checked_* / resolver_* columns, so controls stay hidden.
"""
import csv, json, pathlib, re, sys, unicodedata

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
BASE = pathlib.Path("/Users/sinairusinek/Documents/GitHub/Dybbuk/Zylbercweig")
PEOPLE = BASE / "people"
ER = BASE / "datasets" / "surname_er"
OUT = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else pathlib.Path("items.json")
csv.field_size_limit(sys.maxsize)

BLIND = {c for c in ("checked_verdict","checked_hub_id","checked_alternatives",
                     "checked_confidence","checked_how","resolver_verdict","resolver_hub_id",
                     "surname_prior_hub_id","surname_prior_alt_hub_ids","flag")}

def read(p):
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))

queue = read(PEOPLE / "lp_review_queue.tsv")[:N]
want_mids = {q["mention_id"] for q in queue}
want_hubs = set()
for q in queue:
    want_hubs |= {c for c in (q.get("candidate_order") or "").split("|") if c}

er = {}
for r in read(ER / "er_mention_context.tsv"):
    if r["mention_id"] in want_mids:
        er[r["mention_id"]] = {k: v for k, v in r.items() if k not in BLIND}
for m in er.values():
    want_hubs.add(m["host_hub_id"])

prof = {r["hub_id"]: r for r in read(ER / "er_candidate_profiles.tsv") if r["hub_id"] in want_hubs}
hubs = {r["hub_id"]: r for r in read(PEOPLE / "person_hub.tsv") if r["hub_id"] in want_hubs}

# english names via hub members -> people_db
db = {r["db_id"]: r for r in read(PEOPLE / "people_db.tsv") if r.get("db_id")}
english = {}
for r in read(PEOPLE / "person_hub_members.tsv"):
    if r["member_kind"] == "db" and r["hub_id"] in want_hubs and r["member_id"] in db:
        english.setdefault(r["hub_id"], db[r["member_id"]].get("english", ""))

# entry texts only for the hubs we need
want_pids = set()
for hid in want_hubs:
    h = hubs.get(hid, {})
    want_pids |= {x for x in (h.get("entry_person_ids") or "").split("|") if x}
texts = {}
p = PEOPLE / "entry_texts.tsv"
if p.exists():
    with open(p, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            if r["person_id"] in want_pids:
                texts[r["person_id"]] = r["entry_text"]

# D-G facts
facts = {}
d = ER / "defg"
if (d / "er_mention_defg_links.tsv").exists():
    links = {}
    for r in read(d / "er_mention_defg_links.tsv"):
        if r["mention_id"] in want_mids:
            links.setdefault(r["mkey"], []).append(r["mention_id"])
    events = {r["ekey"]: r for r in read(d / "defg_events.tsv")}
    for r in read(d / "defg_event_participants.tsv"):
        for mid in links.get(r["person_key"], ()):
            e = events.get(r["ekey"], {})
            facts.setdefault(mid, []).append(" ".join(x for x in (
                e.get("event_type","") + ":", e.get("event_text","")[:140],
                f"(role: {r['role']})" if r.get("role") else "",
                e.get("place_en",""), e.get("date_start","")) if x))
    for r in read(d / "defg_performance_facts.tsv"):
        for mid in links.get(r["person_key"], ()):
            facts.setdefault(mid, []).append(" ".join(x for x in (
                r.get("function",""), f"as {r['character']}" if r.get("character") else "",
                f"in {r['work']}" if r.get("work") else "", r.get("org",""),
                r.get("venue",""), r.get("place_en",""), r.get("date_start","")) if x))

def year(s):
    m = re.search(r"(1[5-9]\d\d|20[0-2]\d)", s or "")
    return int(m.group(1)) if m else None

def orgs_of(p):
    out = {}
    for chunk in (p.get("orgs_detail") or "").split(" ;; "):
        if "=" in chunk:
            oid, rest = chunk.split("=", 1)
            out[oid.strip()] = rest.replace("@", "").strip()
    return out

def evidence(m, p):
    chips = []
    host_orgs = set(filter(None, (m.get("host_org_ids") or "").split("|")))
    po = orgs_of(p)
    shared = [po[o].split("[")[0].strip() for o in po if o in host_orgs]
    if shared:
        chips.append("🏛 shared with host: " + " · ".join(shared[:4]))
    if m.get("c_org_id") and m["c_org_id"] in po:
        chips.append("🎭 the organisation in the sentence is in this person's record")
    hs = set(filter(None, (m.get("host_active_settlements") or "").split("|")))
    ps = set(filter(None, (p.get("active_settlements") or "").split("|")))
    if hs & ps:
        chips.append("📍 shared places: " + " · ".join(sorted(hs & ps)[:4]))
    pb, hb = year(p.get("birth_year")), year(m.get("host_birth_year"))
    if pb and hb:
        gap = hb - pb
        chips.append(f"🕰 born {abs(gap)} y {'before' if gap > 0 else 'after'} the host"
                     if gap else "🕰 born the same year as the host")
    dt = year(m.get("rel_date_start")) or year(m.get("rel_date_end"))
    if dt and pb:
        chips.append(f"🎂 aged ~{dt - pb} at the relation date ({dt})")
    hq = set(re.findall(r"Q\d+", m.get("host_defg_places_en") or ""))
    shared_st = []
    for st_ in (p.get("defg_places_en") or "").split("|"):
        if set(re.findall(r"Q\d+", st_)) & hq:
            nm = re.sub(r"\s*\(.*?\)", "", st_.rsplit(":", 1)[0] if re.search(r":\d{4}$", st_) else st_).strip()
            if nm and nm not in shared_st:
                shared_st.append(nm)
    if shared_st:
        chips.append("🗺 life stations shared with host: " + " · ".join(shared_st[:5]))
    yy = re.findall(r"\d{4}", p.get("defg_years") or "")
    if dt and len(yy) == 2:
        inside = int(yy[0]) <= dt <= int(yy[1])
        chips.append(f"{'✅' if inside else '⚠'} relation date {dt} is {'within' if inside else 'outside'} "
                     f"this person's documented years {yy[0]}–{yy[1]}")
    pd_ = year(p.get("death_year"))
    if dt and pd_ and pd_ < dt - 1:
        chips.append(f"⚠ died {pd_}, before the relation date ({dt})")
    if m.get("gender") and p.get("gender") and m["gender"] != p["gender"]:
        chips.append(f"⚥ mention is {m['gender']}, candidate is {p['gender']}")
    return chips or ["no overlapping evidence in the structured data"]

def entry_text(hub, volume=""):
    pids = [x for x in (hub.get("entry_person_ids") or "").split("|") if x]
    pids.sort(key=lambda x: not x.startswith(f"P-{volume}-"))
    return next((texts[x] for x in pids if texts.get(x)), "")

CAP = 9000
items = []
for q in queue:
    m = er.get(q["mention_id"])
    if not m:
        continue
    host = hubs.get(m["host_hub_id"], {})
    cands = [c for c in (q.get("candidate_order") or "").split("|") if c]
    hb = host.get("birth_date") or m.get("host_birth_year") or "?"
    stations = [re.sub(r"\s*\(.*?\)", "", x) for x in (m.get("host_defg_places_en") or "").split("|")
                if x and not x.startswith("?")]
    it = {
        "item": q["item"], "mention_id": q["mention_id"], "name": m["mention_name"],
        "sentence": m["relation_sentence"], "host_hub_id": m["host_hub_id"],
        "relation": (m.get("relation_category","") + " / " + m.get("relation_type","")).strip(" /"),
        "described": m.get("person_description",""),
        "where": m.get("rel_settlement") or m.get("rel_country") or "",
        "when": (m.get("rel_date_start","") + ("–" + m["rel_date_end"] if m.get("rel_date_end") else "")) if m.get("rel_date_start") else "",
        "sent_org": m.get("c_org_name",""), "gender": m.get("gender",""),
        "host_heading": host.get("canonical_heading") or m.get("host_heading",""),
        "host_dates": f"{hb} – {host.get('death_date','') or '?'}",
        "host_stations": stations[:14],
        "expansion": m.get("in_entry_full_form",""),
        "expansion_status": m.get("in_entry_status",""),
        "did": facts.get(q["mention_id"], [])[:8],
        "host_entry": entry_text(host, m.get("volume",""))[:CAP],
        "cands": [],
    }
    for hid in cands:
        p, h = prof.get(hid, {}), hubs.get(hid, {})
        stn = [re.sub(r"\s*\(.*?\)", "", x) for x in (p.get("defg_places_en") or "").split("|")
               if x and not x.startswith("?")]
        it["cands"].append({
            "hub_id": hid,
            "heading": p.get("heading") or h.get("canonical_heading") or "?",
            "english": english.get(hid, ""),
            "dates": f"{p.get('birth_year') or h.get('birth_date') or '?'} – {p.get('death_year') or h.get('death_date') or '?'}",
            "birth_place": p.get("birth_place",""),
            "roles": p.get("roles_db",""),
            "chips": evidence(m, p),
            "stations": stn[:12], "years": p.get("defg_years",""),
            "works": [w for w in (p.get("defg_works") or "").split(" ;; ") if w][:40],
            "orgs": list(orgs_of(p).values()),
            "entry": entry_text(h)[:CAP],
        })
    items.append(it)

OUT.write_text(json.dumps(items, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
print(f"{len(items)} items -> {OUT} ({OUT.stat().st_size/1e6:.2f} MB)")
print("candidates per item:", sorted({len(i['cands']) for i in items}))
print("items with host entry text:", sum(1 for i in items if i["host_entry"]))
