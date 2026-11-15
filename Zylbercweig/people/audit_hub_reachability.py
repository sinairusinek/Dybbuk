"""Audit: which people can mentions NOT reach through person_hub surfaces? (2026-11-15)

Mentions attach to a person hub only through that hub's validated surfaces
(person_hub_members.tsv, member_kind=surface). A hub with no surfaces is unreachable, and a
mention whose spelling is on no hub's list is "hubless".

Writes:
  hub_reachability_empty_surface_hubs.tsv
      every hub with NO surfaces, with the mentions_all rows whose mention_name or
      cluster_name exactly equals one of the hub's own names (heading, DB hebname without
      parentheses, entry name). This is a lower bound on the mentions the hub is missing.
  hub_reachability_reconciled_names.tsv
      the 'no_hub' rows of people_wikidata_qids.tsv, each put into one category:
        A_hub_exists_unreachable    person is in people_db AND has a hub, spelling not a surface
        B_in_db_no_hub              in people_db, but no hub carries that db_id
        C_surname_matches_a_hub     last token equals a token of some hub surface: review
        D_no_trace                  no DB row, no hub (mention-only person)
      Matching is exact after stripping Hebrew points (Yiddish) / diacritics (Latin labels).
Read-only on every input. Prints a summary.
"""
import csv, os, re, unicodedata, collections
csv.field_size_limit(10**9)
HERE = os.path.dirname(os.path.abspath(__file__))


def rd(fn):
    return list(csv.DictReader(open(os.path.join(HERE, fn), encoding='utf-8'), delimiter='\t'))


def heb(s):
    s = re.sub(r'\(.*?\)|\[.*?\]', '', s or '')
    return re.sub(r'[֑-ׇ]', '', unicodedata.normalize('NFKD', s)).replace('־', '-').strip(' ,')


def lat(s):
    s = unicodedata.normalize('NFKD', s or '')
    return re.sub(r'[^a-z ]', '', ''.join(c for c in s if not unicodedata.combining(c)).lower()).strip()


def wr(fn, rows):
    with open(os.path.join(HERE, fn), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter='\t', lineterminator='\n')
        w.writeheader(); w.writerows(rows)


hubs = rd('person_hub.tsv')
members = rd('person_hub_members.tsv')
db = {r['db_id']: r for r in rd('people_db.tsv')}
surf = collections.defaultdict(set)
labels = collections.defaultdict(set)
for m in members:
    if m['member_kind'] == 'surface':
        surf[m['hub_id']].add(m['member_id'])
    elif m['member_kind'] in ('db', 'entry'):
        labels[m['hub_id']].add(heb(m['label']))

# mention counts by exact (pointless) name
by_name = collections.Counter()
for r in csv.DictReader(open(os.path.join(HERE, 'mentions_all.tsv'), encoding='utf-8'), delimiter='\t'):
    for k in {heb(r['mention_name']), heb(r['cluster_name'])}:
        if k:
            by_name[k] += 1

# --- 1. hubs with no surfaces
empty = []
for h in hubs:
    if surf.get(h['hub_id']):
        continue
    dbids = [d for d in h['db_ids'].split('|') if d]
    names = {heb(h['canonical_heading'])} | labels[h['hub_id']]
    names |= {heb(db[d]['hebname']) for d in dbids if d in db}
    # headings are "surname, given" or "surname given": also try the reversed order
    for n in list(names):
        p = n.replace(',', ' ').split()
        if len(p) >= 2:
            names.add(' '.join(p[1:] + p[:1]))
    names.discard('')
    est = sum(by_name[n] for n in names)
    empty.append({'hub_id': h['hub_id'], 'canonical_heading': h['canonical_heading'], 'n_entries': h['n_entries'],
                  'db_ids': h['db_ids'], 'english': '|'.join(db[d]['english'] for d in dbids if d in db),
                  'est_missed_mentions_exact_name': est, 'names_tried': '|'.join(sorted(names))})
empty.sort(key=lambda r: -r['est_missed_mentions_exact_name'])
wr('hub_reachability_empty_surface_hubs.tsv', empty)

# --- 2. the reconciled 'no_hub' names
en, he = collections.defaultdict(set), collections.defaultdict(set)
for d, r in db.items():
    if r['english']:
        en[lat(r['english'])].add(d)
    if r['hebname']:
        he[heb(r['hebname'])].add(d)
hub_of_db = collections.defaultdict(set)
for h in hubs:
    for d in h['db_ids'].split('|'):
        if d:
            hub_of_db[d].add(h['hub_id'])
tok = collections.defaultdict(set)
for hid, ss in surf.items():
    for s in ss:
        for t in heb(s).split():
            tok[t].add(hid)
out = []
for r in rd('people_wikidata_qids.tsv'):
    if r['link_status'] != 'no_hub':
        continue
    ids = en.get(lat(r['wikidata_label']), set()) | he.get(heb(r['towd_name']), set())
    hb = set().union(*[hub_of_db[i] for i in ids]) if ids else set()
    last = (heb(r['towd_name']).split() or [''])[-1]
    if hb:
        cat, cand = 'A_hub_exists_unreachable', hb
    elif ids:
        cat, cand = 'B_in_db_no_hub', set()
    elif last in tok:
        cat, cand = 'C_surname_matches_a_hub', tok[last]
    else:
        cat, cand = 'D_no_trace', set()
    out.append({'category': cat, 'towd_name': r['towd_name'], 'wikidata_label': r['wikidata_label'], 'qid': r['qid'],
                'match_mode': r['match_mode'], 'n_mentions': r['n_mentions'], 'db_ids': '|'.join(sorted(ids)),
                'candidate_hub_ids': '|'.join(sorted(cand)[:12]) + ('|…' if len(cand) > 12 else '')})
out.sort(key=lambda r: (r['category'], -int(r['n_mentions'])))
wr('hub_reachability_reconciled_names.tsv', out)

print(f'hubs: {len(hubs)}; with NO surfaces: {len(empty)} '
      f'(of which with a lexicon entry: {sum(1 for e in empty if e["n_entries"] not in ("", "0"))}); '
      f'est. missed mentions (exact name, lower bound): {sum(e["est_missed_mentions_exact_name"] for e in empty)}')
c = collections.Counter(o['category'] for o in out)
m = collections.Counter()
for o in out:
    m[o['category']] += int(o['n_mentions'])
for k in sorted(c):
    print(f'  {k}: {c[k]} names, {m[k]} mentions')
