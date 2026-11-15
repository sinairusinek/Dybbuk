"""Bring the Wikidata reconciliation of mentioned people (done in OpenRefine) into the repo.

Source: OpenRefine project 1837611796716 "ZylbercweigMentionedPeople20260228", column `towd`
(the clustered mention name sent to Wikidata), reconciled 2026-05. Read-only on the workspace.

Output: people/people_wikidata_qids.tsv, one row per reconciled towd name:
  towd_name, qid, wikidata_label
  match_mode     manual          = matched by hand in OpenRefine (history: "Match item … containing …")
                 auto_reconcile  = OpenRefine auto-matched the top candidate; NOT human-checked
  n_mentions     rows in mentions_all.tsv whose towd / cluster_name / mention_name equals it
                 (towd was edited in OpenRefine after mentions_all was built)
  hub_ids        person_hub ids whose validated surfaces include the towd name or the
                 mention names of those rows ('|'-joined; >1 = ambiguous)
  db_ids         people_db ids of those hubs, plus PersonDBID set on the rows in OpenRefine
  link_status    one_hub | multi_hub | no_hub
                 no_hub = no hub lists this spelling as a validated surface. The person may
                 still have a hub (e.g. Ida Kaminska HUB-D3832 has no surfaces at all): 18 of
                 the 236 no_hub names were in that situation on 2026-11-15.
A sidecar, not a people_db column: people_db is rebuilt by scripts and has no QID field.
"""
import csv, json, os, re, sys, zipfile, collections
csv.field_size_limit(10**9)
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', 'pilot_2024'))
from export_from_openrefine import load_project, WS  # noqa: E402

PID = '1837611796716'
h, rows, meta = load_project(PID)
ix = {c: i for i, c in enumerate(h)}

# hand matches, from the project history
with zipfile.ZipFile(os.path.join(WS, PID + '.project', 'data.zip')) as z:
    hist = [json.loads(l) for l in z.read('data.txt').decode('utf-8').split('\n') if l.startswith('{"id":')]
manual = set()
for e in hist:
    m = re.search(r'^Match .* containing "(.*)" in column towd', e.get('description', ''))
    if m:
        manual.add(m.group(1))

qid_of = {}
or_dbids = collections.defaultdict(set)
for r in rows:
    rec = r[ix['towd__recon']]
    if not rec:
        continue
    qid, label, judg = rec.split('|', 2)
    t = r[ix['towd']]
    if t in qid_of and qid_of[t][0] != qid:
        sys.exit(f'conflicting QIDs for {t!r}')
    qid_of[t] = (qid, label, judg)
    if r[ix['PersonDBID']].strip():
        or_dbids[t].add(r[ix['PersonDBID']].strip())

# repo side
surf_hubs = collections.defaultdict(set)
for m in csv.DictReader(open(os.path.join(HERE, 'person_hub_members.tsv')), delimiter='\t'):
    if m['member_kind'] == 'surface':
        surf_hubs[m['member_id'].strip()].add(m['hub_id'])
hub_db = {r['hub_id']: [d for d in r['db_ids'].split('|') if d]
          for r in csv.DictReader(open(os.path.join(HERE, 'person_hub.tsv')), delimiter='\t')}
n_ment = collections.Counter()
ment_names = collections.defaultdict(set)
keys = {t.strip(): t for t in qid_of}
for r in csv.DictReader(open(os.path.join(HERE, 'mentions_all.tsv')), delimiter='\t'):
    # towd was edited in OpenRefine after mentions_all was built, so match on any name field
    hit = {keys[v.strip()] for v in (r['towd'], r['cluster_name'], r['mention_name']) if v.strip() in keys}
    for t in hit:
        n_ment[t] += 1
        ment_names[t].add(r['mention_name'].strip())

out = []
for t, (qid, label, judg) in sorted(qid_of.items(), key=lambda x: x[1][1]):
    hubs = set(surf_hubs.get(t.strip(), set()))
    for n in ment_names[t]:
        hubs |= surf_hubs.get(n, set())
    dbs = set(or_dbids[t])
    for hb in hubs:
        dbs.update(hub_db.get(hb, []))
    out.append({'towd_name': t, 'qid': qid, 'wikidata_label': label,
                'match_mode': 'manual' if t in manual else 'auto_reconcile',
                'n_mentions': n_ment[t], 'hub_ids': '|'.join(sorted(hubs)), 'db_ids': '|'.join(sorted(dbs, key=int)),
                'link_status': 'no_hub' if not hubs else ('one_hub' if len(hubs) == 1 else 'multi_hub'),
                'source': f'OpenRefine {PID} "{meta["name"]}" (modified {meta["modified"][:10]})'})

with open(os.path.join(HERE, 'people_wikidata_qids.tsv'), 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=list(out[0]), delimiter='\t', lineterminator='\n')
    w.writeheader(); w.writerows(out)
c = collections.Counter(o['link_status'] for o in out)
print(len(out), 'names,', len({o['qid'] for o in out}), 'QIDs;', dict(c),
      '; manual', sum(o['match_mode'] == 'manual' for o in out),
      '; not in mentions_all', sum(o['n_mentions'] == 0 for o in out))
