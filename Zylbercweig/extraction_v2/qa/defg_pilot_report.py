"""Per-entry D–G pilot report: Gemini vs the Claude reference, verbatim evidence, coverage of the last year."""
import json, collections, re, sys
sys.path.insert(0, '.'); import defg_job as d
E = d.entries()
def res(r):
    x = r['result']
    if isinstance(x, list): x = next((y for y in x if isinstance(y, dict)), {})
    return x
src = sys.argv[1] if len(sys.argv) > 1 else 'defg_pilot.jsonl'
recs = [json.loads(l) for l in open(src)]
by = collections.defaultdict(list)
for r in recs:
    if 'error' not in r: by[r['entry_id']].append(r)
K = ('stations', 'events', 'performance_facts', 'source_uses', 'schema_issues')
print('entry chars parts out tok/char | gem st/ev/pf/src/iss | ref st/ev/pf/src | evid-verbatim | last year: text / extracted')
T = collections.Counter()
for eid in open('qa/defg_pilot_ids.txt').read().split():
    rs = by[eid]; t = E[eid]['entry_text']; ch = len(t); out = sum(r['tok_out'] for r in rs)
    g = {k: sum(len(res(r).get(k, []) or []) for r in rs) for k in K}
    ev = [x.get('evidence', '') for r in rs for k in K[:4] for x in res(r).get(k, []) or [] if isinstance(x, dict)]
    vb = sum(all(p.strip() in t for p in e.split('...') if p.strip()) for e in ev if e)
    yrs = [int(y) for y in re.findall(r'\b(1[89]\d\d)\b', t)]
    gy = [int(y[:4]) for r in rs for k in ('stations', 'events') for x in res(r).get(k, []) or [] if isinstance(x, dict)
          for y in (x.get('date_start', ''), x.get('date_end', '')) if re.match(r'1[89]\d\d', str(y or ''))]
    try:
        c = json.load(open(f'qa/defg_claude_ref/out/{eid}__1.json')); ref = '/'.join(str(len(c.get(k, []))) for k in K[:4])
        for k in K[:4]: T['ref_' + k] += len(c.get(k, [])); T['gem_' + k] += g[k]
    except FileNotFoundError:
        ref = '-'
    T['ev'] += len(ev); T['vb'] += vb
    print(f"{E[eid]['heading'][:18]:18} {ch:6} {len(rs):2} {out:6} {out/ch:.2f} | {'/'.join(str(g[k]) for k in K):16} | {ref:12} | {vb}/{len(ev)} | {max(yrs) if yrs else None} / {max(gy) if gy else None}")
print({k: v for k, v in T.items()})
