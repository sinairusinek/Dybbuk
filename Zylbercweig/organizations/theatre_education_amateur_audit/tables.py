import json,csv,collections,re
V={json.loads(l)['db_id']:json.loads(l) for l in open('verdicts.jsonl')}
u={o['db_id']:o for o in json.load(open('universe2.json'))}
CANON=set("Theatre, Non-Yiddish Theatre, Traveling Company, Company on Tour, Amateur, Kleinkunst, Circus, Theatre education, Publisher, Printer, Printer/Publisher, Journals/ Newspapers, Media (Radio/ Film/TV), Library, Heritage Institution, Education, Musical organization, Theatre-related Society/ Union, Sports/Recreation, Fraternal order, Trade Union / Professional Association, Religious institutions/organizations, Jewish political bodies, Non-Jewish political bodies, Welfare/Aid organization, Health institutions, Business, Labour (factory/workshop), Military, Judenrat, Not an organization, OTHER - elaborate!".split(', '))
rows=[];mrows=[]
for d,j in V.items():
  o=u[d]; cur=o['org_type']; v=j['verdict']; pt=(j.get('proposed_type') or '').strip(); note=''
  if pt.startswith('OTHER') and pt!='OTHER - elaborate!': note=f"model suggested '{pt}'"; pt='OTHER - elaborate!'
  if re.search(r'קעמפּ|קעמפ|Camp ',o['name']) and pt=='Sports/Recreation':
    pt='Education'; note='camp: policy routes summer camps to Education (override of model)'
    if cur=='Education': v='KEEP'
  if d=='3735': v='KEEP'; pt='Amateur'; note='Maaty 2026-10-31 explicitly typed this Amateur (workers studio, no pupils in the text); human decision stands — model had said Theatre education'
  if v in('KEEP','OUT_OF_SCOPE') or not pt: pt=pt or cur
  if v=='KEEP' and pt!=cur: v='RETYPE'
  if v=='RETYPE' and pt==cur: v='KEEP'
  labs=j.get('labels',{}); cnt=collections.Counter(labs.values())
  if v in('HOST_AS_UNIT','UNIT_ON_HOST') and re.search(r'קלוב|קלאָבּ|קלאב|club|Palace|פאַראיין|פֿאַראיין|הויז',o['name']+' '+cur,re.I) and pt in('OTHER - elaborate!','Sports/Recreation','Trade Union / Professional Association'): note=(note+'; ' if note else '')+'host is a cultural/workers club — no canonical type fits (open question 4)'
  if v=='RETYPE': act=f'retype {cur} → {pt}'
  elif v=='HOST_AS_UNIT': act=f'retype host {cur} → {pt}; drama unit becomes a child record ({j.get("unit_type") or "Amateur"}, parent_db_id={d})'
  elif v=='UNIT_ON_HOST': act=f'mint child {j.get("unit_type") or "Amateur"} (parent_db_id={d}); move the drama-unit mentions to it (Mentions tab)'+('' if pt==cur else f'; host {cur} → {pt}')
  elif v=='SPLIT': act='split — see reason'
  elif v=='GENERIC': act='GENERIC (bare label, unrelated units)'
  else: act='no change'
  rows.append({'db_id':d,'name':o['name'],'current_type':cur,'verdict':v,'proposed_type':pt,'unit_type':j.get('unit_type',''),'studio_hybrid':'yes' if j.get('hybrid') else '',
    'confidence':j.get('confidence',''),'proposed_action':act,'n_mentions':len(o['mentions']),'T':cnt['T'],'A':cnt['A'],'R':cnt['R'],'H':cnt['H'],'G':cnt['G'],'X':cnt['X'],
    'reason':j.get('reason',''),'quotes':' ‖ '.join(j.get('quotes',[])),'quotes_grounded':'yes' if j.get('quotes_grounded') else 'NO','note':note,
    'scope':'host scan' if o.get('host_scan') else 'name/type','decision':'','reviewer_comment':''})
  ms=o['mentions']
  if o.get('host_scan'):
    from judge_helpers import order; ms=order(ms)
  for i,m in enumerate(ms[:30]):
    mrows.append({'db_id':d,'org_name':o['name'],'i':i,'label':labs.get(str(i),''),'entry':m['heading'],'xml_id':m['xmlid'],'cluster_id':m['cluster'],'title_as_written':m['title'],'relation':m['rel'],'role':m['role'],'settlement':m['settlement'],'date':m['date'],'sentence':m['sent']})
ORD={'RETYPE':0,'HOST_AS_UNIT':1,'UNIT_ON_HOST':2,'SPLIT':3,'GENERIC':4,'KEEP':5,'OUT_OF_SCOPE':6}
rows.sort(key=lambda r:(ORD[r['verdict']],r['current_type'],r['proposed_type'],int(r['db_id'])))
for fn,R in (('records.tsv',rows),('mentions.tsv',mrows)):
  with open(fn,'w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(R[0]),delimiter='\t'); w.writeheader(); w.writerows(R)
c=collections.Counter((r['verdict'],r['current_type'],r['proposed_type']) for r in rows if r['verdict'] not in('KEEP','OUT_OF_SCOPE'))
for k,n in sorted(c.items(),key=lambda x:(ORD[x[0][0]],-x[1])): print(n,k)
print(collections.Counter(r['verdict'] for r in rows), len(mrows))
print(collections.Counter(r['current_type'] for r in rows if r['verdict']=='KEEP'))
