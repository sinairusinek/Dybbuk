import csv,collections,re,unicodedata,json
csv.field_size_limit(10**9)
def sk(s):
  s=unicodedata.normalize('NFKD',s or ''); return ''.join(ch for ch in s if not unicodedata.combining(ch))
r=list(csv.DictReader(open('core_db.tsv'),delimiter='\t'))
CUE=re.compile(r'סטודי|studio|ליבהאב|אמאטאר|אמאטער|amateur|דראמ|דראָמ|drama|(טעאטער|בינע|אקטיאר)\S*[- ]?(שול|סקול|קורס|אקאדעמ|אינסטיטוט|קרייז|סעקציע|גרופע|ליבהאב)|קונסט-שול',re.I)
uni={}
for x in r:
  if x['deprecated'].strip() or x['merged_into'].strip(): continue
  t=x['org_type']
  if t in('Amateur','Theatre education') or CUE.search(sk(x['name'])+' '+sk(x['name_yiddish'])+' '+sk(x['name_variants'])):
    uni[x['db_id']]=x
print(len(uni), collections.Counter(x['org_type'] for x in uni.values()))
cl2db=collections.defaultdict(list)
for d,x in uni.items():
  for c in re.split(r'\s*\|\s*',x['linked_cluster_ids']):
    if c.strip(): cl2db[c.strip()].append(d)
mentions=collections.defaultdict(list)
P='_ - organizations - _ - '
with open('clustered.tsv',encoding='utf-8-sig') as f:
  for row in csv.DictReader(f,delimiter='\t'):
    cid=row['cluster_id'].strip()
    if cid in cl2db:
      m={'cluster':cid,'heading':row['_ - heading'],'xmlid':row['_ - xml:id'],'title':row[P+'title'],'ext_type':row[P+'org_type'],
         'rel':row[P+'relations - _ - category'],'specific':row[P+'relations - _ - specific_relation'],'role':row[P+'relations - _ - role_title'],
         'sent':row[P+'relations - _ - original_sentence'],'settlement':row[P+'locations - _ - settlement'],'venue':row[P+'locations - _ - Venue'],
         'date':row[P+'relations - _ - date_start - date'],'founded':row[P+'founding_date - date']}
      for d in cl2db[cid]: mentions[d].append(m)
out=[]
for d,x in uni.items():
  ms=mentions.get(d,[])
  # dedupe identical mentions
  seen=set();u=[]
  for m in ms:
    k=(m['heading'],m['sent'],m['title'])
    if k in seen: continue
    seen.add(k);u.append(m)
  out.append({'db_id':d,'name':x['name'] or x['name_yiddish'],'org_type':x['org_type'],'address':x['address'],'parent':x['parent_db_id'],'variants':x['name_variants'],'mentions':u})
json.dump(out,open('ae/universe.json','w'),ensure_ascii=False)
print(sum(len(o['mentions']) for o in out), sum(1 for o in out if not o['mentions']))
import statistics
print(sorted(len(o['mentions']) for o in out)[-15:])
