import json,sys,os,re,unicodedata,concurrent.futures as cf
from google import genai
from google.genai import types
M="gemini-3.1-pro-preview"
SYS=open('prompt.txt').read()
u=json.load(open('universe2.json'))
done={}
if os.path.exists('verdicts.jsonl'):
  for l in open('verdicts.jsonl'):
    j=json.loads(l); done[j['db_id']]=j
client=genai.Client()
CUE=re.compile(r'ליבהאב|אמאטאר|דראמ|סטודי|טעאטער-?(קרייז|סעקציע|גרופע|שול|קורס)|אויפגעפירט')
def sk(s):
  s=unicodedata.normalize('NFKD',s or ''); return ''.join(ch for ch in s if not unicodedata.combining(ch))
def render(o):
  ms=o['mentions']
  if o.get('host_scan'):
    ms=sorted(ms,key=lambda m: not CUE.search(sk(m['sent'])))
  ms=ms[:30]
  L=[f"RECORD db_id={o['db_id']}  name: {o['name']}  current type: {o['org_type']}  address: {o['address']}  variants: {o['variants']}  parent_db_id: {o['parent']}",
     f"total linked mentions: {len(o['mentions'])} (showing {len(ms)})"]
  for i,m in enumerate(ms):
    L.append(f"[{i}] entry: {m['heading'][:60]} | title as written: {m['title']} | relation: {m['rel']} / {m['specific']} / role: {m['role']} | place: {m['settlement']} {m['venue']} | date: {m['date']} founded: {m['founded']}\n    sentence: {m['sent'][:600]}")
  if not ms: L.append("(no mentions linked — judge from the name alone, confidence low)")
  return "\n".join(L), ms
def one(o):
  txt,ms=render(o)
  for a in range(3):
    try:
      resp=client.models.generate_content(model=M,contents=txt+"\n\nReply with strict JSON only.",
        config=types.GenerateContentConfig(system_instruction=SYS,temperature=0.0,max_output_tokens=6000,
          response_mime_type="application/json",http_options=types.HttpOptions(timeout=180000)))
      j=json.loads(resp.text)
      if isinstance(j,list): j=j[0]
      # quote grounding
      blob=sk(" ".join(m['sent'] for m in ms)+" "+o['name'])
      j['quotes_grounded']=all(sk(q).strip(' .…"„“') in blob for q in j.get('quotes',[]) if q.strip())
      j['db_id']=o['db_id']; j['n_shown']=len(ms)
      return j
    except Exception as e:
      err=str(e)
  return {'db_id':o['db_id'],'error':err}
todo=[o for o in u if o['db_id'] not in done or 'error' in done[o['db_id']]]
print('todo',len(todo),flush=True)
with open('verdicts.jsonl','a') as f, cf.ThreadPoolExecutor(12) as ex:
  futs=[ex.submit(one,o) for o in todo]
  for i,fu in enumerate(cf.as_completed(futs)):
    j=fu.result(); f.write(json.dumps(j,ensure_ascii=False)+"\n"); f.flush()
    if i%25==0: print(i,flush=True)
