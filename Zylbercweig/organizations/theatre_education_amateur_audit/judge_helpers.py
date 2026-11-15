import re,unicodedata
CUE=re.compile(r'ליבהאב|אמאטאר|דראמ|סטודי|טעאטער-?(קרייז|סעקציע|גרופע|שול|קורס)|אויפגעפירט')
def sk(s):
  s=unicodedata.normalize('NFKD',s or ''); return ''.join(ch for ch in s if not unicodedata.combining(ch))
def order(ms): return sorted(ms,key=lambda m: not CUE.search(sk(m['sent'])))
