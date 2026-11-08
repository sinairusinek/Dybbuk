"""Exact Lexicon references for a quotation: volume, printed columns (from the
page-number regions of the TEI), and the archive.org page of the Yiddish Book
Center scan (vols. 1-6; vol. 7 is not online). Used to build the questions doc.

    from lexicon_ref import ref; ref(person_id, quote)
"""
import re, sys, functools
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent))
import plays_common as pc
D = str(__import__('pathlib').Path(__file__).resolve().parents[1] / 'The Lexicon') + '/'
STRUCT = {1: ['Structured_volume_1III.xml'], 2: ['Structured_volume_2III.xml'], 3: ['Structured_Volume_3III.xml'],
          4: ['Structured_Volume_4III.xml'], 5: ['Structured_Volume5III.xml', 'Structured_Volume5HartIII.xml'],
          6: ['Structured_volume6III.xml', 'Structured_volume6LiliFeinmanIII.xml'], 7: ['Structured_volume7III.xml']}
RAW = {1: 'volume_1.xml', 2: 'volume_2.xml', 3: 'Volume_3.xml', 4: 'Volume_4.xml', 5: 'Volume5.xml', 6: 'volume6.xml', 7: 'volume7.xml'}
IA = {1: 'nybc201089', 2: 'nybc201090', 3: 'nybc201091', 4: 'nybc201092', 5: 'nybc201093', 6: 'nybc201094'}

@functools.lru_cache(None)
def vol(v):
    paras = []   # (page, normtext)
    cols = {}
    for f in STRUCT[v]:
        s = open(D + f, encoding='utf-8').read()
        for m in re.finditer(r'<fw facs="#facs_(\d+)_[^"]*" type="page-number">\s*(\d+)', s):
            cols.setdefault(int(m.group(1)), []).append(int(m.group(2)))
        for m in re.finditer(r'<(p|ab|fw)\b[^>]*facs="#facs_(\d+)[_"][^>]*>(.*?)</\1>', s, re.S):
            paras.append((int(m.group(2)), pc.norm_yiddish(re.sub(r'<[^>]+>', ' ', m.group(3)))))
    raw = open(D + RAW[v], encoding='utf-8').read()
    img = {int(a): b for a, b in re.findall(r"<facsimile xml:id='facs_(\d+)'>\s*<surface[^>]*>\s*<graphic url='([^']*)'", raw)}
    return paras, cols, img

def pair(cols, page):
    """The page's two printed column numbers. OCR sometimes misreads one;
    then infer from a neighbouring page (two columns per page)."""
    c = sorted(set(cols.get(page, [])))
    pr = [x for x in c if x + 1 in c]
    if pr:
        return [pr[0], pr[0] + 1]
    for d in (1, -1, 2, -2):
        n = sorted(set(cols.get(page - d, [])))
        npr = [x for x in n if x + 1 in n]
        if npr:
            a = npr[0] + 2 * d
            if not c or a in c or a + 1 in c:
                return [a, a + 1]
    return c[:1]

def ref(person_id, quote):
    v = int(person_id.split('-')[1])
    paras, cols, img = vol(v)
    q = pc.norm_yiddish(quote)
    page = None
    words = q.split()
    for L in (12, 8, 6, 4):
        for i in range(0, max(1, len(words) - L + 1), max(1, L // 2)):
            key = ' '.join(words[i:i + L])
            hits = [p for p, t in paras if key in t]
            if hits:
                page = hits[0]; break
        if page: break
    if page is None:
        page = int(re.search(r'facs_(\d+)', person_id).group(1))
    c = pair(cols, page)
    url = ''
    if v in IA:
        g = img.get(page, '')
        m = re.search(r'_p(\d+)\.', g)
        n = (int(m.group(1)) if m else page) - 1
        url = f'https://archive.org/details/{IA[v]}/page/n{n}/mode/1up'
    colstr = (f"col. {c[0]}" if len(c) == 1 else f"cols. {c[0]}–{c[-1]}") if c else f"scan p. {page}"
    return {'vol': v, 'page': page, 'cols': colstr, 'url': url}

if __name__ == '__main__':
    print(ref('P-1-facs_93_r_1', 'אין 1896 איז אויפֿגעפֿירט געוואָרן אין זיין איבערזעצונג דיומאָס'))
    print(ref('P-2-facs_90_tr_1744131428', 'קין, אָדער, ליבּע אונד ליידנשאַפט'))
    print(ref('P-3-facs_279_tr_1755674208', 'ביידע איבערזעצונגען, אָן דעם נאָמען פֿון איבערזעצער'))
