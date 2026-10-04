import re, unicodedata

def norm(s: str) -> str:
    """Normalise a Yiddish settlement string to a comparison key.

    Order matters: strip parentheticals and diacritics first, then fold
    orthographic variants, and only then remove the adjectival suffix -- doing
    the suffix first mangles short names (ווין -> וי).
    """
    s = unicodedata.normalize('NFKD', s or '')
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\([^)]*\)', '', s)                      # ...(עסטרייך)
    s = re.sub(r'[\s\-־,\.\(\)\'"״׳]', '', s)
    for a, b in (('ייִ','י'),('אָ','א'),('אַ','א'),('פֿ','פ'),('וּ','ו'),('בּ','ב'),
                 ('כּ','כ'),('פּ','פ'),('שׂ','ש'),('תּ','ת')):
        s = s.replace(a, b)
    s = s.translate(str.maketrans('ךםןףץ', 'כמנפצ'))     # final forms -> base
    s = re.sub(r'(ער|ען)$', '', s)                       # adjectival ending
    for a, b in (('וו','ו'),('יי','י'),('אוי','וי')):    # digraphs AFTER suffix
        s = s.replace(a, b)
    return s


def same_place(a: str, b: str) -> bool:
    """True when two spellings plausibly denote one settlement."""
    x, y = norm(a), norm(b)
    if not x or not y:
        return False
    if x == y:
        return True
    # one spelling truncates or extends the other (לעמבער / לעמבערג,
    # סטאניסלאו / סטאניסלאואו) -- require a solid shared prefix
    shorter, longer = (x, y) if len(x) <= len(y) else (y, x)
    if len(shorter) >= 3 and longer.startswith(shorter):
        return True                      # וינ / וי  (ווינער / ווין)
    if len(shorter) >= 4 and longer.startswith(shorter[:len(shorter) - 2]):
        return True                      # סטאניסלאו / סטאניסלאואו
    # one edit apart: OCR-ish variants like ניוארק / ניויארק
    if abs(len(x) - len(y)) <= 1 and len(shorter) >= 5:
        i = j = diff = 0
        while i < len(x) and j < len(y):
            if x[i] == y[j]:
                i += 1; j += 1; continue
            diff += 1
            if diff > 1:
                return False
            if len(x) > len(y):   i += 1
            elif len(y) > len(x): j += 1
            else:                 i += 1; j += 1
        return diff + (len(x) - i) + (len(y) - j) <= 1
    return False


def cluster_places(places):
    """Group spellings into {canonical: [all spellings]} by same_place."""
    groups = {}
    for p in places:
        for key in groups:
            if same_place(key, p):
                groups[key].append(p)
                break
        else:
            groups[p] = [p]
    return groups
