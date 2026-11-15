"""Export the 2024-25 pilot-era OpenRefine projects into Zylbercweig/pilot_2024/.

Read-only on the OpenRefine workspace (~/Library/Application Support/OpenRefine).
One TSV per project, written exactly as the project's rows stand (OpenRefine display
column order), plus:
  <col>__recon   Wikidata/recon match as  QID|label|judgment   (only for reconciled columns)
  __or_starred / __or_flagged   the row marks set in OpenRefine (1/0)
Files are written COMPLETE (nothing blanked). Any file carrying Steven Lasky's English
translations (museumoffamilyhistory.com) goes under private/ (git-ignored: the repo is
public and that text is not ours to republish); every other file is public.
Lasky text detection: seed corpus = every full-text column (named Full entry / entry /
text_chunk / description / Entry start, or whose header is itself a promoted first-row
bio); a cell (>=40 chars) opening with the same 40 chars as a seed text also counts.
manifest.tsv records each file's visibility and its Lasky-cell count.

usage:  python3 export_from_openrefine.py          # (re)writes all exports + manifest.tsv
"""
import csv, json, os, re, zipfile

WS = os.path.expanduser('~/Library/Application Support/OpenRefine')
HERE = os.path.dirname(os.path.abspath(__file__))
SEED_NAMES = {'full entry', 'entry', 'text_chunk', 'description', 'entry start'}  # all Lasky-derived here
PREFIX = 40
PRIVATE = 'private'  # git-ignored



def load_project(pid):
    """Return (header, rows, meta) for an OpenRefine project, header in display order."""
    d = os.path.join(WS, pid + '.project')
    meta = json.load(open(os.path.join(d, 'metadata.json')))
    with zipfile.ZipFile(os.path.join(d, 'data.zip')) as z:
        lines = z.read('data.txt').decode('utf-8').split('\n')
        pool = {}
        if 'pool.txt' in z.namelist():
            for pl in z.read('pool.txt').decode('utf-8').split('\n'):
                if pl.startswith('{'):
                    o = json.loads(pl)
                    pool[str(o.get('id'))] = o
    cols, raw, mode = [], [], None
    for ln in lines:
        if ln.startswith('columnModel='):
            mode = 'cols'; continue
        if ln.startswith('history='):
            mode = 'hist'; continue
        if ln.startswith('rowCount='):
            mode = 'rows'; continue
        if not ln.startswith('{'):
            if mode == 'cols' and ln.startswith('columnGroupCount'):
                mode = None
            continue
        if mode == 'cols':
            cols.append(json.loads(ln))
        elif mode == 'rows':
            raw.append(json.loads(ln))

    def cell(row, ci):
        cells = row.get('cells', [])
        c = cells[ci] if ci < len(cells) else None
        if not c:
            return '', ''
        v = c.get('v', '')
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        v = '' if v is None else (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v))
        r = c.get('r')
        if isinstance(r, (str, int)):
            r = pool.get(str(r), {})
        rec = ''
        if r:
            m = r.get('m')
            if m:
                rec = f"{m.get('id', '')}|{m.get('name', '')}|{r.get('j', '')}"
        return v, rec

    rec_cols = [c for c in cols if any(cell(r, c['cellIndex'])[1] for r in raw)]
    header = [c['name'] for c in cols] + [c['name'] + '__recon' for c in rec_cols] + ['__or_starred', '__or_flagged']
    rows = []
    for r in raw:
        vals = [cell(r, c['cellIndex'])[0] for c in cols]
        recs = [cell(r, c['cellIndex'])[1] for c in rec_cols]
        rows.append(vals + recs + [str(int(bool(r.get('starred')))), str(int(bool(r.get('flagged'))))])
    return header, rows, meta


def slug(s):
    return re.sub(r'[^A-Za-z0-9]+', '_', s).strip('_')[:70]


def norm_prefix(v):
    return re.sub(r'\s+', '', v)[:PREFIX].lower()


def is_seed(h):
    h0 = h.strip()
    return h0.lower() in SEED_NAMES or (len(h0) > 60 and re.search(r'\b[Bb]orn\b', h0) is not None)


def main():
    from projects import PROJECTS as LIST
    loaded = [(pid, folder) + load_project(pid) for pid, folder in LIST]
    seed = set()
    for _, _, header, rows, _ in loaded:
        for j, h in enumerate(header):
            if is_seed(h):
                seed.update(norm_prefix(r[j]) for r in rows if len(r[j].strip()) >= PREFIX)
    man = []
    for pid, folder, header, rows, meta in loaded:
        # count cells carrying Lasky's text; any such file goes to the git-ignored private/ tree
        lasky = sum(1 for r in rows for j, h in enumerate(header)
                    if r[j].strip() and (is_seed(h) or (len(r[j].strip()) >= PREFIX and norm_prefix(r[j]) in seed)))
        sub = os.path.join(PRIVATE, folder) if lasky else folder
        name = meta.get('name', '')
        fn = f"{meta.get('modified', '')[:10]}_{slug(name)}_{pid}.tsv"
        os.makedirs(os.path.join(HERE, sub), exist_ok=True)
        with open(os.path.join(HERE, sub, fn), 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f, delimiter='\t', lineterminator='\n')
            w.writerow(header)
            for r in rows:
                w.writerow([v.replace('\r', ' ') for v in r])
        man.append({'visibility': 'private' if lasky else 'public', 'path': f'{sub}/{fn}',
                    'openrefine_project_id': pid, 'openrefine_name': name,
                    'created': meta.get('created', '')[:10], 'modified': meta.get('modified', '')[:10],
                    'rows': len(rows), 'columns': len(header),
                    'starred': sum(r[-2] == '1' for r in rows), 'flagged': sum(r[-1] == '1' for r in rows),
                    'lasky_text_cells': lasky})
        print(f'{sub}/{fn}: {len(rows)} rows, {len(header)} cols, lasky cells {lasky}')
    with open(os.path.join(HERE, 'manifest.tsv'), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(man[0]), delimiter='\t', lineterminator='\n')
        w.writeheader(); w.writerows(man)


if __name__ == '__main__':
    main()
