"""Export the pilot-era files that lived only in Google Drive into pilot_2024/.

Usage:
    python3 export_drive_files.py <download_dir>

<download_dir> holds the files as downloaded from Drive (e.g. with
`rclone backend copyid jeckedrive: <file_id> <download_dir>/`). The Drive ids are
listed in DRIVE_FILES below and in drive_2024_25/manifest.tsv.

Each xlsx sheet becomes one TSV. TSVs are copied as they are. Any file whose cells
contain Steven Lasky's English entry text goes under the git-ignored private/ folder
(same rule as export_from_openrefine.py): the repo is public, and his translations are
not ours to republish.
"""
import csv
import datetime
import os
import re
import sys

import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
PUBLIC = os.path.join(HERE, 'drive_2024_25')
PRIVATE = os.path.join(HERE, 'private', 'drive_2024_25')

# (drive_id, downloaded filename, subfolder, what it is)
DRIVE_FILES = [
    ('1kFyv7imIHWQo3S-v-m4ACUMKPOBFt9_y', 'Migrations2025-3-18.xlsx', 'migrations',
     'Hand-curated migrations (T-Migrants format): person, start/destination place ids + QIDs, fuzzy date, reason + reason validation, source, judgement'),
    ('126L2W1OVmuZNW8i_t5AxU7f-NgiEiL_y', 'Migrations2025-3-19.xlsx', 'migrations',
     'Next-day version of the migrations sheet'),
    ('1zZVxuLAHi1aD1AtQNIf9Y-gGIWjR57A-', 'long-articles-100-migration.tsv', 'migrations',
     'Migration stations from 11 long Lasky entries (301 rows): departure/destination, date, reason'),
    ('1kF8tdZaCsIpzw1WXiQv3WCb7WN33572n', 'long-articles-100-migration (1).tsv', 'migrations',
     'Same 11 entries, later version with settlement/country validation columns'),
    ('1PYjWy8KrUrP42xUMo28t6KpyY52LLYPg', 'People in Performance Events.xlsx', 'performance_events',
     'DiJeSt assertions report 2024-07-28: people in performance events with professional role and dramatic role, validated against DiJeSt and Zylbercweig'),
]

# Lasky text: entry-text columns, or English prose of some length.
LASKY_COLS = re.compile(r'^(full entry|entry start|entry|text_chunk|description)$', re.I)
PROSE = re.compile(r'[A-Za-z]{3,}(?: [A-Za-z,.()]{2,}){6,}')


def cell(v):
    if v is None:
        return ''
    if isinstance(v, float) and v.is_integer():
        return str(int(v))          # 2119.0 -> 2119 (never strip '.0' textually)
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    return str(v)


def slug(s):
    return re.sub(r'[^A-Za-z0-9]+', '_', s).strip('_')


def lasky_cells(header, rows):
    n = 0
    for r in rows:
        for j, v in enumerate(r):
            h = header[j] if j < len(header) else ''
            if v and (LASKY_COLS.match(h.strip()) or (len(v) > 150 and PROSE.search(v))):
                n += 1
    return n


def tables(path):
    if path.endswith('.xlsx'):
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        for ws in wb.worksheets:
            rows = [[cell(c) for c in r] for r in ws.iter_rows(values_only=True)]
            while rows and not any(rows[-1]):
                rows.pop()
            yield ws.title, rows
    else:
        csv.field_size_limit(10 ** 9)
        with open(path, newline='', encoding='utf-8') as f:
            yield None, list(csv.reader(f, delimiter='\t'))


def main(src):
    manifest = []
    for drive_id, fn, sub, what in DRIVE_FILES:
        path = os.path.join(src, fn)
        if not os.path.exists(path):
            sys.exit(f'missing download: {path}')
        base = slug(os.path.splitext(fn)[0])
        for sheet, rows in tables(path):
            header, body = (rows[0], rows[1:]) if rows else ([], [])
            lasky = lasky_cells(header, body)
            root = PRIVATE if lasky else PUBLIC
            out_fn = f'{base}__{slug(sheet)}.tsv' if sheet else f'{base}.tsv'
            os.makedirs(os.path.join(root, sub), exist_ok=True)
            with open(os.path.join(root, sub, out_fn), 'w', newline='', encoding='utf-8') as f:
                csv.writer(f, delimiter='\t', lineterminator='\n').writerows(rows)
            rel = os.path.relpath(os.path.join(root, sub, out_fn), HERE)
            manifest.append({'visibility': 'private' if lasky else 'public', 'path': rel,
                             'drive_id': drive_id, 'drive_name': fn, 'sheet': sheet or '',
                             'rows': len(body), 'columns': len(header),
                             'lasky_text_cells': lasky, 'what': what})
            print(f'{rel}: {len(body)} rows, {len(header)} cols, lasky cells {lasky}')
    os.makedirs(PUBLIC, exist_ok=True)
    with open(os.path.join(PUBLIC, 'manifest.tsv'), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(manifest[0]), delimiter='\t', lineterminator='\n')
        w.writeheader()
        w.writerows(manifest)


if __name__ == '__main__':
    main(sys.argv[1])
