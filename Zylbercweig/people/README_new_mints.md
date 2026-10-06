# `new_mints_evidence.tsv` — what the columns mean

Evidence for people minted into `people_db.tsv` by the YiDraCor entity-graph
work. One row per minted person; the full citations live here because they are
richer than any single people_db field.

## `attested_in_leksikon` and the `probably_not_zylbercweig` flag

These two must agree. The rule, settled by Sinai 2026-10-06:

| `attested_in_leksikon` | means | `probably_not_zylbercweig` |
| :-- | :-- | :-- |
| `no` | the Leksikon does not attest this person **at all** | `1` |
| `mentions_only` | mentioned in another entry's text, but given **no heading of their own** | *(empty)* |
| `entry` | has their own Leksikon entry | *(empty)* |

**A mention counts as attestation.** The flag marks people the Leksikon does
not know, not people who lack an entry — so Minkowski (3822) and the two
Lianskys (3827, 3828), all mentioned but unheaded, are **unflagged**.

The flag is shared with the 60 NLI-authority rows that predate this work, so
`probably_not_zylbercweig = 1` answers one question across the whole table:
*which people are in our database but not in Zylbercweig?* Currently 64 rows.

A consistency check, worth running after any mint:

```python
import csv; csv.field_size_limit(10**7)
ev = {r['db_id']: r for r in csv.DictReader(open('new_mints_evidence.tsv'), delimiter='\t')}
for r in csv.DictReader(open('people_db.tsv'), delimiter='\t'):
    e = ev.get(r['db_id'])
    if not e: continue
    flag = (r.get('probably_not_zylbercweig') or '').strip()
    assert (e['attested_in_leksikon'] == 'no') == (flag == '1'), r['db_id']
```

## Other columns

`leksikon_evidence` quotes what the Leksikon actually says, so a later reader
can judge the attestation without re-searching. `primary_attestation` is the
source that justifies the mint — a newspaper ad, a playbill, an ownership
inscription, a scholarly chapter. `open_questions` records what is still
unresolved about that person; read it before building on the row.
