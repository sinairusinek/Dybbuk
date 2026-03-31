import csv, sys
csv.field_size_limit(sys.maxsize)
with open('Zylbercweig_extraction/organizations/organizations_classified.tsv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    nt_col = 'name_type'
    conf_col = 'confidence'
    title_col = '_ - organizations - _ - title'
    desc_col = None
    for h in reader.fieldnames:
        if 'descriptive_name' in h:
            desc_col = h
            break
    print('desc_col:', desc_col)
    dt_with_title = 0
    dt_high_with_title = 0
    dt_med_with_title = 0
    blank_nt = 0
    for r in reader:
        nt = r[nt_col].strip()
        cf = r[conf_col].strip()
        title = r[title_col].strip()
        has_title = title not in ('', 'NA', 'N/A', 'na', 'null', 'NULL', '-', '--', '_')
        if nt == 'descriptive_term' and has_title:
            dt_with_title += 1
            if cf == 'high': dt_high_with_title += 1
            if cf == 'medium': dt_med_with_title += 1
        if nt == '':
            blank_nt += 1
    print(f'descriptive_term with title: total={dt_with_title}, high={dt_high_with_title}, medium={dt_med_with_title}')
    print(f'blank name_type: {blank_nt}')
