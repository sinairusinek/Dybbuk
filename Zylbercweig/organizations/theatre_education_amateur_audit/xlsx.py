import csv,collections
from openpyxl import Workbook
from openpyxl.styles import Font,Alignment,PatternFill
R=list(csv.DictReader(open('records.tsv'),delimiter='\t'))
M=list(csv.DictReader(open('mentions.tsv'),delimiter='\t'))
wb=Workbook(); ws=wb.active; ws.title='README'
txt=["Theatre education and Amateur: audit sheet (2026-11-08)",
"One row per core_db record in scope (397). Scope: every live record typed Amateur or Theatre education; every record whose name has a drama, studio or amateur cue, whatever its type; and 54 host records (party, club, school, choir…) whose mentions describe a drama circle or studio ('host scan').",
"Gemini 3.1 Pro judged each record from its linked mentions (up to 30 shown). Quotes were copied from the sentences and checked against them (quotes_grounded). Nothing has been written to core_db.",
"Verdicts: RETYPE · HOST_AS_UNIT (a host body typed as if it were its drama circle) · UNIT_ON_HOST (a host carrying its drama circle's mentions) · SPLIT · GENERIC · KEEP · OUT_OF_SCOPE.",
"Mention labels (Mentions tab): T training · A amateur performing · R professional performing · H about the host body · G general education · X other.",
"studio_hybrid = described both as training pupils and as performing publicly. These are the genuinely hard cases (rule 2 in the Doc).",
"Please fill in 'decision' (OK / NO / another type) and 'reviewer_comment' on the Records tab. Proposed changes are sorted first."]
for i,t in enumerate(txt,1): ws.cell(i,1,t).alignment=Alignment(wrap_text=True)
ws.column_dimensions['A'].width=140; ws['A1'].font=Font(bold=True,size=13)
s=wb.create_sheet('Summary'); s.append(['verdict','current type','proposed type','records'])
c=collections.Counter((r['verdict'],r['current_type'],r['proposed_type']) for r in R)
ORD={'RETYPE':0,'HOST_AS_UNIT':1,'UNIT_ON_HOST':2,'SPLIT':3,'GENERIC':4,'KEEP':5,'OUT_OF_SCOPE':6}
for k,n in sorted(c.items(),key=lambda x:(ORD[x[0][0]],-x[1])): s.append(list(k)+[n])
for cell in s[1]: cell.font=Font(bold=True)
for col,wd in {'A':16,'B':32,'C':32,'D':10}.items(): s.column_dimensions[col].width=wd
for name,rows,widths in (('Records',R,{'B':40,'I':60,'Q':70,'R':70,'T':40}),('Mentions',M,{'B':35,'E':28,'H':35,'M':90})):
  w=wb.create_sheet(name); hdr=list(rows[0]); w.append(hdr)
  for r in rows: w.append([int(r[h]) if h in('n_mentions','T','A','R','H','G','X','i') and r[h].isdigit() else r[h] for h in hdr])
  for cell in w[1]: cell.font=Font(bold=True); cell.fill=PatternFill('solid',fgColor='DDE6F0')
  w.freeze_panes='C2'; w.auto_filter.ref=w.dimensions
  for col,wd in widths.items(): w.column_dimensions[col].width=wd
wb.save('Theatre_education_amateur_audit_2026-11-08.xlsx'); print('ok')
