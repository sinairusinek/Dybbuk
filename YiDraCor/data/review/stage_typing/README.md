# Stage-typing pass, 2026-08-16

The in-session typing of the manuscript track's stage directions, kept so the
pass is reproducible and auditable rather than surviving only as applied XML.

`b_NNN.tsv` are the exported batches — every untyped `stage` span with its
play, page, `line_id`, offset, the span text and the line it sits in.
`a_NNN.txt` are the answers, one `<n> <type>` per line, with `#` comments
recording why a row was deliberately left unanswered.

Spans are addressed by `(play, page, line_id, offset)`, so the answers can be
re-applied to a page that has since been edited on Transkribus and will land on
the same span. Re-applying is a no-op where a type is already set:

    python3.11 -m annotation.stage_typing_io apply \
        --batch ../data/review/stage_typing/b_001.tsv \
        --answers ../data/review/stage_typing/a_001.txt

1,415 spans over 8 batches. 14 rows are unanswered on purpose: division
numbering (`2. (II)`, `.II A`) and the `ענדע …` trailers and `עפילאָג`, which
are different TEI elements rather than stage directions.
