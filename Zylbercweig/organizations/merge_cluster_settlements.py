"""Collapse settlement-variant duplicates inside each cluster's
extracted_settlements via the kimatch-backed settlement_resolver.

Dry-run by default — prints the rewrite plan and exits without touching
the file. Pass --apply to write changes (a .bak is created next to the
target file).

For each cluster row in org_alignment_review.tsv:
  - parse extracted_settlements (pipe-separated)
  - resolve each string to (qid, canonical_yiddish)
  - collapse entries that share a QID, keeping the resolver's canonical
    Yiddish spelling (falls back to English if no Yiddish)
  - unresolved strings are kept as-is, deduped by string equality

Order in the output is stable: resolved entries first (in first-seen order),
then unresolved (also first-seen order).
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
import unicodedata
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_resolver import get_resolver  # noqa: E402
from settlement_overlay import resolve_anyplace  # noqa: E402

csv.field_size_limit(10**9)

_DEFAULT_ALIGN = _HERE / "org_alignment_review.tsv"
_COL = "extracted_settlements"
_SEP = " | "


def _has_conflict_markers(path: Path) -> bool:
    with path.open() as f:
        for line in f:
            if line.startswith(("<<<<<<<", "=======", ">>>>>>>")):
                return True
    return False


def _split(raw: str) -> list[str]:
    return [p.strip() for p in (raw or "").split("|") if p.strip()]


def _niqqud_count(s: str) -> int:
    """Count Hebrew combining marks (niqqud + cantillation)."""
    return sum(1 for c in s if unicodedata.combining(c))


_ADJ_SUFFIXES = ("ערן", "ערס", "ער")


def _is_adjectival(s: str) -> bool:
    return any(s.endswith(suf) for suf in _ADJ_SUFFIXES)


def _has_separator(s: str) -> bool:
    return any(c in s for c in " -")


def _pick_best_variant(variants: list[str]) -> str:
    """From a group's own variants, prefer:
      1. non-adjectival forms (avoid 'כאַרקאָווער', 'ניו יאָרקער')
      2. more niqqud (preserve pointing)
      3. contains a separator (prefer 'ניו יאָרק' over 'ניויאָרק')
      4. shorter (base noun)
      5. first-seen order
    """
    return max(
        enumerate(variants),
        key=lambda iv: (
            not _is_adjectival(iv[1]),
            _niqqud_count(iv[1]),
            _has_separator(iv[1]),
            -len(iv[1]),
            -iv[0],
        ),
    )[1]


def _collapse(raw: str, _resolver_unused=None) -> tuple[str, list[tuple[str, list[str]]]]:
    """Return (new_value, collapses) where collapses lists groups that were merged.

    Resolved entries are grouped by QID; each group is represented by the
    most-niqqud-bearing variant from the cluster's OWN strings (so the user's
    pointed Yiddish is preserved). Unresolved entries are deduped by string
    equality and kept verbatim.
    """
    parts = _split(raw)
    if not parts:
        return raw, []

    seen_qid: dict[str, list[str]] = {}
    qid_order: list[str] = []
    unresolved: list[str] = []
    unresolved_seen: set[str] = set()

    def _resolve_with_adj_fallback(s):
        hit = resolve_anyplace(s)
        if hit:
            return hit
        # Adjective fallback: וולאָצלאַווקער → וולאָצלאַוועק (Włocławek)
        for suf in ("עווער", "ערן", "ערס", "ער"):
            if s.endswith(suf):
                stem = s[: -len(suf)] + "ע"
                hit = resolve_anyplace(stem)
                if hit:
                    return hit
                break
        return None

    for s in parts:
        hit = _resolve_with_adj_fallback(s)
        if hit:
            if hit.qid not in seen_qid:
                seen_qid[hit.qid] = []
                qid_order.append(hit.qid)
            seen_qid[hit.qid].append(s)
        else:
            if s not in unresolved_seen:
                unresolved_seen.add(s)
                unresolved.append(s)

    qid_pick = {q: _pick_best_variant(seen_qid[q]) for q in qid_order}
    collapses = [
        (qid_pick[q], seen_qid[q]) for q in qid_order if len(seen_qid[q]) > 1
    ]
    had_unresolved_dups = len(unresolved) != sum(1 for s in parts if s in unresolved_seen)

    # No real merges? Leave the row untouched so we don't churn ordering.
    if not collapses and not had_unresolved_dups:
        return raw, []

    new_parts = [qid_pick[q] for q in qid_order] + unresolved
    new_value = _SEP.join(new_parts)
    return new_value, collapses


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Write changes back to the TSV (otherwise dry-run).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Print at most N changed clusters in the plan.")
    ap.add_argument("--input", type=str, default="",
                    help="Alternative input TSV path (default: org_alignment_review.tsv).")
    ap.add_argument("--map-out", type=str, default="",
                    help="Write a (cluster_id, original_variant, collapsed_to, "
                         "qid, english, source) audit TSV to this path.")
    args = ap.parse_args()

    align_path = Path(args.input) if args.input else _DEFAULT_ALIGN

    if _has_conflict_markers(align_path):
        print(f"ERROR: {align_path} contains git conflict markers. "
              "Resolve them before running with --apply.", file=sys.stderr)
        if args.apply:
            sys.exit(1)

    # No resolver instance needed at top level — _collapse uses
    # resolve_anyplace (overlay) directly. Call get_resolver() once to
    # warm its cache so per-string lookups stay fast.
    get_resolver()

    with align_path.open(newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if _COL not in fieldnames:
        print(f"ERROR: column {_COL!r} not found in {align_path}", file=sys.stderr)
        sys.exit(1)

    changed = 0
    total_collapses = 0
    printed = 0
    audit_rows: list[dict[str, str]] = []
    for r in rows:
        old = r.get(_COL, "") or ""
        new, collapses = _collapse(old)
        if new != old:
            changed += 1
            total_collapses += sum(len(v) - 1 for _, v in collapses)
            if args.limit == 0 or printed < args.limit:
                cid = r.get("cluster_id", "?")
                name = (r.get("canonical_yiddish") or "").strip()
                print(f"\n{cid}  {name}")
                print(f"  BEFORE: {old}")
                print(f"  AFTER : {new}")
                for canon, variants in collapses:
                    print(f"    · {canon} ⇐ {variants}")
                printed += 1
            if args.map_out:
                cid = r.get("cluster_id", "?")
                for canon, variants in collapses:
                    hit = resolve_anyplace(canon)
                    qid = hit.qid if hit else ""
                    eng = hit.english if hit else ""
                    src = hit.source if hit else ""
                    for v in variants:
                        audit_rows.append({
                            "cluster_id": cid,
                            "original_variant": v,
                            "collapsed_to": canon,
                            "qid": qid,
                            "english": eng,
                            "source": src,
                        })
            r[_COL] = new

    print("\n" + "─" * 70)
    print(f"Clusters changed         : {changed}")
    print(f"Variant entries removed  : {total_collapses}")

    if args.map_out:
        out = Path(args.map_out)
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["cluster_id", "original_variant", "collapsed_to",
                            "qid", "english", "source"],
                delimiter="\t",
            )
            w.writeheader()
            w.writerows(audit_rows)
        print(f"Wrote audit map to {out} ({len(audit_rows)} rows)")

    if not args.apply:
        print("\n(dry run — re-run with --apply to write changes)")
        return

    bak = align_path.with_suffix(align_path.suffix + ".bak")
    shutil.copy2(align_path, bak)
    print(f"Backup written to {bak}")

    with align_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {align_path}")


if __name__ == "__main__":
    main()
