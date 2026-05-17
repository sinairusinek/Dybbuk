# dybbuk-phonetic

Shared phonetic utilities for bilingual Yiddish/Hebrew and English matching.

Initial implementation provides:

- Script-aware IPA candidate generation
- Lightweight Yiddish/Hebrew transliteration to IPA-like symbols
- English transliteration via `epitran` (with safe fallback)
- Feature-aware phonetic similarity with `panphon` (with safe fallback)
