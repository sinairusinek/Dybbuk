#!/usr/bin/env python3
"""Build the decision form for what the press audit left to Sinai (2026-11-01).

Input:  press_open_items.json — one item per open question: policy questions,
        parked Galron matches, doubted Galron confirmations, groups the judges
        could not decide, clusters a human had already decided differently,
        and the low-confidence new periodical rows.
Output: press_open_form.html — published as an artifact with the `db` and
        `user` capabilities. Each choice saves to the collection `decisions`,
        keyed by item id: {choice, note, reviewer, at}. Read them back with
        ArtifactData (list, collection=decisions) and apply them through
        press_audit_decisions.tsv + apply_press_audit.py.

Usage: python3 build_press_open_form.py
"""
from __future__ import annotations

import html
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ITEMS = HERE / "press_open_items.json"
OUT = HERE / "press_open_form.html"

SECTIONS = [
    ("policy", "Policy", "Three questions that set how whole groups of rows are handled."),
    ("match", "Parked Galron matches", "Title matches the research could not settle."),
    ("doubt", "Doubted Galron confirmations",
     "Confirmed earlier, but the judges think Galron's paper is a namesake in another city."),
    ("unsure", "Groups left undecided", "The mention gives no city or date to go on."),
    ("conflict", "Earlier decisions the audit would have changed",
     "A reviewer had decided these clusters already, so the audit left them alone."),
    ("low", "Low-confidence new rows",
     "Created today from thin evidence, often one mention. Keep unless the mention says otherwise."),
]

PAGE = r"""<title>Press audit decisions</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&family=Noto+Serif+Hebrew:wght@500;600&display=swap">
<style>
/* Layout: a working desk — sticky tally bar, section index, one card per question, choices as a segmented row. */
:root {
  --paper: #f6f5f1; --card: #ffffff; --ink: #1d2124; --muted: #646b70; --rule: #dcdcd4;
  --accent: #1f5c66; --accent-soft: #e2eeef; --done: #2f6b45; --done-soft: #e5f1e8;
  --warn: #8a5b12; --warn-soft: #f7ecd9;
  --f-body: "IBM Plex Sans", system-ui, sans-serif;
  --f-yid: "Noto Serif Hebrew", "Times New Roman", serif;
  --f-mono: "IBM Plex Mono", ui-monospace, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --paper: #15181a; --card: #1d2124; --ink: #e8e9e6; --muted: #9aa1a5; --rule: #30363a;
  --accent: #7fbcc6; --accent-soft: #1d3135; --done: #86c79c; --done-soft: #1c2e22;
  --warn: #e0b46a; --warn-soft: #33281a; color-scheme: dark } }
:root[data-theme="dark"] {
  --paper: #15181a; --card: #1d2124; --ink: #e8e9e6; --muted: #9aa1a5; --rule: #30363a;
  --accent: #7fbcc6; --accent-soft: #1d3135; --done: #86c79c; --done-soft: #1c2e22;
  --warn: #e0b46a; --warn-soft: #33281a; color-scheme: dark }
body { background: var(--paper); color: var(--ink); font: 15px/1.55 var(--f-body); }
body::before { content: ""; position: fixed; inset: 0 0 auto; height: env(safe-area-inset-top, 0px); z-index: 10; background: var(--paper); }
.wrap { max-width: 980px; margin: 0 auto; padding-block: 28px 80px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px)); }
h1 { font-size: 24px; font-weight: 600; margin: 0 0 6px; text-wrap: balance; }
.lede { color: var(--muted); max-width: 66ch; margin: 0 0 18px; }
.bar { position: sticky; top: env(safe-area-inset-top, 0px); z-index: 5; background: var(--paper);
  border-bottom: 1px solid var(--rule); padding: 10px 0; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.tally { font-variant-numeric: tabular-nums; font-size: 13px; color: var(--muted); margin-right: auto; }
.tally b { color: var(--ink); }
.chip { font: 500 12.5px var(--f-body); border: 1px solid var(--rule); background: var(--card); color: var(--ink);
  border-radius: 6px; padding: 4px 9px; cursor: pointer; text-decoration: none; font-variant-numeric: tabular-nums; }
.chip:hover { border-color: var(--accent); }
.ro { background: var(--warn-soft); color: var(--warn); border-radius: 8px; padding: 10px 14px; margin: 12px 0; font-size: 13.5px; }
section { margin-top: 34px; }
section > header { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 14px; margin-bottom: 4px; }
h2 { font-size: 18px; font-weight: 600; margin: 0; }
.count { font: 12.5px var(--f-mono); color: var(--muted); }
.secnote { color: var(--muted); font-size: 13.5px; margin: 0 0 10px; max-width: 70ch; }
.bulk { margin: 0 0 12px; }
.item { background: var(--card); border: 1px solid var(--rule); border-radius: 10px; padding: 13px 15px; margin-bottom: 10px; }
.item.decided { border-color: var(--done); }
.item .t { font: 600 17px/1.4 var(--f-yid); direction: rtl; unicode-bidi: plaintext; text-align: start; overflow-wrap: anywhere; }
.item.policy .t { font: 600 16px var(--f-body); direction: ltr; }
.sub { color: var(--muted); font-size: 13.5px; margin-top: 3px; unicode-bidi: plaintext; overflow-wrap: anywhere; }
.ctx { margin: 8px 0 0; padding: 0; list-style: none; display: grid; gap: 4px; }
.ctx li { font: 14px/1.5 var(--f-yid); direction: rtl; unicode-bidi: plaintext; text-align: start;
  background: var(--paper); border-radius: 6px; padding: 5px 9px; overflow-wrap: anywhere; }
.item.policy .ctx li { font-family: var(--f-body); direction: ltr; }
.links { margin-top: 6px; font-size: 12.5px; display: flex; flex-wrap: wrap; gap: 10px; }
.links a { color: var(--accent); }
.ids { font: 11.5px var(--f-mono); color: var(--muted); margin-top: 6px; overflow-wrap: anywhere; }
.opts { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--rule); align-items: center; }
.opt { font: 13px var(--f-body); border: 1px solid var(--rule); background: var(--paper); color: var(--ink);
  border-radius: 7px; padding: 6px 11px; cursor: pointer; text-align: left; }
.opt:hover { border-color: var(--accent); }
.opt .sug { font-size: 11px; color: var(--accent); margin-left: 6px; letter-spacing: .03em; }
.opt[aria-pressed="true"] { background: var(--done-soft); border-color: var(--done); color: var(--done); font-weight: 600; }
.note { flex: 1 1 200px; min-width: 0; font: 13px var(--f-body); padding: 6px 9px; border-radius: 7px;
  border: 1px solid var(--rule); background: var(--paper); color: var(--ink); }
button:focus-visible, .note:focus-visible, a:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
@media (prefers-reduced-motion: no-preference) { .item { transition: border-color .15s; } }
</style>
<div class="wrap">
<h1>Press audit decisions</h1>
<p class="lede">What the periodical audit of 1 November left for you. Each choice saves as you click; nothing
changes in core_db until Claude applies your answers. The suggested option is marked — the
"accept suggestions" button in a section fills only the items you have not touched.</p>
<div class="bar">
  <span class="tally" id="tally">Loading your saved choices…</span>
  __NAV__
  <button class="chip" id="next" type="button">Next open item</button>
</div>
<div class="ro" id="ro" hidden>Saving is unavailable in this view, so choices will not be kept. Open the page signed in, with edit access.</div>
__BODY__
</div>
<script>
(function () {
  "use strict";
  var COLL = "decisions", db = null, me = "", state = Object.create(null);
  var items = Array.prototype.slice.call(document.querySelectorAll(".item"));
  function paint(id) {
    var el = document.getElementById("i-" + id); if (!el) return;
    var st = state[id] || {};
    el.querySelectorAll(".opt").forEach(function (b) { b.setAttribute("aria-pressed", String(b.dataset.v === st.choice)); });
    var n = el.querySelector(".note"); if (n && document.activeElement !== n) n.value = st.note || "";
    el.classList.toggle("decided", !!st.choice);
  }
  function tally() {
    var done = items.filter(function (el) { return (state[el.dataset.id] || {}).choice; }).length;
    document.getElementById("tally").innerHTML = "<b>" + done + "</b> of <b>" + items.length + "</b> decided";
    document.querySelectorAll("[data-sec-count]").forEach(function (c) {
      var sec = c.dataset.secCount, all = items.filter(function (el) { return el.dataset.sec === sec; });
      var d = all.filter(function (el) { return (state[el.dataset.id] || {}).choice; }).length;
      c.textContent = d + " / " + all.length;
    });
  }
  function save(id) {
    if (!db) return;
    var st = state[id], ref = db.collection(COLL).doc(id);
    var p = st && st.choice ? ref.set({ choice: st.choice, note: st.note || "", reviewer: me, at: new Date().toISOString() }) : ref.delete();
    p.catch(function () { document.getElementById("ro").hidden = false; });
  }
  function set(id, choice, note) {
    var st = state[id] || (state[id] = {});
    if (choice !== undefined) st.choice = choice;
    if (note !== undefined) st.note = note;
    if (!st.choice) delete state[id];
    paint(id); tally(); save(id);
  }
  document.addEventListener("click", function (ev) {
    var b = ev.target.closest("button"); if (!b) return;
    if (b.classList.contains("opt")) {
      var id = b.closest(".item").dataset.id, cur = (state[id] || {}).choice;
      set(id, cur === b.dataset.v ? "" : b.dataset.v);
    } else if (b.dataset.bulk) {
      items.forEach(function (el) {
        if (el.dataset.sec !== b.dataset.bulk || (state[el.dataset.id] || {}).choice || !el.dataset.rec) return;
        set(el.dataset.id, el.dataset.rec);
      });
    } else if (b.id === "next") {
      var nx = items.filter(function (el) { return !(state[el.dataset.id] || {}).choice; })[0];
      if (nx) nx.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  });
  document.addEventListener("change", function (ev) {
    var n = ev.target.closest(".note"); if (!n) return;
    var id = n.closest(".item").dataset.id;
    var st = state[id] || (state[id] = {}); st.note = n.value;
    if (st.choice) save(id);
  });
  tally();
  claude.use("db").then(function (d) {
    if (!d) { document.getElementById("ro").hidden = false; document.getElementById("tally").textContent = "Saving unavailable"; return; }
    db = d;
    return claude.use("user").then(function (u) { return u && u.id ? u.id().then(function (x) { me = x || ""; }) : null; })
      .catch(function () {}).then(function () {
        db.collection(COLL).onSnapshot(function (snap) {
          var seen = Object.create(null);
          snap.docs.forEach(function (doc) { var v = doc.data() || {}; seen[doc.id] = 1;
            var old = state[doc.id] || {}; state[doc.id] = { choice: v.choice || "", note: v.note || old.note || "" }; });
          Object.keys(state).forEach(function (k) { if (!seen[k] && state[k].choice) delete state[k]; });
          items.forEach(function (el) { paint(el.dataset.id); }); tally();
        }, function () { document.getElementById("ro").hidden = false; });
      });
  }).catch(function () { document.getElementById("ro").hidden = false; });
})();
</script>
"""


def esc(s: str) -> str:
    return html.escape(s or "", quote=True)


def render(items: list[dict]) -> str:
    nav, body = [], []
    for key, title, note in SECTIONS:
        its = [i for i in items if i["section"] == key]
        if not its:
            continue
        nav.append(f'<a class="chip" href="#{key}">{esc(title.split()[0])} '
                   f'<span data-sec-count="{key}">0 / {len(its)}</span></a>')
        has_rec = any(i.get("rec") for i in its)
        out = [f'<section id="{key}"><header><h2>{esc(title)}</h2>'
               f'<span class="count" data-sec-count="{key}">0 / {len(its)}</span></header>'
               f'<p class="secnote">{esc(note)}</p>']
        if has_rec and key != "policy":
            out.append(f'<div class="bulk"><button class="chip" type="button" data-bulk="{key}">'
                       f'Accept suggestions for untouched items</button></div>')
        for i in its:
            ctx = "".join(f"<li>{esc(c)}</li>" for c in i.get("context") or [] if c)
            links = "".join(f'<a href="{esc(u)}" rel="noreferrer" target="_blank">Galron scan</a>'
                            for u in i.get("links") or [] if u)
            ids = " · ".join(filter(None, [f"db {i['db']}" if i.get("db") else "",
                                           ", ".join(i.get("clusters") or [])]))
            opts = "".join(
                f'<button type="button" class="opt" data-v="{esc(v)}" aria-pressed="false">{esc(lab)}'
                + ('<span class="sug">suggested</span>' if v == i.get("rec") else "") + "</button>"
                for v, lab in i["options"])
            out.append(
                f'<article class="item {esc(key)}" id="i-{esc(i["id"])}" data-id="{esc(i["id"])}" '
                f'data-sec="{esc(key)}" data-rec="{esc(i.get("rec") or "")}">'
                f'<div class="t">{esc(i["title"])}</div><div class="sub">{esc(i.get("sub", ""))}</div>'
                + (f'<ul class="ctx">{ctx}</ul>' if ctx else "")
                + (f'<div class="links">{links}</div>' if links else "")
                + (f'<div class="ids">{esc(ids)}</div>' if ids else "")
                + f'<div class="opts">{opts}<input class="note" id="n-{esc(i["id"])}" type="text" '
                  f'placeholder="note (optional)" aria-label="note"></div></article>')
        out.append("</section>")
        body.append("".join(out))
    return PAGE.replace("__NAV__", "".join(nav)).replace("__BODY__", "\n".join(body))


if __name__ == "__main__":
    items = json.loads(ITEMS.read_text(encoding="utf-8"))
    OUT.write_text(render(items), encoding="utf-8")
    print(f"wrote {OUT.name}: {len(items)} items")
