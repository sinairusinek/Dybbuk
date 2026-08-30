"""Every routed view must import and expose render().

Both 2026-08-30 outages were of this shape and neither was caught before
deploy: a view registered in VIEWS with no VIEW_STATUS entry (KeyError in
the sidebar radio), and a module deleted as "unrouted" while two views
still imported it as a library (ModuleNotFoundError). A plain "does the
app boot / does curl return 200" check passes in both cases, because
Streamlit only executes a view when it is actually selected.
"""

import ast
import importlib
import pathlib
import re
import sys

import pytest

BASE = pathlib.Path(__file__).resolve().parent
for p in (str(BASE), str(BASE.parent)):
    if p not in sys.path:
        sys.path.insert(0, p)


def _literal_dict(name: str) -> dict:
    src = (BASE / "app.py").read_text(encoding="utf-8")
    m = re.search(rf"^{name}\s*=\s*\{{", src, re.M)
    assert m, f"{name} not found in app.py"
    start = src.index("{", m.start())
    depth = 0
    for i in range(start, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return ast.literal_eval(src[start:i + 1])
    raise AssertionError(f"unterminated {name}")


VIEWS = _literal_dict("VIEWS")
VIEW_STATUS = _literal_dict("VIEW_STATUS")


@pytest.mark.parametrize("label", sorted(VIEWS))
def test_view_module_imports_and_renders(label):
    module = VIEWS[label][0]
    mod = importlib.import_module(f"views.{module}")
    assert hasattr(mod, "render"), f"views/{module}.py defines no render()"


def test_every_view_has_a_status_badge():
    """The sidebar radio's format_func looks up every view here."""
    assert not [v for v in VIEWS if v not in VIEW_STATUS]


def test_no_imports_from_deleted_org_alignment():
    """org_alignment.py was deleted in 1974726d; nothing may import it."""
    offenders = [
        f.name for f in (BASE / "views").glob("*.py")
        if re.search(r"from\s+views\.org_alignment\s+import|import\s+views\.org_alignment",
                     f.read_text(encoding="utf-8"))
    ]
    assert not offenders, f"import a module that no longer exists: {offenders}"
