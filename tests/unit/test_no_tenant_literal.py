"""The ``"n8n-dev"`` tenant literal must not come back into ``src/``.

WHAT WAS WRONG
--------------
``config/settings.py`` carried::

    DEFAULT_TENANT_ID: str = "n8n-dev"

read at ``clients/coding_analysis.py`` to pin ``X-Tenant-Id`` on every outbound
call to coding-analysis — *alongside* a valid ``CODING_SERVICE_TOKEN``.

``"n8n-dev"`` is not junk and it is not a real tenant_id either, which is exactly
what made it dangerous. It **was** a real tenant, as a text tenant_id, until the
2026-04-29 Keycloak migration moved tenant identity to uuid7 and demoted the
label to a ``tenant_name`` attribute. So the fallback pointed at the *stale label
form* of a tenant that really exists: an unset ``DEFAULT_TENANT_ID`` would not
have 401'd and would not have crashed — coding-realtime would have **vouched,
with a valid service credential, for a tenant nobody chose**.

coding-client removed the identical literal from the SDK in 1.9.0 and says so in
``coding_client.auth.outbound``'s module docstring. That fix reached this repo's
*imports* and not its *settings*, because this repo defined its own copy. Two
sibling services had the same shape (``coding-analysis/src/config/settings.py``,
``coding-containers/src/main.py``); both are dead constants and both are gone.

WHY THE SUITE DID NOT CATCH IT
------------------------------
``tests/unit/test_clients.py`` asserted::

    assert request.headers["x-tenant-id"] == get_settings().DEFAULT_TENANT_ID

which is a tautology — it compares the code to itself and is green for *any*
value. And ``tests/conftest.py`` ``setdefault``s the real production uuid7, so no
test ever ran with the literal in play. The behavioural tests now assert an
explicit expected value and a refusal; this file stops the literal returning by a
route those tests do not cover (a new module, a new default, a rename).

MUTANTS — each was run against this file
----------------------------------------
    M1  restore ``DEFAULT_TENANT_ID: str = "n8n-dev"`` in settings   -> FAILS
    M2  reintroduce it under another name, same literal              -> FAILS
    M3  put the literal in a docstring/comment only                  -> passes
        (deliberate: prose explaining the removal must stay legal)
    M4  point the scan root at an empty directory                    -> the
        non-vacuity test FAILS, not the rules
"""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

# Resolve the scan root through an IMPORTED module, never a path relative to
# this file. A relative path is the classic way a guard ends up scanning an
# empty directory and passing vacuously (M4).
_MODULE_FILE = importlib.import_module("clients.coding_analysis").__file__
assert _MODULE_FILE is not None, "clients.coding_analysis has no __file__"
SRC = pathlib.Path(_MODULE_FILE).parent.parent

BANNED_TENANT_LITERAL = "n8n-dev"


def _docstring_nodes(tree: ast.Module) -> set[int]:
    """``id()`` of every docstring Constant — module, class and function.

    Docstrings are prose, not values the code can hand out, so they are exempt
    from the literal rule. They have to be excluded by IDENTITY rather than by
    "is the first statement", because ``ast.walk`` yields them flattened and a
    positional test would also exempt a real assignment that happens to be
    first in its block.
    """
    out: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
            and (doc := ast.get_docstring(node, clean=False)) is not None
        ):
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                assert first.value.value == doc  # the node we exempt IS the docstring
                out.add(id(first.value))
    return out


@pytest.fixture(scope="module")
def parsed() -> list[tuple[pathlib.Path, ast.Module]]:
    """Every ``src/`` module, parsed once."""
    out = []
    for path in sorted(SRC.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        out.append((path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path))))
    return out


def test_the_scan_is_not_vacuous(parsed):
    """Prove the walk found this service's real source before trusting a pass.

    Without this, deleting or mistyping ``SRC`` turns every rule below into a
    green that measured nothing.
    """
    files = {p for p, _ in parsed}
    assert len(files) >= 30, f"only {len(files)} source files found under {SRC}"
    names = {p.name for p in files}
    assert "settings.py" in names, f"settings.py not in the scanned set under {SRC}"
    assert "coding_analysis.py" in names, f"coding_analysis.py not in the scanned set under {SRC}"

    # Positive control that the AST walk actually parses string constants: a
    # string we KNOW is in the tree must be found by the same traversal the
    # rule below uses. If this stops holding, the rule is blind, not clean.
    found_known = any(
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and "coding-analysis" in node.value
        for _, tree in parsed
        for node in ast.walk(tree)
    )
    assert found_known, "the AST walk found no known string constant — it is not seeing code"


def test_the_n8n_dev_tenant_literal_is_absent_from_src(parsed):
    """No ``src/`` module may carry ``"n8n-dev"`` as a string constant.

    Keyed on the LITERAL, not on the variable name, so reintroducing it under
    any other name (``FALLBACK_TENANT``, ``TENANT_DEFAULT``, a pydantic field
    default) is still caught. Comments and docstrings are invisible to the AST
    walk by design — prose explaining why the literal was removed must remain
    legal, and this docstring is itself the standing false-positive control.
    """
    offenders = []
    for path, tree in parsed:
        exempt = _docstring_nodes(tree)
        offenders += [
            f"{path.relative_to(SRC)}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and BANNED_TENANT_LITERAL in node.value
            and id(node) not in exempt
        ]
    assert not offenders, (
        f"the {BANNED_TENANT_LITERAL!r} tenant literal is back at: {offenders}. "
        "It is the stale pre-uuid7 label of a real tenant, so it fails SILENTLY: "
        "calls carry a valid service token while asserting a tenant nobody chose. "
        "Read the tenant from coding_client.auth.tenant.DEFAULT_TENANT_ID (None "
        "when unset) and refuse via outbound.tenant_headers instead."
    )


def test_settings_has_no_default_tenant_field():
    """``config.settings.Settings`` must not re-grow a tenant default.

    Complements the literal rule: this catches the field coming back with a
    *different* (e.g. uuid-shaped) default, which the literal rule cannot see
    and which is the same defect — a tenant chosen by the code rather than by
    configuration.
    """
    from config.settings import Settings

    assert "DEFAULT_TENANT_ID" not in Settings.model_fields, (
        "Settings.DEFAULT_TENANT_ID is back. The tenant must come from "
        "coding_client.auth.tenant.DEFAULT_TENANT_ID, which is None when unset, "
        "so a misconfigured deploy refuses instead of guessing."
    )
