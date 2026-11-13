"""Guard: CI's marker selection must keep collecting the WS lifecycle suite.

``tests/integration/test_websocket_lifecycle.py`` was once marked
``integration``, and CI's ``-m 'not integration'`` (pyproject addopts) then
silently skipped the whole file: when coding-client 1.15.0 made uncredentialed
handshakes a 1008 refusal, 11 of its 12 tests went red and nobody saw it for
weeks. A module-level or conftest-applied marker is invisible in a diff review
of the tests themselves, so this measures the outcome instead: it collects the
file with the SAME addopts CI uses and fails if anything is deselected.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIFECYCLE = "tests/integration/test_websocket_lifecycle.py"


def test_ci_marker_selection_collects_every_lifecycle_test():
    # No -o addopts override: the child reads pyproject's addopts, including
    # its `-m 'not integration'`, exactly as CI's bare `pytest` does.
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "--no-cov",
            "-p",
            "no:cacheprovider",
            LIFECYCLE,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    out = proc.stdout + proc.stderr
    assert "not integration" in (ROOT / "pyproject.toml").read_text(), (
        "CI's marker selection moved out of pyproject addopts; update this guard"
    )
    # addopts carries -v, so the listing format varies with verbosity; the
    # summary line ("15 tests collected", "no tests collected (15 deselected)",
    # "3/15 tests collected (12 deselected)") does not.
    collected = re.search(r"(\d+)(?:/\d+)? tests? collected", out)
    deselected = re.search(r"(\d+) deselected", out)
    assert collected and int(collected.group(1)) > 0, (
        f"CI selection collects 0 tests from {LIFECYCLE}:\n{out[-2000:]}"
    )
    assert deselected is None, f"CI selection deselects {deselected.group(1)} lifecycle tests"
    assert proc.returncode == 0, out[-2000:]
