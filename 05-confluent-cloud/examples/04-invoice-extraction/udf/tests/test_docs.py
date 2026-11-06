# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""The README's pipeline diagram matches the SQL in flink/.

The diagram is generated (``scripts/diagram.py``) rather than drawn, and this
is the half that keeps it honest: change a statement, a view or a table without
regenerating it and this fails, with the command that fixes it.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parents[2]


def test_readme_pipeline_diagram_is_up_to_date():
    result = subprocess.run([sys.executable, str(EXAMPLE / "scripts" / "diagram.py"), "--check"],
                            capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr or result.stdout
