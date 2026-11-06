#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Fill ``${NAME}`` placeholders in a SQL file from the environment.

Strictly, and that is the entire reason this is 30 lines of Python instead of
``envsubst``: ``envsubst`` substitutes an **empty string** for a name it does
not know. That would produce ``INSERT INTO `` `` `` — or worse, a statement
that runs.

    scripts/render.py flink/30_pipeline.sql   # env -> stdout

Placeholders are ``${NAME}`` only. ``$NAME`` is left alone, because Flink SQL
uses ``$rowtime`` and ``$.chunk_id`` and neither is ours to touch.

A name that is **set but empty** is substituted as empty, and only an **unset**
name is an error. That is the line worth being careful about: a typo in a
placeholder is unset and still fails loudly, while a variable that is
legitimately empty — ``BLOCKED_DOC_IDS`` with nothing blocked — does not need a
sentinel value invented for it.
"""

from __future__ import annotations

import os
import re
import sys

PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def render(text: str, env: dict[str, str], source: str) -> str:
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in env:
            missing.append(name)
            return ""
        return env[name]

    out = PLACEHOLDER.sub(replace, text)
    if missing:
        names = ", ".join(sorted(set(missing)))
        raise SystemExit(f"{source}: unset: {names}\nSet them in .env (see .env.example).")
    return out


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: render.py <file.sql>")
    path = sys.argv[1]
    with open(path, encoding="utf-8") as handle:
        sys.stdout.write(render(handle.read(), dict(os.environ), path))


if __name__ == "__main__":
    main()
