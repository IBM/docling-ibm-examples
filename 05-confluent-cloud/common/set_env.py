#!/usr/bin/env python3
"""Write values into a .env file in place, without disturbing anything else.

    common/set_env.py CC_ENVIRONMENT_ID=env-abc123 CC_CLUSTER_ID=lkc-xyz
    common/set_env.py --file examples/02-.../.env ARTIFACT_ID=cfa-xyz

Used by `./account.sh apply` and by each example's `setup.sh apply`, so
that the ids and secrets Terraform outputs land in a file instead of on your
clipboard.

There are two files it writes, and the split is the point of the layout:

* the repo's `.env` — the *account*: credentials, ids, NS. Written by
  `./account.sh`, and the default target here.
* an example's `.env` — what that example's setup discovered, chiefly the
  uploaded artifact ids. Written with `--file`, created if absent.

Four things it is careful about, all of which a `sed -i` would get wrong:

* **It only touches the names it is given.** Most of a config file is derived
  from `NS` and is written as `CHUNKS_TOPIC="${NS}.ex01.chunks"` — a literal
  the shell expands at source time. Rewriting one of those with its expanded
  value would quietly break the link to NS.
* **It keeps the trailing comment.** `CC_CLUSTER_ID=""   # lkc-xxxxxx` is half
  the documentation in that file.
* **It refuses to overwrite a non-empty secret** unless --force is given.
  Re-running `keys` should not silently replace a key you are already using
  with a new one, leaving the old one orphaned in the account.
* **It creates the file if it is not there**, but only for `--file`. A missing
  repo `.env` is a setup mistake worth stopping on; a missing example `.env` is
  just the first run.
"""

from __future__ import annotations

import pathlib
import re
import sys

ASSIGNMENT = re.compile(r'^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>"[^"]*"|\S*)(?P<rest>.*)$')

# Names whose existing value is not replaced without --force: overwriting one
# strands a credential that still exists in the account.
PROTECTED = ("_SECRET", "_API_KEY", "ARTIFACT_ID")

HEADER = """# Written by this example's ./setup.sh. Safe to delete — re-running setup
# recreates it — but then the artifact it points at is orphaned in your account
# until you remove it by hand.
"""


def is_protected(key: str) -> bool:
    return any(key.endswith(suffix) or key == suffix for suffix in PROTECTED)


def apply(path: pathlib.Path, updates: dict[str, str], force: bool) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    seen: set[str] = set()
    changed: list[str] = []

    for index, line in enumerate(lines):
        match = ASSIGNMENT.match(line)
        if not match:
            continue
        key = match.group("key")
        if key not in updates:
            continue
        seen.add(key)
        current = match.group("value").strip('"')
        new = updates[key]
        if current == new:
            continue
        if current and is_protected(key) and not force:
            print(f"    = {key} already set; leaving it (--force to replace)", file=sys.stderr)
            continue
        lines[index] = f'{key}="{new}"{match.group("rest")}'
        changed.append(key)

    for key, value in updates.items():
        if key not in seen:
            lines.append(f'{key}="{value}"')
            changed.append(key)

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return changed


def main() -> None:
    argv = sys.argv[1:]
    force = "--force" in argv
    target: pathlib.Path | None = None
    args: list[str] = []
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--force":
            index += 1
        elif arg == "--file":
            target = pathlib.Path(argv[index + 1])
            index += 2
        else:
            args.append(arg)
            index += 1

    if not args:
        raise SystemExit("usage: set_env.py [--force] [--file PATH] KEY=VALUE [KEY=VALUE ...]")

    updates: dict[str, str] = {}
    for arg in args:
        if "=" not in arg:
            raise SystemExit(f"not a KEY=VALUE pair: {arg!r}")
        key, _, value = arg.partition("=")
        updates[key] = value

    if target is None:
        target = pathlib.Path(__file__).resolve().parent.parent / ".env"
        if not target.exists():
            raise SystemExit(f"{target} does not exist — copy .env.example to .env first")
    elif not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(HEADER, encoding="utf-8")

    for key in apply(target, updates, force):
        # A protected name's *value* is never printed — but an empty one is not
        # a secret, and `destroy` clearing an artifact id should say so rather
        # than claiming it set something.
        secret = (is_protected(key) or key.endswith("_SECRET")) and updates[key]
        print(f"    + {key}={'<set>' if secret else updates[key]}")


if __name__ == "__main__":
    main()
