#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""``.env`` as a dict, the way the shell scripts see it.

Every Python script in this repo needs the same three things — the bootstrap
servers and the Kafka key — and they all have to agree with ``common/lib.sh``
about where those come from. That agreement is this file.

The layering, outermost last:

    <repo>/.env              the account: credentials, ids, NS
    <repo>/.env.local        your private overrides, if you keep any
    <example>/env.sh         the example's names, derived from NS  (committed)
    <example>/.env           what the example's setup wrote back    (generated)
    the real environment     wins over all of it

An example layer is used only when ``EXAMPLE_DIR`` is set — which
``common/lib.sh`` exports before it runs any of these scripts, so a script
invoked through ``./setup.sh`` or ``./pipeline.sh`` sees the example's names
and the same script run by hand sees only the account's.

A hand-rolled reader rather than python-dotenv, because this runs on a machine
that has just been handed the repo, and one fewer dependency between "clone"
and "check" is worth twenty lines. It reads the files as *the shell* would: it
takes the first shlex token, drops a trailing comment, and expands ``${NAME}``
against what it has read so far.
"""

from __future__ import annotations

import os
import pathlib
import re
import shlex

REPO = pathlib.Path(__file__).resolve().parent.parent

_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-((?:\$\{[^{}]*\}|[^}])*))?\}")


def _expand(value: str, sofar: dict[str, str]) -> str:
    """``${NS}.ex01.chunks`` -> ``dol.ex01.chunks``.

    The files are written to be *sourced*, so normally the shell does this for
    free. These scripts are also run on their own — the READMEs say to — and
    without this one would happily create a topic literally called
    ``${NS}.ex01.chunks``. Earlier lines only, exactly as the shell resolves
    them, and the real environment wins over both. ``${NAME:-default}`` too,
    which is how an env.sh leaves a knob to .env: the default applies only
    when nothing set the name, or set it empty.
    """
    return _REF.sub(lambda m: os.environ.get(m.group(1)) or sofar.get(m.group(1)) or _expand(m.group(2) or "", sofar), value)


def _read_into(path: pathlib.Path, env: dict[str, str]) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        # `export FOO=bar` is how env.sh writes it, because env.sh is also
        # sourced by lib.sh and the values have to reach child processes.
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key.strip()):
            continue
        # shlex, not strip('"'): .env.example puts an explanatory comment after
        # several values, and `CC_CLUSTER_ID="lkc-x"  # lkc-xxxxxx` has to mean
        # what the shell means by it. shlex takes the first token and drops the
        # comment; an empty value gives no tokens.
        tokens = shlex.split(value, comments=True)
        env[key.strip()] = _expand(tokens[0] if tokens else "", env)


def load_env(example_dir: str | os.PathLike[str] | None = None) -> dict[str, str]:
    """The layered configuration, as a plain dict."""
    env: dict[str, str] = {}
    _read_into(REPO / ".env", env)
    _read_into(REPO / ".env.local", env)

    example = example_dir or os.environ.get("EXAMPLE_DIR")
    if example:
        base = pathlib.Path(example)
        _read_into(base / "env.sh", env)
        _read_into(base / ".env", env)

    # The real environment wins, so an exported `PARTITIONS=12` reaches a
    # script run by hand without editing anything — but only for names a file already
    # declares, so PATH and friends do not leak into the configuration.
    env.update({k: v for k, v in os.environ.items() if k in env})
    return env


def require(env: dict[str, str], *names: str) -> None:
    missing = [n for n in names if not env.get(n)]
    if missing:
        raise SystemExit(f"unset: {', '.join(missing)} — see .env.example and the example's env.sh")


def kafka_config(env: dict[str, str]) -> dict[str, object]:
    """The client configuration all three Kafka scripts use."""
    require(env, "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET")
    return {
        "bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": env["KAFKA_API_KEY"],
        "sasl.password": env["KAFKA_API_SECRET"],
        # Otherwise librdkafka prints a broker-telemetry notice on stderr in
        # the middle of a script's own output, which reads like an error and
        # is not one.
        "enable.metrics.push": False,
    }
