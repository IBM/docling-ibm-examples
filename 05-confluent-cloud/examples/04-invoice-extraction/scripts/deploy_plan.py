#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""What `./pipeline.sh deploy` would submit: one JSON line per statement.

    {"file": ..., "name": "dol-ex04-pipeline", "fingerprint": "3f9c…",
     "sql": "-- fingerprint: 3f9c…\\nEXECUTE STATEMENT SET …", "views": ["…/20_view_submitted.sql", …]}

A statement is the flink/3*.sql file, rendered — plus everything it depends on
that lives *outside* its text: the views it reads (transitively) and the
functions those call. The fingerprint is a hash of all of it, written into the
statement as its first line; deploy reads it back from the running statement
and leaves the statement alone when it matches. So an edit to a view replaces
the statements that read it, and only those; a new artifact (the function
DDL names its id) replaces the ones that call it.

The two offsets the SQL takes from deploy — where `submissions` starts and
where the earlier `extractions` end (SUBMISSIONS_FROM_OFFSET,
EXTRACTIONS_UNTIL_OFFSET) — are left out of the fingerprint: they change on
every deploy, and are not a change to the pipeline.

    deploy_plan.py            the plan, rendered with the offsets in the environment
    deploy_plan.py offsets    SUBMISSIONS_FROM_OFFSET=… EXTRACTIONS_UNTIL_OFFSET=…, the topics' ends now

Run by pipeline.sh with the environment lib.sh exported; prints, decides
nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
EXAMPLE = HERE.parent
sys.path.insert(0, str(EXAMPLE.parent.parent / "common"))

from render import render  # noqa: E402

FLINK = EXAMPLE / "flink"
NAMED = re.compile(r"CREATE (?:VIEW|FUNCTION)\s+`([^`]+)`")


OFFSETS = ("SUBMISSIONS_FROM_OFFSET", "EXTRACTIONS_UNTIL_OFFSET")


def _render(path: pathlib.Path, env: dict[str, str] | None = None) -> str:
    return render(path.read_text(encoding="utf-8"), env if env is not None else dict(os.environ), path.name)


def _stable(path: pathlib.Path) -> str:
    """As rendered, with the deploy-time offsets pinned: what the fingerprint hashes."""
    return _render(path, {**os.environ, **{k: "0" for k in OFFSETS}})


def _uses(sql: str, name: str) -> bool:
    return re.search(rf"`{re.escape(name)}`|\b{re.escape(name)}\s*\(", sql) is not None


def plan() -> list[dict]:
    views = {}      # name -> (path, sql)
    stable = {}     # name -> sql with the offsets pinned
    functions = {}  # name -> (path, sql)
    for path in sorted(FLINK.glob("2*_view_*.sql")):
        sql = _render(path)
        name = NAMED.search(sql).group(1)
        views[name], stable[name] = (path, sql), _stable(path)
    for path in sorted(FLINK.glob("10_function_*.sql")):
        try:
            sql = _render(path)
        except SystemExit:  # no artifact id yet: setup has not run
            continue
        functions[NAMED.search(sql).group(1)] = (path, sql)

    def closure(sql: str) -> list[str]:
        """The views a text reads, and theirs, in file (= creation) order."""
        need: set[str] = set()
        todo = [sql]
        while todo:
            text = todo.pop()
            for name, (_, body) in views.items():
                if name not in need and _uses(text, name):
                    need.add(name)
                    todo.append(body)
        return sorted(need, key=lambda n: views[n][0].name)

    out = []
    app = os.environ.get("APP_NAME", "app")
    for path in sorted(FLINK.glob("3*.sql")):
        sql = _render(path)
        need = closure(sql)
        texts = [_stable(path)] + [stable[v] for v in need]
        fns = sorted(f for f, (_, body) in functions.items() if any(_uses(t, f) for t in texts))
        digest = hashlib.sha256("\n\0".join(texts + [functions[f][1] for f in fns]).encode()).hexdigest()[:16]
        stem = re.sub(r"^\d+_", "", path.stem).replace("_", "-")
        out.append({"file": str(path), "name": f"{app}-{stem}", "fingerprint": digest,
                    "sql": f"-- fingerprint: {digest}\n{sql}", "views": [str(views[v][0]) for v in need]})
    return out


def offsets() -> str:
    """The end of `submissions` and of `extractions`, as shell assignments."""
    from confluent_kafka import Consumer, TopicPartition

    from ccenv import kafka_config, load_env

    env = load_env(EXAMPLE)
    consumer = Consumer({**kafka_config(env), "group.id": "ex04-deploy-offsets", "log_level": 0})
    try:
        ends = {var: consumer.get_watermark_offsets(TopicPartition(env[topic], 0), timeout=30)[1]
                for var, topic in zip(OFFSETS, ("SUBMISSIONS_TOPIC", "EXTRACTIONS_TOPIC"))}
    finally:
        consumer.close()
    return " ".join(f"{k}={v}" for k, v in ends.items())


if __name__ == "__main__":
    if sys.argv[1:] == ["offsets"]:
        print(offsets())
    else:
        for item in plan():
            print(json.dumps(item))
