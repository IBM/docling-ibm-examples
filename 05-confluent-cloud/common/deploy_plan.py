#!/usr/bin/env python3
"""What `./pipeline.sh deploy` would submit: one JSON line per long-running statement.

    {"name": "dol-ex02-convert", "fingerprint": "3f9c…", "file": "…/31_convert.sql",
     "sql": "-- fingerprint: 3f9c…\\nEXECUTE STATEMENT SET …",
     "views": [{"name": "dol_ex02_converted", "file": "…/22_view_converted.sql"}, …]}

A statement is a `3*.sql` file, rendered — plus everything it depends on that
lives *outside* its text: the views it reads (transitively) and the functions
those call. The fingerprint is a hash of all of it, written into the statement
as its first line; deploy reads it back from the running statement and leaves
the statement alone when it matches. So an edit to a view replaces the
statements that read it, and only those; a new artifact (the function DDL
names its id) replaces the ones that call it. Learned in example 04, where a
restart costs Docling calls: a deploy that restarts what did not change pays
for it.

The files are the example's `flink/` merged with `common/flink/` by file name,
exactly as lib.sh's sql_stage merges them (USE_COMMON_FLINK=no: the example's
only).

An example may also name **deploy-time offsets** in its env.sh:

    DEPLOY_OFFSET_VARS="URLS_FROM_OFFSETS:URLS_TOPIC CONVERSIONS_UNTIL_OFFSETS:CONVERSIONS_TOPIC"

each a variable its SQL uses in a `specific-offsets` hint, set by deploy to
where that topic ends at the moment a statement is replaced —
`partition:0,offset:42;partition:1,offset:7`. They are pinned in the
fingerprint, because they change on every deploy and are not a change to the
pipeline.

    deploy_plan.py            the plan, rendered with the offsets in the environment
    deploy_plan.py offsets    VAR=partition:0,offset:N;… — one line per offset variable, the topics' ends now

Run by common/example.sh with the environment lib.sh exported; prints,
decides nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from render import render  # noqa: E402

NAMED = re.compile(r"CREATE (?:VIEW|FUNCTION)\s+`([^`]+)`")
PINNED = "partition:0,offset:0"


def offset_vars() -> list[tuple[str, str]]:
    """DEPLOY_OFFSET_VARS as (variable, topic variable) pairs."""
    out = []
    for item in (os.environ.get("DEPLOY_OFFSET_VARS") or "").split():
        var, _, topic_var = item.partition(":")
        if not topic_var:
            raise SystemExit(f"DEPLOY_OFFSET_VARS: {item!r} is not VAR:TOPIC_VAR")
        out.append((var, topic_var))
    return out


def stage(prefix: str) -> list[pathlib.Path]:
    """lib.sh's sql_stage: the example's files and common's, merged by name, example first."""
    example = pathlib.Path(os.environ["EXAMPLE_DIR"]) / "flink"
    dirs = [example]
    if os.environ.get("USE_COMMON_FLINK", "yes") != "no":
        dirs.append(pathlib.Path(os.environ["REPO_ROOT"]) / "common" / "flink")
    seen: dict[str, pathlib.Path] = {}
    for d in dirs:
        for path in sorted(d.glob(f"{prefix}*.sql")):
            seen.setdefault(path.name, path)
    return [seen[name] for name in sorted(seen)]


def _render(path: pathlib.Path, env: dict[str, str] | None = None) -> str:
    return render(path.read_text(encoding="utf-8"), env if env is not None else dict(os.environ), path.name)


def _stable(path: pathlib.Path) -> str:
    """As rendered, with the deploy-time offsets pinned: what the fingerprint hashes."""
    return _render(path, {**os.environ, **{var: PINNED for var, _ in offset_vars()}})


def _uses(sql: str, name: str) -> bool:
    return re.search(rf"`{re.escape(name)}`|\b{re.escape(name)}\s*\(", sql) is not None


def plan() -> list[dict]:
    views = {}      # name -> (path, sql)
    stable = {}     # name -> sql with the offsets pinned
    functions = {}  # name -> sql
    for path in stage("2"):
        sql = _render(path)
        match = NAMED.search(sql)
        if not match:
            continue
        views[match.group(1)], stable[match.group(1)] = (path, sql), _stable(path)
    for path in stage("10_function_"):
        try:
            sql = _render(path)
        except SystemExit:  # no artifact id yet: setup has not run
            continue
        functions[NAMED.search(sql).group(1)] = sql

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
    for path in stage("3"):
        sql = _render(path)
        need = closure(sql)
        texts = [_stable(path)] + [stable[v] for v in need]
        fns = sorted(f for f, body in functions.items() if any(_uses(t, f) for t in texts))
        digest = hashlib.sha256("\n\0".join(texts + [functions[f] for f in fns]).encode()).hexdigest()[:16]
        stem = re.sub(r"^\d+_", "", path.stem).replace("_", "-")
        out.append({"name": f"{app}-{stem}", "fingerprint": digest, "file": str(path),
                    "sql": f"-- fingerprint: {digest}\n{sql}",
                    "views": [{"name": v, "file": str(views[v][0])} for v in need]})
    return out


def offsets() -> list[str]:
    """Each offset variable, set to where its topic ends now, every partition."""
    pairs = offset_vars()
    if not pairs:
        return []
    from confluent_kafka import Consumer, TopicPartition

    from ccenv import kafka_config, load_env

    env = load_env(os.environ["EXAMPLE_DIR"])
    consumer = Consumer({**kafka_config(env), "group.id": "deploy-plan-offsets", "log_level": 0})
    lines = []
    try:
        for var, topic_var in pairs:
            topic = env[topic_var]
            metadata = consumer.list_topics(topic, timeout=30).topics.get(topic)
            if metadata is None or metadata.error is not None or not metadata.partitions:
                # Not there yet: nothing to skip and nothing to rebuild from.
                lines.append(f"{var}={PINNED}")
                continue
            ends = []
            for partition in sorted(metadata.partitions):
                _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
                ends.append(f"partition:{partition},offset:{high}")
            lines.append(f"{var}={';'.join(ends)}")
    finally:
        consumer.close()
    return lines


if __name__ == "__main__":
    if sys.argv[1:] == ["offsets"]:
        for line in offsets():
            print(line)
    else:
        for item in plan():
            print(json.dumps(item))
