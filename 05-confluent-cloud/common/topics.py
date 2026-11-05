#!/usr/bin/env python3
"""Topics, as the Kafka admin API sees them.

    common/topics.py list      what is on the cluster; this example's marked *

Which topics are "this example's" comes from ``TOPIC_VARS`` in the example's
``env.sh`` — a space-separated list of variable *names*, so the list and the
names stay in one place. Run by hand with
``EXAMPLE_DIR=examples/01-docling-outside common/topics.py list``.

It no longer creates anything. In Confluent Cloud a Flink table *is* a topic,
and each example's ``CREATE TABLE``s — applied by ``./setup.sh apply`` — create
its topics with the partitions and the retention they declare. Creating some
of them here first was only ever needed for something outside Flink to write
into before a statement ran, and Terraform now applies the tables before
anything that writes.

Uses the *cluster* API key, which is the only one of the three that can list.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from confluent_kafka.admin import AdminClient

from ccenv import kafka_config, load_env


def wanted(env: dict[str, str], list_var: str = "TOPIC_VARS") -> list[str]:
    """The example's topics, in declaration order, deduplicated."""
    names: list[str] = []
    for var in (env.get(list_var) or "").replace(",", " ").split():
        value = env.get(var)
        if value and value not in names:
            names.append(value)
    return names


def client(env: dict[str, str]) -> AdminClient:
    return AdminClient(kafka_config(env))


def cmd_list(env: dict[str, str]) -> None:
    md = client(env).list_topics(timeout=30)
    print(f"    cluster {md.cluster_id}, {len(md.brokers)} brokers")
    ours = set(wanted(env))
    for name in sorted(t for t in md.topics if not t.startswith("_")):
        mark = "*" if name in ours else " "
        print(f"    {mark} {name}  ({len(md.topics[name].partitions)} partitions)")


def main() -> None:
    action = sys.argv[1] if len(sys.argv) > 1 else "list"
    env = load_env()
    if action == "list":
        cmd_list(env)
    elif action == "create":
        sys.exit("topics are created by each example's CREATE TABLEs now: ./setup.sh apply")
    else:
        sys.exit("usage: topics.py [list]")


if __name__ == "__main__":
    main()
