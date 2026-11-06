#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Fill in .env by asking for each value. Run as ``./account.sh init``.

For each value it says what it is, where to get it and whether it is required,
and it asks only for what the examples you choose need. Safe to run again: a
value already set stays unless you type a new one.

Nothing is written until you confirm, so Ctrl-C leaves .env as it was. Secrets
are read without echo and go straight to the file, never on a command line. The
file is written with `set_env.apply`, which keeps the comments and every other
value, and it stays readable only by you.
"""

from __future__ import annotations

import getpass
import os
import pathlib
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass

from set_env import ASSIGNMENT, apply

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"
TEMPLATE = ROOT / ".env.example"
ALL_EXAMPLES = {"1", "2", "3", "4", "5"}

# .env is sourced by the shell, so inside its double quotes these would expand,
# run a command or end the value.
UNSAFE = re.compile(r'["$`\\\x00-\x1f\x7f]')


def _ns(value: str) -> str | None:
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value):
        return None
    return "use a letter, then letters, digits or underscores"


def _url(value: str) -> str | None:
    return None if re.match(r"https?://[^\s/]+", value) else "this should start with https://"


def _yes_no(value: str) -> str | None:
    return None if value in {"yes", "no"} else "answer yes or no"


def _prefix(*prefixes: str) -> Callable[[str], str | None]:
    def check(value: str) -> str | None:
        return None if value.startswith(prefixes) else f"this should start with {' or '.join(prefixes)}"

    return check


@dataclass(frozen=True)
class Field:
    name: str
    what: str
    where: str
    required: bool = False
    secret: bool = False
    check: Callable[[str], str | None] | None = None


ACCOUNT = (
    Field(
        "CONFLUENT_CLOUD_API_KEY",
        "Confluent Cloud API key",
        "https://confluent.cloud: top-right menu > User settings > API keys > Add API key > My account > "
        "Cloud resource management.",
        required=True,
    ),
    Field(
        "CONFLUENT_CLOUD_API_SECRET",
        "Confluent Cloud API secret",
        "Shown once, when you create the key above. Your typing stays hidden.",
        required=True,
        secret=True,
    ),
    Field(
        "NS",
        "Your namespace",
        "Pick a short prefix, such as your initials. Every object an example creates starts with it.",
        required=True,
        check=_ns,
    ),
)
DOCLING = (
    Field(
        "DOCLING_SERVICE_URL",
        "Docling Service URL",
        "Docling for IBM watsonx Workbench (https://www.ibm.com/products/docling) > start free trial > "
        "`API examples` and `API keys` (different tabs).",
        required=True,
        check=_url,
    ),
    Field(
        "DOCLING_API_KEY",
        "Docling API key",
        "The Workbench's `API keys` tab. Leave empty only for a self-hosted docling-serve that wants no key.",
        secret=True,
    ),
)
GLOBAL_KEY = Field(
    "GLOBAL_KEY",
    "Global API key for lightning queries: yes or no",
    "Only for example 04's lightning queries. Needs a Cloud key owned by an OrganizationAdmin.",
    check=_yes_no,
)
SEC = Field(
    "SEC_USER_AGENT",
    "Who you are, for SEC EDGAR",
    'Your own name and e-mail, such as "Your Name you@example.com". The SEC asks every client to identify itself.',
)
REGION = (
    Field("CC_CLOUD", "Cloud for the new cluster", "aws, gcp or azure. Press Enter to keep the value shown."),
    Field("CC_REGION", "Region for the new cluster", "That cloud's region name. Press Enter to keep the value shown."),
)
EXISTING = (
    Field(
        "CC_ENVIRONMENT_ID",
        "Environment id",
        "Listed by ./account.sh check, and in the console URL. Empty means the environment called default.",
        check=_prefix("env-"),
    ),
    Field(
        "CC_CLUSTER_ID",
        "Kafka cluster id",
        "Listed by ./account.sh check, and in the console URL. Empty means apply creates one.",
        check=_prefix("lkc-"),
    ),
    Field(
        "CC_COMPUTE_POOL_ID",
        "Flink compute pool id",
        "Listed by ./account.sh check, and in the console URL. Empty means apply creates one.",
        check=_prefix("lfcp-"),
    ),
    Field(
        "CC_PRINCIPAL_ID",
        "Owner of the API keys",
        "Accounts & access in the console. Empty means apply creates a service account.",
        check=_prefix("u-", "sa-"),
    ),
)


def read_values(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            match = ASSIGNMENT.match(line)
            if match:
                values[match.group("key")] = match.group("value").strip('"')
    return values


def ask_examples() -> set[str]:
    while True:
        answer = input("Which examples will you run? 1-5, separated by commas, or Enter for all: ").strip()
        if not answer or answer.lower() == "all":
            return set(ALL_EXAMPLES)
        chosen = {part.strip().lstrip("0") for part in answer.split(",") if part.strip()}
        if chosen and chosen <= ALL_EXAMPLES:
            return chosen
        print("    Use numbers from 1 to 5, such as 1,4.")


def ask_yes(question: str, default: bool = False) -> bool:
    hint = "Y/n" if default else "y/N"
    while True:
        answer = input(f"{question} [{hint}] ").strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("    Answer y or n.")


def ask_field(field: Field, current: str) -> str:
    print(f"\n{field.what} ({'required' if field.required else 'optional'})")
    print(f"    {field.where}")
    if not current:
        shown = ""
    elif field.secret:
        shown = " [already set: Enter keeps it]"
    else:
        shown = f" [{current}: Enter keeps it]"
    if current and not field.required:
        print("    Type - to clear it.")
    read = getpass.getpass if field.secret else input

    while True:
        value = read(f"{field.name}{shown}: ").strip()
        if not value:
            if current or not field.required:
                return current
            print("    This one is required.")
            continue
        if value == "-" and not field.required:
            return ""
        if UNSAFE.search(value):
            print('    A value here cannot contain " $ ` or a backslash, because the shell reads this file.')
            continue
        error = field.check(value) if field.check else None
        if error:
            print(f"    Check that: {error}.")
            continue
        if field.secret:
            print(f"    Received {len(value)} characters.")
        return value.rstrip("/") if field.name == "DOCLING_SERVICE_URL" else value


def fields_for(examples: set[str], current: dict[str, str], use_existing: bool) -> list[Field]:
    fields = list(ACCOUNT)
    if examples & {"2", "3", "4", "5"}:
        fields += DOCLING
    if "4" in examples:
        fields.append(GLOBAL_KEY)
    if "5" in examples:
        fields.append(SEC)
    if use_existing:
        fields += EXISTING
    elif not current.get("CC_CLUSTER_ID"):
        # A cluster already in .env decides the region, and apply writes it back.
        fields += REGION
    return fields


def write(updates: dict[str, str]) -> None:
    if not ENV_FILE.exists():
        descriptor = os.open(ENV_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(TEMPLATE.read_text(encoding="utf-8"))
    os.chmod(ENV_FILE, 0o600)
    if updates:
        apply(ENV_FILE, updates, force=True)


def run() -> int:
    if not TEMPLATE.is_file():
        print(f"{TEMPLATE} is missing.", file=sys.stderr)
        return 1

    print(f"This fills in {ENV_FILE}. Nothing is written until you confirm at the end. Ctrl-C to stop.\n")
    current = {**read_values(TEMPLATE), **read_values(ENV_FILE)}
    examples = ask_examples()
    use_existing = ask_yes(
        "Use an existing Confluent environment, cluster or compute pool, such as on a shared account?"
    )

    fields = fields_for(examples, current, use_existing)
    answers = {field.name: ask_field(field, current.get(field.name, "")) for field in fields}
    updates = {name: value for name, value in answers.items() if value != current.get(name, "")}

    print("\nTo write:" if updates else "\nNo values changed.")
    secret_names = {field.name for field in fields if field.secret}
    for name, value in updates.items():
        print(f"    {name} = {'<hidden>' if name in secret_names and value else value or '<empty>'}")
    if (updates or not ENV_FILE.exists()) and not ask_yes(f"Write {ENV_FILE}?", default=True):
        print("Nothing written.")
        return 1

    write(updates)
    print(f"{ENV_FILE} is ready, and readable only by you.")
    return 0


def main() -> int:
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        print(
            "./account.sh init asks questions, so it needs a terminal. "
            "Otherwise copy .env.example to .env and edit it.",
            file=sys.stderr,
        )
        return 1
    try:
        return run()
    except (KeyboardInterrupt, EOFError):
        print("\nStopped. Nothing written.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
