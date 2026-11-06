# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Schema Registry client helper for example 05.

Provides schema registry encoding and decoding for Confluent Cloud framed JSON
(value.format = json-registry).

On the wire that is Confluent's framing:
    byte 0     magic, always 0
    bytes 1-4  the schema id, big-endian
    bytes 5-   the JSON document
"""

from __future__ import annotations

import base64
import json
import struct
import urllib.error
import urllib.request
from typing import Any


class SchemaRegistryError(RuntimeError):
    pass


class SchemaRegistry:
    def __init__(self, env: dict[str, str]):
        url, key, secret = (
            env.get(k)
            for k in (
                "SCHEMA_REGISTRY_URL",
                "SCHEMA_REGISTRY_API_KEY",
                "SCHEMA_REGISTRY_API_SECRET",
            )
        )
        if not (url and key and secret):
            raise SchemaRegistryError(
                "SCHEMA_REGISTRY_URL / _API_KEY / _API_SECRET are not set — "
                "run ./account.sh apply — it mints the Schema Registry key"
            )
        self.url = url.rstrip("/")
        self.auth = "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode()
        self._ids: dict[str, int] = {}

    def _get(self, path: str) -> Any:
        req = urllib.request.Request(
            self.url + path,
            headers={
                "Authorization": self.auth,
                "Accept": "application/vnd.schemaregistry.v1+json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            raise SchemaRegistryError(
                f"{path}: HTTP {exc.code} {exc.read().decode(errors='replace')[:200]}"
            ) from exc

    def value_schema_id(self, topic: str) -> int:
        if topic not in self._ids:
            try:
                self._ids[topic] = int(
                    self._get(f"/subjects/{topic}-value/versions/latest")["id"]
                )
            except SchemaRegistryError as exc:
                raise SchemaRegistryError(
                    f"no schema for {topic} — has ./setup.sh apply run? ({exc})"
                ) from exc
        return self._ids[topic]

    def encode(self, topic: str, value: dict[str, Any]) -> bytes:
        return struct.pack(">bI", 0, self.value_schema_id(topic)) + json.dumps(value).encode()


def decode(raw: bytes | None) -> Any:
    """A framed JSON value as a Python object; unframed JSON is accepted too."""
    if raw is None:
        return None
    if len(raw) > 5 and raw[0] == 0:
        raw = raw[5:]
    return json.loads(raw)
