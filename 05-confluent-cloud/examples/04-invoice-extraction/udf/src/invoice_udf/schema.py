# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Just enough JSON Schema to validate an extraction against its target.

Docling validates every extracted item against ``ExtractionTarget.output_schema``
with ``jsonschema``'s ``Draft202012Validator`` and reports ``validation_status``
— ``passed``, ``failed``, ``not_requested``, ``not_run`` — and one error per
violation, ``Schema validation at <json path>: <jsonschema's message>``. The synthetic extraction has to report the same thing, and the UDF runtime has no
``jsonschema`` package (its dependency set is pinned; see ``pyproject.toml``),
so this is the subset an extraction schema actually uses — what
``pydantic.BaseModel.model_json_schema()`` emits for a flat model with an
optional list of sub-models:

    type (one or a list), properties, required, items, anyOf, $ref into $defs,
    enum, pattern, minimum / maximum

``format`` is not checked, because Docling does not check it either: the
validator runs without a format checker, so ``"format": "date"`` lets
``20.02.2026`` through — and the real model does write dates that way.

Anything else in a schema is accepted without being checked. That is the
honest failure mode for a subset: it never rejects a value the real validator
would accept, it only misses some it would reject.
"""

from __future__ import annotations

import re
from typing import Any

_TYPES = {
    "string": lambda v: isinstance(v, str),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
    "null": lambda v: v is None,
}


def resolve(schema: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    """Follow a local ``$ref`` (``#/$defs/LineItem``); anything else as is."""
    ref = schema.get("$ref")
    if not isinstance(ref, str) or not ref.startswith("#/"):
        return schema
    node: Any = root
    for part in ref[2:].split("/"):
        node = node.get(part, {}) if isinstance(node, dict) else {}
    return resolve(node, root) if isinstance(node, dict) else {}


def errors(value: Any, schema: dict[str, Any], root: dict[str, Any] | None = None, path: str = "$") -> list[str]:
    """Every violation, as ``json path: message`` in jsonschema's words. Empty means valid."""
    root = root if root is not None else schema
    schema = resolve(schema, root)
    out: list[str] = []

    if "anyOf" in schema:
        if not any(not errors(value, option, root, path) for option in schema["anyOf"]):
            out.append(f"{path}: {value!r} is not valid under any of the given schemas")
        return out

    expected = schema.get("type")
    if expected is not None:
        kinds = expected if isinstance(expected, list) else [expected]
        if not any(_TYPES.get(kind, lambda v: True)(value) for kind in kinds):
            return [f"{path}: {value!r} is not of type {', '.join(repr(k) for k in kinds)}"]

    if "enum" in schema and value not in schema["enum"]:
        out.append(f"{path}: {value!r} is not one of {schema['enum']!r}")

    if isinstance(value, str):
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and not re.search(pattern, value):
            out.append(f"{path}: {value!r} does not match {pattern!r}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            out.append(f"{path}: {value!r} is less than the minimum of {schema['minimum']!r}")
        if "maximum" in schema and value > schema["maximum"]:
            out.append(f"{path}: {value!r} is greater than the maximum of {schema['maximum']!r}")

    if isinstance(value, dict):
        for name in schema.get("required", []):
            if name not in value:
                out.append(f"{path}: {name!r} is a required property")
        for name, sub in (schema.get("properties") or {}).items():
            if name in value:
                out.extend(errors(value[name], sub, root, f"{path}.{name}"))

    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for i, item in enumerate(value):
            out.extend(errors(item, schema["items"], root, f"{path}[{i}]"))
    return out


def fields(schema: dict[str, Any]) -> dict[str, Any]:
    """The top-level properties of an object schema, with ``$ref``s resolved."""
    return {name: resolve(sub, schema) for name, sub in (resolve(schema, schema).get("properties") or {}).items()}


def item_fields(prop: dict[str, Any], root: dict[str, Any]) -> list[str] | None:
    """For an array-of-objects property, the object's field names; else None."""
    for option in prop.get("anyOf", [prop]):
        option = resolve(option, root)
        if option.get("type") == "array" and isinstance(option.get("items"), dict):
            items = resolve(option["items"], root)
            return list((items.get("properties") or {}).keys()) or None
    return None
