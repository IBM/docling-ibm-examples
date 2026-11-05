"""The pipeline as Flink runs it, for the dashboard's SQL views and diagram.

Three sources, and the page says which one it is showing:

* **the graph** — ``diagram.build()``, read off ``flink/``: the same graph as
  the README's diagram, with the real names, so the two pictures are one.
* **live** — each long-running statement, read back from the Flink SQL API
  (``GET …/statements/{name}``): its phase, the error if it failed, and the
  SQL Confluent Cloud is running — which is what the console shows too.
* **files** — the tables, views and functions, rendered from ``flink/`` with
  the same values ``./setup.sh`` substitutes. A table also gets the
  ``INSERT INTO`` that fills it, cut out of its statement, so a report's SQL
  button shows where its numbers come from.
"""

from __future__ import annotations

import base64
import json
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
EXAMPLE = HERE.parent
sys.path.insert(0, str(EXAMPLE.parent.parent / "common"))
sys.path.insert(0, str(HERE))

import diagram  # noqa: E402

from render import render  # noqa: E402


def _strip_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--")).strip()


def _leading_comment(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        if not line.startswith("--"):
            break
        lines.append(line[2:].strip())
    return "\n".join(lines).strip()


class Pipeline:
    def __init__(self, env: dict[str, str]):
        self.env = env
        self.lock = threading.Lock()
        self._live: dict[str, Any] = {}
        self._fetched = 0.0
        self.graph = diagram.build(env)
        self.files = self._render_files()

    def _render_files(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for path in sorted((EXAMPLE / "flink").glob("*.sql")):
            if path.name[0] == "9":
                continue
            try:
                sql = render(path.read_text(encoding="utf-8"), dict(self.env), path.name)
            except SystemExit:
                continue  # e.g. no ARTIFACT_ID yet: the function DDL cannot render
            m = re.search(r"(?:TABLE IF NOT EXISTS|FUNCTION|VIEW)\s+`([^`]+)`", sql)
            name = m.group(1) if m else None
            if path.name[0] == "3":
                name = self._statement_name(path)
            if name:
                out[name] = {"file": path.name, "sql": _strip_comments(sql), "comment": _leading_comment(sql)}
        # Each sink table: the INSERT that fills it, out of its statement.
        for node in self.graph["nodes"]:
            if node["kind"] != "stmt" or node["id"] not in out:
                continue
            for insert in _inserts(out[node["id"]]["sql"]):
                target = re.search(r"INSERT INTO\s+`([^`]+)`", insert).group(1)
                if target in out:
                    out[target]["filled_by"] = {"statement": node["id"], "sql": insert}
        return out

    def _statement_name(self, path: Path) -> str:
        return f"{self.env.get('APP_NAME', 'app')}-" + re.sub(r"^\d+_", "", path.stem).replace("_", "-")

    def _get(self, name: str) -> dict[str, Any] | None:
        e = self.env
        if not all(e.get(k) for k in ("CC_ORG_ID", "CC_ENVIRONMENT_ID", "FLINK_API_KEY")):
            return None
        url = (f"https://flink.{e.get('CC_REGION', 'us-east-1')}.{e.get('CC_CLOUD', 'aws')}.confluent.cloud"
               f"/sql/v1/organizations/{e['CC_ORG_ID']}/environments/{e['CC_ENVIRONMENT_ID']}"
               f"/statements/{urllib.parse.quote(name)}")
        token = base64.b64encode(f"{e['FLINK_API_KEY']}:{e['FLINK_API_SECRET']}".encode()).decode()
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            return {"status": {"phase": "NOT DEPLOYED" if exc.code == 404 else f"HTTP {exc.code}"}}
        except (urllib.error.URLError, TimeoutError, ValueError):
            return None

    def live(self, max_age: float = 5.0) -> dict[str, Any]:
        """Each statement's state, refreshed at most every ``max_age`` seconds."""
        with self.lock:
            if time.monotonic() - self._fetched < max_age:
                return self._live
        out = {}
        for node in self.graph["nodes"]:
            if node["kind"] != "stmt":
                continue
            got = self._get(node["id"]) or {}
            status, spec = got.get("status") or {}, got.get("spec") or {}
            sql = spec.get("statement")
            out[node["id"]] = {"phase": status.get("phase", "UNKNOWN"), "detail": status.get("detail") or "",
                               "stopped": spec.get("stopped"),
                               "created_at": (got.get("metadata") or {}).get("created_at"),
                               "sql": _strip_comments(sql) if sql else None,
                               "source": "live" if sql else "files"}
        with self.lock:
            self._live, self._fetched = out, time.monotonic()
        return out

    def snapshot(self) -> dict[str, Any]:
        live = self.live()
        objects = {}
        for name, info in self.files.items():
            running = live.get(name, {})
            objects[name] = {**info, **running, "sql": running.get("sql") or info["sql"],
                             "source": running.get("source", "files")}
        return {"graph": self.graph, "objects": objects}


def _inserts(sql: str) -> list[str]:
    """The INSERT INTO … statements in a statement or statement set."""
    body = re.sub(r"^\s*EXECUTE STATEMENT SET\s+BEGIN|END;?\s*$", "", sql.strip())
    return [part.strip() + ";" for part in body.split(";") if "INSERT INTO" in part]
