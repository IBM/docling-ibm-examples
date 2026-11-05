"""Lightning queries: millisecond reads of a topic's current contents.

The Real-Time Context Engine (Confluent Intelligence) keeps a materialized
copy of every topic it is enabled on — the latest value per key for a
compacted topic, every record otherwise — and answers point lookups, range
scans and small scans against it in milliseconds. It complements the Flink
side rather than replacing it: Flink *computes* the reports; the engine
*serves* them. No aggregates, no joins, at most 200 rows per query — those
belong in Flink.

Two ways in, and this uses the generally available one: the engine's MCP
server, the same endpoint an AI agent connects to, spoken here directly —
JSON-RPC over HTTP, ``initialize`` then ``tools/call queryData``. (A plain REST
path exists too, in Early Access.) Authentication is HTTP Basic with a Cloud
API key whose principal can read the cluster and its Schema Registry.

    python3 scripts/lightning.py tools
    python3 scripts/lightning.py query NS.ex04.spend.vendors "SELECT * FROM t LIMIT 5"

Standard library only, so the control app can use it without new
dependencies.
"""

from __future__ import annotations

import base64
import itertools
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent.parent / "common"))

PROTOCOL = "2025-06-18"


class LightningError(RuntimeError):
    pass


class Lightning:
    def __init__(self, env: dict[str, str]):
        need = ("CC_ORG_ID", "CC_ENVIRONMENT_ID", "CC_CLUSTER_ID")
        missing = [k for k in need if not env.get(k)]
        if missing:
            raise LightningError(f"unset: {', '.join(missing)}")
        region = env.get("CC_REGION", "us-east-1")
        self.endpoint = (f"https://mcp.{region}.aws.confluent.cloud/mcp/v1/context-engine/organizations/"
                         f"{env['CC_ORG_ID']}/environments/{env['CC_ENVIRONMENT_ID']}/kafka-clusters/"
                         f"{env['CC_CLUSTER_ID']}")
        key = env.get("RTCE_API_KEY") or env.get("CONFLUENT_CLOUD_API_KEY")
        secret = env.get("RTCE_API_SECRET") or env.get("CONFLUENT_CLOUD_API_SECRET")
        if not key or not secret:
            raise LightningError("no API key for the Real-Time Context Engine")
        self.auth = "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode()
        self.global_key = bool(env.get("RTCE_API_KEY"))
        self.session: str | None = None
        self.ids = itertools.count(1)
        self.lock = threading.Lock()
        self._args: dict[str, str] | None = None

    # --- JSON-RPC over streamable HTTP ----------------------------------------

    def _post(self, payload: dict[str, Any], timeout: float = 25) -> dict[str, Any] | None:
        headers = {"Authorization": self.auth, "Content-Type": "application/json",
                   "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": PROTOCOL}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        req = urllib.request.Request(self.endpoint, data=json.dumps(payload).encode(), headers=headers,
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                self.session = resp.headers.get("Mcp-Session-Id") or self.session
                body = resp.read().decode()
                kind = resp.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:300]
            if exc.code == 404 and self.session:
                self.session = None  # the server dropped the session; the caller retries
            raise LightningError(f"HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise LightningError(str(exc)) from exc
        if "id" not in payload or not body.strip():
            return None
        if "text/event-stream" in kind:
            # One response per request: take the first data line that answers our id.
            for line in body.splitlines():
                if line.startswith("data:"):
                    msg = json.loads(line[5:].strip())
                    if msg.get("id") == payload["id"]:
                        return msg
            raise LightningError("no response in the event stream")
        return json.loads(body)

    def _rpc(self, method: str, params: dict[str, Any] | None = None) -> Any:
        msg = self._post({"jsonrpc": "2.0", "id": next(self.ids), "method": method, "params": params or {}})
        if msg is None:
            raise LightningError(f"{method}: empty response")
        if "error" in msg:
            raise LightningError(f"{method}: {msg['error'].get('message', msg['error'])}")
        return msg.get("result")

    def _connect(self) -> None:
        if self.session:
            return
        self._rpc("initialize", {"protocolVersion": PROTOCOL, "capabilities": {},
                                 "clientInfo": {"name": "ex04-control-app", "version": "1"}})
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def call(self, tool: str, arguments: dict[str, Any]) -> Any:
        """Call one MCP tool; the result's text content, parsed as JSON when it is."""
        with self.lock:
            for attempt in (1, 2):
                self._connect()
                try:
                    result = self._rpc("tools/call", {"name": tool, "arguments": arguments})
                    break
                except LightningError:
                    if attempt == 2 or self.session is not None:
                        raise
        if result.get("isError"):
            text = " ".join(c.get("text", "") for c in result.get("content", []))
            raise LightningError(text or f"{tool} failed")
        if "structuredContent" in result:
            return result["structuredContent"]
        texts = [c.get("text", "") for c in result.get("content", []) if c.get("type") == "text"]
        joined = "\n".join(texts)
        try:
            return json.loads(joined)
        except ValueError:
            return joined

    def tools(self) -> list[dict[str, Any]]:
        with self.lock:
            self._connect()
            return self._rpc("tools/list").get("tools", [])

    # --- what the dashboard asks ------------------------------------------------

    def _query_args(self) -> dict[str, str]:
        """queryData's argument names, read from its declared input schema.

        The documentation names ``max_result_rows`` and describes the others
        (a topic, a SQL fragment); reading the schema means a renamed argument
        is not a broken dashboard.
        """
        if self._args is None:
            props: dict[str, Any] = {}
            for tool in self.tools():
                if tool.get("name") == "queryData":
                    props = (tool.get("inputSchema") or {}).get("properties") or {}
            pick = lambda *words, default: next((k for k in props if any(w in k.lower() for w in words)), default)
            self._args = {"topic": pick("topic", default="topic_name"),
                          "sql": pick("query", "sql", default="query"),
                          "rows": pick("max", "rows", "limit", default="max_result_rows")}
        return self._args

    def query(self, topic: str, sql: str, max_rows: int = 50) -> dict[str, Any]:
        """``queryData`` on one topic. Returns the result, the round trip in ms, and the SQL."""
        args = self._query_args()
        start = time.perf_counter()
        out = self.call("queryData", {args["topic"]: topic, args["sql"]: sql, args["rows"]: max(1, min(max_rows, 200))})
        elapsed = (time.perf_counter() - start) * 1000
        return {"topic": topic, "sql": sql, "ms": round(elapsed), "result": out, "rows": rows_of(out)}


def rows_of(result: Any) -> list[dict[str, Any]]:
    """The rows in a queryData result, as dicts keyed by column name.

    The engine answers

        {"status": "success",
         "rows": {"schema": {"columns": [{"name": "DOC_ID", ...}, ...]},
                  "data": [{"row": ["lumen-inv-0042", ...]}, ...]}}

    with column names upper-cased. They are lower-cased here, which is how every
    table in this example declares them, so a row from the engine and a record
    from the topic have the same keys. Other shapes fall through to the generic
    handling below.
    """
    if isinstance(result, dict) and isinstance(result.get("rows"), dict):
        block = result["rows"]
        cols = (block.get("schema") or {}).get("columns") or block.get("columns") or []
        names = [str(c.get("name") if isinstance(c, dict) else c).lower() for c in cols]
        out = []
        for item in block.get("data") or []:
            row = item.get("row") if isinstance(item, dict) else item
            if isinstance(row, list):
                out.append(dict(zip(names, row)))
        return out
    if isinstance(result, list):
        return [r for r in result if isinstance(r, dict)]
    if isinstance(result, dict):
        for key in ("rows", "data", "results", "records", "items"):
            if isinstance(result.get(key), list):
                inner = result[key]
                cols = result.get("columns")
                if inner and isinstance(inner[0], list) and isinstance(cols, list):
                    names = [c.get("name") if isinstance(c, dict) else c for c in cols]
                    return [dict(zip(names, row)) for row in inner]
                return [r for r in inner if isinstance(r, dict)]
    return []


def topic_status(env: dict[str, str]) -> dict[str, str]:
    """``{topic: phase}`` for every topic the engine is on, via the Cloud API."""
    key, secret = env.get("CONFLUENT_CLOUD_API_KEY"), env.get("CONFLUENT_CLOUD_API_SECRET")
    if not key:
        return {}
    url = (f"https://api.confluent.cloud/rtce/v1/rtce-topics?environment={env.get('CC_ENVIRONMENT_ID')}"
           f"&spec.kafka_cluster={env.get('CC_CLUSTER_ID')}")
    req = urllib.request.Request(url, headers={"Authorization": "Basic " + base64.b64encode(
        f"{key}:{secret}".encode()).decode(), "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.load(resp).get("data") or []
    except (urllib.error.URLError, TimeoutError, ValueError):
        return {}
    return {d["spec"]["topic_name"]: (d.get("status") or {}).get("phase", "?") for d in data}


def main() -> None:
    from ccenv import load_env

    env = load_env(HERE.parent)
    lq = Lightning(env)
    if len(sys.argv) > 1 and sys.argv[1] == "tools":
        for tool in lq.tools():
            print(tool["name"], json.dumps(tool.get("inputSchema", {}))[:400])
    elif len(sys.argv) > 3 and sys.argv[1] == "query":
        print(json.dumps(lq.query(sys.argv[2], sys.argv[3]), indent=2)[:3000])
    else:
        sys.exit("usage: lightning.py tools | query <topic> <sql>")


if __name__ == "__main__":
    main()
