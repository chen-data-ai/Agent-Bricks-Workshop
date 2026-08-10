# Claude.md - Weather MCP Server

Context for AI assistants working on this folder.

## What this is

A small **external** MCP server (Open-Meteo weather, no API key) served over **streamable HTTP**,
hosted as a **Databricks App** and governed through the **Unity AI Gateway**. It's the "external MCP"
example in the workshop notebook on the AI Gateway. The sibling `telco-custom-mcp-server/` is a
larger "custom MCP" example.

## Structure — one file

Everything lives in [`server.py`](./server.py): a `FastMCP` instance, four `@mcp.tool()` functions,
and a `mcp.run(transport="streamable-http")` entrypoint. There is intentionally **no** `server/`
package, no `main.py`, no uvicorn scaffolding — FastMCP serves HTTP itself. Keep it that way; this
file is meant to be readable end-to-end in under a minute.

## The point of the example

Off-the-shelf MCP servers use the stdio transport (a local program, no network address); the Unity
AI Gateway reaches tools over the network, so the server must run over HTTP to have a URL. That
switch is one line: `mcp.run(transport="streamable-http")` instead of `mcp.run()`. The only extras a
hosted deployment needs (all at the top of `server.py`):
- `mcp.settings.port = int(os.environ.get("DATABRICKS_APP_PORT", 8000))` and host `0.0.0.0`.
- `stateless_http=True` so gateway/agent tool calls work without an `initialize` handshake
  (otherwise: "Missing session ID").

## Conventions

- Tools take **flat typed arguments** (e.g. `latitude: float`), not a wrapped Pydantic `params` model.
- `days`/`hours` are **clamped** to valid ranges rather than passed through, so an out-of-range value
  never causes an opaque upstream 400.
- `_get` lets httpx errors surface as a short `Error: ...` string; it never echoes the raw upstream
  response body.

## Run / test / deploy

```bash
uv run python server.py     # local, http://localhost:8000 (MCP at /mcp)
uv run pytest -q            # live smoke tests
```
Deploy: `databricks apps create/sync/deploy` (see README). `app.yaml` runs `uv run python server.py`.
Then register through the gateway per the notebook (grant `EXECUTE` on the MCP Service; never grant
`USE CONNECTION` to end users).

Adapted from [`skyloevil/weather-mcp`](https://github.com/skyloevil/weather-mcp).
