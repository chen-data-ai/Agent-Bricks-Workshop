# Weather MCP Server

A small **external** MCP server that returns weather from the free
[Open-Meteo API](https://open-meteo.com/) (no API key). It's the example server for the
"MCP through the Unity AI Gateway" notebook: we host it on a **Databricks App** and route to it
through the **Unity AI Gateway**.

It's a single file, [`server.py`](./server.py), with four tools:

| Tool | Arguments | What it returns |
|---|---|---|
| `weather_search_location` | `city` | Matching cities and their coordinates |
| `weather_get_current` | `latitude`, `longitude` | Current conditions |
| `weather_get_forecast` | `latitude`, `longitude`, `days=7` | Daily forecast (1–16 days) |
| `weather_get_hourly` | `latitude`, `longitude`, `hours=24` | Hourly forecast (1–168 hours) |

## Why we run it over HTTP

An MCP client and server talk over a *transport*. Off-the-shelf servers like this one use **stdio**:
the server runs as a local program that a client (e.g. a desktop app on your laptop) launches and
talks to through standard input/output. That's fine on one machine, but the server has no network
address — there's nothing to send a request to.

The Unity AI Gateway isn't on your laptop; it's a service in Databricks that reaches tools **over
the network**, so it needs a URL to call. A stdio server can't be reached that way. The fix is to
run the server over **HTTP** instead. MCP defines an HTTP transport ("streamable HTTP") for this,
and switching to it is essentially one line:

```python
mcp.run()                               # stdio — a local program on one machine
mcp.run(transport="streamable-http")    # HTTP — reachable at a URL, on the /mcp path
```

Two small extras for running on Databricks Apps (see the top of `server.py`):

- Bind `0.0.0.0` and the port Apps assigns: `mcp.settings.port = int(os.environ.get("DATABRICKS_APP_PORT", 8000))`.
- Create the server with `stateless_http=True` so each request is self-contained — a gateway or
  agent tool call works without the client first opening a session via an MCP `initialize` handshake
  (otherwise you get *"Missing session ID"*).

## Run locally

```bash
uv sync
uv run python server.py        # serves http://localhost:8000, MCP at /mcp
uv run pytest -q               # smoke tests (live Open-Meteo calls)
```

## Deploy to Databricks Apps

```bash
databricks apps create weather-mcp-server --description "External weather MCP server (Open-Meteo)"
databricks sync . /Workspace/Users/<you>/weather-mcp-server
databricks apps deploy weather-mcp-server --source-code-path /Workspace/Users/<you>/weather-mcp-server
databricks apps get weather-mcp-server -o json | jq -r .url   # the app URL; MCP is at <url>/mcp
```

`app.yaml` runs the server with `uv run python server.py`. Once it's up, register it through the
Unity AI Gateway (UC HTTP connection → MCP Service → grant `EXECUTE`) — the workshop notebook walks
through that.

## Credit

Adapted from the open-source [`skyloevil/weather-mcp`](https://github.com/skyloevil/weather-mcp)
(stdio → HTTP, trimmed to the four tools above).
