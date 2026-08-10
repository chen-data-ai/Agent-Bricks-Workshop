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

## Making an MCP server HTTP-able

That's the whole point of this example, and it's essentially one line. A stdio MCP server ends with:

```python
mcp.run()                               # stdio (talks to a local desktop client)
```

To serve it over HTTP so the Unity AI Gateway can reach it, run the same server with the
streamable-HTTP transport, which exposes the MCP protocol at `/mcp`:

```python
mcp.run(transport="streamable-http")    # HTTP at /mcp
```

The only other things a hosted deployment needs (see the top of `server.py`):

- Bind `0.0.0.0` and the port Databricks Apps assigns: `mcp.settings.port = int(os.environ["DATABRICKS_APP_PORT"])`.
- Create the server with `stateless_http=True` so each request is self-contained — a gateway or
  agent tool call works without first doing an MCP `initialize` handshake (otherwise you get
  *"Missing session ID"*).

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
