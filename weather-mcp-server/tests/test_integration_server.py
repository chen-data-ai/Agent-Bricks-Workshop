"""
Smoke tests for the Weather MCP server. Open-Meteo is keyless, so these make live HTTP calls.
`asyncio_mode = "auto"` (pyproject.toml) runs the async tests without a per-test marker.

Run with:  pytest -q
"""

import asyncio

from server import mcp

EXPECTED_TOOLS = {
    "weather_search_location",
    "weather_get_current",
    "weather_get_forecast",
    "weather_get_hourly",
}


async def _call(name: str, args: dict, attempts: int = 3) -> str:
    """Call a tool, retrying a couple of times on a transient upstream (Open-Meteo) blip."""
    text = ""
    for i in range(attempts):
        text = str(await mcp.call_tool(name, args))
        if "Error" not in text:
            return text
        await asyncio.sleep(1.5 * (i + 1))
    return text


async def test_tools_registered():
    tools = await mcp.list_tools()
    names = {t.name for t in tools}
    assert EXPECTED_TOOLS == names, f"tools mismatch: {names}"


async def test_search_location():
    text = await _call("weather_search_location", {"city": "Seattle"})
    assert "Error" not in text, text
    assert "Seattle" in text and "lat" in text


async def test_current_weather():
    text = await _call("weather_get_current", {"latitude": 47.6062, "longitude": -122.3321})
    assert "Error" not in text, text
    assert "Current weather" in text and "Temperature" in text


async def test_forecast_clamps_out_of_range_days():
    """days beyond 16 is clamped, not sent raw — so we get a forecast, not an upstream 400 error."""
    text = await _call("weather_get_forecast", {"latitude": 47.6062, "longitude": -122.3321, "days": 999})
    assert "Error" not in text, text
    assert "forecast" in text


async def test_unexpected_response_shape_returns_clean_error(monkeypatch):
    """A 200 with an unexpected body must return a friendly 'Error ...' string, not raise a KeyError.

    Guards the fix where the response-shape access (data["current"]/["daily"]/["hourly"]) was moved
    inside the try/except so a degraded upstream response can't leak an uncaught exception to the agent.
    """
    import server

    async def fake_get(url, params):
        return {"unexpected": "shape"}

    monkeypatch.setattr(server, "_get", fake_get)

    for name, args in [
        ("weather_get_current", {"latitude": 47.6, "longitude": -122.3}),
        ("weather_get_forecast", {"latitude": 47.6, "longitude": -122.3, "days": 3}),
        ("weather_get_hourly", {"latitude": 47.6, "longitude": -122.3, "hours": 6}),
    ]:
        text = str(await server.mcp.call_tool(name, args))
        assert "Error" in text and "KeyError" not in text and "Traceback" not in text, f"{name}: {text}"
