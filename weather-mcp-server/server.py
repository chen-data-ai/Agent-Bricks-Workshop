"""
Weather MCP server (Open-Meteo, no API key) — served over HTTP.

The workshop's "external MCP" example: a small MCP server we host on a Databricks App and govern
through the Unity AI Gateway.

Making an MCP server HTTP-able is essentially one line: instead of `mcp.run()` (stdio), call
`mcp.run(transport="streamable-http")`, which serves the MCP protocol at /mcp. The two settings
below are what a hosted deployment needs:
  - host/port: bind 0.0.0.0 and the port Databricks Apps assigns ($DATABRICKS_APP_PORT).
  - stateless_http: each request stands alone, so a gateway/agent tool call works without first
    doing an MCP `initialize` handshake (otherwise you get "Missing session ID").

Everything else in this file is just the weather tools.
"""

import os

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("weather_mcp", stateless_http=True, json_response=True)
mcp.settings.host = "0.0.0.0"
mcp.settings.port = int(os.environ.get("DATABRICKS_APP_PORT", 8000))

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


async def _get(url: str, params: dict) -> dict:
    """GET JSON from Open-Meteo. Raises on HTTP error (the message has status + URL, never the body)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()


@mcp.tool()
async def weather_search_location(city: str) -> str:
    """Find the coordinates of a city. Use this first, then pass the lat/long to the other tools."""
    try:
        data = await _get(GEOCODING_URL, {"name": city, "count": 5, "language": "en"})
    except Exception as e:
        return f"Error searching for '{city}': {e}"
    results = data.get("results")
    if not results:
        return f"No locations found matching '{city}'."
    lines = [f"Locations matching '{city}':"]
    for loc in results:
        region = f", {loc['admin1']}" if loc.get("admin1") else ""
        lines.append(
            f"- {loc['name']}{region}, {loc.get('country', '')} "
            f"(lat {loc['latitude']:.4f}, lon {loc['longitude']:.4f})"
        )
    return "\n".join(lines)


@mcp.tool()
async def weather_get_current(latitude: float, longitude: float) -> str:
    """Get current weather at a latitude/longitude (temperature in °C, wind in km/h)."""
    try:
        data = await _get(FORECAST_URL, {
            "latitude": latitude,
            "longitude": longitude,
            "current": ["temperature_2m", "apparent_temperature", "relative_humidity_2m",
                        "precipitation", "cloud_cover", "wind_speed_10m"],
        })
    except Exception as e:
        return f"Error getting current weather: {e}"
    c = data["current"]
    return (
        f"Current weather at ({latitude}, {longitude}) — {c['time']}\n"
        f"- Temperature: {c['temperature_2m']}°C (feels like {c['apparent_temperature']}°C)\n"
        f"- Humidity: {c['relative_humidity_2m']}%\n"
        f"- Precipitation: {c['precipitation']} mm\n"
        f"- Cloud cover: {c['cloud_cover']}%\n"
        f"- Wind speed: {c['wind_speed_10m']} km/h"
    )


@mcp.tool()
async def weather_get_forecast(latitude: float, longitude: float, days: int = 7) -> str:
    """Get a daily forecast (1-16 days): min/max temperature and precipitation."""
    days = max(1, min(days, 16))
    try:
        data = await _get(FORECAST_URL, {
            "latitude": latitude,
            "longitude": longitude,
            "forecast_days": days,
            "daily": ["temperature_2m_min", "temperature_2m_max", "precipitation_sum"],
        })
    except Exception as e:
        return f"Error getting forecast: {e}"
    d = data["daily"]
    lines = [f"{days}-day forecast at ({latitude}, {longitude}):"]
    for i in range(len(d["time"])):
        lines.append(
            f"- {d['time'][i]}: {d['temperature_2m_min'][i]}–{d['temperature_2m_max'][i]}°C, "
            f"{d['precipitation_sum'][i]} mm precipitation"
        )
    return "\n".join(lines)


@mcp.tool()
async def weather_get_hourly(latitude: float, longitude: float, hours: int = 24) -> str:
    """Get an hourly forecast (1-168 hours): temperature, precipitation, and wind."""
    hours = max(1, min(hours, 168))
    try:
        data = await _get(FORECAST_URL, {
            "latitude": latitude,
            "longitude": longitude,
            "forecast_hours": hours,
            "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
        })
    except Exception as e:
        return f"Error getting hourly forecast: {e}"
    h = data["hourly"]
    lines = [f"{hours}-hour forecast at ({latitude}, {longitude}):"]
    for i in range(len(h["time"])):
        lines.append(
            f"- {h['time'][i]}: {h['temperature_2m'][i]}°C, "
            f"{h['precipitation'][i]} mm precip, wind {h['wind_speed_10m'][i]} km/h"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    # Serve the MCP protocol over HTTP at /mcp (instead of stdio via mcp.run()).
    mcp.run(transport="streamable-http")
