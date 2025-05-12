"""Tests direct stdio communication with MCP servers using the mcp library."""

import asyncio
import sys
import os
import pytest
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

# Mark all tests in this module as async
pytestmark = pytest.mark.asyncio

# Get the absolute path to the weather server script
# Assuming tests run from the project root
WEATHER_SERVER_PATH = str(Path("tests/mcp_servers/weather.py").resolve())


async def test_stdio_connect_and_list_tools_weather_server():
    """Verify we can connect to the weather server via stdio and list tools."""
    server_params = StdioServerParameters(
        command=sys.executable, # Use the current python interpreter
        args=[WEATHER_SERVER_PATH], # Absolute path to the server script
        # Use current environment slightly modified (might need refinement)
        env=os.environ.copy(),
    )

    try:
        async with stdio_client(server_params) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                # Initialize is usually needed
                init_result = await session.initialize()
                assert init_result.serverInfo is not None
                assert init_result.serverInfo.name == "weather"

                # Call list_tools
                list_tools_result = await session.list_tools()

                # Assert results
                assert list_tools_result is not None
                assert len(list_tools_result.tools) == 2
                tool_names = {tool.name for tool in list_tools_result.tools}
                assert tool_names == {"get_alerts", "get_forecast"}
                print(f"Successfully listed tools: {tool_names}")

    except Exception as e:
        pytest.fail(f"Stdio communication test failed: {e}") 