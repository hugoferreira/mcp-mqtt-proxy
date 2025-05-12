**WARNING:** This fork is not working! Go to the original!

# mcp-mqtt-proxy

[![GitHub License](https://img.shields.io/github/license/bytter/mcp-mqtt-proxy)](LICENSE)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mcp-mqtt-proxy)](https://pypi.org/project/mcp-mqtt-proxy/)
[![PyPI - Version](https://img.shields.io/pypi/v/mcp-mqtt-proxy)](https://pypi.org/project/mcp-mqtt-proxy/)
[![codecov](https://codecov.io/gh/bytter/mcp-mqtt-proxy/graph/badge.svg?token=YOUR_CODECOV_TOKEN_HERE)](https://codecov.io/gh/bytter/mcp-mqtt-proxy) <!-- TODO: Update Codecov token/setup -->

**Status: Alpha** - This project adapts the original `mcp-proxy` to bridge between stdio and MQTT v5.

See the [VISION.md](VISION.md) for project goals and design details.

## About

`mcp-mqtt-proxy` is a command-line tool that acts as a bridge for the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/), translating between a standard I/O (stdio) interface and the MQTT messaging protocol (specifically targeting MQTT v5 features).

It allows you to:

1.  Connect a local stdio-based MCP tool/server to an MQTT broker.
2.  Expose a local stdio-based MCP tool/server via an MQTT interface.

This enables integrating MCP-compliant tools (like language models or development tools with stdio interfaces) into MQTT-based distributed systems, IoT environments, or other messaging workflows.

## Modes of Operation

1.  **stdio Client -> MQTT Broker:** Runs a local command (your MCP stdio server) and proxies its MCP communication *to* an MQTT broker. Requests from the stdio process are published to a request topic, and responses are received via a subscription to a response topic.
2.  **MQTT Broker -> stdio Server:** Listens for MCP requests on an MQTT topic, forwards them *to* a local command (your MCP stdio server), and publishes the responses back to the appropriate MQTT response topic (leveraging MQTT v5's request/response features).

## Installation

_(Note: Package not yet published)_.

Once published, you can install via pip or uv:

```bash
# Option 1: With uv (recommended)
uv tool install mcp-mqtt-proxy

# Option 2: With pipx
pipx install mcp-mqtt-proxy

# Option 3: Install from source (latest)
uv pip install git+https://github.com/bytter/mcp-mqtt-proxy
```

## Configuration & Usage

The proxy is configured via command-line arguments.

**Common Arguments:**

*   `--broker-url`: URL of the MQTT broker (e.g., `mqtt://localhost:1883`, `mqtts://user:pass@host:port`).
*   `--request-topic`: The MQTT topic for publishing/subscribing to MCP requests.
*   `--response-topic`: The base MQTT topic for publishing/subscribing to MCP responses.
*   _(Other MQTT options like `--client-id`, `--qos`, `--username`, `--password`, TLS settings will be added)._

**Mode 1: stdio Client -> MQTT Broker**

```bash
# Example (Conceptual - arguments subject to change)
mcp-mqtt-proxy \
    --broker-url mqtt://localhost:1883 \
    --request-topic mcp/server/requests \
    --response-topic mcp/client/my-client-id/responses \
    -- \
    your-mcp-stdio-command --arg1 --arg2 
```

**Mode 2: MQTT Broker -> stdio Server**

```bash
# Example (Conceptual - arguments subject to change)
mcp-mqtt-proxy \
    --listen \
    --broker-url mqtts://secure.broker.com:8883 \
    --request-topic mcp/server/requests \
    -- \
    your-mcp-stdio-command --arg1 --arg2
```

_(Detailed arguments and examples will be added as development progresses)._

## Docker

A Docker image can be built to run the proxy in a container.

```Dockerfile
# Example Dockerfile (Contents will be refined)
FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ ./src

# Install dependencies
RUN uv pip install --system --no-cache . 

# Default command (example)
ENTRYPOINT [ "mcp-mqtt-proxy" ]
CMD [ "--help" ]
```

Build and run:

```bash
# TODO: Update image name/tag
docker build -t mcp-mqtt-proxy:latest .
docker run -it --rm mcp-mqtt-proxy:latest --help
```

## Testing

Tests are located in the `tests/` directory and can be run using `pytest` (requires development dependencies installed via `uv pip install -e .[dev]`):

```bash
pytest
```

### Test Fixtures

The project uses standalone Python scripts for test fixtures rather than embedding code as strings within test files. This makes fixtures:

- Easier to edit (proper syntax highlighting and linting)
- More maintainable (no need for string escaping)
- Reusable across multiple test files

Fixtures can be found in the `tests/integration/fixtures/` directory, with `simple_responder.py` serving as a mock MCP server for integration tests.

## Acknowledgements

This project is derived from the original [`mcp-proxy`](https://github.com/sparfenyuk/mcp-proxy) by Sergey Parfenyuk, which provides the foundation for stdio/MCP handling and the overall proxy structure.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
