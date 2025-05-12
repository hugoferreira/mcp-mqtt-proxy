# TODO.md - mcp-mqtt-proxy Development

This document outlines the steps required to adapt the `mcp-proxy` (stdio <-> SSE/HTTP) project into `mcp-mqtt-proxy` (stdio <-> MQTT).

## Phase 1: Project Setup & Dependency Management

-   [x] Fork or copy the `mcp-proxy` repository.
-   [x] Rename the project directory and relevant identifiers (e.g., package name in `pyproject.toml`).
-   [x] Update `README.md` to reflect the new project name, purpose (MQTT focus), and link to `VISION.md`.
-   [x] Update `pyproject.toml`:
    -   [x] Change `[project].name` to `mcp-mqtt-proxy`.
    -   [x] Update `[project].description`.
    -   [x] Update `[project].urls`.
    -   [x] Remove `uvicorn` from `dependencies`.
    -   [x] Remove `starlette` related dependencies if no longer needed (likely).
    -   [x] Add an async MQTT client library (e.g., `aiomqtt`) to `dependencies`.
    -   [x] Review and update `[project].scripts` if entry point names change.
-   [ ] Update `.gitignore` if necessary.
-   [ ] Update `LICENSE` if needed (though MIT is likely fine).
-   [ ] Update `Dockerfile` (remove uvicorn/starlette setup, add MQTT dependencies).

## Phase 2: Configuration & Entry Point (`__main__.py`)

-   [x] Refactor `__main__.py` argument parsing (`argparse`):
    -   [x] Remove arguments related to SSE/HTTP (`--headers`, `--port`, `--host`, `--sse-port`, `--sse-host`, `--allow-origin`, `--stateless`).
    -   [x] Add arguments for MQTT configuration:
        -   [x] `--broker-url` (e.g., `mqtt://user:pass@host:port`)
        -   [x] `--request-topic` (Topic to publish requests to or listen on)
        -   [x] `--response-topic` (Topic to subscribe to for responses or publish responses to)
        -   [x] `--client-id` (Optional, defaults to auto-generated)
        -   [x] `--qos` (Optional, defaults to 0 or 1)
        -   [x] `--username` (Optional)
        -   [x] `--password` (Optional)
        -   [x] `--tls-ca-certs`, `--tls-certfile`, `--tls-keyfile` (Optional, for TLS)
    -   [x] Add an argument to distinguish modes, e.g., `--listen` for MQTT -> stdio mode.
-   [x] Update the logic to determine the operating mode based on the new arguments (placeholder added).
-   [x] Parse MQTT connection details from `--broker-url` and individual arguments (partially done, more in implementation phases).
-   [x] Update calls to the core run functions (placeholder added).

## Phase 3: Mode 1 Implementation (stdio Client -> MQTT)

-   [x] Rename `sse_client.py` to `mqtt_publisher.py` (or similar).
-   [x] Refactor `run_sse_client` into `run_mqtt_publisher`:
    -   [x] Remove `sse_client` usage.
    -   [x] Add `aiomqtt` client connection logic using parsed configuration.
    -   [x] Implement logic to bridge `stdio_server` streams and `ClientSession` with MQTT (initial structure with Queue):
        -   [x] Subscribe to the specified `response_topic` using `aiomqtt`.
        -   [x] Create an `asyncio.Queue` or pipe to feed messages from the MQTT subscription into the `read_stream` expected by `ClientSession`.
        -   [x] Create a mechanism where data written to the `write_stream` by `ClientSession` is published to the `request_topic` via `aiomqtt`.
        -   [x] Instantiate `ClientSession` with these adapted MQTT-backed streams.
        -   [x] Call `create_proxy_server` (from `proxy_server.py`) with the `ClientSession`.
        -   [x] Run the proxy server using `stdio_server()` streams.
    -   [ ] Ensure proper handling of MQTT connection/disconnection and errors (basic try/except added).
    -   [ ] Handle MCP message serialization (to JSON) before publishing and deserialization after receiving (placeholders added, needs refinement).

## Phase 4: Mode 2 Implementation (MQTT -> stdio Server)

-   [x] Rename `mcp_server.py` to `mqtt_listener.py` (or similar).
-   [x] Remove `uvicorn` and `starlette` related code (`create_starlette_app`, HTTP/SSE transport setup).
-   [x] Refactor `run_mcp_server` into `run_mqtt_listener`:
    -   [x] Add `aiomqtt` client connection logic.
    -   [x] Keep the `stdio_client` logic to start the local MCP process.
    -   [x] Keep `ClientSession` setup connected to the `stdio_client` streams.
    -   [x] Call `create_proxy_server` with the `ClientSession` to get the `mcp_server` instance.
    -   [x] Subscribe to the `request_topic` using `aiomqtt`.
    -   [x] Implement the MQTT message handler callback (initial structure):
        -   [ ] Deserialize incoming MQTT payload into an MCP request object (placeholder added, needs refinement).
        -   [ ] **Crucially:** Determine how to invoke the correct handler on the `mcp_server` instance based on the request type. (Basic type lookup added, needs refinement/robustness).
        -   [ ] Await the handler execution to get the MCP response (basic call added).
        -   [ ] Serialize the response (basic call added, needs refinement).
        -   [ ] Publish the response to the `response_topic` (basic MQTTv5 properties logic added).
    -   [ ] Handle notifications generated by the stdio process and publish them (placeholder added).
    -   [ ] Ensure proper handling of MQTT connection/disconnection and errors (basic try/except added).

## Phase 5: Core Proxy Logic (`proxy_server.py`)

-   [x] Review `create_proxy_server` function.
    -   [x] Verify if it requires any changes due to the transport layer modification. (No changes needed).
    -   [ ] Ensure MCP message serialization/deserialization is handled correctly at the boundaries (MQTT client/server modules) (handled in Phase 3/4, needs refinement).

## Phase 6: Testing

-   [ ] Set up an MQTT broker (e.g., Mosquitto via Docker) for integration testing.
-   [ ] Adapt existing tests in `tests/` or write new ones:
    -   [ ] Test Mode 1: Mock stdio, verify MQTT publications, mock MQTT responses, verify stdio output.
    -   [ ] Test Mode 2: Mock MQTT requests, verify stdio interactions, mock stdio responses, verify MQTT publications.
    -   [ ] Test connection handling, authentication, different QoS levels.
    -   [ ] Test error scenarios (broker disconnect, malformed messages).
-   [ ] Update `codecov.yml` if needed.

## Phase 7: Documentation & Finalization

-   [ ] Thoroughly update `README.md` with:
    -   [ ] Installation instructions.
    -   [ ] Detailed configuration options (MQTT specific).
    -   [ ] Usage examples for both modes.
    -   [ ] Troubleshooting tips.
-   [ ] Update `Dockerfile` with final build steps and entry point.
-   [ ] Ensure pre-commit hooks (`ruff`, `mypy`) pass.
-   [ ] Create release notes/changelog.
-   [ ] Publish to PyPI (if desired).
-   [ ] Build and push Docker image (if desired). 