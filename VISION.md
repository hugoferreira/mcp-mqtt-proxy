# VISION.md - mcp-mqtt-proxy

## Introduction

`mcp-mqtt-proxy` aims to be a versatile command-line tool that acts as a bridge between the Model Context Protocol (MCP) communicated over standard I/O (stdio) and MCP communicated over the MQTT messaging protocol. It leverages the existing `mcp-proxy` (stdio <-> SSE/HTTP) codebase as a foundation, adapting its core proxying logic for the MQTT transport layer.

## Core Problem

Many development tools, language servers, and AI models utilize the Model Context Protocol (MCP) for interaction, often exposing a stdio interface. Separately, MQTT is a prevalent standard for messaging in IoT, distributed systems, and cloud environments. There is currently no straightforward way to integrate stdio-based MCP tools with systems communicating via MQTT.

`mcp-mqtt-proxy` addresses this gap by providing a seamless translation layer between these two communication methods for MCP messages.

## Goal

The primary goal is to create a robust, reliable, and configurable proxy that enables bidirectional MCP communication between a stdio process and an MQTT broker.

## Key Features

1.  **Bidirectional Proxying:**
    *   **stdio Client -> MQTT:** Run a local stdio MCP process and proxy its communication to an MQTT broker. The proxy subscribes to a designated response topic and publishes requests from the stdio process to a designated request topic.
    *   **MQTT -> stdio Server:** Listen for MCP requests on a designated MQTT topic, forward them to a local stdio MCP process, receive responses/notifications, and publish them back to a designated MQTT response topic.
2.  **MCP Compliance:** Fully compliant with the Model Context Protocol, utilizing the official `mcp` Python library for message handling and session management.
3.  **MQTT Transport (v5 Focus):**
    *   Targets **MQTT v5** primarily to leverage its advanced features, enhancing robustness and simplifying implementation.
    *   Utilizes the **`aiomqtt`** library (based on `paho-mqtt`) for asynchronous MQTT communication.
    *   Leverages MQTT v5's native **request/response pattern** (using `Response Topic` and `Correlation Data` properties) for cleaner mapping of MCP interactions.
    *   Connects to standard MQTT v5 brokers (with fallback potential for v3.1.1 where necessary, though v5 is preferred).
    *   Uses configurable topics for requests and responses (simplified by v5 features).
    *   Supports configurable Quality of Service (QoS) levels.
    *   Supports MQTT authentication (username/password) and TLS/SSL encryption.
    *   Uses unique client IDs for proper response routing.
4.  **Configuration:** Flexible configuration via command-line arguments and environment variables for MQTT broker details, topics, credentials, QoS, and stdio command parameters.
5.  **Asynchronous Architecture:** Built on Python's `asyncio` and the `aiomqtt` library for efficient handling of concurrent I/O operations (stdio and MQTT).
6.  **Packaging:** Distributed as an easily installable Python package (via PyPI) and potentially as a Docker container.

## Target Audience

*   Developers integrating LLMs or other MCP-compliant tools (with stdio interfaces) into MQTT-based systems.
*   Developers working with IoT devices or distributed systems that need to interact with MCP services via MQTT.
*   Users needing to expose a local stdio MCP server to remote clients over an MQTT network.

## Non-Goals

*   Implementing the Model Context Protocol itself (handled by the `mcp` library).
*   Acting as an MQTT broker.
*   Providing complex message transformation beyond standard MCP serialization/deserialization.

## Future Considerations

*   Support for additional MQTT v5 features (e.g., user properties for metadata).
*   Dynamic topic generation schemes.
*   Alternative serialization formats (if needed beyond JSON).
*   Enhanced monitoring/status endpoints.
*   Graceful fallback mechanisms if connecting to an MQTT v3.1.1-only broker. 