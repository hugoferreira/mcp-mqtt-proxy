"""The entry point for the mcp-mqtt-proxy application."""

import argparse
import asyncio
import logging
import os
import sys
import typing as t
import uuid

from mcp.client.stdio import StdioServerParameters

# Placeholders for future imports
# from .mqtt_listener import MQTTListenerSettings, run_mqtt_listener
# from .mqtt_publisher import run_mqtt_publisher

# Remove SSE_URL related code
# SSE_URL: t.Final[str | None] = os.getenv(
#     "SSE_URL",
#     None,
# )


def main() -> None:
    """Parse arguments and run the selected proxy mode."""
    parser = argparse.ArgumentParser(
        description=(
            "Start the MCP proxy to bridge between stdio and MQTT v5."
        ),
        epilog=(
            "Examples:\n"
            # Mode 1: stdio -> MQTT
            "  mcp-mqtt-proxy --broker-url mqtt://localhost:1883 --request-topic mcp/srv/req --response-topic mcp/cli/resp/{client_id} -- "
            "                   your-mcp-stdio-cmd --cmd-arg\n\n"
            # Mode 2: MQTT -> stdio (listen mode)
            "  mcp-mqtt-proxy --listen --broker-url mqtts://user:pass@host:8883 --request-topic mcp/srv/req -- "
            "                   your-mcp-stdio-cmd --cmd-arg\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Mode Selection
    parser.add_argument(
        "--listen",
        action="store_true",
        help="Run in listen mode (MQTT -> stdio Server). If not set, runs in publish mode (stdio Client -> MQTT).",
    )

    # Common MQTT Options
    mqtt_group = parser.add_argument_group("MQTT Options")
    mqtt_group.add_argument(
        "--broker-url",
        required=True,
        help="URL of the MQTT broker (e.g., 'mqtt://host:port', 'mqtts://user:pass@host:port').",
    )
    mqtt_group.add_argument(
        "--request-topic",
        required=True,
        help="MQTT topic for sending/receiving MCP requests.",
    )
    mqtt_group.add_argument(
        "--response-topic",
        help="MQTT topic pattern for receiving/sending MCP responses/notifications. "
             "In publish mode (stdio->MQTT), this is subscribed to (use {client_id} placeholder). "
             "In listen mode (MQTT->stdio), this is the base topic responses are published to.",
        # Not strictly required immediately if using MQTT v5 request/response properties primarily,
        # but useful for notifications or as fallback.
        default=None,
    )
    mqtt_group.add_argument(
        "--client-id",
        default=f"mcp-mqtt-proxy-{uuid.uuid4()}",
        help="MQTT client ID to use. Defaults to a random unique ID.",
    )
    mqtt_group.add_argument(
        "--qos",
        type=int,
        choices=[0, 1, 2],
        default=1,
        help="MQTT Quality of Service level for publishing (default: 1).",
    )
    mqtt_group.add_argument(
        "--username",
        default=os.getenv("MQTT_USERNAME"),
        help="MQTT username for authentication. Can also be set via MQTT_USERNAME env var.",
    )
    mqtt_group.add_argument(
        "--password",
        default=os.getenv("MQTT_PASSWORD"),
        help="MQTT password for authentication. Can also be set via MQTT_PASSWORD env var.",
    )
    # Basic TLS support - aiomqtt handles parsing from URL or separate params
    mqtt_group.add_argument(
        "--tls-ca-certs",
        default=None,
        help="Path to CA certificate file for TLS.",
    )
    mqtt_group.add_argument(
        "--tls-certfile",
        default=None,
        help="Path to client certificate file for TLS.",
    )
    mqtt_group.add_argument(
        "--tls-keyfile",
        default=None,
        help="Path to client private key file for TLS.",
    )

    # Stdio Command Options (used in both modes)
    stdio_group = parser.add_argument_group("Stdio Command Options")
    stdio_group.add_argument(
        "command",
        help="The command to run the local MCP stdio server.",
    )
    stdio_group.add_argument(
        "args",
        nargs=argparse.REMAINDER, # Use REMAINDER to capture all following args for the command
        help="Arguments to pass to the local MCP stdio server command.",
    )
    stdio_group.add_argument(
        "-e",
        "--env",
        nargs=2,
        action="append",
        metavar=("KEY", "VALUE"),
        help="Environment variables to set for the stdio server process. Can be used multiple times.",
        default=[],
    )
    stdio_group.add_argument(
        "--cwd",
        default=None,
        help="Working directory for the stdio server process.",
    )
    stdio_group.add_argument(
        "--pass-environment",
        action=argparse.BooleanOptionalAction,
        help="Pass through all environment variables from the proxy to the stdio server process.",
        default=False,
    )

    # General Options
    parser.add_argument(
        "--debug",
        action=argparse.BooleanOptionalAction,
        help="Enable debug mode with detailed logging output.",
        default=False,
    )

    # --- Argument parsing and processing ---
    parsed_args = parser.parse_args()

    # Basic validation
    if not parsed_args.command:
        parser.error("The 'command' argument is required.")

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if parsed_args.debug else logging.INFO,
        format="[%(levelname)1.1s %(asctime)s.%(msecs).03d %(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.debug("Parsed arguments: %s", parsed_args)

    # Prepare stdio parameters
    env: dict[str, str] = {}
    if parsed_args.pass_environment:
        env.update(os.environ)
    env.update(dict(parsed_args.env))

    stdio_params = StdioServerParameters(
        command=parsed_args.command,
        args=parsed_args.args,
        env=env,
        cwd=parsed_args.cwd if parsed_args.cwd else None,
    )

    # --- Mode Dispatch (Placeholder Logic) ---
    # This will be replaced with actual calls to run_mqtt_listener/run_mqtt_publisher
    if parsed_args.listen:
        logger.info("Starting in listen mode (MQTT -> stdio)")
        logger.warning("Listen mode implementation pending.")
        # Example future call:
        # mqtt_settings = MQTTListenerSettings(...)
        # asyncio.run(run_mqtt_listener(stdio_params, mqtt_settings))
    else:
        logger.info("Starting in publish mode (stdio -> MQTT)")
        logger.warning("Publish mode implementation pending.")
        # Example future call:
        # mqtt_config = {...}
        # asyncio.run(run_mqtt_publisher(stdio_params, mqtt_config))

    logger.info("Proxy finished (or placeholder reached).")


if __name__ == "__main__":
    main()
