"""Main CLI entry point for MCP MQTT Proxy."""

import argparse
import asyncio
import importlib.metadata
import json
import logging
import os
import sys
import uuid
import platform
import contextlib
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from mcp_mqtt_proxy.config import MQTTListenerConfig, MQTTPublisherConfig
from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener
from mcp_mqtt_proxy.mqtt_publisher import run_mqtt_publisher
from mcp_mqtt_proxy.utils import generate_id
from mcp.client.stdio import StdioServerParameters

logger = logging.getLogger(__name__)

T = TypeVar("T")

# --- Configuration Helpers ---

def parse_config[T](config_cls: type[T], arg_source: argparse.Namespace) -> T:
    """Parses arguments from argparse.Namespace into a config object.
    
    This serves as a bridge from CLI args to fully typed config objects
    for each mode (listener/publisher).
    
    Args:
        config_cls: The configuration class to instantiate (MQTTListenerConfig or MQTTPublisherConfig)
        arg_source: The parsed arguments from argparse
        
    Returns:
        A populated configuration object
    """
    # Common MQTT connection arguments
    config_dict = {
        "broker_url": arg_source.broker_url,
        "base_topic": arg_source.base_topic,
        "client_id": arg_source.client_id,
        "qos": arg_source.qos,
        "username": arg_source.username,
        "password": arg_source.password,
        "tls_ca_certs": arg_source.tls_ca_certs,
        "tls_certfile": arg_source.tls_certfile,
        "tls_keyfile": arg_source.tls_keyfile,
    }
    
    # Remove None values to allow config defaults to apply
    config_dict = {k: v for k, v in config_dict.items() if v is not None}
    
    # Add mode-specific args
    if config_cls == MQTTListenerConfig:
        # Build stdio process info for listener
        env = {}
        if arg_source.pass_environment:
            env.update(os.environ)
        # Add explicit environment variables, which can override inherited ones
        for key, value in arg_source.env:
            env[key] = value
            
        # Create the stdio process configuration as a StdioServerParameters object
        # instead of a dict to match the MQTTListenerConfig type expectation
        config_dict["stdio_mcp_process"] = StdioServerParameters(
            command=arg_source.command,
            args=arg_source.args,
            env=env,
            cwd=arg_source.cwd,
        )
        config_dict["mcp_timeout"] = arg_source.mcp_timeout
        
    elif config_cls == MQTTPublisherConfig:
        # Add publisher-specific args
        if hasattr(arg_source, "debug_timeout"):
            config_dict["debug_timeout"] = arg_source.debug_timeout
    
    # Handle debug_timeout for listener config too
    if config_cls == MQTTListenerConfig and hasattr(arg_source, "debug_timeout"):
        config_dict["debug_timeout"] = arg_source.debug_timeout
        
    # Create and return the config object
    return config_cls(**config_dict)


def create_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser with all options."""
    # Parent parser for common arguments
    parent_parser = argparse.ArgumentParser(add_help=False)
    
    # --- Common Arguments ---
    parent_parser.add_argument(
        "--broker-url",
        required=True,
        help="URL of the MQTT broker (e.g., 'mqtt://host:port', 'mqtts://user:pass@host:port').",
    )
    parent_parser.add_argument(
        "--base-topic",
        required=True,
        help="MQTT topic base (e.g., 'mcp/server'). Request topic will be '{base-topic}/request', response topics '{base-topic}/response/<client_id>'.",
    )
    parent_parser.add_argument(
        "--client-id",
        default=f"mcp-mqtt-proxy-{generate_id()}",
        help="Client ID for MQTT connection (default: auto-generated).",
    )
    parent_parser.add_argument(
        "--qos",
        type=int,
        choices=[0, 1, 2],
        default=1,
        help="MQTT Quality of Service level for publishing (default: 1).",
    )
    parent_parser.add_argument(
        "--username",
        default=os.getenv("MQTT_USERNAME"),
        help="MQTT username for authentication. Can also be set via MQTT_USERNAME env var.",
    )
    parent_parser.add_argument(
        "--password",
        default=os.getenv("MQTT_PASSWORD"),
        help="MQTT password for authentication. Can also be set via MQTT_PASSWORD env var.",
    )
    parent_parser.add_argument(
        "--tls-ca-certs", default=None, help="Path to CA certificate file for TLS."
    )
    parent_parser.add_argument(
        "--tls-certfile", default=None, help="Path to client certificate file for TLS."
    )
    parent_parser.add_argument(
        "--tls-keyfile", default=None, help="Path to client private key file for TLS."
    )
    parent_parser.add_argument(
        "--debug",
        action=argparse.BooleanOptionalAction,
        help="Enable debug mode with detailed logging output.",
        default=False,
    )

    # Main parser
    parser = argparse.ArgumentParser(
        description=("Start the MCP proxy to bridge between stdio and MQTT v5."),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Subparsers for modes
    subparsers = parser.add_subparsers(
        dest="mode",
        required=True,
        help="Select the proxy mode: 'publish' (stdio -> MQTT) or 'listen' (MQTT -> stdio).",
    )

    # --- Publish Mode (stdio -> MQTT) --- 
    parser_publish = subparsers.add_parser(
        "publish",
        parents=[parent_parser], # Inherit common args
        help="Run in publish mode (stdio Client -> MQTT). Uses process stdin/stdout.",
        epilog=(
            "Examples:\n"
            "  your-client-app | mcp-mqtt-proxy publish --broker-url ... --base-topic mcp/server\n"
            "  mcp-mqtt-proxy publish --broker-url ... --base-topic mcp/server < input.jsonl > output.jsonl"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # Add debug timeout option for testing
    parser_publish.add_argument(
        "--debug-timeout",
        type=float,
        help="For testing: automatically exit after this many seconds.",
        default=None
    )
    
    # REMOVED stdio command arguments from publish mode
    # stdio_group = parser_publish.add_argument_group("Stdio Command Options (for publish mode)")
    # stdio_group.add_argument(...)
    
    # Update description for publish mode
    parser_publish.description = (
        "Runs the proxy in publish mode (stdio -> MQTT). Reads MCP messages from stdin, " 
        "publishes requests to '{base-topic}/request', subscribes to responses on '{base-topic}/response/<client-id>', "
        "and writes responses to stdout."
    )


    # --- Listen Mode (MQTT -> stdio) --- Requires stdio command
    parser_listen = subparsers.add_parser(
        "listen",
        parents=[parent_parser], # Inherit common args
        help="Run in listen mode (MQTT -> stdio Server). Runs and connects to a local stdio server.",
        epilog=(
            "Example:\n" 
            "  mcp-mqtt-proxy listen --broker-url ... --base-topic mcp/server -- your-server-cmd --arg"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # Add specific help for response topic in listen mode
    parser_listen.description = (
        "Runs the proxy in listen mode (MQTT -> stdio). Subscribes to '{base-topic}/request', "
        "runs the specified stdio server command, forwards requests to it, and publishes responses "
        "using MQTT v5 properties (Response Topic, Correlation Data)."
    )
    # Add stdio command arguments, same as publisher mode
    stdio_group_listen = parser_listen.add_argument_group("Stdio Server Command Options (for listen mode)")
    stdio_group_listen.add_argument(
        "command",
        help="The command to run the local MCP stdio server.",
    )
    stdio_group_listen.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="Arguments to pass to the local MCP stdio server command.",
    )
    stdio_group_listen.add_argument(
        "-e",
        "--env",
        nargs=2,
        action="append",
        metavar=("KEY", "VALUE"),
        help="Environment variables to set for the stdio server process. Can be used multiple times.",
        default=[],
    )
    stdio_group_listen.add_argument(
        "--cwd",
        default=None,
        help="Working directory for the stdio server process.",
    )
    stdio_group_listen.add_argument(
        "--pass-environment",
        action=argparse.BooleanOptionalAction,
        help="Pass through all environment variables from the proxy to the stdio server process.",
        default=False,
    )
    parser_listen.add_argument(
        "--mcp-timeout",
        type=float,
        default=30.0,
        help="Timeout in seconds waiting for a response from the stdio MCP server (default: 30).",
    )
    
    # Add debug timeout option for testing
    parser_listen.add_argument(
        "--debug-timeout",
        type=float,
        help="For testing: automatically exit after this many seconds.",
        default=None
    )

    return parser


def main() -> None:
    """Parse arguments and run the selected proxy mode."""
    parser = create_parser()
    parsed_args = parser.parse_args()

    # Setup logging (adjust level based on debug flag)
    log_level = logging.DEBUG if parsed_args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="[%(levelname)1.1s %(asctime)s.%(msecs).03d %(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info(f"Log level set to {logging.getLevelName(log_level)}")
    logger.debug("Parsed arguments: %s", parsed_args)

    # --- Mode Dispatch --- 
    try:
        if parsed_args.mode == "listen":
            if not parsed_args.client_id:
                parsed_args.client_id = f"mcp-mqtt-listen-{generate_id()}"
                logger.info(f"Generated Client ID: {parsed_args.client_id}")

            config = parse_config(MQTTListenerConfig, parsed_args) # Use helper
            logger.debug(f"Listener Config: {config}")
            asyncio.run(run_mqtt_listener(config))

        elif parsed_args.mode == "publish":
            logger.info("Starting in publish mode (stdio -> MQTT)")
            # Removed response_topic requirement check, handled by base_topic derivation
            
            # Prepare publisher config using helper
            config = parse_config(MQTTPublisherConfig, parsed_args)
            logger.debug(f"Publisher Config: {config}")
            # Pass only the config object
            asyncio.run(run_mqtt_publisher(config))
            
        else:
            logger.error(f"Unknown mode: {parsed_args.mode}") # Should be unreachable
            
    except KeyboardInterrupt:
        logger.info("Proxy interrupted by user.")
    except Exception as e:
        logger.exception("An unexpected error occurred: %s", e)
        sys.exit(1)
    finally:
        logger.info("Proxy finished.")


if __name__ == "__main__":
    main()
