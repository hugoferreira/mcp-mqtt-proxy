"""Configuration models and helpers for mcp-mqtt-proxy."""

import logging
import ssl
import typing as t
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Optional

import aiomqtt
# Use the correct import path found in __main__.py
from mcp.client.stdio import StdioServerParameters

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MQTTPublisherConfig:
    """Configuration specific to the MQTT Publisher mode."""
    # Non-default fields first
    broker_url: str
    client_id: str
    qos: int
    base_topic: str # Base topic for deriving request/response topics
    # Default fields last
    username: Optional[str] = None
    password: Optional[str] = None
    tls_ca_certs: Optional[str] = None
    tls_certfile: Optional[str] = None
    tls_keyfile: Optional[str] = None
    message_expiry_interval: Optional[int] = 3600 # In seconds, for MQTTv5 properties
    debug_timeout: Optional[float] = None # Optional timeout in seconds for testing


@dataclass(frozen=True)
class MQTTListenerConfig:
    """Configuration specific to the MQTT Listener mode."""
    # Non-default fields first
    broker_url: str
    client_id: str
    qos: int
    stdio_mcp_process: StdioServerParameters 
    base_topic: str # Base topic for deriving request topic
    # Default fields last
    username: Optional[str] = None
    password: Optional[str] = None
    tls_ca_certs: Optional[str] = None
    tls_certfile: Optional[str] = None
    tls_keyfile: Optional[str] = None
    mcp_timeout: float = 30.0 # Default timeout of 30 seconds for stdio server response
    debug_timeout: Optional[float] = None # Optional timeout in seconds for testing
    test_mode: bool = False # Set to True during tests to disable debug subscriptions
    disable_startup_info: bool = False # Set to True during tests to disable startup info message


def create_mqtt_client_from_config(
    config: MQTTPublisherConfig | MQTTListenerConfig, # Reverted type hint
) -> aiomqtt.Client:
    """Creates and configures an aiomqtt.Client from a config object."""
    logger.debug(f"Parsing broker URL: {config.broker_url}")
    try:
        parsed_url = urlparse(config.broker_url)
        hostname = parsed_url.hostname
        port = parsed_url.port or (8883 if parsed_url.scheme == "mqtts" else 1883)
        username = config.username or parsed_url.username
        password = config.password or parsed_url.password
        transport: t.Literal["tcp", "websockets"] = "tcp" # Default, websockets NYI

        if not hostname:
            raise ValueError("Could not extract hostname from broker URL")

        tls_params = None
        if parsed_url.scheme == "mqtts":
            logger.debug("Configuring TLS for MQTT connection.")
            tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            # Ensure CA certs are loaded if specified, otherwise use system defaults
            if config.tls_ca_certs:
                 tls_context.load_verify_locations(cafile=config.tls_ca_certs)
                 logger.debug(f"Loaded CA certs from: {config.tls_ca_certs}")
            else:
                 tls_context.load_default_certs(ssl.Purpose.SERVER_AUTH)
                 logger.debug("Loaded default system CA certs.")
            
            # Load client cert/key if provided
            if config.tls_certfile and config.tls_keyfile:
                tls_context.load_cert_chain(
                    config.tls_certfile, keyfile=config.tls_keyfile
                )
                logger.debug(f"Loaded client cert from: {config.tls_certfile}")
                logger.debug(f"Loaded client key from: {config.tls_keyfile}")
            elif config.tls_certfile:
                tls_context.load_cert_chain(config.tls_certfile)
                logger.debug(f"Loaded client cert (no key) from: {config.tls_certfile}")
            
            # Set verify mode - require certs if CA is specified or system defaults are used
            tls_context.verify_mode = ssl.CERT_REQUIRED 
            
            # Create aiomqtt.TLSParameters
            tls_params = aiomqtt.TLSParameters(
                ca_certs=config.tls_ca_certs, # Can be None if using system defaults
                certfile=config.tls_certfile,
                keyfile=config.tls_keyfile,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS_CLIENT, # Adjust if needed
                ciphers=None,
            )
            logger.debug(f"aiomqtt TLS parameters configured: {tls_params}")

    except Exception as e:
        logger.exception("Failed to parse Broker URL or configure TLS")
        raise ValueError(f"Invalid broker URL or TLS config: {e}") from e

    logger.info(f"Creating aiomqtt client for {hostname}:{port} (TLS: {bool(tls_params)})")
    return aiomqtt.Client(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
        client_id=config.client_id,
        transport=transport,
        tls_params=tls_params,
        # Using MQTTv5
        protocol=aiomqtt.ProtocolVersion.V5,
        logger=logging.getLogger("aiomqtt"), # Give aiomqtt its own logger instance
    ) 