"""Mode 2: Listen for MCP requests on MQTT and proxy them to a local stdio MCP server."""

import asyncio
import json
import logging
import ssl
import typing as t
from dataclasses import dataclass
from urllib.parse import urlparse

import aiomqtt
from mcp import client, server, types
from mcp.client.stdio import StdioServerParameters, stdio_client

from .proxy_server import create_proxy_server

logger = logging.getLogger(__name__)


@dataclass
class MQTTListenerConfig:
    """Configuration for the MQTT Listener mode."""

    broker_url: str
    request_topic: str # Topic to subscribe to for requests
    # Response topic might be determined by MQTT v5 properties, or a base can be configured
    response_topic_base: str | None = None
    client_id: str
    qos: int # For publishing responses
    username: str | None = None
    password: str | None = None
    tls_ca_certs: str | None = None
    tls_certfile: str | None = None
    tls_keyfile: str | None = None


async def run_mqtt_listener(
    stdio_params: StdioServerParameters, config: MQTTListenerConfig
) -> None:
    """Run the proxy in MQTT Listener -> stdio Server mode."""
    logger.info(
        "Starting MQTT Listener mode: MQTT broker '%s' (topic '%s') -> stdio command '%s'",
        config.broker_url,
        config.request_topic,
        stdio_params.command,
    )

    # --- Parse Broker URL and configure TLS (similar to publisher) ---
    try:
        parsed_url = urlparse(config.broker_url)
        hostname = parsed_url.hostname
        port = parsed_url.port or (8883 if parsed_url.scheme == "mqtts" else 1883)
        username = config.username or parsed_url.username
        password = config.password or parsed_url.password
        transport: t.Literal["tcp", "websockets"] = "tcp"

        tls_params = None
        if parsed_url.scheme == "mqtts":
            tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            tls_context.load_verify_locations(cafile=config.tls_ca_certs)
            if config.tls_certfile and config.tls_keyfile:
                tls_context.load_cert_chain(
                    config.tls_certfile, keyfile=config.tls_keyfile
                )
            elif config.tls_certfile:
                tls_context.load_cert_chain(config.tls_certfile)
            tls_params = aiomqtt.TLSParameters(
                ca_certs=config.tls_ca_certs,
                certfile=config.tls_certfile,
                keyfile=config.tls_keyfile,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
                ciphers=None,
            )
    except Exception as e:
        logger.exception("Failed to parse Broker URL or configure TLS: %s", e)
        return

    if not hostname:
        logger.error("Could not extract hostname from broker URL: %s", config.broker_url)
        return

    # --- Main Execution ---
    # This task will handle notifications from stdio_session to MQTT
    notification_handler_task = None

    try:
        # Establish a client session with the local stdio process
        logger.info("Starting local stdio MCP process: %s", stdio_params.command)
        async with stdio_client(stdio_params) as stdio_streams, client.ClientSession(
            *stdio_streams
        ) as stdio_session:
            logger.info("Local stdio MCP session established.")

            # Create the proxy server instance using the local stdio session's capabilities
            # This mcp_server object will have handlers for various MCP requests.
            mcp_server: server.Server[object] = await create_proxy_server(stdio_session)
            logger.info("MCP Server logic initialized, configured by stdio process capabilities.")

            # --- Handle Notifications from stdio_session to MQTT ---
            # TODO: This needs to be implemented. Listen to stdio_session.notifications()
            # and publish them to an appropriate MQTT topic.
            # This topic might be derived from config or a convention.

            # async def handle_stdio_notifications():
            #     async for notification in stdio_session.notifications():
            #         logger.debug("Received notification from stdio: %s", notification)
            #         # Serialize and publish to MQTT
            #         # ... client.publish( ... )
            # notification_handler_task = asyncio.create_task(handle_stdio_notifications())
            logger.warning("Notification handling from stdio to MQTT is not yet implemented.")


            # Connect to MQTT broker
            async with aiomqtt.Client(
                hostname=hostname,
                port=port,
                username=username,
                password=password,
                client_id=config.client_id,
                transport=transport,
                tls_params=tls_params,
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as mqtt_client:
                logger.info("Connected to MQTT Broker: %s", config.broker_url)
                logger.info("Subscribing to request topic: %s", config.request_topic)
                await mqtt_client.subscribe(config.request_topic, qos=config.qos)

                # Process incoming MQTT messages (requests)
                try:
                    async for message in mqtt_client.messages:
                        logger.debug(
                            "Received MQTT request on topic '%s'", message.topic
                        )
                        try:
                            payload_str = message.payload.decode("utf-8")
                            mcp_request_dict = json.loads(payload_str)
                            mcp_request_obj = types.parse_message(mcp_request_dict)

                            logger.debug("Parsed MCP Request: %s (ID: %s)", mcp_request_obj.method, mcp_request_obj.id)

                            # --- Dispatch the request to the mcp_server --- 
                            # The mcp_server (from create_proxy_server) internally calls stdio_session methods.
                            # We need to find the correct handler on mcp_server and call it.
                            # The `mcp.server.Server` class has `process_request` which might be usable if
                            # we construct a full JSON-RPC request string, or we might need to manually dispatch.

                            # For now, let's assume a manual dispatch based on request type:
                            handler = mcp_server.request_handlers.get(type(mcp_request_obj))
                            
                            mcp_response_obj = None
                            if handler:
                                try:
                                    server_result: types.ServerResult = await handler(mcp_request_obj)
                                    mcp_response_obj = server_result.result # This is the actual response part
                                    logger.debug("Handler executed, got response: %s", mcp_response_obj)
                                except Exception as e:
                                    logger.exception("Error executing MCP request handler for %s: %s", mcp_request_obj.method, e)
                                    # TODO: Create an MCP error response
                                    pass # Fall through to publish if error response created
                            else:
                                logger.error(
                                    "No handler found for MCP request type: %s", type(mcp_request_obj)
                                )
                                # TODO: Create an MCP error response (method not found)

                            # --- Publish the response --- 
                            if mcp_response_obj and message.properties and message.properties.response_topic:
                                response_payload_str = mcp_response_obj.model_dump_json()
                                response_properties = aiomqtt.Properties(
                                    correlation_data=message.properties.correlation_data
                                )
                                await mqtt_client.publish(
                                    topic=message.properties.response_topic,
                                    payload=response_payload_str.encode("utf-8"),
                                    qos=config.qos,
                                    properties=response_properties,
                                )
                                logger.debug(
                                    "Published MCP response to '%s' with correlation data.", 
                                    message.properties.response_topic
                                )
                            elif mcp_response_obj:
                                logger.warning(
                                    "MCP response generated but no response_topic in MQTT message properties. Cannot publish response for request ID %s.",
                                    mcp_request_obj.id
                                )
                            else:
                                logger.debug("No MCP response generated for request ID %s.", mcp_request_obj.id)

                        except json.JSONDecodeError:
                            logger.error(
                                "Failed to decode JSON from MQTT request: %s", message.payload
                            )
                        except Exception as e:
                            logger.exception(
                                "Error processing received MQTT request: %s", e
                            )
                except aiomqtt.MqttError as e:
                    logger.error("MQTT Listener Error: %s. Stopping listener.", e)
                except asyncio.CancelledError:
                    logger.info("MQTT message processing loop cancelled.")

    except aiomqtt.MqttError as error:
        logger.error(f"MQTT connection error: {error}")
    except client.StdioConnectionError as e:
        logger.error(f"Failed to start or connect to stdio process: {e}")
    except asyncio.CancelledError:
        logger.info("Main listener task cancelled.")
    except Exception as e:
        logger.exception("An unexpected error occurred in run_mqtt_listener: %s", e)
    finally:
        if notification_handler_task and not notification_handler_task.done():
            logger.info("Cancelling stdio notification handler task...")
            notification_handler_task.cancel()
            await asyncio.wait([notification_handler_task], timeout=2)
        logger.info("MQTT Listener mode finished.")
