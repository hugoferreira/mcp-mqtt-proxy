"""Mode 1: Run a stdio MCP client and publish its requests to MQTT, receiving responses via MQTT."""

import asyncio
import json
import logging
import ssl
import typing as t
from dataclasses import dataclass
from urllib.parse import urlparse

import aiomqtt
from mcp import client, server, types
from mcp.client.stdio import StdioServerParameters
from mcp.server.stdio import stdio_server

from .proxy_server import create_proxy_server

logger = logging.getLogger(__name__)


@dataclass
class MQTTPublisherConfig:
    """Configuration for the MQTT Publisher mode."""

    broker_url: str
    request_topic: str
    response_topic: str  # Topic to subscribe to for responses
    client_id: str
    qos: int
    username: str | None = None
    password: str | None = None
    tls_ca_certs: str | None = None
    tls_certfile: str | None = None
    tls_keyfile: str | None = None


# TODO: Implement proper stream bridging between MQTT (via Queue) and ClientSession
async def mqtt_read_stream() -> bytes:
    """Placeholder: Reads next message from MQTT via an internal queue."""
    logger.warning("mqtt_read_stream not fully implemented")
    await asyncio.sleep(3600)  # Prevent busy loop for now
    return b""


# TODO: Implement proper stream bridging
async def mqtt_write_stream(data: bytes) -> None:
    """Placeholder: Publishes message data to the MQTT request topic."""
    logger.warning("mqtt_write_stream not fully implemented: %s", data.decode())
    await asyncio.sleep(0) # Prevent blocking for now


async def run_mqtt_publisher(
    stdio_params: StdioServerParameters, config: MQTTPublisherConfig
) -> None:
    """Run the proxy in stdio Client -> MQTT Publisher mode."""
    logger.info(
        "Starting MQTT Publisher mode: stdio command '%s' -> MQTT broker '%s'",
        stdio_params.command,
        config.broker_url,
    )

    # --- Parse Broker URL and configure TLS ---
    try:
        parsed_url = urlparse(config.broker_url)
        hostname = parsed_url.hostname
        port = parsed_url.port or (8883 if parsed_url.scheme == "mqtts" else 1883)
        username = config.username or parsed_url.username
        password = config.password or parsed_url.password
        transport: t.Literal["tcp", "websockets"] = "tcp" # Default, websockets NYI

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
            # Create aiomqtt.TLSParameters if needed
            tls_params = aiomqtt.TLSParameters(
                ca_certs=config.tls_ca_certs,
                certfile=config.tls_certfile,
                keyfile=config.tls_keyfile,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS_CLIENT, # Adjust if needed
                ciphers=None,
            )

    except Exception as e:
        logger.exception("Failed to parse Broker URL or configure TLS: %s", e)
        return

    if not hostname:
         logger.error("Could not extract hostname from broker URL: %s", config.broker_url)
         return


    # --- Main Execution ---
    # Queue for messages coming *from* MQTT (responses) to be read by ClientSession
    incoming_message_queue: asyncio.Queue[types.Message] = asyncio.Queue()
    mqtt_listener_task = None

    try:
        async with aiomqtt.Client(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            client_id=config.client_id,
            transport=transport,
            tls_params=tls_params,
            # Using MQTTv5
            protocol=aiomqtt.ProtocolVersion.V5,
        ) as client:
            logger.info("Connected to MQTT Broker: %s", config.broker_url)

            # --- Define MQTT -> Queue Task ---
            async def mqtt_message_handler():
                logger.info("Subscribing to response topic: %s", config.response_topic)
                await client.subscribe(config.response_topic, qos=config.qos)
                try:
                    async for message in client.messages:
                        logger.debug(
                            "Received MQTT message on topic '%s'", message.topic
                        )
                        try:
                            payload_str = message.payload.decode("utf-8")
                            mcp_message_dict = json.loads(payload_str)
                            # TODO: Validate and properly parse mcp_message_dict into MCP type
                            # For now, assume it's a valid dict representing an MCP message
                            # This needs robust parsing based on message type (jsonrpc, method)
                            logger.debug("MQTT Payload Dict: %s", mcp_message_dict)
                            # Need to convert dict back to an actual MCP Message object instance
                            # This part requires knowing the message structure (Response, Notification)
                            # Example placeholder:
                            mcp_message = types.parse_message(mcp_message_dict) # mcp lib helper
                            await incoming_message_queue.put(mcp_message)

                        except json.JSONDecodeError:
                            logger.error(
                                "Failed to decode JSON from MQTT message: %s", message.payload
                            )
                        except Exception as e:
                            logger.exception(
                                "Error processing received MQTT message: %s", e
                            )
                except aiomqtt.MqttError as e:
                     logger.error("MQTT Listener Error: %s. Stopping listener.", e)
                except asyncio.CancelledError:
                    logger.info("MQTT listener task cancelled.")
                finally:
                    logger.info("MQTT listener task finished.")

            # Start the task to listen for MQTT messages
            mqtt_listener_task = asyncio.create_task(mqtt_message_handler())

            # --- Define Bridged Streams for ClientSession ---
            async def session_read_stream() -> bytes:
                """Reads next message from MQTT via the internal queue."""
                logger.debug("Waiting for message from incoming MQTT queue...")
                mcp_message = await incoming_message_queue.get()
                logger.debug("Got message from queue: %s", mcp_message)
                # Serialize MCP message object back to JSON bytes for ClientSession
                try:
                    # Assuming mcp library can dump models correctly
                    json_str = mcp_message.model_dump_json()
                    return json_str.encode("utf-8")
                except Exception as e:
                    logger.exception("Failed to serialize MCP message for ClientSession: %s", e)
                    # What to return on error? Empty bytes might break session.
                    # Maybe raise, or return a specific error indicator if possible?
                    return b'{}' # Problematic?


            async def session_write_stream(data: bytes) -> None:
                """Publishes message data from ClientSession to the MQTT request topic."""
                logger.debug("Received data from ClientSession to publish via MQTT")
                try:
                    payload_str = data.decode("utf-8")
                    # Optional: Deserialize to MCP object for validation/logging?
                    # mcp_request = types.parse_message(json.loads(payload_str))
                    # logger.debug("Publishing MCP Request: %s", mcp_request.method)

                    # Prepare MQTT v5 properties
                    properties = aiomqtt.Properties(
                        topic_alias=None, # Add other relevant properties if needed
                        response_topic=config.response_topic,
                        # correlation_data=mcp_request.id.encode('utf-8') # Assuming request has an ID
                    )

                    await client.publish(
                        config.request_topic,
                        payload=data, # Send original bytes
                        qos=config.qos,
                        properties=properties,
                    )
                    logger.debug("Published message to topic '%s'", config.request_topic)
                except Exception as e:
                    logger.exception("Failed to publish message via MQTT: %s", e)
                    # How to signal error back to ClientSession? May need specific handling.


            # --- Run the MCP Client Session ---
            logger.info("Creating MCP ClientSession with MQTT-bridged streams...")
            async with client.ClientSession(
                session_read_stream, session_write_stream
            ) as session:
                # Create the proxy server instance using the remote session's capabilities
                app: server.Server[object] = await create_proxy_server(session)
                logger.info(
                    "Proxy server configured based on remote capabilities. Starting stdio server..."
                )

                # Run the actual stdio server, bridging it to the proxy app
                # The proxy app internally uses the MQTT-bridged ClientSession
                async with stdio_server() as (stdio_read, stdio_write):
                    logger.info("Running proxy app against stdio streams.")
                    await app.run(
                        stdio_read,
                        stdio_write,
                        app.create_initialization_options(),
                    )
                    logger.info("Proxy app run finished.")

    except aiomqtt.MqttError as error:
        logger.error(f"MQTT connection error: {error}")
    except asyncio.CancelledError:
         logger.info("Main publisher task cancelled.")
    except Exception as e:
        logger.exception("An unexpected error occurred in run_mqtt_publisher: %s", e)
    finally:
        if mqtt_listener_task and not mqtt_listener_task.done():
            logger.info("Cancelling MQTT listener task...")
            mqtt_listener_task.cancel()
            await asyncio.wait([mqtt_listener_task], timeout=2) # Wait briefly for cleanup
        logger.info("MQTT Publisher mode finished.")
