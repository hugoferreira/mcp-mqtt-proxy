"""Mode 1: Run a stdio MCP client and publish its requests to MQTT, receiving responses via MQTT."""

import asyncio
import json
import logging
import math
import ssl
import subprocess # Needed for starting process
import sys # Added import for sys.stdin/sys.stdout
import typing as t
from dataclasses import dataclass
from urllib.parse import urlparse
import uuid
from typing import AsyncGenerator, Optional

import aiomqtt
import anyio
import paho.mqtt.client as paho_mqtt_client
import paho.mqtt.properties as paho_mqtt_properties
import paho.mqtt.packettypes as paho_mqtt_packettypes
from mcp import McpError
from mcp.client.session import ClientSession # Needed for instantiation
# Removed: from mcp.client.stdio import connect_subprocess
import pydantic
from pydantic import ValidationError

# Import specific types needed
from mcp.types import (
    Notification,
    CallToolResult,
    TextContent,
    JSONRPCMessage,
    JSONRPCRequest,
)

from mcp_mqtt_proxy.config import MQTTPublisherConfig, create_mqtt_client_from_config
# Import the bridge utility
from .utils import bridge_stdio_to_anyio_session

logger = logging.getLogger(__name__)


# Helper to parse incoming MQTT messages
def _parse_mcp_message_from_json(payload: bytes) -> CallToolResult | Notification | None:
    """Attempts to parse bytes (assumed JSON) into an MCP CallToolResult or Notification."""
    try:
        json_str = payload.decode('utf-8')
        data = json.loads(json_str)
        message_id = data.get('id') # Relevant for responses

        # Heuristic to determine message type based on common MCP fields
        if 'id' in data:
            logger.debug(f"Attempting to parse as CallToolResult (ID: {message_id})")
            try:
                result = CallToolResult.model_validate(data)
                if result.isError:
                     logger.warning(f"Parsed CallToolResult indicates an error (ID: {message_id}): {result.content}")
                else:
                     logger.debug(f"Successfully parsed CallToolResult (ID: {message_id})")
                return result
            except ValidationError as e:
                 logger.warning(f"Failed to validate payload as CallToolResult (ID: {message_id}): {e}")
                 logger.debug(f"Payload data: {data}")
                 return None

        elif 'method' in data:
            logger.debug(f"Attempting to parse as Notification (Method: {data.get('method')})")
            try:
                notification = Notification.model_validate(data)
                logger.debug(f"Successfully parsed Notification (Method: {notification.method})")
                return notification
            except ValidationError as e:
                 logger.warning(f"Failed to validate payload as Notification (Method: {data.get('method')}): {e}")
                 logger.debug(f"Payload data: {data}")
                 return None
        else:
            logger.warning(f"Received message with unrecognized structure via MQTT (ID: {message_id}, Keys: {list(data.keys())}): {data if len(str(data)) < 200 else str(data)[:200] + '...'}")
            return None

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning(f"Failed to decode MQTT payload as JSON: {e}")
        logger.debug(f"Payload bytes: {payload!r}")
        return None
    except Exception:
        logger.exception("Unexpected error parsing MCP message from JSON")
        return None


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


async def run_mqtt_publisher(config: MQTTPublisherConfig):
    """
    Main execution function for the MQTT publisher mode.
    Connects to MQTT, reads MCP messages from its own stdin, publishes them,
    listens for responses on MQTT, and writes responses back to its own stdout.
    
    If config.debug_timeout is set, the publisher will automatically exit after that many seconds,
    which is useful for testing.
    """
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    # Setup debug timeout if specified
    debug_timeout_task = None
    if config.debug_timeout is not None and config.debug_timeout > 0:
        logger.info(f"Debug timeout set: will exit after {config.debug_timeout} seconds")
        
        async def trigger_timeout_shutdown():
            await asyncio.sleep(config.debug_timeout)
            logger.info(f"Debug timeout of {config.debug_timeout} seconds reached, shutting down")
            shutdown_event.set()
            
        debug_timeout_task = asyncio.create_task(trigger_timeout_shutdown(), name="debug-timeout")
    
    # --- Get Asyncio Streams for Own Stdin/Stdout ---
    try:
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        writer_transport, writer_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
        writer = asyncio.StreamWriter(writer_transport, writer_protocol, reader, loop)
        logger.info("Successfully opened asyncio streams for stdin/stdout.")
    except Exception as e:
        logger.exception(f"Failed to open stdin/stdout streams: {e}")
        return

    # --- Connect to MQTT Broker using async with --- 
    try:
        async with create_mqtt_client_from_config(config) as mqtt_client:
            logger.info(f"Connected to MQTT broker at {config.broker_url}")

            # --- Prepare MQTT Response Handling (inside async with) ---
            response_topic_to_subscribe = f"{config.base_topic}/response/{config.client_id}"
            logger.info(f"Subscribing to publisher response topic: {response_topic_to_subscribe}")
            await mqtt_client.subscribe(response_topic_to_subscribe, qos=config.qos)

            incoming_message_queue: asyncio.Queue[CallToolResult | Notification] = asyncio.Queue()

            async def mqtt_message_handler() -> None:
                # Handles incoming MQTT messages (responses)
                try: 
                    async with mqtt_client.messages() as messages:
                        async for message in messages:
                            logger.debug(f"MQTT Handler: Received message on '{message.topic}'")
                            try:
                                mcp_message = _parse_mcp_message_from_json(message.payload)
                                if mcp_message:
                                    await incoming_message_queue.put(mcp_message)
                                else:
                                    logger.warning("MQTT Handler: Failed to parse MCP message from MQTT")
                            except Exception:
                                logger.exception("MQTT Handler: Error processing individual message")
                except asyncio.CancelledError:
                     logger.info("MQTT message handler cancelled.")
                     # Do not re-raise cancellation here, let main loop handle shutdown
                except Exception:
                     logger.exception("MQTT Handler: Unhandled error in message loop")
                     shutdown_event.set() # Signal shutdown on unexpected error
                finally:
                     logger.info("MQTT Handler: Exiting.")

            handler_task = asyncio.create_task(mqtt_message_handler(), name="mqtt-handler")

            # --- MQTT Publishing Function (needs access to mqtt_client) ---
            async def publish_request_to_mqtt(message: JSONRPCRequest) -> None:
                # Publishes outgoing requests to MQTT
                request_topic_to_publish = f"{config.base_topic}/request"
                logger.debug(f"Publishing: {message.method} (ID: {message.id}) to '{request_topic_to_publish}'")
                try:
                    payload = message.model_dump_json().encode('utf-8')
                except Exception as e:
                    logger.exception(f"Publishing: Failed to serialize MCP message: {e}")
                    return

                fixed_response_topic = f"{config.base_topic}/response/{config.client_id}"
                properties = paho_mqtt_properties.Properties(paho_mqtt_packettypes.PacketTypes.PUBLISH)
                properties.MessageExpiryInterval = config.message_expiry_interval or 0
                properties.ResponseTopic = fixed_response_topic
                properties.CorrelationData = str(message.id).encode('utf-8')
                try:
                    await mqtt_client.publish(request_topic_to_publish, payload, qos=config.qos, properties=properties)
                except Exception as e:
                    logger.exception(f"Publishing: Failed to publish MQTT request: {e}")
                    # Consider signaling shutdown if publishing fails critically

            # --- Bridge Tasks (using asyncio streams) --- 
            async def bridge_stdin_to_mqtt() -> None:
                """Task to read requests from stdin and publish to MQTT."""
                logger.info("STDIN->MQTT Bridge: Starting to listen for messages from stdin...")
                try:
                    while True:
                        # Read line-by-line from the asyncio StreamReader
                        line_bytes = await reader.readline()
                        if not line_bytes: 
                            logger.info("STDIN->MQTT Bridge: Stdin closed (EOF).")
                            break # End of stream
                        try:
                             json_str = line_bytes.decode('utf-8').strip()
                             if not json_str: continue # Skip empty lines
                             mcp_request = JSONRPCRequest.model_validate_json(json_str)
                             logger.info(f"STDIN->MQTT Bridge: Relaying message from stdin: Method {mcp_request.method}, ID {mcp_request.id}")
                             await publish_request_to_mqtt(mcp_request)
                        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as parse_err:
                             logger.warning(f"STDIN->MQTT Bridge: Failed to parse line from stdin: {parse_err}. Line: {line_bytes!r}")
                        except Exception as e:
                             logger.exception(f"STDIN->MQTT Bridge: Error processing line from stdin: {e}")
                except asyncio.CancelledError:
                     logger.info("STDIN->MQTT bridge cancelled.")
                except Exception:
                    logger.exception("Error in STDIN->MQTT bridge task")
                    shutdown_event.set() # Signal shutdown
                finally:
                    logger.info("STDIN->MQTT Bridge: Exiting.")
                    shutdown_event.set() # Ensure shutdown if this task exits

            async def bridge_mqtt_to_stdout() -> None:
                """Task to read responses from MQTT queue and send to stdout."""
                logger.info("MQTT->STDOUT Bridge: Starting to listen for messages from MQTT queue...")
                try:
                    while True:
                        message_from_queue = await incoming_message_queue.get()
                        msg_type = type(message_from_queue).__name__
                        msg_id = getattr(message_from_queue, 'id', None)
                        id_str = f" (ID: {msg_id})" if msg_id else ""
                        logger.info(f"MQTT->STDOUT Bridge: Relaying {msg_type}{id_str} to stdout")
                        try:
                            # Serialize message and write to asyncio StreamWriter
                            json_payload = message_from_queue.model_dump_json()
                            writer.write((json_payload + '\n').encode('utf-8'))
                            await writer.drain()
                        except (ValidationError, TypeError) as serialize_err:
                            logger.error(f"MQTT->STDOUT Bridge: Failed to serialize message for stdout: {serialize_err}")
                        except ConnectionError as e:
                            logger.error(f"MQTT->STDOUT Bridge: Connection error writing to stdout: {e}")
                            break # Stop if stdout is closed
                        except Exception:
                             logger.exception("MQTT->STDOUT Bridge: Unexpected error sending to stdout")
                        finally:
                             incoming_message_queue.task_done()
                except asyncio.CancelledError:
                    logger.info("MQTT->STDOUT bridge cancelled.")
                except Exception:
                    logger.exception("Error in MQTT->STDOUT bridge task")
                    shutdown_event.set()
                finally:
                    logger.info("MQTT->STDOUT Bridge: Exiting.")
                    shutdown_event.set() # Ensure shutdown if this task exits

            # --- Start Tasks and Wait (inside async with mqtt_client) --- 
            stdin_to_mqtt_task = asyncio.create_task(bridge_stdin_to_mqtt(), name="stdin-to-mqtt")
            mqtt_to_stdout_task = asyncio.create_task(bridge_mqtt_to_stdout(), name="mqtt-to-stdout")
            
            # Monitor tasks and shutdown event
            tasks_to_monitor = {handler_task, stdin_to_mqtt_task, mqtt_to_stdout_task}
            await shutdown_event.wait() # Wait until a task signals shutdown or an error occurs

            logger.info("Shutdown signalled. Cancelling tasks...")
            
    except asyncio.CancelledError:
        logger.info("Publisher run task cancelled.")
    except Exception as e:
        logger.exception(f"Publisher encountered critical error: {e}")
    finally:
        logger.info("Shutting down MQTT publisher...")
        # Cancel debug timeout task if it exists
        if debug_timeout_task and not debug_timeout_task.done():
            debug_timeout_task.cancel()
            try:
                await debug_timeout_task
            except asyncio.CancelledError:
                pass
                
        # Cancel all running tasks gracefully
        all_tasks = [handler_task, stdin_to_mqtt_task, mqtt_to_stdout_task]
        for task in all_tasks:
            if task and not task.done():
                try:
                    task.cancel()
                except Exception as e:
                     logger.warning(f"Error cancelling task {task.get_name()}: {e}")
        # Wait for tasks to finish cancelling
        await asyncio.gather(*[t for t in all_tasks if t and not t.done()], return_exceptions=True)
        
        # MQTT client disconnect happens implicitly via async with
        logger.info("MQTT publisher shut down.")
