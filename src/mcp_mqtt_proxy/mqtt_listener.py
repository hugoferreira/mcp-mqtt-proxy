"""Mode 2: Listen for MCP requests on MQTT and proxy them to a local stdio MCP server."""

import asyncio
import json
import logging
import ssl
import typing as t
from dataclasses import dataclass
from urllib.parse import urlparse
import sys
import subprocess
import math # For infinity stream buffer size
from contextlib import asynccontextmanager # For bridge context manager
from datetime import timedelta # For ClientSession timeout example
import time

import aiomqtt
import paho.mqtt.client as paho_mqtt_client # For PacketTypes constant and MessageType
import paho.mqtt.properties as mqtt_properties # Import Properties class
import paho.mqtt.packettypes as packettypes # Import packet types constants
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.stapled import StapledObjectStream

# Import MCP client components
from mcp.client.session import ClientSession
from pydantic import BaseModel, ValidationError
from mcp import McpError
from mcp.shared.message import SessionMessage # Used by the stream bridge

# Import MCP type definitions needed for JSONRPC messages
from mcp.types import (
    JSONRPCRequest, JSONRPCResponse, JSONRPCError, ErrorData, Implementation,
    ListPromptsRequest, ListPromptsResult, Prompt, Request,
)
# Create alias for JSONRPCResponse with error as JSONRPCErrorResponse for readability
JSONRPCErrorResponse = JSONRPCResponse

from mcp_mqtt_proxy.config import MQTTListenerConfig, StdioServerParameters, create_mqtt_client_from_config
from mcp_mqtt_proxy.utils import generate_id

logger = logging.getLogger(__name__)

# Helper to parse incoming MQTT messages
def _parse_mcp_message_from_json(payload: bytes) -> Request | None:
    """Attempts to parse the MQTT payload bytes as a Request JSON object."""
    try:
        # First try to parse as JSON
        payload_str = payload.decode('utf-8')
        data = json.loads(payload_str)
        
        # Check if this has the basic JSON-RPC structure
        if not isinstance(data, dict):
            logger.error(f"JSON payload is not a dictionary: {data}")
            return None
            
        # Extract required fields from JSON directly to avoid validation errors
        if "jsonrpc" in data and "method" in data and "id" in data:
            # Create a Request object directly
            return Request(
                jsonrpc=data.get("jsonrpc", "2.0"),
                id=data.get("id"),
                method=data.get("method")
            )
        
        # If direct extraction fails, try the full validation
        try:
            # Validate against the base Request model
            request = Request.model_validate_json(payload)
            return request
        except Exception as inner_e:
            logger.warning(f"Pydantic validation failed after basic JSON parsing succeeded: {inner_e}")
            
        # If both approaches fail but we have method and id, we might still be able to proceed
        if "method" in data and "id" in data:
            logger.warning(f"Creating Request with minimal fields: method={data['method']}, id={data['id']}")
            return Request(
                jsonrpc="2.0",  # Assume 2.0 if not specified
                id=data["id"],
                method=data["method"]
            )
            
        logger.error(f"JSON does not have required JSON-RPC fields (jsonrpc, method, id): {data}")
        return None
    except Exception as e:
        # Use PydanticCustomError for better context if possible, otherwise general Exception
        logger.error(f"Failed to parse JSON payload into MCP Request: {e}")
        # Consider how to handle malformed JSON. Return None for now.
        return None


# --- Stream Bridging ---

@asynccontextmanager
async def bridge_stdio_to_anyio_session(
    proc_reader: asyncio.StreamReader,
    proc_writer: asyncio.StreamWriter,
) -> t.AsyncGenerator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ],
    None,
]:
    """Bridges asyncio stdio streams to AnyIO memory streams for ClientSession."""
    send_to_session: MemoryObjectSendStream[SessionMessage | Exception]
    receive_from_process: MemoryObjectReceiveStream[SessionMessage | Exception]
    send_to_process: MemoryObjectSendStream[SessionMessage]
    receive_from_session: MemoryObjectReceiveStream[SessionMessage]

    # Create AnyIO streams for the session side
    send_to_session, receive_from_process = anyio.create_memory_object_stream(math.inf)
    # Create AnyIO streams for the process writing side
    send_to_process, receive_from_session = anyio.create_memory_object_stream(math.inf)

    loop = asyncio.get_running_loop()
    read_task: asyncio.Task | None = None
    write_task: asyncio.Task | None = None

    async def _read_from_process() -> None:
        """Task: Read lines from proc_reader, decode, wrap, send to AnyIO."""
        try:
            while True:
                line_bytes = await proc_reader.readline()
                if not line_bytes:
                    logger.info("Process stdout closed.")
                    break # EOF
                line = line_bytes.decode('utf-8').strip()
                if not line:
                    continue # Ignore empty lines
                try:
                    # Assume JSONRPCMessage covers all incoming possibilities from server
                    msg = JSONRPCMessage.model_validate_json(line)
                    logger.debug(f"Read from process: {type(msg).__name__} - {line[:100]}")
                    await send_to_session.send(SessionMessage(message=msg))
                except (json.JSONDecodeError, ValidationError) as e:
                    logger.error(f"Failed to parse JSON from process stdout: {e}\nLine: '{line}'")
                    # Send the error to the session? Or just log?
                    await send_to_session.send(e) # Propagate error
                except Exception as e:
                     logger.exception("Error in read_from_process task")
                     await send_to_session.send(e) # Propagate error
                     break # Exit task on unexpected error
        except asyncio.CancelledError:
            logger.info("Read from process task cancelled.")
        except Exception as e:
            logger.exception("Unhandled exception in read_from_process task")
            # Try sending exception before exiting
            try:
                await send_to_session.send(e)
            except Exception:
                pass # Ignore if sending fails
        finally:
            logger.debug("Closing send_to_session stream from read_from_process task.")
            await send_to_session.aclose()


    async def _write_to_process() -> None:
        """Task: Receive SessionMessage from AnyIO, encode, write lines to proc_writer."""
        try:
            async for session_msg in receive_from_session:
                try:
                    # Log based on available attributes
                    msg_info = "Unknown Message"
                    if hasattr(session_msg.message, 'method'):
                        msg_info = f"Method: {session_msg.message.method}"
                    elif hasattr(session_msg.message, 'id'):
                        msg_info = f"ID: {session_msg.message.id}"
                    logger.debug(f"Writing to process: {type(session_msg.message).__name__} ({msg_info})")
                    proc_writer.write(session_msg.message.model_dump_json().encode('utf-8') + b'\n')
                    await proc_writer.drain()
                except McpError as e:
                    logger.error(f"Failed to write message to process stdin: {e}")
                    break # Exit task on write error
        except asyncio.CancelledError:
             logger.info("Write to process task cancelled.")
        except Exception:
            logger.exception("Unhandled exception in write_to_process task")
        finally:
            logger.debug("Closing proc_writer stream from write_to_process task.")
            # Ensure writer is closed if task exits
            if not proc_writer.is_closing():
                try:
                    proc_writer.close()
                    await proc_writer.wait_closed()
                except Exception as e:
                     logger.warning(f"Error closing proc_writer: {e}")


    try:
        read_task = loop.create_task(_read_from_process(), name="read_from_process")
        write_task = loop.create_task(_write_to_process(), name="write_to_process")
        # Yield the streams the ClientSession needs to use
        yield receive_from_process, send_to_process
    finally:
        logger.info("Cleaning up stream bridge...")
        # Clean up tasks
        if write_task and not write_task.done():
            write_task.cancel()
            try:
                await write_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error during write_task cleanup")
        if read_task and not read_task.done():
            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass
            except Exception:
                 logger.exception("Error during read_task cleanup")

        # Ensure streams are closed (tasks should handle send sides, check receive sides)
        logger.debug("Closing receive_from_session stream.")
        await receive_from_session.aclose()
        logger.debug("Closing receive_from_process stream.")
        await receive_from_process.aclose()
        logger.info("Stream bridge cleanup complete.")


# --- MQTT Message Handling Helper ---

async def _handle_mqtt_message(
    message: aiomqtt.Message,
    mqtt_client: aiomqtt.Client,
    session: ClientSession,
    config: MQTTListenerConfig
) -> None:
    """Handles a single incoming MQTT message, forwards to MCP, sends response."""
    response_topic: str | None = None
    correlation_data: bytes | None = None
    mcp_request_base: Request | None = None
    mcp_response_data: BaseModel | ErrorData | JSONRPCError | None = None  # Use BaseModel as generic type for results
    request_id_for_response: str | int | None = "unknown"  # Keep track of ID for error cases

    logger.info(f"Handling MQTT message on topic: {message.topic}")
    try:
        payload_str = message.payload.decode('utf-8')
        logger.info(f"Message payload: {payload_str[:200]}")
    except Exception as e:
        logger.error(f"Failed to decode message payload: {e}")
        return
        
    # Outer try block for the whole message handling process
    try:
        # --- Extract MQTTv5 Properties ---
        logger.debug(f"Message properties: {message.properties}")
        if message.properties and message.properties.ResponseTopic:
            response_topic = message.properties.ResponseTopic
            logger.debug(f"Found response topic in properties: {response_topic}")
        if message.properties and message.properties.CorrelationData:
            correlation_data = message.properties.CorrelationData
            logger.debug(f"Found correlation data in properties: {correlation_data}")

        # --- Parse MCP Request ---
        try:
            # Parse JSON-RPC request from message payload
            payload = message.payload.decode()
            logger.info(f"Parsing MCP request from payload: {payload[:200]}...")
            
            # First try to extract just the basics to get the message ID and method
            mcp_request_base = _parse_mcp_message_from_json(message.payload)
            if not mcp_request_base:
                logger.error("Failed to parse MQTT payload as an MCP request")
                return
                
            logger.info(f"Successfully parsed request ID: {mcp_request_base.id}, Method: {mcp_request_base.method}")
            request_id_for_response = mcp_request_base.id  # Store ID for response message

            # --- Specific Method Dispatch --- 
            if mcp_request_base.method == "prompts/list":
                try:
                    logger.info("Processing prompts/list request")
                    list_prompts_req = ListPromptsRequest.model_validate_json(message.payload)
                    logger.debug(f"Dispatching as prompts/list (ID: {list_prompts_req.id})")
                    # Call the session's list_prompts method and await the result
                    mcp_response_data = await session.list_prompts()
                    logger.info(f"Got prompts/list response: {mcp_response_data}")
                except ValidationError as e:
                     logger.error(f"Validation failed for prompts/list params: {e}")
                     mcp_response_data = ErrorData(code=-32602, message=f"Invalid params for prompts/list: {e}")
                except Exception as e:
                     logger.exception(f"Error handling prompts/list: {e}")
                     mcp_response_data = ErrorData(code=-32603, message=f"Internal error in prompts/list handler: {e}")
            # Add support for test/echo method
            elif mcp_request_base.method == "test/echo":
                try:
                    # Parse the request payload to extract the message parameter
                    request_data = json.loads(message.payload)
                    params = request_data.get("params", {})
                    message_param = params.get("message", "")
                    
                    # Create a response with the echoed message (using a dict wrapped in a model)
                    class EchoResponse(BaseModel):
                        message: str
                    
                    mcp_response_data = EchoResponse(message=f"Echo: {message_param}")
                    logger.info(f"Handling test/echo (ID: {mcp_request_base.id}): {message_param}")
                    logger.info(f"Response data: {mcp_response_data.model_dump()}")
                except Exception as e:
                    logger.exception(f"Error handling test/echo: {e}")
                    mcp_response_data = ErrorData(code=-32603, message=f"Internal error in echo handler: {e}")
            else:
                logger.info(f"Attempting to relay unknown method '{mcp_request_base.method}' to MCP server")
                # For other methods, pass through to the MCP server
                try:
                    logger.info(f"Sending request to MCP server via session: {payload[:200]}...")
                    # Send request to the MCP server
                    response = await session.send_request(request_json=payload)
                    logger.info(f"Received response from MCP server: {response}")
                    # Use the raw response directly
                    if isinstance(response, (dict, list, str, int, float, bool)):
                        result_model = {"result": response}
                        mcp_response_data = BaseModel.model_validate(result_model)
                    else:
                        mcp_response_data = response
                except Exception as e:
                    logger.exception(f"Error sending request to MCP server: {e}")
                    mcp_response_data = ErrorData(code=-32603, message=f"Internal server error: {e}")
        except ValidationError as e:
            logger.error(f"Validation error parsing request: {e}")
            mcp_response_data = ErrorData(code=-32600, message=f"Invalid request: {e}")
        except Exception as e:
            logger.exception(f"Error handling MCP request: {e}")
            mcp_response_data = ErrorData(code=-32603, message=f"Internal error: {e}")

        # --- Send Response Message ---
        if response_topic:
            # Format the response as a JSON-RPC response
            if isinstance(mcp_response_data, (BaseModel, dict)):
                # For regular success responses
                if isinstance(mcp_response_data, BaseModel):
                    # Convert the result Pydantic model to dict for JSON-RPC result
                    result_dict = mcp_response_data.model_dump(mode="json")
                else:
                    # If it's already a dict, use as is (from direct echo/other methods)
                    result_dict = mcp_response_data

                # Build the JSONRPC response object
                response_obj = JSONRPCResponse(
                    jsonrpc="2.0",
                    id=request_id_for_response,
                    result=result_dict
                )
            elif isinstance(mcp_response_data, (ErrorData, JSONRPCError)):
                # For error responses
                if isinstance(mcp_response_data, ErrorData):
                    # Convert ErrorData to a JSONRPCError
                    error = JSONRPCError(code=mcp_response_data.code, message=mcp_response_data.message)
                else:
                    # Already a JSONRPCError
                    error = mcp_response_data

                # Build an error response object
                response_obj = JSONRPCErrorResponse(jsonrpc="2.0", id=request_id_for_response, error=error)
            else:
                logger.warning(f"Unexpected response data type: {type(mcp_response_data)}")
                response_obj = JSONRPCErrorResponse(
                    jsonrpc="2.0",
                    id=request_id_for_response,
                    error=JSONRPCError(
                        code=-32603,
                        message=f"Internal error: Unexpected response type {type(mcp_response_data)}"
                    )
                )

            # Create MQTT v5 Properties for the response (including correlation id)
            # Use mqtt_properties instead of paho_mqtt_client.Properties
            mqtt_props = mqtt_properties.Properties(packettypes.PacketTypes.PUBLISH)
            if correlation_data:
                mqtt_props.CorrelationData = correlation_data  # Set correlation data if provided

            # Serialize response to JSON
            response_json = response_obj.model_dump_json()
            logger.info(f"Sending MQTT response to topic {response_topic}: {response_json}")

            # Publish the response
            try:
                await mqtt_client.publish(
                    response_topic,
                    payload=response_json.encode(),
                    qos=config.qos,
                    properties=mqtt_props
                )
                logger.info(f"Response published successfully to {response_topic}")
            except Exception as e:
                logger.exception(f"Error publishing response: {e}")

    except Exception as e:
        logger.exception(f"Unexpected error in MQTT message handler: {e}")
        # Try to send an error response if possible
        try:
            error_response = JSONRPCErrorResponse(
                jsonrpc="2.0",
                id=request_id_for_response,
                error=ErrorData(code=-32603, message=f"Internal error in MQTT proxy: {e}")
            )
            error_json = error_response.model_dump_json()
            
            # If we know where to send the response, try to send it
            if response_topic:
                logger.warning(f"Sending error response to {response_topic}: {error_json}")
                await mqtt_client.publish(
                    response_topic,
                    payload=error_json.encode(),
                    qos=config.qos
                )
        except Exception as inner_e:
            logger.exception(f"Failed to send error response: {inner_e}")


async def run_mqtt_listener(
    config: MQTTListenerConfig,
):
    """
    Listens for MCP requests on an MQTT topic, launches a local stdio MCP server process,
    and forwards requests to it, then publishes responses back to MQTT.
    """
    mqtt_client: t.Optional[aiomqtt.Client] = None
    session: t.Optional[ClientSession] = None
    server_proc: t.Optional[asyncio.subprocess.Process] = None # Store the process
    session_run_task: asyncio.Task | None = None # Task for session.run()

    try:
        # --- Start Stdio MCP Process ---
        logger.info(
            f"Launching stdio server: {config.stdio_mcp_process.command} {' '.join(config.stdio_mcp_process.args)}"
        )
        full_command = [config.stdio_mcp_process.command] + config.stdio_mcp_process.args
        server_proc = await asyncio.create_subprocess_exec(
            *full_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, # Capture stderr for debugging
            cwd=config.stdio_mcp_process.cwd,
            env=config.stdio_mcp_process.env,
        )
        logger.info(f"Stdio server process started (PID: {server_proc.pid})")

        # Check if the process started successfully (give it a moment)
        await asyncio.sleep(0.2) # Small delay
        # Log any initial stderr output
        initial_stderr = b""
        if server_proc and server_proc.stderr:
            try:
                # Use readexactly with a small amount and timeout, or read_nowait?
                # Let's try reading whatever is available without blocking indefinitely.
                 initial_stderr = await server_proc.stderr.read(2048) # Read up to 2KB
                 if initial_stderr:
                     logger.warning(f"Initial server process stderr (PID: {server_proc.pid}):\n{initial_stderr.decode(errors='ignore')}")
            except Exception as e:
                logger.warning(f"Error reading initial stderr from server process: {e}")

        if server_proc.returncode is not None:
            # Use the already read initial_stderr
            raise RuntimeError(f"Server process failed to start (code: {server_proc.returncode}). Initial Stderr:\n{initial_stderr.decode(errors='ignore')}")

        if not server_proc.stdin or not server_proc.stdout:
             raise RuntimeError("Server process stdin/stdout streams not available.")

        # --- Bridge Streams and Connect MCP Session ---
        logger.info("Creating stream bridge and MCP session...")
        # Use the bridge context manager
        async with bridge_stdio_to_anyio_session(server_proc.stdout, server_proc.stdin) as (session_read_stream, session_write_stream):

            # Instantiate ClientSession with the AnyIO streams from the bridge
            session = ClientSession(
                read_stream=session_read_stream,
                write_stream=session_write_stream,
                # Pass other necessary ClientSession options if needed (timeouts, etc.)
                # read_timeout_seconds=timedelta(seconds=config.mcp_timeout) # Example
            )
            logger.info("MCP ClientSession instantiated.")

            # Initialize the MCP session (sends initialize request, etc.)
            # This needs to happen before handling MQTT messages that require the session
            try:
                 logger.info("Initializing MCP session...")
                 try:
                     init_result = await session.initialize()
                     logger.info(f"MCP session initialized successfully. Server info: {init_result.serverInfo}")
                 except Exception as e:
                     logger.warning(f"MCP session initialization failed: {e}. This might be OK if the server doesn't support 'initialize'.")
                     # We'll continue even if initialize fails, since the server might not support it
            except Exception as e:
                 logger.exception("Fatal error during MCP session setup")
                 # Check stderr from process
                 stderr = await server_proc.stderr.read() if server_proc.stderr else b""
                 logger.error(f"Server process stderr during setup failure:\n{stderr.decode()}")
                 raise RuntimeError("MCP session setup failed") from e

            # Start session.run() in background task to handle incoming requests/notifications from server
            loop = asyncio.get_running_loop()
            session_run_task = loop.create_task(session.run(), name="session_run")
            logger.info("MCP session.run() started in background task.")


            # --- Connect to MQTT Broker ---
            mqtt_client = create_mqtt_client_from_config(config)
            await mqtt_client.connect()
            logger.info(f"Connected to MQTT broker at {config.broker_url}")
            
            # Test the MQTT connection by publishing a message to the server info topic
            # Skip this in test mode with disable_startup_info=True
            if not getattr(config, 'disable_startup_info', False):
                server_info_topic = f"{config.base_topic}/server_info"
                test_payload = {
                    "server": "MCP Listener",
                    "startup_time": time.time(),
                    "client_id": config.client_id
                }
                logger.info(f"Testing MQTT connection by publishing to {server_info_topic}")
                await mqtt_client.publish(
                    server_info_topic,
                    json.dumps(test_payload).encode(),
                    qos=config.qos
                )

            # --- Subscribe to Request Topic & Handle Messages ---
            request_topic_to_subscribe = f"{config.base_topic}/request"
            logger.info(f"Subscribing to request topic: {request_topic_to_subscribe}")
            await mqtt_client.subscribe(request_topic_to_subscribe, qos=config.qos)

            # Only subscribe to debugging topics if not in test mode
            if not getattr(config, 'test_mode', False):
                # Also subscribe to the "#" wildcard for debugging purposes
                debug_topic = f"{config.base_topic}/#"
                logger.info(f"Debug: Also subscribing to wildcard topic for debugging: {debug_topic}")
                await mqtt_client.subscribe(debug_topic, qos=config.qos)
                
                # Also subscribe to a global wildcard for debugging
                global_debug_topic = "#"
                logger.info(f"Debug: Also subscribing to global wildcard topic: {global_debug_topic}")
                await mqtt_client.subscribe(global_debug_topic, qos=config.qos)

            # Slight delay to ensure subscriptions are active
            logger.info("Waiting for MQTT subscriptions to settle...")
            await asyncio.sleep(1.0)

            logger.info("Starting main MQTT message loop...")
            
            # Create exit event for timeout if specified
            timeout_task = None
            if config.debug_timeout:
                async def exit_after_timeout():
                    logger.info(f"Debug timeout set: will exit after {config.debug_timeout} seconds")
                    await asyncio.sleep(config.debug_timeout)
                    logger.info("Debug timeout reached, initiating shutdown")
                    # Using CancelledError to trigger orderly shutdown via the finally block
                    raise asyncio.CancelledError("Debug timeout reached")
                
                timeout_task = asyncio.create_task(exit_after_timeout())
            
            # Use a completely different approach similar to our working test
            try:
                async with mqtt_client.messages() as messages:
                    async for message in messages:
                        try:
                            topic = str(message.topic)
                            logger.info(f"MQTT: Received message on topic: {topic}")
                            
                            try:
                                payload_str = message.payload.decode()
                                logger.info(f"MQTT payload: {payload_str[:200]}")
                            except Exception as e:
                                logger.warning(f"Could not decode payload: {e}")
                                continue
                                
                            # Check if this is a request message
                            request_base = f"{config.base_topic}/request"
                            if topic == request_base or topic.startswith(f"{request_base}/"):
                                logger.info(f"Processing request on topic: {topic}")
                                try:
                                    await _handle_mqtt_message(message, mqtt_client, session, config)
                                except Exception as e:
                                    logger.exception(f"Error handling MQTT message: {e}")
                        except Exception as e:
                            logger.exception(f"Unexpected error processing message: {e}")
                            
                logger.info("MQTT message loop completed normally")
            finally:
                # Cancel timeout task if it exists
                if timeout_task and not timeout_task.done():
                    timeout_task.cancel()
                    try:
                        await timeout_task
                    except asyncio.CancelledError:
                        pass


    except asyncio.CancelledError:
        logger.info("Listener task cancelled.")
    except aiomqtt.MqttError as e:
        logger.error(f"MQTT Error in Listener: {e}")
    except McpError as e: # Use the correct exception class here too
        logger.error(f"MCP Client Session Error in Listener: {e}")
    finally:
        logger.info("Shutting down MQTT listener...")
        
        # 1. Cancel session task (before closing session)
        if session_run_task and not session_run_task.done():
            logger.info("Cancelling session.run() task...")
            session_run_task.cancel()
            try:
                await session_run_task
            except asyncio.CancelledError:
                 pass # Expected
            except Exception:
                logger.exception("Error during session.run() task cleanup")

        # 2. Close session (this should signal bridge tasks via stream closure)
        if session: # Close session if it exists
            logger.info("Closing MCP session...")
            # Closing the session should ideally signal the underlying streams/process
            # but we also need to handle the process termination explicitly now.
            await session.close()

        # 3. Terminate server process (bridge cleanup happens via context manager exit)
        if server_proc and server_proc.returncode is None:
            logger.info(f"Terminating stdio server process (PID: {server_proc.pid})...")
            try:
                server_proc.terminate()
                # Wait briefly for termination
                await asyncio.wait_for(server_proc.wait(), timeout=5.0)
                logger.info(f"Stdio server process (PID: {server_proc.pid}) terminated.")
            except asyncio.TimeoutError:
                logger.warning(f"Stdio server process (PID: {server_proc.pid}) did not terminate gracefully, killing.")
                server_proc.kill()
            except ProcessLookupError:
                 logger.info(f"Stdio server process (PID: {server_proc.pid}) already terminated.")
            except Exception:
                 logger.exception(f"Error terminating stdio server process (PID: {server_proc.pid})")
            finally:
                # Ensure stderr is read even if termination fails/succeeds
                if server_proc.stderr:
                    # Try a single large read during cleanup
                    try:
                        final_stderr = await asyncio.wait_for(server_proc.stderr.read(), timeout=0.5) # Read everything with a short timeout
                        if final_stderr:
                             logger.info(f"Stdio server process final stderr (PID: {server_proc.pid}):\n{final_stderr.decode(errors='ignore')}")
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout reading final stderr from server process (PID: {server_proc.pid}).")
                    except Exception as e:
                        logger.error(f"Error reading final stderr from server process (PID: {server_proc.pid}): {e}")

        # 4. Disconnect MQTT client
        if mqtt_client and mqtt_client.is_connected():
            logger.info("Disconnecting from MQTT broker...")
            await mqtt_client.disconnect()

        # AnyIO stdio streams might not need explicit closing, but check docs if needed
        # if 'anyio_stdin_reader' in locals() and hasattr(anyio_stdin_reader, 'aclose'):
        #     await anyio_stdin_reader.aclose()
        # if 'anyio_stdout_writer' in locals() and hasattr(anyio_stdout_writer, 'aclose'):
        #     await anyio_stdout_writer.aclose()

        logger.info("MQTT listener shut down.")
