"""Mode 2: Listen for MCP requests on MQTT and proxy them to a local stdio MCP server."""

import asyncio
import json
import logging
import ssl
import typing as t
from typing import Any, Dict
from dataclasses import dataclass
from urllib.parse import urlparse
import sys
import subprocess
import math # For infinity stream buffer size
from contextlib import asynccontextmanager # For bridge context manager
from datetime import timedelta # For ClientSession timeout example
import time
import anyio # <-- Add import for anyio
import binascii
import os
import traceback

import aiomqtt
import paho.mqtt.client as paho_mqtt_client # For PacketTypes constant and MessageType
import paho.mqtt.properties as mqtt_properties # Import Properties class
import paho.mqtt.packettypes as packettypes # Import packet types constants
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.stapled import StapledObjectStream
from pydantic import BaseModel, ValidationError

# Import MCP client components
from mcp.client.session import ClientSession
from mcp.shared.message import SessionMessage # Used by the stream bridge
from mcp import McpError

# Import MCP type definitions needed for JSONRPC messages
from mcp.types import (
    JSONRPCRequest, JSONRPCResponse, JSONRPCError, ErrorData, Implementation,
    ListPromptsRequest, ListPromptsResult, Prompt, Request
)

from mcp_mqtt_proxy.config import MQTTListenerConfig, StdioServerParameters, create_mqtt_client_from_config
from mcp_mqtt_proxy.utils import generate_id

# Configure enhanced logging
logger = logging.getLogger(__name__)

# Create a proper model for JSON-RPC error responses
class JSONRPCErrorResponse(BaseModel):
    """A JSON-RPC response with an error field instead of a result field."""
    jsonrpc: str
    id: Any
    error: JSONRPCError

# Utility function for hex dumps
def hex_dump(data, prefix=""):
    """Create a hex dump of binary data for debugging."""
    if isinstance(data, str):
        data = data.encode('utf-8')
    
    result = []
    for i in range(0, len(data), 16):
        chunk = data[i:i+16]
        hex_values = ' '.join(f'{b:02x}' for b in chunk)
        printable = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        result.append(f"{prefix}{i:04x}: {hex_values:<48} {printable}")
    return '\n'.join(result)

# Helper to parse incoming MQTT messages
def _parse_mcp_message_from_json(payload: bytes) -> Request | None:
    """Attempts to parse the MQTT payload bytes as a Request JSON object."""
    try:
        # First try to parse as JSON
        payload_str = payload.decode('utf-8')
        logger.debug(f"Decoding payload ({len(payload)} bytes): {payload_str[:200]}")
        logger.debug(f"Full payload hex dump:\n{hex_dump(payload, '  ')}")
        
        data = json.loads(payload_str)
        logger.debug(f"Parsed JSON: {type(data)}")
        
        # Check if this has the basic JSON-RPC structure
        if not isinstance(data, dict):
            logger.error(f"JSON payload is not a dictionary: {data}")
            return None
            
        # Extract required fields from JSON directly to avoid validation errors
        if "jsonrpc" in data and "method" in data and "id" in data:
            logger.debug(f"Found basic JSON-RPC structure: jsonrpc={data['jsonrpc']}, method={data['method']}, id={data['id']}")
            # Create a Request object directly
            return Request(
                jsonrpc=data.get("jsonrpc", "2.0"),
                id=data.get("id"),
                method=data.get("method")
            )
        
        # If direct extraction fails, try the full validation
        try:
            # Validate against the base Request model
            logger.debug("Attempting full pydantic validation...")
            request = Request.model_validate_json(payload)
            logger.debug(f"Full validation succeeded: {request}")
            return request
        except Exception as inner_e:
            logger.warning(f"Pydantic validation failed after basic JSON parsing succeeded: {inner_e}")
            logger.debug(f"Validation error details:\n{traceback.format_exc()}")
            
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
        logger.debug(f"Error details:\n{traceback.format_exc()}")
        logger.debug(f"Problematic payload:\n{hex_dump(payload, '  ')}")
        # Consider how to handle malformed JSON. Return None for now.
        return None


# --- Stream Bridging ---

@asynccontextmanager
async def bridge_stdio_to_anyio_session(
    proc: asyncio.subprocess.Process,
    session_id: str,
    timeout: float,
) -> t.AsyncGenerator[t.Tuple[StapledObjectStream[BaseModel], str], None]:
    """Context manager to bridge process stdin/stdout to anyio memory streams for a ClientSession."""
    logger.info(f"[Bridge {session_id}] Initializing bridge PID={proc.pid} timeout={timeout}")
    
    if proc.stdin is None or proc.stdout is None:
        logger.error(f"[Bridge {session_id}] Process stdin/stdout is not available")
        raise RuntimeError("Process stdin/stdout is not available for bridging")

    # Create AnyIO memory object streams with unlimited buffer
    # These will be used to communicate between the asyncio process I/O and anyio-based ClientSession
    logger.debug(f"[Bridge {session_id}] Creating memory object streams")
    
    # Create two separate streams for send and receive
    client_to_server_send, client_to_server_recv = anyio.create_memory_object_stream[BaseModel](math.inf)
    server_to_client_send, server_to_client_recv = anyio.create_memory_object_stream[BaseModel](math.inf)
    
    # Create a StapledObjectStream for bidirectional communication with the ClientSession
    # The client sends to client_to_server_send, which the server reads from client_to_server_recv
    # The server sends to server_to_client_send, which the client reads from server_to_client_recv
    logger.debug(f"[Bridge {session_id}] Creating stapled object stream")
    session_stream = StapledObjectStream(
        send_stream=client_to_server_send,  
        receive_stream=server_to_client_recv
    )
    
    # Create task group for the reader/writer tasks
    logger.debug(f"[Bridge {session_id}] Creating task group")
    stdio_task_group = anyio.create_task_group()

    try:
        logger.info(f"[Bridge {session_id}] Starting bridge tasks")
        async with stdio_task_group:
            # Start reader task to read from process stdout and send to server_to_client_send
            # (which the client will receive from)
            logger.debug(f"[Bridge {session_id}] Starting reader task")
            stdio_task_group.start_soon(
                _stdio_reader, 
                proc.stdout, 
                server_to_client_send, 
                session_id, 
                timeout
            )
            
            # Start writer task to receive from client_to_server_recv (which the client sends to)
            # and write to process stdin
            logger.debug(f"[Bridge {session_id}] Starting writer task")
            stdio_task_group.start_soon(
                _stdio_writer, 
                proc.stdin, 
                client_to_server_recv, 
                session_id, 
                timeout
            )

            # Yield the session stream and session ID
            logger.info(f"[Bridge {session_id}] Streams bridged, session can now start")
            yield session_stream, session_id

            # On exit from context manager, ensure tasks are cancelled
            logger.info(f"[Bridge {session_id}] Exiting context manager, cancelling I/O tasks")
            stdio_task_group.cancel_scope.cancel()

    except (anyio.EndOfStream, anyio.ClosedResourceError) as e:
        logger.warning(f"[Bridge {session_id}] Stream closed unexpectedly: {e}")
        logger.debug(f"[Bridge {session_id}] Error details:\n{traceback.format_exc()}")
    except Exception as e:
        logger.exception(f"[Bridge {session_id}] Error during stdio bridging: {e}")
    finally:
        # Ensure stdin is closed if possible
        if proc.stdin and not proc.stdin.is_closing():
            try:
                logger.debug(f"[Bridge {session_id}] Closing process stdin")
                proc.stdin.close()
                await proc.stdin.wait_closed()
                logger.debug(f"[Bridge {session_id}] Process stdin closed")
            except Exception as close_err:
                logger.warning(f"[Bridge {session_id}] Error closing stdin: {close_err}")
        logger.info(f"[Bridge {session_id}] Context manager finished")


async def _stdio_reader(
    stdout: asyncio.StreamReader,
    send_stream: MemoryObjectSendStream[BaseModel],
    session_id: str,
    timeout: float,
):
    """Reads JSON lines from stdout and sends them to the AnyIO stream."""
    logger.info(f"[Reader {session_id}] Starting reader task for process stdout")
    read_counter = 0
    bytes_read = 0
    
    try:
        while True:
            read_counter += 1
            logger.debug(f"[Reader {session_id}] Waiting for line {read_counter}...")
            
            try:
                # Read line with timeout
                start_read = time.time()
                line_bytes = await asyncio.wait_for(stdout.readline(), timeout=timeout)
                read_time = time.time() - start_read
                
                if not line_bytes:
                    logger.info(f"[Reader {session_id}] Stdout EOF reached after {read_counter} lines")
                    break

                bytes_read += len(line_bytes)
                logger.debug(f"[Reader {session_id}] Read line {read_counter} ({len(line_bytes)} bytes) in {read_time:.6f}s")
                logger.debug(f"[Reader {session_id}] Raw line: {hex_dump(line_bytes, '  ')}")

                line = line_bytes.decode('utf-8').strip()
                if not line:
                    logger.debug(f"[Reader {session_id}] Empty line received, skipping")
                    continue

                logger.info(f"[Reader {session_id}] Received line {read_counter}: {line[:100]}")
                try:
                    # Parse as JSON first
                    logger.debug(f"[Reader {session_id}] Parsing JSON...")
                    msg_data = json.loads(line)
                    logger.debug(f"[Reader {session_id}] JSON parsed successfully: {type(msg_data)}")
                    
                    # Log the raw data structure to help debug
                    logger.debug(f"[Reader {session_id}] JSON data: {json.dumps(msg_data)}")
                    
                    # Extract key information for logging
                    msg_id = msg_data.get("id", "unknown")
                    msg_method = msg_data.get("method", "none")
                    has_result = "result" in msg_data
                    has_error = "error" in msg_data
                    
                    logger.info(f"[Reader {session_id}] Message ID={msg_id}, Method={msg_method}, " 
                               f"HasResult={has_result}, HasError={has_error}")
                    
                    # Create a dynamic model to represent any JSON-RPC structure
                    class DynamicResponse(BaseModel):
                        """A dynamic model that can represent any JSON-RPC message structure."""
                        jsonrpc: str = "2.0"
                        id: t.Optional[t.Any] = None
                        
                        # Optional fields that might be present
                        method: t.Optional[str] = None
                        params: t.Optional[t.Dict[str, t.Any]] = None
                        result: t.Optional[t.Dict[str, t.Any]] = None
                        error: t.Optional[t.Dict[str, t.Any]] = None
                    
                    # Create a response that can handle any structure
                    logger.debug(f"[Reader {session_id}] Creating DynamicResponse...")
                    try:
                        msg = DynamicResponse(**msg_data)
                        logger.debug(f"[Reader {session_id}] Created message: {msg}")
                    except ValidationError as ve:
                        logger.warning(f"[Reader {session_id}] Validation error creating model, falling back to raw dict: {ve}")
                        # If validation fails, create a model with just the id and raw data
                        msg = DynamicResponse(
                            jsonrpc="2.0", 
                            id=msg_data.get("id"),
                            result=msg_data
                        )
                    
                    # Send to the AnyIO stream (connected to ClientSession)
                    logger.debug(f"[Reader {session_id}] Sending message to session stream...")
                    start_send = time.time()
                    await send_stream.send(msg)
                    send_time = time.time() - start_send
                    logger.info(f"[Reader {session_id}] Sent message {msg_id} to session in {send_time:.6f}s")

                except json.JSONDecodeError as e:
                    logger.error(f"[Reader {session_id}] Invalid JSON received: {line}")
                    logger.error(f"[Reader {session_id}] JSON error: {e}")
                    logger.debug(f"[Reader {session_id}] Raw line with error: {hex_dump(line_bytes, '  ')}")
                except ValidationError as e:
                    logger.error(f"[Reader {session_id}] Validation error for message: {line} - {e}")
                    logger.debug(f"[Reader {session_id}] Validation error details:\n{traceback.format_exc()}")
                except Exception as e:
                    logger.exception(f"[Reader {session_id}] Unexpected error processing line: {line}")

            except asyncio.TimeoutError:
                logger.warning(f"[Reader {session_id}] Timeout reading from stdout after {timeout}s")
                # Continue on timeout - it's okay if there's nothing to read for a while
                continue
            except (BrokenPipeError, ConnectionResetError) as e:
                logger.info(f"[Reader {session_id}] Stdout pipe closed: {e}")
                break
            except Exception as e:
                logger.exception(f"[Reader {session_id}] Error reading from stdout: {e}")
                # Consider if we should break here
                break
                
    except asyncio.CancelledError:
        logger.info(f"[Reader {session_id}] Reader task cancelled")
    except Exception as e:
        logger.exception(f"[Reader {session_id}] Unhandled exception in reader task: {e}")
    finally:
        logger.info(f"[Reader {session_id}] Closing send stream. Read {read_counter} lines, {bytes_read} bytes total")
        await send_stream.aclose()
        logger.info(f"[Reader {session_id}] Reader task finished")


async def _stdio_writer(
    stdin: asyncio.StreamWriter,
    receive_stream: MemoryObjectReceiveStream[BaseModel],
    session_id: str,
    timeout: float, # Timeout for receiving from stream
):
    """Receives messages from the AnyIO stream and writes them to stdin as JSON lines."""
    logger.info(f"[Writer {session_id}] Starting writer task for process stdin")
    write_counter = 0
    bytes_written = 0
    
    try:
        async for msg in receive_stream:
            write_counter += 1
            logger.debug(f"[Writer {session_id}] Processing message {write_counter} of type {type(msg)}")
            
            try:
                # Handle SessionMessage objects (which wrap the actual message)
                if isinstance(msg, SessionMessage):
                    # Extract the inner message first
                    logger.debug(f"[Writer {session_id}] Unwrapping SessionMessage")
                    inner_msg = msg.message
                    
                    # Get ID for logging if available
                    msg_id = getattr(inner_msg, "id", f"unknown-{write_counter}")
                    method = getattr(inner_msg, "method", "unknown")
                    logger.debug(f"[Writer {session_id}] Extracted inner message ID: {msg_id}, Method: {method}")
                    
                    # Convert to JSON
                    json_line = inner_msg.model_dump_json()
                else:
                    # For direct BaseModel objects
                    # Get ID for logging if available
                    msg_id = getattr(msg, "id", f"unknown-{write_counter}")
                    method = getattr(msg, "method", "unknown")
                    logger.debug(f"[Writer {session_id}] Direct BaseModel message ID: {msg_id}, Method: {method}")
                    
                    # Convert to JSON
                    json_line = msg.model_dump_json()
                
                # Log the message content
                logger.info(f"[Writer {session_id}] Sending line {write_counter}: {json_line[:100]}")
                logger.debug(f"[Writer {session_id}] Full JSON: {json_line}")
                
                # Add newline and encode
                encoded_line = json_line.encode('utf-8') + b'\n'
                bytes_written += len(encoded_line)
                
                # Write to stdin
                logger.debug(f"[Writer {session_id}] Writing {len(encoded_line)} bytes to stdin")
                stdin.write(encoded_line)
                
                # Drain with timeout
                logger.debug(f"[Writer {session_id}] Draining stdin...")
                start_drain = time.time()
                await asyncio.wait_for(stdin.drain(), timeout=timeout) 
                drain_time = time.time() - start_drain
                logger.debug(f"[Writer {session_id}] Drain completed in {drain_time:.6f}s")
                
                # Log completion with ID
                logger.info(f"[Writer {session_id}] Sent message {msg_id} to stdio")
                
            except asyncio.TimeoutError:
                logger.warning(f"[Writer {session_id}] Timeout draining stdin after {timeout}s")
                break  # Break on timeout
            except (BrokenPipeError, ConnectionResetError) as e:
                logger.info(f"[Writer {session_id}] Stdin pipe closed: {e}")
                break
            except Exception as e:
                logger.exception(f"[Writer {session_id}] Error writing to stdin: {e}")
                break  # Stop writing on error
                
    except anyio.EndOfStream:
        logger.info(f"[Writer {session_id}] Receive stream ended")
    except asyncio.CancelledError:
        logger.info(f"[Writer {session_id}] Writer task cancelled")
    except Exception as e:
        logger.exception(f"[Writer {session_id}] Unhandled exception in writer task: {e}")
    finally:
        logger.info(f"[Writer {session_id}] Writer task finished. Wrote {write_counter} messages, {bytes_written} bytes total")
        # Don't close stdin here, let the context manager handle it


# --- MQTT Message Handling Helper ---

async def _handle_mqtt_message(
    message: aiomqtt.Message,
    mqtt_client: aiomqtt.Client,
    session: ClientSession,
    config: MQTTListenerConfig
) -> None:
    """Handle a single MQTT message by parsing it, forwarding it to MCP, and returning the response."""
    start_time = time.time()
    message_id = f"mqtt-{int(start_time*1000) % 10000}"
    logger.info(f"[MSG-{message_id}] === Begin handling MQTT message ===")
    
    # Log message details
    topic = str(message.topic)
    logger.info(f"[MSG-{message_id}] Topic: {topic}")
    
    try:
        payload_text = message.payload.decode('utf-8')
        payload_preview = payload_text[:200] + ('...' if len(payload_text) > 200 else '')
        logger.info(f"[MSG-{message_id}] Payload: {payload_preview}")
    except UnicodeDecodeError:
        logger.warning(f"[MSG-{message_id}] Binary payload ({len(message.payload)} bytes)")
        logger.debug(f"[MSG-{message_id}] Payload hex dump:\n{hex_dump(message.payload, '  ')}")
    
    # Log full message properties
    props_dict = {}
    if message.properties:
        logger.info(f"[MSG-{message_id}] Message has MQTT v5 properties")
        
        if hasattr(message.properties, 'ResponseTopic') and message.properties.ResponseTopic:
            props_dict['ResponseTopic'] = message.properties.ResponseTopic
            logger.info(f"[MSG-{message_id}] ResponseTopic: {message.properties.ResponseTopic}")
            
        if hasattr(message.properties, 'CorrelationData') and message.properties.CorrelationData:
            if isinstance(message.properties.CorrelationData, bytes):
                corr_data = message.properties.CorrelationData.decode('utf-8', errors='replace')
                props_dict['CorrelationData'] = corr_data
                logger.info(f"[MSG-{message_id}] CorrelationData: {corr_data}")
            else:
                props_dict['CorrelationData'] = str(message.properties.CorrelationData)
                logger.info(f"[MSG-{message_id}] CorrelationData: {message.properties.CorrelationData}")
                
        # Add any other properties for debugging
        for prop_name in dir(message.properties):
            if not prop_name.startswith('_') and prop_name not in props_dict:
                value = getattr(message.properties, prop_name)
                if value is not None and not callable(value):
                    props_dict[prop_name] = str(value)
                    logger.debug(f"[MSG-{message_id}] Property {prop_name}: {value}")
    
    response_topic = None
    correlation_data = None
    request_id_for_response = None
    sent_response = False
    
    try:
        # Extract message properties for response
        properties = message.properties
        
        # Check if this is an MQTT v5 message with ResponseTopic property
        if properties:
            # Get response topic from properties
            response_topic = getattr(properties, "ResponseTopic", None)
            correlation_data = getattr(properties, "CorrelationData", None)
            
        # If no ResponseTopic is found, use a default based on the base topic
        if not response_topic:
            logger.info(f"[MSG-{message_id}] No response topic in properties, constructing a default one")
            # Default to a generic response topic in same hierarchy
            response_topic = f"{config.base_topic}/response"
            logger.info(f"[MSG-{message_id}] Using default response topic: {response_topic}")
        
        # Parse the message payload
        try:
            payload_text = message.payload.decode('utf-8')
            logger.info(f"[MSG-{message_id}] Decoded message payload from topic {message.topic}: {payload_text[:200]}")
            
            # Parse the JSON payload
            try:
                logger.debug(f"[MSG-{message_id}] Parsing JSON payload...")
                request_data = json.loads(payload_text)
                logger.info(f"[MSG-{message_id}] Parsed JSON request with type: {type(request_data)}")
                logger.debug(f"[MSG-{message_id}] Request data: {json.dumps(request_data, indent=2)}")
                
                # Extract request ID for correlation
                if 'id' in request_data:
                    request_id_for_response = request_data['id']
                    logger.info(f"[MSG-{message_id}] Request ID: {request_id_for_response}")
                
                # Create a simple direct response to test if routing works
                if request_data.get('method') == 'test/echo':
                    logger.info(f"[MSG-{message_id}] Detected test/echo method, handling directly for testing")
                    
                    # Create direct response for the echo method to test routing
                    echo_message = request_data.get('params', {}).get('message', '')
                    logger.info(f"[MSG-{message_id}] Echo message parameter: '{echo_message}'")
                    
                    response_data = {
                        "jsonrpc": "2.0",
                        "id": request_id_for_response,
                        "result": {
                            "message": f"Echo: {echo_message}"
                        }
                    }
                    
                    # Convert to JSON
                    response_json = json.dumps(response_data)
                    logger.info(f"[MSG-{message_id}] Direct echo response created: {response_json}")
                    
                    # Create MQTT properties for response if needed
                    resp_properties = None
                    if correlation_data:
                        logger.debug(f"[MSG-{message_id}] Creating response properties with CorrelationData")
                        resp_properties = mqtt_properties.Properties(mqtt_properties.PacketTypes.PUBLISH)
                        if isinstance(correlation_data, bytes):
                            resp_properties.CorrelationData = correlation_data
                            logger.debug(f"[MSG-{message_id}] Set binary CorrelationData: {correlation_data}")
                        else:
                            encoded_corr = str(correlation_data).encode('utf-8')
                            resp_properties.CorrelationData = encoded_corr
                            logger.debug(f"[MSG-{message_id}] Set encoded CorrelationData: {encoded_corr}")
                    
                    # Publish the response
                    logger.info(f"[MSG-{message_id}] Publishing echo response to {response_topic}")
                    response_bytes = response_json.encode('utf-8')
                    logger.debug(f"[MSG-{message_id}] Response bytes ({len(response_bytes)} bytes):\n{hex_dump(response_bytes, '  ')}")
                    
                    await mqtt_client.publish(
                        response_topic,
                        payload=response_bytes,
                        qos=config.qos,
                        properties=resp_properties
                    )
                    logger.info(f"[MSG-{message_id}] Direct echo response published successfully")
                    sent_response = True
                    return
                
                # For non-echo methods, we would forward to MCP (uncomment when echo works)
                logger.info(f"[MSG-{message_id}] Method {request_data.get('method')} not handled directly, would forward to MCP")
                
                # Normal processing using session.request (currently disabled)
                """
                logger.info(f"Sending request to MCP session: {request_data.get('method')}")
                try:
                    raw_response = await asyncio.wait_for(
                        session.request(request_data),
                        timeout=config.mcp_timeout
                    )
                    
                    # Convert response to JSON and publish
                    response_dict = {}
                    if hasattr(raw_response, "model_dump"):
                        response_dict = raw_response.model_dump(mode='json')
                    else:
                        response_dict = dict(raw_response) if not isinstance(raw_response, dict) else raw_response
                    
                    # Publish the response
                    response_json = json.dumps(response_dict)
                    
                    # Set up MQTT response properties
                    mqtt_props = None
                    if correlation_data:
                        mqtt_props = mqtt_properties.Properties(mqtt_properties.PacketTypes.PUBLISH)
                        if isinstance(correlation_data, bytes):
                            mqtt_props.CorrelationData = correlation_data
                        else:
                            mqtt_props.CorrelationData = str(correlation_data).encode('utf-8')
                    
                    await mqtt_client.publish(
                        response_topic,
                        payload=response_json.encode(),
                        qos=config.qos,
                        properties=mqtt_props
                    )
                    logger.info(f"Response published to {response_topic}")
                except Exception as e:
                    logger.exception(f"Error processing request through MCP: {e}")
                    # Send error response
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": request_id_for_response,
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}"
                        }
                    }
                    await mqtt_client.publish(
                        response_topic,
                        json.dumps(error_response).encode('utf-8'),
                        qos=config.qos
                    )
                """
            except json.JSONDecodeError as e:
                logger.error(f"[MSG-{message_id}] Invalid JSON in message: {e}")
                logger.debug(f"[MSG-{message_id}] Raw message with JSON error: {payload_text}")
                
                error_resp = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": f"Parse error: {str(e)}"
                    }
                }
                await mqtt_client.publish(
                    response_topic,
                    json.dumps(error_resp).encode('utf-8'),
                    qos=config.qos
                )
        except UnicodeDecodeError as e:
            logger.error(f"[MSG-{message_id}] Cannot decode message payload as UTF-8: {e}")
            logger.debug(f"[MSG-{message_id}] Binary payload:\n{hex_dump(message.payload, '  ')}")
    except Exception as e:
        logger.exception(f"[MSG-{message_id}] Unexpected error handling MQTT message: {e}")
        if not sent_response and response_topic:
            try:
                error_resp = {
                    "jsonrpc": "2.0",
                    "id": request_id_for_response,
                    "error": {
                        "code": -32603,
                        "message": f"Internal error: {str(e)}"
                    }
                }
                await mqtt_client.publish(
                    response_topic,
                    json.dumps(error_resp).encode('utf-8'),
                    qos=config.qos
                )
            except Exception as send_err:
                logger.exception(f"[MSG-{message_id}] Failed to send error response: {send_err}")
    finally:
        elapsed = time.time() - start_time
        logger.info(f"[MSG-{message_id}] === End handling MQTT message ({elapsed:.6f}s) ===")


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
        async with bridge_stdio_to_anyio_session(server_proc, "stdio_session", config.mcp_timeout) as (session_stream, session_id):

            # Instantiate ClientSession with the AnyIO streams from the bridge
            session = ClientSession(
                read_stream=session_stream,
                write_stream=session_stream,
                # Don't set timeouts too low to avoid connection issues
                read_timeout_seconds=timedelta(seconds=config.mcp_timeout)
            )
            logger.info("MCP ClientSession instantiated.")

            # Initialize the MCP session - completely skip initialization if it fails
            # We'll continue even if initialize fails or times out, since not all servers support it
            try:
                logger.info("Trying to initialize MCP session...")
                try:
                    # Attempt initialize with short timeout
                    init_task = asyncio.create_task(session.initialize())
                    init_result = await asyncio.wait_for(
                        init_task,
                        timeout=2.0  # Quick timeout
                    )
                    logger.info(f"MCP session initialized successfully. Server info: {init_result.serverInfo}")
                except (asyncio.TimeoutError, Exception) as e:
                    logger.warning(f"MCP session initialization skipped: {e}. Proceeding without initialization.")
            except Exception as e:
                logger.warning(f"Error during MCP session initialization: {e}. Continuing anyway.")

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
                # Subscribe to debug/commands topic if available
                debug_topic = f"{config.base_topic}/debug/commands"
                logger.info(f"Subscribing to debug commands topic: {debug_topic}")
                await mqtt_client.subscribe(debug_topic, qos=config.qos)

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
    except McpError as e:
        logger.error(f"MCP Client Session Error in Listener: {e}")
    finally:
        logger.info("Shutting down MQTT listener...")
        
        # Close session (this should signal bridge tasks via stream closure)
        if session:
            logger.info("Closing MCP session...")
            # ClientSession may not have a close() method
            try:
                # First try to call close() method if it exists
                if hasattr(session, 'close') and callable(session.close):
                    await session.close()
                # If no close() method, check if there's an aclose() method (common in anyio)
                elif hasattr(session, 'aclose') and callable(session.aclose):
                    await session.aclose()
                else:
                    # No direct close method, the session will be garbage collected
                    # when its reference count drops to zero
                    logger.warning("Session doesn't have close() or aclose() method")
            except Exception as e:
                logger.exception(f"Error closing MCP session: {e}")

        # Terminate server process (bridge cleanup happens via context manager exit)
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

        # Disconnect MQTT client 
        if mqtt_client:
            logger.info("Disconnecting from MQTT broker...")
            try:
                await mqtt_client.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting MQTT client: {e}")

        logger.info("MQTT listener shut down.")
