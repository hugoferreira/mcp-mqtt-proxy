"""Bridge utilities for testing."""

import asyncio
import json
import logging
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, Union

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.stapled import StapledObjectStream
from pydantic import BaseModel, TypeAdapter

logger = logging.getLogger(__name__)

# Default timeouts
DEFAULT_BRIDGE_TIMEOUT = 10.0  # seconds
DEFAULT_IO_TIMEOUT = 5.0  # seconds


class MessageType(Enum):
    """Enum representing different types of MCP messages."""
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"
    ERROR = "error"
    UNKNOWN = "unknown"


class JSONRPCRequest(BaseModel):
    """JSON-RPC request model."""
    jsonrpc: str = "2.0"
    id: str
    method: str
    params: Optional[Dict[str, Any]] = None


class JSONRPCResponse(BaseModel):
    """JSON-RPC response model."""
    jsonrpc: str = "2.0"
    id: str
    result: Any
    error: Optional[Dict[str, Any]] = None


class JSONRPCNotification(BaseModel):
    """JSON-RPC notification model."""
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None


class DynamicResponse(BaseModel):
    """Dynamic response model that can handle either success or error responses."""
    jsonrpc: str = "2.0"
    id: str
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


@dataclass
class TestResult:
    """Represents the result of a test operation."""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        return f"{'SUCCESS' if self.success else 'FAILURE'}: {message}"


def determine_message_type(payload: Dict[str, Any]) -> MessageType:
    """Determine the type of JSON-RPC message from its payload."""
    if "jsonrpc" not in payload:
        return MessageType.UNKNOWN
        
    if "method" in payload:
        if "id" in payload:
            return MessageType.REQUEST
        else:
            return MessageType.NOTIFICATION
    elif "id" in payload:
        if "error" in payload and payload["error"] is not None:
            return MessageType.ERROR
        else:
            return MessageType.RESPONSE
    
    return MessageType.UNKNOWN


def hex_dump(data: Union[str, bytes], prefix: str = "") -> str:
    """Create a hex dump of binary data for debugging."""
    if isinstance(data, str):
        data = data.encode('utf-8')
    
    result = []
    for i in range(0, len(data), 16):
        chunk = data[i:i+16]
        hex_values = ' '.join([f'{b:02x}' for b in chunk])
        ascii_values = ''.join([chr(b) if 32 <= b <= 126 else '.' for b in chunk])
        result.append(f"{prefix}{i:04x}: {hex_values.ljust(48)} {ascii_values}")
    
    return '\n'.join(result)


def safe_decode(data: bytes) -> str:
    """Safely decode bytes to string, handling encoding errors."""
    return data.decode('utf-8', errors='replace')


async def setup_stderr_logger(proc: asyncio.subprocess.Process) -> Optional[asyncio.Task]:
    """Set up tasks to read stderr from a process and log it.
    
    Returns a task that should be cancelled when done.
    """
    if proc.stderr is None:
        logger.warning("Process does not have stderr available for logging")
        return None
    
    stderr_queue = asyncio.Queue()
    
    async def stderr_reader():
        """Read stderr lines from a process and put them in the queue."""
        try:
            while True:
                try:
                    # Read with timeout to prevent hangs
                    line = await asyncio.wait_for(proc.stderr.readline(), timeout=DEFAULT_IO_TIMEOUT)
                    if not line:  # EOF
                        logger.debug(f"Stderr EOF from PID {proc.pid}")
                        break
                    await stderr_queue.put(line)
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout reading stderr from PID {proc.pid}")
                    break
                except Exception as e:
                    logger.error(f"Error reading stderr from PID {proc.pid}: {e}")
                    break
        except asyncio.CancelledError:
            logger.debug(f"Stderr reader for PID {proc.pid} cancelled")
            raise
        except Exception as e:
            logger.exception(f"Unhandled exception in stderr reader: {e}")
        finally:
            # Signal end of stream
            await stderr_queue.put(None)
    
    async def stderr_logger():
        """Read from the queue and log the lines."""
        try:
            while True:
                line = await stderr_queue.get()
                if line is None:  # End signal
                    break
                logger.debug(f"[PID {proc.pid}] {safe_decode(line).rstrip()}")
        except asyncio.CancelledError:
            logger.debug(f"Stderr logger for PID {proc.pid} cancelled")
            raise
        except Exception as e:
            logger.exception(f"Unhandled exception in stderr logger: {e}")
    
    # Start the tasks directly
    reader_task = asyncio.create_task(stderr_reader())
    logger_task = asyncio.create_task(stderr_logger())
    
    # Create a task that will cancel both when it's cancelled
    async def combined_task():
        try:
            # Wait for both tasks
            await asyncio.gather(reader_task, logger_task, return_exceptions=True)
        except asyncio.CancelledError:
            # Cancel child tasks
            reader_task.cancel()
            logger_task.cancel()
            raise
        finally:
            # Ensure tasks are cancelled
            if not reader_task.done():
                reader_task.cancel()
            if not logger_task.done():
                logger_task.cancel()
    
    # Return the combined task
    return asyncio.create_task(combined_task())


@asynccontextmanager
async def bridge_stdio_to_anyio_session(
    process: asyncio.subprocess.Process,
    session_id: str,
    timeout: float = DEFAULT_BRIDGE_TIMEOUT
) -> AsyncGenerator[StapledObjectStream[BaseModel], None]:
    """
    Bridge a stdio process to anyio streams.
    
    Args:
        process: The asyncio subprocess to bridge
        session_id: Unique identifier for this bridge session
        timeout: Timeout for operations in seconds
        
    Yields:
        A StapledObjectStream for sending/receiving MCP messages
    """
    if process.stdin is None or process.stdout is None:
        raise ValueError("Process must have stdin and stdout pipes")
    
    logger.info(f"Setting up stdio bridge for session {session_id} with PID {process.pid}")
    
    # Create memory object streams for the bridge
    send_stream, receive_stream = anyio.create_memory_object_stream[BaseModel](max_buffer_size=100)
    
    # Create task group for reader/writer tasks
    async with anyio.create_task_group() as task_group:
        # Start the reader task to read from process stdout and send to memory stream
        async def _stdio_reader():
            """Read from process stdout, parse JSON-RPC messages, and send to memory stream."""
            try:
                logger.info(f"[Reader {session_id}] Starting stdin reader task")
                line_num = 0
                
                while True:
                    try:
                        # Read a line from stdout (with timeout)
                        line_num += 1
                        line = await asyncio.wait_for(process.stdout.readline(), timeout=timeout)
                        
                        # Check for EOF
                        if not line:
                            logger.info(f"[Reader {session_id}] Process stdout closed (EOF)")
                            break
                        
                        # Parse the JSON
                        line_str = line.decode("utf-8").strip()
                        logger.debug(f"[Reader {session_id}] Read line {line_num}: {line_str}")
                        
                        try:
                            data = json.loads(line_str)
                            
                            # Determine message type
                            msg_type = determine_message_type(data)
                            logger.debug(f"[Reader {session_id}] Message type: {msg_type.value}")
                            
                            # Create appropriate model based on message type
                            msg = None
                            msg_id = data.get("id", "unknown")
                            
                            if msg_type == MessageType.RESPONSE or msg_type == MessageType.ERROR:
                                # Use a dynamic response model that can handle either result or error
                                msg = DynamicResponse.model_validate(data)
                            elif msg_type == MessageType.REQUEST:
                                msg = JSONRPCRequest.model_validate(data)
                            elif msg_type == MessageType.NOTIFICATION:
                                msg = JSONRPCNotification.model_validate(data)
                            else:
                                logger.warning(f"[Reader {session_id}] Unknown message type: {data}")
                                continue
                            
                            # Send to the AnyIO stream
                            logger.debug(f"[Reader {session_id}] Sending message to session stream...")
                            start_send = time.time()
                            # Use timeout for send operation
                            with anyio.move_on_after(timeout):
                                await send_stream.send(msg)
                            send_time = time.time() - start_send
                            logger.info(f"[Reader {session_id}] Sent message {msg_id} to session in {send_time:.6f}s")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"[Reader {session_id}] JSON parse error: {e}, line: {line_str}")
                        except Exception as e:
                            logger.exception(f"[Reader {session_id}] Error processing message: {e}")
                    
                    except asyncio.TimeoutError:
                        logger.warning(f"[Reader {session_id}] Timeout reading from stdout")
                        break
                    except asyncio.CancelledError:
                        logger.info(f"[Reader {session_id}] Task cancelled")
                        raise
                    except Exception as e:
                        logger.exception(f"[Reader {session_id}] Unhandled exception: {e}")
                        break
            
            finally:
                logger.info(f"[Reader {session_id}] Reader task ending")
                # Close our end of the stream
                await send_stream.aclose()
        
        # Start the writer task to read from memory stream and write to process stdin
        async def _stdio_writer():
            """Read from memory stream and write to process stdin."""
            try:
                logger.info(f"[Writer {session_id}] Starting stdin writer task")
                
                # Create a receive stream to get messages from
                receive_astream = receive_stream
                
                async for msg in receive_astream:
                    try:
                        # Convert message to JSON
                        if hasattr(msg, "model_dump_json"):
                            json_str = msg.model_dump_json()
                        else:
                            json_str = json.dumps(msg)
                        
                        msg_id = getattr(msg, "id", "unknown")
                        logger.debug(f"[Writer {session_id}] Writing message {msg_id}: {json_str}")
                        
                        # Add newline and encode
                        data = (json_str + "\n").encode("utf-8")
                        
                        # Check that stdin is available
                        if process.stdin is None:
                            logger.error(f"[Writer {session_id}] Process stdin is None")
                            break

                        # Write to stdin with timeout
                        start_write = time.time()
                        try:
                            # Write the data to stdin with timeout
                            await asyncio.wait_for(process.stdin.write(data), timeout=timeout)
                            await asyncio.wait_for(process.stdin.drain(), timeout=timeout)
                            write_time = time.time() - start_write
                            
                            logger.info(f"[Writer {session_id}] Wrote message {msg_id} in {write_time:.6f}s")
                        except (asyncio.TimeoutError, TypeError) as e:
                            logger.warning(f"[Writer {session_id}] Error writing to stdin: {e}")
                            break
                        
                    except asyncio.CancelledError:
                        logger.info(f"[Writer {session_id}] Task cancelled")
                        raise
                    except Exception as e:
                        logger.exception(f"[Writer {session_id}] Error writing message: {e}")
                        continue
            
            except asyncio.CancelledError:
                logger.info(f"[Writer {session_id}] Task cancelled")
                raise
            except Exception as e:
                logger.exception(f"[Writer {session_id}] Unhandled exception: {e}")
            finally:
                logger.info(f"[Writer {session_id}] Writer task ending")
                # Close the pipe if it's not already closed
                if process.stdin and not process.stdin.is_closing():
                    process.stdin.close()
        
        # Start tasks in the task group
        task_group.start_soon(_stdio_reader)
        task_group.start_soon(_stdio_writer)
        
        # Create a stapled stream for the test to use
        session_stream = StapledObjectStream(
            send_stream=send_stream,
            receive_stream=receive_stream
        )
        
        try:
            # Yield the stream to the caller
            logger.info(f"[Bridge {session_id}] Bridge established, yielding session stream")
            yield session_stream
        finally:
            # Clean up will be handled by the task group context exit
            logger.info(f"[Bridge {session_id}] Exiting bridge context")
            # Additional cleanup might be needed here if the task group doesn't handle it all


async def send_jsonrpc_request(
    stream: StapledObjectStream[BaseModel],
    method: str,
    params: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
    timeout: float = DEFAULT_IO_TIMEOUT
) -> Tuple[str, JSONRPCRequest]:
    """
    Send a JSON-RPC request through a session stream.
    
    Args:
        stream: The StapledObjectStream to send the request through
        method: The JSON-RPC method to call
        params: Optional parameters for the method
        request_id: Optional request ID (generated if None)
        timeout: Timeout for the send operation
        
    Returns:
        Tuple of (request_id, request_object)
    """
    # Generate request ID if not provided
    if request_id is None:
        request_id = str(uuid.uuid4())
    
    # Create request object
    request = JSONRPCRequest(
        jsonrpc="2.0",
        id=request_id,
        method=method,
        params=params or {}
    )
    
    # Send the request with timeout
    logger.info(f"Sending request {request_id} to method: {method}")
    with anyio.move_on_after(timeout):
        await stream.send(request)
    
    return request_id, request


async def wait_for_jsonrpc_response(
    stream: StapledObjectStream[BaseModel],
    request_id: str,
    timeout: float = DEFAULT_IO_TIMEOUT
) -> Union[JSONRPCResponse, DynamicResponse]:
    """
    Wait for a JSON-RPC response with matching request ID.
    
    Args:
        stream: The StapledObjectStream to receive the response from
        request_id: The request ID to match
        timeout: Timeout for the receive operation
        
    Returns:
        The response object
        
    Raises:
        asyncio.TimeoutError: If no response is received within timeout
        ValueError: If the received message is not a response
    """
    logger.info(f"Waiting for response to request: {request_id}")
    
    # Use a timeout for the receive operation
    with anyio.move_on_after(timeout) as timeout_scope:
        while True:
            # Receive a message
            message = await stream.receive()
            
            # Check if it's a response with the matching ID
            if hasattr(message, "id") and message.id == request_id:
                if isinstance(message, (JSONRPCResponse, DynamicResponse)):
                    logger.info(f"Received response for request: {request_id}")
                    return message
                else:
                    logger.warning(f"Received non-response message with ID {request_id}")
            
            logger.debug(f"Skipping non-matching message: {message}")
    
    # If we get here, we timed out
    if timeout_scope.cancel_called:
        logger.warning(f"Timeout waiting for response to request: {request_id}")
        raise asyncio.TimeoutError(f"Timeout waiting for response to request: {request_id}")
    
    # We should never reach here
    raise ValueError("Unexpected state in wait_for_jsonrpc_response")


async def request_response(
    stream: StapledObjectStream[BaseModel],
    method: str,
    params: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
    timeout: float = DEFAULT_IO_TIMEOUT
) -> Union[JSONRPCResponse, DynamicResponse]:
    """
    Perform a complete request-response exchange.
    
    Args:
        stream: The StapledObjectStream to use for the exchange
        method: The JSON-RPC method to call
        params: Optional parameters for the method
        request_id: Optional request ID (generated if None)
        timeout: Timeout for the operation
        
    Returns:
        The response object
    """
    # Send request
    request_id, _ = await send_jsonrpc_request(
        stream=stream,
        method=method,
        params=params,
        request_id=request_id,
        timeout=timeout
    )
    
    # Wait for response
    return await wait_for_jsonrpc_response(
        stream=stream,
        request_id=request_id,
        timeout=timeout
    )


async def initialize_session(
    stream: StapledObjectStream[BaseModel],
    timeout: float = DEFAULT_IO_TIMEOUT
) -> bool:
    """
    Initialize a session by sending an 'initialize' request.
    
    Args:
        stream: The StapledObjectStream to use
        timeout: Timeout for the operation
        
    Returns:
        True if initialization succeeded, False otherwise
    """
    try:
        response = await request_response(
            stream=stream,
            method="initialize",
            params={},
            timeout=timeout
        )
        
        # Check if initialization succeeded
        if not hasattr(response, "error") or response.error is None:
            logger.info("Session initialization succeeded")
            return True
        else:
            logger.error(f"Session initialization failed: {response.error}")
            return False
    
    except (asyncio.TimeoutError, Exception) as e:
        logger.exception(f"Error during session initialization: {e}")
        return False 