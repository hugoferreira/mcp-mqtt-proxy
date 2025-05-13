#!/usr/bin/env python
"""
Stdio Bridge Fixtures.
Provides reusable bridge functions between stdio processes and anyio streams.
"""

import asyncio
import json
import logging
import sys
import time
import typing as t
from contextlib import asynccontextmanager

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.stapled import StapledObjectStream
from pydantic import BaseModel

# Configure logging
logger = logging.getLogger(__name__)

# Default timeouts
DEFAULT_BRIDGE_TIMEOUT = 10.0  # seconds
DEFAULT_IO_TIMEOUT = 5.0  # seconds

# --- Utility Functions ---

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


# --- Common Models ---

class JSONRPCRequest(BaseModel):
    """Simple JSON-RPC request model."""
    jsonrpc: str = "2.0"
    id: str
    method: str
    params: dict = {}


class DynamicResponse(BaseModel):
    """A dynamic model that can represent any JSON-RPC message structure."""
    jsonrpc: str = "2.0"
    id: str | None = None
    
    # Optional fields that might be present
    method: str | None = None
    params: dict | None = None
    result: dict | None = None
    error: dict | None = None


class SessionMessage(BaseModel):
    """Wrapper for a message going through a session."""
    message: BaseModel
    session_id: str = "test-session"


# --- Bridge Functions ---

@asynccontextmanager
async def bridge_stdio_to_anyio_session(
    proc: asyncio.subprocess.Process,
    session_id: str,
    timeout: float = DEFAULT_BRIDGE_TIMEOUT,
) -> t.AsyncGenerator[StapledObjectStream[BaseModel], None]:
    """Context manager to bridge process stdin/stdout to anyio memory streams."""
    logger.info(f"[Bridge {session_id}] Initializing bridge PID={proc.pid} timeout={timeout}")
    
    if proc.stdin is None or proc.stdout is None:
        logger.error(f"[Bridge {session_id}] Process stdin/stdout is not available")
        raise RuntimeError("Process stdin/stdout is not available for bridging")

    # Create AnyIO memory object streams
    logger.debug(f"[Bridge {session_id}] Creating memory object streams")
    
    # Create two separate streams for send and receive
    client_to_server_send, client_to_server_recv = anyio.create_memory_object_stream[BaseModel]()
    server_to_client_send, server_to_client_recv = anyio.create_memory_object_stream[BaseModel]()
    
    # Create a StapledObjectStream for bidirectional communication
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
        
        # Create a cancellation scope with timeout
        with anyio.fail_after(timeout):
            async with stdio_task_group:
                # Start reader task
                logger.debug(f"[Bridge {session_id}] Starting reader task")
                stdio_task_group.start_soon(
                    _stdio_reader, 
                    proc.stdout, 
                    server_to_client_send, 
                    session_id, 
                    timeout
                )
                
                # Start writer task
                logger.debug(f"[Bridge {session_id}] Starting writer task")
                stdio_task_group.start_soon(
                    _stdio_writer, 
                    proc.stdin, 
                    client_to_server_recv, 
                    session_id, 
                    timeout
                )

                # Yield the session stream
                logger.info(f"[Bridge {session_id}] Streams bridged, session can now start")
                yield session_stream

                # On exit from context manager, ensure tasks are cancelled
                logger.info(f"[Bridge {session_id}] Exiting context manager, cancelling I/O tasks")
                
    except (anyio.EndOfStream, anyio.ClosedResourceError) as e:
        logger.warning(f"[Bridge {session_id}] Stream closed unexpectedly: {e}")
    except TimeoutError:
        logger.error(f"[Bridge {session_id}] Bridge operation timed out after {timeout} seconds")
    except asyncio.CancelledError:
        logger.info(f"[Bridge {session_id}] Bridge operation cancelled")
    except Exception as e:
        logger.exception(f"[Bridge {session_id}] Error during stdio bridging: {e}")
    finally:
        # Make sure task group is cancelled
        if 'stdio_task_group' in locals():
            stdio_task_group.cancel_scope.cancel()
            
        # Ensure stdin is closed if possible
        if proc.stdin and not proc.stdin.is_closing():
            try:
                logger.debug(f"[Bridge {session_id}] Closing process stdin")
                proc.stdin.close()
                # Use a timeout for waiting for close
                await asyncio.wait_for(proc.stdin.wait_closed(), timeout=1.0)
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
                    
                    # Extract key information for logging
                    msg_id = msg_data.get("id", "unknown")
                    msg_method = msg_data.get("method", "none")
                    has_result = "result" in msg_data
                    has_error = "error" in msg_data
                    
                    logger.info(f"[Reader {session_id}] Message ID={msg_id}, Method={msg_method}, " 
                               f"HasResult={has_result}, HasError={has_error}")
                    
                    # Create a response model
                    logger.debug(f"[Reader {session_id}] Creating DynamicResponse...")
                    msg = DynamicResponse(**msg_data)
                    logger.debug(f"[Reader {session_id}] Created message: {msg}")
                    
                    # Send to the AnyIO stream
                    logger.debug(f"[Reader {session_id}] Sending message to session stream...")
                    start_send = time.time()
                    # Use timeout for send operation
                    with anyio.move_on_after(timeout):
                        await send_stream.send(msg)
                    send_time = time.time() - start_send
                    logger.info(f"[Reader {session_id}] Sent message {msg_id} to session in {send_time:.6f}s")

                except json.JSONDecodeError as e:
                    logger.error(f"[Reader {session_id}] Invalid JSON received: {line}")
                    logger.error(f"[Reader {session_id}] JSON error: {e}")
                    logger.debug(f"[Reader {session_id}] Raw line with error: {hex_dump(line_bytes, '  ')}")
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
                break
                
    except asyncio.CancelledError:
        logger.info(f"[Reader {session_id}] Reader task cancelled")
    except Exception as e:
        logger.exception(f"[Reader {session_id}] Unhandled exception in reader task: {e}")
    finally:
        logger.info(f"[Reader {session_id}] Closing send stream. Read {read_counter} lines, {bytes_read} bytes total")
        try:
            # Close with timeout
            await asyncio.wait_for(send_stream.aclose(), timeout=2.0)
        except Exception as e:
            logger.warning(f"[Reader {session_id}] Error closing send stream: {e}")
        logger.info(f"[Reader {session_id}] Reader task finished")


async def _stdio_writer(
    stdin: asyncio.StreamWriter,
    receive_stream: MemoryObjectReceiveStream[BaseModel],
    session_id: str,
    timeout: float,
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
                    logger.debug(f"[Writer {session_id}] Extracted inner message ID: {msg_id}")
                    
                    # Convert to JSON
                    json_line = inner_msg.model_dump_json()
                else:
                    # For direct BaseModel objects
                    # Get ID for logging if available
                    msg_id = getattr(msg, "id", f"unknown-{write_counter}")
                    logger.debug(f"[Writer {session_id}] Direct BaseModel message ID: {msg_id}")
                    
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
        # No need to close stdin here - let the context manager handle it


# --- Helper Functions ---

async def setup_stderr_logger(proc: asyncio.subprocess.Process):
    """Set up tasks to read stderr from a process and log it.
    
    Returns a tuple of (stderr_reader_task, logger_task) that should be cancelled when done.
    """
    if proc.stderr is None:
        logger.warning("Process does not have stderr available for logging")
        return None, None
    
    stderr_queue = asyncio.Queue()
    
    async def stderr_reader():
        """Read stderr lines from a process and put them in the queue."""
        try:
            while True:
                try:
                    # Read with timeout to prevent hangs
                    line = await asyncio.wait_for(proc.stderr.readline(), timeout=DEFAULT_IO_TIMEOUT)
                    if not line:  # EOF
                        logger.debug(f"Stderr EOF reached for PID {proc.pid}")
                        break
                    await stderr_queue.put(line)
                except asyncio.TimeoutError:
                    # Continue after timeout - might get more data later
                    continue
                except Exception as e:
                    logger.error(f"Error reading stderr: {e}")
                    break
        except asyncio.CancelledError:
            logger.debug(f"Stderr reader for PID {proc.pid} cancelled")
        finally:
            # Signal that we're done
            await stderr_queue.put(None)
    
    async def logger_task():
        """Process lines from the queue and log them."""
        try:
            while True:
                try:
                    # Wait for a line with a short timeout to make cancellation responsive
                    line = await asyncio.wait_for(stderr_queue.get(), timeout=0.5)
                    if line is None:  # EOF signal
                        break
                    stderr_text = line.decode('utf-8', errors='replace').rstrip()
                    if stderr_text:
                        logger.info(f"[Process {proc.pid}] {stderr_text}")
                    stderr_queue.task_done()
                except asyncio.TimeoutError:
                    # Just try again
                    continue
                except Exception as e:
                    logger.error(f"Error processing stderr line: {e}")
        except asyncio.CancelledError:
            logger.debug(f"Stderr logger for PID {proc.pid} cancelled")
    
    # Create and return both tasks
    reader_task = asyncio.create_task(stderr_reader(), name=f"stderr-reader-{proc.pid}")
    log_task = asyncio.create_task(logger_task(), name=f"stderr-logger-{proc.pid}")
    
    return reader_task, log_task 