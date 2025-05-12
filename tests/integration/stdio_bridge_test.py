#!/usr/bin/env python
"""
Direct stdio bridge test script.
This script directly tests the bridge_stdio_to_anyio_session functionality
without involving MQTT, to isolate potential issues in the bridging layer.
"""

import asyncio
import json
import logging
import sys
import os
import time
from pathlib import Path
import traceback
import uuid
import binascii
from contextlib import asynccontextmanager
import typing as t

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.stapled import StapledObjectStream
from pydantic import BaseModel

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(thread)d - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("stdio_bridge_test")

# Path to the simple_responder.py script for testing
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "simple_responder.py"

# Create simple dynamic models for JSON-RPC messages
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

# Session message wrapper (simplified version of what's in MCP)
class SessionMessage(BaseModel):
    """Wrapper for a message going through a session."""
    message: BaseModel
    session_id: str = "test-session"

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


async def stdio_reader(
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
                    
                    # Create a response model
                    logger.debug(f"[Reader {session_id}] Creating DynamicResponse...")
                    msg = DynamicResponse(**msg_data)
                    logger.debug(f"[Reader {session_id}] Created message: {msg}")
                    
                    # Send to the AnyIO stream
                    logger.debug(f"[Reader {session_id}] Sending message to session stream...")
                    start_send = time.time()
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
        await send_stream.aclose()
        logger.info(f"[Reader {session_id}] Reader task finished")


async def stdio_writer(
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


@asynccontextmanager
async def bridge_stdio_to_anyio_session(
    proc: asyncio.subprocess.Process,
    session_id: str,
    timeout: float,
) -> t.AsyncGenerator[StapledObjectStream[BaseModel], None]:
    """Context manager to bridge process stdin/stdout to anyio memory streams."""
    logger.info(f"[Bridge {session_id}] Initializing bridge PID={proc.pid} timeout={timeout}")
    
    if proc.stdin is None or proc.stdout is None:
        logger.error(f"[Bridge {session_id}] Process stdin/stdout is not available")
        raise RuntimeError("Process stdin/stdout is not available for bridging")

    # Create AnyIO memory object streams with unlimited buffer
    logger.debug(f"[Bridge {session_id}] Creating memory object streams")
    
    # Create two separate streams for send and receive
    client_to_server_send, client_to_server_recv = anyio.create_memory_object_stream[BaseModel](float('inf'))
    server_to_client_send, server_to_client_recv = anyio.create_memory_object_stream[BaseModel](float('inf'))
    
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
        async with stdio_task_group:
            # Start reader task to read from process stdout and send to server_to_client_send
            logger.debug(f"[Bridge {session_id}] Starting reader task")
            stdio_task_group.start_soon(
                stdio_reader, 
                proc.stdout, 
                server_to_client_send, 
                session_id, 
                timeout
            )
            
            # Start writer task to receive from client_to_server_recv and write to process stdin
            logger.debug(f"[Bridge {session_id}] Starting writer task")
            stdio_task_group.start_soon(
                stdio_writer, 
                proc.stdin, 
                client_to_server_recv, 
                session_id, 
                timeout
            )

            # Yield the session stream
            logger.info(f"[Bridge {session_id}] Streams bridged, yielding session stream")
            yield session_stream

            # On exit from context manager, ensure tasks are cancelled
            logger.info(f"[Bridge {session_id}] Exiting context manager, cancelling I/O tasks")
            stdio_task_group.cancel_scope.cancel()

    except (anyio.EndOfStream, anyio.ClosedResourceError) as e:
        logger.warning(f"[Bridge {session_id}] Stream closed unexpectedly: {e}")
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


class TestResult:
    """Represents the result of a test operation."""
    def __init__(self, success: bool, message: str, details: dict = None):
        self.success = success
        self.message = message
        self.details = details or {}
    
    def __str__(self):
        return f"{'SUCCESS' if self.success else 'FAILURE'}: {self.message}"


async def start_test_process() -> tuple[asyncio.subprocess.Process, str]:
    """Start the responder process and return it along with session ID."""
    # Ensure the responder script exists and is executable
    if not RESPONDER_SCRIPT.exists():
        raise FileNotFoundError(f"Responder script not found at {RESPONDER_SCRIPT}")
    
    # Make it executable
    RESPONDER_SCRIPT.chmod(0o755)
    logger.info(f"Using responder script: {RESPONDER_SCRIPT}")
    
    # Create a unique session ID
    session_id = f"test-{uuid.uuid4()}"
    logger.info(f"Starting responder process with session ID: {session_id}")
    
    # Configure environment with PYTHONPATH
    env = os.environ.copy()
    if 'PYTHONPATH' not in env:
        root_dir = Path(__file__).parent.parent.parent
        src_dir = root_dir / "src"
        env['PYTHONPATH'] = str(src_dir)
        logger.info(f"Setting PYTHONPATH to {env['PYTHONPATH']}")
    
    env['DEBUG'] = '1'  # Enable more verbose debug output
    
    # Start the process
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(RESPONDER_SCRIPT),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )
    logger.info(f"Responder process started with PID: {proc.pid}")
    
    return proc, session_id


async def setup_stderr_logger(proc: asyncio.subprocess.Process) -> tuple[asyncio.Task, asyncio.Task]:
    """Set up a task to log stderr output from the process."""
    stderr_queue = asyncio.Queue()
    
    async def stderr_reader():
        """Read stderr messages from the process and put them in a queue."""
        if not proc.stderr:
            return
        
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    logger.info("Stderr EOF reached")
                    break
                    
                stderr_line = line.decode('utf-8', errors='replace').rstrip()
                await stderr_queue.put(stderr_line)
        except Exception as e:
            logger.exception(f"Error in stderr reader: {e}")
    
    # Start stderr reader task
    stderr_task = asyncio.create_task(stderr_reader())
    
    # Start a task to log stderr messages
    async def log_stderr():
        """Log stderr messages from the queue."""
        try:
            while True:
                try:
                    line = await asyncio.wait_for(stderr_queue.get(), timeout=0.1)
                    logger.info(f"[STDERR] {line}")
                    stderr_queue.task_done()
                except asyncio.TimeoutError:
                    # No message, check if we should exit
                    if stderr_task.done():
                        if stderr_queue.empty():
                            break
                    await asyncio.sleep(0.1)
        except Exception as e:
            logger.exception(f"Error in stderr logger: {e}")
    
    # Start the stderr logger
    logger_task = asyncio.create_task(log_stderr())
    
    return stderr_task, logger_task


async def test_initialization(session_stream: StapledObjectStream[BaseModel]) -> TestResult:
    """Test the initialization process."""
    logger.info("=== Starting initialization test ===")
    
    # Create test initialization request
    init_id = f"init-{uuid.uuid4()}"
    init_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=init_id,
        method="initialize",
        params={}
    )
    
    # Send the initialization request
    logger.info(f"Sending initialization request: {init_request.model_dump_json()}")
    await session_stream.send(init_request)
    logger.info("Initialization request sent")
    
    # Wait for response with timeout
    logger.info("Waiting for initialization response...")
    try:
        response = await asyncio.wait_for(session_stream.receive(), timeout=5.0)
        logger.info(f"Received response: {response}")
        
        # Check if response is valid
        if not hasattr(response, 'id') or response.id != init_id:
            return TestResult(False, f"Response ID mismatch: expected={init_id}, got={getattr(response, 'id', 'unknown')}")
        
        # Check if we got an error
        if hasattr(response, 'error') and response.error:
            return TestResult(False, f"Received error response: {response.error}", {"error": response.error})
        
        # Check for expected result
        if not hasattr(response, 'result') or not response.result:
            return TestResult(False, "Response missing result field")
        
        if not isinstance(response.result, dict) or 'serverInfo' not in response.result:
            return TestResult(False, "Response missing serverInfo in result")
            
        logger.info("Initialization successful")
        return TestResult(True, "Initialization successful", {"server_info": response.result.get('serverInfo')})
        
    except asyncio.TimeoutError:
        return TestResult(False, "Timeout waiting for initialization response")
    except Exception as e:
        return TestResult(False, f"Unexpected error during initialization: {str(e)}", {"exception": str(e)})


async def test_echo(session_stream: StapledObjectStream[BaseModel]) -> TestResult:
    """Test the echo functionality."""
    logger.info("=== Starting echo test ===")
    
    # Create echo test request
    echo_id = f"echo-{uuid.uuid4()}"
    test_message = "Hello from stdio bridge test"
    echo_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=echo_id,
        method="test/echo",
        params={"message": test_message}
    )
    
    # Send echo request
    logger.info(f"Sending echo request: {echo_request.model_dump_json()}")
    await session_stream.send(echo_request)
    logger.info("Echo request sent")
    
    # Wait for echo response
    logger.info("Waiting for echo response...")
    try:
        echo_response = await asyncio.wait_for(session_stream.receive(), timeout=5.0)
        logger.info(f"Received echo response: {echo_response}")
        
        # Verify echo response
        if not hasattr(echo_response, 'id') or echo_response.id != echo_id:
            return TestResult(False, f"Echo response ID mismatch: expected={echo_id}, got={getattr(echo_response, 'id', 'unknown')}")
        
        # Check if we got an error
        if hasattr(echo_response, 'error') and echo_response.error:
            return TestResult(False, f"Received error response: {echo_response.error}", {"error": echo_response.error})
        
        # Check for expected result
        if not hasattr(echo_response, 'result') or not echo_response.result:
            return TestResult(False, "Echo response missing result field")
        
        result = echo_response.result
        expected_message = f"Echo: {test_message}"
        actual_message = result.get('message') if isinstance(result, dict) else None
        
        if actual_message != expected_message:
            return TestResult(
                False, 
                f"Echo response message mismatch: expected='{expected_message}', got='{actual_message}'",
                {"expected": expected_message, "actual": actual_message}
            )
            
        logger.info("Echo test passed!")
        return TestResult(True, "Echo test successful", {"message": actual_message})
        
    except asyncio.TimeoutError:
        return TestResult(False, "Timeout waiting for echo response")
    except Exception as e:
        return TestResult(False, f"Unexpected error during echo test: {str(e)}", {"exception": str(e)})


async def run_test():
    """Run the test of the stdio bridge functionality."""
    logger.info(f"====== Starting stdio bridge test (PID: {os.getpid()}) ======")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current directory: {os.getcwd()}")
    
    proc = None
    stderr_task = None
    logger_task = None
    exit_code = 1  # Default to failure
    
    try:
        # Start the process
        proc, session_id = await start_test_process()
        
        # Set up stderr logger
        stderr_task, logger_task = await setup_stderr_logger(proc)
        
        # Bridge the stdio streams
        logger.info("Creating stdio bridge")
        
        # Use the bridge within a single context
        async with bridge_stdio_to_anyio_session(proc, session_id, 5.0) as session_stream:
            logger.info("Bridge created successfully")
            
            # Wait a moment for everything to settle
            await asyncio.sleep(0.5)
            
            # Run initialization test
            init_result = await test_initialization(session_stream)
            logger.info(f"Initialization test: {init_result}")
            
            if not init_result.success:
                logger.error(f"Initialization failed: {init_result.message}")
                return 1
            
            # Run echo test
            echo_result = await test_echo(session_stream)
            logger.info(f"Echo test: {echo_result}")
            
            if not echo_result.success:
                logger.error(f"Echo test failed: {echo_result.message}")
                return 1
                
            logger.info("All tests passed successfully!")
            exit_code = 0
            
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
    except Exception as e:
        logger.exception(f"Error during test: {e}")
    finally:
        # Clean up
        logger.info("Cleaning up resources")
        
        # Cancel tasks
        if stderr_task or logger_task:
            logger.info("Cancelling tasks")
            tasks = [t for t in [stderr_task, logger_task] if t]
            for task in tasks:
                task.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.info("Tasks cancelled")
            except Exception as e:
                logger.warning(f"Error while cancelling tasks: {e}")
        
        # Terminate the process
        if proc and proc.returncode is None:
            logger.info(f"Terminating process {proc.pid}")
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
                logger.info(f"Process terminated with code {proc.returncode}")
            except asyncio.TimeoutError:
                logger.warning("Process didn't terminate gracefully, killing")
                proc.kill()
                await proc.wait()
                logger.info(f"Process killed")
        
        logger.info("Cleanup complete")
        logger.info(f"====== Test completed with exit code {exit_code} ======")
        
        return exit_code

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(run_test())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unhandled exception in main: {e}")
        sys.exit(1) 