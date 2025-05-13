"""
Test for the stdio bridge functionality.
Tests the bridge between asyncio stdio processes and anyio streams.
"""

import asyncio
import json
import logging
import pytest
import anyio
from pathlib import Path
import uuid
import os
import sys

from tests.fixtures.bridges.bridge_utils import (
    JSONRPCRequest,
    DynamicResponse,
    TestResult,
    bridge_stdio_to_anyio_session,
    send_jsonrpc_request,
    wait_for_jsonrpc_response,
    request_response,
    initialize_session,
    setup_stderr_logger,
    DEFAULT_IO_TIMEOUT
)

logger = logging.getLogger(__name__)

# Define fixtures directory paths
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "mcp_servers" / "simple_responder.py"

# Timeouts for test operations
TEST_TIMEOUT = 15  # seconds for the whole test
OPERATION_TIMEOUT = 5  # seconds for individual operations

@pytest.fixture
async def session_stream(simple_responder_process):
    """Fixture to provide a bridged session stream to the simple responder process."""
    # Set up a bridge to the process
    logger.info("Setting up session stream fixture")
    session_id = f"test-{uuid.uuid4()}"
    
    async with bridge_stdio_to_anyio_session(
        simple_responder_process, session_id, timeout=10.0
    ) as stream:
        yield stream

@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_bridge_functionality(simple_responder_process):
    """Test bridging a stdio process to anyio streams."""
    # Set up stderr logging
    stderr_task = await setup_stderr_logger(simple_responder_process)
    
    try:
        # Bridge the stdio process
        session_id = f"test-bridge-{uuid.uuid4()}"
        logger.info(f"Starting bridge test with session ID: {session_id}")
        
        async with bridge_stdio_to_anyio_session(
            simple_responder_process, session_id, timeout=OPERATION_TIMEOUT
        ) as session_stream:
            # Create and send a test request
            test_params = {"message": "Hello through bridge"}
            request_id, _ = await send_jsonrpc_request(
                stream=session_stream,
                method="test/echo",
                params=test_params,
                timeout=OPERATION_TIMEOUT
            )
            
            # Wait for the response
            response = await wait_for_jsonrpc_response(
                stream=session_stream,
                request_id=request_id,
                timeout=OPERATION_TIMEOUT
            )
            
            # Verify the response
            assert response.id == request_id
            assert response.result is not None
            assert "message" in response.result
            
            # The responder prepends "Echo: " to the message
            expected_message = f"Echo: {test_params['message']}"
            assert response.result["message"] == expected_message
            
            logger.info(f"Test completed successfully with response: {response}")
    
    finally:
        # Clean up stderr logger
        if stderr_task:
            stderr_task.cancel()
            try:
                await asyncio.wait_for(stderr_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_bridge_initialization(simple_responder_process):
    """Test the initialization process through a bridge."""
    # Set up stderr logging
    stderr_task = await setup_stderr_logger(simple_responder_process)
    
    try:
        # Bridge the stdio process
        session_id = f"test-init-{uuid.uuid4()}"
        logger.info(f"Starting initialization test with session ID: {session_id}")
        
        async with bridge_stdio_to_anyio_session(
            simple_responder_process, session_id, timeout=OPERATION_TIMEOUT
        ) as session_stream:
            # Use the helper function to initialize the session
            init_success = await initialize_session(
                stream=session_stream,
                timeout=OPERATION_TIMEOUT
            )
            
            # Verify initialization succeeded
            assert init_success, "Session initialization should succeed"
            
            logger.info("Initialization test completed successfully")
    
    finally:
        # Clean up stderr logger
        if stderr_task:
            stderr_task.cancel()
            try:
                await asyncio.wait_for(stderr_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_full_session_lifecycle(simple_responder_process):
    """Test a complete session lifecycle with initialization, request, and shutdown."""
    # Set up stderr logging
    stderr_task = await setup_stderr_logger(simple_responder_process)
    
    try:
        # Bridge the stdio process
        session_id = f"test-lifecycle-{uuid.uuid4()}"
        logger.info(f"Starting lifecycle test with session ID: {session_id}")
        
        async with bridge_stdio_to_anyio_session(
            simple_responder_process, session_id, timeout=OPERATION_TIMEOUT
        ) as session_stream:
            # 1. Initialize the session
            init_success = await initialize_session(
                stream=session_stream,
                timeout=OPERATION_TIMEOUT
            )
            assert init_success, "Session initialization should succeed"
            
            # 2. Send an echo request
            echo_response = await request_response(
                stream=session_stream,
                method="test/echo",
                params={"message": "Testing full lifecycle"},
                timeout=OPERATION_TIMEOUT
            )
            assert echo_response.result is not None
            assert echo_response.result["message"] == "Testing full lifecycle"
            
            # 3. Send a shutdown request
            shutdown_response = await request_response(
                stream=session_stream,
                method="shutdown",
                timeout=OPERATION_TIMEOUT
            )
            assert shutdown_response.result is not None
            
            logger.info("Lifecycle test completed successfully")
    
    finally:
        # Clean up stderr logger
        if stderr_task:
            stderr_task.cancel()
            try:
                await asyncio.wait_for(stderr_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

async def start_test_process() -> tuple[asyncio.subprocess.Process, str]:
    """Start the responder process and return it along with session ID."""
    # Get the path to the responder script
    responder_script = FIXTURES_DIR / "mcp_servers" / "simple_responder.py"
    
    # Ensure the responder script exists and is executable
    if not responder_script.exists():
        raise FileNotFoundError(f"Responder script not found at {responder_script}")
    
    # Make it executable
    responder_script.chmod(0o755)
    logger.info(f"Using responder script: {responder_script}")
    
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
        str(responder_script),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )
    logger.info(f"Responder process started with PID: {proc.pid}")
    
    # Wait a moment for startup
    await asyncio.sleep(0.5)
    
    return proc, session_id

@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_bridge_with_initialization_and_echo():
    """Test the complete bridge flow with initialization and echo."""
    logger.info("=== Starting complete bridge test ===")
    
    # Get the path to the responder script
    if not RESPONDER_SCRIPT.exists():
        logger.error(f"Responder script not found at {RESPONDER_SCRIPT}")
        pytest.fail(f"Responder script not found at {RESPONDER_SCRIPT}")
    
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
    
    # Wait a moment for startup
    await asyncio.sleep(0.5)
    
    # Set up stderr logging
    stderr_task, logger_task = await setup_stderr_logger(proc)
    
    try:
        # Set up the bridge
        async with bridge_stdio_to_anyio_session(proc, session_id, timeout=10.0) as stream:
            # Test initialization
            init_request = JSONRPCRequest(
                jsonrpc="2.0",
                id=f"init-{uuid.uuid4()}",
                method="initialize",
                params={}
            )
            
            # Send initialization request
            logger.info(f"Sending initialization request: {init_request}")
            with anyio.move_on_after(OPERATION_TIMEOUT):
                await stream.send(init_request)
            
            # Wait for initialization response
            init_response = None
            with anyio.move_on_after(OPERATION_TIMEOUT):
                init_response = await stream.receive()
            
            # Verify initialization
            assert init_response is not None, "Timeout waiting for initialization response"
            assert init_response.id == init_request.id, f"Init response ID mismatch: {init_response.id} != {init_request.id}"
            assert hasattr(init_response, "result"), f"Init response has no result field: {init_response}"
            assert "serverInfo" in init_response.result, "Init response missing serverInfo"
            
            logger.info(f"Initialization successful: {init_response.result['serverInfo']}")
            
            # Test echo
            echo_request = JSONRPCRequest(
                jsonrpc="2.0",
                id=f"echo-{uuid.uuid4()}",
                method="test/echo",
                params={
                    "message": "Hello from complete test"
                }
            )
            
            # Send echo request
            logger.info(f"Sending echo request: {echo_request}")
            with anyio.move_on_after(OPERATION_TIMEOUT):
                await stream.send(echo_request)
            
            # Wait for echo response
            echo_response = None
            with anyio.move_on_after(OPERATION_TIMEOUT):
                echo_response = await stream.receive()
            
            # Verify echo response
            assert echo_response is not None, "Timeout waiting for echo response"
            assert echo_response.id == echo_request.id, f"Echo response ID mismatch: {echo_response.id} != {echo_request.id}"
            assert hasattr(echo_response, "result"), f"Echo response has no result field: {echo_response}"
            assert "message" in echo_response.result, "Echo response missing message"
            assert echo_response.result["message"] == f"Echo: {echo_request.params['message']}", \
                f"Echo response message mismatch: {echo_response.result['message']}"
            
            logger.info("Echo test successful!")
            logger.info("Complete bridge test successful!")
    finally:
        # Clean up
        logger.info("Cancelling stderr tasks")
        if stderr_task:
            stderr_task.cancel()
        if logger_task:
            logger_task.cancel()
        
        # Terminate the process
        logger.info("Terminating process")
        if proc.returncode is None:
            try:
                proc.terminate()
                try:
                    with anyio.move_on_after(1.0):
                        await proc.wait()
                except asyncio.TimeoutError:
                    logger.warning("Process did not terminate gracefully, killing")
                    proc.kill()
                    try:
                        with anyio.move_on_after(1.0):
                            await proc.wait()
                    except asyncio.TimeoutError:
                        logger.error("Failed to kill process")
            except Exception as e:
                logger.warning(f"Error terminating process: {e}") 