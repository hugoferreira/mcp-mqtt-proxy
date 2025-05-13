"""Common pytest fixtures for all tests."""

import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path

import pytest
import anyio
from typing import AsyncGenerator, Tuple

# Set up logging for tests
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("tests")

# Paths to fixture scripts
FIXTURES_DIR = Path(__file__).parent / "fixtures"
MCP_SERVERS_DIR = FIXTURES_DIR / "mcp_servers"

# Default timeouts
DEFAULT_FIXTURE_TIMEOUT = 10.0  # seconds
DEFAULT_PROCESS_STARTUP_TIMEOUT = 2.0  # seconds
DEFAULT_PROCESS_SHUTDOWN_TIMEOUT = 2.0  # seconds

def make_executable(script_path: Path) -> None:
    """Make a script executable."""
    if os.name != 'nt':  # Skip on Windows
        script_path.chmod(0o755)

@pytest.fixture
def random_id() -> str:
    """Generate a random ID for test uniqueness."""
    return f"test-{uuid.uuid4()}"

@pytest.fixture
def random_topic_prefix() -> str:
    """Generate a random topic prefix for MQTT tests."""
    return f"mcp/test/{uuid.uuid4().hex[:8]}"

@pytest.fixture
async def simple_responder_path() -> Path:
    """Path to the simple_responder.py script."""
    script_path = MCP_SERVERS_DIR / "simple_responder.py"
    
    # Ensure the script directory exists
    script_path.parent.mkdir(parents=True, exist_ok=True)
    
    # If script doesn't exist in fixtures/mcp_servers/, copy from integration/fixtures/
    if not script_path.exists():
        original_path = Path(__file__).parent / "integration" / "fixtures" / "simple_responder.py"
        if original_path.exists():
            script_path.write_text(original_path.read_text())
            logger.info(f"Copied simple_responder.py from {original_path} to {script_path}")
        else:
            raise FileNotFoundError(f"Simple responder script not found at {original_path}")
    
    # Make it executable
    make_executable(script_path)
    
    return script_path

@pytest.fixture
async def notification_responder_path() -> Path:
    """Path to the notification_responder.py script."""
    script_path = MCP_SERVERS_DIR / "notification_responder.py"
    
    # Ensure the script directory exists
    script_path.parent.mkdir(parents=True, exist_ok=True)
    
    # If script doesn't exist in fixtures/mcp_servers/, copy from integration/fixtures/
    if not script_path.exists():
        original_path = Path(__file__).parent / "integration" / "fixtures" / "notification_responder.py"
        if original_path.exists():
            script_path.write_text(original_path.read_text())
            logger.info(f"Copied notification_responder.py from {original_path} to {script_path}")
        else:
            raise FileNotFoundError(f"Notification responder script not found at {original_path}")
    
    # Make it executable
    make_executable(script_path)
    
    return script_path

@pytest.fixture
async def simple_responder_process(simple_responder_path: Path) -> AsyncGenerator[asyncio.subprocess.Process, None]:
    """Start a simple_responder.py process and yield it for testing."""
    logger.info(f"Starting simple responder process: {simple_responder_path}")
    
    # Start the process
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(simple_responder_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    logger.info(f"Simple responder process started with PID {process.pid}")
    
    # Set up a task to log stderr for debugging
    stderr_task = None
    if process.stderr:
        async def log_stderr():
            """Read and log stderr from the process."""
            while True:
                try:
                    # Use a timeout to prevent hanging if stderr pipe breaks
                    line = await asyncio.wait_for(
                        process.stderr.readline(), 
                        timeout=DEFAULT_FIXTURE_TIMEOUT
                    )
                    if not line:
                        break
                    logger.debug(f"[Simple Responder] {line.decode('utf-8', errors='replace').rstrip()}")
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout reading stderr from PID {process.pid}")
                    break
                except Exception as e:
                    logger.error(f"Error reading stderr from PID {process.pid}: {e}")
                    break
                
        stderr_task = asyncio.create_task(log_stderr())
    
    try:
        # Wait briefly to ensure the process is running (with timeout)
        try:
            await asyncio.wait_for(
                asyncio.sleep(0.5),  # Short delay
                timeout=DEFAULT_PROCESS_STARTUP_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for process startup")
            raise
        
        # Check if the process started successfully
        if process.returncode is not None:
            raise RuntimeError(f"Simple responder process failed to start (exit code: {process.returncode})")
        
        yield process
        
    finally:
        # Cleanup
        logger.info(f"Terminating simple responder process (PID {process.pid})")
        
        # Cancel stderr logging task if it exists
        if stderr_task and not stderr_task.done():
            stderr_task.cancel()
            try:
                # Wait with timeout for task cancellation
                await asyncio.wait_for(stderr_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        
        # Terminate the process if it's still running
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(
                    process.wait(), 
                    timeout=DEFAULT_PROCESS_SHUTDOWN_TIMEOUT
                )
                logger.info(f"Simple responder process terminated with exit code {process.returncode}")
            except asyncio.TimeoutError:
                logger.warning("Process did not terminate gracefully within timeout, killing")
                process.kill()
                try:
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.error("Process did not respond to kill signal within timeout")
                logger.info(f"Simple responder process killed")

@pytest.fixture
def event_loop_policy():
    """Override the default event loop policy for tests."""
    # Use the default policy (often sufficient)
    policy = asyncio.get_event_loop_policy()
    
    # Return current policy or configure a specific one here
    return policy

@pytest.fixture(scope="session")
def event_loop_timeout():
    """Set the timeout for asyncio operations in the event loop."""
    return DEFAULT_FIXTURE_TIMEOUT 