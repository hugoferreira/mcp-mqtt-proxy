"""Common pytest fixtures for all tests."""

import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import AsyncGenerator, Dict, Optional, Tuple, Any

import anyio
import pytest

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("tests")

# Define paths to fixtures and scripts
ROOT_DIR = Path(__file__).parent.parent
FIXTURES_DIR = Path(__file__).parent / "fixtures"
MCP_SERVERS_DIR = FIXTURES_DIR / "mcp_servers"
BRIDGES_DIR = FIXTURES_DIR / "bridges"
MQTT_FIXTURES_DIR = FIXTURES_DIR / "mqtt"

# Default timeouts for operations
DEFAULT_OPERATION_TIMEOUT = 5.0  # seconds for individual operations
DEFAULT_TEST_TIMEOUT = 15.0  # seconds for entire tests
DEFAULT_PROCESS_STARTUP_TIMEOUT = 2.0  # seconds to wait for process startup
DEFAULT_PROCESS_SHUTDOWN_TIMEOUT = 2.0  # seconds to wait for process shutdown

# Configure pytest markers
def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (generally avoid running these)")
    config.addinivalue_line("markers", "performance: marks tests as performance tests (resource intensive)")
    config.addinivalue_line("markers", "mqtt: marks tests that require an MQTT broker")

# Helper functions
def make_executable(script_path: Path) -> None:
    """Make a script executable."""
    if os.name != 'nt':  # Skip on Windows
        script_path.chmod(0o755)

# Utility fixtures
@pytest.fixture
def random_id() -> str:
    """Generate a random ID for test uniqueness."""
    return f"test-{uuid.uuid4()}"

@pytest.fixture
def test_base_topic(random_id: str) -> str:
    """Create a unique MQTT base topic for tests."""
    return f"mcp/test/{random_id}"

# Process management fixtures
@pytest.fixture
async def simple_responder_path() -> Path:
    """Get the path to the simple_responder.py script."""
    responder_script = MCP_SERVERS_DIR / "simple_responder.py"
    
    if not responder_script.exists():
        pytest.fail(f"Responder script not found at {responder_script}")
    
    make_executable(responder_script)
    return responder_script

@pytest.fixture
async def simple_responder_process(simple_responder_path: Path) -> AsyncGenerator[asyncio.subprocess.Process, None]:
    """Start a simple responder process as a pytest fixture."""
    # Configure environment with PYTHONPATH
    env = os.environ.copy()
    if 'PYTHONPATH' not in env:
        src_dir = ROOT_DIR / "src"
        env['PYTHONPATH'] = str(src_dir)
        logger.info(f"Setting PYTHONPATH to {env['PYTHONPATH']}")
    
    # Start the process
    proc = await asyncio.create_subprocess_exec(
        sys.executable,  # Use the current Python executable
        str(simple_responder_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )
    logger.info(f"Started responder process with PID: {proc.pid}")
    
    # Wait for process to initialize
    await asyncio.sleep(DEFAULT_PROCESS_STARTUP_TIMEOUT)
    
    yield proc
    
    # Clean up the process
    if proc.returncode is None:
        logger.info(f"Terminating process {proc.pid}")
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=DEFAULT_PROCESS_SHUTDOWN_TIMEOUT)
            logger.info(f"Process terminated with code {proc.returncode}")
        except asyncio.TimeoutError:
            logger.warning(f"Process {proc.pid} didn't terminate gracefully, killing")
            proc.kill()
            await proc.wait()
            logger.info(f"Process killed")

# MQTT fixtures
@pytest.fixture
def mqtt_broker_host() -> str:
    """Get MQTT broker host from environment or use default."""
    return os.environ.get("MQTT_BROKER_HOST", "localhost")

@pytest.fixture
def mqtt_broker_port() -> int:
    """Get MQTT broker port from environment or use default."""
    return int(os.environ.get("MQTT_BROKER_PORT", "1883"))

@pytest.fixture
def mqtt_broker_url(mqtt_broker_host: str, mqtt_broker_port: int) -> str:
    """Create MQTT broker URL from host and port."""
    return f"mqtt://{mqtt_broker_host}:{mqtt_broker_port}"

# Test data fixtures
@pytest.fixture
def test_request_payload() -> Dict[str, Any]:
    """Create a test request payload."""
    return {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "test/echo",
        "params": {
            "message": "Hello from test"
        }
    }

@pytest.fixture
def test_notification_payload() -> Dict[str, Any]:
    """Create a test notification payload."""
    return {
        "jsonrpc": "2.0",
        "method": "status/update",
        "params": {
            "status": "running",
            "timestamp": "2023-01-01T00:00:00Z"
        }
    }

@pytest.fixture
def random_topic_prefix() -> str:
    """Generate a random topic prefix for MQTT tests."""
    return f"mcp/test/{uuid.uuid4().hex[:8]}"

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
def event_loop_policy():
    """Override the default event loop policy for tests."""
    # Use the default policy (often sufficient)
    policy = asyncio.get_event_loop_policy()
    
    # Return current policy or configure a specific one here
    return policy

@pytest.fixture(scope="session")
def event_loop_timeout():
    """Set the timeout for asyncio operations in the event loop."""
    return DEFAULT_TEST_TIMEOUT 