"""System integration tests for the full MCP MQTT Proxy.

These tests run both publisher and listener components together with a real 
broker and test end-to-end communication.
"""

import asyncio
import json
import logging
import os
import pytest
import pytest_asyncio
import subprocess
import sys
import uuid
from pathlib import Path
from typing import AsyncGenerator, Tuple, Dict, Any, Optional, List

from mcp.types import JSONRPCRequest, JSONRPCResponse, JSONRPCError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use pytest-asyncio for async tests
pytestmark = pytest.mark.asyncio

# Default MQTT broker URL - assumes mosquitto running locally
DEFAULT_MQTT_BROKER_URL = "mqtt://localhost:1883"
# Generate a unique base topic for this test run to avoid interference
TEST_BASE_TOPIC = f"mcp/test/system_integration/{uuid.uuid4()}"
# Default QoS
DEFAULT_QOS = 1

# Path to the simple_responder.py script for testing
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "simple_responder.py"


@pytest.fixture(scope="module")
def ensure_responder_script():
    """Ensures the simple_responder.py test script exists."""
    FIXTURES_DIR.mkdir(exist_ok=True)
    
    if not RESPONDER_SCRIPT.exists():
        logger.info(f"Creating test responder script at {RESPONDER_SCRIPT}")
        with open(RESPONDER_SCRIPT, "w") as f:
            f.write('''#!/usr/bin/env python
"""Simple MCP responder script for testing the MQTT Listener."""

import asyncio
import json
import logging
import sys
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.DEBUG, 
                   format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                   stream=sys.stderr)
logger = logging.getLogger("simple_responder")

# Keep track of received requests for testing
received_requests = []

async def process_request(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single MCP request and return a response."""
    # Log and store the request
    logger.info(f"Received request: {request_data}")
    received_requests.append(request_data)
    
    # Basic validation
    if not all(k in request_data for k in ["jsonrpc", "id", "method"]):
        logger.error("Invalid request format")
        return {
            "jsonrpc": "2.0",
            "id": request_data.get("id"),
            "error": {
                "code": -32600,
                "message": "Invalid Request",
            }
        }
    
    # Process based on method
    method = request_data["method"]
    request_id = request_data["id"]
    params = request_data.get("params", {})
    
    if method == "test/echo":
        # Echo the message back
        message = params.get("message", "")
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"message": f"Echo: {message}"}
        }
    elif method == "test/error":
        # Return an error response
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": 1001,
                "message": "Test error message",
                "data": {"details": "This is a test error"}
            }
        }
    elif method == "test/delay":
        # Simulate a delayed response
        delay = params.get("delay", 1.0)
        logger.info(f"Delaying response for {delay} seconds")
        await asyncio.sleep(delay)
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"message": f"Delayed response after {delay}s"}
        }
    else:
        # Unknown method
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}",
            }
        }

async def run_server():
    """Run the MCP server using stdio."""
    logger.info("Starting simple MCP responder")
    
    # Set up stdin/stdout
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    loop = asyncio.get_running_loop()
    
    # Connect to stdin
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    
    # Connect to stdout
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    
    logger.info("Server initialized, waiting for requests")
    
    try:
        while True:
            # Read request line
            request_line = await reader.readline()
            if not request_line:
                logger.info("Received EOF, exiting")
                break
                
            try:
                # Parse request
                request_str = request_line.decode("utf-8").strip()
                if not request_str:
                    continue
                    
                request_data = json.loads(request_str)
                
                # Process request
                response_data = await process_request(request_data)
                
                # Send response
                response_str = json.dumps(response_data) + "\\n"
                writer.write(response_str.encode("utf-8"))
                await writer.drain()
                logger.info(f"Sent response: {response_data}")
                
            except json.JSONDecodeError:
                logger.exception("Failed to parse request JSON")
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": "Parse error",
                    }
                }
                writer.write((json.dumps(error_response) + "\\n").encode("utf-8"))
                await writer.drain()
            except Exception as e:
                logger.exception(f"Error processing request: {e}")
    except asyncio.CancelledError:
        logger.info("Server task cancelled")
    finally:
        logger.info("Server shutting down")
        # Close the writer
        writer.close()

if __name__ == "__main__":
    logger.info("Simple MCP responder starting")
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info("Server interrupted")
    except Exception as e:
        logger.exception(f"Server error: {e}")
    finally:
        logger.info("Server exited")
''')
    
    # Make it executable
    RESPONDER_SCRIPT.chmod(0o755)
    return RESPONDER_SCRIPT


@pytest_asyncio.fixture
async def mcp_mqtt_system(ensure_responder_script) -> AsyncGenerator[Tuple[asyncio.subprocess.Process, asyncio.subprocess.Process], None]:
    """
    Start both the listener and publisher components for full system testing.
    
    Returns:
        Tuple of (listener_process, publisher_process)
    """
    responder_script = ensure_responder_script
    
    # Create unique client IDs for this test
    listener_client_id = f"test-listener-{uuid.uuid4()}"
    publisher_client_id = f"test-publisher-{uuid.uuid4()}"
    
    # Build command for the listener
    listener_cmd = [
        sys.executable, "-m", "mcp_mqtt_proxy",
        "listen",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", listener_client_id,
        "--qos", str(DEFAULT_QOS),
        "--mcp-timeout", "10.0",
        "--debug",
        "--", str(responder_script)
    ]
    
    # Start the listener process first
    logger.info(f"Starting listener process: {' '.join(listener_cmd)}")
    listener_process = await asyncio.create_subprocess_exec(
        *listener_cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Start a task to log listener stderr
    async def log_listener_stderr():
        if listener_process.stderr:
            while True:
                line = await listener_process.stderr.readline()
                if not line:
                    break
                logger.info(f"Listener stderr: {line.decode().strip()}")
    
    listener_stderr_task = asyncio.create_task(log_listener_stderr())
    
    # Wait a moment for the listener to connect to MQTT
    await asyncio.sleep(3.0)
    
    # Build command for the publisher
    publisher_cmd = [
        sys.executable, "-m", "mcp_mqtt_proxy",
        "publish",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", publisher_client_id,
        "--qos", str(DEFAULT_QOS),
        "--debug-timeout", "30.0",  # Auto-exit after 30 seconds for tests
        "--debug"
    ]
    
    # Start the publisher process
    logger.info(f"Starting publisher process: {' '.join(publisher_cmd)}")
    publisher_process = await asyncio.create_subprocess_exec(
        *publisher_cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Start a task to log publisher stderr
    async def log_publisher_stderr():
        if publisher_process.stderr:
            while True:
                line = await publisher_process.stderr.readline()
                if not line:
                    break
                logger.info(f"Publisher stderr: {line.decode().strip()}")
    
    publisher_stderr_task = asyncio.create_task(log_publisher_stderr())
    
    # Wait a moment for the publisher to connect to MQTT
    await asyncio.sleep(3.0)
    
    try:
        yield (listener_process, publisher_process)
    finally:
        logger.info("Cleaning up system test processes")
        
        # Cancel stderr logging tasks
        listener_stderr_task.cancel()
        publisher_stderr_task.cancel()
        try:
            await asyncio.gather(
                listener_stderr_task, 
                publisher_stderr_task, 
                return_exceptions=True
            )
        except asyncio.CancelledError:
            pass
        
        # Terminate processes
        for process, name in [
            (publisher_process, "publisher"), 
            (listener_process, "listener")
        ]:
            if process.returncode is None:
                try:
                    logger.info(f"Terminating {name} process")
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                    logger.info(f"{name.capitalize()} process exited with code {process.returncode}")
                except asyncio.TimeoutError:
                    logger.warning(f"{name.capitalize()} process did not terminate gracefully, killing")
                    process.kill()
                    await process.wait()


async def test_full_system_echo(mcp_mqtt_system):
    """
    Test a full end-to-end request-response cycle through the MCP MQTT proxy.
    
    This test:
    1. Sends a request to the publisher's stdin
    2. The publisher publishes it to MQTT
    3. The listener receives it from MQTT
    4. The listener sends it to the responder script
    5. The responder sends a response back
    6. The listener publishes the response to MQTT
    7. The publisher receives it from MQTT
    8. The publisher writes it to stdout
    9. We verify the response
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Create a test request for the echo method
    test_request_id = str(uuid.uuid4())
    test_request = {
        "jsonrpc": "2.0",
        "id": test_request_id,
        "method": "test/echo",
        "params": {"message": "Hello from system test"}
    }
    
    # Send the request to the publisher's stdin
    request_json = json.dumps(test_request) + "\n"
    publisher_process.stdin.write(request_json.encode())
    await publisher_process.stdin.drain()
    logger.info(f"Sent request to publisher: {request_json.strip()}")
    
    # Wait for and read the response from publisher's stdout
    response_line = await asyncio.wait_for(
        publisher_process.stdout.readline(), 
        timeout=15.0
    )
    
    # Parse and validate the response
    response_json = response_line.decode().strip()
    logger.info(f"Received response from publisher: {response_json}")
    response = json.loads(response_json)
    
    # Verify the response
    assert response.get("jsonrpc") == "2.0", "Invalid jsonrpc version"
    assert response.get("id") == test_request_id, "Response ID does not match request ID"
    assert "result" in response, "Response does not contain result field"
    assert response["result"] == {"message": "Echo: Hello from system test"}, "Unexpected result"


async def test_full_system_error(mcp_mqtt_system):
    """
    Test the full error handling path through the MCP MQTT proxy.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Create a test request that will trigger an error
    test_request_id = str(uuid.uuid4())
    test_request = {
        "jsonrpc": "2.0",
        "id": test_request_id,
        "method": "test/error",
        "params": {"trigger": "error"}
    }
    
    # Send the request to the publisher's stdin
    request_json = json.dumps(test_request) + "\n"
    publisher_process.stdin.write(request_json.encode())
    await publisher_process.stdin.drain()
    logger.info(f"Sent error request to publisher: {request_json.strip()}")
    
    # Wait for and read the response from publisher's stdout
    response_line = await asyncio.wait_for(
        publisher_process.stdout.readline(), 
        timeout=15.0
    )
    
    # Parse and validate the response
    response_json = response_line.decode().strip()
    logger.info(f"Received error response from publisher: {response_json}")
    response = json.loads(response_json)
    
    # Verify the error response
    assert response.get("jsonrpc") == "2.0", "Invalid jsonrpc version"
    assert response.get("id") == test_request_id, "Response ID does not match request ID"
    assert "error" in response, "Response does not contain error field"
    
    error = response["error"]
    assert error["code"] == 1001, "Unexpected error code"
    assert error["message"] == "Test error message", "Unexpected error message"
    assert error["data"] == {"details": "This is a test error"}, "Unexpected error data"


async def test_full_system_delay(mcp_mqtt_system):
    """
    Test the handling of delayed responses through the MCP MQTT proxy.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Create a test request that will trigger a delayed response
    test_request_id = str(uuid.uuid4())
    test_request = {
        "jsonrpc": "2.0",
        "id": test_request_id,
        "method": "test/delay",
        "params": {"delay": 2.0}  # 2 second delay
    }
    
    # Record start time
    start_time = asyncio.get_event_loop().time()
    
    # Send the request to the publisher's stdin
    request_json = json.dumps(test_request) + "\n"
    publisher_process.stdin.write(request_json.encode())
    await publisher_process.stdin.drain()
    logger.info(f"Sent delayed request to publisher: {request_json.strip()}")
    
    # Wait for and read the response from publisher's stdout
    response_line = await asyncio.wait_for(
        publisher_process.stdout.readline(), 
        timeout=15.0
    )
    
    # Calculate elapsed time
    elapsed_time = asyncio.get_event_loop().time() - start_time
    
    # Parse and validate the response
    response_json = response_line.decode().strip()
    logger.info(f"Received delayed response from publisher after {elapsed_time:.2f}s: {response_json}")
    response = json.loads(response_json)
    
    # Verify the response
    assert response.get("jsonrpc") == "2.0", "Invalid jsonrpc version"
    assert response.get("id") == test_request_id, "Response ID does not match request ID"
    assert "result" in response, "Response does not contain result field"
    assert response["result"] == {"message": "Delayed response after 2.0s"}, "Unexpected result"
    assert elapsed_time >= 2.0, f"Response came too quickly: {elapsed_time:.2f}s vs expected 2.0s"


async def test_multiple_sequential_requests(mcp_mqtt_system):
    """
    Test sending multiple sequential requests through the MCP MQTT proxy.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Create multiple test requests
    requests = [
        {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": "test/echo",
            "params": {"message": f"Sequential message {i}"}
        }
        for i in range(5)
    ]
    
    # Send all requests and collect responses
    responses = []
    for i, request in enumerate(requests):
        # Send the request
        request_json = json.dumps(request) + "\n"
        publisher_process.stdin.write(request_json.encode())
        await publisher_process.stdin.drain()
        logger.info(f"Sent request {i+1}: {request_json.strip()}")
        
        # Read the response
        response_line = await asyncio.wait_for(
            publisher_process.stdout.readline(),
            timeout=15.0
        )
        response_json = response_line.decode().strip()
        logger.info(f"Received response {i+1}: {response_json}")
        responses.append(json.loads(response_json))
    
    # Verify all responses
    assert len(responses) == len(requests), f"Expected {len(requests)} responses, got {len(responses)}"
    
    for i, (request, response) in enumerate(zip(requests, responses)):
        assert response.get("jsonrpc") == "2.0", f"Invalid jsonrpc version in response {i+1}"
        assert response.get("id") == request["id"], f"Response ID does not match request ID in response {i+1}"
        assert "result" in response, f"Response {i+1} does not contain result field"
        assert response["result"] == {"message": f"Echo: Sequential message {i}"}, f"Unexpected result in response {i+1}"


async def test_concurrent_requests(mcp_mqtt_system):
    """
    Test sending multiple concurrent requests through the MCP MQTT proxy.
    This verifies that the system can handle multiple in-flight requests at once.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Create multiple test requests
    num_requests = 5
    requests = [
        {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": "test/delay",  # Use delayed responses to ensure they're concurrent
            "params": {"delay": 1.0}  # 1 second delay for all requests
        }
        for _ in range(num_requests)
    ]
    
    # Keep track of request IDs for matching responses
    request_ids = {req["id"]: i for i, req in enumerate(requests)}
    
    # Send all requests without waiting for responses
    start_time = asyncio.get_event_loop().time()
    for i, request in enumerate(requests):
        request_json = json.dumps(request) + "\n"
        publisher_process.stdin.write(request_json.encode())
        await publisher_process.stdin.drain()
        logger.info(f"Sent concurrent request {i+1}: {request_json.strip()}")
    
    # Read and collect all responses (may come back in any order)
    responses = []
    for _ in range(num_requests):
        response_line = await asyncio.wait_for(
            publisher_process.stdout.readline(),
            timeout=15.0
        )
        response_json = response_line.decode().strip()
        response = json.loads(response_json)
        responses.append(response)
        logger.info(f"Received response: {response_json}")
    
    # Calculate total elapsed time
    elapsed_time = asyncio.get_event_loop().time() - start_time
    
    # Verify all responses were received
    assert len(responses) == num_requests, f"Expected {num_requests} responses, got {len(responses)}"
    
    # Verify each response matches a request
    for response in responses:
        assert response.get("jsonrpc") == "2.0", "Invalid jsonrpc version"
        assert response.get("id") in request_ids, f"Unknown response ID: {response.get('id')}"
        assert "result" in response, "Response does not contain result field"
        assert response["result"] == {"message": "Delayed response after 1.0s"}, "Unexpected result"
    
    # Verify that processing was concurrent by checking total elapsed time
    # If all 5 requests with 1s delay each took less than 5s total, they must have been processed concurrently
    assert elapsed_time < 5.0, f"Total elapsed time {elapsed_time:.2f}s suggests requests were not processed concurrently"
    # Should be around ~2s for all requests (startup + 1s delay for processing)
    logger.info(f"Total elapsed time for {num_requests} concurrent requests: {elapsed_time:.2f}s") 