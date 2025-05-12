"""Performance tests for the MCP MQTT Proxy.

These tests measure throughput, latency, and stability under load.
They require a local MQTT broker to run.
"""

import asyncio
import json
import logging
import pytest
import pytest_asyncio
import statistics
import time
import uuid
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Tuple, Any, Optional

from mcp.types import JSONRPCRequest, JSONRPCResponse
from mcp_mqtt_proxy.utils import generate_id

# Set up logging - use lower level for perf tests to reduce output
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Configure for performance testing
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.performance,  # Custom marker for performance tests
]

# Default MQTT broker URL - assumes mosquitto running locally
DEFAULT_MQTT_BROKER_URL = "mqtt://localhost:1883"
# Generate a unique base topic for this test run
TEST_BASE_TOPIC = f"mcp/test/performance/{uuid.uuid4()}"
# Default QoS
DEFAULT_QOS = 1

# Path to the echo_server.py script for testing
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR.parent / "integration" / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "simple_responder.py"

# Test configurations
SMALL_PAYLOAD_SIZE = 100  # bytes
MEDIUM_PAYLOAD_SIZE = 5 * 1024  # 5KB
LARGE_PAYLOAD_SIZE = 50 * 1024  # 50KB

# Test repetitions for more reliable results
NUM_ITERATIONS = 50  # For throughput tests
WARMUP_ITERATIONS = 5  # Warmup iterations to exclude from measurements


@pytest.fixture(scope="module")
def ensure_responder_script():
    """Ensures the simple_responder.py test script exists."""
    FIXTURES_DIR.mkdir(exist_ok=True)
    
    # Use the same responder script from integration tests
    # Skip implementation if file already exists
    return RESPONDER_SCRIPT


@pytest_asyncio.fixture(scope="module")
async def mcp_mqtt_system(ensure_responder_script) -> AsyncGenerator[Tuple[asyncio.subprocess.Process, asyncio.subprocess.Process], None]:
    """
    Start both the listener and publisher components for performance testing.
    Similar to the system integration fixture but configured for performance.
    
    Returns:
        Tuple of (listener_process, publisher_process)
    """
    responder_script = ensure_responder_script
    
    # Create unique client IDs for this test
    listener_client_id = f"perf-listener-{generate_id()}"
    publisher_client_id = f"perf-publisher-{generate_id()}"
    
    # Build command for the listener
    listener_cmd = [
        "python", "-m", "mcp_mqtt_proxy",
        "listen",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", listener_client_id,
        "--qos", str(DEFAULT_QOS),
        "--mcp-timeout", "60.0",  # Longer timeout for perf tests
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
    
    # Start a task to capture stderr but don't log it during perf tests
    async def collect_listener_stderr():
        stderr_data = []
        if listener_process.stderr:
            while True:
                line = await listener_process.stderr.readline()
                if not line:
                    break
                stderr_data.append(line.decode().strip())
        return stderr_data
    
    listener_stderr_task = asyncio.create_task(collect_listener_stderr())
    
    # Wait a moment for the listener to connect to MQTT
    await asyncio.sleep(3.0)
    
    # Build command for the publisher with longer timeout
    publisher_cmd = [
        "python", "-m", "mcp_mqtt_proxy",
        "publish",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", publisher_client_id,
        "--qos", str(DEFAULT_QOS),
        "--debug-timeout", "600.0",  # 10 minutes for long perf tests
    ]
    
    # Start the publisher process
    logger.info(f"Starting publisher process: {' '.join(publisher_cmd)}")
    publisher_process = await asyncio.create_subprocess_exec(
        *publisher_cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Similar stderr collection for publisher
    async def collect_publisher_stderr():
        stderr_data = []
        if publisher_process.stderr:
            while True:
                line = await publisher_process.stderr.readline()
                if not line:
                    break
                stderr_data.append(line.decode().strip())
        return stderr_data
    
    publisher_stderr_task = asyncio.create_task(collect_publisher_stderr())
    
    # Wait a moment for the publisher to connect to MQTT
    await asyncio.sleep(3.0)
    
    try:
        yield (listener_process, publisher_process)
    finally:
        logger.info("Cleaning up performance test processes")
        
        # Cancel stderr collection tasks
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
                except asyncio.TimeoutError:
                    logger.warning(f"{name.capitalize()} process did not terminate gracefully, killing")
                    process.kill()
                    await process.wait()


def generate_test_payload(size_bytes: int) -> Dict[str, Any]:
    """Generate a test payload of approximately the specified size."""
    # Base request structure
    base_request = {
        "jsonrpc": "2.0",
        "id": generate_id(),
        "method": "test/echo",
        "params": {
            "message": "Performance test",
            "data": ""
        }
    }
    
    # Calculate approximate JSON overhead
    base_json = json.dumps(base_request)
    base_size = len(base_json.encode())
    
    # Add padding to reach target size
    padding_needed = max(0, size_bytes - base_size)
    if padding_needed > 0:
        # Create random string of appropriate length
        padding = "x" * padding_needed
        base_request["params"]["data"] = padding
    
    return base_request


async def measure_round_trip(publisher_process, test_payload: Dict[str, Any]) -> float:
    """
    Measure the round-trip time for a single request/response cycle.
    
    Args:
        publisher_process: The publisher process to use
        test_payload: The JSON payload to send
        
    Returns:
        float: Round-trip time in milliseconds
    """
    # Prepare request
    request_json = json.dumps(test_payload) + "\n"
    
    # Send request and measure time
    start_time = time.time()
    publisher_process.stdin.write(request_json.encode())
    await publisher_process.stdin.drain()
    
    # Wait for response
    response_line = await publisher_process.stdout.readline()
    end_time = time.time()
    
    # Parse response just to verify it's valid
    response_text = response_line.decode().strip()
    response = json.loads(response_text)
    
    # Verify response matches request
    assert response.get("id") == test_payload["id"], "Response ID mismatch"
    
    # Calculate round-trip time in milliseconds
    rtt_ms = (end_time - start_time) * 1000
    return rtt_ms


@pytest.mark.parametrize("payload_size", [
    SMALL_PAYLOAD_SIZE,
    MEDIUM_PAYLOAD_SIZE,
    LARGE_PAYLOAD_SIZE
])
async def test_latency(mcp_mqtt_system, payload_size):
    """
    Test request/response latency with different payload sizes.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Generate test payload of the specified size
    test_payload = generate_test_payload(payload_size)
    payload_json = json.dumps(test_payload)
    actual_size = len(payload_json.encode())
    
    logger.info(f"Testing latency with payload size: {actual_size} bytes")
    
    # Warm-up iterations (to exclude JIT and other initialization effects)
    for _ in range(WARMUP_ITERATIONS):
        await measure_round_trip(publisher_process, test_payload)
    
    # Actual test iterations
    latencies = []
    for i in range(NUM_ITERATIONS):
        # Create a new ID for each request
        test_payload["id"] = str(uuid.uuid4())
        latency = await measure_round_trip(publisher_process, test_payload)
        latencies.append(latency)
    
    # Calculate statistics
    avg_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    stdev_latency = statistics.stdev(latencies) if len(latencies) > 1 else 0
    p95_latency = sorted(latencies)[int(0.95 * len(latencies))]
    
    # Log results
    logger.warning(f"Latency results for {actual_size} byte payload (ms):")
    logger.warning(f"  Avg: {avg_latency:.2f}, Median: {median_latency:.2f}")
    logger.warning(f"  Min: {min_latency:.2f}, Max: {max_latency:.2f}")
    logger.warning(f"  Stdev: {stdev_latency:.2f}, p95: {p95_latency:.2f}")
    
    # No assertions for this test, it just collects and logs stats


async def test_throughput(mcp_mqtt_system):
    """
    Test maximum throughput (requests per second) the system can handle.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Use small payloads for throughput testing
    test_payload = generate_test_payload(SMALL_PAYLOAD_SIZE)
    
    # Number of requests to send in the burst
    num_requests = 100
    
    # Prepare all requests in advance
    requests = []
    request_ids = set()
    for _ in range(num_requests):
        request_id = str(uuid.uuid4())
        request_ids.add(request_id)
        new_request = dict(test_payload)
        new_request["id"] = request_id
        requests.append(json.dumps(new_request) + "\n")
    
    # Send all requests as fast as possible and measure time
    logger.info(f"Sending burst of {num_requests} requests")
    start_time = time.time()
    
    for request_json in requests:
        publisher_process.stdin.write(request_json.encode())
        await publisher_process.stdin.drain()
    
    # Read all responses
    responses = []
    for _ in range(num_requests):
        response_line = await asyncio.wait_for(
            publisher_process.stdout.readline(),
            timeout=30.0
        )
        response_text = response_line.decode().strip()
        responses.append(json.loads(response_text))
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Verify all responses were received with matching IDs
    response_ids = {r.get("id") for r in responses}
    assert len(responses) == num_requests, f"Expected {num_requests} responses, got {len(responses)}"
    assert response_ids == request_ids, "Response IDs don't match request IDs"
    
    # Calculate throughput metrics
    requests_per_second = num_requests / total_time
    
    logger.warning(f"Throughput results:")
    logger.warning(f"  Total time: {total_time:.2f} seconds")
    logger.warning(f"  Requests per second: {requests_per_second:.2f}")
    
    # No assertions for this test, it just collects and logs stats


async def test_sustained_load(mcp_mqtt_system):
    """
    Test system behavior under sustained load for a longer period.
    """
    listener_process, publisher_process = mcp_mqtt_system
    
    # Use small payloads for sustained load testing
    test_payload = generate_test_payload(SMALL_PAYLOAD_SIZE)
    
    # Test configuration
    total_duration = 20.0  # seconds
    request_interval = 0.1  # 100ms between requests (10 req/sec)
    
    logger.info(f"Running sustained load test for {total_duration} seconds at {1.0/request_interval:.1f} req/sec")
    
    # Track all request IDs and timing data
    request_times = {}
    response_times = {}
    start_time = time.time()
    end_time = start_time + total_duration
    
    # Start a task to send requests at the specified interval
    async def send_requests():
        current_time = time.time()
        request_count = 0
        
        while current_time < end_time:
            # Create a new request
            request_id = str(uuid.uuid4())
            new_request = dict(test_payload)
            new_request["id"] = request_id
            request_json = json.dumps(new_request) + "\n"
            
            # Record send time and send request
            send_time = time.time()
            request_times[request_id] = send_time
            publisher_process.stdin.write(request_json.encode())
            await publisher_process.stdin.drain()
            request_count += 1
            
            # Wait until next interval
            current_time = time.time()
            next_send_time = start_time + (request_count * request_interval)
            sleep_time = max(0, next_send_time - current_time)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                current_time = time.time()
    
    # Start a task to receive responses
    async def receive_responses():
        while time.time() < end_time + 5.0:  # Allow extra time for responses
            try:
                response_line = await asyncio.wait_for(
                    publisher_process.stdout.readline(),
                    timeout=1.0
                )
                receive_time = time.time()
                
                # Parse response and record
                response_text = response_line.decode().strip()
                response = json.loads(response_text)
                response_id = response.get("id")
                
                if response_id in request_times:
                    response_times[response_id] = receive_time
            except asyncio.TimeoutError:
                # No response received within timeout, check if test is complete
                if time.time() > end_time + 5.0:
                    break
    
    # Run both tasks concurrently
    sender_task = asyncio.create_task(send_requests())
    receiver_task = asyncio.create_task(receive_responses())
    
    # Wait for both tasks to complete
    await asyncio.gather(sender_task, receiver_task)
    
    # Calculate statistics
    total_requests = len(request_times)
    total_responses = len(response_times)
    success_rate = (total_responses / total_requests) if total_requests > 0 else 0
    
    # Calculate latencies for successful requests
    latencies = []
    for req_id, req_time in request_times.items():
        if req_id in response_times:
            latency = (response_times[req_id] - req_time) * 1000  # ms
            latencies.append(latency)
    
    # Calculate and log statistics
    avg_latency = statistics.mean(latencies) if latencies else 0
    median_latency = statistics.median(latencies) if latencies else 0
    p95_latency = sorted(latencies)[int(0.95 * len(latencies))] if len(latencies) >= 20 else 0
    
    logger.warning(f"Sustained load test results:")
    logger.warning(f"  Total requests: {total_requests}, Total responses: {total_responses}")
    logger.warning(f"  Success rate: {success_rate:.2%}")
    logger.warning(f"  Avg latency: {avg_latency:.2f}ms, Median: {median_latency:.2f}ms, P95: {p95_latency:.2f}ms")
    
    # Verify success rate is acceptable
    assert success_rate > 0.95, f"Success rate too low: {success_rate:.2%}" 