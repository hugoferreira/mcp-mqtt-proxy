"""Integration tests for the MQTT Listener component with a real broker.

These tests assume a mosquitto server is running locally on the default port (1883).
"""

import asyncio
import pytest
import pytest_asyncio
import uuid
import json
import logging
from typing import AsyncGenerator, Tuple, Dict, Any, Optional
import io
import subprocess
from pathlib import Path
import os

import aiomqtt
from pydantic import ValidationError

from mcp.types import JSONRPCRequest, JSONRPCResponse, JSONRPCError, ErrorData
from mcp_mqtt_proxy.config import MQTTListenerConfig, StdioServerParameters
from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use pytest-asyncio for async tests
pytestmark = pytest.mark.asyncio

# Default MQTT broker URL - assumes mosquitto running locally
DEFAULT_MQTT_BROKER_URL = "mqtt://localhost:1883"
# Generate a unique base topic for this test run
TEST_BASE_TOPIC = f"mcp/test/listener_integration/{uuid.uuid4()}"
# Default QoS
DEFAULT_QOS = 1

# Path to the simple_responder.py script for testing
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "simple_responder.py"

@pytest.fixture
def ensure_responder_script():
    """Ensures the simple_responder.py test script exists."""
    FIXTURES_DIR.mkdir(exist_ok=True)
    
    if not RESPONDER_SCRIPT.exists():
        logger.info(f"Test responder script not found at {RESPONDER_SCRIPT}")
        logger.info("Please create the script manually or via another test")
        raise FileNotFoundError(f"Test fixture script not found: {RESPONDER_SCRIPT}")
    
    # Make it executable
    RESPONDER_SCRIPT.chmod(0o755)
    return RESPONDER_SCRIPT

@pytest.fixture
def mqtt_listener_config(ensure_responder_script):
    """Creates a listener config pointing to the local broker and test responder script."""
    responder_script = ensure_responder_script
    
    # Create stdio params for the responder script
    stdio_params = StdioServerParameters(
        command=str(responder_script),
        args=[],
        env={},
        cwd=None
    )
    
    return MQTTListenerConfig(
        broker_url=DEFAULT_MQTT_BROKER_URL,
        client_id=f"listener-test-{uuid.uuid4()}",
        qos=DEFAULT_QOS,
        base_topic=TEST_BASE_TOPIC,
        stdio_mcp_process=stdio_params,
        username=None,  # No auth for local test broker
        password=None,
        tls_ca_certs=None,
        tls_certfile=None,
        tls_keyfile=None,
        mcp_timeout=10.0  # Shorter timeout for tests
    )

@pytest_asyncio.fixture
async def mqtt_publisher_client() -> AsyncGenerator[aiomqtt.Client, None]:
    """Creates an MQTT client that acts as a test publisher to send requests."""
    client_id = f"test-publisher-{uuid.uuid4()}"
    
    # Connect to the MQTT broker
    async with aiomqtt.Client(
        hostname="localhost",
        port=1883,
        client_id=client_id,
        protocol=aiomqtt.ProtocolVersion.V5  # Use MQTTv5 for response topic
    ) as client:
        logger.info(f"Test MQTT publisher connected with client_id: {client_id}")
        yield client

@pytest_asyncio.fixture
async def listener_process() -> AsyncGenerator[asyncio.subprocess.Process, None]:
    """
    Starts the listener as a subprocess.
    """
    # Create a unique client ID for this test run
    client_id = f"test-listener-{uuid.uuid4()}"
    
    # Get the path to the responder script
    responder_script = FIXTURES_DIR / "simple_responder.py"
    
    # Build the listener command
    cmd = [
        "python", "-m", "mcp_mqtt_proxy",
        "listen",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", client_id,
        "--qos", str(DEFAULT_QOS),
        "--mcp-timeout", "10.0",
        "--debug",
        "--", str(responder_script)
    ]
    
    logger.info(f"Starting listener process: {' '.join(cmd)}")
    
    # Start the listener process
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Start a task to read and log stderr in the background
    async def log_stderr():
        if process.stderr:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                logger.info(f"Listener stderr: {line.decode().strip()}")
    
    stderr_task = asyncio.create_task(log_stderr())
    
    # Wait a moment for the listener to connect to MQTT
    await asyncio.sleep(3.0)
    
    try:
        yield process
    finally:
        logger.info("Terminating listener process")
        # Cancel the stderr logging task
        stderr_task.cancel()
        try:
            await stderr_task
        except asyncio.CancelledError:
            pass
            
        # Terminate the process
        if process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=2.0)
                logger.info(f"Process terminated with exit code {process.returncode}")
            except asyncio.TimeoutError:
                logger.warning("Process did not terminate gracefully, killing")
                process.kill()
                await process.wait()
        
        # Log any stderr output
        if process.stderr:
            try:
                stderr = await asyncio.wait_for(process.stderr.read(), timeout=1.0)
                if stderr:
                    logger.info(f"Final listener stderr: {stderr.decode()}")
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

async def test_listener_receives_request_and_sends_response(mqtt_publisher_client, listener_process):
    """
    Test that the listener properly:
    1. Receives a request from MQTT
    2. Forwards it to the local MCP server
    3. Gets a response from the local server
    4. Publishes the response back to MQTT using the Response Topic
    """
    # Create a unique response topic for this test
    response_topic = f"{TEST_BASE_TOPIC}/response/test-{uuid.uuid4()}"
    request_topic = f"{TEST_BASE_TOPIC}/request"
    
    # Create a test request
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/echo",
        params={"message": "Hello from MQTT test"}
    )
    
    # Subscribe to the response topic
    await mqtt_publisher_client.subscribe(response_topic, qos=DEFAULT_QOS)
    logger.info(f"Subscribed to response topic: {response_topic}")
    
    # Wait a moment for the subscription to take effect
    await asyncio.sleep(1.0)
    
    # Set up properties for MQTTv5 with response topic
    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes
    
    properties = Properties(PacketTypes.PUBLISH)
    properties.ResponseTopic = response_topic
    properties.CorrelationData = test_request_id.encode()
    
    # Publish the request
    request_payload = test_request.model_dump_json().encode('utf-8')
    logger.info(f"Publishing request to {request_topic}: {test_request.model_dump_json()}")
    await mqtt_publisher_client.publish(
        request_topic,
        payload=request_payload,
        qos=DEFAULT_QOS,
        properties=properties
    )
    
    # Wait for and verify the response
    received_response = None
    
    async def receive_response():
        async with mqtt_publisher_client.messages() as messages:
            async for message in messages:
                logger.info(f"Received message on topic: {message.topic}")
                
                if message.topic.matches(response_topic):
                    try:
                        response_text = message.payload.decode('utf-8')
                        logger.info(f"Response message: {response_text}")
                        response_data = json.loads(response_text)
                        response = JSONRPCResponse.model_validate(response_data)
                        return response
                    except (json.JSONDecodeError, ValidationError) as e:
                        logger.error(f"Failed to parse response: {e}")
                        continue
        return None
    
    try:
        received_response = await asyncio.wait_for(receive_response(), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for response from listener")
    
    # Verify the response
    assert received_response is not None, "No response received"
    assert received_response.id == test_request_id, "Response ID mismatch"
    assert received_response.result == {"message": f"Echo: Hello from MQTT test"}, "Response result mismatch"

async def test_listener_handles_error_responses(mqtt_publisher_client, listener_process):
    """
    Test that the listener properly handles error responses from the local MCP server.
    """
    # Create a unique response topic for this test
    response_topic = f"{TEST_BASE_TOPIC}/response/error-test-{uuid.uuid4()}"
    request_topic = f"{TEST_BASE_TOPIC}/request"
    
    # Create a test request that will trigger an error
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/error",  # This method returns an error in our test script
        params={"trigger": "error"}
    )
    
    # Subscribe to the response topic
    await mqtt_publisher_client.subscribe(response_topic, qos=DEFAULT_QOS)
    logger.info(f"Subscribed to response topic: {response_topic}")
    
    # Wait a moment for the subscription to take effect
    await asyncio.sleep(1.0)
    
    # Set up properties for MQTTv5 with response topic
    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes
    
    properties = Properties(PacketTypes.PUBLISH)
    properties.ResponseTopic = response_topic
    properties.CorrelationData = test_request_id.encode()
    
    # Publish the request
    request_payload = test_request.model_dump_json().encode('utf-8')
    logger.info(f"Publishing error request to {request_topic}: {test_request.model_dump_json()}")
    await mqtt_publisher_client.publish(
        request_topic,
        payload=request_payload,
        qos=DEFAULT_QOS,
        properties=properties
    )
    
    # Wait for and verify the error response
    received_error = None
    
    async def receive_error_response():
        async with mqtt_publisher_client.messages() as messages:
            async for message in messages:
                logger.info(f"Received message on topic: {message.topic}")
                
                if message.topic.matches(response_topic):
                    try:
                        response_text = message.payload.decode('utf-8')
                        logger.info(f"Error response message: {response_text}")
                        response_data = json.loads(response_text)
                        # Check if it has an error field
                        if "error" in response_data:
                            error_response = JSONRPCError.model_validate(response_data)
                            return error_response
                        else:
                            logger.warning(f"Received non-error response: {response_data}")
                    except (json.JSONDecodeError, ValidationError) as e:
                        logger.error(f"Failed to parse error response: {e}")
                        continue
        return None
    
    try:
        received_error = await asyncio.wait_for(receive_error_response(), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for error response from listener")
    
    # Verify the error response
    assert received_error is not None, "No error response received"
    assert received_error.id == test_request_id, "Error response ID mismatch"
    assert received_error.error.code == 1001, "Error code mismatch"
    assert received_error.error.message == "Test error message", "Error message mismatch"
    assert received_error.error.data == {"details": "This is a test error"}, "Error data mismatch"

async def test_listener_handles_delayed_responses(mqtt_publisher_client, listener_process):
    """
    Test that the listener properly handles delayed responses from the local MCP server.
    """
    # Create a unique response topic for this test
    response_topic = f"{TEST_BASE_TOPIC}/response/delay-test-{uuid.uuid4()}"
    request_topic = f"{TEST_BASE_TOPIC}/request"
    
    # Create a test request that will trigger a delayed response
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/delay",
        params={"delay": 2.5}  # 2.5 second delay
    )
    
    # Subscribe to the response topic
    await mqtt_publisher_client.subscribe(response_topic, qos=DEFAULT_QOS)
    logger.info(f"Subscribed to response topic: {response_topic}")
    
    # Wait a moment for the subscription to take effect
    await asyncio.sleep(1.0)
    
    # Set up properties for MQTTv5 with response topic
    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes
    
    properties = Properties(PacketTypes.PUBLISH)
    properties.ResponseTopic = response_topic
    properties.CorrelationData = test_request_id.encode()
    
    # Record start time to verify delay
    start_time = asyncio.get_event_loop().time()
    
    # Publish the request
    request_payload = test_request.model_dump_json().encode('utf-8')
    logger.info(f"Publishing delayed request to {request_topic}: {test_request.model_dump_json()}")
    await mqtt_publisher_client.publish(
        request_topic,
        payload=request_payload,
        qos=DEFAULT_QOS,
        properties=properties
    )
    
    # Wait for and verify the delayed response
    received_response = None
    
    async def receive_delayed_response():
        async with mqtt_publisher_client.messages() as messages:
            async for message in messages:
                logger.info(f"Received message on topic: {message.topic}")
                
                if message.topic.matches(response_topic):
                    try:
                        response_text = message.payload.decode('utf-8')
                        logger.info(f"Delayed response message: {response_text}")
                        response_data = json.loads(response_text)
                        response = JSONRPCResponse.model_validate(response_data)
                        return response
                    except (json.JSONDecodeError, ValidationError) as e:
                        logger.error(f"Failed to parse delayed response: {e}")
                        continue
        return None
    
    try:
        received_response = await asyncio.wait_for(receive_delayed_response(), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for delayed response from listener")
    
    # Calculate elapsed time
    elapsed_time = asyncio.get_event_loop().time() - start_time
    
    # Verify the response
    assert received_response is not None, "No delayed response received"
    assert received_response.id == test_request_id, "Delayed response ID mismatch"
    assert received_response.result == {"message": "Delayed response after 2.5s"}, "Delayed response result mismatch"
    assert elapsed_time >= 2.5, f"Response came too quickly: {elapsed_time:.2f}s vs expected 2.5s"

async def test_direct_listener_run():
    """
    Test running the listener function directly rather than via subprocess.
    This allows for more controlled testing of the internal functionality.
    """
    # Create a unique client ID for this test
    client_id = f"direct-listener-{uuid.uuid4()}"
    
    # Ensure the responder script exists
    if not RESPONDER_SCRIPT.exists():
        pytest.skip(f"Responder script not found at {RESPONDER_SCRIPT}")
    
    # Create stdio params for the responder script
    stdio_params = StdioServerParameters(
        command=str(RESPONDER_SCRIPT),
        args=[],
        env={},
        cwd=None
    )
    
    # Create the listener config
    listener_config = MQTTListenerConfig(
        broker_url=DEFAULT_MQTT_BROKER_URL,
        client_id=client_id,
        qos=DEFAULT_QOS,
        base_topic=TEST_BASE_TOPIC,
        stdio_mcp_process=stdio_params,
        username=None,
        password=None,
        tls_ca_certs=None,
        tls_certfile=None,
        tls_keyfile=None,
        mcp_timeout=10.0
    )
    
    # Create a publisher client for sending the test request
    async with aiomqtt.Client(
        hostname="localhost",
        port=1883,
        client_id=f"direct-test-pub-{uuid.uuid4()}",
        protocol=aiomqtt.ProtocolVersion.V5
    ) as publisher:
        # Create a unique response topic
        response_topic = f"{TEST_BASE_TOPIC}/response/direct-test-{uuid.uuid4()}"
        request_topic = f"{TEST_BASE_TOPIC}/request"
        
        # Create a test request
        test_request_id = str(uuid.uuid4())
        test_request = JSONRPCRequest(
            jsonrpc="2.0",
            id=test_request_id,
            method="test/echo",
            params={"message": "Hello from direct test"}
        )
        
        # Subscribe to the response topic
        await publisher.subscribe(response_topic, qos=DEFAULT_QOS)
        logger.info(f"Subscribed to response topic: {response_topic}")
        
        # Set up response received event
        response_received = asyncio.Event()
        received_response = None
        
        # Listen for responses
        async def listen_for_response():
            nonlocal received_response
            try:
                async with publisher.messages() as messages:
                    async for message in messages:
                        if message.topic.matches(response_topic):
                            try:
                                response_text = message.payload.decode('utf-8')
                                logger.info(f"Direct test received response: {response_text}")
                                response_data = json.loads(response_text)
                                received_response = JSONRPCResponse.model_validate(response_data)
                                response_received.set()
                                break
                            except Exception as e:
                                logger.error(f"Error processing response: {e}")
            except Exception as e:
                logger.error(f"Error in response listener: {e}")
        
        response_listener = asyncio.create_task(listen_for_response())
        
        # Start the listener as a cancellable task
        listener_task = None
        try:
            # Start listener in background with timeout to ensure it exits
            async def run_listener_with_timeout():
                try:
                    await asyncio.wait_for(run_mqtt_listener(listener_config), timeout=12.0)
                except asyncio.TimeoutError:
                    logger.info("Listener timeout reached, cancelling")
                except Exception as e:
                    logger.error(f"Error running listener: {e}")
            
            listener_task = asyncio.create_task(run_listener_with_timeout())
            
            # Wait a moment for the listener to initialize
            await asyncio.sleep(3.0)
            
            # Set up properties for MQTTv5 with response topic
            from paho.mqtt.properties import Properties
            from paho.mqtt.packettypes import PacketTypes
            
            properties = Properties(PacketTypes.PUBLISH)
            properties.ResponseTopic = response_topic
            properties.CorrelationData = test_request_id.encode()
            
            # Publish the request
            request_payload = test_request.model_dump_json().encode('utf-8')
            logger.info(f"Publishing direct test request: {test_request.model_dump_json()}")
            await publisher.publish(
                request_topic,
                payload=request_payload,
                qos=DEFAULT_QOS,
                properties=properties
            )
            
            # Wait for the response with timeout
            try:
                await asyncio.wait_for(response_received.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pytest.fail("Timeout waiting for response in direct test")
            
            # Verify the response
            assert received_response is not None, "No response received in direct test"
            assert received_response.id == test_request_id, "Response ID mismatch"
            assert received_response.result == {"message": f"Echo: Hello from direct test"}, "Response result mismatch"
            
        finally:
            # Clean up
            if listener_task and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass
            
            response_listener.cancel()
            try:
                await response_listener
            except asyncio.CancelledError:
                pass 