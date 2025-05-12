"""Integration tests for the MQTT Publisher component with a real broker.

These tests assume a mosquitto server is running locally on the default port (1883).
"""

import asyncio
import pytest
import pytest_asyncio
import uuid
import json
import logging
from typing import AsyncGenerator, Tuple
import io
from unittest.mock import patch

import aiomqtt
from pydantic import ValidationError

from mcp.types import JSONRPCRequest, JSONRPCResponse, JSONRPCError, ErrorData
from mcp_mqtt_proxy.config import MQTTPublisherConfig
from mcp_mqtt_proxy.mqtt_publisher import run_mqtt_publisher

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use pytest-asyncio for async tests
pytestmark = pytest.mark.asyncio

# Default MQTT broker URL - assumes mosquitto running locally
DEFAULT_MQTT_BROKER_URL = "mqtt://localhost:1883"
# Generate a unique base topic for this test run
TEST_BASE_TOPIC = f"mcp/test/publisher_integration/{uuid.uuid4()}"
# Default QoS
DEFAULT_QOS = 1

@pytest.fixture
def mqtt_publisher_config():
    """Creates a publisher config pointing to the local broker."""
    return MQTTPublisherConfig(
        broker_url=DEFAULT_MQTT_BROKER_URL,
        client_id=f"publisher-test-{uuid.uuid4()}",
        qos=DEFAULT_QOS,
        base_topic=TEST_BASE_TOPIC,
        username=None,  # No auth for local test broker
        password=None,
        tls_ca_certs=None,
        tls_certfile=None,
        tls_keyfile=None,
        message_expiry_interval=None
    )

@pytest.fixture
async def mqtt_listener_client() -> AsyncGenerator[aiomqtt.Client, None]:
    """Creates an MQTT client that acts as a test listener subscribed to the test topics."""
    client_id = f"test-listener-{uuid.uuid4()}"
    
    # Connect to the MQTT broker
    async with aiomqtt.Client(
        hostname="localhost",
        port=1883,
        client_id=client_id
    ) as client:
        logger.info(f"Test MQTT listener connected with client_id: {client_id}")
        # Subscribe to the request topic where the publisher will publish
        await client.subscribe(f"{TEST_BASE_TOPIC}/request", qos=DEFAULT_QOS)
        logger.info(f"Subscribed to {TEST_BASE_TOPIC}/request")
        
        yield client

@pytest_asyncio.fixture
async def publisher_process() -> AsyncGenerator[Tuple[asyncio.StreamWriter, asyncio.StreamReader, asyncio.subprocess.Process], None]:
    """
    Starts the publisher as a subprocess and returns its stdin/stdout streams.
    Uses the debug-timeout parameter to ensure the process exits cleanly after testing.
    """
    # Create a unique client ID for this test run
    client_id = f"test-publisher-{uuid.uuid4()}"
    
    # Build the publisher command with debug timeout
    cmd = [
        "python", "-m", "mcp_mqtt_proxy",
        "publish",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", client_id,
        "--qos", str(DEFAULT_QOS),
        "--debug-timeout", "10"  # 10 second timeout for tests
    ]
    
    logger.info(f"Starting publisher process: {' '.join(cmd)}")
    
    # Start the publisher process
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    if not process.stdin or not process.stdout:
        raise RuntimeError("Failed to get stdin/stdout pipes from publisher process")
    
    # Wait a moment for the publisher to connect to MQTT
    await asyncio.sleep(2.0)
    
    # Start a task to read and log stderr in the background
    async def log_stderr():
        if process.stderr:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                logger.info(f"Publisher stderr: {line.decode().strip()}")
    
    stderr_task = asyncio.create_task(log_stderr())
    
    try:
        yield process.stdin, process.stdout, process
    finally:
        logger.info("Terminating publisher process")
        # Cancel the stderr logging task
        stderr_task.cancel()
        try:
            await stderr_task
        except asyncio.CancelledError:
            pass
            
        # Give the process a moment to terminate naturally via debug-timeout
        if process.returncode is None:
            try:
                await asyncio.wait_for(process.wait(), timeout=1.0)
                logger.info(f"Process exited naturally with code {process.returncode}")
            except asyncio.TimeoutError:
                # If it doesn't exit naturally, terminate it
                logger.info("Process didn't exit naturally, terminating it")
                process.terminate()
                try:
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
                    logger.info(f"Final publisher stderr: {stderr.decode()}")
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

async def test_publisher_sends_request_to_mqtt(mqtt_listener_client, publisher_process):
    """
    Test that the publisher properly sends requests from its stdin to MQTT.
    
    This test:
    1. Sends a request to the publisher's stdin
    2. Verifies the request appears on the MQTT request topic
    """
    publisher_stdin, publisher_stdout, _ = publisher_process
    
    # Explicitly subscribe to the request topic to make sure we receive messages
    request_topic = f"{TEST_BASE_TOPIC}/request"
    await mqtt_listener_client.subscribe(request_topic, qos=DEFAULT_QOS)
    logger.info(f"Subscribed to request topic: {request_topic}")
    
    # Wait a moment for the subscription to take effect
    await asyncio.sleep(1.0)
    
    # Create a test MCP request
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/echo",
        params={"message": "Hello MQTT"}
    )
    
    # Send the request to the publisher's stdin
    request_json = test_request.model_dump_json() + "\n"
    logger.info(f"Sending test request to publisher: {request_json}")
    publisher_stdin.write(request_json.encode())
    await publisher_stdin.drain()
    
    # Listen for the message on the MQTT request topic
    received_message = None
    try:
        async def receive_message():
            async with mqtt_listener_client.messages() as messages:
                async for message in messages:
                    logger.info(f"Received message on topic: {message.topic}")
                    
                    # We're only interested in messages on our request topic
                    if message.topic == request_topic:
                        try:
                            received_json = message.payload.decode()
                            received_data = json.loads(received_json)
                            return JSONRPCRequest.model_validate(received_data)
                        except (json.JSONDecodeError, ValidationError) as e:
                            logger.error(f"Failed to parse message as JSONRPCRequest: {e}")
                            continue
            return None
            
        # Set a timeout for message reception
        received_message = await asyncio.wait_for(receive_message(), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for message on MQTT topic after 10 seconds")
    except Exception as e:
        pytest.fail(f"Error listening for MQTT messages: {e}")
    
    # Verify the received message matches what we sent
    assert received_message is not None, "No message received on the MQTT request topic"
    assert received_message.id == test_request_id, "Request ID mismatch"
    assert received_message.method == "test/echo", "Method mismatch"
    assert received_message.params == {"message": "Hello MQTT"}, "Params mismatch"

async def test_publisher_delivers_mqtt_responses(mqtt_listener_client, publisher_process):
    """
    Test that the publisher properly delivers responses from MQTT to its stdout.
    
    This test:
    1. Sends a request to the publisher's stdin
    2. Subscribes to the MQTT response topic
    3. Sends a response to that topic
    4. Verifies the response appears on the publisher's stdout
    """
    publisher_stdin, publisher_stdout, _ = publisher_process
    
    # Explicitly subscribe to the request topic to make sure we receive messages
    request_topic = f"{TEST_BASE_TOPIC}/request"
    await mqtt_listener_client.subscribe(request_topic, qos=DEFAULT_QOS)
    logger.info(f"Subscribed to request topic: {request_topic}")
    
    # Wait a moment for the subscription to take effect
    await asyncio.sleep(1.0)
    
    # 1. Send a test request to the publisher
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/echo",
        params={"message": "Hello MQTT"}
    )
    
    request_json = test_request.model_dump_json() + "\n"
    logger.info(f"Sending test request to publisher: {request_json}")
    publisher_stdin.write(request_json.encode())
    await publisher_stdin.drain()
    
    # Wait briefly to ensure the request has been processed
    await asyncio.sleep(0.5)
    
    # 2. Prepare a response message
    test_response = JSONRPCResponse(
        jsonrpc="2.0",
        id=test_request_id,
        result={"message": "Echo: Hello MQTT"}
    )
    
    # 3. Get the publisher's client ID from the request topic
    client_id = None
    try:
        async def get_client_id():
            async with mqtt_listener_client.messages() as messages:
                async for message in messages:
                    if message.topic == request_topic:
                        return message.properties.CorrelationData.decode() if hasattr(message.properties, 'CorrelationData') else test_request_id
            return None
            
        # Set a 10 second timeout for getting client ID
        client_id = await asyncio.wait_for(get_client_id(), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for request message on MQTT topic after 10 seconds")
    except Exception as e:
        pytest.fail(f"Error listening for MQTT messages: {e}")
    
    assert client_id is not None, "Failed to determine client ID from request"
    
    # Response topic based on the publisher's client ID
    response_topic = f"{TEST_BASE_TOPIC}/response/{client_id}"
    logger.info(f"Publishing response to topic: {response_topic}")
    
    # 4. Publish the response to the MQTT response topic
    response_json = test_response.model_dump_json()
    await mqtt_listener_client.publish(
        response_topic,
        payload=response_json.encode(),
        qos=DEFAULT_QOS
    )
    
    # 5. Wait for and verify the response on publisher's stdout
    try:
        response_line = await asyncio.wait_for(publisher_stdout.readline(), timeout=10.0)
        response_text = response_line.decode().strip()
        logger.info(f"Received response from publisher stdout: {response_text}")
        
        response_data = json.loads(response_text)
        received_response = JSONRPCResponse.model_validate(response_data)
        
        assert received_response.id == test_request_id, "Response ID mismatch"
        assert received_response.result == {"message": "Echo: Hello MQTT"}, "Response result mismatch"
        
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for response from publisher stdout after 10 seconds")
    except (json.JSONDecodeError, ValidationError) as e:
        pytest.fail(f"Failed to parse response from publisher stdout: {e}")

async def test_publisher_handles_error_responses(mqtt_listener_client, publisher_process):
    """
    Test that the publisher properly delivers error responses from MQTT to its stdout.
    
    This test:
    1. Sends a request to the publisher's stdin
    2. Sends an error response via MQTT
    3. Verifies the error response appears on the publisher's stdout
    """
    publisher_stdin, publisher_stdout, _ = publisher_process
    
    # Explicitly subscribe to the request topic to make sure we receive messages
    request_topic = f"{TEST_BASE_TOPIC}/request"
    await mqtt_listener_client.subscribe(request_topic, qos=DEFAULT_QOS)
    logger.info(f"Subscribed to request topic: {request_topic}")
    
    # Wait a moment for the subscription to take effect
    await asyncio.sleep(1.0)
    
    # 1. Send a test request to the publisher
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/error",
        params={"trigger": "error"}
    )
    
    request_json = test_request.model_dump_json() + "\n"
    logger.info(f"Sending test error request to publisher: {request_json}")
    publisher_stdin.write(request_json.encode())
    await publisher_stdin.drain()
    
    # Wait briefly to ensure the request has been processed
    await asyncio.sleep(0.5)
    
    # 2. Prepare an error response
    error_data = ErrorData(
        code=1001,
        message="Test error message",
        data={"details": "This is a test error"}
    )
    
    test_error_response = JSONRPCError(
        jsonrpc="2.0",
        id=test_request_id,
        error=error_data
    )
    
    # 3. Get the publisher's client ID from the request topic
    client_id = None
    try:
        async def get_client_id():
            async with mqtt_listener_client.messages() as messages:
                async for message in messages:
                    if message.topic == request_topic:
                        return message.properties.CorrelationData.decode() if hasattr(message.properties, 'CorrelationData') else test_request_id
            return None
            
        # Set a 10 second timeout for getting client ID
        client_id = await asyncio.wait_for(get_client_id(), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for request message on MQTT topic after 10 seconds")
    except Exception as e:
        pytest.fail(f"Error listening for MQTT messages: {e}")
    
    assert client_id is not None, "Failed to determine client ID from request"
    
    # Response topic based on the publisher's client ID
    response_topic = f"{TEST_BASE_TOPIC}/response/{client_id}"
    logger.info(f"Publishing error response to topic: {response_topic}")
    
    # 4. Publish the error response to the MQTT response topic
    error_response_json = test_error_response.model_dump_json()
    await mqtt_listener_client.publish(
        response_topic,
        payload=error_response_json.encode(),
        qos=DEFAULT_QOS
    )
    
    # 5. Wait for and verify the error response on publisher's stdout
    try:
        response_line = await asyncio.wait_for(publisher_stdout.readline(), timeout=10.0)
        response_text = response_line.decode().strip()
        logger.info(f"Received error response from publisher stdout: {response_text}")
        
        response_data = json.loads(response_text)
        received_response = JSONRPCError.model_validate(response_data)
        
        assert received_response.id == test_request_id, "Error response ID mismatch"
        assert received_response.error.code == 1001, "Error code mismatch"
        assert received_response.error.message == "Test error message", "Error message mismatch"
        assert received_response.error.data == {"details": "This is a test error"}, "Error data mismatch"
        
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for error response from publisher stdout after 10 seconds")
    except (json.JSONDecodeError, ValidationError) as e:
        pytest.fail(f"Failed to parse error response from publisher stdout: {e}")

@pytest.mark.asyncio
async def test_direct_publisher_send_request():
    """
    Test the publisher function directly by importing and calling it rather than using a subprocess.
    This avoids issues with the publisher exiting on EOF.
    """
    # Create unique IDs for this test
    test_request_id = str(uuid.uuid4())
    client_id = f"direct-test-{uuid.uuid4()}"
    
    # Create a config for the publisher
    publisher_config = MQTTPublisherConfig(
        broker_url=DEFAULT_MQTT_BROKER_URL,
        client_id=client_id,
        qos=DEFAULT_QOS,
        base_topic=TEST_BASE_TOPIC,
        username=None,
        password=None,
        tls_ca_certs=None,
        tls_certfile=None,
        tls_keyfile=None,
        message_expiry_interval=None
    )
    
    # Create a test JSONRPC request
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/echo",
        params={"message": "Hello MQTT"}
    )
    request_json = test_request.model_dump_json() + "\n"
    
    # Create a StringIO to simulate stdin with our request
    mock_stdin = io.StringIO(request_json)
    # Create a StringIO to capture stdout
    mock_stdout = io.StringIO()
    
    # Set up a test subscriber to listen for the message on MQTT
    async with aiomqtt.Client(
        hostname="localhost",
        port=1883,
        client_id=f"test-listener-{uuid.uuid4()}"
    ) as mqtt_listener:
        # Subscribe to the request topic
        request_topic = f"{TEST_BASE_TOPIC}/request"
        await mqtt_listener.subscribe(request_topic, qos=DEFAULT_QOS)
        logger.info(f"Test listener subscribed to topic: {request_topic}")
        
        # Wait to ensure the subscription is active
        await asyncio.sleep(1.0)
        
        # Set up a task to listen for MQTT messages
        message_received = asyncio.Event()
        received_data = None
        
        async def listen_for_message():
            nonlocal received_data
            try:
                async with mqtt_listener.messages() as messages:
                    async for message in messages:
                        if message.topic == request_topic:
                            received_json = message.payload.decode()
                            logger.info(f"Received message: {received_json}")
                            received_data = json.loads(received_json)
                            message_received.set()
                            break
            except Exception as e:
                logger.error(f"Error in listener: {e}")
        
        # Start the listener task
        listener_task = asyncio.create_task(listen_for_message())
        
        # Start the publisher with a cancellation after a short time
        async def run_publisher_with_timeout():
            with patch('sys.stdin', mock_stdin), patch('sys.stdout', mock_stdout):
                try:
                    await asyncio.wait_for(
                        run_mqtt_publisher(publisher_config), 
                        timeout=2.0
                    )
                except asyncio.TimeoutError:
                    # Expected - we're cancelling after 2 seconds
                    pass
        
        # Run the publisher
        await run_publisher_with_timeout()
        
        # Wait for a message or timeout
        try:
            await asyncio.wait_for(message_received.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pytest.fail("Timeout waiting for MQTT message")
        finally:
            # Cancel the listener task
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass
        
        # Verify the received message
        assert received_data is not None, "No message received on MQTT topic"
        assert received_data["id"] == test_request_id, "Message ID does not match"
        assert received_data["method"] == "test/echo", "Method does not match"
        assert received_data["params"] == {"message": "Hello MQTT"}, "Params do not match"

@pytest.mark.asyncio
async def test_mqtt_messaging_directly():
    """
    Test basic MQTT messaging directly using the aiomqtt client without involving the publisher module.
    This ensures our test setup is correct for MQTT communication.
    """
    # Create unique IDs for this test
    test_request_id = str(uuid.uuid4())
    test_topic = f"{TEST_BASE_TOPIC}/direct_test/{uuid.uuid4()}"
    
    # Create a test message
    test_message = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/echo",
        params={"message": "Hello MQTT"}
    )
    message_json = test_message.model_dump_json()
    
    # Set up event for coordinating between publisher and subscriber
    message_received = asyncio.Event()
    received_data = None
    
    # Create a subscriber client
    async with aiomqtt.Client(
        hostname="localhost",
        port=1883,
        client_id=f"test-sub-{uuid.uuid4()}"
    ) as subscriber:
        # Subscribe to the test topic
        await subscriber.subscribe(test_topic, qos=DEFAULT_QOS)
        logger.info(f"Subscribed to test topic: {test_topic}")
        
        # Wait to ensure subscription is active
        await asyncio.sleep(1.0)
        
        # Start listening for messages
        async def listen_for_message():
            nonlocal received_data
            try:
                async with subscriber.messages() as messages:
                    async for message in messages:
                        if message.topic == test_topic:
                            received_json = message.payload.decode()
                            logger.info(f"Received message: {received_json}")
                            received_data = json.loads(received_json)
                            message_received.set()
                            return
            except Exception as e:
                logger.error(f"Error in listener: {e}")
        
        listener_task = asyncio.create_task(listen_for_message())
        
        # Create a publisher client
        async with aiomqtt.Client(
            hostname="localhost",
            port=1883,
            client_id=f"test-pub-{uuid.uuid4()}"
        ) as publisher:
            # Publish the test message
            logger.info(f"Publishing message to {test_topic}: {message_json}")
            await publisher.publish(
                test_topic,
                payload=message_json.encode(),
                qos=DEFAULT_QOS
            )
            
            # Wait for message to be received or timeout
            try:
                await asyncio.wait_for(message_received.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pytest.fail("Timeout waiting for MQTT message")
            finally:
                # Cancel the listener task
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass
            
            # Verify the received message
            assert received_data is not None, "No message received on MQTT topic"
            assert received_data["id"] == test_request_id, "Message ID does not match"
            assert received_data["method"] == "test/echo", "Method does not match"
            assert received_data["params"] == {"message": "Hello MQTT"}, "Params do not match"

@pytest.mark.asyncio
async def test_simple_mqtt_json_message():
    """
    A minimal test for MQTT JSON message publishing and receiving,
    based on the working simple_mqtt_test.py.
    """
    # Generate unique topic and message
    test_topic = f"{TEST_BASE_TOPIC}/simple_test/{uuid.uuid4()}"
    test_request_id = str(uuid.uuid4())
    
    # Create a test message
    test_message = {
        "jsonrpc": "2.0",
        "id": test_request_id,
        "method": "test/echo",
        "params": {"message": "Hello MQTT"}
    }
    
    # Convert to JSON
    message_json = json.dumps(test_message)
    
    # Track received message
    received_message = None
    publisher_done = asyncio.Event()
    
    # Subscriber function
    async def subscriber():
        subscriber_id = f"simple-sub-{uuid.uuid4()}"
        logger.info(f"Starting subscriber with client_id: {subscriber_id}")
        
        try:
            async with aiomqtt.Client(
                hostname="localhost",
                port=1883,
                client_id=subscriber_id
            ) as client:
                logger.info("Subscriber connected to broker")
                logger.info(f"Subscribing to topic: {test_topic}")
                await client.subscribe(test_topic)
                
                # Add a timeout for safety
                timeout = 10.0
                started = asyncio.get_event_loop().time()
                
                async with client.messages() as messages:
                    async for message in messages:
                        # Check for timeout
                        elapsed = asyncio.get_event_loop().time() - started
                        if elapsed > timeout:
                            logger.warning(f"Timeout after {elapsed:.1f}s waiting for message")
                            break
                        
                        if message.topic.matches(test_topic):
                            nonlocal received_message
                            message_text = message.payload.decode()
                            logger.info(f"Received message: {message_text}")
                            received_message = json.loads(message_text)
                            # Wait for publisher to complete before returning
                            await publisher_done.wait()
                            break
                
                logger.info("Subscriber complete")
                return received_message
        except Exception as e:
            logger.error(f"Subscriber error: {e}")
            return None
    
    # Publisher function
    async def publisher():
        publisher_id = f"simple-pub-{uuid.uuid4()}"
        logger.info(f"Starting publisher with client_id: {publisher_id}")
        
        try:
            async with aiomqtt.Client(
                hostname="localhost",
                port=1883,
                client_id=publisher_id
            ) as client:
                logger.info("Publisher connected to broker")
                logger.info(f"Publishing message '{message_json}' to topic: {test_topic}")
                await client.publish(test_topic, message_json.encode())
                # Wait a bit to ensure the message is delivered
                await asyncio.sleep(1)
                # Set event to signal publisher is done
                publisher_done.set()
                logger.info("Publisher complete")
        except Exception as e:
            logger.error(f"Publisher error: {e}")
    
    # Run the test
    logger.info("Starting MQTT JSON messaging test")
    
    # Start subscriber first to ensure it's ready to receive
    subscriber_task = asyncio.create_task(subscriber())
    
    # Wait a moment for subscriber to connect
    await asyncio.sleep(1)
    
    # Start publisher
    await publisher()
    
    # Wait for subscriber to finish
    received_data = await subscriber_task
    
    # Verify results
    assert received_data is not None, "No message received"
    assert received_data["id"] == test_request_id, "Message ID mismatch"
    assert received_data["method"] == "test/echo", "Method mismatch"
    assert received_data["params"] == {"message": "Hello MQTT"}, "Params mismatch"
    logger.info("✅ Test PASSED: JSON message was successfully sent and received")

@pytest.mark.asyncio
async def test_publisher_subprocess_debug():
    """
    Diagnostic test to investigate why subprocess communication is failing.
    This test captures detailed information about the subprocess behavior.
    Uses the new debug-timeout feature to ensure clean exit.
    """
    logger.info("=== STARTING SUBPROCESS DIAGNOSTIC TEST ===")
    
    # Create a unique client ID for this test run
    client_id = f"debug-publisher-{uuid.uuid4()}"
    test_topic = f"{TEST_BASE_TOPIC}/debug/{uuid.uuid4()}"
    
    # Build the publisher command with debug timeout
    cmd = [
        "python", "-m", "mcp_mqtt_proxy",
        "publish",
        "--broker-url", DEFAULT_MQTT_BROKER_URL,
        "--base-topic", TEST_BASE_TOPIC,
        "--client-id", client_id,
        "--qos", str(DEFAULT_QOS),
        "--debug-timeout", "15"  # 15 second timeout for this test
    ]
    
    logger.info(f"Starting publisher process: {' '.join(cmd)}")
    
    # Start the publisher process with all streams captured
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    if not process.stdin or not process.stdout:
        logger.error("Failed to get stdin/stdout pipes from publisher process")
        pytest.fail("Failed to get stdin/stdout pipes from publisher process")
        return
    
    # Wait for the process to initialize
    logger.info("Waiting 2 seconds for publisher to initialize")
    await asyncio.sleep(2.0)
    
    # Create a test message
    test_request_id = str(uuid.uuid4())
    test_request = JSONRPCRequest(
        jsonrpc="2.0",
        id=test_request_id,
        method="test/echo",
        params={"message": "Hello MQTT Debug"}
    )
    
    # Setup a listener to monitor MQTT messages
    async def mqtt_listener():
        logger.info("Starting MQTT listener for debug test")
        async with aiomqtt.Client(
            hostname="localhost",
            port=1883,
            client_id=f"debug-listener-{uuid.uuid4()}"
        ) as client:
            # Subscribe to all base topics
            await client.subscribe(f"{TEST_BASE_TOPIC}/#", qos=DEFAULT_QOS)
            logger.info(f"Subscribed to all topics under {TEST_BASE_TOPIC}/#")
            
            try:
                async with client.messages() as messages:
                    async for message in messages:
                        logger.info(f"MQTT Monitor: Received message on topic: {message.topic}, payload: {message.payload[:200]}")
                        # Just log all messages but don't stop
            except Exception as e:
                logger.error(f"MQTT Listener error: {e}")
    
    # Start the MQTT listener in the background
    mqtt_task = asyncio.create_task(mqtt_listener())
    
    # Tasks to collect stdout and stderr continuously
    async def read_stream(stream, name):
        while True:
            line = await stream.readline()
            if not line:
                logger.info(f"{name} stream closed")
                break
            logger.info(f"{name}: {line.decode().strip()}")
    
    stdout_task = asyncio.create_task(read_stream(process.stdout, "STDOUT"))
    stderr_task = asyncio.create_task(read_stream(process.stderr, "STDERR"))
    
    try:
        # Send a test request to the publisher
        request_json = test_request.model_dump_json() + "\n"
        logger.info(f"Sending test request to publisher stdin: {request_json}")
        try:
            process.stdin.write(request_json.encode())
            await process.stdin.drain()
            logger.info("Request sent successfully")
        except Exception as e:
            logger.error(f"Error sending to stdin: {e}")
            pytest.fail(f"Failed to send request to publisher: {e}")
        
        # Wait a moment to see what happens
        logger.info("Waiting 5 seconds to observe behavior")
        await asyncio.sleep(5)
        
        # Check if process is still running
        if process.returncode is not None:
            logger.error(f"Process exited prematurely with code {process.returncode}")
        else:
            logger.info("Process is still running")
            
            # Try sending another message
            second_request = JSONRPCRequest(
                jsonrpc="2.0",
                id=str(uuid.uuid4()),
                method="test/echo2",
                params={"message": "Second test"}
            )
            second_json = second_request.model_dump_json() + "\n"
            logger.info(f"Sending second request: {second_json}")
            try:
                process.stdin.write(second_json.encode())
                await process.stdin.drain()
                logger.info("Second request sent successfully")
            except Exception as e:
                logger.error(f"Error sending second request: {e}")
            
            # Wait a bit more
            await asyncio.sleep(2)
    
    finally:
        # Clean up
        logger.info("Cleaning up subprocess diagnostic test")
        
        # Cancel the MQTT listener
        mqtt_task.cancel()
        try:
            await mqtt_task
        except asyncio.CancelledError:
            pass
        
        # Cancel stdout/stderr readers
        stdout_task.cancel()
        stderr_task.cancel()
        try:
            await stdout_task
            await stderr_task
        except asyncio.CancelledError:
            pass
        
        # Terminate the process if it's still running
        if process.returncode is None:
            logger.info("Terminating publisher process")
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
                logger.info(f"Process terminated with exit code {process.returncode}")
            except asyncio.TimeoutError:
                logger.warning("Process did not terminate gracefully, killing")
                process.kill()
                await process.wait()
    
    logger.info("=== SUBPROCESS DIAGNOSTIC TEST COMPLETED ===") 