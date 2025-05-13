"""Unit tests for the MQTT Listener mode (MQTT -> stdio Server)."""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from pydantic import BaseModel # Import BaseModel
import aiomqtt # Import aiomqtt for type hinting Message
from datetime import timedelta
import paho.mqtt.properties # Import paho properties for mocking
import subprocess
import logging

# Assume necessary imports from your project structure
from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener
from mcp_mqtt_proxy.config import MQTTListenerConfig
from mcp.client.stdio import StdioServerParameters # Import this instead
from mcp.client.session import ClientSession # Need to mock this
from mcp.types import ( # Import relevant MCP types
    Request,
    ListPromptsRequest,
    ListPromptsResult,
    Prompt,
    JSONRPCResponse,
    InitializeResult,
    Implementation, # Add import for Implementation
    JSONRPCError,
    ErrorData,
)


# Mark all tests in this module as async
pytestmark = pytest.mark.asyncio


# Helper to create a mock aiomqtt.Message
def create_mock_mqtt_message(topic: str, payload: bytes, response_topic: str, correlation_data: bytes) -> aiomqtt.Message:
    mock_message = MagicMock(spec=aiomqtt.Message)
    mock_message.topic = topic
    mock_message.payload = payload
    mock_message.qos = 1 # Assume default
    mock_message.retain = False
    mock_message.mid = 123
    # Mock paho.mqtt.properties.Properties directly
    mock_message.properties = MagicMock(spec=paho.mqtt.properties.Properties)
    # Use correct CamelCase attribute names from paho
    mock_message.properties.ResponseTopic = response_topic
    mock_message.properties.CorrelationData = correlation_data
    # Add other properties if needed (using paho names if mocking)
    # mock_message.properties.PayloadFormatIndicator = None
    # mock_message.properties.MessageExpiryInterval = None
    # mock_message.properties.TopicAlias = None
    # mock_message.properties.ContentType = None
    # mock_message.properties.UserProperty = None
    # mock_message.properties.SubscriptionIdentifier = None

    return mock_message


async def test_listener_handles_valid_prompts_list(mocker):
    """Verify listener handles a prompts/list request, calls session, publishes response."""
    # 1. Config
    test_request_topic = "mcp/test/request"
    test_base_topic = "mcp/test"
    test_broker_url = "mqtt://localhost:1883"
    test_client_id = "test-listener-client"
    config = MQTTListenerConfig(
        broker_url=test_broker_url,
        client_id=test_client_id,
        base_topic=test_base_topic,
        qos=1,
        stdio_mcp_process=StdioServerParameters(command="dummy_server_cmd", args=[]),
        test_mode=True,
        disable_startup_info=True
    )

    # 2. Mocks
    mock_proc = AsyncMock(spec=asyncio.subprocess.Process)
    mock_proc.pid = 1234
    mock_proc.returncode = None
    mock_proc.stdin = AsyncMock(spec=asyncio.StreamWriter)
    mock_proc.stdout = AsyncMock(spec=asyncio.StreamReader)
    mock_proc.stderr = AsyncMock(spec=asyncio.StreamReader)
    mocker.patch("asyncio.create_subprocess_exec", return_value=mock_proc)

    mock_mqtt_client = AsyncMock(spec=aiomqtt.Client)
    # Add is_connected attribute/method mock
    mock_mqtt_client.is_connected = MagicMock(return_value=True) # Assume connected for test purpose
    
    # Create the request message
    request_id = 5
    # Use simple dict for the request JSON to match what would come from MQTT
    list_prompts_request = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "prompts/list",
        "params": None,
        "cursor": None
    }
    list_prompts_payload = json.dumps(list_prompts_request).encode('utf-8')
    derived_request_topic = f"{test_base_topic}/request"
    correlation_bytes = b"corr-data-123"
    mock_req_message = create_mock_mqtt_message(
        topic=derived_request_topic,
        payload=list_prompts_payload,
        response_topic=f"{test_base_topic}/response/some_requester_id",
        correlation_data=correlation_bytes
    )
    
    # Create a custom async iterator for messages
    class MockAsyncMessageIterator:
        def __init__(self, messages):
            self.messages = messages
            self.index = 0
            
        def __aiter__(self):
            return self
            
        async def __anext__(self):
            if self.index < len(self.messages):
                message = self.messages[self.index]
                self.index += 1
                return message
            else:
                # This will cause the async for loop to end
                # but we need to wait until the test is done
                await asyncio.sleep(10)  # This will be cancelled by listener_task.cancel()
                raise StopAsyncIteration
    
    # Create a context manager for messages
    class MockMessageContextManager:
        def __init__(self, messages):
            self.messages = messages
            
        async def __aenter__(self):
            return MockAsyncMessageIterator(self.messages)
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Set up the messages method to return our context manager
    mock_mqtt_client.messages.return_value = MockMessageContextManager([mock_req_message])
    
    mocker.patch("mcp_mqtt_proxy.mqtt_listener.create_mqtt_client_from_config", return_value=mock_mqtt_client)

    # Mock ClientSession and its methods
    mock_session = AsyncMock(spec=ClientSession)
    # Add mock methods for run() and close() that the listener expects
    mock_session.run = AsyncMock(return_value=None)
    mock_session.close = AsyncMock(return_value=None)

    # Use InitializeResult directly
    # Construct the nested Implementation object for serverInfo
    mock_init_result = InitializeResult(
        protocolVersion="2024-11-05",
        capabilities={ }, # Empty ServerCapabilities for mock
        serverInfo=Implementation(name="mock-server", version="1.0") # Use Implementation directly
    )
    mock_session.initialize.return_value = mock_init_result
    
    # Prepare the expected result from the session call
    expected_result = ListPromptsResult(prompts=[Prompt(name="prompt1")])
    mock_session.request = AsyncMock(return_value=expected_result)
    mocker.patch("mcp_mqtt_proxy.mqtt_listener.ClientSession", return_value=mock_session)
    
    # Mock bridge_stdio_to_anyio_session to behave as an async context manager
    mock_stream = AsyncMock() # The stream object
    mock_bridge_cm = AsyncMock() # The context manager object
    mock_bridge_cm.__aenter__.return_value = (mock_stream, "stdio_session") # What the 'as' clause receives
    mock_bridge_cm.__aexit__.return_value = None # What __aexit__ returns
    # Get the actual mock object returned by mocker.patch
    bridge_patch = mocker.patch("mcp_mqtt_proxy.mqtt_listener.bridge_stdio_to_anyio_session", return_value=mock_bridge_cm)

    # 3. Run the listener (with timeout to prevent hanging)
    listener_task = asyncio.create_task(run_mqtt_listener(config))
    
    # Allow time for the listener to connect, subscribe, and process the message
    # Increase sleep to ensure processing happens
    await asyncio.sleep(2.0)  # Use a longer sleep to allow processing

    # Cancel the listener task to stop it gracefully
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass # Expected cancellation

    # 4. Assertions
    # Check MQTT connection and subscription
    mock_mqtt_client.connect.assert_awaited_once()
    mock_mqtt_client.subscribe.assert_awaited_once_with(derived_request_topic, qos=config.qos)

    # Check MCP Session initialization and call
    mock_session.initialize.assert_awaited_once()
    
    # Check if request was called with the expected parameters
    # For request method, we need to verify a Request object was passed
    mock_session.request.assert_awaited()
    
    # Check that the result was published to the correct topic
    mock_mqtt_client.publish.assert_awaited()  # Should have been called at least once
    
    # Get all the publish calls and check for the response
    call_args_list = mock_mqtt_client.publish.await_args_list
    response_published = False
    for call in call_args_list:
        args, kwargs = call
        if len(args) > 0 and args[0] == f"{test_base_topic}/response/some_requester_id":
            response_published = True
            break
    
    assert response_published, "Response was not published to the expected topic"


async def test_listener_handles_invalid_json_payload(mocker):
    """Verify listener logs error and doesn't publish response for invalid JSON."""
    # 1. Config (Similar to valid test, specific command irrelevant)
    test_base_topic = "mcp/test/error"
    config = MQTTListenerConfig(
        broker_url="mqtt://localhost:1883",
        client_id="test-listener-invalid-json",
        base_topic=test_base_topic,
        qos=1,
        stdio_mcp_process=StdioServerParameters(command="dummy_server_cmd", args=[]),
        test_mode=True,
        disable_startup_info=True
    )
    derived_request_topic = f"{test_base_topic}/request"

    # 2. Mocks (Mostly similar, but configure message payload differently)
    # --- Process Mock ---
    mock_proc = AsyncMock(spec=asyncio.subprocess.Process)
    mock_proc.pid = 5678
    mock_proc.returncode = None
    mock_proc.stdin = AsyncMock(spec=asyncio.StreamWriter)
    mock_proc.stdout = AsyncMock(spec=asyncio.StreamReader)
    mock_proc.stderr = AsyncMock(spec=asyncio.StreamReader)
    mocker.patch("asyncio.create_subprocess_exec", return_value=mock_proc)

    # --- MQTT Client Mock ---
    mock_mqtt_client = AsyncMock(spec=aiomqtt.Client)
    mock_mqtt_client.is_connected = MagicMock(return_value=True)
    mocker.patch("mcp_mqtt_proxy.mqtt_listener.create_mqtt_client_from_config", return_value=mock_mqtt_client)

    # --- MQTT Message Iterator Mock (with invalid payload) ---
    invalid_payload = b"this is not valid json"
    correlation_bytes = b"corr-data-invalid"
    mock_invalid_msg = create_mock_mqtt_message(
        topic=derived_request_topic,
        payload=invalid_payload,
        response_topic=f"{test_base_topic}/response/requester_for_invalid",
        correlation_data=correlation_bytes
    )
    
    # Create a custom async iterator for messages - same as in the previous test
    class MockAsyncMessageIterator:
        def __init__(self, messages):
            self.messages = messages
            self.index = 0
            
        def __aiter__(self):
            return self
            
        async def __anext__(self):
            if self.index < len(self.messages):
                message = self.messages[self.index]
                self.index += 1
                return message
            else:
                # This will cause the async for loop to end
                # but we need to wait until the test is done
                await asyncio.sleep(10)  # This will be cancelled by listener_task.cancel()
                raise StopAsyncIteration
    
    # Create a context manager for messages
    class MockMessageContextManager:
        def __init__(self, messages):
            self.messages = messages
            
        async def __aenter__(self):
            return MockAsyncMessageIterator(self.messages)
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Set up the messages method to return our context manager
    mock_mqtt_client.messages.return_value = MockMessageContextManager([mock_invalid_msg])

    # --- Session Mock (Should not be called for specific methods) ---
    mock_session = AsyncMock(spec=ClientSession)
    mock_session.run = AsyncMock(return_value=None)
    mock_session.close = AsyncMock(return_value=None)
    mock_init_result = InitializeResult(
        protocolVersion="2024-11-05",
        capabilities={},
        serverInfo=Implementation(name="mock-server-invalid", version="1.0")
    )
    mock_session.initialize.return_value = mock_init_result
    # Add mocks for methods that *shouldn't* be called
    mock_session.list_prompts = AsyncMock()
    # Add others if listener handles more methods
    mocker.patch("mcp_mqtt_proxy.mqtt_listener.ClientSession", return_value=mock_session)

    # --- Bridge Mock ---
    mock_stream = AsyncMock()
    mock_bridge_cm = AsyncMock()
    mock_bridge_cm.__aenter__.return_value = (mock_stream, "stdio_session")
    mock_bridge_cm.__aexit__.return_value = None
    bridge_patch = mocker.patch("mcp_mqtt_proxy.mqtt_listener.bridge_stdio_to_anyio_session", return_value=mock_bridge_cm)

    # --- Logger Spy (use real logger instead of mock) ---
    # We'll use a spy instead of a mock to keep real functionality
    real_error = logging.getLogger("mcp_mqtt_proxy.mqtt_listener").error
    error_calls = []
    
    def capture_error(*args, **kwargs):
        error_calls.append((args, kwargs))
        # Call the real function
        real_error(*args, **kwargs)
    
    error_spy = MagicMock(side_effect=capture_error)
    mocker.patch("mcp_mqtt_proxy.mqtt_listener.logger.error", error_spy)

    # 3. Run listener
    listener_task = asyncio.create_task(run_mqtt_listener(config))
    await asyncio.sleep(2.0) # Use a longer sleep to allow processing

    # Cancel listener task
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass

    # 4. Assertions
    # Check connection/subscription happened
    mock_mqtt_client.connect.assert_awaited_once()
    mock_mqtt_client.subscribe.assert_awaited_once_with(derived_request_topic, qos=config.qos)

    # Check Session initialized (this happens before message processing)
    mock_session.initialize.assert_awaited_once()

    # Check specific MCP methods were NOT called
    mock_session.list_prompts.assert_not_awaited()

    # Check that an error was logged about invalid JSON
    assert error_spy.call_count > 0, "No errors were logged"
    
    # The actual error message might contain "Invalid JSON" or similar phrases
    found_json_error = False
    for call in error_spy.call_args_list:
        args, _ = call
        if args and any(err_phrase in str(args[0]) for err_phrase in 
                         ["Invalid JSON", "JSON", "Expecting value", "invalid json"]):
            found_json_error = True
            break
    
    assert found_json_error, "Expected an error about invalid JSON"


async def test_listener_initialization_sequence(mocker):
    """Verify the listener performs the correct initialization steps on startup."""
    # 1. Config
    test_base_topic = "mcp/test/init"
    test_cmd = "/usr/bin/python"
    test_args = ["-m", "mcp.server.stdio"]
    test_cwd = "/tmp"
    config = MQTTListenerConfig(
        broker_url="mqtt://broker.example.com:1883",
        client_id="test-listener-init",
        base_topic=test_base_topic,
        qos=2,
        stdio_mcp_process=StdioServerParameters(
            command=test_cmd,
            args=test_args,
            cwd=test_cwd
            # env can be omitted or mocked as needed
        ),
        test_mode=True,
        disable_startup_info=True
    )
    derived_request_topic = f"{test_base_topic}/request"

    # 2. Mocks
    # --- Process Mock ---
    mock_proc = AsyncMock(spec=asyncio.subprocess.Process)
    mock_proc.pid = 9000
    mock_proc.returncode = None # Important: Process is running
    mock_proc.stdin = AsyncMock(spec=asyncio.StreamWriter)
    mock_proc.stdout = AsyncMock(spec=asyncio.StreamReader)
    mock_proc.stderr = AsyncMock(spec=asyncio.StreamReader)
    subprocess_mock = mocker.patch("asyncio.create_subprocess_exec", return_value=mock_proc)

    # --- MQTT Client Mock ---
    mock_mqtt_client = AsyncMock(spec=aiomqtt.Client)
    # Add is_connected mock needed for cleanup phase
    mock_mqtt_client.is_connected = MagicMock(return_value=True) 
    # Mock connect/subscribe to prevent actual network calls
    mock_mqtt_client.connect = AsyncMock(return_value=None)
    mock_mqtt_client.subscribe = AsyncMock(return_value=None)
    # Mock messages context manager to be an async iterable that does nothing
    mock_messages_iterator = MagicMock()
    mock_messages_iterator.__aiter__.return_value = iter([]) # Empty iterator
    mock_messages_ctx = AsyncMock()
    mock_messages_ctx.__aenter__.return_value = mock_messages_iterator
    mock_mqtt_client.messages.return_value = mock_messages_ctx
    mocker.patch("mcp_mqtt_proxy.mqtt_listener.create_mqtt_client_from_config", return_value=mock_mqtt_client)

    # --- Session Mock ---
    mock_session = AsyncMock(spec=ClientSession)
    mock_session.run = AsyncMock(return_value=None)
    mock_session.close = AsyncMock(return_value=None)
    mock_init_result = InitializeResult(
        protocolVersion="2024-11-05", capabilities={}, serverInfo=Implementation(name="mock-init-server", version="1.0")
    )
    mock_session.initialize = AsyncMock(return_value=mock_init_result) # Mock initialize specifically
    # Use mocker.patch.object for the class constructor
    session_class_mock = mocker.patch("mcp_mqtt_proxy.mqtt_listener.ClientSession", return_value=mock_session)

    # --- Bridge Mock ---
    mock_stream = AsyncMock()
    mock_bridge_cm = AsyncMock()
    mock_bridge_cm.__aenter__.return_value = (mock_stream, "stdio_session")
    mock_bridge_cm.__aexit__.return_value = None
    bridge_patch = mocker.patch("mcp_mqtt_proxy.mqtt_listener.bridge_stdio_to_anyio_session", return_value=mock_bridge_cm)

    # --- Notification Task Mock ---
    notification_task_mock = AsyncMock()
    # Replace the default create_task function to catch notification task creation
    original_create_task = asyncio.create_task
    
    def mock_create_task(coro, *, name=None):
        if name == "notification-forwarder":
            return notification_task_mock
        return original_create_task(coro, name=name)
    
    mocker.patch("asyncio.create_task", side_effect=mock_create_task)

    # 3. Run the listener just long enough for init
    listener_task = asyncio.create_task(run_mqtt_listener(config))
    await asyncio.sleep(0.5) # Shorter sleep, just need init sequence

    # Cancel listener task
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass

    # 4. Assertions (Verify sequence and parameters)
    # Check process start
    subprocess_mock.assert_awaited_once_with(
        test_cmd,
        *test_args, # Unpack args
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=test_cwd,
        env=None # Assuming default env
    )
    
    # Check bridge called with correct parameters
    # The new bridge function takes (proc, session_id, timeout)
    bridge_patch.assert_called_once_with(mock_proc, "stdio_session", config.mcp_timeout)
    mock_bridge_cm.__aenter__.assert_awaited_once()
    
    # Check ClientSession instantiated with bridge stream
    session_class_mock.assert_called_once_with(
        read_stream=mock_stream,
        write_stream=mock_stream,
        read_timeout_seconds=timedelta(seconds=config.mcp_timeout)
    )
    
    # Check session initialized
    mock_session.initialize.assert_awaited_once()
    
    # Check MQTT connection
    mock_mqtt_client.connect.assert_awaited_once()
    # Check MQTT subscription
    mock_mqtt_client.subscribe.assert_awaited_once_with(derived_request_topic, qos=config.qos)


# TODO: Add more tests... (placeholder tests removed for clarity)
# async def test_listener_placeholder(): ...
# def test_placeholder_listener(): ...

# TODO: Add more tests:
# - Test initialization sequence (including session init)
# - Test handling invalid MQTT message (parse error)
# - Test handling unsupported MCP method
# - Test handling MQTT messages without response_topic/correlation_data
# - Test error handling (MQTT disconnect, session errors, stdio process errors) 