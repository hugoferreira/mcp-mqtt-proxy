"""Unit tests for the MQTT Publisher mode (stdio Client -> MQTT)."""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

from mcp_mqtt_proxy.config import MQTTPublisherConfig
from mcp_mqtt_proxy.mqtt_publisher import run_mqtt_publisher

# Use pytest-asyncio decorator for async tests
pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture
def mock_publisher_config():
    """Provides a default MQTTPublisherConfig for tests."""
    # Assume simple URL, let create_mqtt_client_from_config handle parsing if needed in tests
    return MQTTPublisherConfig(
        broker_url="mqtt://mqtt.test.com:1883",
        client_id="test_pub_client",
        qos=1,
        # stdio_mcp_process is no longer part of MQTTPublisherConfig
        base_topic="mcp/test_client",
        username="user",
        password="pw",
        tls_ca_certs=None,
        tls_certfile=None,
        tls_keyfile=None,
        message_expiry_interval=None,
        # log_level="DEBUG", # log_level is not part of config
    )

@pytest_asyncio.fixture
def mock_loop_and_created_tasks(mocker):
    """
    Mocks asyncio.create_task directly and captures the created task mocks.
    Returns the patch object and a list of task mocks.
    """
    # Prepare a list to hold task mocks that will be created
    created_task_mocks_list = []

    def create_task_side_effect(coro, *, name=None): # Use * to enforce keyword args like real signature
        # Create a new AsyncMock for each task
        task_mock = AsyncMock(spec=asyncio.Task, name=name or "UnnamedTask")
        task_mock.cancel = MagicMock(name=f"{name}_cancel" if name else "UnnamedTask_cancel")
        # Make the task awaitable (important for gather/wait)
        async def awaitable_mock(*args, **kwargs):
            return None # Simple awaitable behavior
        task_mock.side_effect = awaitable_mock # Make the mock itself awaitable
        created_task_mocks_list.append(task_mock)
        return task_mock

    # Patch asyncio.create_task directly
    create_task_patch = mocker.patch("asyncio.create_task", side_effect=create_task_side_effect)
    
    # Return the patch object and the list of created task mocks
    return create_task_patch, created_task_mocks_list

@pytest_asyncio.fixture
def mock_mqtt_client(mocker):
    """Mocks the aiomqtt.Client."""
    # spec_set defines the interface.
    client_mock = AsyncMock(
        spec_set=['connect', 'subscribe', 'publish', 'disconnect', 'messages', '__aenter__', '__aexit__', 'is_connected'],
        name="MockMQTTClientInstance" 
    )
    
    # Explicitly make specific methods AsyncMocks as they are awaitable in the actual client
    client_mock.connect = AsyncMock(name="MockMQTTClientConnect")
    client_mock.disconnect = AsyncMock(name="MockMQTTClientDisconnect")
    client_mock.subscribe = AsyncMock(name="MockMQTTClientSubscribe")
    client_mock.publish = AsyncMock(name="MockMQTTClientPublish")

    # Make __aenter__ and __aexit__ AsyncMocks to allow await assertions
    client_mock.__aenter__ = AsyncMock(name="MockMQTTClientAenter")
    client_mock.__aexit__ = AsyncMock(name="MockMQTTClientAexit")
    
    # Configure __aenter__ to return the client mock for 'async with ... as client:'
    client_mock.__aenter__.return_value = client_mock 
    # Configure __aexit__ return value (often None or False for no exception)
    client_mock.__aexit__.return_value = None
    
    client_mock.is_connected = MagicMock(return_value=True) 

    # Patch where create_mqtt_client_from_config looks for aiomqtt.Client
    mocker.patch("mcp_mqtt_proxy.config.aiomqtt.Client", return_value=client_mock)
    # Patch where mqtt_publisher module itself might directly look for aiomqtt.Client (if any)
    mocker.patch("mcp_mqtt_proxy.mqtt_publisher.aiomqtt.Client", return_value=client_mock, create=True)
    return client_mock

@pytest_asyncio.fixture
def mock_session_instance(mocker): # Renamed from mock_session to avoid confusion
    """Fixture is no longer needed as publisher doesn't use ClientSession."""
    # This fixture can be removed or return None
    return None 

@pytest_asyncio.fixture
def mock_stdio_streams_setup(mocker):
    """Mocks the asyncio streams for stdin/stdout."""
    # Mock the objects returned by connect_read_pipe/connect_write_pipe
    mock_reader = AsyncMock(spec=asyncio.StreamReader, name="MockLoopStdioReader")
    mock_writer_transport = AsyncMock(spec=asyncio.Transport, name="MockLoopWriterTransport")
    mock_writer_protocol = AsyncMock(spec=asyncio.Protocol, name="MockLoopWriterProtocol")
    mock_writer = AsyncMock(spec=asyncio.StreamWriter, name="MockLoopStdioWriter")
    mock_writer.drain = AsyncMock(name="MockLoopWriterDrain")

    # Mock asyncio.open_connection which is used by _open_stdio_streams
    # It might be called twice (once for stdin, once for stdout) or handled differently.
    # Let's assume _open_stdio_streams is called once and returns both.
    # We need to mock the _open_stdio_streams helper function directly.

    # Mock the loop methods used to set up streams
    mock_loop = AsyncMock(spec=asyncio.AbstractEventLoop, name="MockEventLoop")
    mock_loop.connect_read_pipe = AsyncMock(name="MockConnectReadPipe")
    # connect_write_pipe returns (transport, protocol)
    mock_loop.connect_write_pipe = AsyncMock(return_value=(mock_writer_transport, mock_writer_protocol), name="MockConnectWritePipe")

    # Patch asyncio.get_running_loop to return our mock loop
    get_loop_patch = mocker.patch("asyncio.get_running_loop", return_value=mock_loop)

    # Patch the StreamReader and StreamWriter constructors as they are called directly
    stream_reader_patch = mocker.patch("asyncio.StreamReader", return_value=mock_reader)
    stream_writer_patch = mocker.patch("asyncio.StreamWriter", return_value=mock_writer)

    # Return the relevant mocks needed for assertions
    return get_loop_patch, mock_loop, stream_reader_patch, stream_writer_patch, mock_reader, mock_writer

async def test_publisher_initialization_sequence(
    mocker,
    mock_publisher_config: MQTTPublisherConfig,
    mock_loop_and_created_tasks: tuple[MagicMock, list[AsyncMock]],
    mock_mqtt_client: AsyncMock,
    mock_stdio_streams_setup: tuple[MagicMock, AsyncMock, MagicMock, MagicMock, AsyncMock, AsyncMock],
):
    """Verify the initialization sequence of the publisher."""
    create_task_patch, created_tasks_list = mock_loop_and_created_tasks
    get_loop_patch, mock_loop, stream_reader_patch, stream_writer_patch, mock_reader, mock_writer = mock_stdio_streams_setup
    
    # Patch create_mqtt_client_from_config to return our mqtt client mock
    create_client_patch = mocker.patch("mcp_mqtt_proxy.mqtt_publisher.create_mqtt_client_from_config", return_value=mock_mqtt_client)

    # Mock asyncio.Event to prevent the test from hanging on shutdown_event.wait()
    mock_event = MagicMock(spec=asyncio.Event, name="MockAsyncioEvent")
    # Make wait() return immediately
    mock_event.wait = AsyncMock(return_value=None)
    # Create a regular mock for the set method
    mock_event.set = MagicMock(name="MockEventSet")
    
    # Patch asyncio.Event to return our mock
    event_patch = mocker.patch("asyncio.Event", return_value=mock_event)

    # Make the gather_patch properly awaitable
    async def mock_gather_async(*args, **kwargs):
        # This awaitable function can be configured as needed
        return None
    
    # Make the gather_patch an AsyncMock that actually returns an awaitable when called
    gather_patch = mocker.patch("asyncio.gather", side_effect=mock_gather_async)

    # --- Run the publisher ---
    try:
        await run_mqtt_publisher(mock_publisher_config)
    except Exception as e:
        pytest.fail(f"Publisher raised unexpected exception: {e}")

    # --- Assertions ---

    # 1. Get Event Loop
    get_loop_patch.assert_called_once()

    # 1. Get stdio streams
    # Assert StreamReader constructor called
    stream_reader_patch.assert_called_once()
    # Assert connect_read_pipe called
    mock_loop.connect_read_pipe.assert_awaited_once()
    # Assert connect_write_pipe called
    mock_loop.connect_write_pipe.assert_awaited_once()
    # Assert StreamWriter constructor called
    stream_writer_patch.assert_called_once()

    # 2. MQTT Client creation and connection
    create_client_patch.assert_called_once_with(mock_publisher_config)
    # Assert the MQTT client context manager was entered
    mock_mqtt_client.__aenter__.assert_awaited_once()
    # Assert connect was awaited (usually implicitly by __aenter__ in aiomqtt)
    # mock_mqtt_client.connect.assert_awaited_once() # Might not be explicit if __aenter__ handles it

    # 3. Subscription
    expected_response_topic = f"{mock_publisher_config.base_topic}/response/{mock_publisher_config.client_id}"
    mock_mqtt_client.subscribe.assert_awaited_once_with(expected_response_topic, qos=mock_publisher_config.qos)

    # 4. Bridge Task Creation
    # Check that asyncio.create_task was called three times (mqtt_message_handler and two bridge tasks)
    assert create_task_patch.call_count == 3
    # Check the names of the created tasks
    # Task names depend on the implementation in run_mqtt_publisher
    # Let's assume they are named based on the function names
    call_args_list = create_task_patch.call_args_list
    created_task_names = {call.kwargs.get('name') for call in call_args_list}
    assert "mqtt-handler" in created_task_names
    assert "stdin-to-mqtt" in created_task_names
    assert "mqtt-to-stdout" in created_task_names

    # 5. Wait/Gather Tasks
    # Check that the shutdown event's wait method was called
    mock_event.wait.assert_awaited_once()

    # Check that gather was called in the finally block to clean up tasks
    gather_patch.assert_called() # At least once

    # 6. MQTT Disconnect (__aexit__)
    # Check that the MQTT client context manager was exited (implicitly calls disconnect)
    mock_mqtt_client.__aexit__.assert_awaited_once()

    # 7. Ensure stdio streams are closed (optional, depending on implementation)
    # Publisher might close writer explicitly, reader might close on EOF
    # Check if mock_writer.close() was called if necessary

# TODO: Add tests for message processing:
# - Test _parse_mcp_message_from_line with valid/invalid JSON/MCP

@pytest.mark.skip(reason="MQTT publisher tests not yet implemented")
def test_placeholder_publisher():
    pass 