"""Integration tests for MCP notifications over MQTT."""

import asyncio
import json
import logging
import sys
import uuid
from pathlib import Path

import aiomqtt
import pytest

from mcp_mqtt_proxy.config import MQTTListenerConfig, StdioServerParameters
from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener

from tests.fixtures.mqtt.mqtt_utils import (
    MQTTTestClient,
    wait_for_mqtt_message,
    MQTT_MESSAGE_WAIT_TIMEOUT
)

logger = logging.getLogger(__name__)

# Test timeouts
TEST_TIMEOUT = 20  # seconds for the whole test
NOTIFICATION_WAIT_TIMEOUT = 15  # seconds


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.mqtt
async def test_mqtt_notifications(
    mqtt_broker_url: str,
    notification_responder_path: Path,
    random_topic_prefix: str
):
    """Test that notifications from the MCP server are forwarded to MQTT."""
    # Define topics
    notification_topic = f"{random_topic_prefix}/notification"
    
    # Configure the MQTT listener
    config = MQTTListenerConfig(
        broker_url=mqtt_broker_url,
        client_id=f"test-notif-{uuid.uuid4().hex[:8]}",
        base_topic=random_topic_prefix,
        qos=1,
        stdio_mcp_process=StdioServerParameters(
            command=Path(sys.executable).as_posix(),
            args=[str(notification_responder_path)],
            cwd=None
        ),
        test_mode=True,
        disable_startup_info=True
    )
    
    # Create a test client for subscribing to notifications
    async with MQTTTestClient(
        host=config.broker_host,
        port=config.broker_port
    ) as client:
        # Start the MQTT listener in a separate task
        logger.info(f"Starting MQTT listener with base topic: {random_topic_prefix}")
        listener_task = asyncio.create_task(run_mqtt_listener(config))
        
        try:
            # Allow time for the listener to connect and initialize
            await asyncio.sleep(2.0)
            
            # Subscribe to the notification topic
            if client.client:
                await client.client.subscribe(notification_topic, qos=1)
                logger.info(f"Subscribed to notification topic: {notification_topic}")
            else:
                pytest.fail("MQTT client is not connected")
            
            # Define a predicate to recognize notifications
            def is_status_notification(message):
                """Check if a message is a status notification."""
                if not isinstance(message.payload, dict):
                    return False
                    
                payload = message.payload
                return (
                    "jsonrpc" in payload and
                    "method" in payload and
                    payload["method"] == "status/update" and
                    "id" not in payload
                )
            
            # Wait for a notification (the notification_responder sends them periodically)
            logger.info(f"Waiting for notification on topic: {notification_topic}")
            notification = await wait_for_mqtt_message(
                client=client.client,
                topic_filter=notification_topic,
                predicate=is_status_notification,
                timeout=NOTIFICATION_WAIT_TIMEOUT
            )
            
            # Verify the notification
            logger.info(f"Received notification: {notification.payload}")
            assert notification.payload is not None
            assert "jsonrpc" in notification.payload
            assert notification.payload["jsonrpc"] == "2.0"
            assert "method" in notification.payload
            assert notification.payload["method"] == "status/update"
            assert "params" in notification.payload
            assert "status" in notification.payload["params"]
            assert notification.payload["params"]["status"] == "running"
        
        finally:
            # Clean up listener task
            if not listener_task.done():
                logger.info("Cancelling MQTT listener task")
                listener_task.cancel()
                try:
                    await asyncio.wait_for(listener_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            logger.info("Test completed") 