"""Integration tests for the MQTT listener component."""

import asyncio
import json
import logging
import os
import uuid
import sys
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Any

import aiomqtt
import anyio
import pytest

from mcp_mqtt_proxy.config import MQTTListenerConfig, StdioServerParameters

from tests.fixtures.mqtt.mqtt_utils import (
    MQTTTestClient,
    wait_for_mqtt_message,
    wait_for_jsonrpc_response,
    publish_jsonrpc_request,
    MQTT_MESSAGE_WAIT_TIMEOUT
)

logger = logging.getLogger(__name__)

# Test timeouts
TEST_TIMEOUT = 20  # seconds for the whole test
MQTT_TIMEOUT = 10  # seconds for MQTT operations


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.mqtt
async def test_mqtt_request_response(
    mqtt_broker_url: str,
    simple_responder_path: Path,
    test_base_topic: str,
    test_request_payload: Dict[str, Any]
):
    """Test the basic MQTT request-response flow using the responder script."""
    # Create topics for the test
    request_topic = f"{test_base_topic}/request"
    response_topic = f"{test_base_topic}/response"
    
    # Configure the MQTT listener
    config = MQTTListenerConfig(
        broker_url=mqtt_broker_url,
        client_id=f"test-listener-{uuid.uuid4().hex[:8]}",
        base_topic=test_base_topic,
        qos=1,
        stdio_mcp_process=StdioServerParameters(
            command=Path(sys.executable).as_posix(),
            args=[str(simple_responder_path)],
            cwd=None
        ),
        test_mode=True,
        disable_startup_info=True
    )
    
    # Start MQTT listener in a separate task
    from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener
    
    # Create a test client
    async with MQTTTestClient(host=config.broker_host, port=config.broker_port) as client:
        # Start the MQTT listener in the background
        listener_task = asyncio.create_task(
            run_mqtt_listener(config)
        )
        
        try:
            # Wait for listener to connect and initialize
            await asyncio.sleep(2.0)
            
            # Perform request-response test
            logger.info(f"Publishing request to {request_topic}")
            logger.debug(f"Request payload: {test_request_payload}")
            
            # Send the request and get correlation ID
            correlation_id = await client.publish_request(
                topic=request_topic,
                request=test_request_payload,
                response_topic=response_topic
            )
            
            # Wait for the response
            response = await client.wait_for_response(
                topic=response_topic,
                request_id=correlation_id,
                timeout=MQTT_TIMEOUT
            )
            
            # Verify the response
            logger.info(f"Received response: {response}")
            assert response is not None
            assert "jsonrpc" in response
            assert response["jsonrpc"] == "2.0"
            assert "id" in response
            assert response["id"] == correlation_id
            assert "result" in response
            assert "message" in response["result"]
            assert response["result"]["message"] == f"Echo: {test_request_payload['params']['message']}"
        
        finally:
            # Clean up listener task
            if not listener_task.done():
                logger.info("Cancelling MQTT listener task")
                listener_task.cancel()
                try:
                    await asyncio.wait_for(listener_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.mqtt
async def test_mqtt_notification_forwarding(
    mqtt_broker_url: str,
    notification_responder_path: Path,
    test_base_topic: str
):
    """Test that notifications sent by the responder are forwarded to MQTT."""
    # Create topics for the test
    notification_topic = f"{test_base_topic}/notification"
    
    # Configure the MQTT listener
    config = MQTTListenerConfig(
        broker_url=mqtt_broker_url,
        client_id=f"test-notif-{uuid.uuid4().hex[:8]}",
        base_topic=test_base_topic,
        qos=1,
        stdio_mcp_process=StdioServerParameters(
            command=Path(sys.executable).as_posix(),
            args=[str(notification_responder_path)],
            cwd=None
        ),
        test_mode=True,
        disable_startup_info=True
    )
    
    # Start MQTT listener in a separate task
    from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener
    
    # Create a test client
    async with MQTTTestClient(host=config.broker_host, port=config.broker_port) as client:
        # Start the MQTT listener in the background
        listener_task = asyncio.create_task(
            run_mqtt_listener(config)
        )
        
        try:
            # Wait for listener to connect and initialize
            await asyncio.sleep(2.0)
            
            # Subscribe to the notification topic
            if client.client:
                await client.client.subscribe(notification_topic, qos=1)
            
            # The notification responder should send notifications automatically
            logger.info(f"Waiting for notifications on {notification_topic}")
            
            # Define a predicate to match notification messages
            def is_notification(message):
                if not isinstance(message.payload, dict):
                    return False
                return (
                    "jsonrpc" in message.payload and
                    "method" in message.payload and
                    "id" not in message.payload
                )
            
            # Wait for at least one notification
            notification = await wait_for_mqtt_message(
                client=client.client,
                topic_filter=notification_topic,
                predicate=is_notification,
                timeout=MQTT_TIMEOUT
            )
            
            # Verify the notification
            logger.info(f"Received notification: {notification.payload}")
            assert notification.payload is not None
            assert "jsonrpc" in notification.payload
            assert notification.payload["jsonrpc"] == "2.0"
            assert "method" in notification.payload
            assert "params" in notification.payload
        
        finally:
            # Clean up listener task
            if not listener_task.done():
                logger.info("Cancelling MQTT listener task")
                listener_task.cancel()
                try:
                    await asyncio.wait_for(listener_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass 