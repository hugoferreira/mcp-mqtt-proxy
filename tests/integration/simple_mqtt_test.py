#!/usr/bin/env python
"""
Simple MQTT test helper to check broker connectivity.
Runs a publisher and subscriber to local MQTT broker.

Usage:
    uv run python tests/integration/simple_mqtt_test.py
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Dict, Tuple, Any, List

import aiomqtt
from mcp_mqtt_proxy.utils import generate_id

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test parameters
TEST_TOPIC = f"test/simple-mqtt-test-{generate_id()}"
TEST_MESSAGE = f"Hello MQTT {generate_id()}"

async def publisher():
    """Publish a test message to the broker."""
    client_id = f"publisher-{generate_id()}"
    logger.info(f"Starting publisher with client_id: {client_id}")
    
    try:
        async with aiomqtt.Client(
            hostname="localhost",
            port=1883,
            client_id=client_id
        ) as client:
            logger.info(f"Publisher connected to broker")
            logger.info(f"Publishing message '{TEST_MESSAGE}' to topic: {TEST_TOPIC}")
            await client.publish(TEST_TOPIC, TEST_MESSAGE.encode())
            # Wait a bit to ensure the message is delivered
            await asyncio.sleep(1)
            logger.info("Publisher complete")
    except Exception as e:
        logger.error(f"Publisher error: {e}")

async def subscriber():
    """Subscribe to test topic and receive a message."""
    client_id = f"subscriber-{generate_id()}"
    logger.info(f"Starting subscriber with client_id: {client_id}")
    
    received_message = None
    
    try:
        async with aiomqtt.Client(
            hostname="localhost",
            port=1883,
            client_id=client_id
        ) as client:
            logger.info(f"Subscriber connected to broker")
            logger.info(f"Subscribing to topic: {TEST_TOPIC}")
            await client.subscribe(TEST_TOPIC)
            
            # Use asyncio.wait_for to implement a hard timeout
            try:
                async def receive_message():
                    async with client.messages() as messages:
                        async for message in messages:
                            if message.topic.matches(TEST_TOPIC):
                                return message.payload.decode()
                
                # Set a 5 second timeout
                received_message = await asyncio.wait_for(receive_message(), timeout=5.0)
                logger.info(f"Received message: {received_message}")
            
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for message after 5.0s")
            
            logger.info("Subscriber complete")
            return received_message
            
    except Exception as e:
        logger.error(f"Subscriber error: {e}")
        return None

async def main():
    """Run the test."""
    logger.info("Starting MQTT simple test")
    
    # Start subscriber first to ensure it's ready to receive
    subscriber_task = asyncio.create_task(subscriber())
    
    # Wait a moment for subscriber to connect
    await asyncio.sleep(1)
    
    # Start publisher
    await publisher()
    
    # Wait for subscriber to finish
    received_message = await subscriber_task
    
    # Verify results
    if received_message == TEST_MESSAGE:
        logger.info("✅ Test PASSED: Message was successfully sent and received")
        return True
    else:
        logger.error(f"❌ Test FAILED: Expected '{TEST_MESSAGE}', but got '{received_message or 'None'}'")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    if success:
        print("\nSimple MQTT test completed successfully!")
    else:
        print("\nSimple MQTT test failed!")
        exit(1) 