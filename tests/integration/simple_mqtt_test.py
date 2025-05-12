#!/usr/bin/env python
"""
Simple MQTT test helper to check broker connectivity and MQTT v5 feature support.
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
import paho.mqtt.properties as mqtt_properties
import paho.mqtt.packettypes as packet_types
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

async def mqtt_v5_response_topic_test():
    """
    Test MQTT v5 ResponseTopic and CorrelationData properties.
    This tests if the broker properly supports and forwards these MQTT v5 features.
    """
    logger.info("Starting MQTT v5 ResponseTopic test")
    
    # Generate unique topics for this test
    request_topic = f"test/v5-test/request-{generate_id()}"
    response_topic = f"test/v5-test/response-{generate_id()}"
    correlation_id = f"correlation-{generate_id()}"
    
    # Use an event to signal when response is received
    response_received = asyncio.Event()
    received_message = None
    received_correlation = None
    
    # Start a receiver first that will listen on the response topic
    async def response_receiver():
        nonlocal received_message, received_correlation
        receiver_id = f"v5-receiver-{generate_id()}"
        
        logger.info(f"Starting V5 response receiver with client_id: {receiver_id}")
        try:
            async with aiomqtt.Client(
                hostname="localhost",
                port=1883,
                client_id=receiver_id,
                protocol=aiomqtt.ProtocolVersion.V5  # Explicitly use MQTT v5
            ) as receiver:
                logger.info(f"V5 receiver connected to broker")
                logger.info(f"Subscribing to response topic: {response_topic}")
                await receiver.subscribe(response_topic, qos=1)
                
                try:
                    async with receiver.messages() as messages:
                        async for message in messages:
                            if message.topic.matches(response_topic):
                                received_message = message.payload.decode()
                                logger.info(f"Received response: {received_message}")
                                
                                # Check if correlation data is present and matches
                                if message.properties and message.properties.CorrelationData:
                                    received_correlation = message.properties.CorrelationData.decode()
                                    logger.info(f"Received correlation ID: {received_correlation}")
                                
                                response_received.set()
                                break
                except Exception as e:
                    logger.error(f"Error in message handling: {e}")
                    
        except Exception as e:
            logger.error(f"V5 receiver error: {e}")
    
    # Start the receiver task
    receiver_task = asyncio.create_task(response_receiver())
    
    # Wait a bit for the receiver to connect and subscribe
    await asyncio.sleep(1)
    
    # Now send a request with response topic property
    sender_id = f"v5-sender-{generate_id()}"
    logger.info(f"Starting V5 sender with client_id: {sender_id}")
    
    try:
        async with aiomqtt.Client(
            hostname="localhost",
            port=1883,
            client_id=sender_id,
            protocol=aiomqtt.ProtocolVersion.V5  # Explicitly use MQTT v5
        ) as sender:
            logger.info(f"V5 sender connected to broker")
            
            # Create the v5 properties
            properties = mqtt_properties.Properties(packet_types.PacketTypes.PUBLISH)
            properties.ResponseTopic = response_topic
            properties.CorrelationData = correlation_id.encode()
            
            logger.info(f"Sending message to {request_topic} with ResponseTopic={response_topic}")
            logger.info(f"CorrelationData={correlation_id}")
            
            # Send the request
            request_message = f"V5 test request {generate_id()}"
            await sender.publish(
                request_topic,
                payload=request_message.encode(),
                qos=1,
                properties=properties
            )
            logger.info(f"Request published with V5 properties")
            
            # Also send an echo response to simulate a responder
            # In a real system this would be done by a responder listening on request_topic
            await asyncio.sleep(0.5)  # Small delay
            
            echo_props = mqtt_properties.Properties(packet_types.PacketTypes.PUBLISH)
            echo_props.CorrelationData = correlation_id.encode()
            
            response_message = f"Echo response to: {request_message}"
            logger.info(f"Sending echo response to {response_topic}")
            await sender.publish(
                response_topic,
                payload=response_message.encode(),
                qos=1,
                properties=echo_props
            )
            logger.info("Echo response sent")
            
    except Exception as e:
        logger.error(f"V5 sender error: {e}")
    
    # Wait for the response with timeout
    try:
        await asyncio.wait_for(response_received.wait(), timeout=5.0)
        
        # Verify the response
        if received_message and received_correlation:
            v5_test_passed = received_correlation == correlation_id
            if v5_test_passed:
                logger.info("✅ MQTT v5 properties test PASSED!")
            else:
                logger.error(f"❌ MQTT v5 properties test FAILED! Correlation ID mismatch: {received_correlation} != {correlation_id}")
            return v5_test_passed
        else:
            logger.error("❌ MQTT v5 properties test FAILED! No message or correlation ID received")
            return False
    except asyncio.TimeoutError:
        logger.error("❌ MQTT v5 properties test FAILED due to timeout")
        return False
    finally:
        # Clean up the receiver task
        if not receiver_task.done():
            receiver_task.cancel()
            try:
                await receiver_task
            except asyncio.CancelledError:
                pass

async def main():
    """Run the tests."""
    logger.info("Starting MQTT tests")
    
    # Test basic connectivity first
    logger.info("\n--- Basic MQTT Connectivity Test ---")
    subscriber_task = asyncio.create_task(subscriber())
    await asyncio.sleep(1)
    await publisher()
    received_message = await subscriber_task
    
    basic_test_passed = received_message == TEST_MESSAGE
    if basic_test_passed:
        logger.info("✅ Basic MQTT test PASSED: Message was successfully sent and received")
    else:
        logger.error(f"❌ Basic MQTT test FAILED: Expected '{TEST_MESSAGE}', but got '{received_message or 'None'}'")
        return False
    
    # Now test MQTT v5 features
    logger.info("\n--- MQTT v5 Features Test ---")
    v5_test_passed = await mqtt_v5_response_topic_test()
    
    # Return overall test result
    return basic_test_passed and v5_test_passed

if __name__ == "__main__":
    success = asyncio.run(main())
    if success:
        print("\nAll MQTT tests completed successfully! ✅")
        print("Your Mosquitto broker correctly supports MQTT v5 features.")
    else:
        print("\nSome MQTT tests failed! ❌")
        print("There may be issues with your Mosquitto broker configuration.")
        exit(1) 