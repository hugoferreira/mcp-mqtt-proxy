#!/usr/bin/env python
"""
Direct MQTT request/response echo test.
This completely bypasses the mcp_mqtt_proxy components and directly tests
the MQTT broker's ability to handle request/response patterns with V5 properties.
"""

import asyncio
import json
import logging
import sys
import uuid
import os
import time
from pathlib import Path
import traceback

import aiomqtt
import paho.mqtt.properties as mqtt_properties
import paho.mqtt.packettypes as packettypes

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(thread)d - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("mqtt_direct_echo")

# Test settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
BASE_TOPIC = f"test/direct-echo/{uuid.uuid4()}"

def hex_dump(data, prefix=""):
    """Create a hex dump of binary data for debugging."""
    if isinstance(data, str):
        data = data.encode('utf-8')
    
    result = []
    for i in range(0, len(data), 16):
        chunk = data[i:i+16]
        hex_values = ' '.join(f'{b:02x}' for b in chunk)
        printable = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        result.append(f"{prefix}{i:04x}: {hex_values:<48} {printable}")
    return '\n'.join(result)

def log_mqtt_message(message, prefix=""):
    """Log an MQTT message with detailed information."""
    try:
        topic = str(message.topic)
        try:
            payload = message.payload.decode('utf-8')
            payload_preview = payload[:100] + ('...' if len(payload) > 100 else '')
        except UnicodeDecodeError:
            payload = f"<binary data of length {len(message.payload)}>"
            payload_preview = hex_dump(message.payload, '  ')
            
        props_str = ""
        if message.properties:
            props = []
            if hasattr(message.properties, "ResponseTopic") and message.properties.ResponseTopic:
                props.append(f"ResponseTopic={message.properties.ResponseTopic}")
            if hasattr(message.properties, "CorrelationData") and message.properties.CorrelationData:
                corr_data = message.properties.CorrelationData
                if isinstance(corr_data, bytes):
                    corr_data_str = corr_data.decode('utf-8', errors='replace')
                    props.append(f"CorrelationData={corr_data_str}")
            props_str = ", ".join(props)
            
        log_msg = f"{prefix}MQTT Message on topic '{topic}'"
        if props_str:
            log_msg += f" with properties [{props_str}]"
        log_msg += f": {payload_preview}"
        
        logger.debug(log_msg)
        return payload
    except Exception as e:
        logger.exception(f"Error logging MQTT message: {e}")
        return None

async def responder():
    """Simple MQTT responder that listens on a request topic and echoes back responses."""
    client_id = f"responder-{uuid.uuid4()}"
    logger.info(f"Starting responder with client ID: {client_id}")
    logger.info(f"Request topic: {BASE_TOPIC}/request")
    
    request_topic = f"{BASE_TOPIC}/request"
    
    async with aiomqtt.Client(
        hostname=MQTT_BROKER,
        port=MQTT_PORT,
        client_id=client_id,
        protocol=aiomqtt.ProtocolVersion.V5,
        logger=logging.getLogger("responder.mqtt")
    ) as client:
        # Subscribe to request topic
        await client.subscribe(request_topic, qos=1)
        logger.info(f"Responder subscribed to {request_topic}")
        
        # Track statistics
        msg_count = 0
        start_time = time.time()
        
        # Process messages
        async with client.messages() as messages:
            logger.info("Responder message loop started")
            async for message in messages:
                try:
                    msg_count += 1
                    msg_time = time.time()
                    
                    # Log detailed message info
                    logger.info(f"[MSG-{msg_count}] Responder received message")
                    payload_text = log_mqtt_message(message, f"[MSG-{msg_count}] ")
                    
                    # Extract MQTT v5 properties
                    response_topic = None
                    correlation_data = None
                    
                    if message.properties:
                        response_topic = getattr(message.properties, "ResponseTopic", None)
                        correlation_data = getattr(message.properties, "CorrelationData", None)
                        
                        if response_topic:
                            logger.info(f"[MSG-{msg_count}] Found ResponseTopic: {response_topic}")
                        if correlation_data:
                            if isinstance(correlation_data, bytes):
                                corr_data_str = correlation_data.decode('utf-8', errors='replace')
                                logger.info(f"[MSG-{msg_count}] Found CorrelationData: {corr_data_str}")
                            else:
                                logger.info(f"[MSG-{msg_count}] Found CorrelationData: {correlation_data}")
                    
                    # Check for response topic
                    if not response_topic:
                        logger.warning(f"[MSG-{msg_count}] No ResponseTopic in message, cannot reply")
                        continue
                    
                    # Parse JSON
                    try:
                        request_data = json.loads(payload_text)
                        logger.debug(f"[MSG-{msg_count}] Parsed request: {json.dumps(request_data, indent=2)}")
                        
                        # Process the request (echo it back)
                        if request_data.get("method") == "test/echo":
                            message_param = request_data.get("params", {}).get("message", "")
                            logger.info(f"[MSG-{msg_count}] Echo request with message: {message_param}")
                            
                            # Create echo response
                            response_data = {
                                "jsonrpc": "2.0",
                                "id": request_data.get("id"),
                                "result": {
                                    "message": f"Echo: {message_param}"
                                }
                            }
                            
                            # Prepare response JSON
                            response_json = json.dumps(response_data)
                            logger.debug(f"[MSG-{msg_count}] Prepared response: {response_json}")
                            
                            # Set up properties for the response
                            properties = mqtt_properties.Properties(packettypes.PacketTypes.PUBLISH)
                            
                            # Set correlation data if we have it
                            if correlation_data:
                                properties.CorrelationData = correlation_data
                                logger.debug(f"[MSG-{msg_count}] Set CorrelationData in response")
                            
                            # Publish response
                            logger.info(f"[MSG-{msg_count}] Publishing response to {response_topic}")
                            await client.publish(
                                response_topic,
                                payload=response_json.encode('utf-8'),
                                qos=1,
                                properties=properties
                            )
                            
                            # Log timing
                            elapsed = time.time() - msg_time
                            logger.info(f"[MSG-{msg_count}] Response published successfully in {elapsed:.6f}s")
                        else:
                            logger.warning(f"[MSG-{msg_count}] Unknown method: {request_data.get('method')}")
                    except json.JSONDecodeError as e:
                        logger.error(f"[MSG-{msg_count}] Error parsing JSON: {e}")
                        logger.error(f"[MSG-{msg_count}] Raw payload: {payload_text}")
                except Exception as e:
                    logger.exception(f"[MSG-{msg_count}] Error handling message: {e}")
                    
        # Log final statistics
        runtime = time.time() - start_time
        logger.info(f"Responder processed {msg_count} messages in {runtime:.2f}s")

async def requester():
    """Client that sends a request and waits for a response."""
    client_id = f"requester-{uuid.uuid4()}"
    logger.info(f"Starting requester with client ID: {client_id}")
    
    request_topic = f"{BASE_TOPIC}/request"
    response_topic = f"{BASE_TOPIC}/response"
    
    logger.info(f"Request topic: {request_topic}")
    logger.info(f"Response topic: {response_topic}")
    
    # Create test request
    test_id = f"test-{uuid.uuid4()}"
    test_request = {
        "jsonrpc": "2.0",
        "id": test_id,
        "method": "test/echo",
        "params": {
            "message": "Hello MQTT direct echo test"
        }
    }
    
    # Flag for response received
    response_received = asyncio.Event()
    received_message = None
    
    async with aiomqtt.Client(
        hostname=MQTT_BROKER,
        port=MQTT_PORT,
        client_id=client_id,
        protocol=aiomqtt.ProtocolVersion.V5,
        logger=logging.getLogger("requester.mqtt")
    ) as client:
        # Subscribe to all topics for debugging
        await client.subscribe("#", qos=1)
        logger.info(f"Requester subscribed to all topics for monitoring")
        
        # Subscribe specifically to response topic
        await client.subscribe(response_topic, qos=1)
        logger.info(f"Requester subscribed to response topic: {response_topic}")
        
        # Start message listener
        async def listen_for_response():
            logger.info(f"Starting response listener on {response_topic}")
            msg_count = 0
            start_time = time.time()
            
            try:
                async with client.messages() as messages:
                    logger.info("Response listener started")
                    async for message in messages:
                        msg_count += 1
                        try:
                            topic = str(message.topic)
                            
                            # Log detailed message info
                            logger.debug(f"[RECV-{msg_count}] Requester received message on {topic}")
                            payload = log_mqtt_message(message, f"[RECV-{msg_count}] ")
                            
                            if topic == response_topic:
                                logger.info(f"[RECV-{msg_count}] Found matching response on {topic}")
                                try:
                                    nonlocal received_message
                                    received_message = json.loads(payload)
                                    logger.info(f"[RECV-{msg_count}] Parsed response: {json.dumps(received_message, indent=2)}")
                                    response_received.set()
                                    logger.info(f"[RECV-{msg_count}] Response event set")
                                    
                                    # Check if response has the expected ID
                                    if received_message.get("id") != test_id:
                                        logger.warning(f"[RECV-{msg_count}] Response ID mismatch: expected={test_id}, got={received_message.get('id')}")
                                except json.JSONDecodeError as e:
                                    logger.error(f"[RECV-{msg_count}] Error parsing response JSON: {e}")
                                    logger.error(f"[RECV-{msg_count}] Raw response: {payload}")
                        except Exception as e:
                            logger.exception(f"[RECV-{msg_count}] Error processing response: {e}")
            except Exception as e:
                logger.exception(f"Error in response listener: {e}")
            finally:
                runtime = time.time() - start_time
                logger.info(f"Response listener processed {msg_count} messages in {runtime:.2f}s")
        
        listener_task = asyncio.create_task(listen_for_response())
        
        try:
            # Wait to ensure subscriptions are active
            logger.info("Waiting for subscriptions to be established...")
            await asyncio.sleep(1.0)
            
            # Create properties with ResponseTopic
            properties = mqtt_properties.Properties(packettypes.PacketTypes.PUBLISH)
            properties.ResponseTopic = response_topic
            properties.CorrelationData = test_id.encode('utf-8')
            
            # Log the request we're sending
            logger.info(f"Request ID: {test_id}")
            logger.info(f"Request method: {test_request['method']}")
            logger.debug(f"Full request: {json.dumps(test_request, indent=2)}")
            
            # Send the request
            logger.info(f"Sending request to {request_topic}")
            await client.publish(
                request_topic,
                payload=json.dumps(test_request).encode('utf-8'),
                qos=1,
                properties=properties
            )
            logger.info("Request sent successfully")
            
            # Wait for response
            try:
                logger.info("Waiting for response (timeout: 5s)...")
                await asyncio.wait_for(response_received.wait(), timeout=5.0)
                
                # Verify response
                logger.info(f"Response received: {json.dumps(received_message, indent=2)}")
                
                # Detailed verification
                if received_message.get("id") != test_id:
                    logger.error(f"Response ID mismatch: expected={test_id}, got={received_message.get('id')}")
                    return False
                
                if "result" not in received_message:
                    logger.error(f"No result field in response: {received_message}")
                    return False
                
                expected_message = f"Echo: {test_request['params']['message']}"
                actual_message = received_message.get("result", {}).get("message")
                
                if actual_message != expected_message:
                    logger.error(f"Message content mismatch: expected='{expected_message}', got='{actual_message}'")
                    return False
                
                logger.info("Response verified successfully!")
                return True
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for response after 5s")
                return False
        finally:
            # Clean up
            logger.info("Cleaning up listener task")
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                logger.debug("Listener task cancelled")
            except Exception as e:
                logger.exception(f"Error while cancelling listener task: {e}")

async def main():
    """Run the test."""
    logger.info(f"MQTT Direct Echo Test starting (PID: {os.getpid()})")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Testing with broker: {MQTT_BROKER}:{MQTT_PORT}")
    logger.info(f"Base topic: {BASE_TOPIC}")
    
    # Start responder in background
    logger.info("Starting responder...")
    responder_task = asyncio.create_task(responder())
    
    try:
        # Give responder time to start
        logger.info("Waiting for responder to initialize...")
        await asyncio.sleep(1.0)
        
        # Run requester
        logger.info("Starting requester...")
        start_time = time.time()
        success = await requester()
        elapsed = time.time() - start_time
        
        if success:
            logger.info(f"MQTT Request-Response test PASSED in {elapsed:.2f}s")
            return 0
        else:
            logger.error(f"MQTT Request-Response test FAILED after {elapsed:.2f}s")
            return 1
    except Exception as e:
        logger.exception(f"Unexpected error in test: {e}")
        return 2
    finally:
        # Clean up responder
        logger.info("Cleaning up responder...")
        responder_task.cancel()
        try:
            await responder_task
        except asyncio.CancelledError:
            logger.debug("Responder task cancelled")
        except Exception as e:
            logger.exception(f"Error while cancelling responder task: {e}")
        
        logger.info("Test cleanup complete")

if __name__ == "__main__":
    try:
        logger.info("Starting main test")
        exit_code = asyncio.run(main())
        logger.info(f"Test completed with exit code {exit_code}")
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unhandled exception in main: {e}")
        sys.exit(1) 