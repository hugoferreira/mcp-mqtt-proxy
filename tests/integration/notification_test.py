#!/usr/bin/env python
"""
Integration test for MCP notifications over MQTT.
Tests that notifications sent by the MCP server are properly published to MQTT.
"""

import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path

import aiomqtt
import pytest

from mcp_mqtt_proxy.config import MQTTListenerConfig, StdioServerParameters
from mcp_mqtt_proxy.mqtt_listener import run_mqtt_listener

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("notification_test")

# Path to the notification test script
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures"
NOTIFICATION_SCRIPT = FIXTURES_DIR / "notification_responder.py"

# MQTT test configuration
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
TEST_BASE_TOPIC = "mcp/test/notifications"
TEST_CLIENT_ID = f"mcp-test-{uuid.uuid4()}"
TEST_TIMEOUT = 15  # seconds

@pytest.mark.asyncio
@pytest.mark.timeout(30)  # Set a global timeout
async def test_mqtt_notifications():
    """Test that notifications from the MCP server are published to MQTT."""
    # Ensure notification script exists
    if not NOTIFICATION_SCRIPT.exists():
        # If it doesn't exist, we'll create it
        logger.warning(f"Notification script not found at {NOTIFICATION_SCRIPT}, creating it")
        await create_notification_script()
    
    # Make the script executable
    NOTIFICATION_SCRIPT.chmod(0o755)
    
    # Set up MQTT listener config
    config = MQTTListenerConfig(
        broker_url=f"mqtt://{MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}",
        base_topic=TEST_BASE_TOPIC,
        client_id=f"listener-{TEST_CLIENT_ID}",
        qos=1,
        mcp_timeout=TEST_TIMEOUT,  # Set MCP timeout
        stdio_mcp_process=StdioServerParameters(
            command=sys.executable,
            args=[str(NOTIFICATION_SCRIPT)],
            env=os.environ.copy()
        ),
        test_mode=True
    )
    
    # Start MQTT listener
    listener_task = asyncio.create_task(run_mqtt_listener(config))
    
    # Connect a test client to receive notifications
    received_notifications = []
    notification_received_event = asyncio.Event()
    
    async def notification_receiver():
        try:
            client_options = {
                "hostname": MQTT_BROKER_HOST,
                "port": MQTT_BROKER_PORT,
                "client_id": f"test-receiver-{TEST_CLIENT_ID}",
                "timeout": 5.0  # Set a timeout for MQTT operations
            }
            
            logger.info(f"Connecting to MQTT broker with options: {client_options}")
            async with aiomqtt.Client(**client_options) as client:
                # Subscribe to notifications topic
                notification_topic = f"{TEST_BASE_TOPIC}/notifications"
                logger.info(f"Test client subscribing to {notification_topic}")
                await asyncio.wait_for(
                    client.subscribe(notification_topic, qos=1),
                    timeout=5.0  # Set timeout for subscribe operation
                )
                
                # Wait for messages
                logger.info("Waiting for notifications...")
                async with client.messages() as messages:
                    # Use a timeout to ensure we don't wait forever
                    try:
                        async for message in messages:
                            if str(message.topic) == notification_topic:
                                try:
                                    payload = json.loads(message.payload.decode())
                                    logger.info(f"Received notification: {payload}")
                                    received_notifications.append(payload)
                                    notification_received_event.set()
                                    break  # Stop after receiving one notification
                                except json.JSONDecodeError:
                                    logger.error(f"Invalid JSON in notification: {message.payload.decode()}")
                    except asyncio.TimeoutError:
                        logger.warning("Timed out waiting for notifications")
        except Exception as e:
            logger.exception(f"Error in notification receiver: {e}")
    
    # Start notification receiver
    receiver_task = asyncio.create_task(notification_receiver())
    
    try:
        # Wait for a notification to be received (with timeout)
        try:
            await asyncio.wait_for(notification_received_event.wait(), timeout=10.0)
            logger.info(f"Received {len(received_notifications)} notifications")
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for notification")
            assert False, "Timed out waiting for notification"
            
        # Verify that at least one notification was received
        assert len(received_notifications) > 0, "No notifications received"
        
        # Verify notification structure
        notification = received_notifications[0]
        assert "jsonrpc" in notification, "Missing jsonrpc field in notification"
        assert "method" in notification, "Missing method field in notification"
        assert notification["jsonrpc"] == "2.0", "Incorrect jsonrpc version"
        
        # Depending on the notification type, verify specific fields
        if notification["method"] == "progress":
            assert "params" in notification, "Missing params field in notification"
            assert "progressToken" in notification["params"], "Missing progressToken in notification"
            assert "progress" in notification["params"], "Missing progress in notification"
        elif notification["method"] == "startup":
            # We're now checking for the startup notification as well
            assert "params" in notification, "Missing params field in notification"
            assert "timestamp" in notification["params"], "Missing timestamp in startup notification"
            assert "pid" in notification["params"], "Missing pid in startup notification"
        
        logger.info("Notification test passed!")
    finally:
        # Clean up tasks
        logger.info("Cancelling tasks...")
        listener_task.cancel()
        receiver_task.cancel()
        try:
            # Wait with timeout for tasks to cancel
            await asyncio.wait(
                [listener_task, receiver_task], 
                timeout=5.0,
                return_when=asyncio.ALL_COMPLETED
            )
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for tasks to cancel")
        except asyncio.CancelledError:
            pass
        logger.info("Tasks cancelled")


async def create_notification_script():
    """Create a notification test script that sends notifications."""
    logger.info(f"Creating notification script at {NOTIFICATION_SCRIPT}")
    
    script_content = '''#!/usr/bin/env python
"""
Simple MCP responder that sends progress notifications.
This script implements a basic MCP server that responds to requests
and sends progress notifications.
"""

import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("notification_responder")

class NotificationResponder:
    """Simple MCP responder that sends notifications."""
    
    def __init__(self):
        """Initialize the responder."""
        self.server_info = {
            "name": "notification_responder",
            "version": "1.0.0",
            "capabilities": {
                "notifications": True
            }
        }
        self.request_handlers = {
            "initialize": self.handle_initialize,
            "test/longRunning": self.handle_long_running,
            "test/echo": self.handle_echo
        }
    
    async def handle_initialize(self, request):
        """Handle initialize request."""
        logger.info("Handling initialize request")
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {
                "serverInfo": self.server_info
            }
        }
    
    async def handle_echo(self, request):
        """Echo back the message."""
        message = request.get("params", {}).get("message", "")
        logger.info(f"Handling echo request with message: {message}")
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {
                "message": f"Echo: {message}"
            }
        }
    
    async def handle_long_running(self, request):
        """Handle a long-running request with progress notifications."""
        logger.info("Handling long-running request")
        
        # Extract parameters
        progress_token = request.get("params", {}).get("progressToken", str(uuid.uuid4()))
        steps = request.get("params", {}).get("steps", 5)
        delay = request.get("params", {}).get("delay", 0.5)
        
        # Simulate work with progress notifications
        for step in range(steps):
            # Send progress notification
            progress_notification = {
                "jsonrpc": "2.0",
                "method": "progress",
                "params": {
                    "progressToken": progress_token,
                    "progress": step,
                    "total": steps,
                    "message": f"Step {step+1}/{steps}"
                }
            }
            print(json.dumps(progress_notification))
            sys.stdout.flush()  # Important: ensure notification is sent immediately
            logger.info(f"Sent progress notification: {step+1}/{steps}")
            
            # Simulate work
            await asyncio.sleep(delay)
        
        # Return final result
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {
                "message": "Long-running task completed",
                "steps": steps,
                "completedAt": datetime.now().isoformat()
            }
        }
    
    async def run(self):
        """Run the responder, reading from stdin and writing to stdout."""
        logger.info(f"Starting notification responder (PID: {os.getpid()})")
        logger.info(f"Python executable: {sys.executable}")
        logger.info(f"Arguments: {sys.argv}")
        logger.info(f"Environment: PYTHONPATH={os.environ.get('PYTHONPATH', '')}")
        
        # Send an initial notification to test the notification system
        initial_notification = {
            "jsonrpc": "2.0",
            "method": "startup",
            "params": {
                "timestamp": datetime.now().isoformat(),
                "pid": os.getpid()
            }
        }
        print(json.dumps(initial_notification))
        sys.stdout.flush()  # Important: ensure notification is sent immediately
        logger.info("Sent startup notification")
        
        # Process stdin/stdout
        request_count = 0
        
        while True:
            try:
                # Read request from stdin
                request_count += 1
                logger.debug(f"[REQ-{request_count}] Waiting for next request line...")
                line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
                
                if not line:
                    logger.info("End of input stream, exiting")
                    break
                
                line = line.strip()
                logger.debug(f"[REQ-{request_count}] Raw request length: {len(line)} bytes")
                logger.info(f"[REQ-{request_count}] Received raw request: {line}")
                
                # Parse request
                try:
                    request = json.loads(line)
                    request_id = request.get("id", f"unknown-{request_count}")
                    method = request.get("method", "unknown")
                    
                    logger.info(f"[{request_id}] Processing request...")
                    logger.debug(f"[{request_id}] REQUEST START ===================================")
                    logger.debug(f"[{request_id}] Request type: {type(request)}")
                    logger.debug(f"[{request_id}] Request content: {json.dumps(request, indent=2)}")
                    logger.debug(f"[{request_id}] Environment: PYTHONPATH={os.environ.get('PYTHONPATH', '')}")
                    
                    start_time = asyncio.get_event_loop().time()
                    logger.info(f"[{request_id}] Processing method: {method}")
                    
                    # Process request
                    if method in self.request_handlers:
                        logger.info(f"[{request_id}] Handling {method} method")
                        handler = self.request_handlers[method]
                        response = await handler(request)
                    else:
                        logger.warning(f"[{request_id}] Unknown method: {method}")
                        response = {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {
                                "code": -32601,
                                "message": f"Method not found: {method}"
                            }
                        }
                    
                    # Send response
                    processing_time = asyncio.get_event_loop().time() - start_time
                    logger.info(f"[{request_id}] Request processed in {processing_time:.6f}s")
                    logger.debug(f"[{request_id}] Response: {json.dumps(response, indent=2)}")
                    logger.debug(f"[{request_id}] REQUEST END ===================================")
                    
                    # Write response to stdout
                    response_json = json.dumps(response)
                    logger.info(f"[{request_id}] Sending response: {response_json}")
                    print(response_json)
                    sys.stdout.flush()  # Important: ensure response is sent immediately
                    logger.info(f"[{request_id}] Response sent successfully")
                    logger.info(f"[{request_id}] Total request handling time: {asyncio.get_event_loop().time() - start_time:.6f}s")
                    
                    # Send a post-response notification
                    post_notification = {
                        "jsonrpc": "2.0",
                        "method": "requestCompleted",
                        "params": {
                            "requestId": request_id,
                            "method": method,
                            "processingTime": processing_time,
                            "timestamp": datetime.now().isoformat()
                        }
                    }
                    print(json.dumps(post_notification))
                    sys.stdout.flush()  # Important: ensure notification is sent immediately
                    logger.info(f"[{request_id}] Sent request completion notification")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32700,
                            "message": f"Parse error: {str(e)}"
                        }
                    }
                    print(json.dumps(error_response))
                    sys.stdout.flush()
                
                except Exception as e:
                    logger.exception(f"Error processing request: {e}")
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": request.get("id") if isinstance(request, dict) else None,
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}"
                        }
                    }
                    print(json.dumps(error_response))
                    sys.stdout.flush()
            
            except Exception as e:
                logger.exception(f"Unhandled exception: {e}")
                break
        
        logger.info("Notification responder shutting down")

async def main():
    """Run the notification responder."""
    responder = NotificationResponder()
    await responder.run()

if __name__ == "__main__":
    asyncio.run(main())
'''
    
    # Create the fixtures directory if it doesn't exist
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Write the script
    with open(NOTIFICATION_SCRIPT, "w") as f:
        f.write(script_content)
    
    # Make it executable
    NOTIFICATION_SCRIPT.chmod(0o755)
    logger.info(f"Created notification script at {NOTIFICATION_SCRIPT}")


if __name__ == "__main__":
    asyncio.run(test_mqtt_notifications()) 