#!/usr/bin/env python
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
                    sys.stdout.flush()
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
