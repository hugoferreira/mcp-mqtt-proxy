#!/usr/bin/env python
"""
MCP server script that sends periodic status notifications.
Also responds to normal MCP commands for testing combined functionality.
"""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("notification_responder")


async def process_request(request):
    """Process an incoming JSON-RPC request and return a response."""
    try:
        # Parse the request
        request_data = json.loads(request)
        logger.debug(f"Received request: {request_data}")
        
        # Check for a valid JSON-RPC request
        if "jsonrpc" not in request_data or request_data["jsonrpc"] != "2.0":
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_data.get("id", None),
                "error": {
                    "code": -32600,
                    "message": "Invalid JSON-RPC request"
                }
            })
        
        # Check if we have a method
        if "method" not in request_data:
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_data.get("id", None),
                "error": {
                    "code": -32600,
                    "message": "Method missing"
                }
            })
        
        # Process based on method
        method = request_data["method"]
        request_id = request_data.get("id", None)
        params = request_data.get("params", {})
        
        # Initialize request
        if method == "initialize":
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "serverInfo": {
                        "name": "notification-responder",
                        "version": "1.0.0",
                        "capabilities": ["status/notifications"]
                    }
                }
            })
        
        # Echo request
        elif method == "test/echo":
            message = params.get("message", "No message provided")
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "message": f"Echo: {message}"
                }
            })
        
        # Shutdown request
        elif method == "shutdown":
            logger.info("Received shutdown request")
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "shutdown": True
                }
            })
        
        # Trigger immediate notification request
        elif method == "test/trigger_notification":
            # We'll send the notification separately
            # Here we just acknowledge the trigger request
            trigger_type = params.get("type", "status")
            logger.info(f"Received trigger notification request: {trigger_type}")
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "notification_queued": True,
                    "type": trigger_type
                }
            })
        
        # Unknown method
        else:
            return json.dumps({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            })
    
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        return json.dumps({
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32700,
                "message": f"Parse error: {str(e)}"
            }
        })
    except Exception as e:
        logger.exception(f"Error processing request: {e}")
        return json.dumps({
            "jsonrpc": "2.0",
            "id": request_data.get("id", None) if 'request_data' in locals() else None,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        })


async def send_notification(notification_type, data=None):
    """Send a notification to stdout."""
    # Create timestamp
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Create notification data based on type
    if notification_type == "status":
        notification_data = {
            "jsonrpc": "2.0",
            "method": "status/update",
            "params": {
                "status": "running",
                "timestamp": timestamp,
                "uptime": time.time() - start_time,
                "notifications_sent": notification_count,
                **(data or {})
            }
        }
    elif notification_type == "log":
        notification_data = {
            "jsonrpc": "2.0",
            "method": "log/message",
            "params": {
                "level": "info",
                "message": data.get("message", "Log notification"),
                "timestamp": timestamp
            }
        }
    else:
        notification_data = {
            "jsonrpc": "2.0",
            "method": f"notification/{notification_type}",
            "params": {
                "timestamp": timestamp,
                **(data or {})
            }
        }
    
    # Send the notification to stdout
    notification_text = json.dumps(notification_data)
    print(notification_text, flush=True)
    logger.debug(f"Sent notification: {notification_text}")


async def periodic_notifications():
    """Task to periodically send notifications."""
    global notification_count
    try:
        while True:
            # Send status notification
            await send_notification("status")
            notification_count += 1
            
            # Wait before sending another notification
            await asyncio.sleep(3.0)  # Send every 3 seconds
    except asyncio.CancelledError:
        logger.info("Periodic notification task cancelled")
        raise


async def process_stdin():
    """Process stdin for JSON-RPC requests."""
    logger.info("Starting stdin processor")
    should_exit = False
    
    try:
        while not should_exit:
            # Read a line from stdin
            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            if not line:
                # EOF - exit
                logger.info("Received EOF on stdin, exiting")
                break
            
            # Process the request
            response = await process_request(line.strip())
            
            # Check if it's a shutdown request
            if '"method":"shutdown"' in line:
                should_exit = True
            
            # Send the response
            if response:
                print(response, flush=True)
    
    except asyncio.CancelledError:
        logger.info("Stdin processor task cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in stdin processor: {e}")


async def main():
    """Main entry point for the notification responder."""
    logger.info("Starting notification responder")
    
    # Create a task for periodic notifications
    notif_task = asyncio.create_task(periodic_notifications())
    
    try:
        # Process stdin (this will run until EOF or shutdown)
        await process_stdin()
    finally:
        # Clean up notification task
        if not notif_task.done():
            notif_task.cancel()
            try:
                await notif_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Notification responder shutting down")


if __name__ == "__main__":
    # Global variables
    start_time = time.time()
    notification_count = 0
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, exiting")
        sys.exit(0) 