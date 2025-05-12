#!/usr/bin/env python
"""Simple MCP responder script for testing the MQTT proxy chain."""

import asyncio
import json
import logging
import sys
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.DEBUG, 
                   format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                   stream=sys.stderr)
logger = logging.getLogger("simple_responder")

# Keep track of received requests for testing
received_requests = []

async def process_request(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single MCP request and return a response."""
    # Log and store the request
    logger.info(f"Received request: {request_data}")
    received_requests.append(request_data)
    
    # Basic validation
    if not all(k in request_data for k in ["jsonrpc", "id", "method"]):
        logger.error("Invalid request format")
        return {
            "jsonrpc": "2.0",
            "id": request_data.get("id"),
            "error": {
                "code": -32600,
                "message": "Invalid Request",
            }
        }
    
    # Process based on method
    method = request_data["method"]
    request_id = request_data["id"]
    params = request_data.get("params", {})
    
    # Special handling for prompts/list method
    if method == "prompts/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "prompts": [
                    {"name": "simple_prompt", "description": "A simple test prompt"}
                ],
                "content": [
                    {"type": "text", "text": "Dummy response for prompts/list"}
                ]
            }
        }
    elif method == "test/echo":
        # Echo the message back
        message = params.get("message", "")
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"message": f"Echo: {message}"}
        }
    elif method == "test/error":
        # Return an error response
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": 1001,
                "message": "Test error message",
                "data": {"details": "This is a test error"}
            }
        }
    elif method == "test/delay":
        # Simulate a delayed response
        delay = params.get("delay", 1.0)
        logger.info(f"Delaying response for {delay} seconds")
        await asyncio.sleep(delay)
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"message": f"Delayed response after {delay}s"}
        }
    else:
        # Unknown method
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}",
            }
        }

async def run_server():
    """Run the MCP server using stdio."""
    logger.info("Starting simple MCP responder")
    
    # Set up stdin/stdout
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    loop = asyncio.get_running_loop()
    
    # Connect to stdin
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    
    # Connect to stdout
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    
    logger.info("Server initialized, waiting for requests")
    
    try:
        while True:
            # Read request line
            request_line = await reader.readline()
            if not request_line:
                logger.info("Received EOF, exiting")
                break
                
            try:
                # Parse request
                request_str = request_line.decode('utf-8').strip()
                if not request_str:
                    continue
                    
                request_data = json.loads(request_str)
                
                # Process request
                response_data = await process_request(request_data)
                
                # Send response
                response_str = json.dumps(response_data) + "\n"
                writer.write(response_str.encode('utf-8'))
                await writer.drain()
                logger.info(f"Sent response: {response_data}")
                
            except json.JSONDecodeError:
                logger.exception("Failed to parse request JSON")
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": "Parse error",
                    }
                }
                writer.write((json.dumps(error_response) + "\n").encode('utf-8'))
                await writer.drain()
            except Exception as e:
                logger.exception(f"Error processing request: {e}")
    except asyncio.CancelledError:
        logger.info("Server task cancelled")
    finally:
        logger.info("Server shutting down")
        # Close the writer
        writer.close()

if __name__ == "__main__":
    logger.info("Simple MCP responder starting")
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info("Server interrupted")
    except Exception as e:
        logger.exception(f"Server error: {e}")
    finally:
        logger.info("Server exited")
