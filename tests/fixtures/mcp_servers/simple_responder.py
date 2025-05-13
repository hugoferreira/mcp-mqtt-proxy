#!/usr/bin/env python
"""Simple MCP responder script for testing the MQTT proxy chain."""

import asyncio
import json
import logging
import sys
from typing import Dict, Any, Optional
import traceback
import binascii
import os
import time

# Configure more detailed logging
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(thread)d - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("simple_responder")

# Enable TRACE level (more detailed than DEBUG)
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")

def trace(self, message, *args, **kwargs):
    """Add trace method to logger."""
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kwargs)

logging.Logger.trace = trace

# Keep track of received requests for testing
received_requests = []

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

async def process_request(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single MCP request and return a response."""
    start_time = time.time()
    request_id = request_data.get("id", "unknown")
    
    try:
        # Log and store the request with more detail
        logger.debug(f"[{request_id}] REQUEST START ===================================")
        logger.debug(f"[{request_id}] Request type: {type(request_data)}")
        logger.debug(f"[{request_id}] Request content: {json.dumps(request_data, indent=2)}")
        logger.debug(f"[{request_id}] Environment: PYTHONPATH={os.environ.get('PYTHONPATH', 'Not set')}")
        received_requests.append(request_data)
        
        # Basic validation
        if not all(k in request_data for k in ["jsonrpc", "id", "method"]):
            logger.error(f"[{request_id}] Invalid request format - missing required fields")
            for k in ["jsonrpc", "id", "method"]:
                logger.error(f"[{request_id}] Field '{k}' present: {k in request_data}")
            
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
        
        logger.info(f"[{request_id}] Processing method: {method}")
        logger.debug(f"[{request_id}] Request parameters: {json.dumps(params, indent=2)}")
        
        # Special handling for prompts/list method
        if method == "prompts/list":
            logger.info(f"[{request_id}] Handling prompts/list method")
            response = {
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
            logger.info(f"[{request_id}] Handling test/echo method with message: {message}")
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"message": f"Echo: {message}"}
            }
        elif method == "test/error":
            # Return an error response
            logger.info(f"[{request_id}] Handling test/error method")
            response = {
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
            logger.info(f"[{request_id}] Handling test/delay method with delay: {delay}s")
            await asyncio.sleep(delay)
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"message": f"Delayed response after {delay}s"}
            }
        elif method == "initialize":
            # Handle initialize method
            logger.info(f"[{request_id}] Handling initialize method")
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "serverInfo": {
                        "name": "simple_responder",
                        "version": "1.0.0",
                        "capabilities": {}
                    }
                }
            }
        elif method == "shutdown":
            # Handle shutdown method
            logger.info(f"[{request_id}] Handling shutdown method")
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": None
            }
        else:
            # Unknown method
            logger.warning(f"[{request_id}] Unknown method requested: {method}")
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}",
                }
            }
            
        elapsed = time.time() - start_time
        logger.info(f"[{request_id}] Request processed in {elapsed:.6f}s")
        logger.debug(f"[{request_id}] Response: {json.dumps(response, indent=2)}")
        logger.debug(f"[{request_id}] REQUEST END ===================================")
        return response
    except Exception as e:
        elapsed = time.time() - start_time
        logger.exception(f"[{request_id}] Error processing request after {elapsed:.6f}s: {e}")
        return {
            "jsonrpc": "2.0",
            "id": request_data.get("id"),
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}",
                "data": {"traceback": traceback.format_exc()}
            }
        }

async def run_server():
    """Run the MCP server using stdio."""
    logger.info(f"Starting simple MCP responder (PID: {os.getpid()})")
    logger.info(f"Python executable: {sys.executable}")
    logger.info(f"Arguments: {sys.argv}")
    logger.info(f"Environment: PYTHONPATH={os.environ.get('PYTHONPATH', 'Not set')}")
    
    # Set up stdin/stdout
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    loop = asyncio.get_running_loop()
    
    # Connect to stdin
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    logger.info("Connected to stdin")
    
    # Connect to stdout
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    logger.info("Connected to stdout")
    
    logger.info("Server initialized, waiting for requests")
    
    try:
        req_counter = 0
        while True:
            # Read request line
            req_counter += 1
            logger.debug(f"[REQ-{req_counter}] Waiting for next request line...")
            
            # Log periodically even when waiting
            if req_counter % 10 == 0:
                logger.info(f"Still alive, processed {req_counter-1} requests so far")
            
            try:
                request_line = await asyncio.wait_for(reader.readline(), timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for input, but continuing to wait...")
                continue
                
            if not request_line:
                logger.info("Received EOF, exiting")
                break
                
            try:
                # Parse request with detailed logging
                request_time = time.time()
                logger.debug(f"[REQ-{req_counter}] Raw request length: {len(request_line)} bytes")
                logger.trace(f"[REQ-{req_counter}] Raw request hex:\n{hex_dump(request_line, '  ')}")
                
                request_str = request_line.decode('utf-8').strip()
                if not request_str:
                    logger.debug(f"[REQ-{req_counter}] Empty line received, continuing")
                    continue
                    
                logger.info(f"[REQ-{req_counter}] Received raw request: {request_str}")
                
                # Try to parse JSON
                try:
                    request_data = json.loads(request_str)
                    req_id = request_data.get("id", f"unknown-{req_counter}")
                except Exception as e:
                    logger.error(f"[REQ-{req_counter}] JSON parse error: {e}")
                    logger.error(f"[REQ-{req_counter}] Problematic JSON: {request_str}")
                    raise
                
                # Process request
                logger.info(f"[{req_id}] Processing request...")
                response_data = await process_request(request_data)
                
                # Send response
                response_str = json.dumps(response_data) + "\n"
                logger.info(f"[{req_id}] Sending response: {response_str.strip()}")
                logger.trace(f"[{req_id}] Response hex:\n{hex_dump(response_str, '  ')}")
                
                # Write response and flush
                writer.write(response_str.encode('utf-8'))
                
                try:
                    start_drain = time.time()
                    await asyncio.wait_for(writer.drain(), timeout=5.0)
                    drain_time = time.time() - start_drain
                    logger.info(f"[{req_id}] Response sent successfully (drain took {drain_time:.6f}s)")
                except asyncio.TimeoutError:
                    logger.error(f"[{req_id}] Timeout draining writer after 5s")
                
                # Log total processing time
                total_time = time.time() - request_time
                logger.info(f"[{req_id}] Total request handling time: {total_time:.6f}s")
                
            except json.JSONDecodeError as e:
                logger.exception(f"[REQ-{req_counter}] Failed to parse request JSON: {e}")
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": "Parse error",
                        "data": {"error": str(e)}
                    }
                }
                writer.write((json.dumps(error_response) + "\n").encode('utf-8'))
                await writer.drain()
            except Exception as e:
                logger.exception(f"[REQ-{req_counter}] Error processing request: {e}")
                try:
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}",
                            "data": {"traceback": traceback.format_exc()}
                        }
                    }
                    writer.write((json.dumps(error_response) + "\n").encode('utf-8'))
                    await writer.drain()
                except Exception as inner_e:
                    logger.exception(f"[REQ-{req_counter}] Error sending error response: {inner_e}")
    except asyncio.CancelledError:
        logger.info("Server task cancelled")
    except Exception as e:
        logger.exception(f"Unexpected server error: {e}")
    finally:
        logger.info("Server shutting down")
        # Close the writer
        writer.close()
        try:
            await writer.wait_closed()
            logger.info("Writer closed")
        except Exception as e:
            logger.error(f"Error closing writer: {e}")

if __name__ == "__main__":
    logger.info(f"Simple MCP responder starting (PID: {os.getpid()})")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current directory: {os.getcwd()}")
    
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info("Server interrupted")
    except Exception as e:
        logger.exception(f"Server error: {e}")
    finally:
        logger.info("Server exited")
