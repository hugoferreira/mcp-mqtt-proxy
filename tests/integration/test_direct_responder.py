"""
Integration test for directly communicating with the simple_responder.py script.
This bypasses MQTT and directly sends/receives MCP messages over stdio.
"""

import asyncio
import json
import logging
import pytest

logger = logging.getLogger(__name__)

# Test timeouts
TEST_TIMEOUT = 10  # seconds for the whole test
OPERATION_TIMEOUT = 5.0  # seconds for individual operations

@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_simple_responder(simple_responder_process):
    """Test sending a request to the responder and receiving a response."""
    # Create a test request
    test_request = {
        "jsonrpc": "2.0",
        "id": "test-request-1",
        "method": "test/echo",
        "params": {
            "message": "Hello from direct test"
        }
    }
    
    # Send the request
    request_json = json.dumps(test_request) + "\n"
    logger.info(f"Sending request: {request_json.strip()}")
    simple_responder_process.stdin.write(request_json.encode('utf-8'))
    await asyncio.wait_for(simple_responder_process.stdin.drain(), timeout=OPERATION_TIMEOUT)
    logger.info("Request sent, waiting for response")
    
    # Read response with timeout
    response_line = await asyncio.wait_for(simple_responder_process.stdout.readline(), timeout=OPERATION_TIMEOUT)
    
    assert response_line, "No response received"
    
    # Parse and log the response
    response_str = response_line.decode('utf-8').strip()
    logger.info(f"Response received: {response_str}")
    
    response_data = json.loads(response_str)
    logger.info(f"Parsed response: {response_data}")
    
    # Verify the response
    assert response_data.get("id") == test_request["id"], "Response ID doesn't match"
    assert "result" in response_data, "No result in response"
    assert response_data.get("result", {}).get("message") == f"Echo: {test_request['params']['message']}", "Unexpected message"
    
    logger.info("Response validation successful")

@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_simple_responder_initialize(simple_responder_process):
    """Test sending an initialize request and verifying the server info response."""
    # Create an initialize request
    init_request = {
        "jsonrpc": "2.0",
        "id": "test-init-1",
        "method": "initialize",
        "params": {}
    }
    
    # Send the request
    request_json = json.dumps(init_request) + "\n"
    logger.info(f"Sending initialize request: {request_json.strip()}")
    simple_responder_process.stdin.write(request_json.encode('utf-8'))
    await asyncio.wait_for(simple_responder_process.stdin.drain(), timeout=OPERATION_TIMEOUT)
    logger.info("Initialize request sent, waiting for response")
    
    # Read response with timeout
    response_line = await asyncio.wait_for(simple_responder_process.stdout.readline(), timeout=OPERATION_TIMEOUT)
    
    assert response_line, "No response received"
    
    # Parse and log the response
    response_str = response_line.decode('utf-8').strip()
    logger.info(f"Response received: {response_str}")
    
    response_data = json.loads(response_str)
    logger.info(f"Parsed response: {response_data}")
    
    # Verify the response
    assert response_data.get("id") == init_request["id"], "Response ID doesn't match"
    assert "result" in response_data, "No result in response"
    assert "serverInfo" in response_data.get("result", {}), "No serverInfo in response"
    
    server_info = response_data.get("result", {}).get("serverInfo", {})
    assert "name" in server_info, "No name in serverInfo"
    assert "version" in server_info, "No version in serverInfo"
    
    logger.info(f"Server info: {server_info}")
    logger.info("Initialize response validation successful") 