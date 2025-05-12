#!/usr/bin/env python
"""
Direct test of simple_responder.py script to verify it's functioning correctly.
This script bypasses MQTT and directly communicates with the responder to isolate issues.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("direct_responder_test")

# Path to the simple_responder.py script
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "simple_responder.py"

async def test_responder():
    """Test sending a request to the responder script and receiving a response."""
    
    # Ensure the responder script exists
    if not RESPONDER_SCRIPT.exists():
        logger.error(f"Responder script not found at {RESPONDER_SCRIPT}")
        return False
    
    # Start the responder process
    logger.info(f"Starting responder process: {sys.executable} {RESPONDER_SCRIPT}")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(RESPONDER_SCRIPT),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    logger.info("Process started, waiting a moment for initialization")
    await asyncio.sleep(0.5)
    
    # Create a test request
    test_request = {
        "jsonrpc": "2.0",
        "id": "test-request-1",
        "method": "test/echo",
        "params": {
            "message": "Hello from direct test"
        }
    }
    
    try:
        # Send the request
        request_json = json.dumps(test_request) + "\n"
        logger.info(f"Sending request: {request_json.strip()}")
        process.stdin.write(request_json.encode('utf-8'))
        await process.stdin.drain()
        logger.info("Request sent, waiting for response")
        
        # Read response with timeout
        try:
            response_line = await asyncio.wait_for(process.stdout.readline(), timeout=5.0)
            if not response_line:
                logger.error("Empty response received")
                return False
                
            # Parse and log the response
            response_str = response_line.decode('utf-8').strip()
            logger.info(f"Response received: {response_str}")
            
            try:
                response_data = json.loads(response_str)
                logger.info(f"Parsed response: {response_data}")
                
                # Verify the response
                assert response_data.get("id") == test_request["id"], "Response ID doesn't match"
                assert "result" in response_data, "No result in response"
                assert response_data.get("result", {}).get("message") == f"Echo: {test_request['params']['message']}", "Unexpected message"
                
                logger.info("Response validation successful")
                return True
            except json.JSONDecodeError:
                logger.error(f"Response is not valid JSON: {response_str}")
                return False
            except AssertionError as e:
                logger.error(f"Response validation failed: {e}")
                return False
                
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for response")
            return False
            
    finally:
        # Capture any stderr output for debugging
        stderr_data = await process.stderr.read()
        if stderr_data:
            logger.info(f"Process stderr:\n{stderr_data.decode('utf-8')}")
            
        # Terminate the process
        logger.info("Terminating process")
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("Process did not terminate gracefully, killing")
                process.kill()
                await process.wait()

async def main():
    """Main function to run the test."""
    success = await test_responder()
    logger.info(f"Test result: {'SUCCESS' if success else 'FAILURE'}")
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 