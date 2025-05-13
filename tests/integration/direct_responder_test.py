#!/usr/bin/env python
"""
Direct test of simple_responder.py script to verify it's functioning correctly.
This script bypasses MQTT and directly communicates with the responder to isolate issues.
"""

import asyncio
import json
import logging
import sys
import pytest
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

@pytest.mark.asyncio
@pytest.mark.timeout(10)  # Set a timeout for this test
async def test_responder():
    """Test sending a request to the responder script and receiving a response."""
    
    # Ensure the responder script exists
    if not RESPONDER_SCRIPT.exists():
        logger.error(f"Responder script not found at {RESPONDER_SCRIPT}")
        assert False, f"Responder script not found at {RESPONDER_SCRIPT}"
    
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
    
    success = False
    
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
                assert False, "Empty response received"
                
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
                success = True
            except json.JSONDecodeError:
                logger.error(f"Response is not valid JSON: {response_str}")
                assert False, f"Response is not valid JSON: {response_str}"
            except AssertionError as e:
                logger.error(f"Response validation failed: {e}")
                raise
                
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for response")
            assert False, "Timeout waiting for response"
            
    finally:
        # Try to read stderr, but do so carefully
        try:
            # Create a task to read stderr and race it against a sleep
            stderr_task = asyncio.create_task(process.stderr.read())
            await asyncio.sleep(0.5)  # Give it a short time to complete
            
            if stderr_task.done():
                stderr_data = stderr_task.result()
                if stderr_data:
                    logger.info(f"Process stderr:\n{stderr_data.decode('utf-8')}")
            else:
                # If it's not done, cancel it - we don't want to wait forever
                stderr_task.cancel()
                logger.info("Skipped reading full stderr - operation would block")
        except Exception as e:
            logger.warning(f"Error reading stderr: {e}")
            
        # Terminate the process
        logger.info("Terminating process")
        if process.returncode is None:
            try:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.warning("Process did not terminate gracefully, killing")
                    process.kill()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        logger.error("Failed to kill process")
            except Exception as e:
                logger.warning(f"Error terminating process: {e}")
        
        assert success, "Test failed"

async def main():
    """Main function to run the test."""
    await test_responder()
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 