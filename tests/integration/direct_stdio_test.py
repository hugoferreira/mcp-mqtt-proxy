#!/usr/bin/env python
"""
Direct stdio test script.
This script directly tests basic stdio communication with the simple_responder.py script
without using anyio bridges or the MCP client library.
"""

import asyncio
import json
import logging
import sys
import os
import time
from pathlib import Path
import uuid

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(thread)d - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("direct_stdio_test")

# Path to the simple_responder.py script
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures"
RESPONDER_SCRIPT = FIXTURES_DIR / "simple_responder.py"

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

async def run_test():
    """Run direct stdio test with the responder script."""
    logger.info(f"Starting direct stdio test (PID: {os.getpid()})")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current directory: {os.getcwd()}")
    
    # Ensure the responder script exists and is executable
    if not RESPONDER_SCRIPT.exists():
        logger.error(f"Responder script not found at {RESPONDER_SCRIPT}")
        return 1
    
    # Make it executable
    RESPONDER_SCRIPT.chmod(0o755)
    logger.info(f"Using responder script: {RESPONDER_SCRIPT}")
    
    # Configure environment with PYTHONPATH
    env = os.environ.copy()
    if 'PYTHONPATH' not in env:
        root_dir = Path(__file__).parent.parent.parent
        src_dir = root_dir / "src"
        env['PYTHONPATH'] = str(src_dir)
        logger.info(f"Setting PYTHONPATH to {env['PYTHONPATH']}")
    
    env['DEBUG'] = '1'  # Enable more verbose debug output
    
    # Start the process with pipes for stdin, stdout, stderr
    logger.info(f"Starting responder process...")
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(RESPONDER_SCRIPT),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )
    logger.info(f"Responder process started with PID: {proc.pid}")
    
    # Set up stderr reader task
    stderr_queue = asyncio.Queue()
    
    async def stderr_reader():
        """Read stderr messages from the process and put them in a queue."""
        if not proc.stderr:
            return
        
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    logger.info("Stderr EOF reached")
                    break
                    
                stderr_line = line.decode('utf-8', errors='replace').rstrip()
                await stderr_queue.put(stderr_line)
        except Exception as e:
            logger.exception(f"Error in stderr reader: {e}")
    
    # Start stderr reader task
    stderr_task = asyncio.create_task(stderr_reader())
    
    # Start a task to log stderr messages
    async def log_stderr():
        """Log stderr messages from the queue."""
        try:
            while True:
                try:
                    line = await asyncio.wait_for(stderr_queue.get(), timeout=0.1)
                    logger.info(f"[STDERR] {line}")
                    stderr_queue.task_done()
                except asyncio.TimeoutError:
                    # No message, check if we should exit
                    if stderr_task.done():
                        if stderr_queue.empty():
                            break
                    await asyncio.sleep(0.1)
        except Exception as e:
            logger.exception(f"Error in stderr logger: {e}")
    
    # Start the stderr logger
    logger_task = asyncio.create_task(log_stderr())
    
    try:
        # Wait a moment for the process to initialize
        await asyncio.sleep(1)
        
        # Create a test "initialize" request
        init_request = {
            "jsonrpc": "2.0",
            "id": f"init-{uuid.uuid4()}",
            "method": "initialize",
            "params": {}
        }
        
        # Convert to JSON and send to stdin
        init_json = json.dumps(init_request) + "\n"
        logger.info(f"Sending initialize request: {init_json.strip()}")
        if proc.stdin:
            # Send the request
            init_bytes = init_json.encode('utf-8')
            proc.stdin.write(init_bytes)
            await proc.stdin.drain()
            logger.info(f"Initialize request sent ({len(init_bytes)} bytes)")
        else:
            logger.error("Process stdin is not available")
            return 1
        
        # Wait for the response
        logger.info("Waiting for initialize response...")
        if proc.stdout:
            try:
                init_response_line = await asyncio.wait_for(proc.stdout.readline(), timeout=5.0)
                if not init_response_line:
                    logger.error("Received EOF while waiting for initialize response")
                    return 1
                    
                init_response_str = init_response_line.decode('utf-8').strip()
                logger.info(f"Received initialize response: {init_response_str}")
                
                # Parse the response
                try:
                    init_response = json.loads(init_response_str)
                    if init_response.get('id') == init_request['id']:
                        logger.info("Initialize request successful")
                    else:
                        logger.warning(f"Response ID mismatch: expected={init_request['id']}, got={init_response.get('id')}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse initialize response: {e}")
                    return 1
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for initialize response")
                return 1
        else:
            logger.error("Process stdout is not available")
            return 1
        
        # Create a test "echo" request
        echo_request = {
            "jsonrpc": "2.0",
            "id": f"echo-{uuid.uuid4()}",
            "method": "test/echo",
            "params": {
                "message": "Hello from direct stdio test"
            }
        }
        
        # Convert to JSON and send to stdin
        echo_json = json.dumps(echo_request) + "\n"
        logger.info(f"Sending echo request: {echo_json.strip()}")
        if proc.stdin:
            # Send the request
            echo_bytes = echo_json.encode('utf-8')
            proc.stdin.write(echo_bytes)
            await proc.stdin.drain()
            logger.info(f"Echo request sent ({len(echo_bytes)} bytes)")
        else:
            logger.error("Process stdin is not available")
            return 1
        
        # Wait for the response
        logger.info("Waiting for echo response...")
        if proc.stdout:
            try:
                echo_response_line = await asyncio.wait_for(proc.stdout.readline(), timeout=5.0)
                if not echo_response_line:
                    logger.error("Received EOF while waiting for echo response")
                    return 1
                    
                echo_response_str = echo_response_line.decode('utf-8').strip()
                logger.info(f"Received echo response: {echo_response_str}")
                
                # Parse the response
                try:
                    echo_response = json.loads(echo_response_str)
                    if echo_response.get('id') == echo_request['id']:
                        result = echo_response.get('result', {})
                        expected_message = f"Echo: {echo_request['params']['message']}"
                        if result.get('message') == expected_message:
                            logger.info("Echo test successful!")
                        else:
                            logger.error(f"Unexpected echo response content: {result}")
                            return 1
                    else:
                        logger.warning(f"Response ID mismatch: expected={echo_request['id']}, got={echo_response.get('id')}")
                        return 1
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse echo response: {e}")
                    return 1
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for echo response")
                return 1
        else:
            logger.error("Process stdout is not available")
            return 1
        
        logger.info("All tests passed successfully!")
        return 0
    except Exception as e:
        logger.exception(f"Error during test: {e}")
        return 1
    finally:
        # Clean up
        logger.info("Cleaning up resources")
        
        # Close stdin if it's still open
        if proc.stdin and not proc.stdin.is_closing():
            logger.info("Closing stdin")
            proc.stdin.close()
            try:
                await proc.stdin.wait_closed()
                logger.info("Stdin closed successfully")
            except Exception as e:
                logger.warning(f"Error while closing stdin: {e}")
        
        # Cancel the stderr tasks
        logger.info("Cancelling stderr tasks")
        stderr_task.cancel()
        logger_task.cancel()
        try:
            await asyncio.gather(stderr_task, logger_task, return_exceptions=True)
            logger.info("Stderr tasks cancelled")
        except Exception as e:
            logger.warning(f"Error while cancelling stderr tasks: {e}")
        
        # Terminate the process
        if proc.returncode is None:
            logger.info(f"Terminating process {proc.pid}")
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
                logger.info(f"Process terminated with code {proc.returncode}")
            except asyncio.TimeoutError:
                logger.warning("Process didn't terminate gracefully, killing")
                proc.kill()
                await proc.wait()
                logger.info(f"Process killed, code: {proc.returncode}")
        
        logger.info("Cleanup complete")

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(run_test())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unhandled exception in main: {e}")
        sys.exit(1) 