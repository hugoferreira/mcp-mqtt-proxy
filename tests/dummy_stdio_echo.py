#!/usr/bin/env python
"""Dummy stdio application for testing the MQTT publisher.

Reads lines from stdin, prints them to stderr, and writes a 
dummy JSON response to stdout for each line read.
"""

import sys
import json
import time

def main():
    print("Dummy STDIO Echo started.", file=sys.stderr)
    req_count = 0
    is_initialized = False # Track initialization state
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                print("Dummy STDIO Echo: Received EOF, exiting.", file=sys.stderr)
                break # EOF

            line = line.strip()
            if not line:
                continue # Skip empty lines

            print(f"Dummy STDIO Echo: Received line: {line}", file=sys.stderr)

            # Try to parse as JSON
            req_id = "unknown"
            method = None
            try:
                data = json.loads(line)
                req_id = data.get('id', req_id)
                method = data.get('method') # Check if it's a request method
            except json.JSONDecodeError:
                print("Dummy STDIO Echo: Failed to parse input as JSON.", file=sys.stderr)
                # Cannot respond properly if we can't parse
                continue

            # Handle initialize request specifically
            if method == "initialize" and not is_initialized:
                 print(f"Dummy STDIO Echo: Received initialize request (ID: {req_id}). Sending response.", file=sys.stderr)
                 response = {
                     "jsonrpc": "2.0",
                     "id": req_id,
                     "result": {
                        "serverInfo": {
                             "name": "dummy-stdio-echo",
                             "version": "0.1.0"
                         }
                     }
                 }
                 is_initialized = True
            # Handle other requests after initialization
            elif is_initialized:
                req_count += 1
                print(f"Dummy STDIO Echo: Received request {req_count} (ID: {req_id}, Method: {method}).", file=sys.stderr)
                response = {
                    "jsonrpc": "2.0",
                    "id": req_id, 
                    "result": {
                       "type": "callToolResult", # Dummy result type
                       "isError": False,
                       "content": [
                           {
                               "type": "text",
                               "text": f"Dummy response for request {req_count} (Method: {method})"
                           }
                       ]
                    }
                }
            else:
                # Received request before initialization?
                print(f"Dummy STDIO Echo: Received request before initialize (ID: {req_id}, Method: {method}). Sending error.", file=sys.stderr)
                response = {
                     "jsonrpc": "2.0",
                     "id": req_id,
                     "error": {"code": -32002, "message": "Server not initialized"}
                 }

            response_json = json.dumps(response)
            print(f"Dummy STDIO Echo: Sending response: {response_json}", file=sys.stderr)
            print(response_json, flush=True) # Write response to stdout

            # Add a small delay to simulate work if needed
            # time.sleep(0.1)

        except KeyboardInterrupt:
            print("\nDummy STDIO Echo: KeyboardInterrupt, exiting.", file=sys.stderr)
            break
        except Exception as e:
            print(f"Dummy STDIO Echo: Error: {e}", file=sys.stderr)
            # Decide if we should exit or continue on error
            break # Exit on error for simplicity

if __name__ == "__main__":
    main() 