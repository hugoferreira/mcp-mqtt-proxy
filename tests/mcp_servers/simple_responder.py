#!/usr/bin/env python
"""
A simple stdio MCP server for testing.
Implements prompts/list and echoes other requests.
"""

import asyncio
import logging
import sys
import os # Add os import for path manipulation

from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting SimpleResponder MCP Server...")
    server = Server[object]("simple-responder")

    fixed_prompt = types.Prompt(name="simple_prompt", description="A basic prompt from the simple responder.")

    @server.list_prompts() # type: ignore
    async def list_prompts_handler() -> list[types.Prompt]:
        logger.info(f"Handling prompts/list request. Returning: {[fixed_prompt]}")
        return [fixed_prompt]

    # Optional: Add a default handler to echo other requests for debugging
    @server.route(r'.*') # type: ignore
    async def echo_handler(context: types.RequestContext, request: types.Request) -> types.Response:
        logger.info(f"Handling unknown request (ID: {request.id}, Method: {request.method}). Echoing back.")
        # Echo the request params back as a simple text result
        result_content = types.TextContent(text=f"Echo: Received method '{request.method}' with params: {request.params}")
        return types.CallToolResult(content=[result_content])

    try:
        # Manually get loop and run stdio_server
        # logger.info("SimpleResponder: Getting loop and running stdio_server manually...")
        # loop = asyncio.get_event_loop_policy().get_event_loop()
        # try: 
        #     loop.run_until_complete(stdio_server(server))
        #     logger.info("SimpleResponder: loop.run_until_complete(stdio_server(...)) finished.")
        # except Exception as e_manual:
        #      logger.exception(f"SimpleResponder: loop.run_until_complete(stdio_server(...)) raised an exception: {e_manual}")
        # finally:
        #     # Should we close the loop?
        #     # logger.info("SimpleResponder: Closing loop.")
        #     # loop.close()
        #     pass # Let script exit manage cleanup
        # Restore original asyncio.run call
        logger.info("SimpleResponder: Calling asyncio.run(stdio_server(...))")
        asyncio.run(stdio_server(server))
        logger.info("SimpleResponder: asyncio.run(stdio_server(...)) finished.")

    except KeyboardInterrupt:
        logger.info("SimpleResponder server shut down.")
    except Exception as e_outer:
        logger.exception("SimpleResponder server encountered an error.")
        sys.exit(1)

if __name__ == "__main__":
    main() 