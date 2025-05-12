"""Utility functions for the MCP MQTT Proxy."""

import asyncio
import json
import logging
import math
import typing as t
from contextlib import asynccontextmanager
import base64
import uuid

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic import ValidationError

# Import necessary types from mcp - adjust if needed
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage # Base type for MCP messages

logger = logging.getLogger(__name__)

@asynccontextmanager
async def bridge_stdio_to_anyio_session(
    proc_reader: asyncio.StreamReader,
    proc_writer: asyncio.StreamWriter,
) -> t.AsyncGenerator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception], # Session reads from this
        MemoryObjectSendStream[SessionMessage],             # Session writes to this
    ],
    None,
]:
    """Bridges asyncio stdio streams to AnyIO memory streams for ClientSession.

    Takes asyncio reader/writer for a subprocess and yields AnyIO streams
    compatible with mcp.client.session.ClientSession. Handles message framing
    (newline-delimited JSON) and parsing/serialization.
    """
    send_to_session: MemoryObjectSendStream[SessionMessage | Exception]
    receive_from_process: MemoryObjectReceiveStream[SessionMessage | Exception]
    send_to_process: MemoryObjectSendStream[SessionMessage]
    receive_from_session: MemoryObjectReceiveStream[SessionMessage]

    # Create AnyIO streams for the session side (session reads from receive_from_process)
    send_to_session, receive_from_process = anyio.create_memory_object_stream(math.inf)
    # Create AnyIO streams for the process writing side (session writes to send_to_process)
    send_to_process, receive_from_session = anyio.create_memory_object_stream(math.inf)

    loop = asyncio.get_running_loop()
    read_task: asyncio.Task | None = None
    write_task: asyncio.Task | None = None

    async def _read_from_process() -> None:
        """Task: Read lines from proc_reader, decode, wrap, send to AnyIO."""
        try:
            while True:
                line_bytes = await proc_reader.readline()
                if not line_bytes:
                    logger.info("Bridge: Process stdout closed (EOF).")
                    break # EOF
                line = line_bytes.decode('utf-8').strip()
                if not line:
                    continue # Ignore empty lines
                try:
                    # Parse into the generic JSONRPCMessage type
                    msg = JSONRPCMessage.model_validate_json(line)
                    logger.debug(f"Bridge: Read from process: {getattr(msg, 'type', type(msg))}")
                    # Wrap in SessionMessage before sending to AnyIO stream
                    await send_to_session.send(SessionMessage(message=msg))
                except (json.JSONDecodeError, ValidationError) as e:
                    logger.error(f"Bridge: Failed to parse JSON from process stdout: {e}\nLine: '{line}'")
                    await send_to_session.send(e) # Propagate parse error
                except Exception as e:
                     logger.exception("Bridge: Error in read_from_process task")
                     await send_to_session.send(e) # Propagate other errors
                     break
        except asyncio.CancelledError:
            logger.info("Bridge: Read from process task cancelled.")
        except Exception as e:
            logger.exception("Bridge: Unhandled exception in read_from_process task")
            try: await send_to_session.send(e)
            except Exception: pass
        finally:
            logger.debug("Bridge: Closing send_to_session stream.")
            await send_to_session.aclose()


    async def _write_to_process() -> None:
        """Task: Receive SessionMessage from AnyIO, encode, write lines to proc_writer."""
        try:
            async for session_msg in receive_from_session:
                try:
                    # Extract JSONRPCMessage and serialize
                    json_str = session_msg.message.model_dump_json()
                    # Log the class name or method instead of a non-existent .type
                    log_msg_type = getattr(session_msg.message, 'method', type(session_msg.message).__name__)
                    logger.debug(f"Bridge: Writing to process: {log_msg_type}")
                    proc_writer.write(json_str.encode('utf-8') + b'\n')
                    await proc_writer.drain()
                except Exception as e:
                    logger.exception(f"Bridge: Failed to write message to process stdin: {e}")
                    break # Exit task on write error
        except asyncio.CancelledError:
             logger.info("Bridge: Write to process task cancelled.")
        except Exception:
            logger.exception("Bridge: Unhandled exception in write_to_process task")
        finally:
            logger.debug("Bridge: Closing proc_writer stream.")
            if not proc_writer.is_closing():
                try:
                    proc_writer.close()
                    await proc_writer.wait_closed()
                except Exception as e:
                     logger.warning(f"Bridge: Error closing proc_writer: {e}")


    try:
        logger.debug("Starting stdio <-> anyio bridge tasks...")
        read_task = loop.create_task(_read_from_process(), name="bridge_read_from_process")
        write_task = loop.create_task(_write_to_process(), name="bridge_write_to_process")
        # Yield the streams the ClientSession needs to use
        yield receive_from_process, send_to_process
    finally:
        logger.info("Cleaning up stdio <-> anyio bridge...")
        # Clean up tasks (cancel background tasks)
        tasks_to_cancel = [t for t in [write_task, read_task] if t and not t.done()]
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.debug(f"Bridge background tasks cancelled.")

        # Ensure streams are closed (tasks should handle send sides, check receive sides)
        # It's safer to close the streams yielded to the consumer as well
        logger.debug("Closing bridge AnyIO streams (receive_from_session, receive_from_process)...")
        await receive_from_session.aclose()
        await receive_from_process.aclose()
        logger.info("Stdio <-> anyio bridge cleanup complete.") 

def generate_id() -> str:
    """Generates an ID using UUID and base64 encoding.

    Returns:
        A URL-safe Base64 string representation of the UUID.
    """
    return guid_to_base64_url_safe(str(uuid.uuid4()))

def guid_to_base64_url_safe(guid_str: str) -> str:
    """Converts a GUID string to a URL-safe Base64 string.

    Args:
        guid_str: The GUID string to convert.

    Returns:
        A URL-safe Base64 string representation of the GUID.

    # Example usage
        guid = "a444f59f-c77a-4172-966d-a468c135c68e"
        base64_url_safe = guid_to_base64_url_safe(guid)
        print(base64_url_safe)
    """
    guid_bytes = uuid.UUID(guid_str).bytes
    base64_string = base64.b64encode(guid_bytes).decode('utf-8')
    return base64_string.replace('+', '-').replace('/', '_').rstrip('=')

