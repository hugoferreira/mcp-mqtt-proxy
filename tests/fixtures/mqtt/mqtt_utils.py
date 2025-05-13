"""MQTT utilities for testing."""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import aiomqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

logger = logging.getLogger(__name__)

# Default timeouts
MQTT_CONNECTION_TIMEOUT = 5.0  # seconds
MQTT_OPERATION_TIMEOUT = 10.0  # seconds
MQTT_MESSAGE_WAIT_TIMEOUT = 15.0  # seconds


@dataclass
class MQTTMessage:
    """Simplified wrapper for MQTT messages."""
    topic: str
    payload: Union[str, bytes, dict]
    correlation_id: Optional[str] = None
    response_topic: Optional[str] = None
    properties: Optional[Any] = None
    
    @classmethod
    def from_aiomqtt(cls, message: aiomqtt.Message) -> 'MQTTMessage':
        """Create an MQTTMessage from an aiomqtt.Message."""
        correlation_id = None
        response_topic = None
        
        # Extract important properties
        if message.properties:
            if hasattr(message.properties, 'CorrelationData') and message.properties.CorrelationData:
                if isinstance(message.properties.CorrelationData, bytes):
                    correlation_id = message.properties.CorrelationData.decode('utf-8', errors='replace')
                else:
                    correlation_id = str(message.properties.CorrelationData)
                    
            if hasattr(message.properties, 'ResponseTopic') and message.properties.ResponseTopic:
                response_topic = message.properties.ResponseTopic
        
        # Parse payload
        payload = message.payload
        try:
            # Try to parse as JSON
            payload_str = payload.decode('utf-8')
            payload_dict = json.loads(payload_str)
            return cls(
                topic=str(message.topic),
                payload=payload_dict,
                correlation_id=correlation_id,
                response_topic=response_topic,
                properties=message.properties
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            # Keep as bytes if not valid JSON
            return cls(
                topic=str(message.topic),
                payload=payload,
                correlation_id=correlation_id,
                response_topic=response_topic,
                properties=message.properties
            )


async def create_mqtt_client(
    client_id: Optional[str] = None,
    host: str = "localhost",
    port: int = 1883,
    clean_start: bool = True
) -> aiomqtt.Client:
    """Create and connect an MQTT client with standard settings."""
    if not client_id:
        client_id = f"test-client-{uuid.uuid4()}"
    
    logger.info(f"Creating MQTT client: {client_id} -> {host}:{port}")
    client_options = {
        "hostname": host,
        "port": port,
        "client_id": client_id,
        "clean_start": clean_start,
        "timeout": MQTT_CONNECTION_TIMEOUT
    }
    
    client = aiomqtt.Client(**client_options)
    
    # Connect to broker
    logger.info(f"Connecting MQTT client to {host}:{port}")
    await client.connect()
    logger.info(f"Connected MQTT client: {client_id}")
    
    return client


async def publish_jsonrpc_request(
    client: aiomqtt.Client,
    topic: str,
    request: Dict[str, Any],
    response_topic: Optional[str] = None,
    qos: int = 1
) -> str:
    """Publish a JSON-RPC request to an MQTT topic and return the correlation ID."""
    # Use ID from request or generate one
    correlation_id = request.get("id", str(uuid.uuid4()))
    if "id" not in request:
        request["id"] = correlation_id
    
    # Create MQTTv5 properties
    props = Properties(PacketTypes.PUBLISH)
    props.MessageExpiryInterval = 60  # 1 minute expiry
    props.CorrelationData = correlation_id.encode('utf-8')
    
    if response_topic:
        props.ResponseTopic = response_topic
    
    # Publish request
    payload = json.dumps(request).encode('utf-8')
    logger.debug(f"Publishing request to {topic}: {request}")
    
    await client.publish(
        topic,
        payload=payload,
        qos=qos,
        properties=props
    )
    
    return correlation_id


async def wait_for_mqtt_message(
    client: aiomqtt.Client,
    topic_filter: str,
    predicate: Optional[Callable[[MQTTMessage], bool]] = None,
    timeout: float = MQTT_MESSAGE_WAIT_TIMEOUT
) -> MQTTMessage:
    """
    Wait for a message matching the topic_filter and optional predicate function.
    
    Args:
        client: Connected MQTT client
        topic_filter: MQTT topic filter to subscribe to
        predicate: Optional function to filter messages
        timeout: Maximum time to wait in seconds
        
    Returns:
        The first message that matches the predicate
        
    Raises:
        asyncio.TimeoutError: If no matching message is received within timeout
    """
    # Subscribe to the topic
    logger.info(f"Subscribing to topic: {topic_filter}")
    await client.subscribe(topic_filter, qos=1)
    
    # Create a queue for filtered messages
    message_queue = asyncio.Queue()
    
    # Create a task to process incoming messages
    async def message_handler():
        async with client.messages() as messages:
            async for message in messages:
                wrapped_message = MQTTMessage.from_aiomqtt(message)
                logger.debug(f"Received message on topic {wrapped_message.topic}")
                
                if predicate is None or predicate(wrapped_message):
                    await message_queue.put(wrapped_message)
                    # Keep listening for more messages
    
    handler_task = asyncio.create_task(message_handler())
    
    try:
        # Wait for a matching message with timeout
        logger.info(f"Waiting for message on {topic_filter} (timeout: {timeout}s)")
        message = await asyncio.wait_for(message_queue.get(), timeout=timeout)
        logger.info(f"Received matching message on topic: {message.topic}")
        return message
    finally:
        # Clean up handler task
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


async def wait_for_jsonrpc_response(
    client: aiomqtt.Client,
    topic: str,
    request_id: str,
    timeout: float = MQTT_MESSAGE_WAIT_TIMEOUT
) -> Dict[str, Any]:
    """
    Wait for a JSON-RPC response with matching request ID.
    
    Args:
        client: Connected MQTT client
        topic: Topic to listen on for the response
        request_id: Expected request ID in the response
        timeout: Maximum time to wait in seconds
        
    Returns:
        The JSON-RPC response
        
    Raises:
        asyncio.TimeoutError: If no matching response is received within timeout
        ValueError: If response doesn't have expected format
    """
    def match_response(message: MQTTMessage) -> bool:
        """Check if a message is a JSON-RPC response with matching ID."""
        if not isinstance(message.payload, dict):
            return False
            
        payload = message.payload
        if "jsonrpc" not in payload or "id" not in payload:
            return False
            
        return payload["id"] == request_id
    
    response_message = await wait_for_mqtt_message(
        client=client,
        topic_filter=topic,
        predicate=match_response,
        timeout=timeout
    )
    
    if not isinstance(response_message.payload, dict):
        raise ValueError(f"Response payload is not a dict: {response_message.payload}")
        
    return response_message.payload


class MQTTTestClient:
    """Helper class for MQTT testing with context manager support."""
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        host: str = "localhost",
        port: int = 1883,
        clean_start: bool = True
    ):
        """Initialize the MQTT test client."""
        self.client_id = client_id or f"test-client-{uuid.uuid4()}"
        self.host = host
        self.port = port
        self.clean_start = clean_start
        self.client: Optional[aiomqtt.Client] = None
        
    async def __aenter__(self) -> 'MQTTTestClient':
        """Connect the client when entering the context."""
        self.client = await create_mqtt_client(
            client_id=self.client_id,
            host=self.host,
            port=self.port,
            clean_start=self.clean_start
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Disconnect the client when exiting the context."""
        if self.client:
            if self.client.is_connected():
                await self.client.disconnect()
            self.client = None
    
    async def publish_request(
        self,
        topic: str,
        request: Dict[str, Any],
        response_topic: Optional[str] = None,
        qos: int = 1
    ) -> str:
        """Publish a JSON-RPC request and return the correlation ID."""
        if not self.client:
            raise RuntimeError("Client is not connected")
            
        return await publish_jsonrpc_request(
            client=self.client,
            topic=topic,
            request=request,
            response_topic=response_topic,
            qos=qos
        )
    
    async def wait_for_response(
        self,
        topic: str,
        request_id: str,
        timeout: float = MQTT_MESSAGE_WAIT_TIMEOUT
    ) -> Dict[str, Any]:
        """Wait for a JSON-RPC response with matching request ID."""
        if not self.client:
            raise RuntimeError("Client is not connected")
            
        return await wait_for_jsonrpc_response(
            client=self.client,
            topic=topic,
            request_id=request_id,
            timeout=timeout
        )
    
    async def request_response(
        self,
        request_topic: str,
        response_topic: str,
        request: Dict[str, Any],
        timeout: float = MQTT_MESSAGE_WAIT_TIMEOUT,
        qos: int = 1
    ) -> Dict[str, Any]:
        """
        Perform a request-response exchange over MQTT.
        
        This is a convenience method that:
        1. Publishes a request to the request_topic
        2. Waits for a response on the response_topic with matching ID
        3. Returns the response
        """
        if not self.client:
            raise RuntimeError("Client is not connected")
        
        # Publish request and get correlation ID
        correlation_id = await self.publish_request(
            topic=request_topic,
            request=request,
            response_topic=response_topic,
            qos=qos
        )
        
        # Wait for response
        return await self.wait_for_response(
            topic=response_topic,
            request_id=correlation_id,
            timeout=timeout
        ) 