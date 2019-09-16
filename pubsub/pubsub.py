import asyncio
import json
from typing import Union, List, Type, AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pubsub.message import Message, MessageStatus


class ApacheKafkaPubSub:
    def __init__(self, host: str, *, loop: asyncio.AbstractEventLoop = None):
        self.loop = loop or asyncio.get_event_loop()
        self.host = host
        self.producer = None
        self.consumer = None

    async def send(self, message: Message) -> None:
        if not self.producer:
            self.producer = AIOKafkaProducer(loop=self.loop, bootstrap_servers=self.host)
            await self.producer.start()
        await self.producer.send(message.channel.__name__, json.dumps(message.serialize()).encode())

    async def receive(self, *args: Type[Message],
                      status: Union[MessageStatus, List[MessageStatus]] = None) -> AsyncGenerator:
        statuses = [n for n in MessageStatus] if not status else status if isinstance(status, list) else [status]
        channels = [n.channel.__name__ for n in args]
        self.consumer = AIOKafkaConsumer(*channels, loop=self.loop, bootstrap_servers=self.host)
        await self.consumer.start()
        async for msg in self.consumer:
            msg = Message.deserialize(json.loads(msg.value))
            if msg.status in statuses:
                yield msg
