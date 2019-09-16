import asyncio
import json
import datetime
import re
from typing import Union, List, Type, AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pubsub.message import Message, MessageStatus


class JSONConverter:
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
    DATETIME_REGEX = re.compile(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})')

    def encode(self, value):
        if isinstance(value, datetime.datetime):
            return value.strftime(self.DATETIME_FORMAT)

    def decode(self, data):
        if isinstance(data, list):
            return [self.decode(n) for n in data]
        elif isinstance(data, dict):
            return {k: self.decode(v) for k, v in data.items()}
        else:
            if isinstance(data, str) and self.DATETIME_REGEX.match(data):
                return datetime.datetime.strptime(data, self.DATETIME_FORMAT)
            return data


class ApacheKafkaPubSub:
    def __init__(self, host: str, *, loop: asyncio.AbstractEventLoop = None):
        self.loop = loop or asyncio.get_event_loop()
        self.host = host
        self.producer = None
        self.consumer = None
        self.converter = JSONConverter()

    async def send(self, message: Message) -> None:
        if not self.producer:
            self.producer = AIOKafkaProducer(loop=self.loop, bootstrap_servers=self.host)
            await self.producer.start()
        await self.producer.send(message.channel.__name__, json.dumps(message.serialize(),
                                                                      default=self.converter.encode).encode())

    async def receive(self, *args: Type[Message],
                      status: Union[MessageStatus, List[MessageStatus]] = None) -> AsyncGenerator:
        statuses = [n for n in MessageStatus] if not status else status if isinstance(status, list) else [status]
        channels = [n.channel.__name__ for n in args]
        self.consumer = AIOKafkaConsumer(*channels, loop=self.loop, bootstrap_servers=self.host)
        await self.consumer.start()
        async for msg in self.consumer:
            msg = Message.deserialize(json.loads(msg.value, object_hook=self.converter.decode))
            if msg.status in statuses:
                yield msg
