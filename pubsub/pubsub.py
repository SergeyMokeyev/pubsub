import asyncio
import json
from collections.abc import Iterable
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pubsub.message import Message, MessageStatus


class PubSub:
    def __init__(self, host: str, *, loop: asyncio.AbstractEventLoop = None):
        self.loop = loop or asyncio.get_event_loop()
        self.host = host
        self.producer = AIOKafkaProducer(loop=self.loop, bootstrap_servers=self.host)
        self.consumer = AIOKafkaConsumer('message', loop=self.loop, bootstrap_servers=self.host)
        self.queue = asyncio.Queue()

    @classmethod
    async def connect(cls, host: str, *, loop: asyncio.AbstractEventLoop = None) -> 'PubSub':
        inst = cls(host, loop=loop)
        await inst.producer.start()
        await inst.consumer.start()
        return inst

    async def send(self, message: Message) -> None:
        await self.producer.send('message', json.dumps(message.serialize()).encode())

    async def receive(self, *args: Message, status: MessageStatus = None):
        statuses = (n for n in MessageStatus) if not status else status if isinstance(status, Iterable) else (status,)
        async for msg in self.consumer:
            try:
                msg = Message.deserialize(json.loads(msg.value))
            except KeyError:
                continue
            if msg.channel not in args or msg.status not in statuses:
                continue
            yield msg
