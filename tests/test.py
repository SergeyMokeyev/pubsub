import asyncio
import logging
import datetime
from pubsub.message import Message, MessageStatus
from pubsub.pubsub import ApacheKafkaPubSub


logging.basicConfig(level=logging.INFO)


class CreateTask(Message):
    async def do_something(self):
        print(f'\tmessage {self.id} do something!!!')
        self.status = MessageStatus.Success


class OtherMessage(Message):
    pass


async def service1():
    pubsub = ApacheKafkaPubSub('localhost:9092')

    while True:
        msg = CreateTask({'date': datetime.datetime.utcnow(), 'test1': [datetime.datetime.utcnow()], 'test2': {'date2': datetime.datetime.utcnow()}})

        await asyncio.sleep(2)

        await pubsub.send(msg)
        print('service1 send: ', msg, msg.id, msg.data)


async def service2():
    pubsub = ApacheKafkaPubSub('localhost:9092')

    async for msg in pubsub.receive(CreateTask, status=MessageStatus.New):
        print('service2 handle: ', msg, msg.id, msg.data)
        await msg.do_something()
        await pubsub.send(msg)


async def service3():
    pubsub = ApacheKafkaPubSub('localhost:9092')

    async for msg in pubsub.receive(CreateTask, status=MessageStatus.Success):
        print('service3 receive success: ', msg, msg.id, msg.data, '\n')


async def service4_logger():
    pubsub = ApacheKafkaPubSub('localhost:9092')

    async for msg in pubsub.receive(CreateTask, OtherMessage):
        logging.info('Message %s change status to %s', str(msg.id), msg.status.value)


loop = asyncio.get_event_loop()
loop.create_task(service1())
loop.create_task(service2())
loop.create_task(service3())
loop.create_task(service4_logger())
loop.run_forever()
