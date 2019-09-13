import asyncio
from pubsub.message import Message, MessageStatus
from pubsub.pubsub import PubSub


class CreateTask(Message):
    async def do_something(self):
        print(f'Message {self.id} do something')
        self.status = MessageStatus.Success


async def service1():
    pubsub = await PubSub.connect('localhost:9092')

    while True:
        msg = CreateTask({'test': 'ok'})

        await asyncio.sleep(2)

        await pubsub.send(msg)
        print('send: ', msg, msg.id, msg.data)


async def service2():
    pubsub = await PubSub.connect('localhost:9092')

    async for msg in pubsub.receive(CreateTask, status=MessageStatus.New):
        print('receive: ', msg, msg.id, msg.data)
        await msg.do_something()
        await pubsub.send(msg)


async def service3():
    pubsub = await PubSub.connect('localhost:9092')

    async for msg in pubsub.receive(CreateTask, status=MessageStatus.Success):
        print('success_receive: ', msg, msg.id, msg.data, '\n')


loop = asyncio.get_event_loop()
loop.create_task(service1())
loop.create_task(service2())
loop.create_task(service3())
loop.run_forever()
