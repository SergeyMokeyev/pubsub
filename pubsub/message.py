import uuid
import typing
import enum


class MessageStatus(enum.Enum):
    New = 'New'
    Success = 'Success'
    Error = 'Error'


class MessageMeta(type):
    __classes = {}

    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)
        cls.channel = cls
        MessageMeta.__classes.update({dct['__qualname__']: cls})
        return cls


class Message(metaclass=MessageMeta):
    def __init__(self, data: typing.Any = None, *, status: MessageStatus = None):
        self.id = uuid.uuid4()
        self.channel = self.__class__
        self.status = status or MessageStatus.New
        self.data = data

    def serialize(self) -> dict:
        return {
            'id': str(self.id),
            'channel': self.channel.__name__,
            'status': MessageStatus(self.status).value,
            'data': self.data
        }

    @staticmethod
    def deserialize(data) -> 'Message':
        cls = MessageMeta._MessageMeta__classes[data['channel']]
        inst = cls(data.get('data'), status=MessageStatus(data.get('status')))
        inst.id = uuid.UUID(data.get('id')) or inst.id
        return inst
