from abc import ABC, abstractmethod

class MessageAbstract(ABC):
    @abstractmethod
    async def start(self): pass
    @abstractmethod
    async def stop(self): pass
    @abstractmethod
    def publish(self, topic, data): pass
    @abstractmethod
    def subscribe(self, topic, callback): pass