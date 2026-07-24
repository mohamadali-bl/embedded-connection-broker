from connection.MessageAbstract  import MessageAbstract
from connection.mosquitto_broker import MosquittoBroker
from connection.python_broker    import PythonBroker
from connection.HTTPWebsocket    import HTTPWebsocket
from connection.HTTPSimple       import HTTPSimple
from typing                      import Callable

class Connect(MessageAbstract):

    def __init__(self, backend: str | MessageAbstract):

        if isinstance(backend, str):
            brokers = {
                "mqtt_python":    PythonBroker,
                "mqtt_mosquitto": MosquittoBroker,
                "http_simple":    HTTPSimple,
                "http_websocket": HTTPWebsocket
            }
            try:
                self.broker = brokers[backend.lower()]()
            except KeyError:
                raise ValueError(f"Unknown backend '{backend}'.\nAvailable Backend: {list(brokers)}")
        elif isinstance(backend, MessageAbstract):
            self.broker: MessageAbstract
            self.broker = backend
        else: raise ValueError(f"Unknown Broker: {backend}")

    async def start(self):
        await self.broker.start()

    async def stop(self):
        await self.broker.stop()

    def publish(self, topic: str, data):
        self.broker.publish(topic, data)

    def subscribe(self, topic, callback: list[Callable] | tuple[Callable]):
        self.broker.subscribe(topic, callback)