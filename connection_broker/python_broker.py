from connection.MessageAbstract import MessageAbstract
from amqtt.broker               import Broker
from gmqtt                      import Client
from gmqtt.mqtt.constants       import MQTTv311
from json                       import dumps
from typing                     import Callable, Literal, Any

class PythonBroker(MessageAbstract):
    """
    MQTT broker and client wrapper based on aMQTT and gmqtt.

    This class starts an embedded MQTT broker and also provides
    publish/subscribe functionality through an internal MQTT client.

    Features
    --------
    - Start and stop embedded broker
    - Publish messages
    - Subscribe callbacks
    - Dispatch incoming messages
    """
    def __init__(
        self,
        client_id: str = "Python_Controller",
        host: str = "localhost",
        port: int = 1883,
        config: dict | Literal["default"] = "default"
    ):
        """
        Initialize the embedded MQTT broker.

        Parameters
        ----------
        client_id : str
            MQTT client identifier.

        host : str
            Broker address.

        port : int
            Broker port.

        config : dict | str
            Broker configuration.
            If "default" is provided, the internal default configuration
            will be used.
        """
        self.host = host
        self.port = port
        self.client = Client(client_id)
        self.client.on_connect   = lambda *args: print("CONNECTED")
        self.client.on_subscribe = lambda *args: print("SUBSCRIBED")
        self.client.on_message   = self.__dispatch
        self.callbacks: dict[str, list[Callable]] = {}

        if isinstance(config, dict):
            self.broker = Broker(config)

        elif config == "default":
            self.broker = Broker({
                "listeners": {"default": {"type": "tcp", "bind": f"0.0.0.0:{port}"}},
                "plugins": {
                    "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
                    "amqtt.plugins.sys.broker.BrokerSysPlugin": {"sys_interval": 10}
                }
            })

        else: raise ValueError(f"Unknown Broker Config")

    async def start(self):
        """
        Start the embedded broker and connect the internal MQTT client.
        """
        await self.broker.start()
        await self.client.connect(self.host, self.port, version=MQTTv311)

    async def stop(self):
        """
        Disconnect the MQTT client and shut down the broker.
        """
        await self.client.disconnect()
        await self.broker.shutdown()

    def publish(self, topic: str, data: Any):
        """
        Publish a JSON message to the specified topic.

        Parameters
        ----------
        topic : str
            MQTT topic.

        data : Any
            JSON serializable object.
        """
        self.client.publish(topic, dumps(data))

    def subscribe(
        self,
        topic: str,
        callback: Callable | list[Callable] | tuple[Callable]
    ):
        """
        Subscribe one or more callbacks to an MQTT topic.

        If the topic has not been subscribed before,
        the MQTT client subscribes to it automatically.

        Parameters
        ----------
        topic : str
            MQTT topic.

        callback : Callable | list[Callable]
            Callback function(s) executed when a message
            arrives on the topic.
        """
        if not isinstance(callback, (list, tuple)):
            callback = [callback]

        if topic not in self.callbacks:
            self.callbacks[topic] = []
            self.client.subscribe(topic)

        self.callbacks[topic].extend(callback)

    def unsubscribe(
        self,
        topic: str,
        callback: Callable | None = None
    ):
        """
        Remove callback(s) from an MQTT topic.

        Parameters
        ----------
        topic : str
            MQTT topic.

        callback : Callable | None
            Specific callback to remove.

            If None, the topic will be completely unsubscribed.
        """

        if topic not in self.callbacks:
            return

        if callback is None:
            del self.callbacks[topic]
            self.client.unsubscribe(topic)
            return

        try:
            self.callbacks[topic].remove(callback)
        except ValueError:
            return

        if not self.callbacks[topic]:
            del self.callbacks[topic]
            self.client.unsubscribe(topic)

    def __dispatch(
        self,
        client,
        topic,
        payload,
        qos,
        properties
    ):
        """
        Internal message dispatcher.

        Executes every callback registered
        for the received topic.
        """
        callbacks = self.callbacks.get(topic)
        if callbacks is None:
            return
        for callback in callbacks:
            callback(topic, payload)