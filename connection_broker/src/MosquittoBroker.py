from MessageAbstract import MessageAbstract
from pathlib                       import Path
from typing                        import Callable, Literal,  Any
# lazy importing:
# from gmqtt import Client
# from gmqtt.mqtt.constants import MQTTv311
# from asyncio import sleep
# from subprocess import Popen
# from json import dumps

class MosquittoBroker(MessageAbstract):
    """
    MQTT broker wrapper based on an external Mosquitto process.

    This class optionally starts and stops a Mosquitto broker process
    and provides publish/subscribe functionality through an internal
    MQTT client.

    Features
    --------
    - Start and stop Mosquitto
    - Publish messages
    - Subscribe callbacks
    - Unsubscribe callbacks
    - Dispatch incoming messages
    """

    def __init__(
        self,
        client_id:  str = "Mosquitto_Controller",
        host:       str = "localhost",
        port:       int = 1883,
        config:     str | Path | Literal["default"] = "default",
        executable: str | Path | Literal["default"] = "default",
        manage_broker: bool = True
    ):
        """
        Initialize the Mosquitto broker wrapper.

        Parameters
        ----------
        client_id : str
            MQTT client identifier.

        host : str
            Broker address.

        port : int
            Broker port.

        config : str | PathLike
            Path to Mosquitto configuration file.
            If "default" is provided, the internal configuration
            will be used.

        manage_broker : bool
            If True, this class starts and stops the Mosquitto
            process automatically.
        """
        from gmqtt import Client

        ROOT = Path(__file__).resolve().parent.parent
        self.process = None
        self.host    = host
        self.port    = port
        self.manage_broker = manage_broker
        self.client        = Client(client_id)
        self.client.on_connect   = lambda *args: print("CONNECTED")
        self.client.on_subscribe = lambda *args: print("SUBSCRIBED")
        self.client.on_message   = self.__dispatch
        self.callbacks: dict[str, list[Callable]] = {}
        self.config_path = None
        self.exe_path    = None

        if config == "default":
            self.config_path = ROOT / "config" / "brokers" / "mosquitto.conf"
            if not self.config_path.exists():
                raise ValueError("mosquitto config Path Not Found")
        elif isinstance(config, (str, Path)):
            self.config_path = Path(config)
        else:
            raise ValueError("mosquitto config Not Found")

        if executable == "default":
            self.exe_path = Path("mosquitto")
        elif isinstance(executable, (str, Path)):
            self.exe_path = Path(executable)
        else:
            raise ValueError("mosquitto Executable Not Found")
    # ------------------------------------------------ #
    async def start(self):
        """
        Start the Mosquitto broker (optional) and connect
        the internal MQTT client.
        """
        from subprocess import Popen
        from asyncio    import sleep
        from gmqtt.mqtt.constants import MQTTv311

        if self.client.is_connected:
            return

        if self.manage_broker:
            if self.process is None or self.process.poll() is not None:
                try:
                    if self.config_path:
                        self.process = Popen(
                            [str(self.exe_path), "-c", str(self.config_path)]
                        )
                    else:
                        self.process = Popen([str(self.exe_path)])
                    await sleep(0.5)
                except FileNotFoundError:
                    raise RuntimeError("Mosquitto executable not found.")

        await self.client.connect(
            self.host,
            self.port,
            version=MQTTv311
        )
        print("Broker Started")
    # ------------------------------------------------ #
    async def stop(self):
        """
        Disconnect the MQTT client and stop the Mosquitto
        process if it is managed by this class.
        """
        if self.client.is_connected:
            await self.client.disconnect()

        if (
            self.manage_broker
            and self.process
            and self.process.poll() is None
        ):
            self.process.terminate()
            self.process.wait()
            self.process = None

        print("Broker Stopped")
    # ------------------------------------------------ #
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
        from json import dumps

        if not self.client.is_connected:
            raise RuntimeError("Broker is not connected.")

        self.client.publish(topic, dumps(data))
    # ------------------------------------------------ #
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
            arrives at the topic.
        """

        if not isinstance(callback, (list, tuple)):
            callback = [callback]

        if topic not in self.callbacks:
            self.callbacks[topic] = []
            self.client.subscribe(topic)

        self.callbacks[topic].extend(callback)
    # ------------------------------------------------ #
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
    # ------------------------------------------------ #
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
    # ------------------------------------------------ #