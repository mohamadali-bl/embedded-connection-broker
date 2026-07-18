# Connection Kit

A lightweight communication abstraction layer for Python that provides a unified API for MQTT, HTTP, and WebSocket backends.

Instead of writing application logic around a specific communication protocol, your code interacts with a single interface while the backend can be swapped at runtime.

---

## Features

- Unified communication API
- MQTT (Mosquitto)
- Embedded MQTT Broker (aMQTT)
- HTTP backend
- WebSocket backend
- AsyncIO based
- Multiple callbacks per topic
- Backend-independent application code

---

## Installation

```bash
git clone https://github.com/your_username/connection-kit.git

cd connection-kit

pip install -r requirements.txt
```

---

## Quick Start

```python
from Network.connection_broker.Connect import Connect
from asyncio import Future, run


def pprint(topic, data):
    print(topic, data)
    print("Data received")

def valid(topic, data):
    if topic == "topic/data":
        print(f"{data} validated")



async def main():

    bus = Connect("mqtt_mosquitto")
    await bus.start()

    bus.subscribe(
        "topic/data",
        [pprint, valid]
    )

    try:
        await Future()
    finally:
        await bus.stop()


run(main())
```

---

## Available Backends

```
Connect("mqtt_python")

Connect("mqtt_mosquitto")

Connect("http_simple")

Connect("http_websocket")
```

---

## Multiple Callbacks

```
bus = Connect()
bus.subscribe(
    "sensor/data",
    [
        callback1,
        callback2,
        callback3
    ]
)
```

Every callback receives

```
callback(topic, payload)
```

---

## Project Structure

```
connection/
│
├── Connect.py
├── MessageAbstract.py
├── python_broker.py
├── mosquitto_broker.py
├── HTTPWebsocket.py
└── HTTPSimple.py
```

---

## Architecture

```
               Application
                     │
                     ▼
                Connect API
                     │
      ┌──────────────┼──────────────┐
      │              │              │
      ▼              ▼              ▼
 PythonBroker   MosquittoBroker   HTTP/WebSocket
```

The application never depends on a specific backend.

---

## Requirements

- Python 3.11+
- asyncio
- gmqtt
- amqtt

Mosquitto backend additionally requires the Mosquitto executable installed on the system.

---

## Roadmap

- MQTT QoS configuration
- SSL/TLS support
- Authentication
- Automatic reconnect
- Wildcard subscriptions
- Message serialization
- Logging
- Unit tests

---

## License

MIT License