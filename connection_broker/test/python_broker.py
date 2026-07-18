from connection.Connect import Connect
from asyncio import Future, run

def pprint(topic, data):
    print(topic, data)
def valid(topic, data):
    print("\nData received")

async def main():

    bus = Connect("mqtt_python")
    await bus.start()

    bus.subscribe(
        "esp8266-room-01-key-01/data",
        [pprint, valid]
    )

    try:     await Future()
    finally: await bus.stop()

if __name__ == '__main__':
    try:   run(main())
    except KeyboardInterrupt: print("Bye")