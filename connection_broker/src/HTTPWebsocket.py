from connection.MessageAbstract import MessageAbstract

class HTTPWebsocket(MessageAbstract):

    def start(self):
        pass

    def stop(self):
        pass

    def publish(self, topic: str, data):
        pass

    def subscribe(self, topic: str, callback):
        pass