from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


class HelloWorld(MessagingHandler):
    def __init__(self, server, address):
        super(HelloWorld, self).__init__()
        self.server = server
        self.address = address

    def on_start(self, event):
        print("on_start")
        conn = event.container.connect(self.server)
        event.container.create_receiver(conn, self.address)
        event.container.create_sender(conn, self.address)

    def on_sendable(self, event):
        print("sending message")

        for msg in range(1000):
            event.sender.send(Message(body="Hello World!"))

        event.sender.close()

    def on_message(self, event):
        print(event.message.body)
        event.connection.close()


Container(HelloWorld("artemis:artemis@localhost:61616", "pex2")).run()
