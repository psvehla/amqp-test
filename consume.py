from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

class ReceiveHandler(MessagingHandler):
    def __init__(self, server, address):
        super(ReceiveHandler, self).__init__()
        self.server = server
        self.address = address
        self.count = 0

    def on_start(self, event):
        conn = event.container.connect(self.server)
        event.container.create_receiver(conn, self.address)

    def on_message(self, event):
        self.count += 1
        print(f"Received message #{self.count}: {event.message.body}")

    def on_connection_closed(self, event):
        print(f"Connection closed. Total messages received: {self.count}")

if __name__ == "__main__":
    # Update these variables with your server's information
    server_url = 'amqp://user:password@hostname:port'
    queue_name = 'your_queue_name'

    try:
        handler = ReceiveHandler(server_url, queue_name)
        container = Container(handler)
        container.run()
    except KeyboardInterrupt:
        print('Interrupted')
