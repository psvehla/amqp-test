import amqp

class MyConsumer:
    def __init__(self, queue_name):
        self.queue_name = queue_name
        self.message_count = 0

    def on_message(self, channel, method_frame, header_frame, body):
        self.message_count += 1
        print(f"Received message: {body.decode()}")
        print(f"Message count: {self.message_count}")

    def consume_messages(self):
        conn = amqp.Connection(host="localhost", port=5672, virtual_host="/", username="guest", password="guest", insist=False)
        chan = conn.channel()
        chan.queue_declare(queue=self.queue_name, durable=True, exclusive=False, auto_delete=False)
        chan.basic_consume(queue=self.queue_name, callback=self.on_message, no_ack=True)
        print("Waiting for messages. To exit press CTRL+C")
        while True:
            chan.wait()

if __name__ == "__main__":
    consumer = MyConsumer('my_queue')
    consumer.consume_messages()
