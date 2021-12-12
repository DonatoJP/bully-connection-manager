import pika
import pickle


class VaultClient:
    def __init__(self, rabbit_addr, input_queue_name):
        connection_parameters = pika.ConnectionParameters(rabbit_addr)
        connection = pika.BlockingConnection(connection_parameters)

        self.channel = connection.channel()

        self.input_queue_name = input_queue_name
        self.channel.queue_declare(input_queue_name)

        res = self.channel.queue_declare("")
        self.res_queue_name = res.method.queue

    def get(self, key):
        message = f"GET {self.res_queue_name} {key}"
        self.channel.basic_publish("", self.input_queue_name, message)

        method_frame, properties, body = next(self.channel.consume(
            self.res_queue_name, auto_ack=True))

        return pickle.loads(bytes.fromhex(body.decode()))

    def post(self, key: str, value):
        value = pickle.dumps(value).hex()
        message = f"POST {key}={value}"
        self.channel.basic_publish("", self.input_queue_name, message)
