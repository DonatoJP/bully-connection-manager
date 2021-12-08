import time
import logging
import signal

import pika


class RabbitMessageProcessor:
    def set_rabbit_channel(self, channel, queue_name):
        self.channel = channel
        self.queue_name = queue_name

    def process(self, message):
        return message

    def __call__(self, ch, method, properties, body):
        message = body.decode()
        result = self.process(message)
        if result is not None:
            self.channel.basic_publish(
                exchange='', routing_key=self.queue_name, body=result.encode())


class RabbitConsumerServer:
    def __init__(self, rabbit_addr, input_queue_name, output_queue_name, message_processor):
        self.connection_parameters = pika.ConnectionParameters(rabbit_addr)
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.message_processor = message_processor

    def start(self):
        connection = self._connect_to_rabbit()
        self.channel = connection.channel()

        self.channel.queue_declare(queue=self.input_queue_name)
        self.channel.queue_declare(queue=self.output_queue_name)

        self.message_processor.set_rabbit_channel(
            self.channel, self.output_queue_name)

        self.channel.basic_consume(queue=self.input_queue_name,
                                   on_message_callback=self.message_processor)

        def stop_signal_handler(*args):
            self.stop()

        signal.signal(signal.SIGTERM, stop_signal_handler)
        signal.signal(signal.SIGINT, stop_signal_handler)

        logging.info('Waiting for messages from rabbit. To exit press CTRL+C')
        self.channel.start_consuming()

        self.channel.start_consuming()

        logging.info('Server stopped')

        self.channel.close()

    def stop(self):
        self.channel.stop_consuming()

    def _connect_to_rabbit(self):
        retries = 5
        while True:
            try:
                return pika.BlockingConnection(self.connection_parameters)
            except pika.exceptions.AMQPConnectionError:
                if retries > 0:
                    logging.info(
                        "Connection to rabbitMQ failed. Retrying after 1 second")
                    retries -= 1
                    time.sleep(1)
                else:
                    logging.info(f"{retries} retries failed. Exiting")
                    return None
