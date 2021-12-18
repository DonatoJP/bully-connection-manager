import time
import logging
import signal

import pika


class RabbitMessageProcessor:
    def process(self, message):
        return message

    def __call__(self, ch, method, properties, body):
        message = body.decode()
        queue, result = self.process(message)
        if result is not None and queue is not None:
            ch.basic_publish(
                exchange='', routing_key=queue, body=result.encode())

        ch.basic_ack(method.delivery_tag)


class RabbitConsumerServer:
    def __init__(self, rabbit_addr, input_queue_name, message_processor):
        self.connection_parameters = pika.ConnectionParameters(rabbit_addr)
        self.input_queue_name = input_queue_name
        self.message_processor = message_processor
        self.connection = None

    def start(self):
        self.connection = self._connect_to_rabbit()
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.input_queue_name)

        logging.info(self.input_queue_name)

        self.channel.basic_consume(queue=self.input_queue_name,
                                   on_message_callback=self.message_processor)

        logging.info('Waiting for messages from rabbit. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
            logging.info('Server stopped')

            self.channel.close()
            self.connection.close()
        except:
            print('======================================================================================================================================================================================================================================================================================================')
            pass

    def stop(self):
        self.channel.stop_consuming()
        # self.channel.close()
        # self.connection.close()

    def _connect_to_rabbit(self):
        retries = 5
        logging.info(f"Connecting to Rabbit at {self.connection_parameters}")
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
