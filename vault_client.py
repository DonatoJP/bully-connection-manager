import logging
import time
import os
import random
import pickle

import pika


def _connect_to_rabbit(rabbit_addr):
    connection_parameters = pika.ConnectionParameters(rabbit_addr)

    retries = 5
    logging.info(f"Connecting to Rabbit at {connection_parameters}")
    while True:
        try:
            return pika.BlockingConnection(connection_parameters)
        except pika.exceptions.AMQPConnectionError:
            if retries > 0:
                logging.info(
                    "Connection to rabbitMQ failed. Retrying after 1 second")
                retries -= 1
                time.sleep(1)
            else:
                logging.info(f"{retries} retries failed. Exiting")
                return None


def main():
    rabbit_adress = os.environ['RABBIT_ADDRESS']
    connection = _connect_to_rabbit(rabbit_adress)

    channel = connection.channel()

    input_queue_name = os.environ['INPUT_QUEUE_NAME']
    channel.queue_declare(input_queue_name)

    res = channel.queue_declare("")
    res_queue_name = res.method.queue

    for i in [random.randint(1, 20) for _ in range(1, 50)]:
        value = pickle.dumps({f"Juan {i}": "Capo"}).hex()
        message = f"POST key{i}={value}"
        print(message)
        channel.basic_publish("", input_queue_name, message)

    for i in [random.randint(1, 20) for _ in range(1, 50)]:
        message = f"GET {res_queue_name} key{i}"
        print(message)
        channel.basic_publish("", input_queue_name, message)

        method_frame, properties, body = next(channel.consume(
            res_queue_name))

        print(body.decode())
        print(pickle.loads(bytes.fromhex(body.decode())))


if __name__ == "__main__":
    main()
