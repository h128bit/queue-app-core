import pika 
import logging
import threading
from typing import Callable, Any

from tenacity import retry, stop_after_attempt, wait_fixed



class _BaseRabbit:
    def __init__(self, 
                 url: str,
                 user: str,
                 password: str,
                 port: int=5672,
                 q_name: str="queue"):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.url = url 
        self.q_name = q_name

        self._thread_local = threading.local()

        credentials = pika.PlainCredentials(user, password)

        self._connect_params = pika.ConnectionParameters(self.url, port=port, heartbeat=60, blocked_connection_timeout=300, credentials=credentials)


    def _get_connection(self):
        try:
            if not hasattr(self._thread_local, 'connection') or self._thread_local.connection.is_closed:
                connection = pika.BlockingConnection(self._connect_params)
                channel = connection.channel()
                self._thread_local.connection = connection
                self._thread_local.channel = channel
                self._thread_local.channel.confirm_delivery()

                channel.queue_declare(queue=self.q_name, durable=True)
        except Exception as e:
            self._logger.error(f"ERROR IN CREATING THE PIKA CONNECTION! Message: {e}")

        return self._thread_local.connection, self._thread_local.channel


class RabbitPublisher(_BaseRabbit): 
    def __init__(self, 
                 url: str, 
                 user: str,
                 password: str,
                 port: int=5672,
                 q_name: str="queue"):
        super().__init__(url, user, password, port, q_name)
        self._logger.info("Rabbit Publisher created")
    

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def publish(self, msg: str):
        self._logger.info("publish message")

        try:
            _, channel = self._get_connection()

            properties = pika.BasicProperties(delivery_mode=2) 
            channel.basic_publish(
                exchange="",
                routing_key=self.q_name,
                body=msg.encode(),
                properties=properties,
                mandatory=True)
        except Exception as e:
            self._logger.error(f"ERROR IN MESSAGE PUBLISH! Message: {e}")
            raise e
        

class RabbitConsumer(_BaseRabbit):
    def __init__(self, 
                 url: str, 
                 user: str,
                 password: str, 
                 on_message: Callable[[Any], bytes],
                 port: int=5672,
                 q_name: str="queue"):
        super().__init__(url, user, password, port, q_name)
        self._logger.info("Rabbit Consumer created")

        self.on_message_method = on_message


    def _on_message(self, ch, method, properties, body):
        msg = body.decode('utf-8')
        self.on_message_method(msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def start_consume(self):

        while True:
            try:
                conn, channel = self._get_connection()

                channel.basic_qos(prefetch_count=1)

                channel.basic_consume(
                queue=self.q_name,
                on_message_callback=self._on_message,
                auto_ack=False)

                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as e:
                self._logger.info(f"Brocker reconnect. Message: {e}")
            except Exception as e:
                self._logger.error(f"ERROR IN CONSUME! Message: {e}") 
        
    