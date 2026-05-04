import pika 
import threading
from typing import Callable, Any



class _BaseRabbit:
    def __init__(self, 
                 url: str,
                 user: str,
                 password: str,
                 q_name: str="queue"):
        self.url = url 
        self.q_name = q_name

        self._thread_local = threading.local()

        credentials = pika.PlainCredentials(user, password)

        self._connect_params = pika.ConnectionParameters(self.url, heartbeat=600, blocked_connection_timeout=10, credentials=credentials)
        self._connection = None 
        self._channel = None


    def _get_connection(self):
        if self._connection is None or not hasattr(self._thread_local, 'connection') or self._connection.is_closed:
            connection = pika.BlockingConnection(self._connect_params)
            channel = connection.channel()
            self._connection = connection 
            self._channel = channel
            self._thread_local.connection = self._connection
            self._thread_local.channel = self._channel
            self._thread_local.channel.confirm_delivery()

            channel.queue_declare(queue=self.q_name, durable=True)

        return self._thread_local.connection, self._thread_local.channel


class RabbitPublisher(_BaseRabbit):
    def __init__(self, 
                 url: str, 
                 user: str,
                 password: str,
                 q_name: str="queue"):
        super().__init__(url, user, password, q_name)
    

    def publish(self, msg: str):
        _, channel = self._get_connection()

        properties = pika.BasicProperties(delivery_mode=2) 
        channel.basic_publish(
            exchange="",
            routing_key=self.q_name,
            body=msg.encode(),
            properties=properties,
            mandatory=True)
        

class RabbitConsumer(_BaseRabbit):
    def __init__(self, 
                 url: str, 
                 user: str,
                 password: str, 
                 on_message: Callable[[Any], bytes],
                 q_name: str="queue"):
        super().__init__(url, user, password, q_name)

        self.on_message_method = on_message


    def _on_message(self, ch, method, properties, body):
        msg = body.decode('utf-8')
        self.on_message_method(msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def start_consume(self):
        conn, channel = self._get_connection()

        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
        queue=self.q_name,
        on_message_callback=self._on_message,
        auto_ack=False)

        channel.start_consuming()

        try:
            channel.start_consuming()
        except Exception as e:
            pass 
        #TODO: process error
        
    