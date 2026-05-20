import pika 
import asyncio
import logging
import aio_pika
import threading
from typing import Callable, Any, Optional

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type



class _BaseRabbit:
    """
    Base class for synchronous work with RabbitMQ.
    """

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

        self._connect_params = pika.ConnectionParameters(self.url, port=port, heartbeat=6000, blocked_connection_timeout=300, credentials=credentials)


    def _get_connection(self) -> pika.connection.Connection:
        """
        Method for getting new connection if old connection or channel was closed.
        """

        try:
            if not hasattr(self._thread_local, 'connection') or self._thread_local.connection.is_closed:

                connection = pika.BlockingConnection(self._connect_params)
                self._thread_local.connection = connection

                channel = connection.channel()
                with channel:
                    channel.confirm_delivery()

                    channel.queue_declare(queue=self.q_name, durable=True)

        except Exception as e:
            self._logger.error(f"ERROR IN CREATING THE PIKA CONNECTION! Message: {e}")
            raise 

        return self._thread_local.connection


class RabbitPublisher(_BaseRabbit): 
    def __init__(self, 
                 url: str, 
                 user: str,
                 password: str,
                 port: int=5672,
                 q_name: str="queue"):
        super().__init__(url, user, password, port, q_name)
        self._logger.info("Rabbit Publisher created")
    

    @retry(stop=stop_after_attempt(3), 
           wait=wait_fixed(2),
           retry=retry_if_exception_type(Exception))
    def publish(self, msg: str):
        """
        Synchronous publish method.
        If in publish was raise exception method doing retry 3 times with pause between 2 seconds 
        """

        self._logger.info("Publish message")

        try:
            conn = self._get_connection()
            channel = conn.channel()
            channel.confirm_delivery()
            properties = pika.BasicProperties(delivery_mode=2) 

            with channel:
                channel.basic_publish(
                    exchange="",
                    routing_key=self.q_name,
                    body=msg.encode(),
                    properties=properties,
                    mandatory=True)
        except Exception as e:
            self._logger.error(f"ERROR IN MESSAGE PUBLISH! Message: {e}")
            raise 
        

class RabbitConsumer(_BaseRabbit):
    """
    Synchronous Rabbit Consumer class
    """

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
        """
        Method for processing input rabbit message
        """

        msg = body.decode('utf-8')
        self.on_message_method(msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def start_consume(self):
        """
        Consume method
        """

        while True:
            try:
                conn = self._get_connection()
                channel = conn.channel()
                
                with channel:
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


class RabbitPublisherAsync:
    """
    Asynchronous Rabbit Publisher class
    """

    def __init__(self, 
                 url: str, 
                 user: str, 
                 password: str, 
                 port: int=5672, 
                 q_name: str="queue",
                 max_connections: int=1):
        
        self._logger = logging.getLogger(self.__class__.__name__)
        self._amqp_url = f"amqp://{user}:{password}@{url}:{port}/"
        self.q_name = q_name

        self.max_connections = max_connections

        self._connection_pool: Optional[aio_pika.pool.Pool] = None

        self._init_lock = asyncio.Lock()
        self._pool_is_init = False

        self._logger.info("Async Rabbit Publisher created")


    async def _get_connection(self):
        connection = await aio_pika.connect_robust(self._amqp_url, heartbeat=60, timeout=300)
        return connection


    async def _initialize(self):
        """
        Method for initialize pool of connections
        """

        try:
            async with self._init_lock:
                if not self._pool_is_init:
                    self._connection_pool = aio_pika.pool.Pool(self._get_connection, max_size=self.max_connections)

                    async with self._connection_pool.acquire() as conn:
                        channel = await conn.channel()
                        async with channel:
                            await channel.declare_queue(self.q_name, durable=True)
                    
                    self._pool_is_init = True
        except Exception as e:
            self._logger.error(f"ERROR IN CREATING APIKA CONNECTION! Message: {e}")
            raise


    @retry(stop=stop_after_attempt(3), 
           wait=wait_fixed(2),
           retry=retry_if_exception_type(Exception))
    async def publish(self, body: str):
        """
        Async publish method.
        If in publish was raise exception method doing retry 3 times with pause between 2 seconds 
        """

        await self._initialize()

        try:
            async with self._connection_pool.acquire() as conn:
                
                channel = await conn.channel()
                await channel.declare_queue(self.q_name, durable=True)
                await channel.confirm_delivery()

                async with channel:
                    message = aio_pika.Message(body=body.encode("utf-8"), delivery_mode=2)
                    await channel.default_exchange.publish(
                        message,
                        routing_key=self.q_name
                    )

        except Exception as e:
            self._logger.error(f"ERROR IN MESSAGE PUBLISH! Message: {e}")
            raise 
