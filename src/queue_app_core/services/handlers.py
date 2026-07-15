from typing import Callable, Any

from queue_app_core.services.base_service import BaseService


class SimpleHandler(BaseService):
    def __init__(self, 
                 config_path: str, 
                 method: Callable[[bytes], bytes], 
                 request_q: str="queue", 
                 response_q: str="response",):
        """
        Simple handler service for processing messages.

        Extends BaseService as a consumer handler that processes incoming messages
        using a user-provided callback function.

        Args:
            config_path (str): Path to the configuration file containing queue and database settings.
            method (Callable[[bytes], bytes]): Callback function for processing messages.
                Accepts bytes input and returns bytes output.
            request_q (str, optional): Name of the request queue to consume from.
                Defaults to 'queue'.
            response_q (str, optional): Name of the response queue for storing processed results.
                Defaults to 'response'.

        Raises:
            ValueError: If configuration file is invalid or missing required parameters.
            FileNotFoundError: If the configuration file does not exist.
            ConnectionError: If unable to connect to RabbitMQ or SeaweedFS services.

        Example:
            >>> def process(data: bytes) -> bytes:
            ...     return data.upper()
            >>> handler = SimpleHandler('config.json', process, request_q='input', response_q='output')
            >>> handler.start_consume()  # Start consuming messages
        """
        super().__init__(config_path, handler_type="handler", method=method, request_q=request_q, response_q=response_q)
        self.method = method


    def _on_message(self, object_id: str):
        response: dict = {"name": object_id, "status": 200}
        obj = self._obj_db_client.check_object_and_return(object_id, self._request_q)

        file_obj = obj["object"]

        try:
            res = self.method(file_obj)
            res = self._obj_db_client.push_object(res, self._response_q, object_id)
        except Exception as e:
            self._logger.error(f"SERVICE ERROR! Message {e}")
            response["status"] = 500


    def start_consume(self):
        """
        Start consuming messages from the RabbitMQ queue.

        Sets the message callback method and initiates the consumer loop
        to process incoming messages from the configured queue.

        Raises:
            ConnectionError: If unable to connect to RabbitMQ.
            RuntimeError: If the consumer is already running.
            Exception: If an error occurs during message consumption.

        Example:
            >>> handler = SimpleHandler('config.json', process_func)
            >>> handler.start_consume()  # Starts infinite consuming loop
        """
        self._q_broker.on_message_method = self._on_message        

        self._q_broker.start_consume()