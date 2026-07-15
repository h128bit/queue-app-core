import logging
from dotenv import dotenv_values
from typing import Callable, Any

from queue_app_core.queue.seaweed_publishers_consumers import SimpleSeaweedPublisherConsumer
from queue_app_core.queue.rabbit_publishers_consumers import RabbitConsumer, RabbitPublisher, RabbitPublisherAsync


class BaseService:
    """
    Base class for Services.

    In creating class building RabbitMQ publisher and consumer clients and SeaweedFS client.
    """

    def __init__(self, 
                 config_path: str, 
                 handler_type: str, 
                 request_q: str="queue",
                 response_q: str="response",
                 method: Callable[[Any], bytes]|None=None):
        """
        Base class for Services.

        Provides core functionality for building RabbitMQ publisher and consumer clients
        along with SeaweedFS client integration. Supports both router (publisher) and
        handler (consumer) modes.

        Args:
            config_path (str): Path to the configuration file containing queue and database settings.
            handler_type (str): Type of handler to initialize. Must be either 'router' for publishing
                or 'handler' for consuming messages.
            request_q (str, optional): Name of the request queue. Defaults to 'queue'.
            response_q (str, optional): Name of the response queue. Defaults to 'response'.
            method (Callable[[Any], bytes] | None, optional): Callback function for processing messages.
                Required when handler_type is 'handler'. Accepts a message and returns bytes.
                Defaults to None.

        Raises:
            ValueError: If handler_type is neither 'router' nor 'handler', or if required
                parameters are missing for the specified type.
            FileNotFoundError: If the configuration file does not exist.
            ConnectionError: If unable to connect to RabbitMQ or SeaweedFS services.
        """

        self._logger = logging.getLogger(self.__class__.__name__)

        q_config, db_config = self._read_config(config_path)

        self._request_q = request_q
        self._response_q = response_q

        q_config["q_name"] = request_q
        match handler_type:
            case "router":
                self._q_broker = RabbitPublisher(**q_config)
            case "handler":
                self._q_broker = RabbitConsumer(on_message=method, **q_config)
            case _:
                raise ValueError(f"Unsupported handler type, expected `router` or `handler`, got {handler_type}")
            
        self._obj_db_client = SimpleSeaweedPublisherConsumer(object_db_path_or_url=db_config["object_db_path_or_url"],
                                                             query_q=request_q, 
                                                             response_q=response_q)
    def _read_config(self, config_path: str):
        config = dotenv_values(config_path)

        q_url = config["QUEUE_URL"]
        q_user = config["QUEUE_USER"]
        q_pass = config["QUEUE_PASSWORD"]
        q_port = int(config["QUEUE_PORT"])

        obj_url = config["OBJECT_DB_PATH"]

        queue_config = {
            "url": q_url,
            "user": q_user,
            "password": q_pass,
            "port": q_port,
        }

        object_db_config = {
            "object_db_path_or_url": obj_url
        }

        return queue_config, object_db_config



class BaseServiceAsync(BaseService):
    def __init__(self, 
                 config_path: str, 
                 handler_type: str, 
                 request_q: str="queue",
                 response_q: str="response",
                 method: Callable[[Any], bytes]|None=None):
        
        """
        Async version of the BaseService class.

        Extends BaseService with asynchronous RabbitMQ publisher support
        for non-blocking message publishing operations.

        Args:
            config_path (str): Path to the configuration file containing queue and database settings.
            handler_type (str): Type of handler to initialize. Must be either 'router' for publishing
                or 'handler' for consuming messages. Defaults to 'router' for async operations.
            request_q (str, optional): Name of the request queue. Defaults to 'queue'.
            response_q (str, optional): Name of the response queue. Defaults to 'response'.
            method (Callable[[Any], bytes] | None, optional): Callback function for processing messages.
                Required when handler_type is 'handler'. Defaults to None.

        Raises:
            ValueError: If handler_type is neither 'router' nor 'handler'.
            FileNotFoundError: If the configuration file does not exist.
            ConnectionError: If unable to connect to RabbitMQ services.
        """

        super().__init__(config_path=config_path, 
                         handler_type="router", 
                         request_q=request_q, 
                         response_q=response_q, 
                         method=method)
        
        if handler_type == "router":
            q_config, _ = self._read_config(config_path)
            q_config["q_name"] = request_q
            self._q_broker = RabbitPublisherAsync(**q_config)
                