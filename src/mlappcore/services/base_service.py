import logging
from dotenv import dotenv_values
from typing import Callable, Any

from mlappcore.queue.seaweed_publishers_consumers import SimpleSeaweedPublisherConsumer
from mlappcore.queue.rabbit_publishers_consumers import RabbitConsumer, RabbitPublisher


class BaseService:
    def __init__(self, 
                 config_path: str, 
                 handler_type: str, 
                 request_q: str="queue",
                 response_q: str="response",
                 method: Callable[[Any], bytes]|None=None):
        self._logger = logging.getLogger(self.__class__.__name__)

        config = dotenv_values(config_path)

        q_url = config["QUEUE_URL"]
        q_user = config["QUEUE_USER"]
        q_pass = config["QUEUE_PASSWORD"]
        q_port = int(config["QUEUE_PORT"])

        obj_url = config["OBJECT_DB_PATH"]

        self._request_q = request_q
        self._response_q = response_q

        config = {
            "url": q_url,
            "user": q_user,
            "password": q_pass,
            "port": q_port,
            "q_name": request_q 
        }

        match handler_type:
            case "router":
                self._q_broker = RabbitPublisher(**config)
            case "handler":
                self._q_broker = RabbitConsumer(on_message=method, **config)
            case _:
                raise ValueError(f"Unsupported handler type, expected `router` or `handler`, got {handler_type}")
            
        self._obj_db_client = SimpleSeaweedPublisherConsumer(obj_url, 
                                                             query_q=request_q, 
                                                             response_q=response_q)
