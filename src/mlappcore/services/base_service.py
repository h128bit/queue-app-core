import logging
from dotenv import dotenv_values
from typing import Callable, Any

from mlappcore.queue.seaweed_publishers_consumers import SimpleSeaweedPublisherConsumer
from mlappcore.queue.rabbit_publishers_consumers import RabbitConsumer, RabbitPublisher, RabbitPublisherAsync


class BaseService:
    """
    Base class for Services.
    """

    def __init__(self, 
                 config_path: str, 
                 handler_type: str, 
                 request_q: str="queue",
                 response_q: str="response",
                 method: Callable[[Any], bytes]|None=None):
        self._logger = logging.getLogger(self.__class__.__name__)

        q_config, db_config = self._read_config(config_path)

        # config = dotenv_values(config_path)

        # q_url = config["QUEUE_URL"]
        # q_user = config["QUEUE_USER"]
        # q_pass = config["QUEUE_PASSWORD"]
        # q_port = int(config["QUEUE_PORT"])

        # obj_url = config["OBJECT_DB_PATH"]

        # self._request_q = request_q
        # self._response_q = response_q

        # config = {
        #     "url": q_url,
        #     "user": q_user,
        #     "password": q_pass,
        #     "port": q_port,
        #     "q_name": request_q 
        # }

        q_config["q_name"] = request_q
        match handler_type:
            case "router":
                self._q_broker = RabbitPublisher(**q_config)
            case "handler":
                self._q_broker = RabbitConsumer(on_message=method, **q_config)
            case _:
                raise ValueError(f"Unsupported handler type, expected `router` or `handler`, got {handler_type}")
            
        self._obj_db_client = SimpleSeaweedPublisherConsumer(query_q=request_q, 
                                                             response_q=response_q,
                                                             *db_config)
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
        super().__init__(config_path=config_path, 
                         handler_type="router", 
                         request_q=request_q, 
                         response_q=response_q, 
                         method=method)
        
        if handler_type == "router":
            q_config, _ = self._read_config(config_path)
            q_config["q_name"] = request_q
            self._q_broker = RabbitPublisherAsync(**q_config)
                