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
        config = dotenv_values(config_path)

        q_url = config["QUEUE_URL"]
        q_user = config["QUEUE_USER"]
        q_pass = config["QUEUE_PASSWORD"]

        obj_url = config["OBJECT_DB_PATH"]

        self._request_q = request_q
        self._response_q = response_q

        match handler_type:
            case "router":
                self._q_broker = RabbitPublisher(q_url, q_user, q_pass, request_q)
            case "handler":
                self._q_broker = RabbitConsumer(q_url, q_user, q_pass, method, request_q)
            case _:
                raise ValueError(f"Unsupported handler type, expected `router` or `handler`, got {handler_type}")
            
        self._obj_db_client = SimpleSeaweedPublisherConsumer(obj_url, 
                                                             query_q=request_q, 
                                                             response_q=response_q)

        # self._orm = ResponseResultTableORM()