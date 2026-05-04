from typing import Callable, Any

from mlappcore.services.base_service import BaseService


class SimpleHandler(BaseService):
    def __init__(self, 
                 config_path: str, 
                 method: Callable[[bytes], bytes], 
                 request_q: str="queue", 
                 response_q: str="response",):
        super().__init__(config_path, "handler", method=method, request_q=request_q, response_q=response_q)
        self.method = method


    def _on_message(self, object_id: str):
        response: dict = {"name": object_id, "status": "ok"}
        obj = self._obj_db_client.check_object_and_return(object_id, self._request_q)

        file_obj = obj["object"]

        try:
            res = self.method(file_obj)
            self._obj_db_client.push_object(res, self._response_q, object_id)
        except Exception as e:
            response["status"] = "500"
            # TODO: add error log


    def start_consume(self):
        self._q_broker.on_message_method = self._on_message        

        self._q_broker.start_consume()