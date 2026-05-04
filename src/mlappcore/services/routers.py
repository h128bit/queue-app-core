from mlappcore.services.base_service import BaseService


class SimpleRouter(BaseService):
    def __init__(self, 
                 config_path: str, 
                 request_q: str="queue", 
                 response_q: str="response",):
        super().__init__(config_path, "router", request_q=request_q, response_q=response_q)

    
    def push(self, object: bytes) -> dict:
        response = self._obj_db_client.push_object(object, self._request_q)
        if response["status"] == "ok":
            uuid = response["uuid"]
            self._q_broker.publish(uuid)
        return response 
    

    def chek_object(self, object_id) -> dict:
        obj = self._obj_db_client.check_object_and_return(object_id, self._response_q)
            
        return obj