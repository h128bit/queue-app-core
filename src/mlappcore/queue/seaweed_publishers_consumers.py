import os 
import uuid 
from mlappcore.dbs.object_dbs import SeaweedFSClient
from requests.exceptions import HTTPError


class SimpleSeaweedPublisherConsumer:
    def __init__(self,
                 object_db_path_or_url: str):

        self.object_db_client = SeaweedFSClient(filer_url=object_db_path_or_url)


    def _error_handler(self, err: HTTPError) -> int:
        code = int(str(err)[0:3])
        return code


    def push_object(self, 
                    object: bytes,
                    direct: str,
                    obj_name: str|None=None) -> dict:
        if obj_name is None:
            obj_name = str(uuid.uuid4())
        response: dict = {"uuid": None, "status": "ok"}

        try: 
            self.object_db_client.push(object, direct, obj_name)
            response["uuid"] = obj_name
        except HTTPError as err:
            response["status"] = self._error_handler(err)
                
        return response
    

    def check_object_and_return(self, 
                                object_uuid: str,
                                direct: str) -> dict:
        response: dict = {"object": None, "status": "ok"}

        try:
            content = self.object_db_client.pull(object_uuid, direct)
            response["object"] = content
            self.object_db_client.delete(object_uuid, direct)
        except HTTPError as err:
            response["status"] = self._error_handler(err)

        return response
