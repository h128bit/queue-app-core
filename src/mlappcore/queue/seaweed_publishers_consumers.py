import uuid 
import logging
from requests.exceptions import HTTPError

from mlappcore.dbs.object_dbs import SeaweedFSClient


class SimpleSeaweedPublisherConsumer:
    def __init__(self,
                 object_db_path_or_url: str,
                 query_q: str="query",
                 response_q: str="response"):
        self._logger = logging.getLogger(self.__class__.__name__)

        self._logger.info("SeaweedFS-Publisher-Consumer created")
        self.object_db_client = SeaweedFSClient(filer_url=object_db_path_or_url, query_q=query_q, response_q=response_q)


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
            self.object_db_client.push(direct=direct, object=object, file_name=obj_name)
            response["uuid"] = obj_name
        except HTTPError as err:
            response["status"] = self._error_handler(err)
        
        self._logger.info(f"Push object with status {response["status"]}")

        return response
    

    def check_object_and_return(self, 
                                object_uuid: str,
                                direct: str) -> dict:
        response: dict = {"object": None, "status": "ok"}

        try:
            content = self.object_db_client.pull(direct=direct, file_name=object_uuid)
            response["object"] = content
            self.object_db_client.delete(direct=direct, file_name=object_uuid)
        except HTTPError as err:
            response["status"] = self._error_handler(err)

        self._logger.info(f"The object was requested with status {response["status"]}")

        return response
