import uuid 
import logging
from requests.exceptions import HTTPError

from queue_app_core.dbs.object_dbs import SeaweedFSClient


class SimpleSeaweedPublisherConsumer:
    """
    Class for storage files in queue via seaweedfs
    """
    def __init__(self,
                 object_db_path_or_url: str,
                 query_q: str="query",
                 response_q: str="response"):
        """
        Args:
            object_db_path_or_url (str): url to connect to seaweedfs filer
            query_q (str): folder name for storage query files. Defaults 'query'
            response_q (str): folder name for storage response files. Defaults 'response'
        """
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
        """
        Method for push object to storagte

        Args:
            object (bytes): object to storage
            direct (str): folder name for storage
            obj_name (str): object name in storage
        Returns:
            dict: dict in format {uuid: str, status: int}
        """

        if obj_name is None:
            obj_name = str(uuid.uuid4())
        response: dict = {"uuid": None, "status": 200}

        try: 
            self.object_db_client.push(direct=direct, object=object, file_name=obj_name)
            response["uuid"] = obj_name
        except HTTPError as err:
            response["status"] = self._error_handler(err)
        
        self._logger.info(f"Push object with status {response['status']}")

        return response
    

    def check_object_and_return(self, 
                                object_uuid: str,
                                direct: str) -> dict:
        """
        Method for checkong object in db by file id and return his if exists

        Args:
            object_uuid (str): file id 
            direct (str): Search folder in the storage
        Returns:
            dict: dict in format {object: none or bytes, status: int}
        """
        response: dict = {"object": None, "status": 200}

        try:
            content = self.object_db_client.pull(direct=direct, file_name=object_uuid)
            response["object"] = content
            self.object_db_client.delete(direct=direct, file_name=object_uuid)
            self._logger.info(f"The object was requested with status {response['status']}")
        except HTTPError as err:
            response["status"] = self._error_handler(err)

        return response
