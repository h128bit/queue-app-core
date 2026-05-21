import logging
import requests
from yarl import URL



class SeaweedFSClient:
    def __init__(self, 
                 filer_url: str,
                 root: str="data/files",
                 ttl: str|None="1d",
                 query_q: str="query",
                 response_q: str="response"
                 ):
        """
        Client for interaction with SeaweedFS. 
        """

        self._logger = logging.getLogger(self.__class__.__name__)

        self.filer_url = URL(filer_url)
        self.root = root
        self.query_q = query_q
        self.response_q = response_q

        self.base_location = self.filer_url / self.root
        self._query_path = self.base_location / self.query_q
        self._response_path = self.base_location / self.response_q
        self.ttl = ttl


    def _get_location(self, direct, file_name) -> str:
        """
        Build correct path for seaweedfs 
        """

        match direct:
            case self.query_q:
                location = str(self._query_path / file_name)
            case self.response_q:
                location = str(self._response_path / file_name)
            case _:
                raise ValueError(f"Unknow direct type. Expected `{self.query_q}` or `{self.response_q}`, got {direct}")
        return location

    
    def push(self,
             direct: str,
             object: bytes, 
             file_name: str) -> dict:
        """
        Method for send object in db.
        params:
        direct: str -- folder name where will be saveing object
        object: bytes -- object for save in db
        file_name: str -- object name in db
        """

        self._logger.info("Push the object into SeaweedFS")

        location = self._get_location(direct, file_name)

        params = {"ttl": self.ttl}
        files = {"file": (file_name, object, "application/octet-stream")}
        
        response = requests.post(location, files=files, params=params)
        response.raise_for_status()
        return response.json()
    

    def pull(self,
             direct: str,
             file_name: str) -> bytes:
        """
        Method for getting object from db.
        After was got object his will be deleted from db.
        params:
        direct: str -- folder name from which need get object
        file_name: str -- object file name
        """

        self._logger.info("Pull the object from SeaweedFS")

        location = self._get_location(direct, file_name)

        response = requests.get(location)
        response.raise_for_status()
        return response.content
    

    def delete(self, 
               direct: str,
               file_name: str) -> int:
        """
        Method for deleting object from db
        params:
        direct: str -- folder name from which need get object
        file_name: str -- object file name
        """

        location = self._get_location(direct, file_name)

        response = requests.delete(location)

        self._logger.info(f"The object {file_name} was deleted with status {response.status_code}")
        return response.status_code

