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

        Args:
            filer_url (str): Filer host URL.
            root (str, optional): Root folder for storage files. Defaults to 'data/files'.
            ttl (str, optional): Time to live for files in storage. Defaults to '1d'.
            query_q (str, optional): Folder name for query input files. Defaults to 'query'.
            response_q (str, optional): Folder name for response files. Defaults to 'response'.
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

        """
        Build correct path for SeaweedFS.

        Constructs a file path based on the specified directory type and file name.
        The path is built by joining the appropriate base path (query or response)
        with the provided file name.

        Args:
            direct (str): Directory type identifier. 
            file_name (str): Name of the file to build the path for.

        Returns:
            str: Constructed full path string for the file in SeaweedFS.
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

        Args:
        direct (str): Folder name where the object will be saved.
            Must be either `query_q` or `response_q`.
        object (bytes): Object data in bytes format to be stored.
        file_name (str): Name of the file/object in the storage.

        Returns:
            dict: JSON response from SeaweedFS 
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

        Args:
        direct (str): Folder name from which to retrieve the object.
        file_name (str): Name of the file/object to retrieve.

        Returns:
            bytes: The retrieved object data in binary format.
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
        
        Args:
        direct (str): Folder name from which to delete the object.
        file_name (str): Name of the file/object to delete.

        Returns:
            int: HTTP status code of the deletion request. Typically 200 or 204 
        """

        location = self._get_location(direct, file_name)

        response = requests.delete(location)

        self._logger.info(f"The object {file_name} was deleted with status {response.status_code}")
        return response.status_code

