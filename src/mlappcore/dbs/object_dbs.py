import requests
from yarl import URL



class SeaweedFSClient:
    def __init__(self, 
                 filer_url: str,
                 root: str="data/files",
                 ttl: str|None="1d"
                 ):
        """
        Client for interaction with SeaweedFS. 

        """

        self.filer_url = URL(filer_url)
        self.root = root
        self.base_location = self.filer_url / self.root
        self._query_path = self.base_location / "query"
        self._response_path = self.base_location / "response"
        self.ttl = ttl


    def _get_location(self, direct, file_name) -> str:
        match direct:
            case "query":
                location = str(self._query_path / file_name)
            case "response":
                location = str(self._response_path / file_name)
            case _:
                raise ValueError(f"Unknow direct type. Expected `query` or `response`, got {direct}")
        return location

    
    def push(self,
             object: bytes,
             direct: str, 
             file_name: str) -> dict:
        
        location = self._get_location(direct, file_name)

        params = {"ttl": self.ttl}
        files = {"file": (file_name, object, "application/octet-stream")}
        
        response = requests.post(location, files=files, params=params)
        response.raise_for_status()
        return response.json()
    

    def pull(self,
             direct: str,
             file_name: str) -> bytes:
        location = self._get_location(direct, file_name)

        response = requests.get(location)
        response.raise_for_status()
        return response.content
    

    def delete(self, 
               file_name: str, 
               direct: str) -> int:
        location = self._get_location(direct, file_name)

        response = requests.delete(location)
        return response.status_code

