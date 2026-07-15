import asyncio

from queue_app_core.services.base_service import BaseService, BaseServiceAsync


class SimpleRouter(BaseService):
    def __init__(self, 
                 config_path: str, 
                 request_q: str="queue", 
                 response_q: str="response",):
        """
        Simple router service for publishing objects and checking results.

        Extends BaseService as a publisher that stores objects in SeaweedFS,
        publishes their UUIDs to RabbitMQ, and retrieves processed results.

        Args:
            config_path (str): Path to the configuration file containing queue and database settings.
            request_q (str, optional): Name of the request queue for publishing object UUIDs.
                Defaults to 'queue'.
            response_q (str, optional): Name of the response queue for retrieving processed objects.
                Defaults to 'response'.

        Raises:
            ValueError: If configuration file is invalid or missing required parameters.
            FileNotFoundError: If the configuration file does not exist.
            ConnectionError: If unable to connect to RabbitMQ or SeaweedFS services.

        Example:
            >>> router = SimpleRouter('config.json', request_q='tasks', response_q='results')
            >>> response = router.push(b'Hello World')
            >>> print(response)
            {'status': 200, 'uuid': '12345'}
            >>> 
            >>> result = router.chek_object('12345')
            >>> print(result)
            {'status': 200, 'data': b'PROCESSED'}
        """
        super().__init__(config_path, "router", request_q=request_q, response_q=response_q)

    
    def push(self, object: bytes) -> dict:
        """
        Store an object in SeaweedFS and publish its UUID to RabbitMQ.

        Args:
            object (bytes): Object data to be stored.

        Returns:
            dict: Response containing status and UUID if successful.
                Example: {'status': 200, 'uuid': '12345'}

        Raises:
            ConnectionError: If unable to connect to SeaweedFS or RabbitMQ.
            Exception: If publish operation fails.
        """

        response = self._obj_db_client.push_object(object, self._request_q)
        if response["status"] == 200:
            uuid = response["uuid"]
            try:
                self._q_broker.publish(uuid)
            except Exception as e:
                response["status"] = 500
        return response 
    

    def chek_object(self, object_id) -> dict:
        """
        Retrieve a processed object from SeaweedFS by its UUID.

        Args:
            object_id (str): UUID of the object to retrieve.

        Returns:
            dict: Response containing status and object data if found.
                Example: {'status': 200, 'data': b'processed'}

        Raises:
            ValueError: If object_id is invalid or empty.
            FileNotFoundError: If the object does not exist in storage.
        """

        obj = self._obj_db_client.check_object_and_return(object_id, self._response_q)
            
        return obj
    

class AsyncRouter(BaseServiceAsync):
    def __init__(self, 
                 config_path: str, 
                 request_q: str="queue", 
                 response_q: str="response",):
        """
        Asynchronous router service for publishing objects and checking results.

        Extends BaseServiceAsync as an async publisher that stores objects in SeaweedFS,
        publishes their UUIDs to RabbitMQ asynchronously, and retrieves processed results
        using non-blocking operations.

        Args:
            config_path (str): Path to the configuration file containing queue and database settings.
            request_q (str, optional): Name of the request queue for publishing object UUIDs.
                Defaults to 'queue'.
            response_q (str, optional): Name of the response queue for retrieving processed objects.
                Defaults to 'response'.

        Raises:
            ValueError: If configuration file is invalid or missing required parameters.
            FileNotFoundError: If the configuration file does not exist.
            ConnectionError: If unable to connect to RabbitMQ or SeaweedFS services.

        Example:
            >>> router = AsyncRouter('config.json', request_q='tasks', response_q='results')
            >>> response = await router.push(b'Hello World')
            >>> print(response)
            {'status': 200, 'uuid': '12345'}
            >>> 
            >>> result = await router.chek_object('12345')
            >>> print(result)
            {'status': 200, 'data': b'PROCESSED'}
        """
        super().__init__(config_path, "router", request_q=request_q, response_q=response_q)

        
    async def push(self, object: bytes) -> dict:
        """
        Asynchronously store an object in SeaweedFS and publish its UUID to RabbitMQ.

        Args:
            object (bytes): Object data to be stored.

        Returns:
            dict: Response containing status and UUID if successful.
                Example: {'status': 200, 'uuid': '12345'}

        Raises:
            ConnectionError: If unable to connect to SeaweedFS or RabbitMQ.
            Exception: If publish operation fails.
        """
        response = await asyncio.to_thread(self._obj_db_client.push_object, object, self._request_q) 
        if response["status"] == 200:
            uuid = response["uuid"]
            try:
                await self._q_broker.publish(uuid)
            except Exception as e:
                response["status"] = 500
        return response 
    

    async def chek_object(self, object_id) -> dict:
        """
        Asynchronously retrieve a processed object from SeaweedFS by its UUID.

        Args:
            object_id (str): UUID of the object to retrieve.

        Returns:
            dict: Response containing status and object data if found.
                Example: {'status': 200, 'data': b'processed'}

        Raises:
            ValueError: If object_id is invalid or empty.
            FileNotFoundError: If the object does not exist in storage.
        """
        obj = await asyncio.to_thread(self._obj_db_client.check_object_and_return, object_id, self._response_q)
            
        return obj