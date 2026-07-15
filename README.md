# Queue app core

---

## Template for quickly embed queue in you app.

Supported work with RabbitMQ and SeaweedFS

### Usage example

#### Create queue modules factory

```python 
from queue_app_core.services.routers import SimpleRouter, AsyncRouter
from queue_app_core.services.handlers import SimpleHandler


class ServicesFactory:
    def __init__(self, config_path):
        status = load_dotenv(config_path)

        self.QUEUE_APP_CORE_CONFIG_PATH = os.environ["QUEUE_APP_CORE"]
        self.Q_IN = os.environ["Q_QUERY_NAME"]
        self.Q_OUT = os.environ["Q_RESPONSE_NAME"]

        self.base_service_config = {
            "config_path": self.ML_APP_CORE_CONFIG_PATH,
            "request_q": self.Q_IN,
            "response_q": self.Q_OUT
        }
    
    # Router is the module for pushing messages to queue and transmission their to user from queue
    def get_router(self):
        router = SimpleRouter(**self.base_service_config)
        return router
    
    def get_async_router(self):
        router = AsyncRouter(**self.base_service_config)
        return router
    
    # Consumer is the module for transmission message from queue to process unit and to back
    def create_and_run_consumer(self):
        
        process_unit = ... # return process result as string

        def connector(file: bytes) -> bytes:
            result = process_unit.process(file)
            result = json.dumps(result, ensure_ascii=False).encode('utf-8')
            return result

        handler = SimpleHandler(method=connector, **self.base_service_config)

        handler.start_consume()
```

#### Embed queue in app

```python
import json 
from multiprocessing import Process

import uvicorn
from dotenv import dotenv_values
from fastapi import FastAPI, UploadFile, File

from app.factories import ServicesFactory
from app.loglog import configurate_logger

# Create factory for queue modules
factory = ServicesFactory("configs/config.env")

# Create router 
router = factory.get_async_router()


# Create FastAPI app
fastapi_app = FastAPI(
    title="My app with queue",
    version="1.1.0")


@fastapi_app.post("/process")
async def process_file(file: UploadFile=File(...)):
    content = await file.read()

    # Push messenge to queue
    file_id = await router.push(content)
    return file_id


@fastapi_app.post("/request_result")
async def return_result(req: RequestForm):
    file_id = req.uuid

    # Check process result in reponse queue
    result = await router.chek_object(file_id)
    if result["object"]:
        result["object"] = json.loads(result["object"].decode("utf-8") )

    return result


if __name__ == "__main__":

    # Start consumer 
    consume_process = Process(target=factory.create_and_run_consumer)
    consume_process.start()
    
    # start fastapi app
    uvicorn.run(fastapi_app, 
                host="0.0.0.0", 
                port=1234)
```
