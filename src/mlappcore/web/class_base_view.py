from fastapi_utils.cbv import cbv
from fastapi import APIRouter, FastAPI, UploadFile, File

router = APIRouter()
app = FastAPI()

@cbv(router)
class SimpleMLAppService:
    def __init__(self, 
                 db_path_or_url: str, 
                 object_db_path_or_url: str):
        self.sql_db = db_path_or_url
        self.object_db = object_db_path_or_url

    
    @router.post("/process")
    async def process(self, file: UploadFile = File(...)):
        pass 



    