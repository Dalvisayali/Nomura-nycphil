from fastapi import FastAPI

from db_utils import models, engine 
from routers import db_router
import uvicorn


models.Base.metadata.create_all(bind=engine)

app = FastAPI()


app.include_router(db_router.router)


@app.get("/")
async def root():
    return {"message": "Hello World"}

