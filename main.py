from fastapi import FastAPI
from app.api.cn_stock import router as cn_router

app = FastAPI()

app.include_router(cn_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
