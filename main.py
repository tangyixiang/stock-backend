from fastapi import FastAPI
from app.api.cn_stock import router as cn_router
from app.api.analysis import router as analysis_router

app = FastAPI()

app.include_router(cn_router)
app.include_router(analysis_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
