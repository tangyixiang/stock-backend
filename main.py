from fastapi import FastAPI
from app.api.cn_stock import router as cn_router

app = FastAPI()

app.include_router(cn_router)

# from apscheduler.schedulers.blocking import BlockingScheduler
# def job():
#     print("I'm working...")
# scheduler = BlockingScheduler()
# # 每隔 1 分钟执行一次 job 函数
# scheduler.add_job(job, "interval",  seconds=1)
# # 启动调度器
# scheduler.start()


@app.get("/")
async def root():
    return {"message": "Hello World"}
