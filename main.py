import uvicorn
import asyncio
from fastapi import FastAPI, responses
from app.api import StockData, StockIndicator, StockIniit
from app.task.DataScheduleTask import app as app_rocketry


app = FastAPI(default_response_class=responses.ORJSONResponse)
app.include_router(StockIndicator.router)
app.include_router(StockData.router)
app.include_router(StockIniit.router)


class Server(uvicorn.Server):
    """Customized uvicorn.Server

    Uvicorn server overrides signals and we need to include
    Rocketry to the signals."""

    def handle_exit(self, sig: int, frame) -> None:
        app_rocketry.session.shut_down()
        return super().handle_exit(sig, frame)


async def main():
    """Run scheduler and the API"""
    server = Server(config=uvicorn.Config(app, host="0.0.0.0", reload=True, workers=1, loop="asyncio"))
    api = asyncio.create_task(server.serve())
    sched = asyncio.create_task(app_rocketry.serve())
    await asyncio.wait([sched, api])


if __name__ == "__main__":
    asyncio.run(main())
