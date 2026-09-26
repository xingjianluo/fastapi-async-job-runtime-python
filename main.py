"""应用入口：装配存储、运行时与 HTTP 接口。

保留既有 GET / 返回 {"Hello": "World"} 的行为。
本地运行：uv run uvicorn main:app 或 uv run python main.py
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from app.api import job_error_handler, validation_error_handler
from app.api import router as job_router
from app.config import Settings
from app.errors import JobError
from app.handlers import default_handlers
from app.manager import JobManager
from app.storage import SqliteJobStorage


def create_app(settings: Settings | None = None) -> FastAPI:
    """构造应用并装配作业运行时。"""
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        storage = SqliteJobStorage(settings.db_path)
        manager = JobManager(storage, default_handlers(), settings)
        await manager.start()  # 内含建表与 RUNNING->PENDING 重启恢复
        application.state.storage = storage
        application.state.manager = manager
        try:
            yield
        finally:
            await manager.shutdown()
            storage.close()

    application = FastAPI(title="Async Job Runtime", lifespan=lifespan)

    @application.get("/")
    def read_root():
        # 既有对外接口行为保持不变。
        return {"Hello": "World"}

    application.include_router(job_router)
    application.add_exception_handler(JobError, job_error_handler)
    application.add_exception_handler(RequestValidationError, validation_error_handler)
    application.state.settings = settings
    return application


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
