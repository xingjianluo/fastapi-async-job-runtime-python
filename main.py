"""FastAPI entrypoint.

The original ``GET /`` endpoint is unchanged. The async job runtime is
mounted under ``/jobs`` (see :mod:`job_runtime.api`).

Run locally::

    uv run uvicorn main:app
"""

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from job_runtime.api import install_exception_handlers, router as jobs_router
from job_runtime.config import RuntimeConfig
from job_runtime.log import configure_logging
from job_runtime.service import JobService


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging(logging.INFO)
    service = JobService(RuntimeConfig.from_env())
    await service.startup()
    app.state.job_service = service
    try:
        yield
    finally:
        if not service.store.closed:
            await service.shutdown()
            service.store.close()


app = FastAPI(title="Async Job Runtime", lifespan=lifespan)
app.include_router(jobs_router)
install_exception_handlers(app)


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
