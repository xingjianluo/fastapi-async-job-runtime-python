"""FastAPI router exposing the job runtime.

Endpoints
---------
* ``POST   /jobs``                 submit (idempotent with ``Idempotency-Key``)
* ``GET    /jobs/{job_id}``        query status / result
* ``POST   /jobs/{job_id}/cancel`` cancel a non-terminal job

Distinguishable error responses share the JSON shape
``{"error": {"code", "message", "job_id?"}}``:

* ``IDEMPOTENCY_PAYLOAD_CONFLICT`` (409) same key, different payload
* ``TERMINAL_STATE_CONFLICT``      (409) re-submit/cancel of a terminal job
* ``JOB_NOT_FOUND``                (404)
* ``UNKNOWN_JOB_TYPE``             (422) unregistered job type
* ``QUEUE_FULL``                   (503) bounded queue saturated
"""

from __future__ import annotations

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from .models import (
    APIError,
    JobSubmitRequest,
    JobSubmitResponse,
    JobView,
)
from .service import JobService, QueueFullError

router = APIRouter(prefix="/jobs", tags=["jobs"])


def get_service(request: Request) -> JobService:
    return request.app.state.job_service  # type: ignore[no-any-return]


def _job_view(job) -> JobView:
    return JobView(**job.to_api_dict())


@router.post("", response_model=JobSubmitResponse, status_code=201)
async def submit_job(
    body: JobSubmitRequest,
    request: Request,
    idempotency_key: str | None = Header(default=None,
                                         alias="Idempotency-Key",
                                         max_length=200),
) -> JobSubmitResponse:
    service = get_service(request)
    key = body.idempotency_key or idempotency_key
    job, reused = await service.submit(
        body.job_type,
        body.payload,
        idempotency_key=key,
        max_attempts=body.max_attempts,
    )
    return JobSubmitResponse(job=_job_view(job), reused=reused)


@router.get("/{job_id}", response_model=JobView)
async def get_job(job_id: str, request: Request) -> JobView:
    service = get_service(request)
    job = await service.get(job_id)
    return _job_view(job)


@router.post("/{job_id}/cancel", response_model=JobView)
async def cancel_job(job_id: str, request: Request) -> JobView:
    service = get_service(request)
    job = await service.cancel(job_id)
    return _job_view(job)


def install_exception_handlers(app) -> None:
    @app.exception_handler(APIError)
    async def _handle_api_error(_request, exc: APIError):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                }
            },
        )

    @app.exception_handler(QueueFullError)
    async def _handle_queue_full(_request, exc: QueueFullError):
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "code": "QUEUE_FULL",
                    "message": str(exc),
                }
            },
        )
