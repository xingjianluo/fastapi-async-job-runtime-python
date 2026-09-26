"""HTTP 接口层：提交、查询、取消、列表。保留既有 / 接口行为（定义在 main.py）。"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.errors import ErrorCode, JobError
from app.models import JobCancellationResponse, JobSubmitRequest, JobSubmitResponse

router = APIRouter(prefix="", tags=["jobs"])


def get_manager(request: Request):
    manager = getattr(request.app.state, "manager", None)
    if manager is None:
        raise RuntimeError("作业运行时尚未启动（lifespan 未执行）")
    return manager


@router.post("/jobs", response_model=JobSubmitResponse, status_code=201)
async def submit_job(body: JobSubmitRequest, request: Request):
    """提交作业。相同幂等键 + 相同载荷返回既有作业（deduplicated=true）。"""
    job, deduplicated = await get_manager(request).submit(body)
    return JobSubmitResponse(
        id=job.id, state=job.state, deduplicated=deduplicated
    )


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, request: Request):
    """查询作业状态与结果（终态结果持久化，重启后仍可查）。"""
    return (await get_manager(request).get(job_id)).to_public_dict()


@router.post("/jobs/{job_id}/cancel", response_model=JobCancellationResponse)
async def cancel_job(job_id: str, request: Request):
    """取消非终态作业；已终态或已取消返回 409 与可区分错误码。"""
    job = await get_manager(request).cancel(job_id)
    return JobCancellationResponse(id=job.id, state=job.state)


@router.get("/jobs")
async def list_jobs(request: Request, limit: int = 100):
    jobs = await get_manager(request).list_jobs(limit)
    return {"items": [j.to_public_dict() for j in jobs]}


async def job_error_handler(request, exc: JobError) -> JSONResponse:
    return JSONResponse(status_code=exc.http_status, content=exc.to_dict())


async def validation_error_handler(request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": ErrorCode.VALIDATION_ERROR.value,
                "message": "请求参数校验失败",
                "details": {"issues": exc.errors()},
            }
        },
    )
