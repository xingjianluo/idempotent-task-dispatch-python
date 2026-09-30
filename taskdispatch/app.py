"""FastAPI 装配层：对外提供投递、状态查询接口。

HTTP 判定映射：
    InvalidRequestError   -> 400 {"reason": "invalid_request", ...}
    KeyOccupiedError      -> 409 {"reason": "key_occupied", ...}
    PayloadConflictError  -> 409 {"reason": "payload_conflict", ...}
    幂等重放（已终态同内容） -> 200 {"duplicated": true, ...}，不重新执行
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .exceptions import (
    InvalidRequestError,
    KeyOccupiedError,
    PayloadConflictError,
)
from .models import TaskState
from .service import TaskService
from .store import TaskStore
from .worker import Handler, Worker

logger = logging.getLogger("taskdispatch.api")


class DispatchRequest(BaseModel):
    group: str = Field(..., description="消费分组名")
    key: str = Field(..., description="业务键；同组内唯一，作为幂等键")
    payload: Any = Field(..., description="任务内容，任意可 JSON 序列化对象")
    max_attempts: int | None = Field(None, ge=1, description="最大尝试次数，缺省用服务端配置")


def create_app(
    store: TaskStore | None = None,
    *,
    service: TaskService | None = None,
    worker: Worker | None = None,
) -> FastAPI:
    store = store or TaskStore()
    service = service or TaskService(store)

    app = FastAPI(
        title="idempotent-task-dispatch",
        version="0.1.0",
        description="本地任务投递与消费：幂等投递、分组顺序消费、退避重试与死信",
    )
    app.state.store = store
    app.state.service = service
    app.state.workers: dict[str, Worker] = {}
    if worker is not None:
        app.state.workers[worker.group] = worker

    @app.exception_handler(InvalidRequestError)
    async def _invalid(_request, exc: InvalidRequestError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"reason": "invalid_request", "detail": str(exc)},
        )

    @app.exception_handler(KeyOccupiedError)
    async def _occupied(_request, exc: KeyOccupiedError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={
                "reason": "key_occupied",
                "detail": str(exc),
                "group": exc.group,
                "key": exc.key,
            },
        )

    @app.exception_handler(PayloadConflictError)
    async def _conflict(_request, exc: PayloadConflictError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={
                "reason": "payload_conflict",
                "detail": str(exc),
                "group": exc.group,
                "key": exc.key,
            },
        )

    @app.post("/tasks")
    async def dispatch_task(req: DispatchRequest) -> JSONResponse:
        outcome = service.dispatch(
            req.group, req.key, req.payload, max_attempts=req.max_attempts
        )
        # 新建任务 201；幂等重放 200（未产生新资源，也没有重新执行）
        return JSONResponse(
            status_code=200 if outcome.duplicated else 201,
            content={
                "accepted": outcome.accepted,
                "duplicated": outcome.duplicated,
                "task_id": outcome.task_id,
                "state": outcome.state.value,
            },
        )

    @app.get("/groups/{group}")
    async def group_status(group: str) -> dict:
        counts = store.count_by_state(group)
        dead = store.list_dead(group)
        total = sum(counts.values())
        return {
            "group": group,
            "total": total,
            "counts": counts,
            "dead": [
                {
                    "task_id": t.id,
                    "key": t.key,
                    "attempts": t.attempts,
                    "last_error": t.last_error,
                }
                for t in dead
            ],
        }

    @app.get("/health")
    async def health() -> dict:
        counts = store.count_by_state()
        return {"status": "ok", "states": counts}

    return app


def build_default_app(
    db_path: str = ":memory:",
    handlers: dict[str, Handler] | None = None,
    *,
    start_workers: bool = False,
) -> FastAPI:
    """构造带处理函数注册表的应用；start_workers=True 时随进程启动消费者。"""
    store = TaskStore(db_path)
    service = TaskService(store)
    app = create_app(store, service=service)
    for group, handler in (handlers or {}).items():
        worker = Worker(store, group, handler)
        app.state.workers[group] = worker
        if start_workers:
            worker.start()
    return app
