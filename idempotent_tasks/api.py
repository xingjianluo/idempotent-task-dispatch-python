"""FastAPI application exposing the local task dispatch flow.

Rejection mapping (each body also carries a machine-readable ``reason``):

* invalid_request -> 400
* key_occupied    -> 409 (an in-flight task owns the key)
* content_conflict -> 422 (a terminal task owns the key with other content)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from .dispatcher import Dispatcher
from .errors import (
    ContentConflictError,
    DispatchError,
    InvalidRequestError,
    KeyOccupiedError,
)
from .models import TaskRecord
from .store import TaskStore


class DispatchRequest(BaseModel):
    business_key: str
    group_name: str = "default"
    payload: str = ""


def _record_dict(t: TaskRecord) -> dict:
    return {
        "task_id": t.id,
        "business_key": t.business_key,
        "group": t.group_name,
        "state": t.state.value,
        "attempts": t.attempts,
        "visible_after": t.visible_after,
        "last_error": t.last_error,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    }


def create_app(
    store: TaskStore,
    dispatcher: Dispatcher,
    *,
    logger: logging.Logger | None = None,
) -> FastAPI:
    log = logger or logging.getLogger("idempotent_tasks")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        dispatcher.start()
        try:
            yield
        finally:
            dispatcher.shutdown()

    app = FastAPI(title="Idempotent Local Task Dispatch", lifespan=lifespan)

    def reject(exc: DispatchError, status_code: int) -> HTTPException:
        detail: dict = {"reason": exc.reason.value,
                        "message": str(exc)}
        if hasattr(exc, "existing_task_id"):
            detail["existing_task_id"] = exc.existing_task_id
        return HTTPException(status_code=status_code, detail=detail)

    @app.post("/tasks")
    def submit_task(req: DispatchRequest, response: Response):
        try:
            result = dispatcher.dispatch(
                req.business_key,
                group_name=req.group_name,
                payload=req.payload,
            )
        except InvalidRequestError as exc:
            raise reject(exc, 400)
        except KeyOccupiedError as exc:
            raise reject(exc, 409)
        except ContentConflictError as exc:
            raise reject(exc, 422)
        response.status_code = 201 if result.accepted else 200
        return {
            "accepted": result.accepted,
            "reason": result.reason,
            "task_id": result.task_id,
            "state": result.state.value,
        }

    @app.get("/tasks/{business_key}")
    def get_task(business_key: str):
        task = store.find_by_key(business_key)
        if task is None:
            raise HTTPException(404, detail={"reason": "not_found",
                                             "message": "no task for key"})
        return _record_dict(task)

    @app.get("/counters/summary")
    def counters():
        c = store.counters()
        return {
            "pending": c.pending,
            "processing": c.processing,
            "succeeded": c.succeeded,
            "dead": c.dead,
            "total": c.total,
        }

    @app.get("/dead-letters")
    def dead_letters():
        return [_record_dict(t) for t in store.list_dead_letters()]

    @app.get("/")
    def root():
        return {"service": "idempotent-task-dispatch"}

    return app
