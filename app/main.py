"""Web application for the institutional-repository harvester.

Pages
-----
``/``                          catalogue browser, watermark, run controls
``/runs``                      round history (windows, attempts, restarts, errors)
``/duplicates``                duplicate candidate pairs and still-valid verdicts
``/status``                    statically rendered status JSON
``GET  /api/status``           polling endpoint used by the page
``GET  /api/catalog``          catalogue JSON (search + pagination)
``GET  /api/duplicates``       candidate pairs + valid verdicts as JSON
``POST /api/duplicates/judgements``  record one reviewer verdict (JSON)
``POST /duplicates/judgements``      same, from the HTML form
``POST /api/harvest``          start a round
``POST /api/harvest/cancel``   cancel the active round without waiting for it
``POST /api/harvest/restart``  cancel (if any) and immediately start a new round
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .harvest import HarvestConfig, Harvester
from .store import Store

BASE_DIR = Path(__file__).resolve().parent


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data"))


def _oai_url() -> str:
    return os.environ.get("OAI_URL", "http://demo-oai:8000/oai")


def create_app(
    *,
    db_path: str | Path | None = None,
    oai_url: str | None = None,
    read_timeout: float | None = None,
    max_attempts: int | None = None,
    client_factory: Any = None,
) -> FastAPI:
    data_dir = _data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(db_path or (data_dir / "harvester.sqlite3"))
    config = HarvestConfig(
        oai_url=oai_url or _oai_url(),
        read_timeout=float(
            read_timeout
            if read_timeout is not None
            else os.environ.get("OAI_READ_TIMEOUT", "20")
        ),
        connect_timeout=float(os.environ.get("OAI_CONNECT_TIMEOUT", "10")),
        max_attempts=int(
            max_attempts
            if max_attempts is not None
            else os.environ.get("HARVEST_MAX_ATTEMPTS", "3")
        ),
        retry_backoff_ms=int(os.environ.get("HARVEST_RETRY_BACKOFF_MS", "300")),
    )
    harvester = Harvester(store, config, client_factory=client_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # A fresh process owns no in-flight rounds; Store already marked any
        # 'running' rows from a previous process as failed without publishing.
        yield
        await harvester.shutdown()
        store.close()

    app = FastAPI(title="机构库采集控制台", lifespan=lifespan)
    app.state.store = store
    app.state.harvester = harvester
    app.state.config = config
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    static_dir = BASE_DIR / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def status_payload() -> dict[str, Any]:
        active = store.get_active_run()
        published = store.published_state()
        last = store.list_runs(limit=1)
        return {
            "active": active,
            "published": published,
            "last_run": last[0] if last else None,
            "config": {
                "oai_url": config.oai_url,
                "max_attempts": config.max_attempts,
                "read_timeout": config.read_timeout,
            },
        }

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        message: str | None = None,
    ) -> HTMLResponse:
        catalogue = store.catalog_page(q, page, page_size)
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "catalogue": catalogue,
                "published": store.published_state(),
                "status": status_payload(),
                "runs": store.list_runs(limit=10),
                "message": message,
            },
        )

    @app.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "runs": store.list_runs(limit=50),
                "published": store.published_state(),
                "status": status_payload(),
            },
        )

    @app.get("/api/status")
    async def api_status() -> JSONResponse:
        return JSONResponse(status_payload())

    @app.get("/duplicates", response_class=HTMLResponse)
    async def duplicates_page(
        request: Request,
        message: str | None = None,
        error: str | None = None,
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "duplicates.html",
            {
                "duplicates": store.duplicate_overview(),
                "published": store.published_state(),
                "status": status_payload(),
                "message": message,
                "error": error,
            },
        )

    @app.get("/api/duplicates")
    async def api_duplicates() -> JSONResponse:
        return JSONResponse(store.duplicate_overview())

    @app.post("/api/duplicates/judgements")
    async def api_record_judgement(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="请求体必须是 JSON") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
        try:
            left = str(payload["left_identifier"])
            right = str(payload["right_identifier"])
            left_token = str(payload["left_token"])
            right_token = str(payload["right_token"])
            verdict = str(payload["verdict"])
        except (KeyError, TypeError) as exc:
            raise HTTPException(
                status_code=400,
                detail="需要 left_identifier/right_identifier/left_token/"
                "right_token/verdict",
            ) from exc
        if verdict not in ("same", "different"):
            raise HTTPException(
                status_code=400, detail="verdict 只能是 same 或 different"
            )
        result = store.record_judgement(
            left, right, left_token, right_token, verdict
        )
        if not result["accepted"]:
            # A stale submission is a conflict: the page must show the current
            # candidates instead of silently applying an outdated verdict.
            return JSONResponse(
                {
                    "accepted": False,
                    "reason": result["reason"],
                    "duplicates": result["overview"],
                },
                status_code=409,
            )
        return JSONResponse(
            {
                "accepted": True,
                "judgement": result["judgement"],
                "duplicates": result["overview"],
            }
        )

    @app.post("/duplicates/judgements")
    async def form_record_judgement(
        left_identifier: str = Form(...),
        right_identifier: str = Form(...),
        left_token: str = Form(...),
        right_token: str = Form(...),
        verdict: str = Form(...),
    ) -> RedirectResponse:
        if verdict not in ("same", "different"):
            return RedirectResponse(
                url="/duplicates?error=" + quote("判定只能是同一成果或不同成果"),
                status_code=303,
            )
        result = store.record_judgement(
            left_identifier, right_identifier, left_token, right_token, verdict
        )
        if not result["accepted"]:
            return RedirectResponse(
                url="/duplicates?error=" + quote(result["reason"]),
                status_code=303,
            )
        message = "已记录判定：" + (
            "同一成果" if result["judgement"]["verdict"] == "same" else "不同成果"
        )
        return RedirectResponse(
            url="/duplicates?message=" + quote(message), status_code=303
        )

    @app.get("/status")
    async def status_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request, "status.html", {"status": status_payload()}
        )

    @app.post("/api/harvest")
    async def start_harvest() -> JSONResponse:
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.post("/api/harvest/cancel")
    async def cancel_harvest() -> JSONResponse:
        active = store.get_active_run()
        if active is None:
            raise HTTPException(status_code=409, detail="当前没有正在运行的采集轮次")
        await harvester.cancel_run(active["id"], "用户在网页取消", "user")
        return JSONResponse({"run_id": active["id"], "status": "cancelled"})

    @app.post("/api/harvest/restart")
    async def restart_harvest() -> JSONResponse:
        """Cancel the active round (if any) and immediately start a new one.

        ``cancel_run`` returns as soon as SQLite has recorded the cancellation
        and the old asyncio task has been asked to stop; it never waits for a
        slow HTTP page.  The new run gets a new ``run_id`` and its own window,
        and the commit ownership check makes the old round unable to publish.
        """
        active = store.get_active_run()
        if active is not None:
            await harvester.cancel_run(
                active["id"], "用户取消了本轮以开始新一轮", "superseded"
            )
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.get("/api/catalog")
    async def api_catalog(q: str = "", page: int = 1, page_size: int = 20) -> JSONResponse:
        return JSONResponse(store.catalog_page(q, page, page_size))

    return app


app = create_app()
