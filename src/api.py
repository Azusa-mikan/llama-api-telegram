import asyncio
import hmac
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from src import llama_service
from src.config import CONFIG
from src.log import apilog
from src.constant import TIME_FORMAT
from src.counters import counters
from src.db import (
    TZ,
    flush_counters,
    get_daily_usage,
    get_user_by_key,
    init_db,
    seed_counters,
)
from src.events import UsageRow, alert_queue, publish_alert

FLUSH_INTERVAL = 5.0
STATUS_POLL_INTERVAL = 3.0
USAGE_SUMMARY_INTERVAL = 60 * 60.0

_client: httpx.AsyncClient
_last_status: str | None = None


def publish_status(
    status: Literal["ready", "stopped"], *, force: bool = False
) -> None:
    global _last_status
    if not force and status == _last_status:
        return
    _last_status = status
    publish_alert({"type": "llama_status", "status": status})


async def _monitor_llama_status() -> None:
    """补偿 agent 回调丢失，并只在确认控制端可达时发布 stopped。"""
    observed: Literal["ready", "stopped"] | None = None
    while True:
        try:
            st = await llama_service.status()
            if st.reachable:
                current: Literal["ready", "stopped"] = "ready" if st.ready else "stopped"
                if observed is None:
                    observed = current
                    # Preserve the old behavior: announce a running model on startup,
                    # but do not announce an already-stopped model as a new event.
                    if current == "ready":
                        publish_status(current)
                elif current != observed:
                    observed = current
                    publish_status(current)
        except asyncio.CancelledError:
            raise
        except Exception:
            apilog.exception("模型状态监视失败")
        await asyncio.sleep(STATUS_POLL_INTERVAL)


def _remote_headers() -> dict[str, str]:
    if CONFIG.llama_remote.api_key:
        return {"Authorization": f"Bearer {CONFIG.llama_remote.api_key}"}
    return {}


def _openai_error(status: int, message: str) -> dict:
    return {
        "error": {
            "message": message,
            "type": "invalid_request_error" if status == 400 else "server_error",
            "code": None,
        }
    }


def _try_json(resp: httpx.Response) -> dict | None:
    try:
        return resp.json()
    except Exception:
        return None


async def _flush_loop():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        data = counters.drain()
        try:
            await flush_counters(data)
        except asyncio.CancelledError:
            counters.restore(data)
            raise
        except Exception:
            counters.restore(data)
            apilog.exception("计数落盘失败")


async def _usage_summary_loop() -> None:
    while True:
        await asyncio.sleep(USAGE_SUMMARY_INTERVAL)
        date = counters.daily_usage_snapshot()
        raw_rows = await get_daily_usage(date)
        if not raw_rows:
            continue
        rows: list[UsageRow] = [
            {
                "tgid": int(row["tgid"]),
                "name": str(row["name"]),
                "seconds": float(row["seconds"]),
            }
            for row in raw_rows
        ]
        rows.sort(key=lambda row: row["seconds"], reverse=True)
        publish_alert({"type": "usage_summary", "date": date, "rows": rows})


def require_admin(x_admin_key: str = Header(default="")) -> None:
    if not CONFIG.secret or not hmac.compare_digest(x_admin_key, CONFIG.secret):
        raise HTTPException(status_code=401, detail="invalid admin key")


async def require_user(authorization: str = Header(default="")) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    user = await get_user_by_key(authorization.removeprefix("Bearer ").strip())
    if user is None:
        raise HTTPException(status_code=401, detail="invalid api key")
    if user["banned"]:
        raise HTTPException(status_code=403, detail="user banned")
    return user


async def _record_request(user: dict, request: Request) -> None:
    ip = request.client.host if request.client else "unknown"
    is_new, ip_count, event_time = counters.record(user["tgid"], ip)
    if is_new:
        publish_alert(
            {
                "type": "abuse_alert",
                "name": user["name"],
                "ip": ip,
                "count": ip_count,
                "time": event_time,
            }
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    await init_db()
    counters.seed(**await seed_counters())
    _client = httpx.AsyncClient(
        base_url=CONFIG.llama_remote.base_url,
        timeout=httpx.Timeout(600.0, connect=5.0),
    )
    from src.alert_listener import listen_alerts
    from src.bot import start_bot

    flusher = asyncio.create_task(_flush_loop())
    tasks = [
        asyncio.create_task(start_bot()),
        asyncio.create_task(listen_alerts()),
        asyncio.create_task(_monitor_llama_status()),
        asyncio.create_task(_usage_summary_loop()),
        flusher,
    ]
    yield
    alert_queue.put(None)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    data = counters.drain()
    try:
        await flush_counters(data)
    except Exception:
        counters.restore(data)
        apilog.exception("关停时计数落盘失败")
    await _client.aclose()
    await llama_service.close()


app = FastAPI(lifespan=lifespan)


@app.post("/v1/chat/completions")
async def chat_completions(
    body: dict,
    request: Request,
    user: dict = Depends(require_user),
):
    await _record_request(user, request)
    body = {"model": CONFIG.llama_remote.model, **body, **CONFIG.llama_remote.model_parameters}
    if body.get("stream"):
        return await _stream_completion(body, user)
    return await _plain_completion(body, user)


async def _plain_completion(body: dict, user: dict):
    started_at = counters.begin_request()
    try:
        return await _plain_completion_impl(body, user)
    finally:
        counters.finish_request(user["tgid"], started_at)


async def _plain_completion_impl(body: dict, user: dict):
    try:
        resp = await _client.post(
            "/v1/chat/completions", json=body, headers=_remote_headers()
        )
    except httpx.HTTPError:
        return JSONResponse(
            status_code=503, content=_openai_error(503, "model not running")
        )
    if resp.status_code != 200:
        return JSONResponse(
            status_code=resp.status_code,
            content=_try_json(resp) or _openai_error(resp.status_code, resp.text),
        )
    try:
        data = resp.json()
    except ValueError:
        return JSONResponse(
            status_code=502, content=_openai_error(502, "invalid response from model")
        )
    return data


async def _stream_completion(body: dict, user: dict):
    started_at = counters.begin_request()
    try:
        req = _client.build_request(
            "POST", "/v1/chat/completions", json=body, headers=_remote_headers()
        )
        resp = await _client.send(req, stream=True)
    except httpx.HTTPError:
        counters.finish_request(user["tgid"], started_at)
        return JSONResponse(
            status_code=503, content=_openai_error(503, "model not running")
        )
    if resp.status_code != 200:
        await resp.aread()
        content = _try_json(resp) or _openai_error(resp.status_code, resp.text)
        await resp.aclose()
        counters.finish_request(user["tgid"], started_at)
        return JSONResponse(status_code=resp.status_code, content=content)
    return StreamingResponse(
        _iter_stream(resp, user["tgid"], started_at),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no"},
    )


async def _iter_stream(resp: httpx.Response, tgid: int, started_at: float):
    try:
        async for line in resp.aiter_lines():
            yield (line + "\n").encode()
    finally:
        await resp.aclose()
        counters.finish_request(tgid, started_at)


@app.get("/v1/models")
async def list_models(_: dict = Depends(require_user)):
    try:
        resp = await _client.get("/v1/models", headers=_remote_headers())
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="model not running")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@app.post("/admin/agent/event", dependencies=[Depends(require_admin)])
async def agent_event(body: dict):
    status = body.get("status")
    if status not in ("stopped", "ready"):
        raise HTTPException(status_code=400, detail="invalid status")
    publish_status(status, force=body.get("force") is True)
    return {"ok": True}
