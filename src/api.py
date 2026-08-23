import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from src import llama_service
from src.config import CONFIG
from src.constant import TIME_FORMAT
from src.counters import counters
from src.db import (
    TZ,
    flush_counters,
    get_user_by_key,
    init_db,
    seed_counters,
)
from src.events import alert_queue, publish_alert

FLUSH_INTERVAL = 5.0

_client: httpx.AsyncClient
_last_status: str | None = None


def _publish_status(status: Literal["ready", "stopped"]) -> None:
    global _last_status
    if status == _last_status:
        return
    _last_status = status
    publish_alert({"type": "llama_status", "status": status})


async def _notify_initial() -> None:
    await asyncio.sleep(5)
    st = await llama_service.status()
    if st.ready:
        _publish_status("ready")


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
        try:
            await flush_counters(counters.drain())
        except Exception:
            logging.getLogger("api").exception("计数落盘失败")


def require_admin(x_admin_key: str = Header(default="")) -> None:
    if x_admin_key != CONFIG.secret:
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


def _maybe_rate_alert(user: dict, tokens: int) -> None:
    if tokens <= 0:
        return
    logging.getLogger("api").info(f"TOKENS tgid={user['tgid']} tokens={tokens}")
    alerted, window_total = counters.record_tokens(
        user["tgid"], tokens, CONFIG.alert_token_limit
    )
    if alerted:
        publish_alert(
            {
                "type": "rate_alert",
                "tgid": user["tgid"],
                "name": user["name"],
                "tokens": window_total,
                "time": datetime.now(TZ).strftime(TIME_FORMAT),
            }
        )


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
        asyncio.create_task(_notify_initial()),
        flusher,
    ]
    yield
    alert_queue.put(None)
    for t in tasks:
        t.cancel()
    await flush_counters(counters.drain())
    await _client.aclose()


app = FastAPI(lifespan=lifespan)


@app.post("/v1/chat/completions")
async def chat_completions(
    body: dict,
    request: Request,
    user: dict = Depends(require_user),
):
    await _record_request(user, request)
    if body.get("stream"):
        return await _stream_completion(body, user)
    return await _plain_completion(body, user)


async def _plain_completion(body: dict, user: dict):
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
    data = resp.json()
    _maybe_rate_alert(user, data.get("usage", {}).get("completion_tokens", 0))
    return data


async def _stream_completion(body: dict, user: dict):
    try:
        req = _client.build_request(
            "POST", "/v1/chat/completions", json=body, headers=_remote_headers()
        )
        resp = await _client.send(req, stream=True)
    except httpx.HTTPError:
        return JSONResponse(
            status_code=503, content=_openai_error(503, "model not running")
        )
    if resp.status_code != 200:
        content = _try_json(resp) or _openai_error(resp.status_code, resp.text)
        await resp.aclose()
        return JSONResponse(status_code=resp.status_code, content=content)
    return StreamingResponse(
        _iter_stream(resp, user),
        media_type="text/event-stream",
    )


async def _iter_stream(resp: httpx.Response, user: dict):
    completion_tokens = 0
    try:
        async for line in resp.aiter_lines():
            yield (line + "\n").encode()
            if not line.startswith("data:") or line == "data: [DONE]":
                continue
            payload = line[5:].strip()
            if not payload:
                continue
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                continue
            choices = chunk.get("choices")
            if choices:
                delta = choices[0].get("delta", {})
                if delta.get("content"):
                    completion_tokens += 1
    finally:
        await resp.aclose()
    if completion_tokens:
        logging.getLogger("api").info(f"TOKENS tgid={user['tgid']} tokens={completion_tokens}")
    alerted, window_total = counters.record_tokens(
        user["tgid"], completion_tokens, CONFIG.alert_token_limit
    )
    if alerted:
        publish_alert(
            {
                "type": "rate_alert",
                "tgid": user["tgid"],
                "name": user["name"],
                "tokens": window_total,
                "time": datetime.now(TZ).strftime(TIME_FORMAT),
            }
        )


@app.get("/v1/models")
async def list_models():
    try:
        resp = await _client.get("/v1/models", headers=_remote_headers())
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="model not running")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@app.post("/admin/unload", dependencies=[Depends(require_admin)])
async def unload():
    await llama_service.stop()
    return {"status": "unloaded"}


@app.post("/admin/agent/event", dependencies=[Depends(require_admin)])
async def agent_event(body: dict):
    status = body.get("status")
    if status not in ("stopped", "ready"):
        raise HTTPException(status_code=400, detail="invalid status")
    _publish_status(status)
    return {"ok": True}
