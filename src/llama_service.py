from dataclasses import dataclass

import httpx

from src.config import CONFIG
from src.counters import counters


@dataclass
class LlamaStatus:
    ready: bool
    loading: bool
    total_requests: int
    today_requests: int


_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=CONFIG.llama_remote.control_url,
            timeout=httpx.Timeout(10.0, connect=3.0),
        )
    return _client


async def _call(method: str, path: str) -> dict:
    resp = await _get_client().request(
        method, path, headers={"X-Token": CONFIG.llama_remote.control_token}
    )
    if resp.status_code != 200:
        raise RuntimeError(f"agent {method} {path}: HTTP {resp.status_code}")
    return resp.json()


async def status() -> LlamaStatus:
    try:
        data = await _call("GET", "/status")
        running = bool(data.get("running"))
        ready = bool(data.get("ready"))
    except Exception:
        running = ready = False
    snapshot = counters.snapshot()
    return LlamaStatus(
        ready=ready,
        loading=running and not ready,
        total_requests=snapshot["total_requests"],
        today_requests=snapshot["today_requests"],
    )


async def start() -> str:
    data = await _call("POST", "/start")
    return data.get("status", "starting")


async def stop() -> str:
    data = await _call("POST", "/stop")
    return data.get("status", "stopped")
