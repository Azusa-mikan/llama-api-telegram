import hmac
import json
import logging
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException

BASE_DIR = Path(__file__).resolve().parent
CONFIG = json.loads((BASE_DIR / "config.json").read_text(encoding="utf-8"))
TOKEN = CONFIG["token"]
HOST = CONFIG.get("host", "0.0.0.0")
PORT = int(CONFIG.get("port", 9090))
CMD = CONFIG["cmd"]
HEALTH_URL = CONFIG["health_url"]
NOTIFY_URL = CONFIG.get("notify_url", "")
NOTIFY_TOKEN = CONFIG.get("notify_token", "")

logger = logging.getLogger("llama-server")


_QUIET_PATHS = {"/status", "/health"}


class _QuietRequestFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if record.name == "httpx" and isinstance(args, tuple) and len(args) >= 2:
            method, url = args[0], args[1]
            return not (
                str(method) == "GET"
                and urlsplit(str(url)).path in _QUIET_PATHS
            )
        message = record.getMessage()
        return '"GET /status HTTP/' not in message and '"GET /health HTTP/' not in message


stream_handler = logging.StreamHandler()
stream_handler.addFilter(_QuietRequestFilter())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[stream_handler],
)

_job: int | None = None

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9

    class _BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class _ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimitInformation),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    _kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

    def _new_job() -> int:
        job = _kernel32.CreateJobObjectW(None, None)
        if not job:
            return 0
        info = _ExtendedLimitInformation()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not _kernel32.SetInformationJobObject(
            job,
            JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(info),
            ctypes.sizeof(info),
        ):
            _kernel32.CloseHandle(job)
            return 0
        return job

    _job = _new_job()

    def _assign_to_job(proc: subprocess.Popen) -> None:
        if _job and not _kernel32.AssignProcessToJobObject(
            _job, wintypes.HANDLE(proc._handle)
        ):
            logger.warning("AssignProcessToJobObject failed")

else:
    def _assign_to_job(proc: subprocess.Popen) -> None:
        pass


app = FastAPI()
_proc: subprocess.Popen | None = None
_proc_lock = threading.Lock()


def _healthy() -> bool:
    try:
        resp = httpx.get(HEALTH_URL, timeout=1.0)
        return resp.status_code == 200
    except Exception:
        return False


def _running() -> bool:
    return _proc is not None and _proc.poll() is None


def _log_lines(stream):
    for line in iter(stream.readline, b""):
        logger.info(line.decode("utf-8", errors="replace").rstrip())


def _auth(x_token: str = Header(default="")) -> None:
    if not TOKEN or not hmac.compare_digest(x_token, TOKEN):
        raise HTTPException(status_code=401)


def _notify(status: str, *, force: bool = False) -> bool:
    if not NOTIFY_URL:
        return False
    try:
        resp = httpx.post(
            NOTIFY_URL,
            json={"status": status, "force": force},
            headers={"X-Admin-Key": NOTIFY_TOKEN},
            timeout=5.0,
        )
        return 200 <= resp.status_code < 300
    except Exception:
        logger.warning("notify %s failed", status)
        return False


def _notify_ready(force: bool = False) -> None:
    if not NOTIFY_URL:
        return
    for _ in range(60):
        if _healthy() and _notify("ready", force=force):
            return
        time.sleep(1)


@app.get("/status", dependencies=[Depends(_auth)])
def status():
    healthy = _healthy()
    return {"running": _running() or healthy, "ready": healthy}


@app.post("/start", dependencies=[Depends(_auth)])
def start():
    global _proc
    with _proc_lock:
        already = _running() or _healthy()
        if not already:
            _proc = subprocess.Popen(
                CMD,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            _assign_to_job(_proc)
            threading.Thread(target=_log_lines, args=(_proc.stdout,), daemon=True).start()
            threading.Thread(
                target=_notify_ready, args=(False,), daemon=True
            ).start()
    return {"status": "running" if already else "starting"}


@app.post("/stop", dependencies=[Depends(_auth)])
def stop():
    global _proc
    with _proc_lock:
        proc = _proc
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            _proc = None
        stopped = not _healthy()
    if stopped:
        if not NOTIFY_URL:
            return {"status": "stopped"}
        for _ in range(3):
            if _notify("stopped"):
                break
            time.sleep(1)
    return {"status": "stopped" if stopped else "running"}


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_config=None)
