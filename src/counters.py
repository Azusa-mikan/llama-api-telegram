import threading
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from src.constant import TIME_FORMAT

TZ = ZoneInfo("Asia/Shanghai")

REQUEST_WINDOW_10M = 10 * 60.0
REQUEST_WINDOW_1H = 60 * 60.0


class Counters:
    """内存用量计数：请求路径只碰这里，定期由 flush 落盘。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._usage: dict[int, int] = {}
        self._last_used: dict[int, datetime] = {}
        self._ips: dict[int, set[str]] = {}
        self._new_ips: dict[int, set[str]] = {}
        self._requests: dict[int, deque[float]] = {}
        self._total = 0
        self._today = 0
        self._flushed_total = 0
        self._flushed_today = 0
        self._today_date = self._date()

    @staticmethod
    def _date() -> str:
        return datetime.now(TZ).strftime("%Y-%m-%d")

    def seed(
        self,
        total: int,
        today: int,
        date: str,
        ips: dict[int, set[str]],
    ) -> None:
        with self._lock:
            self._total = total
            self._today = today
            self._flushed_total = total
            self._flushed_today = today
            self._today_date = date or self._date()
            self._ips = {tgid: set(ips.get(tgid, ())) for tgid in ips}

    def _rollover_locked(self) -> None:
        current_date = self._date()
        if current_date != self._today_date:
            self._today_date = current_date
            self._today = 0
            self._flushed_today = 0

    def record(self, tgid: int, ip: str) -> tuple[bool, int, str]:
        """纯内存记账。返回 (是否新 IP, 累计 IP 数, 本次时间串)。"""
        with self._lock:
            now = datetime.now(TZ)
            self._rollover_locked()
            self._usage[tgid] = self._usage.get(tgid, 0) + 1
            self._last_used[tgid] = now
            self._total += 1
            self._today += 1

            seen = self._ips.setdefault(tgid, set())
            is_new = ip not in seen
            if is_new:
                seen.add(ip)
                self._new_ips.setdefault(tgid, set()).add(ip)
            return is_new, len(seen), now.strftime(TIME_FORMAT)

    def snapshot(self) -> dict:
        with self._lock:
            self._rollover_locked()
            return {
                "total_requests": self._total,
                "today_requests": self._today,
            }

    def record_request(
        self, tgid: int, limit_10m: int, limit_1h: int
    ) -> tuple[bool, str | None, int]:
        """记录请求并返回 (是否告警, 窗口名称, 窗口请求数)。"""
        with self._lock:
            now = time.monotonic()
            q = self._requests.setdefault(tgid, deque())
            q.append(now)
            while q and now - q[0] > REQUEST_WINDOW_1H:
                q.popleft()

            count_10m = sum(
                1 for timestamp in q if now - timestamp <= REQUEST_WINDOW_10M
            )
            count_1h = len(q)
            if count_10m >= limit_10m:
                window, count = "10 分钟", count_10m
            elif count_1h >= limit_1h:
                window, count = "1 小时", count_1h
            else:
                window, count = None, 0
            return window is not None, window, count

    def drain(self) -> dict:
        """取走自上次落盘以来的增量并清零增量区。"""
        with self._lock:
            self._rollover_locked()
            data = {
                "usage": dict(self._usage),
                "last_used": dict(self._last_used),
                "new_ips": {tgid: set(values) for tgid, values in self._new_ips.items()},
                "ips": {tgid: set(values) for tgid, values in self._ips.items()},
                "total_delta": self._total - self._flushed_total,
                "today_delta": self._today - self._flushed_today,
                "date": self._today_date,
            }
            self._usage = {}
            self._last_used = {}
            self._new_ips = {}
            self._flushed_total = self._total
            self._flushed_today = self._today
            return data

    def restore(self, data: dict) -> None:
        """Put a failed persistence snapshot back into the pending deltas."""
        with self._lock:
            for tgid, delta in data["usage"].items():
                self._usage[tgid] = self._usage.get(tgid, 0) + delta
            for tgid, value in data["last_used"].items():
                current = self._last_used.get(tgid)
                if current is None or value > current:
                    self._last_used[tgid] = value
            for tgid, values in data["new_ips"].items():
                self._new_ips.setdefault(tgid, set()).update(values)
            self._flushed_total -= data["total_delta"]
            if data["date"] == self._today_date:
                self._flushed_today -= data["today_delta"]


counters = Counters()
