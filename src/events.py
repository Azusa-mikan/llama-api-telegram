import queue
from typing import Literal, TypedDict, Union


class AbuseAlert(TypedDict):
    type: Literal["abuse_alert"]
    name: str
    ip: str
    count: int
    time: str


class LlamaStatusEvent(TypedDict):
    type: Literal["llama_status"]
    status: Literal["ready", "stopped"]


class UsageRow(TypedDict):
    tgid: int
    name: str
    seconds: float


class UsageSummary(TypedDict):
    type: Literal["usage_summary"]
    date: str
    rows: list[UsageRow]


Event = Union[AbuseAlert, LlamaStatusEvent, UsageSummary]

alert_queue: queue.Queue[Event | None] = queue.Queue()


def publish_alert(payload: Event) -> None:
    alert_queue.put(payload)
