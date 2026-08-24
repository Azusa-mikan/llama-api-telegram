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


class RateAlert(TypedDict):
    type: Literal["rate_alert"]
    tgid: int
    name: str
    window: str
    requests: int
    time: str


Event = Union[AbuseAlert, LlamaStatusEvent, RateAlert]

alert_queue: queue.Queue[Event | None] = queue.Queue()


def publish_alert(payload: Event) -> None:
    alert_queue.put(payload)
