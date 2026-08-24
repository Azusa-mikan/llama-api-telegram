import logging
import re
from urllib.parse import urlsplit


_QUIET_PATHS = {"/status", "/health"}


def _is_quiet_request(record: logging.LogRecord) -> bool:
    if record.name == "httpx" and len(record.args) >= 2:
        method, url = record.args[0], record.args[1]
        return str(method) == "GET" and urlsplit(str(url)).path in _QUIET_PATHS

    message = record.getMessage()
    return bool(re.search(r'"(?:GET|HEAD) /(?:status|health)(?:\?| |HTTP/)', message))


class _QuietRequestFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not _is_quiet_request(record)


stream_handler = logging.StreamHandler()
stream_handler.addFilter(_QuietRequestFilter())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[stream_handler],
)

logging.captureWarnings(True)

class _ForwardToFastAPI(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        record.name = "fastapi"
        logging.getLogger("fastapi").handle(record)


for _name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    _logger = logging.getLogger(_name)
    _logger.handlers.clear()
    _logger.propagate = False
    _logger.addHandler(_ForwardToFastAPI())

tglog = logging.getLogger("bot")
apilog = logging.getLogger("api")
