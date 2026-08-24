import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler()],
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
