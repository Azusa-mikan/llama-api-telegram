import logging
import os
from logging.handlers import RotatingFileHandler

from src.constant import PROJECT_ROOT

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 10
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT,
    encoding="utf-8",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        file_handler,
    ],
)

logging.captureWarnings(True)

if os.path.exists(file_handler.baseFilename) and os.path.getsize(file_handler.baseFilename) > 0:
    file_handler.doRollover()

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
