import uvicorn

from src.api import app
from src.config import CONFIG
import src.log

if __name__ == "__main__":
    uvicorn.run(app, host=CONFIG.bind, port=CONFIG.port, log_config=None)
