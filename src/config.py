import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel

from src.constant import PROJECT_ROOT


class TelergamConfig(BaseModel):
    bot_token: str
    admins: list[int]


class LlamaRemoteConfig(BaseModel):
    base_url: str
    model: str
    api_key: str = ""
    control_url: str = ""
    control_token: str = ""


class DatabaseConfig(BaseModel):
    type: Literal["sqlite", "mysql", "mariadb", "postgresql"]
    host: str
    port: int
    name: str
    user: str = ""
    password: str = ""


class FinalConfig(BaseModel):
    bind: str
    port: int
    secret: str
    alert_token_limit: int = 5000
    database: DatabaseConfig
    telegram: TelergamConfig
    llama_remote: LlamaRemoteConfig

    @classmethod
    def load(cls, path: str | Path | None = None) -> "FinalConfig":
        config_file = Path(path) if path else Path(
            os.environ.get("CONFIG_FILE", PROJECT_ROOT / "config.yaml")
        )
        data = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}

        telegram = data.setdefault("telegram", {})
        telegram["bot_token"] = os.environ.get(
            "TELEGRAM_BOT_TOKEN", telegram.get("bot_token", "")
        )
        data["secret"] = os.environ.get("ADMIN_KEY", data.get("secret", ""))
        return cls.model_validate(data)


CONFIG = FinalConfig.load()
