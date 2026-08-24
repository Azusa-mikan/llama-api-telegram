import hashlib
import secrets
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    JSON,
    String,
    select,
    update,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.constant import PROJECT_ROOT, TIME_FORMAT
from src.config import CONFIG

TZ = ZoneInfo("Asia/Shanghai")


def _db_url() -> str:
    db = CONFIG.database
    if db.type == "sqlite":
        path = Path(db.name)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{path}"
    if db.type == "mysql":
        return f"mysql+asyncmy://{quote_plus(db.user)}:{quote_plus(db.password)}@{db.host}:{db.port}/{db.name}"
    if db.type == "mariadb":
        return f"mariadb+asyncmy://{quote_plus(db.user)}:{quote_plus(db.password)}@{db.host}:{db.port}/{db.name}"
    return f"postgresql+asyncpg://{quote_plus(db.user)}:{quote_plus(db.password)}@{db.host}:{db.port}/{db.name}"


engine = create_async_engine(_db_url())
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tgid: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    api_key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    banned: Mapped[bool] = mapped_column(Boolean, default=False)
    usage_count: Mapped[int] = mapped_column(Integer, default=0)
    final_usage_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ip_addresses: Mapped[list] = mapped_column(JSON, default=list)


class Stats(Base):
    __tablename__ = "stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_requests: Mapped[int] = mapped_column(Integer, default=0)
    today_requests: Mapped[int] = mapped_column(Integer, default=0)
    stat_date: Mapped[str] = mapped_column(String(10))


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with async_session() as s:
        if await s.get(Stats, 1) is None:
            s.add(Stats(id=1, total_requests=0, today_requests=0, stat_date=_today()))
            await s.commit()


def _today() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def _new_key() -> str:
    return "sk-" + secrets.token_hex(32)


def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def _user_dict(row: User) -> dict:
    return {
        "tgid": row.tgid,
        "name": row.name,
        "banned": row.banned,
        "usage_count": row.usage_count,
        "final_usage_at": (
            row.final_usage_at.strftime(TIME_FORMAT)
            if row.final_usage_at
            else None
        ),
        "ip_addresses": list(row.ip_addresses),
    }


async def list_users() -> list[dict]:
    async with async_session() as s:
        result = await s.execute(select(User).order_by(User.id))
        return [_user_dict(r) for r in result.scalars().all()]


async def get_user_by_tgid(tgid: int) -> dict | None:
    async with async_session() as s:
        row = await s.scalar(select(User).where(User.tgid == tgid))
        return _user_dict(row) if row else None


async def get_user_by_key(api_key: str) -> dict | None:
    async with async_session() as s:
        row = await s.scalar(
            select(User).where(User.api_key == _hash_key(api_key))
        )
        return _user_dict(row) if row else None


async def add_user(tgid: int, name: str) -> dict:
    async with async_session() as s:
        row = await s.scalar(select(User).where(User.tgid == tgid))
        if row is None:
            raw_key = _new_key()
            row = User(tgid=tgid, name=name, api_key=_hash_key(raw_key))
            s.add(row)
            await s.commit()
            await s.refresh(row)
            return {**_user_dict(row), "api_key": raw_key}
        return _user_dict(row)


async def rotate_key(tgid: int) -> dict | None:
    async with async_session() as s:
        row = await s.scalar(select(User).where(User.tgid == tgid))
        if row is None:
            return None
        raw_key = _new_key()
        row.api_key = _hash_key(raw_key)
        await s.commit()
        await s.refresh(row)
        return {**_user_dict(row), "api_key": raw_key}


async def set_banned(tgid: int, banned: bool) -> dict | None:
    async with async_session() as s:
        row = await s.scalar(select(User).where(User.tgid == tgid))
        if row is None:
            return None
        row.banned = banned
        await s.commit()
        await s.refresh(row)
        return _user_dict(row)


async def flush_counters(data: dict) -> None:
    """把内存计数器的增量批量落盘（usage / 最后调用 / 新 IP / 统计）。"""
    usage = data["usage"]
    last_used = data["last_used"]
    new_ips = data["new_ips"]
    ips = data["ips"]
    total_delta = data["total_delta"]
    today_delta = data["today_delta"]
    date = data["date"]

    if not (usage or new_ips or total_delta or today_delta):
        return

    async with async_session() as s:
        for tgid, delta in usage.items():
            await s.execute(
                update(User)
                .where(User.tgid == tgid)
                .values(
                    usage_count=User.usage_count + delta,
                    final_usage_at=last_used.get(tgid),
                )
            )
        for tgid, ip_set in new_ips.items():
            await s.execute(
                update(User)
                .where(User.tgid == tgid)
                .values(ip_addresses=sorted(ips.get(tgid, ())))
            )

        stats = await s.get(Stats, 1)
        if stats is None:
            stats = Stats(id=1, total_requests=0, today_requests=0, stat_date=date)
            s.add(stats)
        if stats.stat_date != date:
            stats.stat_date = date
            stats.today_requests = 0
        stats.total_requests += total_delta
        stats.today_requests += today_delta
        await s.commit()


async def seed_counters() -> dict:
    """启动时把存量 IP 和统计载入内存计数器。"""
    async with async_session() as s:
        ips: dict[int, set[str]] = {}
        result = await s.execute(select(User.tgid, User.ip_addresses))
        for tgid, ip_list in result.all():
            ips[tgid] = set(ip_list or [])

        stats = await s.get(Stats, 1)
        return {
            "total": stats.total_requests if stats else 0,
            "today": stats.today_requests if stats else 0,
            "date": stats.stat_date if stats else "",
            "ips": ips,
        }
