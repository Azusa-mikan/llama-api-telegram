from dataclasses import dataclass

from src import db


@dataclass
class UserView:
    tgid: int
    name: str
    banned: bool
    usage_count: int
    final_usage_at: str | None
    ip_addresses: list[str]


def _to_view(row: dict) -> UserView:
    return UserView(
        tgid=row["tgid"],
        name=row["name"],
        banned=row["banned"],
        usage_count=row["usage_count"],
        final_usage_at=row["final_usage_at"],
        ip_addresses=row["ip_addresses"],
    )


async def get_or_create(tgid: int, name: str) -> UserView:
    return _to_view(await db.add_user(tgid, name))


async def get(tgid: int) -> UserView | None:
    row = await db.get_user_by_tgid(tgid)
    return _to_view(row) if row else None


async def issue_api_key(tgid: int) -> str | None:
    row = await db.rotate_key(tgid)
    return row["api_key"] if row else None


async def list_users() -> list[UserView]:
    return [_to_view(row) for row in await db.list_users()]


async def ban(tgid: int) -> bool:
    return await db.set_banned(tgid, True) is not None


async def unban(tgid: int) -> bool:
    return await db.set_banned(tgid, False) is not None
