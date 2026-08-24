import asyncio
import logging

from src import users_service
from src.bot import bot as main_bot
from src.config import CONFIG
from src.events import AbuseAlert, LlamaStatusEvent, RateAlert, alert_queue

logger = logging.getLogger("alert")
SEND_RETRIES = 3


async def send_new_ip(data: AbuseAlert):
    text = (
        f"⚠️ 新 IP 请求\n"
        f"用户: {data['name']}\n"
        f"新 IP: {data['ip']}\n"
        f"累计 IP 数: {data['count']}\n"
        f"时间: {data['time']}"
    )
    for uid in CONFIG.telegram.admins:
        try:
            await main_bot.send_message(uid, text)
        except Exception:
            logger.warning(f"发送告警给 {uid} 失败")


async def send_llama_status(data: LlamaStatusEvent):
    text = {
        "ready": "🚀 翻译服务已启动，可以开始使用了！",
        "stopped": "🛑 翻译服务已关闭",
    }.get(data["status"])
    if text is None:
        return
    try:
        tgids = [u.tgid for u in await users_service.list_users()]
    except Exception:
        logger.exception("获取用户列表失败，仅通知管理员")
        tgids = CONFIG.telegram.admins
    if not tgids:
        logger.warning("模型状态通知没有可发送的用户: status=%s", data["status"])
        return

    async def send_one(uid: int) -> bool:
        for attempt in range(1, SEND_RETRIES + 1):
            try:
                await main_bot.send_message(uid, text)
                return True
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt == SEND_RETRIES:
                    logger.exception(
                        "模型状态通知发送失败: uid=%s status=%s attempts=%s",
                        uid,
                        data["status"],
                        attempt,
                    )
                else:
                    logger.warning(
                        "模型状态通知发送失败，将重试: uid=%s status=%s attempt=%s",
                        uid,
                        data["status"],
                        attempt,
                    )
                    await asyncio.sleep(1)
        return False

    results = await asyncio.gather(*(send_one(uid) for uid in tgids))
    logger.info(
        "模型状态通知完成: status=%s recipients=%s sent=%s failed=%s",
        data["status"],
        len(tgids),
        sum(results),
        len(results) - sum(results),
    )


async def send_rate_alert(data: RateAlert):
    text = (
        f"⚠️ 请求频率预警\n"
        f"用户: {data['name']}\n"
        f"60 秒内生成 token: {data['tokens']}\n"
        f"时间: {data['time']}"
    )
    for uid in CONFIG.telegram.admins:
        try:
            await main_bot.send_message(uid, text)
        except Exception:
            logger.warning(f"发送频率预警给 {uid} 失败")


async def listen_alerts():
    while True:
        data = await asyncio.to_thread(alert_queue.get)
        if data is None:
            break
        try:
            if data["type"] == "abuse_alert":
                await send_new_ip(data)
            elif data["type"] == "llama_status":
                await send_llama_status(data)
            elif data["type"] == "rate_alert":
                await send_rate_alert(data)
            else:
                logger.warning("未知告警类型: %s", data.get("type"))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("处理告警失败: %s", data)
