import asyncio
from html import escape

from telebot import types
from telebot.async_telebot import AsyncTeleBot

from src import llama_service, users_service
from src.api import publish_status
from src.config import CONFIG
from src.log import tglog
from src.constant import PROJECT_ROOT

bot = AsyncTeleBot(CONFIG.telegram.bot_token)
disclaimer_file = PROJECT_ROOT / "Disclaimer.md"
ADMIN_IDS = CONFIG.telegram.admins


async def setup_commands():
    commands = [
        types.BotCommand("key", "获取或查看 API Key"),
        types.BotCommand("start_llama", "启动翻译引擎"),
        types.BotCommand("stop_llama", "停止翻译引擎"),
        types.BotCommand("status", "查看引擎状态"),
        types.BotCommand("usage", "查看调用统计"),
        types.BotCommand("ban", "封禁用户"),
        types.BotCommand("unban", "解封用户"),
        types.BotCommand("users", "用户列表"),
    ]
    await bot.set_my_commands(commands)
    tglog.info("命令菜单已注册")


async def start_bot():
    await setup_commands()
    await bot.infinity_polling()


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def _uid(message: types.Message) -> int | None:
    return message.from_user.id if message.from_user else None


def _text(message: types.Message) -> str:
    return message.text or ""


async def _ensure_user(message: types.Message) -> None:
    user = message.from_user
    if user is None:
        return
    try:
        await users_service.get_or_create(
            user.id, user.full_name or user.username or str(user.id)
        )
    except Exception:
        tglog.exception("注册用户失败")


async def _get_user(telegram_id: int) -> users_service.UserView | None:
    try:
        return await users_service.get(telegram_id)
    except Exception:
        return None


@bot.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await _ensure_user(message)
    uid = _uid(message)
    if uid is None:
        return
    u = await _get_user(uid)
    if u and u.banned:
        await bot.reply_to(message, "你已被禁止使用此翻译服务")
        return
    await bot.send_document(
        chat_id=message.chat.id,
        document=("Disclaimer 免责声明.md", disclaimer_file.read_bytes()),
    )
    await bot.reply_to(
        message,
        "欢迎使用 Azusa-Mikan 翻译服务 🤖\n"
        "基于 AI 大模型的翻译 API，完全自建，不保证稳定。\n"
        "/key — 获取 API Key\n"
        "/status — 查看服务状态\n\n"
        f"API 地址: <code>{escape(CONFIG.public_url)}</code>",
        parse_mode="HTML",
    )


@bot.message_handler(commands=["key"])
async def cmd_key(message: types.Message):
    uid = _uid(message)
    if uid is None:
        return
    u = await _get_user(uid)
    if u is None:
        await bot.reply_to(message, "你尚未注册，请发 /start 注册")
        return
    if u.banned:
        await bot.reply_to(message, "你已被禁止使用此翻译服务")
        return

    try:
        new_key = await users_service.issue_api_key(uid) or ""
    except Exception:
        await bot.reply_to(message, "生成失败，请联系管理员。")
        return

    msg = await bot.reply_to(
        message,
        (
            f"✅ 生成成功！你的 API Key：\n\n"
            f"<code>{escape(new_key)}</code>\n"
            f"\n⚠️ 此消息 30 秒后自动删除，请妥善保管你的 Key。"
        ),
        parse_mode="HTML",
        protect_content=False,
    )
    await asyncio.sleep(30)
    try:
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception:
        pass


@bot.message_handler(commands=["status"])
async def cmd_status(message: types.Message):
    try:
        st = await llama_service.status()
        text = (
            "🟢 正常运行"
            if st.ready
            else "🟡 加载中"
            if st.loading
            else "🔴 未启动"
        )
        await bot.reply_to(
            message,
            (
                f"服务状态: {text}\n"
                f"总请求数: {st.total_requests}\n"
                f"今日请求数: {st.today_requests}\n\n"
                f"API 地址: <code>{escape(CONFIG.public_url)}</code>"
            ),
            parse_mode="HTML",
        )
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")


@bot.message_handler(commands=["start_llama"])
async def cmd_start_llama(message: types.Message):
    uid = _uid(message)
    if uid is None or not _is_admin(uid):
        return
    try:
        state = await llama_service.start()
        if state in {"starting", "loading"}:
            reply = "✅ 已开始加载，完成后会通知"
        else:
            status = await llama_service.status()
            if status.ready:
                publish_status("ready", force=True)
            reply = "✅ 已在运行"
        await bot.reply_to(message, reply)
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")


@bot.message_handler(commands=["stop_llama"])
async def cmd_stop_llama(message: types.Message):
    uid = _uid(message)
    if uid is None or not _is_admin(uid):
        return
    try:
        await llama_service.stop()
        await bot.reply_to(message, "🛑 已停止")
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")


@bot.message_handler(commands=["usage"])
async def cmd_usage(message: types.Message):
    uid = _uid(message)
    if uid is None or not _is_admin(uid):
        return
    try:
        rows = sorted(
            await users_service.list_users(),
            key=lambda u: u.usage_count,
            reverse=True,
        )
        lines = ["📊 调用统计"]
        lines += [
            f"  {u.name}: {u.usage_count} 次"
            for u in rows
            if u.usage_count > 0
        ]
        await bot.reply_to(message, "\n".join(lines) if len(lines) > 1 else "暂无数据")
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")


@bot.message_handler(commands=["ban"])
async def cmd_ban(message: types.Message):
    uid = _uid(message)
    if uid is None or not _is_admin(uid):
        return
    args = _text(message).split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await bot.reply_to(message, "用法: /ban 用户ID")
        return
    try:
        await users_service.ban(int(args[1]))
        await bot.reply_to(message, f"🔨 已封禁用户 {args[1]}")
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")


@bot.message_handler(commands=["unban"])
async def cmd_unban(message: types.Message):
    uid = _uid(message)
    if uid is None or not _is_admin(uid):
        return
    args = _text(message).split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await bot.reply_to(message, "用法: /unban 用户ID")
        return
    try:
        await users_service.unban(int(args[1]))
        await bot.reply_to(message, f"🔓 已解封用户 {args[1]}")
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")


@bot.message_handler(commands=["users"])
async def cmd_users(message: types.Message):
    uid = _uid(message)
    if uid is None or not _is_admin(uid):
        return
    args = _text(message).split(maxsplit=1)
    try:
        rows = await users_service.list_users()

        if len(args) >= 2 and args[1].isdigit():
            target = int(args[1])
            user = next((u for u in rows if u.tgid == target), None)
            if user is None:
                await bot.reply_to(message, "未找到该用户。")
                return
            status = "🔴 封禁" if user.banned else "🟢 正常"
            await bot.reply_to(
                message,
                f"📋 用户 {target} 详情:\n"
                f"名称: {escape(user.name)}\n"
                f"状态: {status}\n"
                f"调用次数: {user.usage_count}\n"
                f"最后调用: {user.final_usage_at or '无'}\n"
                f"IP: {', '.join(user.ip_addresses) if user.ip_addresses else '无'}",
            )
            return

        if not rows:
            await bot.reply_to(message, "暂无用户")
            return
        lines = [
            f"{'🔴 ' if u.banned else '🟢 '}{u.tgid} {u.name}"
            for u in rows
        ]
        for i in range(0, len(lines), 30):
            await bot.reply_to(message, "\n".join(lines[i : i + 30]))
    except Exception:
        await bot.reply_to(message, "操作失败，请稍后重试。")
