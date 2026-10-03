"""Dual-channel sports odds and promotions bot built with aiogram 3."""

import asyncio
import logging
import os
import re
import sqlite3
import time
from collections import OrderedDict
from contextlib import contextmanager
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LinkPreviewOptions,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("zan_odds_bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_VALUE = os.getenv("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ID_VALUE) if re.fullmatch(r"[1-9][0-9]*", ADMIN_ID_VALUE) else 0
BOT_LOGO_FILE_ID = os.getenv("BOT_LOGO_FILE_ID", "").strip()
BOT_LOGO_URL_VALUE = os.getenv("BOT_LOGO_URL", "").strip()
_logo_url = urlparse(BOT_LOGO_URL_VALUE)
BOT_LOGO_URL = (
    BOT_LOGO_URL_VALUE
    if _logo_url.scheme == "https" and _logo_url.netloc
    else ""
)
if BOT_LOGO_URL_VALUE and not BOT_LOGO_URL:
    logger.warning("BOT_LOGO_URL must be an HTTPS URL; logo URL disabled")
LOGO_FILE_ID_CACHE = BOT_LOGO_FILE_ID
HTTP_TIMEOUT_SECONDS = 20
POLLING_TIMEOUT_SECONDS = 30
RATE_LIMIT_SECONDS = 0.8
RATE_LIMIT_MAX_USERS = 50_000

DATABASE_PATH = Path(os.getenv("BOT_DATABASE", "bot_data.sqlite3"))
CHANNEL_USERNAME = "@zansportnews"
PUBLIC_CHANNEL_URL = "https://t.me/zansportnews"
try:
    configured_message_id = int(os.getenv("CHANNEL_CODE_MESSAGE_ID", "0"))
    CHANNEL_CODE_MESSAGE_ID = configured_message_id if configured_message_id > 0 else None
except ValueError:
    logger.warning("CHANNEL_CODE_MESSAGE_ID must be a positive integer; using channel-link fallback")
    CHANNEL_CODE_MESSAGE_ID = None
PROMO_CODE = "ZANF"
REGISTER_LINK = "https://cropped.link/Zanf"
ADS_CONTACT = "@zan_fvrr"
AGENT_CONTACT = "@Zanspo1"
CHANNELS = (
    {"username": "@zansportnews", "link": "https://t.me/zansportnews", "title": "Zan Sport News"},
)
MENU_CODES = "🎯 Today's Free Codes"
MENU_PROMO = "💎 1XBet Promo"
MENU_AGENT = "⚡ Deposit & Withdraw"
MENU_PREDICTIONS = "⚽ Predictions & Ads"
MENU_VIP = "⭐ Join VIP Channel"
MENU_HELP = "📩 Help & Support"
DEFAULT_CODES = {
    "1XBet": ("ZANF2026", "2.45"),
    "SportyBet": ("BC89210", "3.10"),
    "Bet365": ("B365VIP", "1.95"),
    "Melbet": ("MELZANF", "2.20"),
}
PREVIOUS_DEFAULT_CODES = {
    "1XBet": ("ZAN-1X-2026", "2.15"),
    "SportyBet": ("SPT-88219", "3.10"),
    "Bet365": ("B365-99012", "1.85"),
    "Melbet": ("MEL-77123", "2.50"),
}
BOOKING_CODE_RE = re.compile(r"^[A-Za-z0-9-]{5,32}$")
ODDS_RE = re.compile(r"^\d{1,5}(?:\.\d{1,3})?$")
CHANNEL_CODE_RE = re.compile(r"(?<![A-Z0-9])[A-Z0-9]{5,8}(?![A-Z0-9])")
PROMO_MARKER = "\n\n📣 Follow Zan Odds:"

router = Router()


class UserRateLimitMiddleware(BaseMiddleware):
    """Limit each user's incoming bot updates without unbounded state growth."""

    def __init__(self, interval: float = RATE_LIMIT_SECONDS, cache_ttl: float = 600) -> None:
        self.interval = interval
        self.cache_ttl = cache_ttl
        self._last_update: OrderedDict[int, float] = OrderedDict()
        self._lock = asyncio.Lock()
        self._checks = 0

    async def __call__(self, handler: Any, event: Any, data: dict[str, Any]) -> Any:
        user = getattr(event, "from_user", None)
        if user is None:
            return await handler(event, data)

        now = time.monotonic()
        async with self._lock:
            previous = self._last_update.get(user.id)
            allowed = previous is None or now - previous >= self.interval
            if allowed:
                self._last_update[user.id] = now
                self._last_update.move_to_end(user.id)
            self._checks += 1
            if self._checks % 256 == 0:
                cutoff = now - self.cache_ttl
                while self._last_update:
                    oldest_user, oldest_update = next(iter(self._last_update.items()))
                    if oldest_update >= cutoff:
                        break
                    self._last_update.pop(oldest_user)
            while len(self._last_update) > RATE_LIMIT_MAX_USERS:
                self._last_update.popitem(last=False)

        if not allowed:
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer("Please wait a moment before trying again.")
                except TelegramAPIError:
                    logger.debug("Could not acknowledge rate-limited callback")
            return None
        return await handler(event, data)


user_rate_limit = UserRateLimitMiddleware()
router.message.outer_middleware(user_rate_limit)
router.callback_query.outer_middleware(user_rate_limit)


@contextmanager
def open_database() -> Iterator[sqlite3.Connection]:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_database() -> None:
    with open_database() as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS booking_codes (
                bookie TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                odds TEXT NOT NULL
            )"""
        )
        connection.execute("DROP TABLE IF EXISTS daily_spins")
        connection.executemany(
            "INSERT OR IGNORE INTO booking_codes (bookie, code, odds) VALUES (?, ?, ?)",
            [(bookie, code, odds) for bookie, (code, odds) in DEFAULT_CODES.items()],
        )
        for bookie, (old_code, old_odds) in PREVIOUS_DEFAULT_CODES.items():
            new_code, new_odds = DEFAULT_CODES[bookie]
            connection.execute(
                """UPDATE booking_codes SET code = ?, odds = ?
                   WHERE bookie = ? AND code = ? AND odds = ?""",
                (new_code, new_odds, bookie, old_code, old_odds),
            )


def load_booking_code(bookie: str) -> tuple[str, str] | None:
    with open_database() as connection:
        row = connection.execute(
            "SELECT code, odds FROM booking_codes WHERE bookie = ?", (bookie,)
        ).fetchone()
    if row is None:
        return None
    return row["code"], row["odds"]


def save_booking_code(bookie: str, code: str, odds: str) -> None:
    with open_database() as connection:
        connection.execute(
            """INSERT INTO booking_codes (bookie, code, odds) VALUES (?, ?, ?)
               ON CONFLICT(bookie) DO UPDATE SET code = excluded.code, odds = excluded.odds""",
            (bookie, code, odds),
        )


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_CODES)],
            [KeyboardButton(text=MENU_PROMO), KeyboardButton(text=MENU_AGENT)],
            [KeyboardButton(text=MENU_PREDICTIONS), KeyboardButton(text=MENU_VIP)],
            [KeyboardButton(text=MENU_HELP)],
        ],
        resize_keyboard=True,
        row_width=1,
        input_field_placeholder="Choose a service",
    )


def bookies_keyboard() -> InlineKeyboardMarkup:
    bookies = list(DEFAULT_CODES)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"⚽ {bookie}", callback_data=f"code:{bookie}")
                for bookie in bookies[index:index + 2]
            ]
            for index in range(0, len(bookies), 2)
        ]
    )


async def check_all_subscriptions(user_id: int) -> bool:
    """Subscription bypass; access is intentionally open to every user."""
    return True


@router.message(CommandStart())
async def start(message: Message) -> None:
    global LOGO_FILE_ID_CACHE
    if not message.from_user:
        return
    name = escape(message.from_user.first_name or "there")
    welcome_text = (
        "🔥 <b>WELCOME TO ZAN ODDS OFFICIAL BOT</b> 🔥\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👋 <b>Welcome, {name}!</b>\n\n"
        "🚀 Get live prediction links, 1XBet bonuses, and instant agent support.\n"
        "👇 <b>Select a service below</b>"
    )
    logo_sources = [source for source in (LOGO_FILE_ID_CACHE, BOT_LOGO_URL) if source]
    for photo in dict.fromkeys(logo_sources):
        try:
            sent_message = await message.answer_photo(
                photo=photo,
                caption=welcome_text,
                reply_markup=main_menu(),
            )
            if sent_message.photo:
                LOGO_FILE_ID_CACHE = sent_message.photo[-1].file_id
            return
        except TelegramAPIError:
            logger.warning("Could not send configured logo source; trying fallback", exc_info=True)
            if photo == LOGO_FILE_ID_CACHE:
                LOGO_FILE_ID_CACHE = ""
    await message.answer(welcome_text, reply_markup=main_menu())


@router.message(F.text == MENU_CODES)
async def show_free_codes(message: Message, bot: Bot) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 View Latest Codes 🚀", url=PUBLIC_CHANNEL_URL)]
        ]
    )
    if CHANNEL_CODE_MESSAGE_ID:
        try:
            await bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=CHANNEL_USERNAME,
                message_id=CHANNEL_CODE_MESSAGE_ID,
            )
            await message.answer(
                "📌 <b>Latest prediction copied.</b>\n"
                "Open the live channel for newer codes.",
                reply_markup=keyboard,
            )
            return
        except TelegramAPIError:
            logger.exception(
                "Could not copy channel post %s from %s",
                CHANNEL_CODE_MESSAGE_ID,
                CHANNEL_USERNAME,
            )

    await message.answer(
        "🎯 <b>TODAY'S OFFICIAL FREE CODES</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Today's verified prediction codes are posted live in our official channel.\n\n"
        "👇 <b>Open the latest post</b>",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.callback_query(F.data.startswith("code:"))
async def show_booking_code(query: CallbackQuery) -> None:
    bookie = (query.data or "").partition(":")[2]
    data = await asyncio.to_thread(load_booking_code, bookie)
    if data is None:
        await query.answer("Unknown bookmaker.", show_alert=True)
        return
    code, odds = data
    if query.message:
        await query.message.answer(
            f"🏆 <b>𝑫𝑨𝑰𝑳𝒀 𝑻𝑰𝑪𝑲𝑬𝑻 · {escape(bookie)}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 <b>Total odds:</b> <code>{escape(odds)}</code>\n"
            f"📌 <b>Booking code:</b> <code>{escape(code)}</code>\n\n"
            "Tap the code to copy, then paste it in your bookmaker app. 🍀"
        )
    await query.answer()


@router.message(F.text == MENU_PROMO)
async def show_promo(message: Message) -> None:
    await message.answer(
        "💎 <b>𝟏𝐗𝐁𝐄𝐓 · 200% WELCOME BONUS</b> 💎\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Register with our promo code for the welcome offer of up to <b>30,000 ETB</b>.\n"
        "Check the current offer terms and eligibility before registering.\n\n"
        f"🏆 <b>PROMO CODE ➡️</b> <code>{PROMO_CODE}</code>\n\n"
        f"🔗 <a href=\"{REGISTER_LINK}\">REGISTER WITH 1XBET</a>\n\n"
        "Tap the code to copy it. Eligibility and bonus terms are set by 1XBet. Please gamble responsibly.",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_PREDICTIONS)
async def show_predictions_and_ads(message: Message) -> None:
    await message.answer(
        "⚽ <b>𝑭𝑹𝑬𝑬 𝑷𝑹𝑬𝑫𝑰𝑪𝑻𝑰𝑶𝑵𝑺 & 𝑨𝑫𝑺</b> ⚽\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Follow our channels for daily predictions:\n"
        f"• <a href=\"{PUBLIC_CHANNEL_URL}\">Zan Sport News</a>\n\n"
        f"📢 <b>FOR ADS 👉</b> {ADS_CONTACT}",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_AGENT)
async def show_agent(message: Message) -> None:
    await message.answer(
        "⚡ <b>𝑭𝑨𝑺𝑻 𝑫𝑬𝑷𝑶𝑺𝑰𝑻 & 𝑾𝑰𝑻𝑯𝑫𝑹𝑨𝑾</b> ⚡\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Fast, reliable deposit and cashout support, available 24/7.\n\n"
        f"👤 <b>Agent:</b> {AGENT_CONTACT}\n"
        "⏰ <b>Service hours:</b> 24/7\n\n"
        "Confirm fees before sending funds."
    )


@router.message(F.text == MENU_VIP)
async def show_vip_channel(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📩 Contact @Zanspo1 for Payment", url="https://t.me/Zanspo1")]
        ]
    )
    await message.answer(
        "⭐ <b>VIP CHANNEL ACCESS</b> ⭐\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Get premium sports predictions and exclusive updates in our VIP channel.\n\n"
        "Contact <a href=\"https://t.me/Zanspo1\">@Zanspo1</a> for the current payment instructions. "
        "After paying, send your proof of payment or transaction ID directly to @Zanspo1 for review.\n\n"
        "Only after @Zanspo1 verifies and approves your payment will the VIP invite link be sent to you privately. "
        "The link is not available before approval.\n\n"
        "Please wait for confirmation before expecting access. Contact the admin if you need help.",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_HELP)
async def show_help(message: Message) -> None:
    await message.answer(
        "ℹ️ <b>HELP & SUPPORT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"For advertising and business questions, contact {ADS_CONTACT}.",
        reply_markup=main_menu(),
    )


@router.message(Command("setcode"))
async def set_booking_code(message: Message) -> None:
    if (
        not message.from_user
        or not isinstance(message.from_user.id, int)
        or isinstance(message.from_user.id, bool)
        or message.from_user.id != ADMIN_ID
    ):
        return
    parts = (message.text or "").split(maxsplit=3)
    if len(parts) != 4:
        await message.reply(
            "⚙️ <b>Usage</b>\n<code>/setcode 1XBet CODE123 2.15</code>"
        )
        return
    _, bookie, code, odds = parts
    if bookie not in DEFAULT_CODES:
        await message.reply(
            "❌ Unknown bookmaker. Use 1XBet, SportyBet, Bet365, or Melbet."
        )
        return
    if not BOOKING_CODE_RE.fullmatch(code) or not ODDS_RE.fullmatch(odds):
        await message.reply(
            "❌ Code must be 5–32 letters, numbers, or hyphens; odds must be positive."
        )
        return
    if Decimal(odds) <= 0:
        await message.reply("❌ Odds must be greater than zero.")
        return
    await asyncio.to_thread(save_booking_code, bookie, code, odds)
    await message.reply(
        f"✅ <b>Code updated · {escape(bookie)}</b>\n"
        f"📌 Code: <code>{escape(code)}</code>\n"
        f"📊 Odds: <b>{escape(odds)}</b>"
    )


@router.message(F.photo)
async def forward_receipt(message: Message, bot: Bot) -> None:
    if not message.from_user:
        return
    if not ADMIN_ID:
        logger.error("ADMIN_ID is missing; cannot forward receipt from user %s", message.from_user.id)
        await message.answer(
            "📩 Receipt forwarding is unavailable right now. Contact support."
        )
        return
    user = message.from_user
    username = f"@{escape(user.username)}" if user.username else "not set"
    caption = (
        "📩 <b>NEW USER PHOTO / RECEIPT</b>\n\n"
        f"👤 <b>Name:</b> {escape(user.full_name)}\n"
        f"🔗 <b>Username:</b> {username}\n"
        f"🆔 <b>User ID:</b> <code>{user.id}</code>"
    )
    try:
        await bot.send_photo(chat_id=ADMIN_ID, photo=message.photo[-1].file_id, caption=caption)
    except TelegramAPIError:
        logger.exception("Could not forward photo from user %s", user.id)
        await message.answer(
            "❌ Could not send the screenshot. Try again later."
        )
        return
    await message.answer(
        "✅ Screenshot sent to the admin for review."
    )


@router.channel_post()
async def enhance_channel_post(message: Message, bot: Bot) -> None:
    """Format short booking codes and append channel promotion links to channel posts."""
    original = message.text or message.caption
    if not original or PROMO_MARKER.strip() in original:
        return
    promo = (
        f'{PROMO_MARKER}\n<a href="{PUBLIC_CHANNEL_URL}">Zan Sport News</a>'
    )
    limit = 4000 if message.text is not None else 900
    body = original[:max(0, limit - len(promo))]
    escaped_body = escape(body)
    formatted_body = CHANNEL_CODE_RE.sub(
        lambda match: f"<code>{match.group(0)}</code>", escaped_body
    )
    enhanced = f"{formatted_body}{promo}"
    try:
        if message.text is not None:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=message.message_id,
                text=enhanced,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
        else:
            await bot.edit_message_caption(
                chat_id=message.chat.id,
                message_id=message.message_id,
                caption=enhanced,
            )
    except TelegramAPIError:
        logger.exception("Could not enhance channel post %s", message.message_id)


@router.errors()
async def handle_unexpected_error(event: ErrorEvent) -> bool:
    error = event.exception
    logger.error(
        "Unhandled error while processing update: %s",
        error,
        exc_info=(type(error), error, error.__traceback__),
    )
    return True


async def on_startup() -> None:
    await asyncio.to_thread(initialize_database)
    logger.info("Database initialized")


async def health_check(request: web.Request) -> web.Response:
    return web.Response(text="Bot is running live 24/7!")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN in .env before starting the bot.")
    if not ADMIN_ID:
        raise RuntimeError("Set a numeric ADMIN_ID in .env before starting the bot.")

    if ADMIN_ID <= 0:
        raise RuntimeError("Set ADMIN_ID to a positive integer in .env before starting the bot.")

    session = AiohttpSession(timeout=HTTP_TIMEOUT_SECONDS)
    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(router)
    dispatcher.startup.register(on_startup)
    app = web.Application()
    app.router.add_get("/", health_check)
    web_runner = web.AppRunner(app)
    try:
        await web_runner.setup()
        port = int(os.environ.get("PORT", 10000))
        site = web.TCPSite(web_runner, host="0.0.0.0", port=port)
        await site.start()
        logger.info("Health server listening on 0.0.0.0:%s", port)
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
            polling_timeout=POLLING_TIMEOUT_SECONDS,
        )
    finally:
        await web_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")