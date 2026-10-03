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
from aiogram.exceptions import TelegramAPIError, TelegramConflictError
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
POLLING_CONFLICT_RETRY_SECONDS = 10
UPDATE_CONCURRENCY_LIMIT = 512
HTTP_CONNECTION_LIMIT = 256
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
PROMO_CODE = "ZANODDS"
REGISTER_LINK = "https://cropped.link/Zanf"
ADS_CONTACT = "@zan_fvrr"
COLLAB_CONTACT = "@sent2000s"
AGENT_CONTACT = "@Zanspo1"
CHANNELS = (
    {"username": "@zansportnews", "link": "https://t.me/zansportnews", "title": "Zan Sport News"},
)
MENU_CODES = "🎯 Today's Free Codes"
MENU_BONUS = "🎁 200% Bonus & Promo Code"
MENU_AGENT = "⚡ Deposit & Withdraw"
MENU_PREDICTIONS = "⚽ Free Predictions"
MENU_VIP = "⭐ VIP Channel"
MENU_DEPOSIT_WITHDRAWAL = "💳 Deposit & Withdrawal"
MENU_SUPPORT = "📢 For Ads / Support"
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
            elif isinstance(event, Message):
                response = (
                    "Please wait a moment before sending another message."
                    if event.text
                    else "⚠️ Only button navigation is supported!"
                )
                try:
                    await event.answer(response, reply_markup=main_menu())
                except TelegramAPIError:
                    logger.debug("Could not acknowledge rate-limited message")
            return None
        return await handler(event, data)


user_rate_limit = UserRateLimitMiddleware()


class UpdateErrorBoundaryMiddleware(BaseMiddleware):
    """Contain handler failures and return a safe, concise response to the user."""

    async def __call__(self, handler: Any, event: Any, data: dict[str, Any]) -> Any:
        try:
            return await handler(event, data)
        except Exception:
            logger.exception("Unhandled failure while processing update")
            try:
                if isinstance(event, Message):
                    await event.answer("⚠️ Only button navigation is supported!", reply_markup=main_menu())
                elif isinstance(event, CallbackQuery):
                    await event.answer("⚠️ Please use the menu buttons.", show_alert=True)
            except TelegramAPIError:
                logger.exception("Could not send update-failure response")
            return None


update_error_boundary = UpdateErrorBoundaryMiddleware()
router.message.outer_middleware(update_error_boundary)
router.callback_query.outer_middleware(update_error_boundary)
router.channel_post.outer_middleware(update_error_boundary)
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
            [KeyboardButton(text=MENU_PREDICTIONS), KeyboardButton(text=MENU_VIP)],
            [KeyboardButton(text=MENU_DEPOSIT_WITHDRAWAL), KeyboardButton(text=MENU_BONUS)],
            [KeyboardButton(text=MENU_SUPPORT)],
        ],
        resize_keyboard=True,
        is_persistent=True,
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
        "🎯 <b>Welcome to ZAN SPORT NEWS OFFICIAL BOT!</b>\n"
        "🎯 <b>እንኳን ወደ ዛን ስፖርት ዜና ኦፊሴላዊ ቦት በደህና መጡ!</b>\n\n"
        f"👋 {name}\n\n"
        "Select an option below to continue / ለመቀጠል ከታች ካሉት አማራጮች አንዱን ይምረጡ፦"
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


@router.message(F.text == MENU_BONUS)
async def show_bonus_promo(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 Register & Claim Bonus", url=REGISTER_LINK)]
        ]
    )
    await message.answer(
        "🎁 <b>EXCLUSIVE 200% FIRST DEPOSIT BONUS</b> 🎁\n"
        "🎁 <b>ልዩ የመጀመሪያ ተቀማጭ 200% ቦነስ</b> 🎁\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚀 <b>Eligible new customers may qualify for up to 60,000 ETB.</b>\n"
        "🚀 <b>ብቁ የሆኑ አዳዲስ ደንበኞች እስከ 60,000 ETB ቦነስ ሊያገኙ ይችላሉ።</b>\n\n"
        "📌 <b>How to claim / እንዴት እንደሚወስዱ</b>\n"
        f"1️⃣ Register using our official link: <a href=\"{REGISTER_LINK}\">Zan registration</a>\n"
        f"1️⃣ በይፋዊ ሊንካችን ይመዝገቡ: <a href=\"{REGISTER_LINK}\">ይመዝገቡ</a>\n"
        f"2️⃣ Enter promo code <code>{PROMO_CODE}</code> during registration.\n"
        f"2️⃣ በምዝገባ ጊዜ የፕሮሞ ኮድ <code>{PROMO_CODE}</code> ያስገቡ።\n"
        "3️⃣ Make your first deposit and check the offer terms to confirm eligibility.\n"
        "3️⃣ የመጀመሪያ ተቀማጭዎን ያድርጉና የቦነሱን ውሎች ያረጋግጡ።\n\n"
        "🔥 Check the current offer conditions before depositing. Bonus availability, amount, and crediting "
        "are subject to the operator's terms. Please gamble responsibly.\n"
        "🔥 ከመቀበልዎ በፊት የአሁኑን የቦነስ ውሎች ያንብቡ። ቦነሱ በኦፕሬተሩ ውሎች መሠረት ነው።\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_PREDICTIONS)
async def show_predictions_and_ads(message: Message) -> None:
    await message.answer(
        "⚽ <b>ZAN SPORT NEWS · DAILY FREE TIPS</b> ⚽\n"
        "⚽ <b>ዛን ስፖርት ዜና · ዕለታዊ ነፃ ትንበያዎች</b> ⚽\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>Today's free prediction channels / የዛሬ ነፃ ትንበያ ቻናሎች</b>\n"
        "• 🎯 <a href=\"https://t.me/mrt_tips\">MRT Tips</a>\n"
        f"• 📢 <a href=\"{PUBLIC_CHANNEL_URL}\">Zan Sport News</a>\n\n"
        "📌 Follow for daily match analysis and free predictions.\n"
        "📌 ዕለታዊ የጨዋታ ትንተናና ነፃ ትንበያዎችን ለማግኘት ይከታተሉ።\n"
        "━━━━━━━━━━━━━━━━━━━━",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_DEPOSIT_WITHDRAWAL)
async def show_deposit_withdrawal_instructions(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📩 Contact @Zanspo1", url="https://t.me/Zanspo1")]
        ]
    )
    await message.answer(
        "💳 <b>DEPOSITS & WITHDRAWALS</b> 🏧\n"
        "💳 <b>ተቀማጭ እና ወጪ ገንዘብ</b> 🏧\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📥 <b>To deposit / ተቀማጭ ለማድረግ</b>\n"
        "1️⃣ Contact @Zanspo1 for current payment details.\n"
        "1️⃣ የአሁኑን የክፍያ ዝርዝር ለማግኘት @Zanspo1 ያናግሩ።\n"
        "2️⃣ Complete your transfer using the instructions provided.\n"
        "2️⃣ በተሰጡት መመሪያዎች መሠረት ገንዘቡን ያስተላልፉ።\n"
        "3️⃣ Send the transaction confirmation or receipt to @Zanspo1 for verification.\n\n"
        "3️⃣ የግብይት ማረጋገጫውን ወይም ደረሰኙን ለማረጋገጥ ለ@Zanspo1 ይላኩ።\n\n"
        "📤 <b>To withdraw / ገንዘብ ለማውጣት</b>\n"
        "1️⃣ Contact @Zanspo1 with your payout request.\n"
        "1️⃣ የወጪ ገንዘብ ጥያቄዎን ለ@Zanspo1 ያቅርቡ።\n"
        "2️⃣ Provide your account identifier and preferred payment details.\n"
        "2️⃣ የመለያ መረጃዎንና የሚመርጡትን የክፍያ ዝርዝር ያቅርቡ።\n"
        "3️⃣ Follow the admin's instructions while your request is reviewed.\n"
        "3️⃣ ጥያቄዎ እስኪገመገም ድረስ የአስተዳዳሪውን መመሪያ ይከተሉ።\n\n"
        "All deposit and withdrawal requests are handled directly by @Zanspo1. Processing depends on verification and payment method.\n"
        "ሁሉም የተቀማጭና የወጪ ገንዘብ ጥያቄዎች በቀጥታ በ@Zanspo1 ይከናወናሉ።\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
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
            [InlineKeyboardButton(text="📩 Message Admin @Zanspo1", url="https://t.me/Zanspo1")]
        ]
    )
    await message.answer(
        "⭐ <b>ZAN PREMIUM VIP CLUB</b> ⭐\n"
        "⭐ <b>ዛን ፕሪሚየም VIP ክለብ</b> ⭐\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "💎 <b>VIP benefits / የVIP ጥቅሞች</b>\n"
        "• 🎯 Premium daily tips and match analysis\n"
        "• 🎯 ፕሪሚየም ዕለታዊ ትንበያዎችና የጨዋታ ትንተና\n"
        "• 🚀 Exclusive selections and accumulator slips\n"
        "• 🚀 ልዩ የጨዋታ ምርጫዎችና የአኩሙሌተር ትኬቶች\n"
        "• 📈 Private channel access after payment approval\n"
        "• 📈 ክፍያዎ ከጸደቀ በኋላ የግል ቻናል መዳረሻ\n\n"
        "🔒 <b>How to join / እንዴት መቀላቀል እንደሚቻል</b>\n"
        "1️⃣ Contact @Zanspo1 for payment instructions.\n"
        "1️⃣ ስለ ክፍያ መመሪያዎች @Zanspo1 ያናግሩ።\n"
        "2️⃣ Complete payment and send the receipt or transaction ID to the admin.\n"
        "2️⃣ ክፍያዎን ፈጽመው ደረሰኙን ወይም የግብይት መለያ ቁጥሩን ለአስተዳዳሪው ይላኩ።\n"
        "3️⃣ The private invite is sent only after @Zanspo1 verifies and approves your payment.\n"
        "3️⃣ የግል የመቀላቀያ ሊንኩ የሚላክልዎ @Zanspo1 ክፍያዎን ካረጋገጠና ካጸደቀ በኋላ ብቻ ነው።\n\n"
        "💬 Use the button below to contact the admin. The private invite is not displayed here.\n"
        "💬 አስተዳዳሪውን ለማነጋገር ከታች ያለውን ቁልፍ ይጠቀሙ። የግል ሊንኩ እዚህ አይታይም።\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_SUPPORT)
async def show_support(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Contact @zan_fvrr", url="https://t.me/zan_fvrr")],
            [InlineKeyboardButton(text="🤝 Contact @sent2000s", url="https://t.me/sent2000s")],
            [InlineKeyboardButton(text="📩 General Support · @Zanspo1", url="https://t.me/Zanspo1")],
        ]
    )
    await message.answer(
        "📢 <b>ADVERTISING · PARTNERSHIPS · SUPPORT</b> 📢\n"
        "📢 <b>ማስታወቂያ · ትብብር · ድጋፍ</b> 📢\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "💼 <b>Business and promotions / ንግድና ማስታወቂያ</b>\n"
        "Reach an active sports audience through advertising and collaborations.\n"
        "በማስታወቂያና በትብብር ከስፖርት ተከታዮች ጋር ይድረሱ።\n"
        f"• Ads: {ADS_CONTACT}\n"
        f"• ማስታወቂያ: {ADS_CONTACT}\n"
        f"• Company collaborations: {COLLAB_CONTACT}\n\n"
        f"• የኩባንያ ትብብር: {COLLAB_CONTACT}\n\n"
        "🛠️ <b>Customer care and account support / የደንበኛና የመለያ ድጋፍ</b>\n"
        f"• General support and VIP assistance: {AGENT_CONTACT}\n"
        f"• አጠቃላይ ድጋፍና VIP እገዛ: {AGENT_CONTACT}\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
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


@router.message(F.text)
async def fallback_text(message: Message) -> None:
    await message.answer(
        "I didn’t recognize that message. Use one of the menu buttons below, or send /start to reopen the menu.",
        reply_markup=main_menu(),
    )


@router.message()
async def fallback_unsupported_message(message: Message) -> None:
    await message.answer("⚠️ Only button navigation is supported!", reply_markup=main_menu())


@router.callback_query()
async def fallback_callback(query: CallbackQuery) -> None:
    await query.answer("This action is no longer available. Please use the menu.", show_alert=True)


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


async def run_health_server(started: asyncio.Future[None]) -> None:
    app = web.Application()
    app.router.add_get("/", health_check)
    web_runner = web.AppRunner(app)
    try:
        await web_runner.setup()
        port = int(os.environ.get("PORT", 10000))
        site = web.TCPSite(web_runner, host="0.0.0.0", port=port)
        await site.start()
        logger.info("Health server listening on 0.0.0.0:%s", port)
        if not started.done():
            started.set_result(None)
        await asyncio.Event().wait()
    except BaseException as error:
        if not started.done():
            started.set_exception(error)
        raise
    finally:
        await web_runner.cleanup()


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN in .env before starting the bot.")
    if not ADMIN_ID:
        raise RuntimeError("Set a numeric ADMIN_ID in .env before starting the bot.")

    if ADMIN_ID <= 0:
        raise RuntimeError("Set ADMIN_ID to a positive integer in .env before starting the bot.")

    session = AiohttpSession(timeout=HTTP_TIMEOUT_SECONDS, limit=HTTP_CONNECTION_LIMIT)
    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(router)
    dispatcher.startup.register(on_startup)
    server_started: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    server_task = asyncio.create_task(
        run_health_server(server_started),
        name="render-health-server",
    )
    try:
        await server_started
        await bot.delete_webhook(drop_pending_updates=True)
        while True:
            try:
                await dispatcher.start_polling(
                    bot,
                    allowed_updates=dispatcher.resolve_used_update_types(),
                    polling_timeout=POLLING_TIMEOUT_SECONDS,
                    handle_as_tasks=True,
                    tasks_concurrency_limit=UPDATE_CONCURRENCY_LIMIT,
                )
                break
            except TelegramConflictError:
                logger.exception(
                    "Telegram polling conflict: another process is polling this bot token. "
                    "Retrying in %s seconds; configure Render to run exactly one polling instance.",
                    POLLING_CONFLICT_RETRY_SECONDS,
                )
                await asyncio.sleep(POLLING_CONFLICT_RETRY_SECONDS)
    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Health server task stopped unexpectedly")
        finally:
            await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")