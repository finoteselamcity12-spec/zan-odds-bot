"""Dual-channel sports odds and promotions bot built with aiogram 3."""

import asyncio
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramConflictError
from aiogram.filters import Command
from aiogram.filters.command import CommandObject
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
ADMIN_IDS = [5578838045, 8827929191]
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
PROMO_MESSAGE: str | None = None
VIP_INFO: str | None = None
ACTIVE_USER_IDS: set[int] = set()
REGISTER_LINK = "https://cropped.link/Zanf"
ADS_CONTACT = "@zan_fvrr"
COLLAB_CONTACT = "@sent2000s"
AGENT_CONTACT = "@Zanspo1"
DEFAULT_CONTACT_INFO = "@Zanspo1"
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
VIP_INVITE_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?t\.me/\+[A-Za-z0-9_-]+", re.IGNORECASE)
TELEGRAM_USERNAME_RE = re.compile(r"^@?[A-Za-z0-9_]{5,32}$")
MAX_ADMIN_TEXT_LENGTH = 2500
PROMO_MARKER = "\n\n📣 Follow Zan Odds:\n\n📣 የዛን ኦድስን ይከታተሉ:"

command_router = Router(name="commands")
router = Router(name="user-messages")


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
        connection.execute(
            """CREATE TABLE IF NOT EXISTS bot_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT NOT NULL
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


def load_setting(setting_key: str) -> str | None:
    with open_database() as connection:
        row = connection.execute(
            "SELECT setting_value FROM bot_settings WHERE setting_key = ?",
            (setting_key,),
        ).fetchone()
    return row["setting_value"] if row else None


def load_contact_info() -> tuple[str, str]:
    contact_info = load_setting("contact_info") or DEFAULT_CONTACT_INFO
    if not TELEGRAM_USERNAME_RE.fullmatch(contact_info):
        logger.warning("Stored contact information is invalid; using the default Telegram contact")
        contact_info = DEFAULT_CONTACT_INFO
    contact_info = f"@{contact_info.lstrip('@')}"
    return contact_info, f"https://t.me/{contact_info.lstrip('@')}"


def save_setting(setting_key: str, setting_value: str) -> None:
    with open_database() as connection:
        connection.execute(
            """INSERT INTO bot_settings (setting_key, setting_value) VALUES (?, ?)
               ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value""",
            (setting_key, setting_value),
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
        row_width=2,
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


@command_router.message(Command("start"))
async def start(message: Message) -> None:
    global LOGO_FILE_ID_CACHE
    if not message.from_user:
        return
    ACTIVE_USER_IDS.add(message.from_user.id)
    name = escape(message.from_user.first_name or "there")
    welcome_text = (
        "🎯 <b>Welcome to ZAN SPORT NEWS OFFICIAL BOT!</b>\n"
        f"👋 Welcome, {name}.\n\n"
        "Select an option below to continue.\n"
        "\n🎯 <b>እንኳን ወደ ዛን ስፖርት ዜና ኦፊሴላዊ ቦት በደህና መጡ!</b>\n"
        f"👋 እንኳን ደህና መጡ፣ {name}።\n\n"
        "ለመቀጠል ከታች ካሉት አማራጮች አንዱን ይምረጡ።"
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
                "Open the live channel for newer codes.\n\n"
                "📌 <b>የቅርብ ጊዜ ትንበያ ተቀድቷል።</b>\n"
                "አዳዲስ ኮዶችን በቀጥታ ቻናሉ ላይ ይመልከቱ።",
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
        "👇 <b>Open the latest post</b>\n\n"
        "የዛሬ የተረጋገጡ ኮዶች በዋናው ቻናላችን ላይ በቀጥታ ይለቀቃሉ።\n"
        "👇 <b>የቅርብ ጊዜ መልዕክት ይመልከቱ</b>",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.callback_query(F.data.startswith("code:"))
async def show_booking_code(query: CallbackQuery) -> None:
    bookie = (query.data or "").partition(":")[2]
    data = await asyncio.to_thread(load_booking_code, bookie)
    if data is None:
        await query.answer("Unknown bookmaker.\n\nያልታወቀ የውርርድ ድርጅት።", show_alert=True)
        return
    code, odds = data
    if query.message:
        await query.message.answer(
            f"🏆 <b>DAILY TICKET · {escape(bookie)}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 <b>Total odds:</b> <code>{escape(odds)}</code>\n"
            f"📌 <b>Booking code:</b> <code>{escape(code)}</code>\n"
            "Tap the code to copy it, then paste it into your bookmaker app. 🍀\n\n"
            f"🏆 <b>የዕለቱ ትኬት · {escape(bookie)}</b>\n"
            f"📊 <b>ጠቅላላ ኦድ:</b> <code>{escape(odds)}</code>\n"
            f"📌 <b>የቦኪንግ ኮድ:</b> <code>{escape(code)}</code>\n"
            "ኮዱን በመንካት ኮፒ ያድርጉና በቡኪንግ መተግበሪያዎ ላይ ይለጥፉ። 🍀"
        )
    await query.answer()


@router.message(F.text == MENU_BONUS)
async def show_bonus_promo(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 Register & Claim Bonus", url=REGISTER_LINK)]
        ]
    )
    custom_message = PROMO_MESSAGE
    if custom_message:
        english_promo = escape(custom_message)
        amharic_promo = (
            "በይፋዊ ሊንክ ይመዝገቡ፣ የተጠቀሰውን የፕሮሞ ኮድ ያስገቡ፣ "
            "ከዚያም የቦነሱን ውሎችና ብቁነት ያረጋግጡ።"
        )
    else:
        english_promo = (
            "🚀 <b>Eligible new customers may qualify for up to 60,000 ETB.</b>\n\n"
            "📌 <b>How to claim</b>\n"
            f"1️⃣ Register using our official link: <a href=\"{REGISTER_LINK}\">Zan registration</a>\n"
            f"2️⃣ Enter promo code <code>{PROMO_CODE}</code> during registration.\n"
            "3️⃣ Make your first deposit and check the offer terms to confirm eligibility.\n"
            "🔥 Check current conditions before depositing. Bonus availability and crediting follow the operator's terms."
        )
        amharic_promo = (
            f"🚀 <b>ብቁ የሆኑ አዳዲስ ደንበኞች እስከ 60,000 ETB ቦነስ ሊያገኙ ይችላሉ።</b>\n\n"
            "📌 <b>ቦነሱን እንዴት ማግኘት ይችላሉ</b>\n"
            f"1️⃣ በይፋዊ ሊንካችን ይመዝገቡ: <a href=\"{REGISTER_LINK}\">የዛን ምዝገባ</a>\n"
            f"2️⃣ በምዝገባ ጊዜ የፕሮሞ ኮድ <code>{PROMO_CODE}</code> ያስገቡ።\n"
            "3️⃣ የመጀመሪያ ተቀማጭዎን ያድርጉና ብቁነትዎን ለማረጋገጥ የቦነሱን ውሎች ይመልከቱ።\n"
            "🔥 ከመቀመጡ በፊት ውሎቹን ያረጋግጡ። የቦነሱ አገኘትና አሰጣጥ በኦፕሬተሩ ውሎች መሠረት ነው። በኃላፊነት ይጫወቱ።"
        )
    await message.answer(
        "🎁 <b>EXCLUSIVE 200% FIRST DEPOSIT BONUS</b> 🎁\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{english_promo}\n\n"
        "🎁 <b>ልዩ የመጀመሪያ ተቀማጭ 200% ቦነስ</b> 🎁\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{amharic_promo}\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_PREDICTIONS)
async def show_predictions_and_ads(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 View Latest Codes 🚀", url=PUBLIC_CHANNEL_URL)]
        ]
    )
    if PROMO_MESSAGE:
        await message.answer(
            escape(PROMO_MESSAGE),
            reply_markup=keyboard,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        return
    await message.answer(
        "⚽ <b>ZAN SPORT NEWS · DAILY FREE TIPS</b> ⚽\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>Today's free prediction channels</b>\n"
        "• 🎯 <a href=\"https://t.me/mrt_tips\">MRT Tips</a>\n"
        f"• 📢 <a href=\"{PUBLIC_CHANNEL_URL}\">Zan Sport News</a>\n"
        "📌 Follow for daily match analysis and free predictions.\n\n"
        "⚽ <b>ዛን ስፖርት ዜና · ዕለታዊ ነፃ ትንበያዎች</b> ⚽\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>የዛሬ ነፃ ትንበያ ቻናሎች</b>\n"
        "• 🎯 <a href=\"https://t.me/mrt_tips\">MRT Tips</a>\n"
        f"• 📢 <a href=\"{PUBLIC_CHANNEL_URL}\">ዛን ስፖርት ዜና</a>\n"
        "📌 ዕለታዊ የጨዋታ ትንተናና ነፃ ትንበያዎችን ለማግኘት ይከታተሉ።\n"
        "━━━━━━━━━━━━━━━━━━━━",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_DEPOSIT_WITHDRAWAL)
async def show_deposit_withdrawal_instructions(message: Message) -> None:
    contact_info, contact_url = await asyncio.to_thread(load_contact_info)
    safe_contact_info = escape(contact_info)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"📩 Contact {contact_info}", url=contact_url)]
        ]
    )
    if PROMO_MESSAGE:
        await message.answer(
            f"{escape(PROMO_MESSAGE)}\n\n"
            "📩 Contact Admin / Support for assistance:\n"
            f"👉 Telegram: {safe_contact_info}\n\n"
            "📩 ለመረጃ ወይም ለእርዳታ አድሚኖችን ያግኙ፦\n"
            f"👉 ቴሌግራም፦ {safe_contact_info}",
            reply_markup=keyboard,
        )
        return
    await message.answer(
        "💳 <b>DEPOSITS & WITHDRAWALS</b> 🏧\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📥 <b>Deposit</b>\n"
        f"1️⃣ Contact {safe_contact_info} for current payment details.\n"
        "2️⃣ Complete your transfer using the instructions provided.\n"
        f"3️⃣ Send the transaction confirmation or receipt to {safe_contact_info} for verification.\n\n"
        "📤 <b>Withdrawal</b>\n"
        f"1️⃣ Contact {safe_contact_info} with your payout request.\n"
        "2️⃣ Provide your account identifier and preferred payment details.\n"
        "3️⃣ Follow the admin's instructions while your request is reviewed.\n"
        f"All deposit and withdrawal requests are handled directly by {safe_contact_info}. Processing depends on verification and payment method.\n"
        f"\n📩 Contact Admin / Support for assistance:\n👉 Telegram: {safe_contact_info}\n\n"
        "\n💳 <b>ተቀማጭ እና ወጪ ገንዘብ</b> 🏧\n"
        "📥 <b>ተቀማጭ ለማድረግ</b>\n"
        f"1️⃣ የአሁኑን የክፍያ ዝርዝር ለማግኘት {safe_contact_info} ያናግሩ።\n"
        "2️⃣ በተሰጡት መመሪያዎች መሠረት ገንዘቡን ያስተላልፉ።\n"
        f"3️⃣ የግብይት ማረጋገጫውን ወይም ደረሰኙን ለማረጋገጥ ለ{safe_contact_info} ይላኩ።\n\n"
        "📤 <b>ገንዘብ ለማውጣት</b>\n"
        f"1️⃣ የወጪ ገንዘብ ጥያቄዎን ለ{safe_contact_info} ያቅርቡ።\n"
        "2️⃣ የመለያ መረጃዎንና የሚመርጡትን የክፍያ ዝርዝር ያቅርቡ።\n"
        "3️⃣ ጥያቄዎ እስኪገመገም ድረስ የአስተዳዳሪውን መመሪያ ይከተሉ።\n"
        f"ሁሉም የተቀማጭና የወጪ ገንዘብ ጥያቄዎች በቀጥታ በ{safe_contact_info} ይከናወናሉ። የማስኬጃ ጊዜው በማረጋገጫና በክፍያ ዘዴ ይወሰናል።\n"
        f"\n📩 ለመረጃ ወይም ለእርዳታ አድሚኖችን ያግኙ፦\n👉 ቴሌግራም፦ {safe_contact_info}\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
    )


@router.message(F.text == MENU_AGENT)
async def show_agent(message: Message) -> None:
    await message.answer(
        "⚡ <b>FAST DEPOSIT & WITHDRAWAL</b> ⚡\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Fast, reliable deposit and cashout support, available 24/7.\n\n"
        f"👤 <b>Agent:</b> {AGENT_CONTACT}\n"
        "⏰ <b>Service hours:</b> 24/7\n\n"
        "Confirm fees before sending funds.\n\n"
        "⚡ <b>ፈጣን ተቀማጭና ወጪ ገንዘብ</b> ⚡\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "ፈጣንና አስተማማኝ ተቀማጭና የገንዘብ ማውጣት እገዛ 24/7 ይገኛል።\n"
        f"👤 <b>ኤጀንት:</b> {AGENT_CONTACT}\n"
        "⏰ <b>የአገልግሎት ሰዓት:</b> 24/7\n\n"
        "ገንዘብ ከመላክዎ በፊት ክፍያዎችን ያረጋግጡ።"
    )


@router.message(F.text == MENU_VIP)
async def show_vip_channel(message: Message) -> None:
    contact_info, contact_url = await asyncio.to_thread(load_contact_info)
    safe_contact_info = escape(contact_info)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"📩 Message Admin {contact_info}", url=contact_url)]
        ]
    )
    custom_vip_info = VIP_INFO
    if custom_vip_info and VIP_INVITE_URL_RE.search(custom_vip_info):
        logger.warning("Stored VIP information contained a private invite URL; using safe defaults")
        custom_vip_info = None
    if custom_vip_info:
        vip_info = escape(custom_vip_info)
    else:
        vip_info = (
            "💎 <b>VIP benefits</b>\n"
            "• 🎯 Premium daily tips and match analysis\n"
            "• 🚀 Exclusive selections and accumulator slips\n"
            "• 📈 Private channel access after payment approval"
        )
    await message.answer(
        "⭐ <b>ZAN PREMIUM VIP CLUB</b> ⭐\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{vip_info}\n\n"
        "🔒 <b>How to join</b>\n"
        f"1️⃣ Contact {safe_contact_info} for payment instructions.\n"
        "2️⃣ Complete payment and send the receipt or transaction ID to the admin.\n"
        f"3️⃣ The private invite is sent only after {safe_contact_info} verifies and approves your payment.\n"
        "💬 Use the button below to contact the admin. The private invite is not displayed here.\n\n"
        f"📩 Contact Admin / Support for assistance:\n👉 Telegram: {safe_contact_info}\n\n"
        "⭐ <b>ዛን ፕሪሚየም VIP ክለብ</b> ⭐\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "💎 <b>የVIP ጥቅሞች</b>\n"
        "• 🎯 ፕሪሚየም ዕለታዊ ትንበያዎችና የጨዋታ ትንተና\n"
        "• 🚀 ልዩ የጨዋታ ምርጫዎችና የአኩሙሌተር ትኬቶች\n"
        "• 📈 ክፍያ ከጸደቀ በኋላ የግል ቻናል መዳረሻ\n\n"
        "🔒 <b>እንዴት መቀላቀል እንደሚቻል</b>\n"
        f"1️⃣ ስለ ክፍያ መመሪያዎች {safe_contact_info} ያናግሩ።\n"
        "2️⃣ ክፍያዎን ፈጽመው ደረሰኙን ወይም የግብይት መለያ ቁጥሩን ለአስተዳዳሪው ይላኩ።\n"
        f"3️⃣ {safe_contact_info} ክፍያዎን ካረጋገጠና ካጸደቀ በኋላ ብቻ የግል የመቀላቀያ ሊንኩ ይላክልዎታል።\n\n"
        "💬 አስተዳዳሪውን ለማነጋገር ከታች ያለውን ቁልፍ ይጠቀሙ። የግል ሊንኩ በዚህ መልዕክት አይታይም።\n"
        f"\n📩 ለመረጃ ወይም ለእርዳታ አድሚኖችን ያግኙ፦\n👉 ቴሌግራም፦ {safe_contact_info}\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@router.message(F.text == MENU_SUPPORT)
async def show_support(message: Message) -> None:
    contact_info, contact_url = await asyncio.to_thread(load_contact_info)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Contact @zan_fvrr", url="https://t.me/zan_fvrr")],
            [InlineKeyboardButton(text="🤝 Contact @sent2000s", url="https://t.me/sent2000s")],
            [InlineKeyboardButton(text=f"📩 General Support · {contact_info}", url=contact_url)],
        ]
    )
    if VIP_INFO:
        await message.answer(
            f"{escape(VIP_INFO)}\n\n"
            "📩 Contact Admin / Support for assistance:\n"
            f"👉 Telegram: {escape(contact_info)}\n\n"
            "📩 ለመረጃ ወይም ለእርዳታ አድሚኖችን ያግኙ፦\n"
            f"👉 ቴሌግራም፦ {escape(contact_info)}",
            reply_markup=keyboard,
        )
        return
    await message.answer(
        "📢 <b>ADVERTISING · PARTNERSHIPS · SUPPORT</b> 📢\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "💼 <b>Business and promotions</b>\n"
        "Reach an active sports audience through advertising and collaborations.\n"
        f"• Ads: {ADS_CONTACT}\n"
        f"• Company collaborations: {COLLAB_CONTACT}\n\n"
        "🛠️ <b>Customer care and account support</b>\n"
        f"• General support and VIP assistance: {escape(contact_info)}\n"
        f"\n📩 Contact Admin / Support for assistance:\n👉 Telegram: {escape(contact_info)}\n\n"
        "\n📢 <b>ማስታወቂያ · ትብብር · ድጋፍ</b> 📢\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "💼 <b>ንግድና ማስታወቂያ</b>\n"
        "በማስታወቂያና በትብብር ከስፖርት ተከታዮች ጋር ይድረሱ።\n"
        f"• ማስታወቂያ: {ADS_CONTACT}\n"
        f"• የኩባንያ ትብብር: {COLLAB_CONTACT}\n\n"
        "🛠️ <b>የደንበኛና የመለያ ድጋፍ</b>\n"
        f"• አጠቃላይ ድጋፍና VIP እገዛ: {escape(contact_info)}\n"
        f"\n📩 ለመረጃ ወይም ለእርዳታ አድሚኖችን ያግኙ፦\n👉 ቴሌግራም፦ {escape(contact_info)}\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=keyboard,
    )


@command_router.message(Command("set_promo"))
async def set_promo_message(message: Message) -> None:
    if message.from_user is None or message.from_user.id not in ADMIN_IDS:
        return
    global PROMO_MESSAGE
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        await message.reply(
            "Usage: <code>/set_promo Your promo announcement</code>\n\n"
            "አጠቃቀም: <code>/set_promo የፕሮሞ ማስታወቂያዎ</code>"
        )
        return
    custom_text = parts[1].strip()
    if len(custom_text) > MAX_ADMIN_TEXT_LENGTH:
        await message.reply(
            f"Promo text must be {MAX_ADMIN_TEXT_LENGTH} characters or fewer.\n\n"
            f"የፕሮሞ ጽሑፉ ከ{MAX_ADMIN_TEXT_LENGTH} ቁምፊዎች መብለጥ የለበትም።"
        )
        return
    PROMO_MESSAGE = custom_text
    await message.reply(
        "✅ Updated successfully! The promo message will appear on the Bonus button.\n\n"
        "✅ የፕሮሞ መልዕክቱ ተዘምኗል። በቦነስ ቁልፉ ላይ ይታያል።"
    )


@command_router.message(Command("set_vip"))
async def set_vip_message(message: Message) -> None:
    if message.from_user is None or message.from_user.id not in ADMIN_IDS:
        return
    global VIP_INFO
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        await message.reply(
            "Usage: <code>/set_vip Your VIP information</code>\n\n"
            "አጠቃቀም: <code>/set_vip የVIP መረጃዎ</code>"
        )
        return
    custom_text = parts[1].strip()
    if len(custom_text) > MAX_ADMIN_TEXT_LENGTH:
        await message.reply(
            f"VIP text must be {MAX_ADMIN_TEXT_LENGTH} characters or fewer.\n\n"
            f"የVIP ጽሑፉ ከ{MAX_ADMIN_TEXT_LENGTH} ቁምፊዎች መብለጥ የለበትም።"
        )
        return
    if VIP_INVITE_URL_RE.search(custom_text):
        await message.reply(
            "Do not include private Telegram invite links. Approved users receive the invite directly from an admin.\n\n"
            "የግል የTelegram መቀላቀያ ሊንክ አያካትቱ። የጸደቁ ተጠቃሚዎች ሊንኩን በቀጥታ ከአስተዳዳሪ ያገኛሉ።"
        )
        return
    VIP_INFO = custom_text
    await message.reply(
        "✅ Updated successfully! VIP information changed; payment approval instructions remain in place.\n\n"
        "✅ የVIP መረጃው ተዘምኗል። የክፍያ ማጽደቂያ መመሪያዎቹ እንደተጠበቁ ይቆያሉ።"
    )


@command_router.message(Command("set_contact"))
async def set_contact_info(message: Message) -> None:
    if message.from_user is None or message.from_user.id not in ADMIN_IDS:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        await message.reply(
            "Usage: <code>/set_contact @TelegramUsername</code>\n\n"
            "አጠቃቀም: <code>/set_contact @TelegramUsername</code>"
        )
        return
    contact_info = parts[1].strip()
    if not TELEGRAM_USERNAME_RE.fullmatch(contact_info):
        await message.reply(
            "Enter a valid Telegram username (5–32 letters, numbers, or underscores).\n\n"
            "ትክክለኛ የቴሌግራም ስም ያስገቡ (5–32 ፊደሎች፣ ቁጥሮች ወይም underscore)።"
        )
        return
    contact_info = f"@{contact_info.lstrip('@')}"
    await asyncio.to_thread(save_setting, "contact_info", contact_info)
    await message.reply(
        "✅ Updated successfully! Contact information has been changed.\n\n"
        "✅ የእውቂያ መረጃው ተዘምኗል።"
    )


@command_router.message(Command("broadcast"))
async def broadcast_message(message: Message, bot: Bot, command: CommandObject) -> None:
    if message.from_user is None or message.from_user.id not in ADMIN_IDS:
        return
    broadcast_text = (command.args or "").strip()
    supported_media = ("photo", "audio", "document", "video", "animation")

    def contains_media(candidate: Message | None) -> bool:
        return candidate is not None and any(getattr(candidate, media_type, None) for media_type in supported_media)

    media_message = message if contains_media(message) else (
        message.reply_to_message if contains_media(message.reply_to_message) else None
    )
    if not broadcast_text and media_message is None:
        await message.reply(
            "Usage: <code>/broadcast Your message</code>, or reply to a photo with "
            "<code>/broadcast Your caption</code>. Photos, audio, and documents are supported.\n\n"
            "አጠቃቀም: <code>/broadcast መልዕክትዎ</code> ወይም ፎቶ፣ ድምፅ ወይም ሰነድን በ"
            "<code>/broadcast መግለጫዎ</code> መልሰው ይላኩ።"
        )
        return

    max_text_length = 1024 if media_message is not None else 4096
    if len(broadcast_text) > max_text_length:
        await message.reply(
            f"Broadcast text must be {max_text_length:,} characters or fewer.\n\n"
            f"የስርጭት መልዕክቱ ከ{max_text_length:,} ቁምፊዎች መብለጥ የለበትም።"
        )
        return

    recipients = sorted(ACTIVE_USER_IDS)
    if not recipients:
        await message.reply(
            "There are no registered bot users to receive this broadcast.\n\n"
            "ይህን ስርጭት የሚቀበሉ የተመዘገቡ የቦት ተጠቃሚዎች የሉም።"
        )
        return

    safe_text = escape(broadcast_text)
    semaphore = asyncio.Semaphore(32)

    async def deliver(user_id: int) -> bool:
        async with semaphore:
            try:
                if media_message is not None:
                    copy_options: dict[str, Any] = {
                        "chat_id": user_id,
                        "from_chat_id": media_message.chat.id,
                        "message_id": media_message.message_id,
                    }
                    if broadcast_text or media_message is message:
                        copy_options["caption"] = safe_text
                        copy_options["parse_mode"] = ParseMode.HTML
                    else:
                        copy_options["parse_mode"] = None
                    await bot.copy_message(**copy_options)
                else:
                    await bot.send_message(chat_id=user_id, text=safe_text)
                return True
            except TelegramAPIError:
                logger.info("Broadcast delivery failed for user %s", user_id)
                return False

    delivered = 0
    failed = 0
    for start_index in range(0, len(recipients), 200):
        batch = recipients[start_index:start_index + 200]
        results = await asyncio.gather(*(deliver(user_id) for user_id in batch))
        delivered += sum(results)
        failed += len(results) - sum(results)

    await message.reply(
        f"✅ Broadcast complete. Delivered: {delivered}. Failed: {failed}.\n\n"
        f"📣 ስርጭቱ ተጠናቋል። የደረሰላቸው: {delivered}። ያልደረሳቸው: {failed}።"
    )


@command_router.message(Command("setcode"))
async def set_booking_code(message: Message) -> None:
    if message.from_user is None or message.from_user.id not in ADMIN_IDS:
        return
    parts = (message.text or "").split(maxsplit=3)
    if len(parts) != 4:
        await message.reply(
            "⚙️ <b>Usage</b>\n<code>/setcode 1XBet CODE123 2.15</code>\n\n"
            "⚙️ <b>አጠቃቀም</b>\n<code>/setcode 1XBet CODE123 2.15</code>"
        )
        return
    _, bookie, code, odds = parts
    if bookie not in DEFAULT_CODES:
        await message.reply(
            "❌ Unknown bookmaker. Use 1XBet, SportyBet, Bet365, or Melbet.\n\n"
            "❌ ያልታወቀ የውርርድ ድርጅት ነው። 1XBet፣ SportyBet፣ Bet365 ወይም Melbet ይጠቀሙ።"
        )
        return
    if not BOOKING_CODE_RE.fullmatch(code) or not ODDS_RE.fullmatch(odds):
        await message.reply(
            "❌ Code must be 5–32 letters, numbers, or hyphens; odds must be positive.\n\n"
            "❌ ኮዱ 5–32 ፊደሎች፣ ቁጥሮች ወይም ሰረዞች መሆን አለበት፤ ኦዱም ከዜሮ በላይ ይሁን።"
        )
        return
    if Decimal(odds) <= 0:
        await message.reply("❌ Odds must be greater than zero.\n\n❌ ኦዱ ከዜሮ በላይ መሆን አለበት።")
        return
    await asyncio.to_thread(save_booking_code, bookie, code, odds)
    await message.reply(
        f"✅ <b>Code updated · {escape(bookie)}</b>\n"
        f"📌 Code: <code>{escape(code)}</code>\n"
        f"📊 Odds: <b>{escape(odds)}</b>\n\n"
        f"✅ <b>ኮዱ ተቀይሯል · {escape(bookie)}</b>\n"
        f"📌 ኮድ: <code>{escape(code)}</code>\n"
        f"📊 ኦድ: <b>{escape(odds)}</b>"
    )


@router.message(F.text)
async def fallback_text(message: Message) -> None:
    await message.answer(
        "I didn’t recognize that message. Use a menu button or send /start to reopen the menu.\n\n"
        "ይህን መልዕክት አልተረዳሁትም። ከታች ያለውን ቁልፍ ይጠቀሙ ወይም ምናሌውን ለመክፈት /start ይላኩ።",
        reply_markup=main_menu(),
    )


@router.message()
async def fallback_unsupported_message(message: Message) -> None:
    await message.answer(
        "⚠️ Only button navigation is supported!\n\n"
        "⚠️ እባክዎ የምናሌ ቁልፎቹን ብቻ ይጠቀሙ።",
        reply_markup=main_menu(),
    )


@router.callback_query()
async def fallback_callback(query: CallbackQuery) -> None:
    await query.answer(
        "This action is no longer available. Please use the menu.\n\n"
        "ይህ አማራጭ አይገኝም። እባክዎ ምናሌውን ይጠቀሙ።",
        show_alert=True,
    )


@router.channel_post()
async def enhance_channel_post(message: Message, bot: Bot) -> None:
    """Format short booking codes and append channel promotion links to channel posts."""
    original = message.text or message.caption
    if not original or PROMO_MARKER.strip() in original:
        return
    promo = (
        f'{PROMO_MARKER}\n'
        f'<a href="{PUBLIC_CHANNEL_URL}">Zan Sport News</a>\n\n'
        f'<a href="{PUBLIC_CHANNEL_URL}">ዛን ስፖርት ዜና</a>'
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


command_router.errors.register(handle_unexpected_error)


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

    session = AiohttpSession(timeout=HTTP_TIMEOUT_SECONDS, limit=HTTP_CONNECTION_LIMIT)
    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(command_router)
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