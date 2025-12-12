from __future__ import annotations
import aiohttp
import io
from datetime import datetime, timezone
import time
import json
import os
import logging, sys
from typing import Optional
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.filters import Command
import redis.asyncio as redis
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
import re
import secrets
import string
import asyncio
from typing import Dict, List, Optional
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("support-bot")
try:
    from simple_bot_manager import bot_manager
    logger.info("✅ Успешно импортирован simple_bot_manager из установленного модуля")
except ImportError as e:
    logger.warning(f"⚠️ Не удалось импортировать simple_bot_manager: {e}")
    try:
        import importlib.util
        import sys
        module_path = "simple_bot_manager.py"
        if os.path.exists(module_path):
            spec = importlib.util.spec_from_file_location("simple_bot_manager", module_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["simple_bot_manager"] = module
            spec.loader.exec_module(module)
            bot_manager = module.bot_manager
            logger.info("✅ Успешно импортирован simple_bot_manager из локального файла")
        else:
            logger.error(f"❌ Файл {module_path} не найден")
            bot_manager = None
    except Exception as e2:
        logger.error(f"❌ Ошибка при локальном импорте simple_bot_manager: {e2}")
        bot_manager = None
if bot_manager is None:
    logger.warning("⚠️ simple_bot_manager не загружен, создаем заглушку")
    class DummyBotManager:
        def __init__(self):
            self.bots = {}
        async def validate_bot_token(self, token):
            logger.warning("⚠️ Используется заглушка для validate_bot_token")
            return {"is_valid": False, "error": "Bot manager не загружен"}
        async def register_bot_instance(self, user_id, bot_token, bot_data):
            logger.warning("⚠️ Используется заглушка для register_bot_instance")
            return False
        async def start_bot_instance(self, bot_username):
            logger.warning("⚠️ Используется заглушка для start_bot_instance")
            return False
        async def stop_bot_instance(self, bot_username):
            logger.warning("⚠️ Используется заглушка для stop_bot_instance")
            return False
        async def get_bot_status(self, bot_username):
            logger.warning("⚠️ Используется заглушка для get_bot_status")
            return {
                "active": False,
                "status": "unknown",
                "users_count": 0,
                "active_users": 0,
                "total_trades": 0,
                "total_volume": 0,
                "total_deposits": 0,
                "total_withdrawals": 0,
                "uptime": "N/A"
            }
    bot_manager = DummyBotManager()
BOT_WEBHOOK_URL = os.getenv("BOT_WEBHOOK_URL", "https://your-domain.com/webhook")
BOT_MANAGEMENT_API_URL = os.getenv("BOT_MANAGEMENT_API_URL", "http://localhost:8000")
SUPPORT_BOT_TOKEN = os.getenv("SUPPORT_BOT_TOKEN", "7780936403:AAEK6oNpS5rrN2Z3SnDvvLtJ6IMCWEWZMrY")
REDIS_URL = "redis://default:UwRBirrNGabYOycgxafXyqWNu78KJH26@redis-14197.c340.ap-northeast-2-1.ec2.cloud.redislabs.com:14197"
SUPPORT_QUEUE_KEY = os.getenv("SUPPORT_QUEUE_KEY", "support:queue")
NOTIFY_QUEUE_KEY = os.getenv("NOTIFY_QUEUE_KEY", "trading:notify:ru,trading:notify:en")
SUPPORT_CHAT_ID_ENV = int(os.getenv("SUPPORT_CHAT_ID", "0"))
TRADING_BOT_USERNAME_RU = os.getenv("TRADING_BOT_USERNAME_RU", "GPT5CRYPTO_bot")
TRADING_BOT_TOKEN_RU = os.getenv("TRADING_BOT_TOKEN_RU", "8385870509:AAHdzf0X2wDITzh2hBMmY7g4CHBJ-ab8jzU")
TRADING_BOT_USERNAME_EN = TRADING_BOT_USERNAME_RU
TRADING_BOT_TOKEN_EN = TRADING_BOT_TOKEN_RU
NOTIFY_QUEUE_RU = os.getenv("NOTIFY_QUEUE_RU", "trading:notify:ru")
NOTIFY_QUEUE_EN = os.getenv("NOTIFY_QUEUE_EN", "trading:notify:en")
PAYMENT_CONFIRMATION_CHAT_ID = int(os.getenv("PAYMENT_CONFIRMATION_CHAT_ID", "-1002691532093")) 
SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "aitradingsupport_bot")
REFERRAL_CODE_KEY = "user:referral_code:{user_id}"
REFERRAL_CODE_TO_USER_KEY = "referral_code:to_user:{code}"
USER_REFERRALS_KEY = "user:referrals:{user_id}"
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "7229194724").split(",")]
MIN_DEPOSIT_GLOBAL_KEY = "config:min_deposit_global"
MIN_DEPOSIT_PER_USER_KEY = "config:min_deposit_per_user"
MIN_WITHDRAWAL_GLOBAL_KEY = "config:min_withdrawal_global"
MIN_DEPOSIT_USER_KEY = "user:{uid}:min_deposit"

USER_APPROVAL_KEY = "user:approval:pending"
USER_APPROVAL_APPROVED_KEY = "user:approval:approved"
USER_BOT_TOKENS_KEY = "user:bot_tokens:{user_id}"
BOT_OWNER_INDEX_KEY = os.getenv("BOT_OWNER_INDEX_KEY", "bot:owner_index")
SUPPORT_CHAT_ID_KEY = "support:chat_id"
USER_ROLE_KEY = "user:role:{user_id}"
USER_ACCESSIBLE_USERS_KEY = "user:accessible_users:{user_id}"
EVENT_KEY = "support:event:{event_id}"
CARD_TEMP_KEY = "support:card_temp:{event_id}"
SUPPORT_FEED_KEY = os.getenv('SUPPORT_FEED_KEY', 'support:feed')
if not SUPPORT_BOT_TOKEN:
    raise SystemExit("[ERR] SUPPORT_BOT_TOKEN is required")
bot = Bot(token=SUPPORT_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher()
r = redis.from_url(REDIS_URL, decode_responses=False)
async def set_global_min_deposit(amount: float):
    await r.set("config:min_deposit_global", str(amount))
async def get_global_min_deposit() -> float:
    raw = await r.get("config:min_deposit_global")
    if not raw:
        return 0
    return float(raw.decode())
async def set_user_min_deposit(uid: int, amount: float):
    await r.set(f"user:{uid}:min_deposit", str(amount))
async def get_user_min_deposit(uid: int) -> float:
    raw = await r.get(f"user:{uid}:min_deposit")
    if raw:
        return float(raw.decode())
    return await get_global_min_deposit()
async def send_balance_update_to_trading_bot(user_id: int, amount: float, new_balance: float):
    try:
        event = {
            "type": "balance_update_from_support",
            "user_id": user_id,
            "amount": amount,
            "new_balance": new_balance,
            "timestamp": time.time()
        }
        await r.lpush("trading:balance_updates", json.dumps(event))  
        logger.info(f"✅ Событие обновления баланса отправлено в трейдинг-бот: {event}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки события обновления баланса: {e}")
async def safe_redis_operation(operation, *args, **kwargs):
    try:
        return await operation(*args, **kwargs)
    except redis.ConnectionError as e:
        logger.error(f"❌ Redis connection error: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Redis operation error: {e}")
        return None
async def get_user_by_id(user_id: int) -> dict:
    try:
        user_data = await r.get(f"user:{user_id}")
        if user_data:
            return json.loads(user_data)
        return {
            'user_id': user_id,
            'balance': 0.0,
            'username': 'N/A',
            'language_code': 'ru'
        }
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return {
            'user_id': user_id,
            'balance': 0.0,
            'username': 'N/A',
            'language_code': 'ru'
        }
async def get_all_users_data() -> list[dict]:
    users = []
    try:
        keys = await r.keys("user:*")
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            if (":" in key_str and 
                any(x in key_str for x in [
                    ":positions", ":history", ":last10", ":assets_msg", 
                    ":wd_", ":dep_", ":wallet_ready", ":signal", ":last_signal"
                ])):
                continue
            parts = key_str.split(":")
            if len(parts) != 2 or not parts[1].isdigit():
                continue
                
            raw = await r.get(key)
            if raw:
                try:
                    user_data = json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
                    if isinstance(user_data, dict) and 'user_id' in user_data:
                        users.append(user_data)
                except Exception as e:
                    logger.warning(f"Failed to parse user data from {key_str}: {e}")
    except Exception as e:
        logger.error(f"Error getting users: {e}")
    return users
async def push_notify_event(payload: dict):
    try:
        owner_id = payload.get("bot_owner_id") or payload.get("owner_id")
        bot_code = str(payload.get("bot", "ru")).lower().strip()
        lang = "ru" if bot_code == "ru" else "en"
        if owner_id:
            queue = f"trading:notify:{owner_id}:{lang}"
        else:
            queue = f"trading:notify:admin:{lang}"
        logger.info(
            f"📤 Pushing notify event to {queue}: "
            f"owner_id={owner_id}, bot_code={bot_code}, payload={payload}"
        )
        await r.lpush(queue, json.dumps(payload).encode())
        queue_length = await r.llen(queue)
        logger.info(f"📊 Queue length for {queue}: {queue_length}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to push notify event: {e}")
        return False
async def get_user_primary_bot(user_id: int) -> Optional[dict]:
    try:
        data = await r.hgetall(USER_BOT_TOKENS_KEY.format(user_id=user_id))
        if not data:
            return None
        bots = []
        for _, raw in data.items():
            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", errors="ignore")
                bots.append(json.loads(raw))
            except Exception:
                continue
        bots = [b for b in bots if b.get("is_active", True)]
        if not bots:
            return None
        bots.sort(key=lambda b: float(b.get("created_at", 0)), reverse=True)
        return bots[0]
    except Exception:
        return None
class UserRole:
    ADMIN = "admin"
    USER = "user"
async def set_user_role(user_id: int, role: str):
    await r.set(USER_ROLE_KEY.format(user_id=user_id), role)
async def get_user_role(user_id: int) -> str:
    role = await r.get(USER_ROLE_KEY.format(user_id=user_id))
    if role:
        return role.decode() if isinstance(role, bytes) else role
    return UserRole.USER
async def is_user_admin(user_id: int) -> bool:
    test_admin_ids = [7229194724, 123456789] 
    is_admin = user_id in test_admin_ids or await get_user_role(user_id) == UserRole.ADMIN
    logger.info(f"🔍 Проверка прав администратора для {user_id}: {is_admin}")
    return is_admin
async def get_accessible_users(user_id: int) -> list[int]:
    if await is_user_admin(user_id):
        all_users = await get_all_users_data()
        return [user['user_id'] for user in all_users]
    return await get_user_referrals(user_id)
class UserApprovalStates(StatesGroup):
    WAIT_APPROVAL_REASON = State()
    WAIT_BOT_TOKEN = State()
NEXT_SEND_AT_CHAT = {}
MIN_SEND_INTERVAL_CHAT = 0.1
def _fmt_username(u):
    return f"@{u}" if u else "—"
def _escape(s: str | None) -> str:
    if not s:
        return "—"
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
async def get_owner_support_chat_id(owner_id: int | None) -> int:
    chat_id = await get_support_chat_id()
    return chat_id
@router.message(Command("queue_status"))
async def cmd_queue_status(m: Message):
    try:
        support_queue_len = await r.llen(SUPPORT_QUEUE_KEY)
        notify_queue_ru_len = await r.llen(NOTIFY_QUEUE_RU)
        notify_queue_en_len = await r.llen(NOTIFY_QUEUE_EN)
        feed_queue_len = await r.llen(SUPPORT_FEED_KEY)  
        text = (
            "📊 <b>Статус очередей</b>\n\n"
            f"🔄 Очередь поддержки: {support_queue_len}\n"
            f"📨 Очередь уведомлений RU: {notify_queue_ru_len}\n"
            f"📨 Очередь уведомлений EN: {notify_queue_en_len}\n"
            f"📝 Очередь событий: {feed_queue_len}"
        )
        if notify_queue_ru_len > 0:
            last_event_ru = await r.lindex(NOTIFY_QUEUE_RU, 0)
            if last_event_ru:
                try:
                    event_data = json.loads(last_event_ru)
                    text += f"\n\n📨 <b>Последнее в очереди RU:</b>\n{json.dumps(event_data, ensure_ascii=False)}"
                except:
                    pass                    
        if notify_queue_en_len > 0:
            last_event_en = await r.lindex(NOTIFY_QUEUE_EN, 0)
            if last_event_en:
                try:
                    event_data = json.loads(last_event_en)
                    text += f"\n\n📨 <b>Последнее в очереди EN:</b>\n{json.dumps(event_data, ensure_ascii=False)}"
                except:
                    pass       
        await m.answer(text)      
    except Exception as e:
        await m.answer(f"❌ Ошибка при проверке очередей: {e}")
async def set_support_chat_id(chat_id: int):
    await r.set(SUPPORT_CHAT_ID_KEY, str(chat_id))
async def get_support_chat_id() -> int:
    if SUPPORT_CHAT_ID_ENV:
        return SUPPORT_CHAT_ID_ENV
    raw = await r.get(SUPPORT_CHAT_ID_KEY)
    if not raw:
        return 0
    try:
        return int(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
    except Exception:
        return 0
async def save_event(ev: dict):
    event_id = ev.get("event_id")
    if not event_id:
        logger.warning(f"save_event called without event_id, skipping: {ev}")
        return
    key = EVENT_KEY.format(event_id=event_id)
    await r.set(key, json.dumps(ev))
async def get_event(event_id: str) -> Optional[dict]:
    key = EVENT_KEY.format(event_id=event_id)
    raw = await r.get(key)
    return json.loads(raw) if raw else None
def user_key(uid: int) -> str:
    return f"user:{uid}"
def balance_button():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Баланс", url=f"https://t.me/{TRADING_BOT_USERNAME_RU}?start=balance")
    ]])
def approve_deny_kb(event_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"support:approve:{event_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"support:deny:{event_id}"),
    ]])
def admin_request_pdf_kb(event_id: str):
    event_id_safe = event_id.replace(':', '_')
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📄 Запросить PDF", callback_data=f"admin_request_pdf:{event_id_safe}"),
            InlineKeyboardButton(text="✅ Подтвердить по фото", callback_data=f"admin_confirm_payment:{event_id_safe}")
        ],
        [
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{event_id_safe}")
        ]
    ])
def generate_referral_code(length=8):
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))
async def get_or_create_referral_code(user_id: int) -> str:
    existing_code = await r.get(REFERRAL_CODE_KEY.format(user_id=user_id))
    if existing_code:
        return existing_code.decode() if isinstance(existing_code, bytes) else existing_code
    while True:
        code = generate_referral_code()
        existing_user = await r.get(REFERRAL_CODE_TO_USER_KEY.format(code=code))
        if not existing_user:
            break
    await r.set(REFERRAL_CODE_KEY.format(user_id=user_id), code)
    await r.set(REFERRAL_CODE_TO_USER_KEY.format(code=code), str(user_id))
    return code
async def get_user_by_referral_code(code: str) -> Optional[int]:
    raw = await r.get(REFERRAL_CODE_TO_USER_KEY.format(code=code))
    if raw:
        try:
            return int(raw.decode() if isinstance(raw, bytes) else raw)
        except ValueError:
            return None
    return None
async def add_referral(referrer_id: int, referred_user_id: int):
    await r.sadd(USER_REFERRALS_KEY.format(user_id=referrer_id), str(referred_user_id))
async def get_user_referrals_count(user_id: int) -> int:
    return await r.scard(USER_REFERRALS_KEY.format(user_id=user_id))
async def get_user_referrals(user_id: int) -> list[int]:
    referrals = await r.smembers(USER_REFERRALS_KEY.format(user_id=user_id))
    return [int(ref.decode() if isinstance(ref, bytes) else ref) for ref in referrals]
async def get_user_referrer(user_id: int) -> Optional[int]:
    try:
        referrer_raw = await r.get(f"user:{user_id}:referrer")
        if referrer_raw:
            return int(referrer_raw.decode() if isinstance(referrer_raw, bytes) else referrer_raw)
    except Exception as e:
        logger.error(f"Error getting referrer for user {user_id}: {e}")
    return None
async def set_user_referrer(user_id: int, referrer_id: int, ttl_days: Optional[int] = None):
    try:
        key = f"user:{user_id}:referrer"
        if ttl_days and ttl_days > 0:
            await r.setex(key, ttl_days * 86400, str(referrer_id))
        else:
            await r.set(key, str(referrer_id))
        logger.info(f"Referrer set: user={user_id} -> referrer={referrer_id}")
    except Exception as e:
        logger.error(f"Error setting referrer for user {user_id} -> {referrer_id}: {e}")
async def get_temp_data(event_id: str) -> Optional[dict]:
    temp_data = await get_card_temp(event_id)
    if temp_data:
        return temp_data
    general_temp_key = f"card_temp:{event_id}"
    raw = await r.get(general_temp_key)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    ev = await get_event(event_id)
    if ev:
        return {
            'user_id': ev.get('user_id'),
            'amount': ev.get('amount'),
            'bot_code': ev.get('bot', 'ru'),
            'event_id': event_id
        }
    return None
async def is_bot_available_for_user(user_id: int) -> bool:
    try:
        chat = await bot.get_chat(user_id)
        return True
    except Exception:
        return False
async def safe_send_text(chat_id: int, text: str, **kwargs):
    try:
        if not await is_bot_available_for_user(chat_id):
            logger.warning(f"Bot is not available for user {chat_id}")
            return None        
        now = time.time()
        wait = max(0.0, NEXT_SEND_AT_CHAT.get(chat_id, 0.0) - now)
        if wait > 0:
            await asyncio.sleep(min(wait, 1.0))       
        try:
            msg = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            NEXT_SEND_AT_CHAT[chat_id] = time.time() + MIN_SEND_INTERVAL_CHAT
            return msg
        except TelegramRetryAfter as e:
            delay = float(getattr(e, "retry_after", 1.0))
            if delay > 8:
                async def delayed():
                    try:
                        await asyncio.sleep(delay + 0.1)
                        await bot.send_message(chat_id=chat_id, text=text, **kwargs)
                    except Exception:
                        logger.exception("Delayed send failed")
                asyncio.create_task(delayed())
                NEXT_SEND_AT_CHAT[chat_id] = time.time() + delay
                return None
            await asyncio.sleep(delay + 0.05)
            try:
                msg = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
                NEXT_SEND_AT_CHAT[chat_id] = time.time() + MIN_SEND_INTERVAL_CHAT
                return msg
            except Exception:
                logger.exception("Send after retry failed")
                return None
        except Exception as e:
            logger.error(f"Failed to send message to {chat_id}: {e}")
            return None            
    except Exception as e:
        logger.error(f"Error in safe_send_text for user {chat_id}: {e}")
        return None
async def send_message_to_user_via_trading_bot(
    user_id: int,
    text: str,
    reply_markup=None,
    bot_code: str = "ru",
    bot_username: str | None = None,
) -> bool:
    try:
        logger.info(f"🔍 Attempting to send message to user {user_id}, bot_code={bot_code}")
        token = None
        if bot_username:
            try:
                raw = await r.hget(USER_BOT_TOKENS_KEY.format(user_id=user_id), bot_username)
                if raw:
                    data = json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
                    token = data.get("token")
                    if token:
                        logger.info(f"✅ Using user bot token: {bot_username}")
            except Exception as e:
                logger.error(f"Error getting user bot token: {e}")
        if not token:
            if str(bot_code).lower() == "en":
                token = TRADING_BOT_TOKEN_EN
                logger.info("✅ Using default EN bot token")
            else:
                token = TRADING_BOT_TOKEN_RU
                logger.info("✅ Using default RU bot token")   
        trb = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        await trb.send_message(
            chat_id=user_id, 
            text=text, 
            reply_markup=reply_markup
        )  
        logger.info(f"✅ Message sent successfully to user {user_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send message to user {user_id}: {e}")
        return False
    finally:
        try:
            if 'trb' in locals() and trb.session:
                await trb.session.close()
        except Exception:
            pass
async def credit_balance(user_id: int, amount: float, bot_code: str = "ru") -> None:
    try:
        await push_notify_event({
            "type": "balance_credit",
            "user_id": user_id,
            "amount": amount,
            "bot": bot_code,
            "timestamp": time.time()
        })
        logger.info(f"Balance credit requested: user_id={user_id}, amount={amount}, bot={bot_code}")
    except Exception as e:
        logger.error(f"Error requesting balance credit: {e}")
class SupportStates(StatesGroup):
    WAIT_CARD_DETAILS = State()
    WAIT_PAYMENT_PROOF = State()
    WAIT_APPROVAL_REASON = State()
    WAIT_BOT_TOKEN = State()  
    WAIT_BALANCE_AMOUNT = State()
    WAIT_BALANCE_CONFIRMATION = State()
    WAIT_VERIFICATION_REASON = State() 
async def save_card_temp(event_id: str, data: dict):
    key = CARD_TEMP_KEY.format(event_id=event_id)
    await r.setex(key, 3600, json.dumps(data))
async def get_card_temp(event_id: str) -> Optional[dict]:
    key = CARD_TEMP_KEY.format(event_id=event_id)
    raw = await r.get(key)
    return json.loads(raw) if raw else None
async def delete_card_temp(event_id: str):
    try:
        key = CARD_TEMP_KEY.format(event_id=event_id)
        await r.delete(key)
        logger.info(f"🗑️ Deleted card temp data for {event_id}")
    except Exception as e:
        logger.error(f"Error deleting card temp {event_id}: {e}")
async def find_payment_data_support(event_id: str) -> Optional[dict]:
    temp_data = await get_card_temp(event_id)
    if temp_data:
        return temp_data
    general_temp_key = f"card_temp:{event_id}"
    raw = await r.get(general_temp_key)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    ev = await get_event(event_id)
    if ev:
        return {
            'user_id': ev.get('user_id'),
            'amount': ev.get('amount'),
            'username': ev.get('username'),
            'bot_code': ev.get('bot', 'ru'),
            'event_id': event_id
        }
    return None
@router.callback_query(F.data == "confirm_balance_change")
async def confirm_balance_change(cb: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        user_id = data.get('target_user_id')
        amount = data.get('amount')
        if not user_id or amount is None:
            await cb.answer("❌ Ошибка: данные не найдены", show_alert=True)
            await state.clear()
            return
        user_data = await get_user_by_id(user_id)
        if not user_data:
            await cb.answer("❌ Пользователь не найден", show_alert=True)
            await state.clear()
            return
        user_language = "ru"  
        if 'language_code' in user_data:
            user_language = user_data['language_code']
        elif 'language' in user_data:
            user_language = user_data['language']
        try:
            trading_user_data = await r.get(f"user:{user_id}")
            if trading_user_data:
                trading_user = json.loads(trading_user_data)
                if trading_user.get('language_code'):
                    user_language = trading_user['language_code']
                elif trading_user.get('language'):
                    user_language = trading_user['language']
        except Exception as e:
            logger.warning(f"Не удалось получить язык из трейдинг-бота для пользователя {user_id}: {e}")
        bot_code = "ru"
        if user_language and user_language.lower() in ['en', 'english']:
            bot_code = "en"
        logger.info(f"🌐 Определен язык пользователя {user_id}: {user_language} -> bot_code: {bot_code}")
        old_balance = user_data.get('balance', 0)
        new_balance = old_balance + amount
        user_data['balance'] = new_balance
        user_data['last_activity'] = time.time()
        await r.set(f"user:{user_id}", json.dumps(user_data))
        logger.info(f"✅ Баланс пользователя {user_id} обновлен: ${old_balance:.2f} -> ${new_balance:.2f}")
        await push_notify_event({
            "type": "balance_credit",
            "user_id": user_id,
            "amount": amount,
            "new_balance": new_balance,
            "old_balance": old_balance,
            "bot": bot_code,
            "reason": "admin_manual_adjustment",
            "admin_id": cb.from_user.id,
            "timestamp": time.time()
        })
        await r.lpush("trading:balance_updates", json.dumps({
            "type": "balance_update",
            "user_id": user_id,
            "amount": amount,
            "new_balance": new_balance,
            "old_balance": old_balance,
            "reason": "admin_adjustment",
            "timestamp": time.time()
        }))
        if bot_code == "en":
            if amount > 0:
                user_message = (
                    f"💰 <b>Funds have been credited to your balance</b>\n\n"
                    f"Amount credited to your account: <b>${amount:.2f}</b>\n"
                    f"New balance: <b>${new_balance:.2f}</b>\n\n"
                )
            else:
                user_message = (
                    f"💰 <b>Amount deducted from your balance</b>\n\n"
                    f"Amount deducted from your account: <b>${abs(amount):.2f}</b>\n"
                    f"New balance: <b>${new_balance:.2f}</b>\n\n"
                )
        else:
            if amount > 0:
                user_message = (
                    f"💰 <b>Средства зачислены на ваш баланс</b>\n\n"
                    f"Зачислено на ваш счёт: <b>${amount:.2f}</b>\n"
                    f"Новый баланс: <b>${new_balance:.2f}</b>\n\n"
                )
            else:
                user_message = (
                    f"💰 <b>Произведено списание с вашего баланса</b>\n\n"
                    f"С вашего счёта списано: <b>${abs(amount):.2f}</b>\n"
                    f"Новый баланс: <b>${new_balance:.2f}</b>\n\n"
                )
        success = await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            bot_code=bot_code
        )
        updated_user = await get_user_by_id(user_id)
        updated_balance = updated_user.get('balance', 0) if updated_user else 0
        logger.info(f"🔍 Проверка баланса после обновления: ${updated_balance:.2f}")
        operation_type = "🟢 Зачисление" if amount > 0 else "🔴 Списание"
        notification_status = "уведомлён" if success else "НЕ уведомлён"
        language_status = "английский" if bot_code == "en" else "русский"
        await cb.message.edit_text(
            f"✅ <b>Баланс успешно изменён</b>\n\n"
            f"👤 Пользователь: @{user_data.get('username', 'N/A')}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"📊 Операция: {operation_type}\n"
            f"💳 Сумма: <b>${abs(amount):.2f}</b>\n"
            f"💰 Старый баланс: ${old_balance:.2f}\n"
            f"💰 Новый баланс: <b>${new_balance:.2f}</b>\n"
            f"🌐 Язык уведомления: {language_status}\n"
            f"📨 Пользователь: {notification_status}\n\n"
            f"💾 Данные обновлены в системе"
        )
        await cb.answer("✅ Баланс успешно изменён")
        await state.clear()
    except Exception as e:
        logger.error(f"Error in confirm_balance_change: {e}")
        await cb.answer("❌ Ошибка при изменении баланса", show_alert=True)
        await state.clear()
@router.message(SupportStates.WAIT_BALANCE_AMOUNT)
async def process_balance_amount(m: Message, state: FSMContext):
    try:
        amount_text = m.text.strip()
        try:
            amount = float(amount_text)
        except ValueError:
            await m.answer("❌ Неверный формат суммы. Введите число:\n<i>Пример: 100.50 или -50.25</i>")
            return
        data = await state.get_data()
        user_id = data.get('target_user_id')
        if not user_id:
            await m.answer("❌ Ошибка: не найден ID пользователя")
            await state.clear()
            return
        user_data = await get_user_by_id(user_id)
        username = user_data.get('username', 'N/A') if user_data else 'N/A'
        current_balance = user_data.get('balance', 0) if user_data else 0
        await state.update_data(amount=amount)
        operation_type = "🟢 Зачисление" if amount > 0 else "🔴 Списание"
        new_balance = current_balance + amount
        await m.answer(
            f"💰 <b>Подтверждение изменения баланса</b>\n\n"
            f"👤 Пользователь: @{username}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"💵 Текущий баланс: <b>${current_balance:.2f}</b>\n\n"
            f"📊 Операция: {operation_type}\n"
            f"💳 Сумма: <b>${abs(amount):.2f}</b>\n"
            f"💰 Новый баланс: <b>${new_balance:.2f}</b>\n\n"
            "Подтвердите операцию:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_balance_change"),
                    InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_balance_change")
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Error in process_balance_amount: {e}")
        await m.answer("❌ Ошибка при обработке суммы")
        await state.clear()
@router.callback_query(F.data == "confirm_balance_change")
async def confirm_balance_change(cb: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        user_id = data.get('target_user_id')
        amount = data.get('amount')
        if not user_id or amount is None:
            await cb.answer("❌ Ошибка: данные не найдены", show_alert=True)
            await state.clear()
            return
        user = await get_user_by_id(user_id)
        if not user:
            await cb.answer("❌ Пользователь не найден", show_alert=True)
            await state.clear()
            return
        old_balance = user.get('balance', 0)
        new_balance = old_balance + amount
        user['balance'] = new_balance
        user['last_activity'] = time.time()
        await r.set(f"user:{user_id}", json.dumps(user))
        logger.info(f"✅ Баланс пользователя {user_id} обновлен: ${old_balance:.2f} -> ${new_balance:.2f}")
        bot_code = "ru"  
        user_language = user.get('language', 'ru')
        if user_language and user_language.lower() in ['en', 'english']:
            bot_code = "en"
        await push_notify_event({
            "type": "balance_credit",
            "user_id": user_id,
            "amount": amount,
            "new_balance": new_balance,
            "old_balance": old_balance,
            "bot": bot_code,
            "reason": "admin_manual_adjustment",
            "admin_id": cb.from_user.id,
            "timestamp": time.time()
        })
        await r.lpush("trading:balance_updates", json.dumps({
            "type": "balance_update",
            "user_id": user_id,
            "amount": amount,
            "new_balance": new_balance,
            "old_balance": old_balance,
            "reason": "admin_adjustment",
            "timestamp": time.time()
        }))
        if bot_code == "en":
            if amount > 0:
                user_message = (
                    f"💰 <b>Funds have been credited to your balance</b>\n"
                    f"Amount credited to your account: <b>${amount:.2f}</b>\n"
                )
            else:
                user_message = (
                    f"💰 <b>Amount deducted from your balance</b>\n\n"
                    f"Amount deducted from your account: <b>${abs(amount):.2f}</b>\n"
                    f"If you have any questions, please contact support."
                )
        else:
            if amount > 0:
                user_message = (
                    f"💰 <b>Средства возвращены на ваш баланс</b>\n"
                    f"Зачислено на ваш счёт: <b>${amount:.2f}</b>\n"
                    f"Спасибо за использование нашего сервиса!"
                )
            else:
                user_message = (
                    f"💰 <b>Произведено списание с вашего баланса</b>\n"
                    f"С вашего счёта списано: <b>${abs(amount):.2f}</b>\n"
                    f"Если у вас есть вопросы, обратитесь в поддержку."
                )
        success = await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            bot_code=bot_code
        )
        updated_user = await get_user_by_id(user_id)
        updated_balance = updated_user.get('balance', 0) if updated_user else 0
        logger.info(f"🔍 Проверка баланса после обновления: ${updated_balance:.2f}")
        operation_type = "🟢 Зачисление" if amount > 0 else "🔴 Списание"
        notification_status = "уведомлён" if success else "НЕ уведомлён"
        language_status = "английский" if bot_code == "en" else "русский"
        await cb.message.edit_text(
            f"✅ <b>Баланс успешно изменён</b>\n\n"
            f"👤 Пользователь: @{user.get('username', 'N/A')}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"📊 Операция: {operation_type}\n"
            f"💳 Сумма: <b>${abs(amount):.2f}</b>\n"
            f"💰 Старый баланс: ${old_balance:.2f}\n"
            f"💰 Новый баланс: <b>${new_balance:.2f}</b>\n"
            f"🌐 Язык уведомления: {language_status}\n"
            f"📨 Пользователь: {notification_status}\n\n"
            f"💾 Данные обновлены в системе"
        )
        await cb.answer("✅ Баланс успешно изменён")
        await state.clear()
    except Exception as e:
        logger.error(f"Error in confirm_balance_change: {e}")
        await cb.answer("❌ Ошибка при изменении баланса", show_alert=True)
        await state.clear()
@router.callback_query(F.data == "cancel_balance_change")
async def cancel_balance_change(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("❌ <b>Изменение баланса отменено</b>")
    await cb.answer("Операция отменена")
@router.callback_query(F.data.startswith("debug_event:"))
async def debug_event(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("debug_event:", 1)[1]
        event_id = event_id_safe.replace('_', ':')        
        temp_data = await get_temp_data(event_id)
        ev = await get_event(event_id)        
        debug_info = f"🔍 <b>Debug Event</b>\n\nEvent ID: {event_id}\n\n"
        debug_info += f"<b>Temp Data:</b>\n{json.dumps(temp_data, indent=2, ensure_ascii=False)}\n\n"
        debug_info += f"<b>Event Data:</b>\n{json.dumps(ev, indent=2, ensure_ascii=False)}"        
        await cb.message.answer(debug_info)
        await cb.answer()        
    except Exception as e:
        await cb.answer(f"Debug error: {e}", show_alert=True)
def card_payment_kb(event_id: str):
    event_id_safe = event_id.replace(':', '_')
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💳 Отправить карту", callback_data=f"card_send_details:{event_id_safe}"),
            InlineKeyboardButton(text="❌ Отказать", callback_data=f"card_reject:{event_id_safe}") 
        ]
    ])
def user_confirm_payment_kb(event_id: str, bot_code: str = "ru"):
    event_id_safe = event_id.replace(':', '_')   
    if str(bot_code).lower() == "en":
        confirm_text = "✅ Confirm payment"
        cancel_text = "❌ Cancel"
    else:
        confirm_text = "✅ Подтвердить оплату"
        cancel_text = "❌ Отмена"   
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=confirm_text,
                callback_data=f"user_confirm_payment:{event_id_safe}"
            ),
            InlineKeyboardButton(
                text=cancel_text,
                callback_data=f"user_cancel_payment:{event_id_safe}"
            )
        ]
    ])
def admin_confirm_payment_kb(event_id: str):
    event_id_safe = event_id.replace(':', '_')
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_payment:{event_id_safe}"),
            InlineKeyboardButton(text="📄 Запросить PDF", callback_data=f"admin_request_pdf:{event_id_safe}")
        ],
        [
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{event_id_safe}"),
            InlineKeyboardButton(text="🐛 Debug", callback_data=f"debug_event:{event_id_safe}")
        ]
    ])
@router.callback_query(F.data.startswith("card_reject:"))
async def card_reject_handler(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("card_reject:", 1)[1]
        event_id = event_id_safe.replace("_", ":")
        ev = await get_event(event_id)
        if not ev:
            await cb.answer("❌ Событие не найдено", show_alert=True)
            return
        user_id = ev.get("user_id")
        amount = ev.get("amount", 0)
        fio = ev.get("fio", "Не указано")
        bank = ev.get("bank", "Не указан")
        bot_code = (ev.get("bot") or "ru").lower()
        bot_username = ev.get("bot_username")  
        support_start_param = f"reject_{user_id}_{bot_username or 'default'}_{bot_code}"
        if bot_code == "en":
            text = (
                "❌ <b>Top-up request declined</b>\n\n"
                f"Top-up for ${amount} has been declined.\n"
                f"Full name: {fio}\n"
                f"Bank: {bank}\n\n"
                "Please contact support for details."
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📞 Contact Support", 
                    url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start={support_start_param}"
                )]
            ])
        else:
            text = (
                "❌ <b>Запрос на пополнение отклонён</b>\n\n"
                f"Пополнение на сумму ${amount} отклонено.\n"
                f"ФИО: {fio}\n"
                f"Банк: {bank}\n\n"
                "Свяжитесь с поддержкой для уточнения деталей."
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📞 Поддержка", 
                    url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start={support_start_param}"
                )]
            ])
        await send_message_to_user_via_trading_bot(
            user_id,
            text,
            reply_markup=keyboard,
            bot_code=bot_code,
            bot_username=bot_username
        )
        await cb.message.edit_text(
            "❌ <b>Пополнение отклонено</b>\n\n"
            f"👤 Пользователь: {ev.get('username', 'N/A')}\n"
            f"🆔 ID: {user_id}\n"
            f"👤 ФИО: {fio}\n"
            f"🏦 Банк: {bank}\n"
            f"💵 Сумма: ${amount}\n"
            f"🤖 Трейдинг бот: {bot_username or 'неизвестно'}\n"
            f"🌐 Язык: {'английский' if bot_code == 'en' else 'русский'}\n"
            f"🔗 Параметр поддержки: {support_start_param}\n"
            "👤 Пользователь уведомлён об отклонении"
        )
        await cb.answer("Пополнение отклонено")
    except Exception as e:
        logger.error(f"Error in card_reject_handler: {e}")
        await cb.answer("Ошибка при отклонении", show_alert=True)
@router.callback_query(F.data.startswith("card_send_details:"))
async def card_send_details(cb: CallbackQuery, state: FSMContext):
    try:
        event_id_safe = cb.data.split("card_send_details:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        await state.update_data(event_id=event_id)
        await state.set_state(SupportStates.WAIT_CARD_DETAILS)       
        await cb.message.edit_text(
            "💳 <b>Отправка реквизитов карты</b>\n\n"
            "Введите данные карты в формате:\n"
            "<code>Номер карты | Имя Фамилия | Срок действия</code>\n\n"
            "Пример:\n"
            "<code>1234 5678 9012 3456 | IVAN IVANOV | 12/25</code>"
        )
        await cb.answer()
    except Exception as e:
        logger.error(f"Error in card_send_details: {e}")
        await cb.answer("Ошибка при обработке", show_alert=True)
@router.message(SupportStates.WAIT_CARD_DETAILS)
async def process_card_details(m: Message, state: FSMContext):
    event_id = None
    try:
        data = await state.get_data()
        event_id = data.get('event_id')        
        if not event_id:
            await m.answer("❌ Ошибка: не найден event_id")
            await state.clear()
            return
        ev = await get_event(event_id)
        if not ev:
            await m.answer("❌ Событие не найдено")
            await state.clear()
            return
        card_data = m.text.strip()
        parts = [part.strip() for part in card_data.split('|')]        
        if len(parts) < 3:
            await m.answer("❌ Неверный формат. Используйте: Номер карты | Имя Фамилия | Срок действия")
            return       
        card_number, card_holder, expiry_date = parts[0], parts[1], parts[2]       
        user_id = ev.get('user_id')
        amount = ev.get('amount', 0)
        amount_rub = ev.get('amount_rub', amount * 91.10) 
        fio = ev.get('fio', 'Не указано')
        bank = ev.get('bank', 'Не указан')
        bot_code = (ev.get('bot') or 'ru').lower()    
        logger.info(f"🔄 Sending card details to user {user_id} via {bot_code} bot")
        temp_data = {
            'event_id': event_id,
            'card_number': card_number,
            'card_holder': card_holder,
            'expiry_date': expiry_date,
            'user_id': user_id,
            'username': ev.get('username'),
            'amount': amount,
            'amount_rub': amount_rub,
            'fio': fio,
            'bank': bank,
            'admin_id': m.from_user.id,
            'bot_code': bot_code,
            'timestamp': time.time()
        }
        await save_card_temp(event_id, temp_data)
        general_temp_key = f"card_temp:{event_id}"
        await r.setex(general_temp_key, 7200, json.dumps(temp_data))       
        logger.info(f"✅ Card data saved for {bot_code} bot: {event_id}")
        user_message = (
            f"💳 <b>Payment details</b>\n\n"
            f"Amount to pay: <b>{int(amount_rub)} RUB (${amount})</b>\n\n" 
            f"<b>Card details:</b>\n"
            f"Number: <code>{card_number}</code>\n"
            f"Holder: {card_holder}\n"
            f"Expiry: {expiry_date}\n\n"
            f"After payment, click the confirmation button below:"
        ) if bot_code == 'en' else (
            f"💳 <b>Реквизиты для оплаты</b>\n\n"
            f"Сумма к оплате: <b>{int(amount_rub)} RUB (${amount})</b>\n\n" 
            f"<b>Данные карты:</b>\n"
            f"Номер: <code>{card_number}</code>\n"
            f"Держатель: {card_holder}\n"
            f"Срок: {expiry_date}\n\n"
            f"После оплаты нажмите кнопку подтверждения ниже:"
        )
        success = await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            reply_markup=user_confirm_payment_kb(event_id, bot_code=bot_code),
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )
        if success:
            bot_info = "английского" if bot_code == 'en' else "русского"
            await m.answer(
                f"✅ <b>Реквизиты отправлены пользователю</b>\n\n"
                f"👤 Пользователь: {ev.get('username', 'N/A')}\n"
                f"🤖 Через: {bot_info} бот\n"
                f"💵 Сумма: {int(amount_rub)} RUB (${amount})\n"
                f"👤 ФИО: {fio}\n"
                f"🏦 Банк: {bank}\n"
                f"💳 Карта: {card_number}\n\n"
                f"Ожидаем подтверждения оплаты от пользователя..."
            )            
            logger.info(f"✅ Card details sent to user {user_id} via {bot_code} bot, event_id: {event_id}")
        else:
            await m.answer(
                f"❌ <b>Не удалось отправить реквизиты пользователю</b>\n\n"
                f"Пользователь, возможно, не запускал бота.\n"
                f"ID пользователя: {user_id}\n"
                f"Бот: {'английский' if bot_code == 'en' else 'русский'}"
            )      
        await state.clear()        
    except Exception as e:
        logger.error(f"❌ Error in process_card_details. Event ID: {event_id}. Error: {e}")
        await m.answer("❌ Ошибка при обработке данных карты")
        await state.clear()
@router.callback_query(F.data.startswith("user_confirm_payment:"))
async def user_confirm_payment_handler(cb: CallbackQuery, state: FSMContext):
    try:
        event_id_safe = cb.data.split("user_confirm_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        temp_data = await get_card_temp(event_id)
        if not temp_data:
            await cb.answer("❌ Данные оплаты не найдены", show_alert=True)
            return       
        await state.update_data(event_id=event_id)
        await state.set_state(SupportStates.WAIT_PAYMENT_PROOF)        
        await cb.message.edit_text(
            "📎 <b>Подтверждение оплаты</b>\n\n"
            "Пожалуйста, отправьте подтверждение оплаты:\n"
            "• 📸 Фото квитанции/чека\n"
            "• 📄 PDF-документ с подтверждением\n\n"
            "<i>Отправьте файл как фото или документ</i>"
        )
        await cb.answer()       
    except Exception as e:
        logger.error(f"Error in user_confirm_payment_handler: {e}")
        await cb.answer("Ошибка при обработке", show_alert=True)
@router.callback_query(F.data.startswith("user_cancel_payment:"))
async def user_cancel_payment_handler(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("user_cancel_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        ev = await get_event(event_id)
        temp_data = await get_card_temp(event_id)
        if ev:
            bot_code = (ev.get("bot") or "ru").lower()
            bot_username = ev.get("bot_username")
            admin_id = temp_data.get('admin_id') if temp_data else None
            support_start_param = f"cancel_{ev.get('user_id')}_{bot_username or 'default'}_{bot_code}"
            if admin_id:
                await bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"❌ <b>Пользователь отменил оплату</b>\n\n"
                        f"Пользователь: {ev.get('username', 'N/A')}\n"
                        f"ID: {ev.get('user_id', 'N/A')}\n"
                        f"Сумма: ${ev.get('amount', 0)}\n"
                        f"Трейдинг бот: {bot_username or 'неизвестно'}\n"
                        f"Язык: {'английский' if bot_code == 'en' else 'русский'}\n"
                        f"🔗 Параметр поддержки: {support_start_param}"
                    )
                )
        if bot_code == "en":
            user_text = (
                "❌ <b>Payment cancelled</b>\n\n"
                "If you have any questions, please contact our support team:"
            )
            user_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📞 Contact Support", 
                    url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start={support_start_param}"
                )]
            ])
        else:
            user_text = (
                "❌ <b>Оплата отменена</b>\n\n"
                "Если у вас есть вопросы, обратитесь в нашу службу поддержки:"
            )
            user_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📞 Поддержка", 
                    url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start={support_start_param}"
                )]
            ])
        await cb.message.edit_text(user_text, reply_markup=user_keyboard)
        await delete_card_temp(event_id)
        await cb.answer()        
    except Exception as e:
        logger.error(f"Error in user_cancel_payment_handler: {e}")
        await cb.answer("Ошибка при обработке", show_alert=True)
@router.message(SupportStates.WAIT_PAYMENT_PROOF, F.photo | F.document)
async def process_payment_proof(m: Message, state: FSMContext):
    try:
        data = await state.get_data()
        event_id = data.get('event_id')       
        if not event_id:
            await m.answer("❌ Ошибка: не найден event_id")
            await state.clear()
            return        
        temp_data = await get_card_temp(event_id)
        if not temp_data:
            await m.answer("❌ Данные оплаты не найдены")
            await state.clear()
            return
        ev = await get_event(event_id)
        if not ev:
            await m.answer("❌ Событие не найдено")
            await state.clear()
            return
        admin_id = temp_data.get('admin_id')
        amount = temp_data.get('amount')
        card_number = temp_data.get('card_number')
        bot_code = temp_data.get('bot_code', 'ru')
        file_id = None
        file_type = None
        if m.photo:
            file_id = m.photo[-1].file_id
            file_type = "photo"
            logger.info(f"📸 Получено фото подтверждения: {file_id}")
            proof_message = (
                f"📎 <b>Подтверждение оплаты</b>\n\n"
                f"👤 Пользователь: @{m.from_user.username or m.from_user.id}\n"
                f"💵 Сумма: ${amount}\n"
                f"💳 Карта: {card_number}\n"
                f"📁 Тип файла: Фото\n\n"
                f"Вы можете запросить PDF-версию квитанции или подтвердить оплату:"
            )
            try:
                await bot.send_photo(
                    chat_id=admin_id,
                    photo=file_id,
                    caption=proof_message,
                    reply_markup=admin_confirm_payment_kb(event_id) 
                )
                await m.answer(
                    "✅ <b>Квитанция отправлена на проверку</b>\n\n"
                    "Администратор проверит вашу квитанцию. Если потребуется PDF-версия, с вами свяжутся."
                )
            except Exception as e:
                logger.error(f"❌ Ошибка отправки квитанции админу: {e}")
                await m.answer("❌ Не удалось отправить квитанцию администратору")
        elif m.document:
            if m.document.mime_type == 'application/pdf' or m.document.file_name.lower().endswith('.pdf'):
                file_id = m.document.file_id
                file_type = "document"
                logger.info(f"📄 Получен PDF документ: {m.document.file_name}")
                proof_message = (
                    f"📎 <b>Получена квитанция об оплате (PDF)</b>\n\n"
                    f"👤 Пользователь: @{m.from_user.username or m.from_user.id}\n"
                    f"💵 Сумма: ${amount}\n"
                    f"💳 Карта: {card_number}\n"
                    f"📁 Тип файла: PDF-документ\n\n"
                    f"Подтвердите получение платежа:"
                )
                try:
                    await bot.send_document(
                        chat_id=admin_id,
                        document=file_id,
                        caption=proof_message,
                        reply_markup=admin_confirm_payment_kb(event_id) 
                    )
                    await m.answer(
                        "✅ <b>PDF-квитанция отправлена на проверку</b>\n\n"
                        "Ожидайте подтверждения платежа администратором."
                    )
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки PDF админу: {e}")
                    await m.answer("❌ Не удалось отправить PDF администратору")
            else:
                await m.answer("❌ Пожалуйста, отправьте фото или PDF-документ")
                return
        if not file_id:
            await m.answer("❌ Не удалось получить файл подтверждения")
            await state.clear()
            return
        await state.clear()
    except Exception as e:
        logger.error(f"Error in process_payment_proof: {e}")
        await m.answer("❌ Ошибка при обработке квитанции")
        await state.clear()
@router.callback_query(F.data.startswith("admin_confirm_payment:"))
async def admin_confirm_payment(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_confirm_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"🔄 Admin confirming payment: event_id={event_id}")
        temp_data = await find_payment_data_support(event_id)
        if not temp_data:
            await cb.answer("❌ Данные платежа не найдены", show_alert=True)
            return
        user_id = temp_data.get('user_id')
        amount = temp_data.get('amount', 0)
        bot_code = temp_data.get('bot_code', 'ru')
        if not user_id:
            await cb.answer("❌ Не найден user_id в данных платежа", show_alert=True)
            return
        if temp_data.get("payment_processed"):
            await cb.answer("✅ Платёж уже был обработан ранее", show_alert=True)
            return
        user_data = await get_user_by_id(user_id)
        if not user_data:
            await cb.answer("❌ Пользователь не найден", show_alert=True)
            return
        old_balance = user_data.get('balance', 0)
        new_balance = old_balance + amount
        user_data['balance'] = new_balance
        user_data['last_activity'] = time.time()
        if 'stats' not in user_data:
            user_data['stats'] = {}
        if 'total_deposits' not in user_data['stats']:
            user_data['stats']['total_deposits'] = 0
        user_data['stats']['total_deposits'] += amount
        await r.set(f"user:{user_id}", json.dumps(user_data))
        await send_balance_update_to_trading_bot(user_id, amount, new_balance)
        await push_notify_event({
            "type": "balance_update_from_support", 
            "user_id": user_id,
            "amount": amount,
            "new_balance": new_balance,
            "bot": bot_code,
            "reason": "payment_confirmed",
            "admin_id": cb.from_user.id,
            "timestamp": time.time()
        })
        if bot_code == "en":
            user_message = (
                f"✅ <b>Payment confirmed!</b>\n\n"
                f"Your deposit of <b>${amount:.2f}</b> has been successfully processed.\n"
                f"💰 New balance: <b>${new_balance:.2f}</b>\n\n"
                f"Thank you for your payment!"
            )
        else:
            user_message = (
                f"✅ <b>Платёж подтверждён!</b>\n\n"
                f"Ваш депозит на сумму <b>${amount:.2f}</b> успешно зачислен.\n"
                f"💰 Новый баланс: <b>${new_balance:.2f}</b>\n\n"
                f"Спасибо за пополнение!"
            )
        success = await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            bot_code=bot_code,
            bot_username=temp_data.get("bot_username")
        )
        temp_data["payment_processed"] = True
        await save_card_temp(event_id, temp_data)
        admin_text = (
            "✅ <b>Платеж подтвержден</b>\n\n"
            f"👤 Пользователь: @{temp_data.get('username', 'N/A')}\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"💰 Баланс до: ${old_balance:.2f}\n"
            f"💰 Баланс после: ${new_balance:.2f}\n"
            f"📨 Пользователь уведомлён: {'✅ Да' if success else '❌ Нет'}\n"
            f"✅ Статус: ЗАЧИСЛЕНО"
        )
        try:
            msg = cb.message
            if getattr(msg, "photo", None) or getattr(msg, "document", None):
                await msg.edit_caption(admin_text)
            else:
                await msg.edit_text(admin_text)
        except Exception as e:
            logger.error(f"Failed to edit admin message after confirm: {e}")
        await delete_card_temp(event_id)
        await cb.answer("Платеж подтвержден и средства зачислены")
    except Exception as e:
        logger.error(f"Error in admin_confirm_payment: {e}")
        await cb.answer("Ошибка при подтверждении", show_alert=True)
async def find_payment_data_support(event_id: str) -> Optional[dict]:
    temp_data = await get_card_temp(event_id)
    if temp_data:
        return temp_data
    general_temp_key = f"card_temp:{event_id}"
    raw = await r.get(general_temp_key)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    ev = await get_event(event_id)
    if ev:
        return {
            'user_id': ev.get('user_id'),
            'amount': ev.get('amount'),
            'username': ev.get('username'),
            'bot_code': ev.get('bot', 'ru'),
            'bot_username': ev.get('bot_username'),
            'event_id': event_id
        }
    return None        
async def increment_deposits(self, uid: int, amount: float):
    try:
        user = await self.get_user(uid)
        old_balance = user.balance
        user.balance += amount
        user.stats.total_deposits += amount
        await self.save_user(user)
        logger.info(f"✅ Баланс пользователя {uid} увеличен на ${amount:.2f}, старый: ${old_balance:.2f}, новый: ${user.balance:.2f}")
    except Exception as e:
        logger.error(f"❌ Ошибка при увеличении баланса пользователя {uid}: {e}")

@router.callback_query(F.data.startswith("admin_wd_approve:"))
async def admin_wd_approve_handler(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_wd_approve:", 1)[1]
        event_id = event_id_safe.replace("_", ":")
        ev = await get_event(event_id)
        if not ev:
            await cb.answer("❌ Событие не найдено", show_alert=True)
            return
        user_id = ev.get("user_id")
        amount = ev.get("amount", 0)
        bot_code = (ev.get("bot") or "ru").lower()
        await push_notify_event({
            "type": "withdraw_approved",
            "user_id": user_id,
            "amount": amount,
            "bot": bot_code,
            "event_id": event_id,
            "timestamp": time.time()
        })
        if bot_code == "en":
            user_message = f"✅ <b>Withdrawal approved!</b>\n\nYour withdrawal request for ${amount} has been approved and will be processed shortly."
        else:
            user_message = f"✅ <b>Вывод подтверждён!</b>\n\nВаша заявка на вывод ${amount} подтверждена и будет обработана в ближайшее время."    
        await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )
        await cb.message.edit_text(f"✅ Вывод подтверждён! Пользователь уведомлён.")
        await cb.answer()
    except Exception as e:
        logger.error(f"Error in admin_wd_approve_handler: {e}")
        await cb.answer("❌ Ошибка при подтверждении вывода", show_alert=True)
@router.callback_query(F.data.startswith("admin_reject_payment:"))
async def admin_reject_payment_support(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_reject_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')        
        logger.info(f"🔄 Support bot: Admin rejecting payment: {event_id}")
        temp_data = await find_payment_data_support(event_id)        
        if not temp_data:
            await cb.answer("❌ Данные платежа не найдены", show_alert=True)
            return       
        user_id = temp_data.get('user_id')
        amount = temp_data.get('amount', 0)
        notification = {
            "type": "payment_rejected", 
            "event_id": event_id,
            "user_id": user_id,
            "amount": amount,
            "admin_id": cb.from_user.id,
            "timestamp": time.time()
        }
        await r.lpush("trading:notify:ru", json.dumps(notification))
        await cb.message.edit_text(
            f"❌ <b>Платеж отклонен</b>\n\n"
            f"👤 Пользователь: @{temp_data.get('username', 'N/A')}\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"🔄 Уведомление отправлено в трейдинг-бот"
        )
        await cb.answer("Платеж отклонен")
    except Exception as e:
        logger.error(f"Support bot error in admin_reject_payment: {e}")
        await cb.answer("Ошибка при отклонении", show_alert=True)
@router.callback_query(F.data.startswith("admin_request_pdf:"))
async def admin_request_pdf_handler(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_request_pdf:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"Admin requesting PDF receipt: event_id={event_id}")
        temp_data = await get_card_temp(event_id)
        if not temp_data:
            await cb.answer("❌ Данные оплаты не найдены", show_alert=True)
            return
        ev = await get_event(event_id)
        if not ev:
            await cb.answer("❌ Событие не найдено", show_alert=True)
            return
        user_id = temp_data.get('user_id')
        amount = temp_data.get('amount')
        bot_code = temp_data.get('bot_code', 'ru')
        await save_card_temp(event_id, {
            **temp_data,
            'pdf_requested': True,
            'pdf_requested_at': time.time(),
            'pdf_requested_by': cb.from_user.id
        })
        if bot_code == 'en':
            user_message = (
                "📄 <b>PDF Receipt Requested</b>\n\n"
                f"Administrator requested PDF receipt for your payment of <b>${amount}</b>.\n\n"
                "Please send the PDF receipt/document for verification.\n\n"
                "<i>Send the file as a document (PDF format)</i>\n\n"
                "⚠️ <b>Important:</b> Send the PDF file directly as a document (not as photo)."
            )
        else:
            user_message = (
                "📄 <b>Запрошена PDF-квитанция</b>\n\n"
                f"Администратор запросил PDF-версию квитанции для вашего платежа на <b>${amount}</b>.\n\n"
                "Пожалуйста, отправьте PDF-квитанцию/документ для проверки.\n\n"
                "<i>Отправьте файл как документ (формат PDF)</i>\n\n"
                "⚠️ <b>Важно:</b> Отправляйте PDF файл напрямую как документ (не как фото)."
            )
        success = await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )      
        if success:
            await cb.message.edit_text(
                cb.message.text + "\n\n📄 <b>Запрос PDF отправлен пользователю</b>"
            )
            await cb.answer("Запрос PDF отправлен пользователю")
            logger.info(f"✅ PDF request sent to user {user_id} for event {event_id}")
        else:
            await cb.answer("❌ Не удалось отправить запрос пользователю", show_alert=True)
    except Exception as e:
        logger.error(f"Error in admin_request_pdf_handler: {e}")
        await cb.answer("Ошибка при запросе PDF", show_alert=True)
@router.message(F.photo & F.chat.type == "private")
async def handle_photo_message(m: Message):
    try:
        user_id = m.from_user.id
        keys_pattern = CARD_TEMP_KEY.format(event_id="*").replace(":", "\\:")
        all_temp_keys = await r.keys(keys_pattern)
        found_event_id = None
        temp_data = None
        for key in all_temp_keys:
            try:
                key_str = key.decode() if isinstance(key, bytes) else key
                event_id = key_str.split(":")[-1]
                
                data = await get_card_temp(event_id)
                if data and data.get('user_id') == user_id and data.get('pdf_requested'):
                    found_event_id = event_id
                    temp_data = data
                    break
            except Exception:
                continue
        if not found_event_id or not temp_data:
            return  
        admin_id = temp_data.get('admin_id')
        amount = temp_data.get('amount')
        photo_message = (
            f"📸 <b>Пользователь отправил фото вместо PDF</b>\n\n"
            f"👤 Пользователь: @{m.from_user.username or m.from_user.id}\n"
            f"💵 Сумма: ${amount}\n\n"
            f"Вы можете повторно запросить PDF или подтвердить оплату по фото:"
        )
        await bot.send_photo(
            chat_id=admin_id,
            photo=m.photo[-1].file_id,
            caption=photo_message,
            reply_markup=admin_request_pdf_kb(found_event_id)
        )
        await m.answer(
            "📸 <b>Фото отправлено администратору</b>\n\n"
            "Администратор получил ваше фото. Если потребуется PDF-версия, с вами свяжутся."
        )
    except Exception as e:
        logger.error(f"Error in handle_photo_message: {e}")
async def attach_existing_user_to_referrer(user_id: int, referral_code: str) -> bool:
    try:
        existing_referrer = await get_user_referrer(user_id)
        if existing_referrer:
            logger.info(f"User {user_id} already has referrer: {existing_referrer}")
            return False
        referrer_id = await get_user_by_referral_code(referral_code)
        if not referrer_id:
            logger.warning(f"Referral code not found: {referral_code}")
            return False
        if referrer_id == user_id:
            logger.warning(f"User {user_id} tried to refer themselves")
            return False
        await r.setex(f"user:{user_id}:referrer", 86400 * 30, str(referrer_id))
        existing_refs = await get_user_referrals(referrer_id)
        if user_id not in existing_refs:
            await add_referral(referrer_id, user_id)
            logger.info(f"Existing user {user_id} attached to referrer {referrer_id}")
            user_data = await get_user_by_id(user_id)
            username = user_data.get('username') if user_data else None
            await push_notify_event({
                "type": "referral_registered",
                "referrer_id": referrer_id,
                "referred_user_id": user_id,
                "referred_username": username,
                "timestamp": time.time(),
                "is_existing_user": True  
            })
            try:
                await bot.send_message(
                    chat_id=referrer_id,
                    text=(
                        "🎉 <b>Новый реферал по вашей ссылке!</b>\n\n"
                        f"По вашей реферальной ссылке зашел существующий пользователь:\n"
                        f"👤 @{username or 'без username'}\n"
                        f"🆔 ID: <code>{user_id}</code>\n\n"
                        f"Теперь он будет учитываться в вашей реферальной статистике!\n"
                        f"Используйте /refstats для просмотра статистики"
                    )
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить реферера {referrer_id}: {e}")
        return True
    except Exception as e:
        logger.error(f"Error attaching existing user to referrer: {e}")
        return False
@router.message(Command("ref_attach"))
async def cmd_ref_attach(m: Message):
    user_id = m.from_user.id
    existing_user = await get_user_by_id(user_id)
    if not existing_user:
        await m.answer("❌ Сначала зарегистрируйтесь через /start")
        return
    existing_referrer = await get_user_referrer(user_id)
    if existing_referrer:
        await m.answer(
            f"❌ Вы уже привязаны к реферальной программе\n"
            f"Ваш реферер: ID {existing_referrer}"
        )
        return
    if len(m.text.split()) < 2:
        await m.answer(
            "🔗 <b>Привязка к реферальной программе</b>\n\n"
            "Использование:\n"
            "<code>/ref_attach CODE</code>\n\n"
            "Где CODE - реферальный код пользователя\n"
            "Пример: <code>/ref_attach ABC123</code>"
        )
        return
    referral_code = m.text.split()[1].strip().upper()
    success = await attach_existing_user_to_referrer(user_id, referral_code)
    if success:
        await m.answer(
            "✅ <b>Успешная привязка!</b>\n\n"
            "Теперь вы участвуете в реферальной программе.\n"
            "Используйте /refstats чтобы посмотреть статистику."
        )
    else:
        await m.answer(
            "❌ <b>Не удалось привязаться</b>\n\n"
            "Возможные причины:\n"
            "• Неверный реферальный код\n"
            "• Вы уже привязаны к реферальной программе\n"
            "• Попытка пригласить самого себя\n"
            "• Техническая ошибка"
        )
@router.message(Command("refstats"))
async def cmd_refstats(m: Message):
    user_id = m.from_user.id
    existing_user = await get_user_by_id(user_id)
    if not existing_user:
        await m.answer("❌ Сначала зарегистрируйтесь через /start")
        return
    referral_code = await get_or_create_referral_code(user_id)
    referral_link = f"https://t.me/{TRADING_BOT_USERNAME_RU}?start=ref_{referral_code}"
    referrer_id = await get_user_referrer(user_id)
    referrer_info = ""
    if referrer_id:
        referrer_data = await get_user_by_id(referrer_id)
        referrer_name = f"@{referrer_data.get('username')}" if referrer_data and referrer_data.get('username') else f"ID {referrer_id}"
        referrer_info = f"👤 <b>Ваш реферер:</b> {referrer_name}\n\n"
@router.message(Command("setchat"))
async def cmd_setchat(m: Message):
    chat_id = m.chat.id
    await set_support_chat_id(chat_id)
    await m.answer(f"Чат назначен для уведомлений: {chat_id}")
    logger.info("Bound support chat id: %s", chat_id)
@router.message(Command("getchat"))
async def cmd_getchat(m: Message):
    cid = await get_support_chat_id()
    await m.answer(f"Текущий support chat id: {cid}")
@router.message(Command("queue"))
async def cmd_queue(m: Message):
    try:
        length = await r.llen(SUPPORT_QUEUE_KEY)
        await m.answer(f"Длина очереди: {length}")
    except Exception as e:
        await m.answer(f"Ошибка при чтении очереди: {e}")
async def process_assets_opened_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping assets opened notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        text = (
            "💰 <b>Пользователь открыл активы</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'}\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь просматривает свой баланс и открытые позиции."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Assets opened notification sent for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Error processing assets_opened event: {e}")
async def process_deposit_opened_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping deposit opened notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = "ru"
        if bot_username and "en" in bot_username.lower():
            bot_code = "en"
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "💰 <b>Пользователь открыл меню пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь собирается пополнить баланс."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Deposit opened notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing deposit_opened event: {e}")
async def process_bank_card_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping bank card selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        text = (
            "💳 <b>Пользователь выбрал оплату банковской картой</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'}\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал способ оплаты банковской картой и переходит к выбору суммы."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Bank card selected notification sent for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Error processing bank_card_selected event: {e}")
async def process_crypto_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping crypto selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "₿ <b>Пользователь выбрал криптовалюту для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал пополнение через криптовалюту и переходит к выбору токена."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Crypto selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing crypto_selected event: {e}")
async def process_usdt_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping USDT selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "💎 <b>Пользователь выбрал USDT для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал USDT для пополнения баланса."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ USDT selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing usdt_selected event: {e}")
async def process_ethereum_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping Ethereum selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "🔷 <b>Пользователь выбрал Ethereum для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал Ethereum (ETH) для пополнения баланса."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Ethereum selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing ethereum_selected event: {e}")
async def process_bitcoin_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping Bitcoin selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "🟡 <b>Пользователь выбрал Bitcoin для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал Bitcoin (BTC) для пополнения баланса."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Bitcoin selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing bitcoin_selected event: {e}")
async def process_generic_token_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        token = event_data.get("token", "Unknown")
        token_display = event_data.get("token_display", token)
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping generic token selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            f"💰 <b>Пользователь выбрал {token_display} для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал {token_display} для пополнения баланса."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ {token_display} selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing generic token selected event: {e}")
async def process_deposit_amount_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        amount = event_data.get("amount", 0)
        token = event_data.get("token", "USDT")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping deposit amount selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        amount_category = ""
        if amount == 500:
            amount_category = "минимальную"
        elif amount == 1000:
            amount_category = "стандартную"
        elif amount == 2500:
            amount_category = "среднюю"
        elif amount == 5000:
            amount_category = "максимальную"
        else:
            amount_category = f"${amount}"
        text = (
            f"💰 <b>Пользователь выбрал сумму для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Сумма: <b>${amount}</b> ({amount_category})\n"
            f"💰 Токен: {token}\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал сумму для пополнения и переходит к подтверждению платежа."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Deposit amount selected notification sent for user {user_id}, amount: ${amount}, token: {token}")
    except Exception as e:
        logger.error(f"❌ Error processing deposit_amount_selected event: {e}")
async def process_deposit_network_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        network = event_data.get("network", "Unknown")
        token = event_data.get("token", "USDT")
        amount = event_data.get("amount", 0)
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping deposit network selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"        
        network_display = get_network_display_name_support(network)
        text = (
            f"🌐 <b>Пользователь выбрал сеть для пополнения</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💰 Токен: {token}\n"
            f"💵 Сумма: <b>${amount}</b>\n"
            f"🌐 Сеть: <b>{network_display}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал сеть для пополнения и готов к получению реквизитов."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Deposit network selected notification sent for user {user_id}, network: {network}, token: {token}, amount: ${amount}")
    except Exception as e:
        logger.error(f"❌ Error processing deposit_network_selected event: {e}")
def get_network_display_name_support(network):
    network_display_map = {
        "TRC20": "TRC20 (Tron) 🚀",
        "ERC20": "ERC20 (Ethereum) 🔷", 
        "BEP20": "BEP20 (Binance Smart Chain) ⚡",
        "BTC": "Bitcoin Network 🟡",
        "ETH": "Ethereum Network 🔷",
        "POLYGON": "Polygon Network 💜",
        "ARBITRUM": "Arbitrum Network 🔵",
        "OPTIMISM": "Optimism Network 🟢"
    }
    return network_display_map.get(network, f"{network} 🌐")
async def process_withdraw_opened_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping withdraw opened notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        text = (
            "💰 <b>Пользователь открыл меню вывода</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь собирается вывести средства с баланса."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Withdraw opened notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing withdraw_opened event: {e}")
async def process_withdraw_crypto_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping withdraw crypto selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        text = (
            "₿ <b>Пользователь выбрал вывод криптовалютой</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал вывод средств через криптовалюту."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Withdraw crypto selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing withdraw_crypto_selected event: {e}")
async def process_withdraw_card_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping withdraw card selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        text = (
            "💳 <b>Пользователь выбрал вывод на банковскую карту</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал вывод средств на банковскую карту."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
        )
        logger.info(f"✅ Withdraw card selected notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing withdraw_card_selected event: {e}")
async def process_withdraw_token_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        token = event_data.get("token", "USDT")
        token_display = event_data.get("token_display", token)
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping withdraw token selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        token_icons = {
            "USDT": "💎",
            "ETH": "🔷", 
            "ETHEREUM": "🔷",
            "BTC": "🟡",
            "BITCOIN": "🟡"
        }
        token_icon = token_icons.get(token, "💰")
        text = (
            f"{token_icon} <b>Пользователь выбрал токен для вывода</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💰 Токен: <b>{token_display}</b>\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал {token_display} для вывода средств."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Withdraw token selected notification sent for user {user_id}, token: {token_display}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing withdraw_token_selected event: {e}")
async def process_withdraw_network_selected_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        token = event_data.get("token", "USDT")
        network = event_data.get("network", "TRC20")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping withdraw network selected notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        network_display = get_network_display_name_support(network)
        text = (
            f"🌐 <b>Пользователь выбрал сеть для вывода</b>\n\n"
            f"👤 Пользователь: @{username or 'без username'}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💰 Токен: {token}\n"
            f"🌐 Сеть: <b>{network_display}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь выбрал сеть для вывода {token}."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text
        )
        logger.info(f"✅ Withdraw network selected notification sent for user {user_id}, network: {network}, token: {token}")
    except Exception as e:
        logger.error(f"❌ Error processing withdraw_network_selected event: {e}")
async def process_user_started_bot_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        is_new_user = event_data.get("is_new_user", False)
        ref_code = event_data.get("ref_code")
        bot_username = event_data.get("bot_username", "")
        language_code = event_data.get("language_code", "unknown")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping user started bot notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        user_status = "🆕 НОВЫЙ пользователь" if is_new_user else "↩️ Возврат пользователя"
        ref_info = f"🔗 Реферальный код: {ref_code}" if ref_code else "🔗 Реферальный код: нет"
        text = (
            f"🚀 <b>{user_status}</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'}\n"
            f"🌐 Язык: {language_code}\n"
            f"{ref_info}\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь запустил трейдинг бота."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                ),
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ User started bot notification sent for user {user_id}, new_user={is_new_user}")
    except Exception as e:
        logger.error(f"❌ Error processing user_started_bot event: {e}")
async def process_user_registered_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        ref_code = event_data.get("ref_code")
        chat_id = await get_support_chat_id()
        if not chat_id:
            return
        text = (
            "🎉 <b>НОВАЯ РЕГИСТРАЦИЯ</b>\n\n"
            f"👤 Пользователь: @{username or 'без username'}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🔗 Реферальный код: {ref_code or 'нет'}\n\n"
            f"Пользователь зарегистрировался в системе."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ]
        ])
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
        logger.info(f"✅ User registered notification sent for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Error processing user_registered event: {e}")
async def process_user_returned_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        language = event_data.get("language", "unknown")
        chat_id = await get_support_chat_id()
        if not chat_id:
            return
        text = (
            "↩️ <b>ВОЗВРАТ ПОЛЬЗОВАТЕЛЯ</b>\n\n"
            f"👤 Пользователь: @{username or 'без username'}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🌐 Язык: {language}\n\n"
            f"Пользователь вернулся в бота после перерыва."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                ),
                InlineKeyboardButton(
                    text="💰 Баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                )
            ]
        ])
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
        logger.info(f"✅ User returned notification sent for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Error processing user_returned event: {e}")
async def process_open_positions_opened_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping open positions opened notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "📊 <b>Пользователь открыл список сделок</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь просматривает свои открытые торговые позиции."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Open positions opened notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing open_positions_opened event: {e}")
async def process_trade_history_opened_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping trade history opened notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        text = (
            "📊 <b>Пользователь открыл историю сделок</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь просматривает историю своих торговых операций."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Trade history opened notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing trade_history_opened event: {e}")
async def process_trade_history_page_viewed_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        page = event_data.get("page", 0)
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping trade history page viewed notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "📄 <b>Пользователь просматривает историю сделок</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"📄 Страница: <b>{page + 1}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь просматривает {page + 1} страницу истории сделок."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text
        )
        logger.info(f"✅ Trade history page viewed notification sent for user {user_id}, page: {page + 1}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing trade_history_page_viewed event: {e}")
async def process_ai_trading_started_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping AI trading started notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "🤖 <b>Пользователь запустил AI Трейдинг</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь активировал автоматический режим AI Трейдинга."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ AI Trading started notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing ai_trading_started event: {e}")
async def process_ai_trading_stopped_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping AI trading stopped notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "⏹️ <b>Пользователь остановил AI Трейдинг</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь остановил автоматический режим AI Трейдинга."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ AI Trading stopped notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing ai_trading_stopped event: {e}")
async def process_settings_opened_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping settings opened notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        text = (
            "⚙️ <b>Пользователь открыл настройки</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь просматривает настройки бота."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Settings opened notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing settings_opened event: {e}")
async def process_open_market_clicked_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping open market clicked notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        user_data = await get_user_by_id(user_id)
        balance = user_data.get('balance', 0) if user_data else 0
        text = (
            "📈 <b>Пользователь открыл сделку из сигнала</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"💵 Текущий баланс: <b>${balance:.2f}</b>\n"
            f"⏰ Время: {event_time}\n\n"
            f"Пользователь начал торговлю на рынке."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Open market clicked notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing open_market_clicked event: {e}")
async def process_position_closed_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        position_id = event_data.get("position_id")
        symbol = event_data.get("symbol")
        side = event_data.get("side")
        entry_price = event_data.get("entry_price")
        exit_price = event_data.get("exit_price")
        pnl_abs = event_data.get("pnl_abs", 0)
        pnl_pct = event_data.get("pnl_pct", 0)
        closed_by = event_data.get("closed_by")
        order_amount = event_data.get("order_amount", 0)
        leverage = event_data.get("leverage", 1)
        duration_sec = event_data.get("duration_sec", 0)
        bot_username = event_data.get("bot_username", "")
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping position closed notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        closed_by_info = {
            "TP": ("✅", "Take Profit"),
            "SL": ("❌", "Stop Loss"), 
            "TIME": ("⏱️", "По времени")
        }
        icon, close_type_text = closed_by_info.get(closed_by, ("📊", "Неизвестно"))
        side_text = "LONG" if side == "LONG" else "SHORT"
        side_icon = "🟢" if side == "LONG" else "🔴"
        pnl_abs_formatted = f"+${pnl_abs:.2f}" if pnl_abs >= 0 else f"-${abs(pnl_abs):.2f}"
        pnl_pct_formatted = f"+{pnl_pct:.2f}%" if pnl_pct >= 0 else f"{pnl_pct:.2f}%"
        pnl_icon = "📈" if pnl_abs >= 0 else "📉"
        duration_min = duration_sec // 60
        duration_sec_remaining = duration_sec % 60
        text = (
            f"{icon} <b>Сделка закрыта</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n\n"
            f"📊 <b>Детали сделки:</b>\n"
            f"• Символ: <b>{symbol}</b>\n"
            f"• Сторона: {side_icon} {side_text}\n"
            f"• Позиция: ${order_amount:.2f} (x{leverage})\n"
            f"• Вход: {entry_price:.5f}\n"
            f"• Выход: {exit_price:.5f}\n"
            f"• Причина: {close_type_text}\n"
            f"• Длительность: {duration_min}м {duration_sec_remaining}с\n\n"
            f"{pnl_icon} <b>Результат:</b> {pnl_abs_formatted} ({pnl_pct_formatted})"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                ),
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                ),
                InlineKeyboardButton(
                    text="📊 История сделок", 
                    url=f"https://t.me/{bot_username}?start=history"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Position closed notification sent for user {user_id}, symbol: {symbol}, PnL: ${pnl_abs:.2f}")
    except Exception as e:
        logger.error(f"❌ Error processing position_closed event: {e}")
async def process_bot_blocked_event(event_data: dict):
    try:
        user_id = event_data.get("user_id")
        bot_username = event_data.get("bot_username", "unknown")
        reason = event_data.get("reason", "unknown")
        timestamp = event_data.get("timestamp", time.time())
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping bot blocked notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        text = (
            "🚫 <b>Пользователь заблокировал трейдинг-бота</b>\n\n"
            f"👤 Пользователь ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username}\n"
            f"⏰ Время: {event_time}\n"
            f"⚠️ Причина: {reason}\n\n"
            f"<i>Бот автоматически удален из списка наблюдателей</i>"
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text
        )
        logger.info(f"✅ Bot blocked notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing bot_blocked event: {e}")
async def process_bot_unblocked_event(event_data):
    try:
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        first_name = event_data.get("first_name", "")
        last_name = event_data.get("last_name", "")
        bot_username = event_data.get("bot_username", "")
        was_blocked = event_data.get("was_blocked", False)
        timestamp = event_data.get("timestamp", time.time())
        first_name = first_name or ""
        last_name = last_name or ""
        full_name = (first_name + " " + last_name).strip() or "без имени"
        chat_id = await get_support_chat_id()
        if not chat_id:
            logger.warning("Support chat not bound, skipping bot unblocked notification")
            return
        event_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        bot_code = event_data.get("bot", "ru")
        bot_language = "русский" if bot_code == "ru" else "английский"
        text = (
            "✅ <b>Бот разблокирован пользователем</b>\n\n"
            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🤖 Бот: @{bot_username or 'неизвестно'} ({bot_language})\n"
            f"⏰ Время: {event_time}\n"
            f"📊 Был заблокирован: {'Да' if was_blocked else 'Нет'}\n\n"
            f"<i>Пользователь снова начал использовать бота</i>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Детали пользователя", 
                    callback_data=f"user_detail:{user_id}"
                ),
                InlineKeyboardButton(
                    text="💰 Изменить баланс", 
                    callback_data=f"admin_change_balance:{user_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💳 Баланс пользователя", 
                    url=f"https://t.me/{bot_username}?start=balance"
                )
            ]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb
        )
        logger.info(f"✅ Bot unblocked notification sent for user {user_id}, bot: {bot_username}")
    except Exception as e:
        logger.error(f"❌ Error processing bot_unblocked event: {e}")
async def process_queue():
    logger.info("Queue worker started, key=%s", SUPPORT_QUEUE_KEY)
    while True:
        try:
            item = await r.brpop(SUPPORT_QUEUE_KEY, timeout=5)
            if not item:
                await asyncio.sleep(0.1)
                continue
            _, data = item
            try:
                ev = json.loads(
                    data.decode() if isinstance(data, (bytes, bytearray)) else data
                )
            except Exception:
                logger.warning("Bad event json: %r", data)
                continue
            logger.info(f"📨 Processing event from queue: {ev.get('type')}")
            logger.info(f"🔍 Full event data: {ev}")
            await save_event(ev)
            chat_id = await get_support_chat_id()
            if not chat_id:
                await r.lpush(SUPPORT_QUEUE_KEY, json.dumps(ev).encode())
                logger.warning("Support chat not bound, event requeued")
                await asyncio.sleep(2)
                continue
            event_type = ev.get("type")
            if event_type == "settings_opened":
                await process_settings_opened_event(ev)
            elif event_type == "bot_blocked":
                await process_bot_blocked_event(ev)
            elif event_type == "bot_unblocked":
                await process_bot_unblocked_event(ev)
            elif event_type == "open_market_clicked":
                await process_open_market_clicked_event(ev)
            elif event_type == "position_closed":
                await process_position_closed_event(ev)
            elif event_type == "open_market_clicked": 
                await process_open_market_clicked_event(ev) 
            elif event_type == "ai_trading_started":
                await process_ai_trading_started_event(ev)
            elif event_type == "ai_trading_stopped":
                await process_ai_trading_stopped_event(ev)
            elif event_type == "trade_history_opened":
                await process_trade_history_opened_event(ev)
            elif event_type == "trade_history_page_viewed":
                await process_trade_history_page_viewed_event(ev)
            elif event_type == "deposit_network_selected":
                await process_deposit_network_selected_event(ev)
            elif event_type == "open_positions_opened":
                await process_open_positions_opened_event(ev)
            elif event_type == "user_registered":
                await process_user_registered_event(ev)
            elif event_type == "user_returned":
                await process_user_returned_event(ev)
            elif event_type == "user_started_bot":
                await process_user_started_bot_event(ev)
            elif event_type == "withdraw_card_selected":
                await process_withdraw_card_selected_event(ev)    
            elif event_type == "withdraw_opened":
                await process_withdraw_opened_event(ev)
            elif event_type == "withdraw_crypto_selected":
                await process_withdraw_crypto_selected_event(ev)
            elif event_type == "withdraw_network_selected":
                await process_withdraw_network_selected_event(ev)
            elif event_type == "withdraw_token_selected":
                await process_withdraw_token_selected_event(ev)
            elif event_type == "deposit_amount_selected":
                await process_deposit_amount_selected_event(ev)
            elif event_type == "usdt_selected":
                await process_usdt_selected_event(ev)
            elif event_type == "ethereum_selected":
                await process_ethereum_selected_event(ev)
            elif event_type == "bitcoin_selected":
                await process_bitcoin_selected_event(ev)
            elif event_type == "crypto_selected":
                await process_crypto_selected_event(ev)
            elif event_type == "bank_card_selected":
                await process_bank_card_selected_event(ev)
            elif event_type == "assets_opened":
                await process_assets_opened_event(ev)
            elif event_type == "deposit_opened":
                await process_deposit_opened_event(ev)
            elif event_type == "withdraw_request":
                uname = ev.get("username") or str(ev.get("user_id"))
                display_amount = ev.get(
                    "display_amount", f"${ev.get('amount', 0):.2f}"
                )
                token = ev.get("token", "USDT")
                network = ev.get("network", "TRC20")
                address = ev.get("address", "")
                bot_code = ev.get("bot", "ru")
                bot_code = bot_code.lower()
                logger.info(f"🔍 Withdraw request - Bot code from event: {bot_code}, Bot username: {ev.get('bot_username')}")
                trading_bot_username = (
                    TRADING_BOT_USERNAME_RU
                    if bot_code == "ru"
                    else TRADING_BOT_USERNAME_EN
                )
                text = (
                    "🔄 <b>Запрос на вывод</b>\n\n"
                    f"👤 Пользователь: @{uname}\n"
                    f"🆔 ID: {ev.get('user_id')}\n"
                    f"🤖 Бот: {'английский' if bot_code == 'en' else 'русский'} (@{ev.get('bot_username', '?')})\n"
                    f"💵 Сумма: {display_amount} {token}\n"
                    f"🌐 Сеть: {network}\n"
                    f"📮 Адрес: <code>{address}</code>"
                )
                event_id_safe = ev.get("event_id", "").replace(":", "_")
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="✅ Одобрить",
                                callback_data=f"admin_wd_approve:{event_id_safe}",
                            ),
                            InlineKeyboardButton(
                                text="❌ Отказ: реквизиты",
                                callback_data=f"admin_wd_reject_requisites:{event_id_safe}",
                            ),
                        ],
                        [
                            InlineKeyboardButton(
                                text="💳 Баланс",
                                url=f"https://t.me/{trading_bot_username}?start=balance",
                            )
                        ],
                    ]
                )
                await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
            elif event_type == "withdraw_request_card":
                uname = ev.get("username") or str(ev.get("user_id"))
                display_amount = ev.get(
                    "display_amount", f"${ev.get('amount', 0):.2f}"
                )
                amount_rub = ev.get(
                    "amount_rub", float(ev.get("amount", 0)) * 91.10
                )
                fio = ev.get("fio", "Не указано")
                bank = ev.get("bank", "Не указан")
                card_number = ev.get("card_number", "")
                bot_code = ev.get("bot", "ru")
                bot_code = bot_code.lower()
                logger.info(f"🔍 Card withdraw request - Bot code from event: {bot_code}, Bot username: {ev.get('bot_username')}")
                trading_bot_username = (
                    TRADING_BOT_USERNAME_RU
                    if bot_code == "ru"
                    else TRADING_BOT_USERNAME_EN
                )
                text = (
                    "💳 <b>Запрос на вывод на карту</b>\n\n"
                    f"👤 Пользователь: @{uname}\n"
                    f"🆔 ID: {ev.get('user_id')}\n"
                    f"🤖 Бот: {'английский' if bot_code == 'en' else 'русский'} (@{ev.get('bot_username', '?')})\n"
                    f"💵 Сумма: {display_amount} (~{amount_rub:.0f} RUB)\n"
                    f"👤 ФИО: {fio}\n"
                    f"🏦 Банк: {bank}\n"
                    f"💳 Номер карты: <code>{card_number}</code>"
                )
                event_id_safe = ev.get("event_id", "").replace(":", "_")
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="✅ Одобрить",
                                callback_data=f"admin_wd_approve:{event_id_safe}",
                            ),
                            InlineKeyboardButton(
                                text="❌ Отклонить",
                                callback_data=f"admin_wd_reject:{event_id_safe}",
                            ),
                        ],
                        [
                            InlineKeyboardButton(
                                text="💳 Баланс",
                                url=f"https://t.me/{trading_bot_username}?start=balance",
                            )
                        ],
                    ]
                )
                await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
            elif event_type == "card":
                uname = ev.get("username") or str(ev.get("user_id"))
                amt = ev.get("amount")
                amount_rub = ev.get("amount_rub", amt * 90)
                fio = ev.get("fio", "Не указано")
                bank = ev.get("bank", "Не указан")
                bot_code = ev.get("bot", "ru")
                bot_code = bot_code.lower()  
                logger.info(f"🔍 Card deposit - Bot code from event: {bot_code}, Bot username: {ev.get('bot_username')}")
                bot_info = "английского" if bot_code == "en" else "русского"
                text = (
                    "💳 <b>Запрос на пополнение картой</b>\n\n"
                    f"👤 Пользователь: @{uname}\n"
                    f"🤖 Бот: {bot_info} (@{ev.get('bot_username', '?')})\n"
                    f"🆔 ID: {ev.get('user_id')}\n"
                    f"👤 ФИО: {fio}\n"
                    f"🏦 Банк: {bank}\n"
                    f"💵 Сумма: ${amt} (~{amount_rub:.0f} RUB)\n\n"
                    "Отправьте пользователю реквизиты карты:"
                )
                await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=card_payment_kb(ev["event_id"]),
                )
            elif event_type == "crypto":
                await process_crypto_event(ev)
            elif event_type == "user_started":
                user_id = ev.get("user_id")
                username = ev.get("username")
                first_name = ev.get("first_name") or ""
                last_name = ev.get("last_name") or ""
                bot_username = ev.get("bot_username") or ev.get("bot")
                bot_code = ev.get("bot", "ru")
                lang_code = (ev.get("language_code") or "").lower()
                if lang_code == "en":
                    bot_code = "en"
                bot_code = bot_code.lower()
                full_name = (first_name + " " + last_name).strip() or "без имени"
                bot_owner_id_raw = await r.hget(BOT_OWNER_INDEX_KEY, bot_username)
                if bot_owner_id_raw:
                    try:
                        bot_owner_id = int(
                            bot_owner_id_raw.decode() if isinstance(bot_owner_id_raw, (bytes, bytearray)) else bot_owner_id_raw
                        )
                        if user_id and bot_owner_id and user_id != bot_owner_id:
                            current_ref = await get_user_referrer(user_id)
                            if not current_ref:
                                await set_user_referrer(user_id, bot_owner_id)
                                await add_referral(bot_owner_id, user_id)
                                await push_notify_event({
                                    "type": "referral_registered",
                                    "referrer_id": bot_owner_id,
                                    "referred_user_id": user_id,
                                    "referred_username": username,
                                    "timestamp": time.time(),
                                    "bot": bot_code,
                                })
                        text = (
                            "🚀 <b>Новый пользователь в вашем боте!</b>\n\n"
                            f"🤖 Ваш бот: @{bot_username}\n"
                            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
                            f"🆔 ID: <code>{user_id}</code>\n"
                            f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        await bot.send_message(bot_owner_id, text)
                        logger.info(f"✅ Sent user_started notification to bot owner {bot_owner_id} for user_id={user_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send user_started notification to bot owner: {e}")
                        support_chat_id = await get_support_chat_id()
                        if support_chat_id:
                            fallback_text = (
                                "🚀 <b>Пользователь нажал /start в трейдинг-боте</b>\n\n"
                                f"🤖 Бот: @{bot_username or 'неизвестно'} (русский)\n"
                                f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
                                f"🆔 ID: <code>{user_id}</code>\n"
                                f"⚠️ Не удалось уведомить создателя бота: {e}"
                            )
                            await bot.send_message(support_chat_id, fallback_text)
                else:
                    support_chat_id = await get_support_chat_id()
                    if support_chat_id:
                        text = (
                            "🚀 <b>Пользователь нажал /start в трейдинг-боте</b>\n\n"
                            f"🤖 Бот: @{bot_username or 'неизвестно'} (русский)\n"
                            f"👤 Пользователь: {full_name} (@{username or 'без username'})\n"
                            f"🆔 ID: <code>{user_id}</code>\n"
                            f"⚠️ Создатель бота не найден"
                        )
                        await bot.send_message(support_chat_id, text)
                        logger.info(f"Bot owner not found for {bot_username}, sent to support chat")
            elif event_type == "language_selected":
                if "event_id" not in ev:
                    ev["event_id"] = f"lang_ev:{int(time.time() * 1000)}"
                user_id = ev.get("user_id")
                username = ev.get("username")
                first_name = ev.get("first_name") or ""
                last_name = ev.get("last_name") or ""
                bot_username = ev.get("bot_username") or ev.get("bot")
                selected_language = ev.get("language", "unknown")
                lang_display = "🇷🇺 Русский" if selected_language == "ru" else "🇺🇸 English"
                await save_event(ev)
                bot_owner_id_raw = await r.hget(BOT_OWNER_INDEX_KEY, bot_username)
                notification_sent = False
                if bot_owner_id_raw:
                    try:
                        bot_owner_id = int(
                            bot_owner_id_raw.decode() if isinstance(bot_owner_id_raw, (bytes, bytearray)) else bot_owner_id_raw
                        )
                        text = (
                            "🌐 <b>Пользователь выбрал язык</b>\n\n"
                            f"🤖 Ваш бот: @{bot_username}\n"
                            f"👤 Пользователь: {first_name} {last_name} (@{username or 'без username'})\n"
                            f"🆔 ID: <code>{user_id}</code>\n"
                            f"🗣️ Выбранный язык: {lang_display}\n"
                            f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        await bot.send_message(bot_owner_id, text)
                        notification_sent = True
                        logger.info(f"✅ Sent language_selected notification to bot owner {bot_owner_id} for user_id={user_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send language_selected notification to bot owner: {e}")
                support_chat_id = await get_support_chat_id()
                if support_chat_id:
                    try:
                        if notification_sent:
                            fallback_text = (
                                "🌐 <b>Пользователь выбрал язык</b> (дублирование)\n\n"
                                f"🤖 Бот: @{bot_username or 'неизвестно'}\n"
                                f"👤 Пользователь: {first_name} {last_name} (@{username or 'без username'})\n"
                                f"🆔 ID: <code>{user_id}</code>\n"
                                f"🗣️ Выбранный язык: {lang_display}\n"
                                f"👑 Владелец: {f'ID {bot_owner_id}' if bot_owner_id_raw else 'не найден'}\n"
                                f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                        else:
                            fallback_text = (
                                "🌐 <b>Пользователь выбрал язык</b>\n\n"
                                f"🤖 Бот: @{bot_username or 'неизвестно'}\n"
                                f"👤 Пользователь: {first_name} {last_name} (@{username or 'без username'})\n"
                                f"🆔 ID: <code>{user_id}</code>\n"
                                f"🗣️ Выбранный язык: {lang_display}\n"
                                f"👑 Владелец: не найден\n"
                                f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                        await bot.send_message(support_chat_id, fallback_text)
                        logger.info(f"✅ Sent language_selected notification to support chat {support_chat_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send language_selected notification to support chat: {e}")
            elif event_type == "payment_proof":
                uname = ev.get("username") or str(ev.get("user_id"))
                amount = ev.get("amount", 0)
                bot_code = ev.get("bot", "ru")
                bot_code = bot_code.lower()
                text = (
                    "📎 <b>Подтверждение оплаты</b>\n\n"
                    f"👤 Пользователь: @{uname}\n"
                    f"🆔 ID: {ev.get('user_id')}\n"
                    f"🤖 Бот: {'английский' if bot_code == 'en' else 'русский'}\n"
                    f"💵 Сумма: ${amount}\n"
                    f"⏰ Время: {datetime.fromtimestamp(ev.get('timestamp', time.time())).strftime('%Y-%m-%d %H:%M:%S')}"
                )
                await bot.send_message(chat_id=chat_id, text=text)
            else:
                logger.warning(f"Unknown event type in queue: {event_type}")
        except Exception as e:
            logger.exception("Queue loop error: %s", e)
            await asyncio.sleep(1)
async def process_crypto_event(event_data):
    try:
        bot_code = event_data.get("bot", "ru")
        bot_username = event_data.get("bot_username", "")
        if bot_code in ["ru", "en"]:
            chat_id = await get_support_chat_id()
            if not chat_id:
                await r.lpush(SUPPORT_QUEUE_KEY, json.dumps(event_data).encode())
                logger.warning("Support chat not bound, event requeued")
                return
            uname = event_data.get("username") or str(event_data.get("user_id"))
            amt = event_data.get("amount")
            net = event_data.get("network", "TRC20")
            asset = event_data.get("asset", "USDT")
            lang_display = "русский" if bot_code == "ru" else "английский"
            text = (
                "₿ <b>Пополнение криптовалютой</b>\n\n"
                f"👤 Пользователь: @{uname}\n"
                f"🤖 Бот: {lang_display} (@{bot_username or '?'})\n"
                f"💵 Сумма: ${amt} {asset}\n"
                f"🌐 Сеть: {net}"
            )
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=approve_deny_kb(event_data["event_id"]),
            )
        else:
            logger.warning(f"Unknown bot code for crypto payment: {bot_code}")
            await push_notify_event({
                "type": "payment", 
                "status": "denied", 
                "user_id": event_data.get("user_id"),
                "bot": bot_code  
            })
    except Exception as e:
        logger.error(f"❌ Error processing crypto event: {e}")
async def download_and_forward_payment_proof(file_id: str, user_id: int, username: str, event_id: str, file_type: str = "photo"):
    if not PAYMENT_CONFIRMATION_CHAT_ID:
        logger.warning("PAYMENT_CONFIRMATION_CHAT_ID not set")
        return
    try:
        logger.info(f"🔄 Forwarding payment proof ({file_type}) with admin buttons: {event_id}")
        if file_type == "photo":
            admin_kb = admin_request_pdf_kb(event_id.replace(':', '_'))
        else:
            admin_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Подтвердить зачисление", 
                        callback_data=f"admin_confirm_payment:{event_id.replace(':', '_')}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отклонить", 
                        callback_data=f"admin_reject_payment:{event_id.replace(':', '_')}"
                    )
                ]
            ])
        caption = (
            f"📎 <b>Подтверждение оплаты</b>\n\n"
            f"👤 Пользователь: @{username or 'N/A'} (ID: {user_id})\n"
            f"🆔 Event ID: <code>{event_id}</code>\n"
            f"🕒 Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📁 Тип: {'Фото' if file_type == 'photo' else 'PDF-документ'}\n\n"
            f"<i>Подтвердите зачисление средств:</i>"
        )
        if file_type == "photo":
            await bot.send_photo(
                chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
                photo=file_id,
                caption=caption,
                reply_markup=admin_kb
            )
        else:  
            await bot.send_document(
                chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
                document=file_id,
                caption=caption,
                reply_markup=admin_kb
            )
        logger.info(f"✅ Payment proof ({file_type}) forwarded with admin buttons for event {event_id}")
    except Exception as e:
        logger.error(f"❌ Failed to forward payment proof: {e}")
async def process_feed_queue():
    logger.info("Support feed worker started, key=%s", SUPPORT_FEED_KEY)
    while True:
        try:
            item = await r.brpop(SUPPORT_FEED_KEY, timeout=5)
            if not item:
                await asyncio.sleep(0.1)
                continue
            _, data = item
            try:
                ev = json.loads(data.decode() if isinstance(data, (bytes, bytearray)) else data)
                logger.info("📨 Processing feed event: %s", ev.get('type'))
            except Exception:
                logger.exception("Bad feed json: %r", data)
                continue
            if ev.get("type") == "payment_proof_pdf":
                try:
                    await handle_payment_proof_pdf(ev)
                except Exception as e:
                    logger.exception("❌ Failed to process payment proof PDF")
            elif ev.get("type") == "payment_proof" and (ev.get("has_photo") or ev.get("has_document")):
                try:
                    file_type = "photo" if ev.get("has_photo") else "document"
                    logger.info(f"🔄 Processing payment proof ({file_type}): {ev.get('file_id')}")
                    await download_and_forward_payment_proof(
                        file_id=ev.get("file_id", ""),
                        user_id=ev.get("user_id"),
                        username=ev.get("username"),
                        event_id=ev.get("event_id"),
                        file_type=file_type
                    )
                except Exception as e:
                    logger.exception("❌ Failed to process payment proof")
        except Exception as e:
            logger.exception("Feed worker loop error: %s", e)
            await asyncio.sleep(1)
async def handle_payment_proof_pdf(ev: dict):
    try:
        file_id = ev.get("file_id")
        user_id = ev.get("user_id")
        username = ev.get("username")
        event_id = ev.get("event_id")
        file_name = ev.get("file_name", "document.pdf")
        bot_code = ev.get("bot", "ru")
        logger.info(f"📄 Processing PDF payment proof: event_id={event_id}, user_id={user_id}")
        temp_data = await get_card_temp(event_id)
        if not temp_data:
            logger.error(f"❌ No temp data found for event {event_id}")
            return
        admin_id = temp_data.get('admin_id')
        amount = temp_data.get('amount')
        card_number = temp_data.get('card_number')
        if not admin_id:
            logger.error(f"❌ No admin_id found for event {event_id}")
            return
        proof_message = (
            f"📎 <b>Получена PDF-квитанция об оплате</b>\n\n"
            f"👤 Пользователь: @{username or user_id}\n"
            f"💵 Сумма: ${amount}\n"
            f"💳 Карта: {card_number}\n"
            f"📁 Тип: PDF-документ\n"
            f"📄 Файл: {file_name}\n\n"
            f"Подтвердите получение платежа:"
        )
        await bot.send_document(
            chat_id=admin_id,
            document=file_id,
            caption=proof_message,
            reply_markup=admin_confirm_payment_kb(event_id)
        )
        if PAYMENT_CONFIRMATION_CHAT_ID:
            await bot.send_document(
                chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
                document=file_id,
                caption=proof_message,
                reply_markup=admin_confirm_payment_kb(event_id)
            )
        logger.info(f"✅ PDF payment proof forwarded for event {event_id}")
    except Exception as e:
        logger.error(f"❌ Error in handle_payment_proof_pdf: {e}")
def is_pdf_document(message: Message) -> bool:
    if not message.document:
        return False
    if (message.document.mime_type == 'application/pdf' or 
        (message.document.file_name and message.document.file_name.lower().endswith('.pdf'))):
        return True
    return False
@router.callback_query(F.data.startswith("admin_wd_reject:"))
async def admin_wd_reject(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_wd_reject:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"Admin rejecting card withdraw: event_id={event_id}")
        ev = await get_event(event_id)
        if not ev:
            await cb.answer("❌ Событие не найдено", show_alert=True)
            return
        user_id = ev.get('user_id')
        amount = ev.get('amount', 0)
        amount_rub = ev.get('amount_rub', amount * 91.10)
        fio = ev.get('fio', 'Не указано')
        bank = ev.get('bank', 'Не указан')
        card_number = ev.get('card_number', '')
        bot_code = (ev.get('bot') or 'ru').lower()
        await push_notify_event({
            "type": "balance_credit",
            "user_id": user_id,
            "amount": amount,
            "bot": bot_code,
            "reason": "withdraw_rejection",
            "timestamp": time.time()
        })
        if bot_code == 'en':
            text = (
                "❌ <b>Withdrawal declined</b>\n\n"
                f"Your withdrawal request for ${amount} (~{int(amount_rub)} RUB) has been declined.\n\n"
                f"<b>Reason:</b> Please provide details of the card used for deposit.\n\n"
                f"💰 <b>Funds returned to your balance</b>\n\n"
                "Contact support for verification:"
            )
            support_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=support_button_text,
                    url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start=GPT5CRYPTO_{bot_code}"
                )]
            ])
        else:
            text = (
                "❌ <b>Вывод отклонен</b>\n\n"
                f"Ваша заявка на вывод ${amount} (~{int(amount_rub)} RUB) отклонена.\n\n"
                f"<b>Причина:</b> Для вывода укажите реквизиты карты, с которой пополняли счёт.\n\n"
                f"💰 <b>Средства возвращены на ваш баланс</b>\n\n"
                "Свяжитесь если у вас остались вопросы:"
            )
            support_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=support_button_text,
                    url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start=GPT5CRYPTO_{bot_code}"
                )]
            ])
        await send_message_to_user_via_trading_bot(
            user_id,
            text,
            reply_markup=support_kb,
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )
        await push_notify_event({
            "type": "withdraw_decision", 
            "status": "denied",
            "event_id": event_id,
            "skip_notification": True 
        })
        await cb.message.edit_text(
            "❌ <b>Вывод отклонен - запрошены реквизиты</b>\n\n"
            f"👤 Пользователь: {ev.get('username', 'N/A')}\n"
            f"👤 ФИО: {fio}\n"
            f"🏦 Банк: {bank}\n"
            f"💵 Сумма: ${amount} (~{int(amount_rub)} RUB)\n"
            f"💳 Карта: {card_number}\n\n"
            f"📝 Пользователю отправлен запрос на предоставление реквизитов\n"
            f"💰 Средства возвращены на баланс"  
        )
        await cb.answer("Вывод отклонен, средства возвращены")
    except Exception as e:
        logger.error(f"Error in admin_wd_reject: {e}")
        await cb.answer("Ошибка при отклонении", show_alert=True)
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
@router.message(Command("mindeposit"))
async def cmd_mindeposit(m: Message):
    if not await is_user_admin(m.from_user.id):
        await m.answer("❌ Эта команда доступна только администраторам")
        return
    if len(m.text.split()) < 3:
        global_min = await get_global_min_deposit()
        text = (
            "💰 <b>Настройка минимального депозита</b>\n\n"
            f"📊 <b>Глобальный минимальный депозит:</b> ${global_min:.2f}\n\n"
            "<b>Использование:</b>\n"
            "<code>/mindeposit global СУММА</code> - для всех пользователей\n"
            "<code>/mindeposit USER_ID СУММА</code> - для конкретного пользователя\n"
            "<code>/mindeposit show USER_ID</code> - показать для пользователя\n\n"
            "<b>Примеры:</b>\n"
            "<code>/mindeposit global 100</code>\n"
            "<code>/mindeposit 123456789 50</code>\n"
            "<code>/mindeposit show 123456789</code>"
        )
        await m.answer(text)
        return
    parts = m.text.split()
    command_type = parts[1].lower()
    if command_type == "global":
        try:
            amount = float(parts[2])
            if amount < 0:
                await m.answer("❌ Сумма не может быть отрицательной")
                return
            await set_global_min_deposit(amount)
            await m.answer(f"✅ Глобальный минимальный депозит установлен: <b>${amount:.2f}</b>")
        except ValueError:
            await m.answer("❌ Неверный формат суммы. Используйте число, например: 100.50")
    elif command_type == "show":
        try:
            user_id = int(parts[2])
            user_min = await get_user_min_deposit(user_id)
            global_min = await get_global_min_deposit()
            user_data = await get_user_by_id(user_id)
            username = user_data.get('username', 'N/A') if user_data else 'N/A'
            text = (
                f"👤 <b>Минимальный депозит для пользователя</b>\n\n"
                f"Пользователь: @{username}\n"
                f"ID: <code>{user_id}</code>\n"
                f"🌐 Глобальный минимум: ${global_min:.2f}\n"
                f"👤 Персональный минимум: <b>${user_min:.2f}</b>\n"
                f"🏦 Фактический минимум: <b>${user_min:.2f}</b>"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="💰 Изменить", callback_data=f"change_mindeposit:{user_id}"),
                    InlineKeyboardButton(text="🔄 Сбросить", callback_data=f"reset_mindeposit:{user_id}")
                ]
            ])
            await m.answer(text, reply_markup=kb)
        except ValueError:
            await m.answer("❌ Неверный формат USER_ID")
    else:
        try:
            user_id = int(command_type)
            amount = float(parts[2])
            if amount < 0:
                await m.answer("❌ Сумма не может быть отрицательной")
                return
            await set_user_min_deposit(user_id, amount)
            user_data = await get_user_by_id(user_id)
            username = user_data.get('username', 'N/A') if user_data else 'N/A'
            await m.answer(
                f"✅ Минимальный депозит для пользователя @{username} установлен: <b>${amount:.2f}</b>\n\n"
                f"Теперь пользователь сможет пополнять баланс от ${amount:.2f}"
            )
        except ValueError:
            await m.answer("❌ Неверный формат. Используйте: /mindeposit USER_ID СУММА")
@router.callback_query(F.data.startswith("change_mindeposit:"))
async def change_mindeposit_callback(cb: CallbackQuery, state: FSMContext):
    if not await is_user_admin(cb.from_user.id):
        logger.warning(f"❌ Неавторизованная попытка доступа к изменению мин. депозита: user_id={cb.from_user.id}")
        await cb.answer("❌ Доступ запрещен", show_alert=True)
        return
    user_id = int(cb.data.split(":")[1])
    admin_id = cb.from_user.id
    logger.info(f"🔄 Админ {admin_id} начинает изменение мин. депозита для пользователя {user_id}")
    await state.update_data(target_user_id=user_id)
    user_data = await get_user_by_id(user_id)
    username = user_data.get('username', 'N/A') if user_data else 'N/A'
    current_min = await get_user_min_deposit(user_id)
    global_min = await get_global_min_deposit()
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="50"), KeyboardButton(text="100"), KeyboardButton(text="250")],
            [KeyboardButton(text="500"), KeyboardButton(text="1000"), KeyboardButton(text="2500")],
            [KeyboardButton(text="5000"), KeyboardButton(text="Ввести свою сумму")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await cb.message.answer(
        f"💰 <b>Изменение минимального депозита</b>\n\n"
        f"👤 Пользователь: @{username}\n"
        f"📊 Текущий минимум: ${current_min:.2f}\n"
        f"🌐 Глобальный минимум: ${global_min:.2f}\n\n"
        f"Выберите сумму или введите свою:",
        reply_markup=keyboard
    )
    logger.info(f"✅ Меню изменения мин. депозита показано для пользователя {user_id}")
    await cb.answer()
@router.message(F.text.in_(["50", "100", "250", "500", "1000", "2500", "5000"]))
async def handle_min_deposit_button(m: Message, state: FSMContext):
    if not await is_user_admin(m.from_user.id):
        logger.warning(f"❌ Неавторизованная попытка установки мин. депозита через кнопку: user_id={m.from_user.id}")
        await m.answer("❌ Доступ запрещен")
        return
    try:
        amount = float(m.text)
        admin_id = m.from_user.id
        data = await state.get_data()
        user_id = data.get('target_user_id')
        if not user_id:
            logger.error(f"❌ Не найден target_user_id в состоянии для админа {admin_id}")
            await m.answer("❌ Не выбран пользователь", reply_markup=None)
            await state.clear()
            return
        user_data = await get_user_by_id(user_id)
        username = user_data.get('username', 'N/A') if user_data else 'N/A'
        old_min = await get_user_min_deposit(user_id)
        logger.info(f"🔄 Админ {admin_id} устанавливает мин. депозит для {user_id} (@{username}): ${old_min:.2f} -> ${amount:.2f}")
        await set_user_min_deposit(user_id, amount)
        logger.info(f"✅ Мин. депозит для пользователя {user_id} (@{username}) установлен: ${amount:.2f} (было: ${old_min:.2f})")
        await m.answer(
            f"✅ Минимальный депозит для пользователя @{username} установлен: <b>${amount:.2f}</b>",
            reply_markup=None
        )
        try:
            support_chat_id = await get_support_chat_id()
            if support_chat_id:
                await bot.send_message(
                    chat_id=support_chat_id,
                    text=(
                        f"💰 <b>Изменение минимального депозита</b>\n\n"
                        f"👤 Администратор: @{m.from_user.username or m.from_user.id}\n"
                        f"👤 Пользователь: @{username}\n"
                        f"🆔 ID: <code>{user_id}</code>\n"
                        f"💰 Старый мин. депозит: ${old_min:.2f}\n"
                        f"💰 Новый мин. депозит: <b>${amount:.2f}</b>"
                    )
                )
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления в чат поддержки: {e}")
        await state.clear()
    except Exception as e:
        logger.error(f"❌ Ошибка обработки кнопки мин. депозита: {e}")
        await m.answer("❌ Ошибка при установке суммы", reply_markup=None)
@router.message(F.text == "Ввести свою сумму")
async def handle_custom_amount(m: Message):
    if not await is_user_admin(m.from_user.id):
        logger.warning(f"❌ Неавторизованная попытка ввода кастомной суммы: user_id={m.from_user.id}")
        await m.answer("❌ Доступ запрещен")
        return
    logger.info(f"🔧 Админ {m.from_user.id} выбрал ввод кастомной суммы мин. депозита")
    await m.answer(
        "💰 <b>Введите свою сумму</b>\n\n"
        "Отправьте число (например: 75.50 или 150):",
        reply_markup=None
    )
@router.message(lambda m: m.text.replace('.', '', 1).isdigit() and m.chat.type == "private")
async def handle_custom_min_deposit(m: Message, state: FSMContext):
    if not await is_user_admin(m.from_user.id):
        logger.warning(f"❌ Неавторизованная попытка установки кастомного мин. депозита: user_id={m.from_user.id}")
        await m.answer("❌ Доступ запрещен")
        return
    try:
        amount = float(m.text)
        if amount <= 0:
            logger.warning(f"⚠️ Админ {m.from_user.id} попытался установить отрицательный мин. депозит: ${amount}")
            await m.answer("❌ Сумма должна быть больше 0")
            return
        admin_id = m.from_user.id
        data = await state.get_data()
        user_id = data.get('target_user_id')
        if not user_id:
            logger.error(f"❌ Не найден target_user_id в состоянии для админа {admin_id}")
            await m.answer("❌ Не выбран пользователь")
            await state.clear()
            return
        user_data = await get_user_by_id(user_id)
        username = user_data.get('username', 'N/A') if user_data else 'N/A'
        old_min = await get_user_min_deposit(user_id)
        logger.info(f"🔄 Админ {admin_id} устанавливает кастомный мин. депозит для {user_id} (@{username}): ${old_min:.2f} -> ${amount:.2f}")
        await set_user_min_deposit(user_id, amount)
        logger.info(f"✅ Кастомный мин. депозит для пользователя {user_id} (@{username}) установлен: ${amount:.2f} (было: ${old_min:.2f})")
        await m.answer(
            f"✅ Минимальный депозит для пользователя @{username} установлен: <b>${amount:.2f}</b>"
        )
        try:
            support_chat_id = await get_support_chat_id()
            if support_chat_id:
                await bot.send_message(
                    chat_id=support_chat_id,
                    text=(
                        f"💰 <b>Изменение минимального депозита (кастомная сумма)</b>\n\n"
                        f"👤 Администратор: @{m.from_user.username or m.from_user.id}\n"
                        f"👤 Пользователь: @{username}\n"
                        f"🆔 ID: <code>{user_id}</code>\n"
                        f"💰 Старый мин. депозит: ${old_min:.2f}\n"
                        f"💰 Новый мин. депозит: <b>${amount:.2f}</b>"
                    )
                )
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления в чат поддержки: {e}")
        await state.clear()
    except ValueError:
        logger.warning(f"⚠️ Админ {m.from_user.id} ввел невалидный формат суммы: {m.text}")
        await m.answer("❌ Неверный формат суммы. Используйте число, например: 75.50")
    except Exception as e:
        logger.error(f"❌ Ошибка установки кастомного мин. депозита: {e}")
        await m.answer("❌ Ошибка при установке суммы")
@router.callback_query(F.data.startswith("reset_mindeposit:"))
async def reset_mindeposit_callback(cb: CallbackQuery):
    if not await is_user_admin(cb.from_user.id):
        logger.warning(f"❌ Неавторизованная попытка сброса мин. депозита: user_id={cb.from_user.id}")
        await cb.answer("❌ Доступ запрещен", show_alert=True)
        return
    admin_id = cb.from_user.id
    user_id = int(cb.data.split(":")[1])
    user_data = await get_user_by_id(user_id)
    username = user_data.get('username', 'N/A') if user_data else 'N/A'
    old_min = await get_user_min_deposit(user_id)
    global_min = await get_global_min_deposit()
    logger.info(f"🔄 Админ {admin_id} сбрасывает мин. депозит для {user_id} (@{username}): ${old_min:.2f} -> ${global_min:.2f} (глобальный)")
    await r.delete(f"user:{user_id}:min_deposit")
    logger.info(f"✅ Мин. депозит для пользователя {user_id} (@{username}) сброшен к глобальному: ${global_min:.2f} (было: ${old_min:.2f})")
    await cb.message.edit_text(
        f"🔄 <b>Минимальный депозит сброшен</b>\n\n"
        f"👤 Пользователь: @{username}\n"
        f"📊 Теперь используется глобальный минимум: <b>${global_min:.2f}</b>\n"
        f"📊 Предыдущий минимум: ${old_min:.2f}"
    )
    try:
        support_chat_id = await get_support_chat_id()
        if support_chat_id:
            await bot.send_message(
                chat_id=support_chat_id,
                text=(
                    f"🔄 <b>Сброс минимального депозита</b>\n\n"
                    f"👤 Администратор: @{cb.from_user.username or cb.from_user.id}\n"
                    f"👤 Пользователь: @{username}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"💰 Предыдущий мин. депозит: ${old_min:.2f}\n"
                    f"💰 Новый мин. депозит (глобальный): <b>${global_min:.2f}</b>"
                )
            )
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления в чат поддержки: {e}")
    await cb.answer("✅ Настройки сброшены")
@router.message(Command("checkmindeposit"))
async def cmd_checkmindeposit(m: Message):
    if not await is_user_admin(m.from_user.id):
        await m.answer("❌ Эта команда доступна только администраторам")
        return
    if len(m.text.split()) < 2:
        global_min = await get_global_min_deposit()
        await m.answer(f"🌐 <b>Глобальный минимальный депозит:</b> ${global_min:.2f}")
        return
    try:
        user_id = int(m.text.split()[1])
        user_min = await get_user_min_deposit(user_id)
        global_min = await get_global_min_deposit()
        user_data = await get_user_by_id(user_id)
        username = user_data.get('username', 'N/A') if user_data else 'N/A'
        
        status = "🔴" if user_min > global_min else "🟢"
        
        await m.answer(
            f"💰 <b>Минимальный депозит</b> {status}\n\n"
            f"👤 Пользователь: @{username}\n"
            f"🌐 Глобальный минимум: ${global_min:.2f}\n"
            f"👤 Персональный минимум: ${user_min:.2f}\n"
            f"🏦 Фактический минимум: <b>${user_min:.2f}</b>"
        )
    except ValueError:
        await m.answer("❌ Неверный формат USER_ID")
@router.callback_query(F.data == "settings")
async def settings_callback(cb: CallbackQuery):
    chat_id = await get_support_chat_id()
    await cb.message.answer(f"⚙️ <b>Настройки</b>\n\nТекущий чат для уведомлений: <code>{chat_id}</code>")
    await cb.answer()
@router.callback_query(F.data == "queue_info")
async def queue_info_callback(cb: CallbackQuery):
    try:
        support_queue_len = await r.llen(SUPPORT_QUEUE_KEY)
        notify_queue_len = await r.llen(NOTIFY_QUEUE_KEY)
        feed_queue_len = await r.llen(SUPPORT_FEED_KEY)
        text = (
            "📊 <b>Информация об очередях</b>\n\n"
            f"🔄 Очередь поддержки: {support_queue_len}\n"
            f"📨 Очередь уведомлений: {notify_queue_len}\n"
            f"📝 Очередь событий: {feed_queue_len}"
        )
        await cb.message.answer(text)
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка при получении информации об очередях: {e}")
    await cb.answer()
@router.callback_query(F.data.startswith("support:approve:"))
async def on_approve(cb: CallbackQuery):
    parts = cb.data.split(":")
    event_id = ":".join(parts[2:])
    ev = await get_event(event_id)
    if not ev:
        await cb.answer("Событие не найдено", show_alert=True)
        return
    uid = int(ev.get("user_id"))
    amount = float(ev.get("amount", 0) or 0)
    bot_code = (ev.get("bot") or "ru").lower()
    try:
        user = await get_user_by_id(uid)
        if not user:
            await cb.answer("❌ Пользователь не найден", show_alert=True)
            return
        before = float(user.get("balance", 0))
        new_balance = before + amount
        user['balance'] = new_balance
        user['last_activity'] = time.time()
        if 'stats' not in user:
            user['stats'] = {}
        if 'total_deposits' not in user['stats']:
            user['stats']['total_deposits'] = 0
        user['stats']['total_deposits'] += amount
        await r.set(f"user:{uid}", json.dumps(user))
    except Exception as e:
        logger.error(f"Balance update failed: {e}")
        await cb.answer("❌ Ошибка при зачислении", show_alert=True)
        return
    if bot_code == "en":
        msg = f"✅ <b>Payment approved!</b>\nYour deposit of ${amount} has been added.\nBalance: ${new_balance}"
    else:
        msg = f"✅ <b>Платёж подтверждён!</b>\nНа ваш баланс зачислено ${amount}.\nБаланс: ${new_balance}"
    try:
        await send_message_to_user_via_trading_bot(
            uid,
            msg,
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )
    except Exception as e:
        logger.error(f"Failed to notify user {uid}: {e}")
    await cb.message.edit_text(
        cb.message.text + "\n\n✅ <b>ОДОБРЕНО — средства зачислены</b>"
    )
    await cb.answer("Одобрено")
@router.callback_query(F.data.startswith("support:deny:"))
async def on_deny(cb: CallbackQuery):
    parts = cb.data.split(":")
    event_id = ":".join(parts[2:])
    ev = await get_event(event_id)
    if not ev:
        await cb.answer("Событие не найдено", show_alert=True)
        return
    uid = int(ev.get("user_id"))
    bot_code = ev.get("bot", "ru")
    bot_username = ev.get("bot_username", "")
    if bot_username and "en" in bot_username.lower():
        bot_code = "en"
    else:
        bot_code = "ru"
    logger.info(f"🔍 Denying crypto payment - Bot: {bot_code}, Username: {bot_username}")   
    await push_notify_event({
        "type": "payment",
        "status": "denied", 
        "user_id": uid,
        "bot": bot_code
    })
    await cb.answer("Отклонено")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
        lang_text = "английский" if bot_code == "en" else "русский"
        await cb.message.edit_text(cb.message.text + f"\n\n❌ <b>ОТКЛОНЕНО</b> (бот: {lang_text})")
    except Exception:
        pass
@router.callback_query(F.data.startswith("admin_wd_approve:"))
async def admin_wd_approve(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_wd_approve:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"🔄 Admin approving withdraw: event_id={event_id}")
        ev = await get_event(event_id)
        if not ev:
            await cb.answer("❌ Событие не найдено", show_alert=True)
            return
        bot_code = ev.get("bot", "ru")
        bot_code = bot_code.lower()
        logger.info(f"🔍 Admin approval - Bot code: {bot_code}, Bot username: {ev.get('bot_username')}, Event type: {ev.get('type')}")
        event_type = ev.get("type")
        await push_notify_event({
            "type": "withdraw_decision", 
            "status": "approved",
            "event_id": event_id,
            "bot": bot_code  
        })
        user_id = ev.get('user_id')
        amount = ev.get('amount', 0)
        if bot_code == 'en':
            if event_type == "withdraw_request_card":
                amount_rub = ev.get('amount_rub', amount * 91.10)
                user_message = (
                    f"✅ <b>Withdrawal Approved</b>\n\n"
                    f"Your withdrawal request for <b>${amount}</b> (~{int(amount_rub)} RUB) has been approved.\n\n"
                    f"Funds will be transferred to your card within 1-3 business days."
                )
            else:
                user_message = (
                    f"✅ <b>Withdrawal Approved</b>\n\n"
                    f"Your withdrawal request for <b>${amount}</b> has been approved.\n\n"
                    f"Transaction will be processed shortly."
                )
        else:
            if event_type == "withdraw_request_card":
                amount_rub = ev.get('amount_rub', amount * 91.10)
                user_message = (
                    f"✅ <b>Вывод подтверждён</b>\n\n"
                    f"Ваша заявка на вывод <b>${amount}</b> (~{int(amount_rub)} RUB) подтверждена.\n\n"
                    f"Средства будут переведены на вашу карту в течение 1-3 рабочих дней."
                )
            else:
                user_message = (
                    f"✅ <b>Вывод подтверждён</b>\n\n"
                    f"Ваша заявка на вывод <b>${amount}</b> подтверждена.\n\n"
                    f"Транзакция будет обработана в ближайшее время."
                )
        success = await send_message_to_user_via_trading_bot(
            user_id,
            user_message,
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )
        await cb.answer("Вывод подтвержден")
        try:
            await cb.message.edit_reply_markup(reply_markup=None)
            bot_info = "английский" if bot_code == 'en' else "русский"
            new_text = cb.message.text + f"\n\n✅ <b>ПОДТВЕРЖДЕНО АДМИНОМ</b>\n🤖 Бот: {bot_info}\n💰 Пользователь {'уведомлён' if success else 'НЕ уведомлён'}"
            await cb.message.edit_text(new_text)
        except Exception as e:
            logger.error(f"Failed to edit message: {e}")    
        logger.info(f"✅ Withdraw approved and user notified: event_id={event_id}, user_id={user_id}, bot={bot_code}, success={success}")
    except Exception as e:
        logger.error(f"❌ Error in admin_wd_approve: {e}")
        await cb.answer("Ошибка при подтверждении", show_alert=True)
@router.callback_query(F.data.startswith("admin_wd_reject_requisites:"))
async def admin_wd_reject_requisites(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_wd_reject_requisites:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"Admin rejecting withdraw with requisites request: event_id={event_id}")
        ev = await get_event(event_id)
        if not ev:
            await cb.answer("❌ Событие не найдено", show_alert=True)
            return
        user_id = ev.get('user_id')
        amount = ev.get('amount', 0)
        address = ev.get('address', '')
        bot_code = (ev.get('bot') or 'ru').lower()
        if bot_code == 'en':
            text = (
                "❌ <b>Withdrawal declined</b>\n\n"
                f"Your withdrawal request for ${amount} has been declined.\n\n"
                f"<b>Reason:</b> Please provide details of the wallet used for deposit.\n\n"
                f"Address: <code>{address}</code>\n\n"
                f"💰 <b>Funds returned to your balance</b>\n\n"
                "Contact support for verification:"
            )
            support_button_text = "💬 Contact Support"
        else:
            text = (
                "❌ <b>Вывод отклонен</b>\n\n"
                f"Ваша заявка на вывод ${amount} отклонена.\n\n"
                f"<b>Причина:</b> Для вывода укажите реквизиты кошелька, с которого пополняли счёт.\n\n"
                f"Адрес: <code>{address}</code>\n\n"
                f"💰 <b>Средства возвращены на ваш баланс</b>\n\n"
                "Свяжитесь с поддержкой для верификации:"
            )
            support_button_text = "💬 Связаться с поддержкой"
        await push_notify_event({
            "type": "balance_credit",
            "user_id": user_id,
            "amount": amount,
            "bot": bot_code,
            "reason": "withdraw_rejection",
            "timestamp": time.time()
        })
        support_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=support_button_text,
                url=f"https://t.me/{SUPPORT_BOT_USERNAME}?start=GPT5CRYPTO_{bot_code}"
            )]
        ])
        await send_message_to_user_via_trading_bot(
            user_id,
            text,
            reply_markup=support_kb,
            bot_code=bot_code,
            bot_username=ev.get("bot_username")
        )
        await r.set(f"withdraw:{event_id}:status", "rejected_requisites")
        bot_info = "английский" if bot_code == 'en' else "русский"
        await cb.message.edit_text(
            "❌ <b>Вывод отклонен - запрошены реквизиты</b>\n\n"
            f"👤 Пользователь: {ev.get('username', 'N/A')}\n"
            f"💵 Сумма: ${amount}\n"
            f"🤖 Бот: {bot_info}\n"
            f"🌐 Адрес: {address}\n\n"
            f"📝 Пользователю отправлен запрос на предоставление реквизитов\n"
            f"💰 Средства возвращены на баланс"
        )
        await cb.answer("Вывод отклонен, средства возвращены")
    except Exception as e:
        logger.error(f"Error in admin_wd_reject_requisites: {e}")
        await cb.answer("Ошибка при отклонении", show_alert=True)
@router.callback_query(F.data == "moderation_panel")
async def moderation_panel_callback(cb: CallbackQuery):
    if not await is_user_admin(cb.from_user.id):
        await cb.answer("❌ Доступ запрещен", show_alert=True)
        return
    pending_count = await r.scard(USER_APPROVAL_KEY)
    approved_count = await r.scard(USER_APPROVAL_APPROVED_KEY)
    pending_users = []
    pending_user_ids = await r.smembers(USER_APPROVAL_KEY)
    for user_id in list(pending_user_ids)[:5]:
        user_data_raw = await r.get(f"user:approval:data:{user_id}")
        if user_data_raw:
            user_data = json.loads(user_data_raw)
            pending_users.append(user_data)
    text = (
        "⚙️ <b>Панель модерации пользователей</b>\n\n"
        f"⏳ Ожидают одобрения: {pending_count}\n"
        f"✅ Одобрено пользователей: {approved_count}\n\n"
    )    
    if pending_users:
        text += "<b>Последние заявки:</b>\n"
        for user in pending_users:
            reg_time = datetime.fromtimestamp(user["registration_date"]).strftime("%H:%M")
            text += f"• @{user.get('username', 'нет')} (ID: {user['user_id']}) - {reg_time}\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список заявок", callback_data="moderation_list")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="moderation_panel")]
    ])    
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("admin_approve_user:"))
async def admin_approve_user(cb: CallbackQuery):
    try:
        user_id = int(cb.data.split(":")[1])
        if not await is_user_admin(cb.from_user.id):
            await cb.answer("❌ Доступ запрещен", show_alert=True)
            return
        user_data_raw = await r.get(f"user:approval:data:{user_id}")
        if user_data_raw:
            user_data = json.loads(user_data_raw)
            await r.srem(USER_APPROVAL_KEY, str(user_id))
            await r.sadd(USER_APPROVAL_APPROVED_KEY, str(user_id))
            referral_code = await get_or_create_referral_code(user_id)
            referral_link = f"https://t.me/{TRADING_BOT_USERNAME_RU}?start=ref_{referral_code}"
            referrer_id = user_data.get('referrer_id')
            if referrer_id and referrer_id != user_id:
                existing_refs = await get_user_referrals(referrer_id)
                if user_id not in existing_refs:
                    await add_referral(referrer_id, user_id)
                    logger.info(f"Новый реферал после одобрения: {user_id} -> {referrer_id}")
                    try:
                        await bot.send_message(
                            chat_id=referrer_id,
                            text=(
                                "🎉 <b>Новый реферал одобрен!</b>\n\n"
                                f"Пользователь, которого вы пригласили, был одобрен:\n"
                                f"👤 @{user_data.get('username', 'без username')}\n"
                                f"🆔 ID: <code>{user_id}</code>\n\n"
                                f"Используйте /refstats для просмотра статистики"
                            )
                        )
                    except Exception as e:
                        logger.error(f"Не удалось уведомить реферера {referrer_id}: {e}")
                    await push_notify_event({
                        "type": "referral_registered",
                        "referrer_id": referrer_id,
                        "referred_user_id": user_id,
                        "referred_username": user_data.get('username'),
                        "timestamp": time.time()
                    })
            await r.delete(f"user:approval:data:{user_id}")
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        "🎉 <b>Ваша заявка одобрена!</b>\n\n"
                        "Теперь вы можете создавать своих трейдинг ботов.\n\n"
                        "📊 <b>Ваша уникальная реферальная ссылка:</b>\n"
                        f"<code>{referral_link}</code>\n\n"
                        "Приглашайте трейдеров по этой ссылке и получайте статистику:\n"
                        "• Количество приглашенных трейдеров\n"
                        "• Их торговую активность\n"
                        "• Общую статистику по вашей реферальной сети\n\n"
                        "Используйте команду /createbot чтобы создать своего бота."
                    )
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
            referrer_info = f"🔗 Реферер: ID {referrer_id}" if referrer_id else "🔗 Реферер: нет"
            await cb.message.edit_text(
                f"✅ <b>Пользователь одобрен</b>\n\n"
                f"👤 Пользователь: @{user_data.get('username', 'N/A')}\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"👤 Имя: {user_data.get('first_name', '')} {user_data.get('last_name', '')}\n"
                f"{referrer_info}\n"
                f"🔗 Реферальный код: <code>{referral_code}</code>\n\n"
                f"✅ Пользователь уведомлён о одобрении и получил реферальную ссылку"
            )
            await cb.answer("Пользователь одобрен")
        else:
            await cb.answer("❌ Данные пользователя не найдены", show_alert=True) 
    except Exception as e:
        logger.error(f"Error in admin_approve_user: {e}")
        await cb.answer("Ошибка при одобрении", show_alert=True)
@router.message(Command("refstats"))
async def cmd_refstats(m: Message):
    user_id = m.from_user.id
    is_approved = await r.sismember(USER_APPROVAL_APPROVED_KEY, str(user_id))
    if not is_approved:
        await m.answer("❌ Ваш аккаунт ещё не одобрен администрацией")
        return
    referral_code = await get_or_create_referral_code(user_id)
    referral_link = f"https://t.me/{TRADING_BOT_USERNAME_RU}?start=ref_{referral_code}"
    referrals_count = await get_user_referrals_count(user_id)
    referrals_list = await get_user_referrals(user_id)
    active_referrals = 0
    total_balance = 0.0
    total_trades = 0
    for ref_id in referrals_list:
        user_data = await get_user_by_id(ref_id)
        if user_data:
            if time.time() - user_data.get('last_activity', 0) < 7 * 24 * 3600:
                active_referrals += 1
            total_balance += user_data.get('balance', 0)
            total_trades += user_data.get('stats', {}).get('total_trades', 0)
    text = (
        "📊 <b>Ваша реферальная статистика</b>\n\n"
        f"🔗 <b>Ваша ссылка:</b>\n<code>{referral_link}</code>\n\n"
        f"👥 <b>Общее количество рефералов:</b> {referrals_count}\n"
        f"🟢 <b>Активных рефералов (7 дней):</b> {active_referrals}\n"
        f"💰 <b>Суммарный баланс рефералов:</b> ${total_balance:.2f}\n"
        f"📈 <b>Всего сделок рефералов:</b> {total_trades}\n\n"
        "💡 <i>Приглашайте трейдеров по вашей ссылке и отслеживайте их активность</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Создать бота", callback_data="create_trading_bot")],
        [InlineKeyboardButton(text="👥 Мои боты", callback_data="my_bots")]
    ])
    await m.answer(text, reply_markup=kb)
@router.callback_query(F.data.startswith("admin_reject_user:"))
async def admin_reject_user(cb: CallbackQuery, state: FSMContext):
    if not await is_user_admin(cb.from_user.id):
        await cb.answer("❌ Доступ запрещен", show_alert=True)
        return
    user_id = int(cb.data.split(":")[1])
    await state.update_data(reject_user_id=user_id)
    await state.set_state(SupportStates.WAIT_APPROVAL_REASON)
    await cb.message.answer(
        "📝 <b>Укажите причину отказа</b>\n\n"
        "Напишите причину, по которой вы отклоняете заявку пользователя:"
    )
    await cb.answer()
@router.message(SupportStates.WAIT_APPROVAL_REASON)
async def process_rejection_reason(m: Message, state: FSMContext):
    data = await state.get_data()
    user_id = data.get('reject_user_id')
    reason = m.text
    if not user_id:
        await m.answer("❌ Ошибка: не найден ID пользователя")
        await state.clear()
        return
    user_data_raw = await r.get(f"user:approval:data:{user_id}")
    if user_data_raw:
        user_data = json.loads(user_data_raw)
        await r.srem(USER_APPROVAL_KEY, str(user_id))
        await r.delete(f"user:approval:data:{user_id}")
        try:
            await bot.send_message(
                chat_id=user_id,
                text=(
                    "❌ <b>Ваша заявка отклонена</b>\n\n"
                    f"<b>Причина:</b> {reason}\n\n"
                    "Если вы считаете, что это ошибка, свяжитесь с поддержкой."
                )
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
    await m.answer(
        f"❌ <b>Заявка пользователя отклонена</b>\n\n"
        f"Причина: {reason}"
    )
    await state.clear()
@router.callback_query(F.data == "create_trading_bot")
async def create_trading_bot_callback(cb: CallbackQuery, state: FSMContext):
    user_id = cb.from_user.id
    is_approved = await r.sismember(USER_APPROVAL_APPROVED_KEY, str(user_id))
    if not is_approved:
        await cb.answer("❌ Ваш аккаунт ещё не одобрен администрацией", show_alert=True)
        return
    await state.set_state(SupportStates.WAIT_BOT_TOKEN)
    await cb.message.answer(
        "🤖 <b>Создание трейдинг бота</b>\n\n"
        "Чтобы создать своего трейдинг бота, вам нужно:\n\n"
        "1. Создать бота через @BotFather\n"
        "2. Получить API токен\n"
        "3. Отправить токен мне\n\n"
        "📝 <b>Пришлите токен вашего бота:</b>\n"
        "<i>Пример: 1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi</i>\n\n"
        "⚠️ <b>Важно:</b> Бот должен быть создан через @BotFather и иметь username"
    )
    await cb.answer()
@router.message(SupportStates.WAIT_BOT_TOKEN)
async def process_bot_token(m: Message, state: FSMContext):
    user_id = m.from_user.id
    bot_token_raw = (m.text or "").strip()
    if not bot_token_raw or ":" not in bot_token_raw:
        await m.answer(
            "❌ <b>Похоже, это не токен бота</b>\n\n"
            "Пришлите строку формата:\n"
            "<code>1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi</code>"
        )
        return
    await m.answer("⏳ Проверяю токен бота, подождите пару секунд...")
    try:
        valid_info = await bot_manager.validate_bot_token(bot_token_raw)
    except Exception as e:
        logger.exception("Bot token validation failed")
        await m.answer(f"⚠️ Ошибка проверки токена:\n<code>{e}</code>")
        return
    if not valid_info.get("is_valid"):
        await m.answer(
            "❌ Токен не прошёл проверку.\n"
            f"<i>{valid_info.get('error','')}</i>"
        )
        return
    bot_username = valid_info.get("username")
    if not bot_username:
        await m.answer(
            "❌ Не удалось определить @username бота.\n"
            "Проверьте, что бот создан через @BotFather."
        )
        return
    try:
        rec = {
            "username": bot_username,
            "created_at": time.time(),
            "is_active": True,
            "first_name": valid_info.get("first_name"),
            "bot_id": valid_info.get("id"),
            "token": bot_token_raw,
        }
        await r.hset(
            USER_BOT_TOKENS_KEY.format(user_id=user_id),
            bot_username,
            json.dumps(rec)
        )
        await r.hset(BOT_OWNER_INDEX_KEY, bot_username, user_id)
    except Exception as e:
        logger.exception("Failed to persist user bot data / owner index")
        await m.answer(f"⚠️ Не удалось сохранить данные бота: {e}")
        return
    ok_reg = await bot_manager.register_bot_instance(
        user_id=user_id,
        bot_token=bot_token_raw,
        bot_data={"username": bot_username},
    )
    if not ok_reg:
        await m.answer(
            "⚠️ Не удалось зарегистрировать инстанс бота.\n"
            "Попробуйте позже."
        )
        return
    ok_run = await bot_manager.start_bot_instance(bot_username)
    if not ok_run:
        await m.answer(
            "⚠️ Бот сохранён, но не удалось запустить процесс.\n"
            "Попробуйте позже или запустите вручную через меню управления."
        )
        return
    await m.answer(
        "✅ <b>Бот подключён и запущен!</b>\n\n"
        f"🤖 Имя: @{bot_username}\n"
        "Теперь вы можете управлять своим трейдинг-ботом через меню."
    )
    await state.clear()
@router.callback_query(F.data == "my_bots")
async def my_bots_callback(cb: CallbackQuery):
    user_id = cb.from_user.id
    user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
    bots_data = await r.hgetall(user_bots_key)
    if not bots_data:
        text = "🤖 <b>У вас пока нет созданных ботов</b>\n\nИспользуйте кнопку ниже чтобы создать первого бота:"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Создать бота", callback_data="create_trading_bot")]
        ])
    else:
        text = "🤖 <b>Ваши трейдинг боты</b>\n\n"
        keyboard_buttons = []
        for bot_username, bot_data_raw in bots_data.items():
            bot_data = json.loads(bot_data_raw)
            status_info = await bot_manager.get_bot_status(bot_username)
            is_active = status_info.get("active", False)
            status = "🟢" if is_active else "🔴"
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=f"{status} @{bot_username}",
                    callback_data=f"manage_bot:{bot_username}"
                )
            ])
        keyboard_buttons.append([
            InlineKeyboardButton(text="🤖 Создать ещё бота", callback_data="create_trading_bot"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="my_bots")
        ])
        kb = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.message(Command("mybots"))
async def my_bots_command(m: Message):
    user_id = m.from_user.id
    user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
    bots_data = await r.hgetall(user_bots_key)
    if not bots_data:
        text = "🤖 <b>У вас пока нет созданных ботов</b>\n\nИспользуйте кнопку ниже чтобы создать первого бота:"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Создать бота", callback_data="create_trading_bot")]
        ])
    else:
        text = "🤖 <b>Ваши трейдинг боты</b>\n\n"
        keyboard_buttons = []
        for bot_username, bot_data_raw in bots_data.items():
            bot_data = json.loads(bot_data_raw)
            status = "🟢 Активен" if bot_data.get("is_active", True) else "🔴 Неактивен"
            created_date = datetime.fromtimestamp(bot_data["created_at"]).strftime("%d.%m.%Y")
            text += f"• @{bot_username} - {status} (с {created_date})\n"
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=f"⚙️ {bot_username}",
                    callback_data=f"manage_bot:{bot_username}"
                )
            ])
        keyboard_buttons.append([
            InlineKeyboardButton(text="🤖 Создать ещё бота", callback_data="create_trading_bot")
        ])
        kb = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    await m.answer(text, reply_markup=kb)
@router.callback_query(F.data.startswith("bot_stats:"))
async def bot_stats_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    status_info = await bot_manager.get_bot_status(bot_username)
    
    text = (
        f"📊 <b>Статистика бота @{bot_username}</b>\n\n"
        f"👥 Всего пользователей: {status_info.get('users_count', 0)}\n"
        f"🟢 Активных пользователей: {status_info.get('active_users', 0)}\n"
        f"📈 Всего сделок: {status_info.get('total_trades', 0)}\n"
        f"💰 Общий оборот: ${status_info.get('total_volume', 0):.2f}\n"
        f"📥 Пополнений: {status_info.get('total_deposits', 0)}\n"
        f"📤 Выводов: {status_info.get('total_withdrawals', 0)}\n"
        f"⏰ Время работы: {status_info.get('uptime', 'N/A')}"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👈 Назад", callback_data=f"manage_bot:{bot_username}"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"bot_stats:{bot_username}")
        ]
    ])
    
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("bot_settings:"))
async def bot_settings_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    
    text = (
        f"⚙️ <b>Настройки бота @{bot_username}</b>\n\n"
        f"Настройки будут доступны в будущих обновлениях.\n\n"
        f"Планируемые функции:\n"
        f"• Изменение приветственного сообщения\n"
        f"• Настройка уведомлений\n"
        f"• Изменение комиссий\n"
        f"• Настройка лимитов\n"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👈 Назад", callback_data=f"manage_bot:{bot_username}")]
    ])
    
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("manage_bot:"))
async def manage_bot_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    owner_id = await r.hget(BOT_OWNER_INDEX_KEY, bot_username)
    if not owner_id or int(owner_id) != user_id:
        await cb.answer("❌ У вас нет прав для управления этим ботом", show_alert=True)
        return
    user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
    bot_data_raw = await r.hget(user_bots_key, bot_username)
    if not bot_data_raw:
        await cb.answer("❌ Бот не найден", show_alert=True)
        return
    bot_data = json.loads(bot_data_raw)
    status_info = await bot_manager.get_bot_status(bot_username)
    is_active = status_info.get("active", False)
    bot_data["is_active"] = is_active
    bot_data["status"] = status_info.get("status", "unknown")
    await r.hset(user_bots_key, bot_username, json.dumps(bot_data))
    users_count = status_info.get("users_count", 0)
    active_users = status_info.get("active_users", 0)
    total_trades = status_info.get("total_trades", 0)
    text = (
        f"🤖 <b>Управление ботом @{bot_username}</b>\n\n"
        f"📝 Имя: {bot_data.get('first_name', 'Не указано')}\n"
        f"🔄 Статус: {'🟢 Активен' if is_active else '🔴 Остановлен'}\n"
        f"📊 Пользователей: {users_count} (🟢 {active_users})\n"
        f"📈 Сделок: {total_trades}\n"
        f"📅 Создан: {datetime.fromtimestamp(bot_data['created_at']).strftime('%d.%m.%Y %H:%M')}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="▶️ Запустить" if not is_active else "⏸️ Остановить",
                callback_data=f"bot_toggle:{bot_username}"
            ),
            InlineKeyboardButton(
                text="📊 Статистика",
                callback_data=f"bot_stats:{bot_username}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⚙️ Настройки",
                callback_data=f"bot_settings:{bot_username}"
            ),
            InlineKeyboardButton(
                text="🗑️ Удалить",
                callback_data=f"bot_delete:{bot_username}"
            )
        ],
        [
            InlineKeyboardButton(
                text="💬 Открыть бота",
                url=f"https://t.me/{bot_username}"
            ),
            InlineKeyboardButton(
                text="👈 Назад",
                callback_data="my_bots"
            )
        ]
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("bot_delete:"))
async def bot_delete_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    confirmation_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"bot_delete_confirm:{bot_username}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"manage_bot:{bot_username}")
        ]
    ])
    await cb.message.edit_text(
        f"🗑️ <b>Подтверждение удаления</b>\n\n"
        f"Вы уверены, что хотите удалить бота @{bot_username}?\n\n"
        f"⚠️ <b>Внимание:</b> Это действие необратимо! Бот будет:\n"
        f"• Остановлен\n"
        f"• Удален из вашего списка\n"
        f"• Все данные будут утеряны\n\n"
        f"Пользователи бота смогут продолжить использовать сервис через других ботов.",
        reply_markup=confirmation_kb
    )
    await cb.answer()
@router.callback_query(F.data.startswith("bot_delete_confirm:"))
async def bot_delete_confirm(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    process_msg = await cb.message.answer("🗑️ <b>Удаляем бота...</b>")
    try:
        owner_id = await r.hget(BOT_OWNER_INDEX_KEY, bot_username)
        if not owner_id or int(owner_id) != user_id:
            await process_msg.edit_text("❌ <b>У вас нет прав для удаления этого бота</b>")
            return
        await bot_manager.stop_bot_instance(bot_username)
        user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
        await r.hdel(user_bots_key, bot_username)
        await r.hdel(BOT_OWNER_INDEX_KEY, bot_username)
        await process_msg.edit_text(
            f"✅ <b>Бот @{bot_username} успешно удален!</b>\n\n"
            f"Бот был остановлен и удален из вашего списка."
        )
        await my_bots_callback(cb)
    except Exception as e:
        logger.error(f"Error deleting bot: {e}")
        await process_msg.edit_text(f"❌ <b>Ошибка при удалении бота: {str(e)}</b>")

@router.callback_query(F.data.startswith("bot_start:"))
async def bot_start_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    process_msg = await cb.message.answer("🔄 <b>Запускаем бота...</b>")
    try:
        success = await bot_manager.start_bot_instance(bot_username)
        if success:
            user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
            bot_data_raw = await r.hget(user_bots_key, bot_username)
            if bot_data_raw:
                bot_data = json.loads(bot_data_raw)
                bot_data["is_active"] = True
                bot_data["status"] = "running"
                bot_data["started_at"] = time.time()
                await r.hset(user_bots_key, bot_username, json.dumps(bot_data))
            await process_msg.edit_text(f"✅ <b>Бот @{bot_username} успешно запущен!</b>")
            await manage_bot_callback(cb)
        else:
            await process_msg.edit_text(f"❌ <b>Не удалось запустить бота @{bot_username}</b>")  
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        await process_msg.edit_text(f"❌ <b>Ошибка при запуске бота: {str(e)}</b>")
@router.callback_query(F.data.startswith("bot_stop:"))
async def bot_stop_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    process_msg = await cb.message.answer("🔄 <b>Останавливаем бота...</b>")
    try:
        success = await bot_manager.stop_bot_instance(bot_username)
        if success:
            user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
            bot_data_raw = await r.hget(user_bots_key, bot_username)
            if bot_data_raw:
                bot_data = json.loads(bot_data_raw)
                bot_data["is_active"] = False
                bot_data["status"] = "stopped"
                await r.hset(user_bots_key, bot_username, json.dumps(bot_data))
            await process_msg.edit_text(f"✅ <b>Бот @{bot_username} остановлен!</b>")
            await manage_bot_callback(cb)
        else:
            await process_msg.edit_text(f"❌ <b>Не удалось остановить бота @{bot_username}</b>")
    except Exception as e:
        logger.error(f"Error stopping bot: {e}")
        await process_msg.edit_text(f"❌ <b>Ошибка при остановке бота: {str(e)}</b>")
@router.callback_query(F.data.startswith("bot_toggle:"))
async def bot_toggle_callback(cb: CallbackQuery):
    bot_username = cb.data.split(":")[1]
    user_id = cb.from_user.id
    user_bots_key = USER_BOT_TOKENS_KEY.format(user_id=user_id)
    bot_data_raw = await r.hget(user_bots_key, bot_username)
    if not bot_data_raw:
        await cb.answer("❌ Бот не найден", show_alert=True)
        return
    bot_data = json.loads(bot_data_raw)
    current_status = bot_data.get("is_active", True)
    new_status = not current_status
    bot_data["is_active"] = new_status
    await r.hset(user_bots_key, bot_username, json.dumps(bot_data))
    status_text = "активирован" if new_status else "деактивирован"
    await cb.answer(f"✅ Бот {status_text}")
    await manage_bot_callback(cb)
@router.callback_query(F.data.startswith("moderation_list"))
async def moderation_list_callback(cb: CallbackQuery):
    if not await is_user_admin(cb.from_user.id):
        await cb.answer("❌ Доступ запрещен", show_alert=True)
        return
    pending_user_ids = await r.smembers(USER_APPROVAL_KEY)
    if not pending_user_ids:
        await cb.message.answer("⏳ Нет заявок ожидающих модерации")
        await cb.answer()
        return
    text = "📋 <b>Заявки на модерацию</b>\n\n"
    keyboard_buttons = []
    for user_id_str in list(pending_user_ids)[:10]:
        user_id = int(user_id_str)
        user_data_raw = await r.get(f"user:approval:data:{user_id}")        
        if user_data_raw:
            user_data = json.loads(user_data_raw)
            reg_time = datetime.fromtimestamp(user_data["registration_date"]).strftime("%H:%M")            
            text += f"• @{user_data.get('username', 'нет')} (ID: {user_id}) - {reg_time}\n"
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=f"👤 {user_data.get('username', user_id)}",
                    callback_data=f"admin_approve_user:{user_id}"
                )
            ])
    keyboard_buttons.append([
        InlineKeyboardButton(text="👈 Назад", callback_data="moderation_panel")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.message(Command("users"))
async def cmd_users(m: Message):
    user_id = m.from_user.id
    is_admin = await is_user_admin(user_id)
    if is_admin:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Общая статистика", callback_data="stats:overview")],
            [InlineKeyboardButton(text="👥 Все пользователи", callback_data="stats:user_list:0")],
            [InlineKeyboardButton(text="👤 Мои рефералы", callback_data="stats:my_referrals:0")],
            [InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="stats:search")],
            [InlineKeyboardButton(text="🔄 Активные пользователи", callback_data="stats:active")],
        ])
        text = "📈 <b>Статистика пользователей</b> 👑 <b>АДМИНИСТРАТОР</b>\n\nВыберите действие:"
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика рефералов", callback_data="stats:overview")],
            [InlineKeyboardButton(text="👤 Список рефералов", callback_data="stats:my_referrals:0")],
            [InlineKeyboardButton(text="📈 Детальная статистика", callback_data="ref_stats")],
        ])
        text = "📈 <b>Статистика ваших рефералов</b>\n\nВыберите действие:"
    await m.answer(text, reply_markup=kb)
@router.message(Command("setrole"))
async def cmd_setrole(m: Message):
    if not await is_user_admin(m.from_user.id):
        await m.answer("❌ Эта команда доступна только администраторам")
        return
    if len(m.text.split()) < 3:
        await m.answer(
            "👑 <b>Установка роли пользователя</b>\n\n"
            "Использование:\n"
            "<code>/setrole USER_ID ROLE</code>\n\n"
            "Доступные роли:\n"
            "• <code>admin</code> - администратор\n"
            "• <code>user</code> - обычный пользователь\n\n"
            "Пример:\n"
            "<code>/setrole 123456789 admin</code>"
        )
        return
    try:
        target_user_id = int(m.text.split()[1])
        role = m.text.split()[2].lower()
        if role not in [UserRole.ADMIN, UserRole.USER]:
            await m.answer("❌ Неверная роль. Используйте: admin или user")
            return
        await set_user_role(target_user_id, role)
        user_data = await get_user_by_id(target_user_id)
        username = user_data.get('username', 'N/A') if user_data else 'N/A'
        await m.answer(
            f"✅ <b>Роль пользователя обновлена</b>\n\n"
            f"👤 Пользователь: @{username}\n"
            f"🆔 ID: <code>{target_user_id}</code>\n"
            f"👑 Новая роль: <b>{role.upper()}</b>"
        )
    except ValueError:
        await m.answer("❌ Неверный формат USER_ID")
    except Exception as e:
        logger.error(f"Error in setrole: {e}")
        await m.answer("❌ Ошибка при установке роли")
@router.message(Command("stats"))
async def cmd_stats(m: Message):
    await show_overview_stats(m)
async def search_users(query: str) -> list[dict]:
    users = await get_all_users_data()
    results = []
    query_lower = query.lower()
    for user in users:
        if not isinstance(user, dict):
            continue
        if query.isdigit() and str(user.get('user_id', '')).startswith(query):
            results.append(user)
            continue
        username = user.get('username', '').lower()
        if query_lower in username:
            results.append(user)
            continue
        first_name = user.get('first_name', '').lower()
        if query_lower in first_name:
            results.append(user)
            continue
        last_name = user.get('last_name', '').lower()
        if query_lower in last_name:
            results.append(user)
    return results
@router.message(Command("start"))
async def cmd_start(m: Message):
    user_id = m.from_user.id
    username = m.from_user.username
    first_name = m.from_user.first_name or ""
    last_name = m.from_user.last_name or ""
    referral_code = None
    referrer_id = None
    if len(m.text.split()) > 1:
        start_param = m.text.split()[1]
        if start_param.startswith('ref_'):
            referral_code = start_param[4:]  
            referrer_id = await get_user_by_referral_code(referral_code)
    existing_user = await get_user_by_id(user_id)
    if existing_user and referral_code and referrer_id:
        success = await attach_existing_user_to_referrer(user_id, referral_code)
        if success:
            await m.answer(
                "🔗 <b>Вы привязаны к реферальной программе!</b>\n\n"
                f"Теперь вы участвуете в реферальной программе пользователя.\n"
                f"Используйте /refstats чтобы посмотреть свою статистику."
            )
            return
    current_chat_id = await get_support_chat_id()
    if not current_chat_id:
        await set_support_chat_id(m.chat.id)
        logger.info(f"Auto-set support chat_id to: {m.chat.id}")
    if await is_user_admin(user_id):
        await set_user_role(user_id, UserRole.ADMIN)
        user_role = UserRole.ADMIN
    else:
        await set_user_role(user_id, UserRole.USER)
        user_role = UserRole.USER
    is_admin = await is_user_admin(user_id)
    if is_admin:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика пользователей", callback_data="stats:overview")],
            [InlineKeyboardButton(text="👥 Все пользователи", callback_data="stats:user_list:0")],
            [InlineKeyboardButton(text="👤 Мои рефералы", callback_data="stats:my_referrals:0")],
            [InlineKeyboardButton(text="⏳ Модерация пользователей", callback_data="moderation_panel")],
            [
                InlineKeyboardButton(text="⚙️ Настройки чата", callback_data="settings"),
                InlineKeyboardButton(text="📊 Очередь", callback_data="queue_info")
            ]
        ])
        await m.answer(
            "🤖 <b>Бот поддержки активен</b> 👑 <b>АДМИНИСТРАТОР</b>\n\n"
            "Доступные команды:\n"
            "• /users - Управление пользователями\n"
            "• /stats - Общая статистика\n"
            "• /moderation - Панель модерации\n"
            "• /refstats - Реферальная статистика\n"
            "• /setchat - Привязать чат для уведомлений\n"
            "• /getchat - Показать текущий chat_id\n"
            "• /queue - Длина очереди\n\n"
            "Или используйте кнопки ниже:",
            reply_markup=kb
        )
        return
    if referrer_id and referrer_id != user_id and not existing_user:
        await r.setex(f"user:{user_id}:referrer", 86400 * 30, str(referrer_id))
        logger.info(f"Реферальная связь сохранена: {user_id} -> {referrer_id}")
    is_approved = await r.sismember(USER_APPROVAL_APPROVED_KEY, str(user_id))
    if is_approved:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Создать трейдинг бота", callback_data="create_trading_bot")],
            [InlineKeyboardButton(text="📊 Мои боты", callback_data="my_bots")],
            [InlineKeyboardButton(text="👤 Мои рефералы", callback_data="stats:my_referrals:0")],
            [InlineKeyboardButton(text="📈 Реферальная статистика", callback_data="ref_stats")],
            [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")],
        ])
        await m.answer(
            "🎉 <b>Добро пожаловать!</b>\n\n"
            "Вы уже одобрены и можете создавать своих трейдинг ботов.\n\n"
            "Используйте /refstats чтобы посмотреть вашу реферальную статистику.",
            reply_markup=kb
        )
        if referrer_id and referrer_id != user_id:
            existing_refs = await get_user_referrals(referrer_id)
            if user_id not in existing_refs:
                await add_referral(referrer_id, user_id)
                logger.info(f"Новый реферал: {user_id} приглашен пользователем {referrer_id}")
                try:
                    await bot.send_message(
                        chat_id=referrer_id,
                        text=(
                            "🎉 <b>Новый реферал!</b>\n\n"
                            f"По вашей ссылке зарегистрировался новый пользователь:\n"
                            f"👤 @{username or 'без username'}\n"
                            f"🆔 ID: <code>{user_id}</code>\n\n"
                            f"Используйте /refstats для просмотра статистики"
                        )
                    )
                except Exception as e:
                    logger.error(f"Не удалось уведомить реферера {referrer_id}: {e}")
                
                await push_notify_event({
                    "type": "referral_registered",
                    "referrer_id": referrer_id,
                    "referred_user_id": user_id,
                    "referred_username": username,
                    "timestamp": time.time()
                })
    else:
        user_data = {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "registration_date": time.time(),
            "referral_code": referral_code,
            "referrer_id": referrer_id, 
            "role": user_role
        }
        if not await validate_user_data(user_data):
            await m.answer("❌ Ошибка регистрации. Попробуйте позже.")
            return
        await r.sadd(USER_APPROVAL_KEY, str(user_id))
        await r.setex(f"user:approval:data:{user_id}", 86400, json.dumps(user_data))
        admin_chat_id = await get_support_chat_id()
        if admin_chat_id:
            role_display = "👑 Администратор" if user_role == UserRole.ADMIN else "👤 Обычный пользователь"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_approve_user:{user_id}")]
            ])
            await bot.send_message(
                chat_id=admin_chat_id,
                text=(
                    "🆕 <b>Новая заявка на доступ</b>\n\n"
                    f"👤 Пользователь: @{username or 'без username'}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"👤 Имя: {first_name} {last_name}\n"
                    f"📊 Реферер: {f'ID {referrer_id}' if referrer_id else 'нет'}\n"
                    f"🎭 Роль: {role_display}\n\n"
                    "Выберите действие:"
                ),
                reply_markup=kb
            )
        
        await m.answer(
            "⏳ <b>Ваша заявка на рассмотрении</b>\n\n"
            "Администратор получил вашу заявку и скоро её рассмотрит.\n\n"
            "После одобрения вы сможете:\n"
            "• Создавать своих трейдинг ботов\n"
            "• Использовать реферальную систему\n"
            "• Просматривать статистику ваших рефералов\n"
            "• Получить доступ ко всем функциям\n\n"
            "О результате вам сообщат в этом чате."
        )
async def validate_user_data(user_data: dict) -> bool:
    try:
        required_fields = ['user_id', 'username', 'first_name', 'last_name']
        for field in required_fields:
            if field not in user_data:
                logger.warning(f"Missing required field {field} in user data")
                return False
        if not isinstance(user_data['user_id'], int):
            logger.warning(f"Invalid user_id type: {type(user_data['user_id'])}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error validating user data: {e}")
        return False
@router.callback_query(F.data.startswith("stats:my_referrals:"))
async def stats_my_referrals(cb: CallbackQuery):
    user_id = cb.from_user.id
    page = int(cb.data.split(":")[2])
    await show_my_referrals(cb.message, user_id, page)
    await cb.answer()
async def show_my_referrals(message: Message, user_id: int, page: int = 0, page_size: int = 10):
    referrals = await get_user_referrals(user_id)
    if not referrals:
        text = "👥 <b>У вас пока нет рефералов</b>\n\n"
        if await is_user_admin(user_id):
            text += "Используйте кнопку 'Все пользователи' для просмотра всех пользователей."
        else:
            text += "Приглашайте пользователей по вашей реферальной ссылке!"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Реферальная статистика", callback_data="ref_stats")],
            [InlineKeyboardButton(text="👈 Назад", callback_data="stats:overview")]
        ])
        await message.answer(text, reply_markup=kb)
        return
    referrals_data = []
    for ref_id in referrals:
        user_data = await get_user_by_id(ref_id)
        if user_data:
            referrals_data.append(user_data)
    referrals_data.sort(key=lambda x: x.get('last_activity', 0), reverse=True)
    total_pages = (len(referrals_data) + page_size - 1) // page_size
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_referrals = referrals_data[start_idx:end_idx]
    is_admin = await is_user_admin(user_id)
    role_text = "👑 АДМИНИСТРАТОР | " if is_admin else ""
    text = f"👥 <b>Ваши рефералы</b> ({role_text}стр. {page + 1}/{total_pages})\n\n"
    text += f"📊 Всего рефералов: {len(referrals)}\n\n"
    keyboard = []
    for user in page_referrals:
        user_id_ref = user.get('user_id', 'N/A')
        username = user.get('username', '')
        first_name = user.get('first_name', '')
        last_name = user.get('last_name', '')
        balance = user.get('balance', 0)
        display_name = f"{first_name} {last_name}".strip() or "Без имени"
        if username:
            display_name = f"@{username}"
        is_active = time.time() - user.get('last_activity', 0) < 24 * 3600
        status = "🟢" if is_active else "⚫"
        user_text = f"{status} {display_name} | ${balance:.2f}"
        keyboard.append([
            InlineKeyboardButton(
                text=user_text,
                callback_data=f"user_detail:{user_id_ref}"
            )
        ])
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"stats:my_referrals:{page-1}"))
    if page < total_pages - 1:
        pagination_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"stats:my_referrals:{page+1}"))
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    if is_admin:
        keyboard.append([
            InlineKeyboardButton(text="👥 Все пользователи", callback_data="stats:user_list:0"),
            InlineKeyboardButton(text="📊 Общая статистика", callback_data="stats:overview")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="📊 Реферальная статистика", callback_data="ref_stats"),
            InlineKeyboardButton(text="👈 Назад", callback_data="stats:overview")
        ])
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await message.answer(text, reply_markup=kb)
@router.callback_query(F.data == "stats:overview")
async def stats_overview(cb: CallbackQuery):
    await show_overview_stats(cb.message)
    await cb.answer()
@router.callback_query(F.data.startswith("stats:user_list:"))
async def stats_user_list(cb: CallbackQuery):
    page = int(cb.data.split(":")[2])
    await show_user_list(cb.message, page)
    await cb.answer()
@router.callback_query(F.data == "stats:search")
async def stats_search(cb: CallbackQuery):
    await cb.message.answer(
        "🔍 <b>Поиск пользователя</b>\n\n"
        "Введите username, имя, фамилию или ID пользователя:"
    )
    await cb.answer()
@router.callback_query(F.data == "stats:active")
async def stats_active(cb: CallbackQuery):
    await show_active_users(cb.message)
    await cb.answer()
@router.callback_query(F.data.startswith("user_detail:"))
async def user_detail(cb: CallbackQuery):
    user_id = int(cb.data.split(":")[1])
    await show_user_detail(cb.message, user_id, cb.from_user.id)
    await cb.answer()
@router.callback_query(F.data == "moderation_panel")
async def moderation_panel_callback(cb: CallbackQuery):
    if not await is_user_admin(cb.from_user.id):
        await cb.answer("❌ Доступ запрещен", show_alert=True)
        return
    pending_count = await r.scard(USER_APPROVAL_KEY)
    approved_count = await r.scard(USER_APPROVAL_APPROVED_KEY)
    pending_users = []
    pending_user_ids = await r.smembers(USER_APPROVAL_KEY)
    for user_id in list(pending_user_ids)[:5]: 
        user_data_raw = await r.get(f"user:approval:data:{user_id}")
        if user_data_raw:
            user_data = json.loads(user_data_raw)
            pending_users.append(user_data)
    text = (
        "⚙️ <b>Панель модерации пользователей</b>\n\n"
        f"⏳ Ожидают одобрения: {pending_count}\n"
        f"✅ Одобрено пользователей: {approved_count}\n\n"
    )
    if pending_users:
        text += "<b>Последние заявки:</b>\n"
        for user in pending_users:
            reg_time = datetime.fromtimestamp(user["registration_date"]).strftime("%H:%M")
            text += f"• @{user.get('username', 'нет')} (ID: {user['user_id']}) - {reg_time}\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список заявок", callback_data="moderation_list")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="moderation_panel")]
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
async def show_overview_stats(message: Message):
    user_id = message.from_user.id
    is_admin = await is_user_admin(user_id)
    if is_admin:
        users = await get_all_users_data()
        total_users = len(users)
        total_balance = sum(user.get('balance', 0) for user in users)
        total_deposits = sum(user.get('stats', {}).get('total_deposits', 0) for user in users)
        total_withdrawals = sum(user.get('stats', {}).get('total_withdrawals', 0) for user in users)
        total_trades = sum(user.get('stats', {}).get('total_trades', 0) for user in users)
        active_users = len([
            user for user in users 
            if time.time() - user.get('last_activity', 0) < 24 * 3600
        ])
        new_today = len([
            user for user in users 
            if time.time() - user.get('registration_date', time.time()) < 24 * 3600
        ])
        text = (
            f"📊 <b>Общая статистика</b> 👑 <b>АДМИНИСТРАТОР</b>\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"🟢 Активных (24ч): {active_users}\n"
            f"🆕 Новых сегодня: {new_today}\n"
            f"💰 Общий баланс: ${total_balance:.2f}\n"
            f"📥 Всего депозитов: ${total_deposits:.2f}\n"
            f"📤 Всего выводов: ${total_withdrawals:.2f}\n"
            f"📈 Всего сделок: {total_trades}\n"
            f"🏦 Общий оборот: ${total_deposits + total_withdrawals:.2f}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Все пользователи", callback_data="stats:user_list:0")],
            [InlineKeyboardButton(text="👤 Мои рефералы", callback_data="stats:my_referrals:0")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="stats:overview")]
        ])
    else:
        referrals = await get_user_referrals(user_id)
        referrals_count = len(referrals)
        active_referrals = 0
        total_balance = 0.0
        total_deposits = 0.0
        total_withdrawals = 0.0
        total_trades = 0
        for ref_id in referrals:
            user_data = await get_user_by_id(ref_id)
            if user_data:
                if time.time() - user_data.get('last_activity', 0) < 7 * 24 * 3600:
                    active_referrals += 1
                total_balance += user_data.get('balance', 0)
                total_deposits += user_data.get('stats', {}).get('total_deposits', 0)
                total_withdrawals += user_data.get('stats', {}).get('total_withdrawals', 0)
                total_trades += user_data.get('stats', {}).get('total_trades', 0)
        text = (
            f"📊 <b>Статистика ваших рефералов</b>\n\n"
            f"👥 Всего рефералов: {referrals_count}\n"
            f"🟢 Активных (7 дней): {active_referrals}\n"
            f"💰 Суммарный баланс: ${total_balance:.2f}\n"
            f"📥 Всего депозитов: ${total_deposits:.2f}\n"
            f"📤 Всего выводов: ${total_withdrawals:.2f}\n"
            f"📈 Всего сделок: {total_trades}\n"
            f"🏦 Общий оборот: ${total_deposits + total_withdrawals:.2f}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Список рефералов", callback_data="stats:my_referrals:0")],
            [InlineKeyboardButton(text="📈 Детальная статистика", callback_data="ref_stats")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="stats:overview")]
        ])
    await message.answer(text, reply_markup=kb)
async def show_user_list(message: Message, page: int = 0, page_size: int = 10):
    users = await get_all_users_data()
    users.sort(key=lambda x: x.get('last_activity', 0), reverse=True)
    total_pages = (len(users) + page_size - 1) // page_size
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_users = users[start_idx:end_idx]
    if not page_users:
        await message.answer("❌ Пользователи не найдены")
        return
    text = f"👥 <b>Список пользователей</b> (стр. {page + 1}/{total_pages})\n\n"
    keyboard = []
    for user in page_users:
        user_id = user.get('user_id', 'N/A')
        username = user.get('username', '')
        first_name = user.get('first_name', '')
        last_name = user.get('last_name', '')
        balance = user.get('balance', 0)
        display_name = f"{first_name} {last_name}".strip() or "Без имени"
        if username:
            display_name = f"@{username}"
        is_active = time.time() - user.get('last_activity', 0) < 24 * 3600
        status = "🟢" if is_active else "⚫"
        user_text = f"{status} {display_name} | ${balance:.2f}"
        keyboard.append([
            InlineKeyboardButton(
                text=user_text,
                callback_data=f"user_detail:{user_id}"
            )
        ])
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"stats:user_list:{page-1}"))
    if page < total_pages - 1:
        pagination_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"stats:user_list:{page+1}"))
    if pagination_buttons:
        keyboard.append(pagination_buttons)   
    keyboard.append([
        InlineKeyboardButton(text="📊 Общая статистика", callback_data="stats:overview"),
        InlineKeyboardButton(text="🔍 Поиск", callback_data="stats:search")
    ])   
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)   
    if message.chat.type == "private":
        await message.answer(text, reply_markup=kb)
    else:
        await message.reply(text, reply_markup=kb)
async def show_user_detail(message: Message, user_id: int, callback_user_id: int = None):
    if callback_user_id is None:
        current_user_id = message.from_user.id
    else:
        current_user_id = callback_user_id
    logger.info(f"🔍 Текущий пользователь: {current_user_id}, запрашивает информацию о пользователе: {user_id}")
    user = await get_user_by_id(user_id)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return
    stats = user.get('stats', {})
    username = user.get('username', '')
    first_name = user.get('first_name', '')
    last_name = user.get('last_name', '')
    balance = user.get('balance', 0)
    registration_date = user.get('registration_date', time.time())
    last_activity = user.get('last_activity', time.time())
    reg_date = datetime.fromtimestamp(registration_date).strftime('%Y-%m-%d %H:%M')
    last_active = datetime.fromtimestamp(last_activity).strftime('%Y-%m-%d %H:%M')
    is_active = time.time() - last_activity < 24 * 3600
    status = "🟢 Активен" if is_active else "⚫ Неактивен"
    min_deposit = await get_user_min_deposit(user_id)
    referrer_id = await get_user_referrer(user_id)
    referrer_info = ""
    if referrer_id:
        referrer_data = await get_user_by_id(referrer_id)
        if referrer_data:
            referrer_username = referrer_data.get('username', '')
            referrer_name = f"@{referrer_username}" if referrer_username else f"ID {referrer_id}"
            referrer_info = f"👥 Реферер: {referrer_name}\n"
    referrals_count = await get_user_referrals_count(user_id)
    referrer_info += f"📊 Рефералов: {referrals_count}\n"
    text = (
        f"👤 <b>Детальная информация</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"👤 Имя: {first_name} {last_name}\n"
        f"📱 Username: @{username if username else 'нет'}\n"
        f"📅 Регистрация: {reg_date}\n"
        f"⏰ Последняя активность: {last_active}\n"
        f"📊 Статус: {status}\n"
        f"💰 Минимальный депозит: <b>${min_deposit:.2f}</b>\n" 
        f"{referrer_info}\n"
        f"💰 Баланс: ${balance:.2f}\n"
        f"📈 Сделок: {stats.get('total_trades', 0)}\n"
        f"✅ Побед: {stats.get('wins', 0)}\n"
        f"❌ Поражений: {stats.get('losses', 0)}\n"
        f"📥 Депозитов: ${stats.get('total_deposits', 0):.2f}\n"
        f"📤 Выводов: ${stats.get('total_withdrawals', 0):.2f}\n"
        f"🎯 Общий PnL: ${stats.get('total_pnl', 0):.2f}"
    )
    keyboard_buttons = []
    is_admin_result = await is_user_admin(current_user_id)
    logger.info(f"🔍 DEBUG show_user_detail: current_user_id={current_user_id}, is_admin={is_admin_result}")
    if is_admin_result:
        keyboard_buttons.append([
            InlineKeyboardButton(text="💰 Изменить баланс", callback_data=f"admin_change_balance:{user_id}"),
            InlineKeyboardButton(text="🏦 Мин. депозит", callback_data=f"change_mindeposit:{user_id}")  
        ])
        keyboard_buttons.append([
            InlineKeyboardButton(text="🗑️ Удалить пользователя", callback_data=f"admin_delete_user:{user_id}")
        ])
        logger.info(f"✅ Кнопка 'Мин. депозит' добавлена для админа {current_user_id}")
    else:
        logger.info(f"❌ Пользователь {current_user_id} не является администратором")
    logger.info(f"🔍 DEBUG show_user_detail: user_id={current_user_id}, is_admin={is_admin_result}")
    if username:
        keyboard_buttons.append([
            InlineKeyboardButton(text="💬 Написать", url=f"https://t.me/{username}")
        ])
    else:
        keyboard_buttons.append([
            InlineKeyboardButton(text="📋 ID пользователя", callback_data=f"show_id:{user_id}")
        ])
    if referrals_count > 0 and is_admin_result:
        keyboard_buttons.append([
            InlineKeyboardButton(text="👥 Рефералы пользователя", callback_data=f"user_referrals:{user_id}:0")
        ])
    keyboard_buttons.append([
        InlineKeyboardButton(text="👥 Назад к списку", callback_data="stats:user_list:0"),
        InlineKeyboardButton(text="📊 Общая статистика", callback_data="stats:overview")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    try:
        profile_photos = await bot.get_user_profile_photos(user_id, limit=1)
        if profile_photos.total_count > 0:
            file_id = profile_photos.photos[0][-1].file_id
            await message.answer_photo(
                photo=file_id,
                caption=text,
                reply_markup=kb
            )
            return
    except Exception as e:
        logger.warning(f"Could not get profile photo for user {user_id}: {e}")
    await message.answer(text, reply_markup=kb)
@router.callback_query(F.data.startswith("admin_change_balance:"))
async def admin_change_balance(cb: CallbackQuery, state: FSMContext):
    try:
        if not await is_user_admin(cb.from_user.id):
            await cb.answer("❌ Доступ запрещен", show_alert=True)
            return
        user_id = int(cb.data.split(":")[1])
        logger.info(f"🔄 Admin {cb.from_user.id} changing balance for user {user_id}")
        user_data = await get_user_by_id(user_id)
        if not user_data:
            await cb.answer("❌ Пользователь не найден", show_alert=True)
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(SupportStates.WAIT_BALANCE_AMOUNT)
        username = user_data.get('username', 'N/A')
        current_balance = user_data.get('balance', 0)
        await cb.message.answer(
            f"💰 <b>Изменение баланса пользователя</b>\n\n"
            f"👤 Пользователь: @{username}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"💵 Текущий баланс: <b>${current_balance:.2f}</b>\n\n"
            "Введите сумму для изменения:\n"
            "<i>Положительное число - зачисление, отрицательное - списание</i>\n"
            "<i>Пример: 100.50 или -50.25</i>"
        )
        await cb.answer()
    except Exception as e:
        logger.error(f"Error in admin_change_balance: {e}")
        await cb.answer("❌ Ошибка при изменении баланса", show_alert=True)    
@router.message(Command("debug_notify"))
async def cmd_debug_notify(m: Message):
    try:
        await r.ping()
        notify_ru_len = await r.llen("trading:notify:ru")
        notify_en_len = await r.llen("trading:notify:en")
        support_queue_len = await r.llen(SUPPORT_QUEUE_KEY)
        last_support_events = []
        for i in range(5):
            event = await r.lindex(SUPPORT_QUEUE_KEY, i)
            if event:
                try:
                    event_data = json.loads(event)
                    last_support_events.append(event_data.get('type', 'unknown'))
                except:
                    pass
        text = (
            "🐛 <b>Debug: Система уведомлений</b>\n\n"
            f"🟢 Redis: подключен\n"
            f"📨 Очередь RU: {notify_ru_len}\n"
            f"📨 Очередь EN: {notify_en_len}\n"
            f"🔄 Очередь поддержки: {support_queue_len}\n"
        )
        if last_support_events:
            text += f"\n📊 Последние события в очереди поддержки:\n"
            for i, event_type in enumerate(last_support_events):
                text += f"{i+1}. {event_type}\n"
        await m.answer(text)
    except Exception as e:
        await m.answer(f"❌ Ошибка отладки: {e}")
@router.callback_query(F.data.startswith("user_referrals:"))
async def user_referrals_handler(cb: CallbackQuery):
    try:
        parts = cb.data.split(":")
        target_user_id = int(parts[1])
        page = int(parts[2])
        if not await is_user_admin(cb.from_user.id):
            await cb.answer("❌ Доступ запрещен", show_alert=True)
            return
        await show_user_referrals_detail(cb.message, target_user_id, page, callback_user_id=cb.from_user.id)
        await cb.answer()
    except Exception as e:
        logger.error(f"Error in user_referrals_handler: {e}")
        await cb.answer("❌ Ошибка при загрузке рефералов", show_alert=True)
async def show_user_referrals_detail(message: Message, user_id: int, page: int = 0, page_size: int = 10, callback_user_id: int = None):
    if callback_user_id is None:
        current_user_id = message.from_user.id
    else:
        current_user_id = callback_user_id
    if not await is_user_admin(current_user_id):
        await message.answer("❌ Доступ запрещен")
        return
    referrals = await get_user_referrals(user_id)
    user_data = await get_user_by_id(user_id)
    username = user_data.get('username', 'N/A') if user_data else 'N/A'
    if not referrals:
        text = f"👥 <b>Рефералы пользователя @{username}</b>\n\nУ пользователя пока нет рефералов."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👈 Назад", callback_data=f"user_detail:{user_id}")]
        ])
        await message.answer(text, reply_markup=kb)
        return
    referrals_data = []
    for ref_id in referrals:
        ref_data = await get_user_by_id(ref_id)
        if ref_data:
            referrals_data.append(ref_data)
    referrals_data.sort(key=lambda x: x.get('last_activity', 0), reverse=True)
    total_pages = (len(referrals_data) + page_size - 1) // page_size
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_referrals = referrals_data[start_idx:end_idx]
    text = f"👥 <b>Рефералы пользователя @{username}</b> (стр. {page + 1}/{total_pages})\n\n"
    text += f"📊 Всего рефералов: {len(referrals)}\n\n"
    keyboard = []
    for user in page_referrals:
        user_id_ref = user.get('user_id', 'N/A')
        username_ref = user.get('username', '')
        first_name = user.get('first_name', '')
        last_name = user.get('last_name', '')
        balance = user.get('balance', 0)
        display_name = f"{first_name} {last_name}".strip() or "Без имени"
        if username_ref:
            display_name = f"@{username_ref}"
        is_active = time.time() - user.get('last_activity', 0) < 24 * 3600
        status = "🟢" if is_active else "⚫"
        user_text = f"{status} {display_name} | ${balance:.2f}"
        keyboard.append([
            InlineKeyboardButton(
                text=user_text,
                callback_data=f"user_detail:{user_id_ref}"
            )
        ])
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton(
            text="⬅️ Назад", 
            callback_data=f"user_referrals:{user_id}:{page-1}"
        ))
    if page < total_pages - 1:
        pagination_buttons.append(InlineKeyboardButton(
            text="Вперед ➡️", 
            callback_data=f"user_referrals:{user_id}:{page+1}"
        ))
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    keyboard.append([
        InlineKeyboardButton(text="👈 Назад к пользователю", callback_data=f"user_detail:{user_id}")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await message.answer(text, reply_markup=kb)
@router.callback_query(F.data.startswith("show_id:"))
async def show_user_id(cb: CallbackQuery):
    try:
        user_id = int(cb.data.split(":")[1])
        await cb.answer(f"ID пользователя: {user_id}", show_alert=True)
    except Exception as e:
        logger.error(f"Error in show_user_id: {e}")
        await cb.answer("Ошибка при получении ID", show_alert=True)
async def show_active_users(message: Message):
    users = await get_all_users_data()
    active_users = [
        user for user in users 
        if time.time() - user.get('last_activity', 0) < 24 * 3600
    ]
    if not active_users:
        await message.answer("❌ Нет активных пользователей за последние 24 часа")
        return
    active_users.sort(key=lambda x: x.get('last_activity', 0), reverse=True)
    text = "🟢 <b>Активные пользователи</b> (последние 24 часа)\n\n"
    keyboard = []
    for user in active_users[:15]:
        user_id = user.get('user_id', 'N/A')
        username = user.get('username', '')
        first_name = user.get('first_name', '')
        last_name = user.get('last_name', '')
        display_name = f"{first_name} {last_name}".strip() or "Без имени"
        if username:
            display_name = f"@{username}"
        last_active = user.get('last_activity', time.time())
        hours_ago = int((time.time() - last_active) / 3600)        
        user_text = f"🟢 {display_name} ({hours_ago}ч назад)" 
        keyboard.append([
            InlineKeyboardButton(
                text=user_text,
                callback_data=f"user_detail:{user_id}"
            )
        ])
    keyboard.append([
        InlineKeyboardButton(text="👥 Все пользователи", callback_data="stats:user_list:0"),
        InlineKeyboardButton(text="📊 Общая статистика", callback_data="stats:overview")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await message.answer(text, reply_markup=kb)
@router.message(F.text & ~F.command)
async def handle_user_search(m: Message):
    query = m.text.strip()
    if len(query) < 2:
        await m.answer("❌ Слишком короткий запрос для поиска")
        return
    await m.answer("🔍 <b>Ищем пользователей...</b>")
    results = await search_users(query)
    if not results:
        await m.answer("❌ Пользователи не найдены")
        return
    text = f"🔍 <b>Результаты поиска:</b> \"{query}\"\n\nНайдено: {len(results)} пользователей\n\n"
    keyboard = []
    for user in results[:10]: 
        user_id = user.get('user_id', 'N/A')
        username = user.get('username', '')
        first_name = user.get('first_name', '')
        last_name = user.get('last_name', '')
        display_name = f"{first_name} {last_name}".strip() or "Без имени"
        if username:
            display_name = f"@{username}"
        user_text = f"👤 {display_name} (ID: {user_id})"
        keyboard.append([
            InlineKeyboardButton(
                text=user_text,
                callback_data=f"user_detail:{user_id}"
            )
        ])
    keyboard.append([
        InlineKeyboardButton(text="👥 Все пользователи", callback_data="stats:user_list:0")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await m.answer(text, reply_markup=kb)
@router.message(F.document & F.chat.type == "private")
async def handle_pdf_document(m: Message):
    try:
        if not (m.document.mime_type == 'application/pdf' or 
                (m.document.file_name and m.document.file_name.lower().endswith('.pdf'))):
            return  
        logger.info(f"📄 Получен PDF документ от пользователя {m.from_user.id}: {m.document.file_name}")
        user_id = m.from_user.id
        keys_pattern = CARD_TEMP_KEY.format(event_id="*").replace(":", "\\:")
        all_temp_keys = await r.keys(keys_pattern)
        found_event_id = None
        temp_data = None
        for key in all_temp_keys:
            try:
                key_str = key.decode() if isinstance(key, bytes) else key
                event_id = key_str.split(":")[-1]
                data = await get_card_temp(event_id)
                if data and data.get('user_id') == user_id:
                    found_event_id = event_id
                    temp_data = data
                    break
            except Exception:
                continue
        if not found_event_id or not temp_data:
            await m.answer("❌ Не найдено активных запросов на оплату")
            return
        ev = await get_event(found_event_id)
        if not ev:
            await m.answer("❌ Событие не найдено")
            return
        admin_id = temp_data.get('admin_id')
        amount = temp_data.get('amount')
        card_number = temp_data.get('card_number')
        bot_code = temp_data.get('bot_code', 'ru')
        proof_message = (
            f"📎 <b>Получена квитанция об оплате (PDF)</b>\n\n"
            f"👤 Пользователь: @{m.from_user.username or m.from_user.id}\n"
            f"💵 Сумма: ${amount}\n"
            f"💳 Карта: {card_number}\n"
            f"📁 Тип файла: PDF-документ\n"
            f"📄 Файл: {m.document.file_name}\n\n"
            f"Подтвердите получение платежа:"
        )
        try:
            await bot.send_document(
                chat_id=admin_id,
                document=m.document.file_id,
                caption=proof_message,
                reply_markup=admin_confirm_payment_kb(found_event_id)
            )
            if PAYMENT_CONFIRMATION_CHAT_ID:
                await bot.send_document(
                    chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
                    document=m.document.file_id,
                    caption=proof_message,
                    reply_markup=admin_confirm_payment_kb(found_event_id)
                )
            await m.answer(
                "✅ <b>PDF-квитанция отправлена на проверку</b>\n\n"
                "Ожидайте подтверждения платежа администратором."
            )
            logger.info(f"✅ PDF отправлен админу {admin_id} для события {found_event_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки PDF админу: {e}")
            await m.answer("❌ Не удалось отправить PDF администратору")
    except Exception as e:
        logger.error(f"Error in handle_pdf_document: {e}")
        await m.answer("❌ Ошибка при обработке PDF документа")
async def on_startup():
    asyncio.create_task(process_queue())
    asyncio.create_task(process_feed_queue())
def main():
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(on_startup)
    asyncio.run(dp.start_polling(
        bot, 
        allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"],
        drop_pending_updates=True
    ))
if __name__ == "__main__":
    main()