from __future__ import annotations
from aiogram.exceptions import TelegramForbiddenError
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram import F, Router
from aiogram.client.default import DefaultBotProperties
import asyncio
import aiohttp
import json
import math
import os
import random
import re
import signal
import time
import hashlib
import structlog
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.types import Update
from aiogram.methods import GetChat
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command
from aiogram.enums import ParseMode
from functools import wraps
import subprocess
from aiogram.enums import ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from pydantic import BaseModel, Field
import redis.asyncio as redis
import logging, traceback
import hashlib as _hl
import sys, codecs, io
import argparse
import socket
from redis.asyncio import Redis, ConnectionPool
from redis.exceptions import ConnectionError, TimeoutError
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram import F, Router
from aiogram.client.default import DefaultBotProperties
logging.getLogger().handlers = logging.getLogger().handlers[:1]
logger = structlog.get_logger("autotrading-bot") if 'structlog' in globals() else logging.getLogger("autotrading-bot")
BOT_TOKEN_RU = None
TRADE_BOT_TOKEN = os.getenv("TRADE_BOT_TOKEN", "8385870509:AAHdzf0X2wDITzh2hBMmY7g4CHBJ-ab8jzU")
try:
    bot = Bot(token=TRADE_BOT_TOKEN)
    logger.info("✅ Bot instance created")
except Exception as e:
    logger.error(f"❌ Failed to create bot instance: {e}")
    bot = None
router = Router()
channel_router = Router()
REDIS_URL = "redis://default:UwRBirrNGabYOycgxafXyqWNu78KJH26@redis-14197.c340.ap-northeast-2-1.ec2.cloud.redislabs.com:14197"
PAYMENT_CONFIRMATION_CHAT_ID = int(os.getenv("paysmi", "-1002691532093"))
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE")
if LOG_FILE:
    _fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _fh.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    _fh.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(_fh)
CRYPTO_WALLETS_FILE = os.getenv("CRYPTO_WALLETS_FILE", "crypto_wallets.json")
SUPPORT_BOT_URL_RU = os.getenv("SUPPORT_BOT_URL_RU", "https://t.me/aitradingsupport_bot")
SUPPORT_BOT_URL_EN = os.getenv("SUPPORT_BOT_URL_EN", "https://t.me/tradingsupportrobot")
SIGNAL_CHANNEL_ID = int(os.getenv("SIGNAL_CHANNEL_ID", "-1003185878952"))
SUPPORT_FEED_KEY = os.getenv("SUPPORT_FEED_KEY", "support:feed")
BOT_START_TIME = datetime.now(timezone.utc)
_CRYPTO_WALLETS_CACHE = {"mtime": None, "data": {}}
EXCHANGE_RATE_CACHE_DEFAULT = {
    "usd_rub": {"rate": 0.0, "timestamp": 0},
    "usd_uzs": {"rate": 0.0, "timestamp": 0},
    "eth_usdt": {"rate": 0.0, "timestamp": 0},
    "btc_usdt": {"rate": 0.0, "timestamp": 0},
}
_exchange_rate_cache = EXCHANGE_RATE_CACHE_DEFAULT.copy()
CACHE_TTL = 300
REDIS_KEYS = {
    "assets_msg": "user:{uid}:assets_msg",
    "dep_amount": "user:{uid}:dep_amount",
    "dep_token": "user:{uid}:dep_token",
    "wd_token": "user:{uid}:wd_token",
    "wd_network": "user:{uid}:wd_network",
    "wd_address": "user:{uid}:wd_address",
    "wd_pending": "user:{uid}:wd_pending",
    "wd_pending_list": "user:{uid}:wd_pending_list",
    "ref_code": "user:{uid}:ref_code",
    "ref_code_owner": "ref_code:{ref_code}",
    "ref_stats": "user:{uid}:ref_stats",
    "ref_earnings": "user:{uid}:ref_earnings",
    "ref_users": "user:{uid}:ref_users",
    "support_chat_id": "support:chat_id",
    "support_feed": "support:feed",
}
SIDE = ["LONG", "SHORT"]
POS_STATUS = ["OPEN", "CLOSED_TP", "CLOSED_SL", "CLOSED_TIME"]
DEFAULT_NETWORKS = {
    "USDT": ["TRC20", "BEP20", "ERC20"],
    "ETH": ["ERC20"],
    "BTC": ["BTC"],
}
SUPPORT_BOT_USERNAME_RU = os.getenv("SUPPORT_BOT_USERNAME_RU", "aitradingsupport_bot")
SUPPORT_BOT_USERNAME_EN = os.getenv("SUPPORT_BOT_USERNAME_EN", "tradingsupportrobot")
TRADING_BOT_USERNAME = os.getenv("TRADING_BOT_USERNAME", "")
SUPPORT_QUEUE_KEY = os.getenv("SUPPORT_QUEUE_KEY", "support:queue")
NOTIFY_QUEUE_KEY = os.getenv("NOTIFY_QUEUE_KEY", "trading:notify:ru") 
BOT_OWNER_INDEX_KEY = os.getenv("BOT_OWNER_INDEX_KEY", "bot:owner_index")
AMOUNTS = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
LEVERAGES = [1, 2, 3]  
NOTIFY_WORKER_LAST_ACTIVE = 0
NEXT_COUNTDOWN_AT: dict[tuple[int, int], float] = {}  
NEXT_SEND_AT_CHAT: dict[int, float] = {}
WELCOME_IMAGE_URL = "https://i.ibb.co/7JWyRRdp/94af51c3330e.jpg"
ASSETS_IMAGE_URL = WELCOME_IMAGE_URL

async def check_redis_health():
    """定期检查Redis连接状态"""
    while True:
        try:
            start = time.time()
            await r.ping()
            ping_time = (time.time() - start) * 1000
            if ping_time > 500:  # 超过500ms警告
                logger.warning(f"Redis响应缓慢: {ping_time:.1f}ms")
            await asyncio.sleep(60)  # 每分钟检查一次
        except Exception as e:
            logger.error(f"Redis健康检查失败: {e}")
            # 尝试重新连接
            try:
                await r.close()
                await r.initialize()
            except Exception as reconnect_error:
                logger.error(f"Redis重连失败: {reconnect_error}")
            await asyncio.sleep(10)
async def _close_leftover_open_positions_optimized():
    """优化版的位置清理函数"""
    start_time = time.time()
    closed = 0
    processed = 0
    
    try:
        # 使用SCAN而不是KEYS来避免阻塞
        cursor = '0'
        position_keys = []
        
        while True:
            try:
                cursor, keys = await store.r.scan(
                    cursor=cursor, 
                    match="position:*", 
                    count=100
                )
                position_keys.extend(keys)
                if cursor == '0':
                    break
            except Exception as e:
                logger.error(f"扫描位置键失败: {e}")
                break
        
        logger.info(f"找到位置键: {len(position_keys)}")
        
        if not position_keys:
            logger.info("没有需要处理的位置")
            return
        
        # 分批处理，避免内存溢出
        batch_size = 50
        for i in range(0, len(position_keys), batch_size):
            batch = position_keys[i:i+batch_size]
            
            # 获取批量数据
            pipe = store.r.pipeline()
            for key in batch:
                pipe.get(key)
            raw_positions = await pipe.execute()
            
            # 处理每个位置
            tasks = []
            for raw in raw_positions:
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                    p = Position(**data)
                    if p.status == PosStatus.OPEN:
                        tasks.append(_process_single_position(p))
                except Exception as e:
                    logger.warning(f"解析位置数据失败: {e}")
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                closed += sum(1 for r in results if isinstance(r, bool) and r)
            
            processed += len(batch)
            
            # 避免过快处理
            if i + batch_size < len(position_keys):
                await asyncio.sleep(0.1)
        
        elapsed = time.time() - start_time
        logger.info(f"优化清理完成: 处理 {processed}，关闭 {closed}，耗时 {elapsed:.2f}秒")
        
    except Exception as e:
        logger.error(f"优化清理失败: {e}")
async def safe_send_text(
    chat_id: int,
    text: str,
    user_id: Optional[int] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = ParseMode.HTML
) -> Optional[Message]:
    try:
        if user_id is None:
            user_id = chat_id
        owner = await store.get_bot_owner(user_id)
        token = await store.get_user_bot_token(owner)
        trb = Bot(token=token)
        return await trb.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except TelegramForbiddenError:
        logger.info(f"Bot blocked by user {user_id}")
        await send_bot_blocked_event(user_id, "safe_send_text")
        await store.remove_watcher(user_id)
        return None
    except TelegramRetryAfter as e:
        delay = float(getattr(e, "retry_after", 1.0))
        logger.warning(f"Flood control for user {user_id}: {delay}s")
        await asyncio.sleep(delay)
        return await safe_send_text(chat_id, text, user_id, reply_markup, parse_mode)
    except Exception as e:
        logger.error(f"Error sending message to {user_id}: {e}")
        return None
async def get_filtered_amounts(user_id: int) -> list[int]:
    min_dep = await get_user_min_deposit(user_id)
    return [x for x in AMOUNTS if x >= min_dep]

def _load_crypto_wallets() -> dict:
    try:
        cfg_path = Path(__file__).with_name(CRYPTO_WALLETS_FILE)
        if not cfg_path.exists():
            return {}
        mtime = cfg_path.stat().st_mtime
        if _CRYPTO_WALLETS_CACHE.get("mtime") == mtime:
            return _CRYPTO_WALLETS_CACHE.get("data", {})
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _CRYPTO_WALLETS_CACHE["mtime"] = mtime
        _CRYPTO_WALLETS_CACHE["data"] = data or {}
        return _CRYPTO_WALLETS_CACHE["data"]
    except Exception as e:
        try:
            logger.exception("Failed to load crypto wallets file")
        except Exception:
            pass
        return {}
def get_wallet_address(token: str, network: str) -> str | None:
    data = _load_crypto_wallets()
    if not data:
        return None
    token_u = (token or "USDT").upper()
    net_u = (network or "").upper()
    try:
        if isinstance(data, dict) and token_u in {k.upper(): k for k in data}.keys():
            tk = next(k for k in data.keys() if k.upper() == token_u)
            nets = data.get(tk) or {}
            for k, v in nets.items():
                if k.upper() == net_u:
                    return str(v)
            return None
        for k, v in data.items():
            if k.upper() == net_u:
                return str(v)
        return None
    except Exception:
        return None
@router.error()
async def errors_handler(event: Exception, *args, **kwargs):
    try:
        update = None
        user_id = None
        if args:
            for arg in args:
                if isinstance(arg, Update):
                    update = arg
                    break
        if not update and "update" in kwargs:
            update = kwargs["update"]
        if update:
            if update.message:
                user_id = update.message.from_user.id
            elif update.callback_query:
                user_id = update.callback_query.from_user.id
            elif update.my_chat_member:
                user_id = update.my_chat_member.from_user.id
            elif update.channel_post and hasattr(update.channel_post, "sender_chat"):
                user_id = update.channel_post.sender_chat.id
        if user_id:
            error_msg = str(event).lower()
            blocked_phrases = [
                "bot was blocked",
                "user is deactivated",
                "chat not found",
                "forbidden: bot was blocked",
                "bot was kicked",
                "bot was blocked by the user",
            ]
            if any(phrase in error_msg for phrase in blocked_phrases):
                try:
                    await _init_trading_bot_username_once()
                    owner = await store.get_bot_owner(user_id) 
                    support_event = {
                        "type": "bot_blocked",
                        "event_id": f"bot_blocked_{user_id}_{int(time.time() * 1000)}",
                        "user_id": user_id,
                        "timestamp": time.time(),
                        "bot_username": TRADING_BOT_USERNAME,
                        "reason": error_msg,
                        "bot": "ru",
                        "detected_by": "error_handler",
                        "bot_owner_id": owner or user_id,
                    }
                    await store.push_support_event(support_event)
                    logger.info(
                        f"🚫 Bot blocked event sent to support: user {user_id}, reason: {error_msg}"
                    )
                    await store.remove_watcher(user_id)
                except Exception as e:
                    logger.error(f"Failed to send bot_blocked event: {e}")
        
        logger.exception(f"Unhandled exception in bot: {event}")
    except Exception as e:
        logger.error(f"Error in errors_handler: {e}")
    return True
async def check_active_users_blocked_status():
    """修复用户封禁状态检查"""
    while True:
        try:
            # 使用增量检查，避免一次性加载所有用户
            cursor = '0'
            while True:
                try:
                    cursor, keys = await store.r.scan(
                        cursor=cursor, 
                        match="user:*", 
                        count=50
                    )
                    for key in keys:
                            # 解析用户ID
                            if isinstance(key, bytes):
                                key_str = key.decode('utf-8')
                            else:
                                key_str = str(key)
                            
                            parts = key_str.split(':')
                            if len(parts) >= 2 and parts[0] == "user":
                                try:
                                    user_id = int(parts[1])
                                    
                                    # 检查是否是真正的用户键（不是子键）
                                    if ':' not in key_str[5:]:  # "user:"之后没有冒号
                                        # 更可靠的封禁检查
                                        try:
                                            await asyncio.wait_for(
                                                bot.get_chat(user_id),
                                                timeout=3.0
                                            )
                                        except asyncio.TimeoutError:
                                            logger.warning(f"用户 {user_id} 检查超时，跳过")
                                            continue
                                        except Exception as e:
                                            error_msg = str(e).lower()
                                            blocked_phrases = [
                                                "bot was blocked", 
                                                "user is deactivated",
                                                "chat not found",
                                                "forbidden: bot was blocked",
                                                "bot was kicked"
                                            ]
                                            if any(phrase in error_msg for phrase in blocked_phrases):
                                                logger.info(f"用户 {user_id} 封禁了机器人")
                                                await send_bot_blocked_event(user_id, "periodic_check")
                                                await store.remove_watcher(user_id)
                                except ValueError:
                                    continue
                except Exception as e:
                    logger.error(f"检查用户封禁状态失败: {e}")
                
                if cursor == '0':
                    break
            
        except Exception as e:
            logger.error(f"周期性封禁状态检查失败: {e}")
        
        # 增加检查间隔到1小时
        await asyncio.sleep(3600)

async def start_background_tasks():
    asyncio.create_task(check_active_users_blocked_status(), name="blocked_status_checker")
def get_available_networks(token: str) -> list[str]:
    data = _load_crypto_wallets()
    token_u = (token or "USDT").upper()
    nets: list[str] = []
    if isinstance(data, dict):
        for tk, section in data.items():
            if tk.upper() == token_u and isinstance(section, dict):
                nets = [str(k) for k in section.keys()]
                break
        if not nets:
            if token_u == "USDT":
                nets = ["TRC20", "BEP20", "ERC20"]
            elif token_u in ("ETH", "ETHEREUM"):
                nets = ["ERC20"]
            elif token_u in ("BTC", "BITCOIN"):
                nets = ["BTC"]
    seen = set(); ordered = []
    for n in nets:
        U = n.upper()
        if U not in seen:
            seen.add(U); ordered.append(U)
    return ordered or (["TRC20", "BEP20", "ERC20"] if token_u == "USDT" else (["ERC20"] if token_u in ("ETH","ETHEREUM") else ["BTC"]))
def _excepthook(exc_type, exc, tb):
    logger.error("Uncaught exception", exc_info=(exc_type, exc, tb))
sys.excepthook = _excepthook
def spawn(coro, *, name: str = "task"):
    async def _runner():
        try:
            await coro
        except asyncio.CancelledError:
            logger.info("Task %s cancelled", name)
            raise
        except Exception:
            logger.exception("Unhandled exception in task %s", name)
    try:
        return asyncio.create_task(_runner(), name=name)
    except TypeError:
        return asyncio.create_task(_runner())
async def get_support_button(user_id: int) -> InlineKeyboardMarkup:
    support_bot_username, support_bot_url = await get_support_bot_info(user_id)
    user_language = await get_user_language(user_id)
    if user_language == "en":
        text = "📞 Support"
        start_param = "GPT5CRYPTO_en"
    else:
        text = "📞 Поддержка" 
        start_param = "GPT5CRYPTO_ru"
    url = f"{support_bot_url}?start={start_param}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=text, url=url)]
    ])
class RKeys:
    @staticmethod
    def user_ref_code(uid: int) -> str:
        return f"user:{uid}:ref_code"
    @staticmethod
    def ref_code_owner(ref_code: str) -> str:
        return f"ref_code:{ref_code}"
    @staticmethod
    def ref_stats(uid: int) -> str:
        return f"user:{uid}:ref_stats"
    @staticmethod
    def ref_earnings(uid: int) -> str:
        return f"user:{uid}:ref_earnings"
    @staticmethod
    def ref_users(uid: int) -> str:
        return f"user:{uid}:ref_users"
    @staticmethod
    def wallet_ready(uid: int, token: str, network: str) -> str:
        token_u = (token or "USDT").upper()
        net_u = (network or "").upper()
        return f"user:{uid}:wallet_ready:{token_u}:{net_u}"
    @staticmethod
    def signal_msg(chat_id: int, msg_id: int) -> str:
        return f"signal:msg:{chat_id}:{msg_id}"
    @staticmethod
    def signal_fp(hash_hex: str) -> str:
        return f"signal:fp:{hash_hex}"
    @staticmethod
    def user(uid: int) -> str:
        return f"user:{uid}"
    @staticmethod
    def positions_of(uid: int) -> str:
        return f"user:{uid}:positions"  
    @staticmethod
    def position(pid: str) -> str:
        return f"position:{pid}"
    @staticmethod
    def history(uid: int) -> str:
        return f"history:{uid}"  
    @staticmethod
    def watchers() -> str:
        return "watchers"
    @staticmethod
    def last10(uid: int) -> str:
        return f"user:{uid}:last10"  
    @staticmethod
    def last_signal(uid: int) -> str:
        return f"user:{uid}:last_signal"  
    @staticmethod
    def last_signal_data(uid: int) -> str:
        return f"user:{uid}:last_signal_data"  
    @staticmethod
    def assets_msg(uid: int) -> str:
        return f"user:{uid}:assets_msg"  
    @staticmethod
    def dep_amount(uid: int) -> str:
        return f"user:{uid}:dep_amount"
    @staticmethod
    def dep_token(uid: int) -> str:
        return f"user:{uid}:dep_token"
    @staticmethod
    def wd_token(uid: int) -> str:
        return f"user:{uid}:wd_token"
    @staticmethod
    def wd_network(uid: int) -> str:
        return f"user:{uid}:wd_network"
    @staticmethod
    def wd_address(uid: int) -> str:
        return f"user:{uid}:wd_address"
    @staticmethod
    def wd_pending(uid: int) -> str:
        return f"user:{uid}:wd_pending"
    @staticmethod
    def wd_pending_list(uid: int) -> str:
        return f"user:{uid}:wd_pending_list"
SUPPORT_CHAT_ID_KEY = "support:chat_id"
async def get_support_bot_info(user_id: int) -> tuple[str, str]:
    user_language = await get_user_language(user_id)
    if user_language == "en":
        return SUPPORT_BOT_USERNAME_EN, SUPPORT_BOT_URL_EN
    else:
        return SUPPORT_BOT_USERNAME_RU, SUPPORT_BOT_URL_RU
async def get_support_chat_id() -> int:
    try:
        raw = await r.get(SUPPORT_CHAT_ID_KEY)
        if not raw:
            return 0
        return int(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
    except Exception:
        return 0
async def set_support_chat_id(chat_id: int):
    try:
        await r.set(SUPPORT_CHAT_ID_KEY, str(chat_id))
    except Exception:
        pass
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN_RU")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    MIN_SEND_INTERVAL = float(os.getenv("MIN_SEND_INTERVAL_CHAT", "1.0"))
class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
class PosStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED_TP = "CLOSED_TP"
    CLOSED_SL = "CLOSED_SL"
    CLOSED_TIME = "CLOSED_TIME"
class UserStats(BaseModel):
    wins: int = 0
    losses: int = 0
    last10_outcomes: List[str] = Field(default_factory=list) 
    total_trades: int = 0
    total_deposits: float = 0.0
    total_withdrawals: float = 0.0
    total_pnl: float = 0.0
    registration_date: float = Field(default_factory=time.time)
    ref_users_count: int = 0
    ref_earnings: float = 0.0
    ref_deposits_total: float = 0.0
class Position(BaseModel):
    id: str
    user_id: int
    symbol: str
    side: Side
    entry_price: float
    tp: float
    sl: float
    leverage: int
    order_amount: float
    margin_used: float
    opened_at: float 
    max_duration_sec: int
    status: PosStatus = PosStatus.OPEN
    pnl_current: float = 0.0
    price_now: float = 0.0
    last_tick_at: float = 0.0
    pnl_history: List[Tuple[float, float]] = Field(default_factory=list)   
class TradeHistory(BaseModel):
    position_id: str
    symbol: str
    side: Side
    entry: float
    exit: float
    pnl_abs: float
    pnl_pct: float
    closed_by: Literal["TP", "SL", "TIME"]
    closed_at: float
class Store:
    def __init__(self, r: redis.Redis = None):
        if r is None:
            # Используйте глобальный экземпляр
            self.r = r  # Используйте глобальный r
        else:
            self.r = r
    async def get_bot_owner(self, user_id: int) -> Optional[int]:
        try:
            raw = await self.r.get(f"user:{user_id}:bot_owner")
            if raw:
                if isinstance(raw, bytes):
                    owner_str = raw.decode('utf-8')
                else:
                    owner_str = str(raw)
                return int(owner_str)
            return None
        except Exception as e:
            logger.error(f"Error getting bot owner for user {user_id}: {e}")
            return None
    async def set_bot_owner(self, user_id: int, owner_id: int):
        await self.r.set(f"user:{user_id}:bot_owner", str(owner_id))
    async def get_tenant_users(self, owner_id: int) -> List[int]:
        try:
            pattern = f"user:*:bot_owner"
            keys = await self.r.keys(pattern)
            users = []
            for key in keys:
                try:
                    if isinstance(key, bytes):
                        key_str = key.decode('utf-8')
                    else:
                        key_str = str(key)
                    raw_owner = await self.r.get(key)
                    if raw_owner:
                        if isinstance(raw_owner, bytes):
                            owner_str = raw_owner.decode('utf-8')
                        else:
                            owner_str = str(raw_owner)
                        if int(owner_str) == owner_id:
                            parts = key_str.split(":")
                            if len(parts) >= 2:
                                user_id = int(parts[1])
                                users.append(user_id)
                except Exception as e:
                    logger.error(f"Error processing key {key}: {e}")
                    continue
            return users
        except Exception as e:
            logger.error(f"Error getting tenant users for owner {owner_id}: {e}")
            return []
    async def get_tenant_signal_channel(self, owner_id: int) -> Optional[int]:
        raw = await self.r.get(f"tenant:{owner_id}:signal_channel")
        return int(raw.decode()) if raw else None
    async def set_tenant_signal_channel(self, owner_id: int, channel_id: int):
        await self.r.set(f"tenant:{owner_id}:signal_channel", str(channel_id))
    async def set_wallet_ready(self, uid: int, token: str, network: str) -> None:
        try:
            await self.r.set(RKeys.wallet_ready(uid, token, network), b"1")
        except Exception:
            pass
    async def is_wallet_ready(self, uid: int, token: str, network: str) -> bool:
        try:
            return bool(await self.r.exists(RKeys.wallet_ready(uid, token, network)))
        except Exception:
            return False
    async def mark_signal_message(self, chat_id: int, msg_id: int, ttl_sec: int = 7*24*3600) -> bool:
        try:
            k = RKeys.signal_msg(int(chat_id), int(msg_id))
            ok = await self.r.set(k, b"1", ex=ttl_sec, nx=True)
            return bool(ok)
        except Exception:
            return True
    async def mark_signal_fingerprint(self, fp_hex: str, ttl_sec: int = 12*3600) -> bool:
        try:
            k = RKeys.signal_fp(fp_hex)
            ok = await self.r.set(k, b"1", ex=ttl_sec, nx=True)
            return bool(ok)
        except Exception:
            return True
    async def get_user(self, uid: int) -> User:
        raw = await self.r.get(RKeys.user(uid))
        if raw:
            data = json.loads(raw)
            return User(**data)
        u = User(user_id=uid) 
        await self.save_user(u)
        return u
    async def save_user(self, user: User) -> None:
        await self.r.set(RKeys.user(user.user_id), user.model_dump_json())
    async def add_position(self, p: Position) -> None:
        pipe = self.r.pipeline()
        pipe.sadd(RKeys.positions_of(p.user_id), p.id)
        pipe.set(RKeys.position(p.id), p.model_dump_json())
        await pipe.execute()
    async def get_position(self, pid: str) -> Optional[Position]:
        raw = await self.r.get(RKeys.position(pid))
        return Position(**json.loads(raw)) if raw else None
    async def update_position(self, p: Position) -> None:
        await self.r.set(RKeys.position(p.id), p.model_dump_json())
    async def remove_position(self, uid: int, pid: str) -> None:
        pipe = self.r.pipeline()
        pipe.srem(RKeys.positions_of(uid), pid)
        pipe.delete(RKeys.position(pid))
        await pipe.execute()
    async def list_positions(self, uid: int) -> List[Position]:
        ids = await self.r.smembers(RKeys.positions_of(uid))
        res = []
        for b in ids:
            pid = b.decode()
            raw = await self.r.get(RKeys.position(pid))
            if raw:
                res.append(Position(**json.loads(raw)))
        res.sort(key=lambda x: x.opened_at, reverse=True)
        return res
    async def add_history(self, uid: int, h: TradeHistory) -> None:
        await self.r.lpush(RKeys.history(uid), h.model_dump_json())
    async def get_history_page(self, uid: int, page: int = 0, page_size: int = 10) -> Tuple[List[TradeHistory], int]:
        start = page * page_size
        end = start + page_size - 1
        raw = await self.r.lrange(RKeys.history(uid), start, end)
        total = await self.r.llen(RKeys.history(uid))
        items = [TradeHistory(**json.loads(x)) for x in raw]
        return items, total
    async def add_watcher(self, uid: int) -> None:
        await self.r.sadd(RKeys.watchers(), uid)
    async def remove_watcher(self, uid: int) -> None:
        await self.r.srem(RKeys.watchers(), uid)
    async def list_active_watchers(self) -> List[int]:
        try:
            raw = await self.r.smembers(RKeys.watchers())
            watchers = []
            for raw_user in raw:
                try:
                    if isinstance(raw_user, bytes):
                        user_id = int(raw_user.decode('utf-8'))
                    else:
                        user_id = int(raw_user)
                    watchers.append(user_id)
                except Exception as e:
                    logger.error(f"Error decoding watcher ID: {raw_user}, error: {e}")
                    continue
            
            logger.info(f"📊 Active watchers count: {len(watchers)}")
            return watchers
        except Exception as e:
            logger.error(f"Error getting active watchers: {e}")
            return []
    async def push_outcome(self, uid: int, w_or_l: str) -> None:
        pipe = self.r.pipeline()
        pipe.lpush(RKeys.last10(uid), w_or_l)
        pipe.ltrim(RKeys.last10(uid), 0, 9)
        await pipe.execute()
    async def get_last10(self, uid: int) -> List[str]:
        raw = await self.r.lrange(RKeys.last10(uid), 0, 9)
        return [x.decode() for x in raw]
    async def set_last_signal_msg(self, uid: int, chat_id: int, msg_id: int):
        await self.r.set(RKeys.last_signal(uid), json.dumps({"chat_id": chat_id, "msg_id": msg_id}))
    async def get_last_signal_msg(self, uid: int) -> Optional[Tuple[int, int]]:
        raw = await self.r.get(RKeys.last_signal(uid))
        if not raw:
            return None
        data = json.loads(raw)
        return data.get("chat_id"), data.get("msg_id")
    async def clear_last_signal_msg(self, uid: int):
        await self.r.delete(RKeys.last_signal(uid))
    async def set_assets_msg(self, uid: int, msg_id: int):
        await self.r.set(RKeys.assets_msg(uid), json.dumps({"msg_id": msg_id}))
    async def get_assets_msg(self, uid: int) -> Optional[int]:
        raw = await self.r.get(RKeys.assets_msg(uid))
        if not raw:
            return None
        return json.loads(raw).get("msg_id")
    async def clear_assets_msg(self, uid: int):
        await self.r.delete(RKeys.assets_msg(uid))
    async def set_dep_amount(self, uid: int, amount: int) -> None:
        await self.r.set(RKeys.dep_amount(uid), str(amount))
    async def get_dep_amount(self, uid: int) -> int | None:
        raw = await self.r.get(RKeys.dep_amount(uid))
        if not raw:
            return None
        try:
            return int(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
        except Exception:
            return None
    async def clear_dep_amount(self, uid: int) -> None:
        await self.r.delete(RKeys.dep_amount(uid))
    async def set_dep_token(self, uid: int, token: str) -> None:
        await self.r.set(RKeys.dep_token(uid), (token or "USDT"))
    async def get_dep_token(self, uid: int) -> str | None:
        raw = await self.r.get(RKeys.dep_token(uid))
        if not raw:
            return None
        try:
            return raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            return None
    async def clear_dep_token(self, uid: int) -> None:
        await self.r.delete(RKeys.dep_token(uid))
    async def push_support_event(self, payload: dict) -> None:
        data = json.dumps(payload)
        await self.r.lpush(SUPPORT_QUEUE_KEY, data.encode())
    async def set_last_signal_data(self, uid: int, ps: ParsedSignal) -> None:
        data = {
            "symbol": ps.symbol,
            "tf": ps.tf,
            "side": ps.side.value if hasattr(ps.side, "value") else str(ps.side),
            "entry": ps.entry,
            "sl": ps.sl,
            "tp": ps.tp,
            "rec_amount": ps.rec_amount,
            "date_utc": ps.date_utc,
            "strength": getattr(ps, "strength", None),
        }
        await self.r.set(RKeys.last_signal_data(uid), json.dumps(data))
    async def get_last_signal_data(self, uid: int) -> Optional[ParsedSignal]:
        raw = await self.r.get(RKeys.last_signal_data(uid))
        if not raw:
            return None
        try:
            data = json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
            side = Side(data.get("side", "LONG"))
            return ParsedSignal(
                symbol=data["symbol"],
                tf=data["tf"],
                side=side,
                entry=float(data["entry"]),
                sl=float(data["sl"]),
                tp=float(data["tp"]),
                rec_amount=int(data["rec_amount"]),
                date_utc=data["date_utc"],
                strength=(data.get("strength") or None),
            )
        except Exception:
            return None
    async def set_wd_token(self, uid: int, token: str) -> None:
        await self.r.set(RKeys.wd_token(uid), (token or "USDT"))
    async def get_wd_token(self, uid: int):
        raw = await self.r.get(RKeys.wd_token(uid))
        if not raw:
            return None
        return raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
    async def set_wd_network(self, uid: int, net: str) -> None:
        await self.r.set(RKeys.wd_network(uid), net)
    async def get_wd_network(self, uid: int):
        raw = await self.r.get(RKeys.wd_network(uid))
        if not raw:
            return None
        return raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
    async def set_wd_address(self, uid: int, addr: str) -> None:
        await self.r.set(RKeys.wd_address(uid), addr)
    async def get_wd_address(self, uid: int):
        raw = await self.r.get(RKeys.wd_address(uid))
        if not raw:
            return None
        return raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
    async def clear_withdraw_flow(self, uid: int) -> None:
        pipe = self.r.pipeline()
        pipe.delete(RKeys.wd_token(uid))
        pipe.delete(RKeys.wd_network(uid))
        pipe.delete(RKeys.wd_address(uid))
        await pipe.execute()
    async def set_wd_pending(self, uid: int, data: dict) -> None:
        try:
            await self.r.set(RKeys.wd_pending(uid), json.dumps(data).encode())
        except Exception:
            pass
    async def get_wd_pending(self, uid: int) -> dict | None:
        raw = await self.r.get(RKeys.wd_pending(uid))
        if not raw:
            return None
        try:
            return json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
        except Exception:
            return None
    async def clear_wd_pending(self, uid: int) -> None:
        try:
            await self.r.delete(RKeys.wd_pending(uid))
        except Exception:
            pass
    async def add_pending_item(self, uid: int, payload: dict) -> None:
        import json as _json
        await self.r.lpush(RKeys.wd_pending_list(uid), _json.dumps(payload, ensure_ascii=False))
    async def list_pending_items(self, uid: int) -> list[dict]:
        import json as _json
        try:
            rows = await self.r.lrange(RKeys.wd_pending_list(uid), 0, 50)
        except Exception:
            rows = []
        out = []
        for x in rows:
            try:
                out.append(_json.loads(x.decode() if isinstance(x, (bytes, bytearray)) else x))
            except Exception:
                pass
        return out
    async def update_user_activity(self, uid: int):
        try:
            user = await self.get_user(uid)
            user.last_activity = time.time()
            await self.save_user(user)
        except Exception:
            pass
    async def increment_user_trades(self, uid: int, pnl: float):
        try:
            user = await self.get_user(uid)
            user.stats.total_trades += 1
            user.stats.total_pnl += pnl
            await self.save_user(user)
        except Exception:
            pass
    async def increment_deposits(self, uid: int, amount: float, payment_id: str = None) -> bool:
        try:
            if payment_id:
                existing_key = f"payment_processed:{payment_id}"
                if await self.r.exists(existing_key):
                    logger.warning(f"⚠️ Попытка повторного зачисления платежа {payment_id} для пользователя {uid}")
                    return False
                await self.r.setex(existing_key, 3600, "1")
            user = await self.get_user(uid)
            old_balance = user.balance
            user.balance += amount
            user.stats.total_deposits += amount
            await self.save_user(user)
            logger.info(f"✅ Баланс пользователя {uid} увеличен на ${amount:.2f}, старый: ${old_balance:.2f}, новый: ${user.balance:.2f}")
            balance_event = {
                "type": "balance_update",
                "user_id": uid,
                "amount": amount,
                "new_balance": user.balance,
                "old_balance": old_balance,
                "reason": "deposit",
                "timestamp": time.time()
            }
            await self.r.lpush("trading:balance_updates", json.dumps(balance_event))
            await self.r.lpush("balance_updates", json.dumps(balance_event))
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при увеличении баланса пользователя {uid}: {e}")
            return False
    async def increment_withdrawals(self, uid: int, amount: float):
        try:
            user = await self.get_user(uid)
            user.stats.total_withdrawals += amount
            await self.save_user(user)
        except Exception:
            pass
    async def update_user_profile(self, uid: int, username: str = "", first_name: str = "", last_name: str = "", language_code: str = ""):
        try:
            user = await self.get_user(uid)
            if username:
                user.username = username
            if first_name:
                user.first_name = first_name
            if last_name:
                user.last_name = last_name
            if language_code:
                user.language_code = language_code
            await self.save_user(user)
        except Exception:
            pass
    async def get_all_users(self) -> List[User]:
        users: List[User] = []
        try:
            keys = []
            async for key in self.r.scan_iter(match="user:*", count=1000):
                key_str = key.decode() if isinstance(key, bytes) else str(key)
                if (
                    ":positions" in key_str
                    or ":history" in key_str
                    or ":last10" in key_str
                    or ":assets_msg" in key_str
                    or ":ref_" in key_str
                    or ":wd_" in key_str
                ):
                    continue
                keys.append(key)
            if not keys:
                return users
            raws = await self.r.mget(keys)
            for raw in raws:
                if not raw:
                    continue
                try:
                    data = json.loads(
                        raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
                    )
                    users.append(User(**data))
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"get_all_users failed: {e}")
        return users
    async def get_user_full_info(self, uid: int) -> Dict[str, Any]:
        user = await self.get_user(uid)
        positions = await self.list_positions(uid)
        history, total_history = await self.get_history_page(uid, page=0, page_size=1000)
        pending_withdrawals = await self.list_pending_items(uid)
        last10 = await self.get_last10(uid)
        return {
            "user": user.model_dump(),
            "open_positions": len(positions),
            "total_history_trades": total_history,
            "pending_withdrawals": len([p for p in pending_withdrawals if p.get("status") == "PENDING"]),
            "recent_performance": last10,
            "current_balance": user.balance,
            "unrealized_pnl": await calc_unrealized(self, uid)
        }
    async def generate_ref_code(self, uid: int) -> str:
        import hashlib
        base_code = f"{uid}{time.time()}"
        ref_code = hashlib.md5(base_code.encode()).hexdigest()[:8].upper()
        existing = await self.r.get(RKeys.ref_code_owner(ref_code))
        if not existing:
            await self.r.set(RKeys.ref_code_owner(ref_code), str(uid))
            await self.r.set(RKeys.user_ref_code(uid), ref_code)
            return ref_code
        return await self.generate_ref_code(uid)
    async def get_ref_code(self, uid: int) -> str:
        ref_code = await self.r.get(RKeys.user_ref_code(uid))
        if ref_code:
            return ref_code.decode()
        return await self.generate_ref_code(uid)
    async def get_user_by_ref_code(self, ref_code: str) -> Optional[int]:
        raw = await self.r.get(RKeys.ref_code_owner(ref_code))
        return int(raw.decode()) if raw else None
    async def add_referral(self, referrer_id: int, referral_id: int):
        await self.r.sadd(RKeys.ref_users(referrer_id), referral_id)
        user = await self.get_user(referrer_id)
        user.stats.ref_users_count += 1
        await self.save_user(user)
    async def get_ref_stats(self, uid: int) -> Dict[str, Any]:
        user = await self.get_user(uid)
        ref_users = await self.r.smembers(RKeys.ref_users(uid))
        return {
            "ref_users_count": user.stats.ref_users_count,
            "ref_earnings": user.stats.ref_earnings,
            "ref_deposits_total": user.stats.ref_deposits_total,
            "ref_users_list": [int(uid) for uid in ref_users] if ref_users else []
        }
    async def get_user_min_deposit(self, uid: int) -> int:
        try:
            personal_key = f"user:{uid}:min_deposit"
            raw = await self.r.get(personal_key)
            if raw:
                try:
                    if isinstance(raw, bytes):
                        return int(raw.decode('utf-8'))
                    return int(raw)
                except:
                    pass
            global_key = "config:min_deposit_global"
            raw = await self.r.get(global_key)
            if raw:
                try:
                    if isinstance(raw, bytes):
                        return int(raw.decode('utf-8'))
                    return int(raw)
                except:
                    pass
        except Exception as e:
            logger.error(f"Error getting min deposit for user {uid}: {e}")
        return 0  
    async def set_user_min_deposit(self, uid: int, amount: int):
        try:
            await self.r.set(f"user:{uid}:min_deposit", str(amount))
        except Exception as e:
            logger.error(f"Error setting min deposit for user {uid}: {e}")
    async def reset_user_min_deposit(self, uid: int):
        try:
            await self.r.delete(f"user:{uid}:min_deposit")
        except Exception as e:
            logger.error(f"Error resetting min deposit for user {uid}: {e}")
    async def get_user_bot_token(self, owner_id: int) -> Optional[str]:
        try:
            raw = await self.r.get(f"tenant:{owner_id}:bot_token")
            if raw:
                if isinstance(raw, bytes):
                    return raw.decode('utf-8')
                return str(raw)
            return TRADE_BOT_TOKEN
        except Exception as e:
            logger.error(f"Error getting bot token for owner {owner_id}: {e}")
            return TRADE_BOT_TOKEN
    async def set_user_bot_token(self, owner_id: int, token: str):
        await self.r.set(f"tenant:{owner_id}:bot_token", token)
    async def process_ref_deposit(self, referral_id: int, amount: float):
        referral = await self.get_user(referral_id)
        if not referral.referred_by:
            return
        referrer_id = referral.referred_by
        referrer = await self.get_user(referrer_id)
        ref_bonus = amount * 0.10
        referrer.balance += ref_bonus
        referrer.stats.ref_earnings += ref_bonus
        referrer.stats.ref_deposits_total += amount
        await self.save_user(referrer)
        try:
            owner = await self.get_bot_owner(referrer_id)
            token = await self.get_user_bot_token(owner)
            trb = Bot(token=token)
            await trb.send_message(
                chat_id=referrer_id,
                text=f"🎉 <b>Реферальный бонус!</b>\n\n"
                    f"Ваш реферал пополнил счёт на ${amount:.2f}\n"
                    f"Вам начислен бонус: ${ref_bonus:.2f}\n"
                    f"Новый баланс: ${referrer.balance:.2f}"
            )
        except Exception:
            pass
class User(BaseModel):
    user_id: int
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    language_code: str = ""
    balance: float = 0.0
    order_amount: float = 10.0
    leverage: int = 1
    loss_ratio_target: float = 0.3
    stats: UserStats = Field(default_factory=UserStats)
    last_activity: float = Field(default_factory=time.time)
    is_verified: bool = False
    trading_enabled: bool = True
    ref_code: str = ""
    referred_by: int = 0  
    registration_source: str = ""
    class Config:
        arbitrary_types_allowed = True
async def get_texts(uid: int) -> dict:
    user = await store.get_user(uid)
    is_english = user.language_code == "en"
    if is_english:
        return ENGLISH_TEXTS
    else:
        return RUSSIAN_TEXTS
RUSSIAN_TEXTS = {
    "main_menu": "Выберите действие…",
    "assets": "Активы",
    "open_positions": "Открытые сделки", 
    "trade_history": "История сделок",
    "ai_trading": "AI Трейдинг",
    "settings": "Настройки",
    "welcome": "👋 <b>Добро пожаловать в Автотрейдинг</b>\n\nПожалуйста, выберите язык:",
    "balance_welcome": "✅ <b>Язык установлен: Русский</b>\n\nВаш баланс: ${balance:.2f}\nПо умолчанию: плечо x1, сумма ордера $10.\n\nВыберите действие ниже ⤵️",
    "assets_balance": "💰 Баланс",
    "open_positions_count": "Открытые позиции",
    "unrealized_pnl": "Нереализованный PnL",
    "pending_withdrawals": "📤 Ожидают вывода",
    "settings_details": "Сумма ордера: ${amount:.2f}\nПлечо: x{leverage}\nЗадействованная маржа на ордер: ${margin:.2f}",
    "deposit_amount_display": "💵 Сумма к оплате: <b>{amount_rub:.0f} RUB (${amount_usd})</b>\n📊 Курс: 1 USD = {rate:.2f} RUB",
    "deposit_title": "💳 Пополнение счёта",
    "deposit_choose_method": "Выберите способ пополнения:",
    "deposit_bank_card": "Банковская карта",
    "deposit_crypto": "Криптовалюта",
    "deposit_choose_amount": "Выберите сумму пополнения:",
    "deposit_enter_fio": "👤 <b>Введите ваше ФИО</b>",
    "deposit_fio_example": "Пример: Иванов Иван Иванович",
    "deposit_choose_bank": "🏦 <b>Выберите ваш банк</b>",
    "deposit_choose_country": "🌍 <b>Выберите страну</b>",
    "deposit_request_sent": "✅ <b>Запрос на пополнение отправлен!</b>",
    "deposit_wait_requisites": "⏳ Ожидайте реквизиты для оплаты.",
    "fio_accepted": "✅ ФИО принято: <b>{fio}</b>",
    "crypto_choose_token": "Выберите токен для пополнения:",
    "crypto_choose_network": "Выберите сеть:",
    "crypto_generating_wallet": "⏳ Подождите, генерируется ваш адрес кошелька для пополнения…",
    "crypto_deposit_instructions": "💳 <b>Пополните на {amount}</b>",
    "withdraw_title": "💰 <b>Вывод средств</b>",
    "withdraw_available": "Доступно для вывода: ${balance:.2f}",
    "withdraw_choose_method": "Выберите способ вывода",
    "withdraw_crypto": "Криптовалюта", 
    "withdraw_bank_card": "Банковская карта",
    "withdraw_cancelled" : "Вывод отменён",
    "withdraw_card_title": "💳 <b>Вывод на банковскую карту</b>",
    "withdraw_card_enter_fio": "👤 Введите ваше полное ФИО",
    "withdraw_card_choose_bank": "🏦 Выберите банк получателя",
    "withdraw_card_enter_card": "💳 Введите номер банковской карты",
    "withdraw_card_confirm": "✅ <b>Данные для вывода получены</b>",
    "payment_approved": "Платёж подтверждён ${amount:.2f}",
    "payment_rejected": "❌ Платёж отклонён",
    "new_balance": "Новый баланс: ${balance:.2f}",
    "withdraw_approved": "Ваш вывод подтвержден!",
    "withdraw_rejected": "Вывод отклонен",
    "contact_support": "Обратитесь в поддержку для уточнения деталей.",
    "referral_bonus_received": "Реферальный бонус!",
    "trade_pnl_update": "Обновление баланса после сделки",
    "withdraw_crypto_title": "💰 <b>Вывод криптовалютой</b>",
    "withdraw_choose_token": "Выберите токен для вывода",
    "withdraw_choose_network": "Выберите сеть",
    "withdraw_enter_wallet": "📝 Укажите адрес кошелька",
    "withdraw_enter_amount": "Укажите сумму вывода",
    "withdraw_all_balance": "Весь баланс",
    "withdraw_request_sent": "<b>Запрос на вывод принят!</b>",
    "ai_trading_enabled": "🛰 <b>AI Трейдинг</b> включен",
    "ai_trading_searching": "Ищу новый сигнал...",
    "ai_trading_stop": "⏹ Остановить AI Трейдинг",
    "ai_trading_stopped": "⏹ Режим AI Трейдинг остановлен",
    "new_signal": "🛰 <b>Новый сигнал</b>",
    "symbol": "Пара",
    "timeframe": "Таймфрейм", 
    "direction": "Направление",
    "entry_price": "Цена входа",
    "take_profit": "TP",
    "stop_loss": "SL",
    "recommended_amount": "Реком. сумма",
    "date": "Дата",
    "time_left": "⏳ Осталось",
    "open_order": "🔄 Открываем ордер…",
    "order_opened": "✅ Ордер открыт",
    "position_closed_tp": "✅ Сделка исполнена по Тейк-профиту",
    "position_closed_sl": "❌ Сделка исполнена по Стоп-лоссу", 
    "position_closed_time": "⏱️ Сделка завершена по времени",
    "settings_title": "⚙️ Настройки",
    "settings_choose_amount": "Шаг 1: выберите <b>сумму ордера</b>",
    "settings_choose_leverage": "Шаг 2: выберите <b>плечо</b>",
    "settings_updated": "⚙️ Настройки обновлены:",
    "order_amount": "Сумма ордера",
    "leverage": "Плечо",
    "margin_used": "Задействованная маржа на ордер",
    "no_open_positions" : "Нет открытых позиций",   
    "history_title": "📜 История",
    "history_empty": "История пуста.",
    "show_more": "Показать ещё",
    "verification_title": "Верификация",
    "verification_text": "Для прохождения процедуры верификации Клиент обязан обеспечить наличие не менее 20 (двадцати) закрытых ордеров на счёте.",
    "requisites_title": "💳 Ваши кошельки для пополнения",
    "requisites_not_configured": "⚙️ <b>Реквизиты не настроены</b>",
    "cancel": "Отмена",
    "confirm": "Подтвердить",
    "back": "Назад",
    "continue": "Продолжить",
    "insufficient_funds": "❌ Недостаточно средств",
    "error": "❌ Ошибка",
    "success": "Успешно",
    "pending": "⏳ Ожидание",
    "settings_cannot_change": "❌ <b>Невозможно изменить настройки</b>\n\nУ вас есть открытые сделки. Дождитесь их закрытия для изменения настроек.",
    "settings_title": "⚙️ <b>Настройки</b>\n\nВыберите параметр для изменения:",
    "settings_choose_amount": "📊 <b>Выберите сумму ордера</b>",
    "settings_choose_leverage": "⚡ <b>Выберите плечо</b>",
    "order_amount": "Сумма ордера",
    "leverage": "Плечо",
    "open_order": "🔄 Открываем ордер…",
    "order_opened": "✅ Ордер открыт",
    "insufficient_funds": "Недостаточно средств", 
    "deposit": "Пополнить",
    "entry_price": "Вход",
    "current_price": "Текущая",
    "position_pnl": "PNL",
    "invalid_fio": "❌ Некорректное ФИО. Попробуйте ещё раз.",
    "deposit_fio_accepted": "✅<b>{fio}</b>",
    "order_amount_changed": "Сумма ордера изменена",
    "leverage_changed": "Плечо изменено",
    "balance_welcome": "Ваш баланс: ${balance:.2f}\nПлечо: x{leverage}, сумма ордера ${order_amount:.2f}.\n\nВыберите действие ниже ⤵️",
    "withdraw_available": "Доступно для вывода: ${balance:.2f}",
    "withdraw_wallet_accepted": "Адрес кошелька подтверждён",
}
ENGLISH_TEXTS = {
    "order_amount_changed": "Order amount changed",
    "leverage_changed": "Leverage changed",
    "main_menu": "Choose action…",
    "assets": "Assets",
    "open_positions": "Open Positions", 
    "trade_history": "Trade History",
    "ai_trading": "AI Trading",
    "settings": "Settings",
    "welcome": "👋 <b>Welcome to Autotrading</b>\n\nPlease choose your language:",
    "balance_welcome": "✅ <b>Language set: English</b>\n\nYour balance: ${balance:.2f}\nDefault: leverage x1, order amount $10.\n\nChoose action below ⤵️",
    "assets_balance": "💰 Balance",
    "open_positions_count": "Open positions",
    "unrealized_pnl": "Unrealized PnL",
    "pending_withdrawals": "📤 Pending withdrawals",
    "no_open_positions" : "No open positions",
    "settings_details": "Order amount: ${amount:.2f}\nLeverage: x{leverage}\nMargin used per order: ${margin:.2f}",     
    "deposit_amount_display": "💵 Amount to pay: <b>{amount_rub:.0f} RUB (${amount_usd})</b>\n📊 Rate: 1 USD = {rate:.2f} RUB",
    "deposit_title": "💳 Top up account",
    "deposit_choose_method": "Choose deposit method:",
    "deposit_bank_card": "Bank card",
    "deposit_crypto": "Cryptocurrency",
    "deposit_choose_amount": "Choose deposit amount:",
    "deposit_enter_fio": "👤 <b>Enter your full name</b>",
    "deposit_fio_example": "Example: John Smith",
    "deposit_choose_bank": "🏦 <b>Choose your bank</b>",
    "deposit_choose_country": "🌍 <b>Choose country</b>",
    "deposit_request_sent": "✅ <b>Deposit request sent!</b>",
    "deposit_wait_requisites": "⏳ Wait for payment details.",
    "fio_accepted": "✅ Full name accepted: <b>{fio}</b>",
    "payment_approved": "Payment credited: ${amount:.2f}",
    "payment_rejected": "❌ Payment rejected",
    "new_balance": "New balance: ${balance:.2f}",
    "withdraw_approved": "Your withdrawal has been confirmed!",
    "withdraw_rejected": "Withdrawal rejected",
    "contact_support": "Please contact support for details.",
    "referral_bonus_received": "Referral bonus!",
    "trade_pnl_update": "Balance update after trade",
    "crypto_choose_token": "Choose token for deposit:",
    "crypto_choose_network": "Choose network:",
    "crypto_generating_wallet": "⏳ Generating your wallet address for deposit…",
    "crypto_deposit_instructions": "💳 <b>Deposit {amount}</b>",
    "withdraw_title": "💰 <b>Withdraw funds</b>",
    "withdraw_available": "Available for withdrawal: ${balance:.2f}",
    "withdraw_choose_method": "Choose withdrawal method:",
    "withdraw_crypto": "Cryptocurrency", 
    "withdraw_bank_card": "Bank card",
    "withdraw_cancelled" : "Withdraw cancelled",
    "withdraw_card_title": "💳 <b>Withdrawal to bank card</b>",
    "withdraw_card_enter_fio": "👤 Enter your full name",
    "withdraw_card_choose_bank": "🏦 Choose recipient's bank:",
    "withdraw_card_enter_card": "💳 Enter bank card number",
    "withdraw_card_confirm": "✅ <b>Withdrawal data received</b>",
    "withdraw_crypto_title": "<b>Cryptocurrency withdrawal</b>",
    "withdraw_choose_token": "Choose token for withdrawal",
    "withdraw_choose_network": "Choose network",
    "withdraw_enter_wallet": "📝 Enter wallet address",
    "withdraw_enter_amount": "Enter withdrawal amount",
    "withdraw_all_balance": "Entire balance",
    "withdraw_request_sent": "<b>Withdrawal request accepted!</b>",
    "withdraw_wallet_accepted": "Wallet address accepted",
    "withdraw_processing": "⏳ Processing withdrawal request...",
    "payment_rejected": "❌ Payment rejected",
    "new_balance": "New balance: ${balance:.2f}",   
    "ai_trading_enabled": "🛰 <b>AI Trading</b> enabled",
    "ai_trading_searching": "Looking for new signal...",
    "ai_trading_stop": "⏹ Stop AI Trading",
    "ai_trading_stopped": "⏹ AI Trading mode stopped",
    "new_signal": "🛰 <b>New signal</b>",
    "symbol": "Symbol",
    "timeframe": "Timeframe", 
    "direction": "Direction",
    "entry_price": "Entry price",
    "take_profit": "TP",
    "stop_loss": "SL",
    "recommended_amount": "Recommended amount",
    "date": "Date",
    "time_left": "⏳ Time left",
    "open_order": "🔄 Opening order…",
    "order_opened": "✅ Order opened",
    "position_closed_tp": "✅ Position closed by Take Profit",
    "position_closed_sl": "❌ Position closed by Stop Loss", 
    "position_closed_time": "⏱️ Position closed by time",
    "settings_title": "⚙️ Settings",
    "settings_choose_amount": "Step 1: choose <b>order amount</b>:",
    "settings_choose_leverage": "Step 2: choose <b>leverage</b>:",
    "settings_updated": "⚙️ Settings updated:",
    "order_amount": "Order amount",
    "leverage": "Leverage",
    "margin_used": "Margin used per order",
    "history_title": "📜 History:",
    "history_empty": "History is empty.",
    "show_more": "Show more",
    "verification_title": "Verification",
    "verification_text": "To complete the verification procedure, the Client must have at least 20 (twenty) closed orders on the account.",
    "requisites_title": "💳 Wallets for deposit",
    "requisites_not_configured": "⚙️ <b>Requisites not configured</b>",
    "settings_cannot_change": "❌ <b>Cannot change settings</b>\n\nYou have open positions. Wait for them to close to change settings.",
    "settings_title": "⚙️ <b>Settings</b>\n\nChoose parameter to change:",
    "settings_choose_amount": "📊 <b>Choose order amount</b>",
    "settings_choose_leverage": "⚡ <b>Choose leverage</b>",
    "order_amount": "Order amount", 
    "leverage": "Leverage",
    "cancel": "Cancel",
    "confirm": "Confirm",
    "back": "Back",
    "continue": "Continue",
    "insufficient_funds": "❌ Insufficient funds",
    "error": "❌ Error",
    "success": "Success",
    "pending": "⏳ Pending",
    "open_order": "🔄 Opening order…",
    "order_opened": "✅ Order opened", 
    "insufficient_funds": "Insufficient funds",
    "deposit": "Deposit",
    "entry_price": "Entry",
    "current_price": "Current", 
    "position_pnl": "PNL",
    "deposit_fio_accepted": "✅<b>{fio}</b>",
    "balance_welcome": "Your balance: ${balance:.2f}\nLeverage: x{leverage}, order amount ${order_amount:.2f}.\n\nChoose action below ⤵️",
    "withdraw_wallet_accepted": "Wallet address verified",
}
async def get_user_language(uid: int) -> str:
    user = await store.get_user(uid)
    return user.language_code or "en"  
async def is_english_user(uid: int) -> bool:
    user = await store.get_user(uid)
    return user.language_code == "en"
async def get_localized_text(uid: int, key: str, **kwargs) -> str:
    texts = await get_texts(uid)
    text = texts.get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text
def get_deposit_methods_kb(is_english: bool = False) -> InlineKeyboardMarkup:
    if is_english:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Cryptocurrency", callback_data="dep_crypto")],  
            [InlineKeyboardButton(text="🔙 Back", callback_data="open_assets")]
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Криптовалюта", callback_data="dep_crypto")],  
            [InlineKeyboardButton(text="🔙 Назад", callback_data="open_assets")]
        ])
async def get_localized_kb(uid: int, kb_type: str, **kwargs) -> InlineKeyboardMarkup:
    is_english = await is_english_user(uid)
    if kb_type == "assets":
        return await get_assets_keyboard(uid)
    elif kb_type == "deposit_methods":
        return get_deposit_methods_kb(is_english)
    elif kb_type == "withdraw_methods":
        return get_withdraw_methods_kb(is_english)
    elif kb_type == "watch_controls":
        return get_watch_controls_kb(is_english)
    elif kb_type == "banks":
        return get_banks_kb(is_english)
    elif kb_type == "countries":
        return get_countries_kb(is_english)
    elif kb_type == "tokens":
        return get_tokens_kb(is_english)
    elif kb_type == "networks":
        token = kwargs.get('token', 'USDT')
        return await get_networks_kb(is_english, token)  
    elif kb_type == "withdraw_amount":
        user = await store.get_user(uid)
        token = kwargs.get('token', 'USDT')
        balance = kwargs.get('balance', user.balance)
        return await withdraw_amount_kb(uid, token, balance)
    elif kb_type == "settings_amount":
        return get_settings_amount_kb()
    elif kb_type == "settings_leverage":
        return get_settings_leverage_kb()
    elif kb_type == "history_more":
        return get_history_more_kb(is_english)
    elif kb_type == "withdraw_token":
        return get_withdraw_token_kb(is_english)
    elif kb_type == "withdraw_network":
        token = kwargs.get('token', 'USDT')
        return get_withdraw_network_kb(is_english, token)
    else:
        return get_default_kb(kb_type)
def get_networks_kb(is_english: bool = False, token: str = "USDT") -> InlineKeyboardMarkup:
    networks = get_available_networks(token)
    rows = []
    row = []
    for i, net in enumerate(networks, 1):
        row.append(InlineKeyboardButton(text=net, callback_data=f"dep_net:{net}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
def balance_link_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Баланс", url=f"https://t.me/{TRADING_BOT_USERNAME}?start=balance")]])
def assets_button_kb(bot_code: str | None = None) -> InlineKeyboardMarkup:
    if bot_code == "en":
        text = "📊 Assets"
    else:
        text = "📊 Активы"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data="open_assets")]
        ]
    )
def get_main_menu_kb(language_code: str = "ru") -> ReplyKeyboardMarkup:
    if language_code == "en":
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Assets"), KeyboardButton(text="Open Positions")],
                [KeyboardButton(text="Trade History"), KeyboardButton(text="AI Trading")],
                [KeyboardButton(text="Settings")],
            ],
            resize_keyboard=True,
            input_field_placeholder="Choose action…",
        )
    else:
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Активы"), KeyboardButton(text="Открытые сделки")],
                [KeyboardButton(text="История сделок"), KeyboardButton(text="AI Трейдинг")],
                [KeyboardButton(text="Настройки")],
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие…",
        )
def settings_amount_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, a in enumerate(AMOUNTS, 1):
        row.append(InlineKeyboardButton(text=f"${a}", callback_data=f"set_amount:{a}"))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
def settings_leverage_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, l in enumerate(LEVERAGES, 1):
        row.append(InlineKeyboardButton(text=f"x{l}", callback_data=f"set_lev:{l}"))
        if i % 4 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
def get_withdraw_token_kb(is_english: bool = False) -> InlineKeyboardMarkup:
    if is_english:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="USDT", callback_data="wd_token:USDT")],
            [InlineKeyboardButton(text="ETHEREUM", callback_data="wd_token:ETH")],
            [InlineKeyboardButton(text="BITCOIN", callback_data="wd_token:BTC")],
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="USDT", callback_data="wd_token:USDT")],
            [InlineKeyboardButton(text="ETHEREUM", callback_data="wd_token:ETH")],
            [InlineKeyboardButton(text="BITCOIN", callback_data="wd_token:BTC")],
        ])
def get_withdraw_network_kb(is_english: bool = False, token: str = "USDT") -> InlineKeyboardMarkup:
    networks = get_available_networks(token)
    rows = []
    row = []
    for i, net in enumerate(networks, 1):
        row.append(InlineKeyboardButton(text=net, callback_data=f"wd_net:{net}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
async def amount_choice_kb(prefix: str = "dep_card_amt", user_id: int = None) -> InlineKeyboardMarkup:
    try:
        min_deposit = 0
        if user_id:
            min_deposit = await get_user_min_deposit(user_id)
            logger.info(f"📊 Минимальный депозит для пользователя {user_id}: ${min_deposit}")
        base_amounts = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
        if min_deposit > 0:
            filtered_amounts = [a for a in base_amounts if a >= min_deposit]
            if not filtered_amounts:
                filtered_amounts = [min_deposit]
        else:
            filtered_amounts = base_amounts
        rows = []
        row = []
        for i, amount in enumerate(filtered_amounts, start=1):
            row.append(
                InlineKeyboardButton(
                    text=f"${amount}",
                    callback_data=f"{prefix}:{amount}"
                )
            )
            if i % 3 == 0:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        return InlineKeyboardMarkup(inline_keyboard=rows)
    except Exception as e:
        logger.error(f"Ошибка в amount_choice_kb: {e}")
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="$500", callback_data=f"{prefix}:500"),
                InlineKeyboardButton(text="$1000", callback_data=f"{prefix}:1000"),
                InlineKeyboardButton(text="$2500", callback_data=f"{prefix}:2500"),
            ]
        ])
def watch_controls_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⏹ Остановить AI Трейдинг", callback_data="stop_watch")]]
    )
def open_market_kb(is_english: bool = False) -> InlineKeyboardMarkup:
    text = "🟢 Open at market" if is_english else "🟢 Открыть по рынку"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=text, callback_data="open_market")]]
    )
def disabled_open_kb(is_english: bool = False) -> InlineKeyboardMarkup:
    text = "⏳ Time is over" if is_english else "⏳ Время вышло"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=text, callback_data="noop")]]
    )
SIG_RE = re.compile(
    r"Сигнал\s+([A-Z]+USDT)\s*\((\d+[mh])\)[\s\S]*?" 
    r"(SHORT|LONG|ШОРТ|ЛОНГ)[\s\S]*?" 
    r"ТВХ:\s*([\d\.,]+)[\s\S]*?" 
    r"SL:\s*([\d\.,]+)[\s\S]*?"
    r"(?:TP[^:]*:\s*([0-9\.,\s/]+)|TP:\s*([\d\.,]+))[\s\S]*?"
    r"Реком\.\s*сумма:\s*\$(\d+)[\s\S]*?"
    r"Дата:\s*([0-9:\-\sUTC]+)",
    re.S | re.I,
)
async def get_user_min_deposit(uid: int) -> int:
    raw = await r.get(f"user:{uid}:min_deposit")
    if raw:
        return int(raw.decode())
    raw_global = await r.get("config:min_deposit_global")
    return int(raw_global.decode()) if raw_global else 0
def generate_deposit_buttons(min_amount: int = 0) -> InlineKeyboardMarkup:
    base_amounts = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
    if min_amount > 0:
        amounts = [a for a in base_amounts if a >= min_amount]
        if not amounts:
            amounts = [min_amount]
    else:
        amounts = base_amounts
    buttons = []
    row = []
    for i, amt in enumerate(amounts, start=1):
        row.append(InlineKeyboardButton(text=f"${amt}", callback_data=f"deposit:{amt}"))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)
async def get_user_min_deposit(uid: int) -> float:
    raw = await r.get(f"user:{uid}:min_deposit")
    if raw:
        return float(raw.decode())
    return 0
async def generate_crypto_deposit_buttons(user_id: int = None) -> InlineKeyboardMarkup:
    async def get_filtered_amounts():
        try:
            if user_id:
                min_deposit = await store.get_user_min_deposit(user_id)
            else:
                min_deposit = 0
            base_amounts = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
            if min_deposit > 0:
                filtered = [amt for amt in base_amounts if amt >= min_deposit]
                if not filtered:
                    filtered = [min_deposit]
                return filtered
            return base_amounts
        except:
            return base_amounts
    amounts = await get_filtered_amounts()
    keyboard = []
    for amt in amounts:
        keyboard.append([InlineKeyboardButton(
            text=f"Пополнить ${amt}",
            callback_data=f"deposit_crypto_amt:{amt}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
async def get_global_min_deposit() -> float:
    try:
        raw = await r.get("config:min_deposit_global")
        if raw:
            if isinstance(raw, bytes):
                return float(raw.decode('utf-8'))
            return float(raw)
    except Exception as e:
        logger.error(f"Error getting global min deposit: {e}")
    return 0.0
async def set_global_min_deposit(amount: float):
    try:
        await r.set("config:min_deposit_global", str(amount))
        logger.info(f"Global min deposit set to: ${amount:.2f}")
    except Exception as e:
        logger.error(f"Error setting global min deposit: {e}")
@dataclass
class ParsedSignal:
    symbol: str
    tf: str
    side: Side
    entry: float
    sl: float
    tp: float
    rec_amount: int
    date_utc: str
    strength: str | None = None
def signal_fingerprint(ps: ParsedSignal) -> str:
    try:
        base = f"{ps.symbol}|{ps.tf}|{getattr(ps.side,'value',str(ps.side))}|{float(ps.entry):.6f}|{float(ps.tp):.6f}|{float(ps.sl):.6f}|{ps.strength or ''}".lower()
    except Exception:
        base = str(ps).lower()
    import hashlib 
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
def parse_signal(text: str) -> Optional[ParsedSignal]:
    t = (text or "").strip()
    if not t:
        return None
    t_norm = re.sub(r"[\u2014\u2013]+", "-", t)
    t_norm = re.sub(r"[\xa0]+", " ", t_norm)
    t_norm = re.sub(r"\s+", " ", t_norm)
    logger.info(f"🔍 Parsing signal text: {t_norm[:200]}...")
    patterns = [
        r"(?i)(СИЛЬНЫЙ|СРЕДНИЙ|СЛАБЫЙ|НИЗКИЙ)?\s*СИГНАЛ\s+([A-Z]+USDT)\s*\((\d+[mh])\)\s*(LONG|SHORT|ЛОНГ|ШОРТ)",
        r"(?i)СИГНАЛ\s+([A-Z]+USDT)\s*\((\d+[mh])\)\s*-\s*(LONG|SHORT|ЛОНГ|ШОРТ)",
        r"(?i)СИГНАЛ\s+-\s+([A-Z]+USDT)\s*\((\d+[mh])\)\s*-\s*(LONG|SHORT|ЛОНГ|ШОРТ)",
        r"(?i)(STRONG|MEDIUM|WEAK|LOW)?\s*SIGNAL\s+([A-Z]+USDT)\s*\((\d+[mh])\)\s*(LONG|SHORT)",
        r"(?i)SIGNAL\s+([A-Z]+USDT)\s*\((\d+[mh])\)\s*-\s*(LONG|SHORT)",
    ]
    symbol = tf = side_raw = strength = None
    for pattern in patterns:
        m = re.search(pattern, t_norm)
        if m:
            groups = m.groups()
            logger.info(f"✅ Pattern matched: {pattern}, groups: {groups}")
            if len(groups) == 4:  
                strength, symbol, tf, side_raw = groups
            elif len(groups) == 3:  
                symbol, tf, side_raw = groups
                strength = None
            break
    if not symbol:
        logger.info("🔄 Trying fallback parsing...")
        m_sym = re.search(r"\b([A-Z]{2,}USDT)\b", t_norm)
        if m_sym:
            symbol = m_sym.group(1)
        m_tf = re.search(r"\((\d+[mh])\)", t_norm, flags=re.I)
        if m_tf:
            tf = m_tf.group(1)
        m_side = re.search(r"\b(LONG|SHORT|ЛОНГ|ШОРТ)\b", t_norm, flags=re.I)
        if m_side:
            side_raw = m_side.group(1)
        m_strength = re.search(r"(?i)\b(СИЛЬНЫЙ|СРЕДНИЙ|СЛАБЫЙ|НИЗКИЙ|STRONG|MEDIUM|WEAK|LOW)\b", t_norm)
        if m_strength:
            strength = m_strength.group(1)
        if symbol and tf and side_raw:
            logger.info(f"✅ Fallback matched: {symbol}, {tf}, {side_raw}, {strength}")
        else:
            logger.warning("❌ No pattern matched in signal")
            return None
    dr = (side_raw or "").upper()
    side = Side.LONG if ("LONG" in dr or "ЛОНГ" in dr) else Side.SHORT
    st = (strength or "").upper().strip() if strength else ""
    if st == "НИЗКИЙ" or st == "LOW": 
        st = "СЛАБЫЙ"
    if not st: 
        st = None
    def fnum(rx: str):
        m = re.search(rx, t_norm, flags=re.I)
        if not m:
            return None
        s = m.group(1)
        s = re.sub(r"\s", "", s)  
        s = s.replace(',', '.')
        try:
            return float(s)
        except Exception:
            return None
    entry = None
    entry_patterns = [
        r"ТВХ:\s*([0-9][0-9\s.,]*)",
        r"TBX:\s*([0-9][0-9\s.,]*)", 
        r"TVX:\s*([0-9][0-9\s.,]*)",
        r"Вход\s*\(TVX\)\s*:\s*([0-9][0-9\s.,]*)",
        r"Вход:\s*([0-9][0-9\s.,]*)",
        r"Цена\s*входа:\s*([0-9][0-9\s.,]*)",
        r"Entry:\s*([0-9][0-9\s.,]*)",
        r"ENTRY:\s*([0-9][0-9\s.,]*)",
    ]
    for rx in entry_patterns:
        entry = fnum(rx)
        if entry is not None:
            logger.info(f"✅ Entry found: {entry}")
            break
    sl = None
    sl_patterns = [
        r"SL:\s*([0-9][0-9\s.,]*)",
        r"СТОП[-\s]*ЛОСС?:\s*([0-9][0-9\s.,]*)",
        r"STOP[-\s]*LOSS?:\s*([0-9][0-9\s.,]*)",
    ]
    for rx in sl_patterns:
        sl = fnum(rx)
        if sl is not None:
            logger.info(f"✅ SL found: {sl}")
            break
    tp = None
    tp_patterns = [
        r"TP\d*(?:\s*/\s*TP\d*)*:\s*([0-9][0-9\s.,]*)",
        r"TP:\s*([0-9][0-9\s.,]*)",
        r"ТЕЙК[-\s]*ПРОФИТ?:\s*([0-9][0-9\s.,]*)",
        r"TAKE[-\s]*PROFIT?:\s*([0-9][0-9\s.,]*)",
    ]
    for rx in tp_patterns:
        tp = fnum(rx)
        if tp is not None:
            logger.info(f"✅ TP found: {tp}")
            break
    if tp is None or sl is None:
        sl_tp_match = re.search(r"SL:\s*([0-9\s.,]+)\s+TP:\s*([0-9\s.,]+)", t_norm, flags=re.I)
        if sl_tp_match:
            try:
                if sl is None:
                    sl_str = sl_tp_match.group(1).replace(' ', '').replace(',', '.')
                    sl = float(sl_str)
                    logger.info(f"✅ SL found after pattern: {sl}")
                if tp is None:
                    tp_str = sl_tp_match.group(2).replace(' ', '').replace(',', '.')
                    tp = float(tp_str)
                    logger.info(f"✅ TP found after pattern: {tp}")
            except Exception as e:
                logger.warning(f"Failed to parse SL/TP from pattern: {e}")
    if entry is None or sl is None or tp is None:
        logger.warning(f"❌ Missing required values: entry={entry}, sl={sl}, tp={tp}")
        numbers = re.findall(r"(\d+[.,]\d+)", t_norm)
        if len(numbers) >= 3:
            try:
                if entry is None:
                    entry = float(numbers[0].replace(',', '.'))
                if sl is None:
                    sl = float(numbers[1].replace(',', '.'))
                if tp is None:
                    tp = float(numbers[2].replace(',', '.'))
                logger.info(f"🔄 Using fallback numbers: entry={entry}, sl={sl}, tp={tp}")
            except Exception:
                pass
        
        if entry is None or sl is None or tp is None:
            return None
    rec_amount = 10  
    rec_patterns = [
        r"Реком\.\s*сумма:\s*\$?\s*([0-9]+(?:[.,][0-9]+)?)",
        r"Рекомендуемая\s*сумма:\s*\$?\s*([0-9]+(?:[.,][0-9]+)?)",
        r"Сумма:\s*\$?\s*([0-9]+(?:[.,][0-9]+)?)",
        r"Amount:\s*\$?\s*([0-9]+(?:[.,][0-9]+)?)",
        r"RECOMMENDED\s*AMOUNT:\s*\$?\s*([0-9]+(?:[.,][0-9]+)?)",
    ]
    for rx in rec_patterns:
        m_rec = re.search(rx, t_norm, flags=re.I)
        if m_rec:
            try:
                rec_amount = int(float(m_rec.group(1).replace(',', '.')))
                logger.info(f"✅ Recommended amount found: {rec_amount}")
                break
            except Exception:
                continue
    date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    date_patterns = [
        r"Дата:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+UTC)",
        r"Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+UTC)",
    ]
    for rx in date_patterns:
        m_dt = re.search(rx, t_norm, flags=re.I)
        if m_dt:
            date_utc = m_dt.group(1)
            logger.info(f"✅ Date found: {date_utc}")
            break
    logger.info(f"🎯 Successfully parsed signal: {symbol} {side.value} entry={entry} tp={tp} sl={sl} rec_amount={rec_amount}")
    return ParsedSignal(
        symbol=symbol,
        tf=tf,
        side=side,
        entry=entry,
        sl=sl,
        tp=tp,
        rec_amount=rec_amount,
        date_utc=date_utc,
        strength=st
    )
def gen_event_id() -> str:
    return f"ev:{int(time.time()*1000)}:{random.randint(1000,9999)}"
def fmt_money(x: float) -> str:
    s = f"{x:.2f}"
    return ("-" if x < 0 else "") + "$" + s.replace("-", "")
def fmt_pct(x: float) -> str:
    return ("-" if x < 0 else "+") + f"{abs(x):.2f}%"
def ts_to_hms(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%H:%M:%S UTC")
def seconds_left(opened_ts: float, max_dur: int) -> str:
    left = max(0, int(opened_ts + max_dur - time.time()))
    return f"{left // 60:02d}:{left % 60:02d}"
async def is_bot_available_for_user(user_id: int) -> bool:
    try:
        await bot.get_chat(user_id)
        return True
    except TelegramAPIError as e:
        error_msg = str(e).lower()
        blocked_phrases = [
            "bot was blocked", 
            "user is deactivated", 
            "chat not found",
            "forbidden: bot was blocked",
            "bot was kicked"
        ]
        if any(phrase in error_msg for phrase in blocked_phrases):
            logger.info(f"Bot blocked by user {user_id}")
            await send_bot_blocked_event(user_id, error_msg)
            return False
        logger.warning(f"Other Telegram API error for user {user_id}: {e}")
        return True
async def choose_outcome(store: Store, uid: int, base_loss: float = 0.3) -> Literal["TP", "SL"]:
    last10 = await store.get_last10(uid)
    losses = last10.count("L")
    p_loss = base_loss
    if len(last10) >= 5:
        if losses < 3:
            p_loss = min(0.6, base_loss + 0.05 * (3 - losses))
        elif losses > 3:
            p_loss = max(0.1, base_loss - 0.05 * (losses - 3))
    return "SL" if random.random() < p_loss else "TP"
async def run_position_loop(bot: Bot, store: Store, p: Position, message_chat_id: int, message_id: int):
    try:
        tick_dt = 0.5
        fallback_dt = 1.0
        outcome_hint = "TP"
        price = p.entry_price
        p.price_now = price
        await store.update_position(p)
        async def compute_pnl(price_now: float):
            if p.side == Side.LONG:
                pnl_pct = (price_now - p.entry_price) / p.entry_price * p.leverage * 100.0
            else:
                pnl_pct = (p.entry_price - price_now) / p.entry_price * p.leverage * 100.0
            pnl_abs = p.order_amount * pnl_pct / 100.0
            return pnl_abs, pnl_pct
        deadline_ts = p.opened_at + p.max_duration_sec
        while True:
            now = time.time()
            goal = p.tp if outcome_hint == "TP" else p.sl
            dist = (goal - price)
            drift = 0.05 * dist  
            noise = random.gauss(0.0, max(1e-9, p.entry_price) * 0.0006)
            price += drift + noise
            hit_tp = (price >= p.tp) if p.side == Side.LONG else (price <= p.tp)
            hit_sl = (price <= p.sl) if p.side == Side.LONG else (price >= p.sl)
            p.price_now = price
            pnl_abs, pnl_pct = await compute_pnl(price)
            p.pnl_current = pnl_abs
            p.pnl_history.append((now, pnl_abs))
            p.last_tick_at = now
            is_english = await is_english_user(p.user_id)
            if is_english:
                entry_text = "Entry"
                current_text = "Current"
                pnl_text = "PNL"
            else:
                entry_text = "Вход"
                current_text = "Текущая"
                pnl_text = "PNL"
            pnl_display = f"${abs(pnl_abs):.2f}" if pnl_abs >= 0 else f"-${abs(pnl_abs):.2f}"
            txt = (
                f"{p.symbol} {p.side.value}\n"
                f"{entry_text}: {p.entry_price:.5f} | {current_text}: {price:.5f}\n"
                f"{pnl_text}: {pnl_display} ({pnl_pct:+.2f}%)"
            )
            try:
                await bot.edit_message_text(
                    chat_id=message_chat_id,
                    message_id=message_id,
                    text=txt,
                )
                dt = tick_dt
            except TelegramRetryAfter as e:
                try:
                    dt = max(fallback_dt, float(e.retry_after))
                except Exception:
                    dt = fallback_dt
            except TelegramBadRequest:
                dt = fallback_dt
            await store.update_position(p)
            closed_by = None
            exit_price = price
            if hit_tp:
                p.status = PosStatus.CLOSED_TP
                closed_by = "TP"
            elif hit_sl:
                p.status = PosStatus.CLOSED_SL
                closed_by = "SL"
            elif now >= deadline_ts:
                p.status = PosStatus.CLOSED_TIME
                closed_by = "TIME"
            if closed_by:
                pnl_abs, pnl_pct = await compute_pnl(exit_price)
                user = await store.get_user(p.user_id)
                before_balance = user.balance
                user.balance += pnl_abs
                await store.save_user(user)
                try:
                    await _init_trading_bot_username_once()
                    owner = await store.get_bot_owner(p.user_id)
                    support_event = {
                        "type": "position_closed",
                        "event_id": f"position_closed_{p.user_id}_{int(time.time() * 1000)}",
                        "user_id": p.user_id,
                        "username": user.username or str(p.user_id),
                        "first_name": user.first_name or "",
                        "last_name": user.last_name or "",
                        "position_id": p.id,
                        "symbol": p.symbol,
                        "side": p.side.value,
                        "entry_price": p.entry_price,
                        "exit_price": exit_price,
                        "pnl_abs": pnl_abs,
                        "pnl_pct": pnl_pct,
                        "closed_by": closed_by,
                        "order_amount": p.order_amount,
                        "leverage": p.leverage,
                        "duration_sec": int(time.time() - p.opened_at),
                        "bot_username": TRADING_BOT_USERNAME,
                        "timestamp": time.time(),
                        "bot": "ru" if await get_user_language(p.user_id) == "ru" else "en",
                        "bot_owner_id": owner or user_id,    
                    }
                    await store.push_support_event(support_event)
                    logger.info(f"✅ Position closed event sent to support queue: {p.symbol} {p.side.value} PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)")
                except Exception as e:
                    logger.error(f"❌ Failed to send position_closed event to support queue: {e}")
                try:
                    owner = await store.get_bot_owner(m.from_user.id)
                    await store.push_support_event({
                        "type": "balance_update", 
                        "user_id": p.user_id,
                        "username": None,
                        "before": before_balance,
                        "after": user.balance,
                        "reason": "trade_pnl",
                        "timestamp": time.time()
                    })
                except Exception:
                    pass
                await store.remove_position(p.user_id, p.id)
                hist = TradeHistory(
                    position_id=p.id,
                    symbol=p.symbol,
                    side=p.side,
                    entry=p.entry_price,
                    exit=exit_price,
                    pnl_abs=pnl_abs,
                    pnl_pct=pnl_pct,
                    closed_by=closed_by,
                    closed_at=time.time(),
                )
                await store.add_history(p.user_id, hist)
                await store.push_outcome(p.user_id, "L" if closed_by == "SL" else "W")
                try:
                    await bot.delete_message(chat_id=message_chat_id, message_id=message_id)
                except Exception:
                    pass
                try:
                    await support_emit({
                        "type": "position_closed",
                        "user_id": p.user_id,
                        "username": None,
                        "symbol": p.symbol,
                        "side": p.side.value if hasattr(p.side, "value") else str(p.side),
                        "exit": exit_price,
                        "pnl_abs": pnl_abs,
                        "pnl_pct": pnl_pct,
                        "closed_by": closed_by
                    })
                except Exception:
                    pass
                if closed_by == "TP":
                    if is_english:
                        text = f"✅ Position closed by Take Profit: {p.symbol} {p.side.value} | PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)"
                    else:
                        text = f"✅ Сделка исполнена по Тейк-профиту: {p.symbol} {p.side.value} | PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)"
                elif closed_by == "SL":
                    if is_english:
                        text = f"❌ Position closed by Stop Loss: {p.symbol} {p.side.value} | PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)"
                    else:
                        text = f"❌ Сделка исполнена по Стоп-лоссу: {p.symbol} {p.side.value} | PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)"
                else:
                    if is_english:
                        text = f"⏱️ Position closed by time: {p.symbol} {p.side.value} | PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)"
                    else:
                        text = f"⏱️ Сделка завершена по времени: {p.symbol} {p.side.value} | PnL: ${pnl_abs:.2f} ({pnl_pct:+.2f}%)"
                await safe_send_text(p.user_id, text)
                break
            await store.increment_user_trades(p.user_id, pnl_abs)
            await store.update_user_activity(p.user_id)
            await asyncio.sleep(dt)
    except Exception:
        logger.exception("run_position_loop crashed")
def with_error_handling(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper
async def clear_user_data(uid: int):
    pipe = store.r.pipeline()
    pipe.delete(RKeys.user(uid))
    pipe.delete(RKeys.positions_of(uid))
    pipe.delete(RKeys.history(uid))
    await pipe.execute()
async def log_user_action(user_id: int, action: str, **kwargs):
    logger.info("user_action", 
                user_id=user_id, 
                action=action, 
                timestamp=time.time(),
                **kwargs)
def mask_card(card: str) -> str:
    import re
    dig = re.sub(r"\D+", "", card or "")
    if len(dig) < 8:
        return "*" * max(0, len(dig) - 2) + dig[-2:]
    return f"{dig[:4]} **** **** {dig[-4:]}"
def wd_card_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить вывод", callback_data="wd_card_confirm"),
         InlineKeyboardButton(text="✖️ Отмена", callback_data="wd_card_cancel")],
    ])
def wd_reject_requisites_kb(event_id: str) -> InlineKeyboardMarkup:
    event_id_safe = event_id.replace(':', '_')
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_wd_approve:{event_id_safe}"),
            InlineKeyboardButton(text="❌ Отказ: реквизиты", callback_data=f"admin_wd_reject_requisites:{event_id_safe}")
        ],
        [InlineKeyboardButton(text="💳 Баланс", url=f"https://t.me/{TRADING_BOT_USERNAME}?start=balance")]
    ])
async def check_bot_blocked_status(user_id: int) -> bool:
    try:
        await bot.get_chat(user_id)
        return False  
    except Exception as e:
        error_msg = str(e).lower()
        blocked_phrases = [
            "bot was blocked", 
            "user is deactivated",
            "chat not found",
            "forbidden: bot was blocked",
            "bot was kicked"
        ]
        if any(phrase in error_msg for phrase in blocked_phrases):
            return True 
        return False
async def send_bot_blocked_event(user_id: int, reason: str):
    try:
        owner = await store.get_bot_owner(user_id)
        support_event = {
            "type": "bot_blocked",
            "event_id": f"bot_blocked_{user_id}_{int(time.time() * 1000)}",
            "user_id": user_id,
            "timestamp": time.time(),
            "bot_username": TRADING_BOT_USERNAME,
            "reason": reason,
            "bot": "ru",
            "detected_by": "periodic_check",
            "bot_owner_id": owner or user_id,
        }
        await store.push_support_event(support_event)
        logger.info(
            f"🚫 Bot blocked event sent: user {user_id}, reason: {reason}"
        )
    except Exception as e:
        logger.error(f"Failed to send bot_blocked event: {e}")
async def _init_trading_bot_username_once():
    """Initialize bot username if not already set"""
    global TRADING_BOT_USERNAME
    if not TRADING_BOT_USERNAME:
        try:
            if not bot:
                logger.error("❌ Bot instance is not initialized")
                return
            me = await bot.get_me()
            TRADING_BOT_USERNAME = me.username
            logger.info(f"✅ Bot username initialized: @{TRADING_BOT_USERNAME}")
            await r.setex("bot:username", 86400, TRADING_BOT_USERNAME)
        except Exception as e:
            logger.error(f"❌ Failed to get bot username: {e}")
            try:
                cached_username = await r.get("bot:username")
                if cached_username:
                    TRADING_BOT_USERNAME = cached_username.decode() if isinstance(cached_username, bytes) else cached_username
                    logger.info(f"📦 Using cached bot username: @{TRADING_BOT_USERNAME}")
            except Exception:
                pass
from redis.asyncio import ConnectionPool
redis_pool = ConnectionPool.from_url(
    REDIS_URL,
    decode_responses=False,
    max_connections=15,  # Увеличьте для Windows
    socket_keepalive=True,
    socket_connect_timeout=15,  # Увеличьте таймаут
    socket_timeout=30,  # Увеличьте таймаут операций
    retry_on_timeout=True,
    health_check_interval=30,
)

# Создайте глобальный клиент Redis
r: redis.Redis = redis.Redis(connection_pool=redis_pool)
store = Store(r)
class S(StatesGroup):
    CHOOSING_LANGUAGE = State()  
    IDLE = State()
    WATCHING_SIGNALS = State()
    COUNTDOWN_OPEN = State()
    ORDER_OPENING = State()
    POSITION_ACTIVE = State()
    SETTINGS_AMOUNT = State()
    SETTINGS_LEVERAGE = State()
    DONATE_FLOW = State()
    WD_AMOUNT = State()
    WD_WAIT_ADDR = State()
    WD_CONFIRM = State()
    WD_CHOOSE_METHOD = State()
    WD_CHOOSE_TOKEN = State()
    WD_CHOOSE_NETWORK = State()
    WD_WAIT_WALLET = State()
    WD_WAIT_AMOUNT = State()
    DEP_CARD_FIO = State()
    DEP_CARD_BANK = State()
    WAIT_CARD_DETAILS = State()
    WAIT_PAYMENT_CONFIRMATION = State()
    WD_WAIT_FIO = State()
    WD_WAIT_BANK = State()
    WD_WAIT_CARD = State()
    WD_WAIT_COUNTRY = State()
    WD_WAIT_UZBEK_BANK = State()
    DEP_WAIT_COUNTRY = State()
    DEP_WAIT_UZBEK_BANK = State()
    SETTINGS_LANGUAGE = State()  
    SETTINGS_MAIN = State()     
def deposit_country_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇺🇿 Узбекистан", callback_data="dep_country_uzbekistan")],
        [InlineKeyboardButton(text="🇷🇺 Россия", callback_data="dep_country_russia")],
        [InlineKeyboardButton(text="🌍 Другая страна", callback_data="dep_country_other")],
    ])
def deposit_uzbek_bank_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Каспи банк", callback_data="dep_uzbek_bank_kaspi")],
        [InlineKeyboardButton(text="Халык Банк", callback_data="dep_uzbek_bank_halyk")],
        [InlineKeyboardButton(text="Другой банк Узбекистана", callback_data="dep_uzbek_bank_other")],
    ])
def withdraw_country_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇺🇿 Узбекистан", callback_data="country_uzbekistan")],
        [InlineKeyboardButton(text="🇷🇺 Россия", callback_data="country_russia")],
        [InlineKeyboardButton(text="🌍 Другая страна", callback_data="country_other")],
    ])
def withdraw_uzbek_bank_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Каспи Банк", callback_data="uzbek_bank_kaspi")],
        [InlineKeyboardButton(text="Халык Банк", callback_data="uzbek_bank_halyk")],
        [InlineKeyboardButton(text="Другой банк Узбекистана", callback_data="uzbek_bank_other")],
    ])
def get_settings_main_kb(is_english: bool = False, current_amount: float = None, current_leverage: int = None, current_language: str = "ru") -> InlineKeyboardMarkup:
    language_flag = "🇷🇺" if current_language == "ru" else "🇺🇸"
    if is_english:
        amount_display = current_amount if current_amount is not None else 10.0
        leverage_display = current_leverage if current_leverage is not None else 1
        amount_text = f"📊 Order Amount (${amount_display:.2f})"
        leverage_text = f"⚡ Leverage (x{leverage_display})"
        language_text = f"{language_flag} Language"
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=amount_text, callback_data="settings_order")],
            [InlineKeyboardButton(text=leverage_text, callback_data="settings_leverage")],
            [InlineKeyboardButton(text=language_text, callback_data="settings_language")],
            [InlineKeyboardButton(text="🔙 Back to Main Menu", callback_data="settings_back")], 
        ])
    else:
        amount_display = current_amount if current_amount is not None else 10.0
        leverage_display = current_leverage if current_leverage is not None else 1
        amount_text = f"📊 Сумма ордера (${amount_display:.2f})"
        leverage_text = f"⚡ Плечо (x{leverage_display})"
        language_text = f"{language_flag} Язык"
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=amount_text, callback_data="settings_order")],
            [InlineKeyboardButton(text=leverage_text, callback_data="settings_leverage")],
            [InlineKeyboardButton(text=language_text, callback_data="settings_language")],
            [InlineKeyboardButton(text="🔙 Назад в главное меню", callback_data="settings_back")],  
        ])
def get_settings_language_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="set_lang:ru")],
        [InlineKeyboardButton(text="🇺🇸 English", callback_data="set_lang:en")],
    ])
def get_settings_amount_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, a in enumerate(AMOUNTS, 1):
        row.append(InlineKeyboardButton(text=f"${a}", callback_data=f"set_amount:{a}"))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)    
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="settings_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
def get_settings_leverage_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, l in enumerate(LEVERAGES, 1):
        row.append(InlineKeyboardButton(text=f"x{l}", callback_data=f"set_lev:{l}"))
        if i % 4 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="settings_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
def withdraw_token_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="USDT", callback_data="wd_token:USDT")],
        [InlineKeyboardButton(text="ETHEREUM", callback_data="wd_token:ETH")],
        [InlineKeyboardButton(text="BITCOIN", callback_data="wd_token:BTC")],
    ])
def withdraw_network_kb(token: str = "USDT") -> InlineKeyboardMarkup:
    networks = get_available_networks(token)
    rows = []
    row = []
    for i, net in enumerate(networks, 1):
        row.append(InlineKeyboardButton(text=net, callback_data=f"wd_net:{net}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
def validate_fio(fio: str) -> bool:
    if not fio or len(fio.strip()) < 2:
        return False
    parts = fio.strip().split()
    if len(parts) < 2:
        return False
    if len(fio) > 100:
        return False
    import re
    if not re.match(r'^[a-zA-Zа-яА-ЯёЁ\s\-\.]+$', fio):
        return False
    return True
async def withdraw_amount_kb(user_id: int, token: str, balance: float) -> InlineKeyboardMarkup:    
    is_english = await is_english_user(user_id)
    all_balance_text = await get_localized_text(user_id, "withdraw_all_balance")
    cancel_text = await get_localized_text(user_id, "cancel")
    display_text = ""
    if token in ("ETH", "ETHEREUM"):
        eth_price = await fetch_usd_price("ETHUSDT")
        if eth_price and eth_price > 0:
            eth_amount = balance / eth_price
            display_text = f"{all_balance_text} ({eth_amount:.6f} ETH)"
        else:
            display_text = f"{all_balance_text} (${balance:.2f})"
    elif token in ("BTC", "BITCOIN"):
        btc_price = await fetch_usd_price("BTCUSDT")
        if btc_price and btc_price > 0:
            btc_amount = balance / btc_price
            display_text = f"{all_balance_text} ({btc_amount:.8f} BTC)"
        else:
            display_text = f"{all_balance_text} (${balance:.2f})"
    else:
        display_text = f"{all_balance_text} (${balance:.2f})"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=display_text, callback_data="wd_amount_all")],
        [InlineKeyboardButton(text=cancel_text, callback_data="wd_cancel")],
    ])
def withdraw_method_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Криптовалюта", callback_data="wd_method_crypto")],
        [InlineKeyboardButton(text="Банковская карта", callback_data="wd_method_card")],
    ])
@router.message(S.WD_WAIT_FIO)
async def wd_wait_fio(m: Message, state: FSMContext):
    fio = m.text.strip()
    if len(fio.split()) < 2:
        error_text = await get_localized_text(m.from_user.id, "invalid_fio")
        await m.answer(error_text)
        return
    await state.update_data(fio=fio)
    await state.set_state(S.WD_WAIT_BANK)
    success_text = await get_localized_text(
        m.from_user.id,
        "fio_accepted",
        fio=fio,
    )
    bank_text = await get_localized_text(
        m.from_user.id,
        "withdraw_card_choose_bank",
    )
    banks_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сбербанк",    callback_data="wd_bank_sber")],
        [InlineKeyboardButton(text="Тинькофф",    callback_data="wd_bank_tinkoff")],
        [InlineKeyboardButton(text="Альфа-Банк",  callback_data="wd_bank_alpha")],
        [InlineKeyboardButton(text="ВТБ",         callback_data="wd_bank_vtb")],
        [InlineKeyboardButton(text="Другой банк", callback_data="wd_bank_other")],
    ])
    await m.answer(f"{success_text}\n\n{bank_text}", reply_markup=banks_kb)
async def ensure_bot_initialized():
    global TRADING_BOT_USERNAME
    if not TRADING_BOT_USERNAME:
        await _init_trading_bot_username_once()
@router.callback_query(F.data.startswith("wd_bank_"), S.WD_WAIT_BANK)
async def wd_bank_select(cb: CallbackQuery, state: FSMContext):
    bank_map = {
        "wd_bank_sber": "Сбербанк",
        "wd_bank_tinkoff": "Тинькофф", 
        "wd_bank_alpha": "Альфа-Банк",
        "wd_bank_vtb": "ВТБ",
        "wd_bank_other": "Другой банк"
    }
    bank_key = cb.data
    bank_name = bank_map.get(bank_key, "Неизвестный банк")
    await state.update_data(bank=bank_name)
    if bank_key == "wd_bank_other":
        await state.set_state(S.WD_WAIT_COUNTRY)
        await cb.message.edit_text(
            "🌍 <b>Выберите страну</b>\n\n"
            "Выберите страну, в которой находится ваш банк:",
            reply_markup=withdraw_country_kb()
        )
    else:
        await state.set_state(S.WD_WAIT_CARD)
        await cb.message.edit_text(
            "✅ <b>Банк выбран</b>\n\n"
            f"🏦 Банк: {bank_name}\n\n"
            "💳 Теперь введите номер банковской карты для получения средств:\n\n"
            "<i>Пример: 2200 1234 5678 9010</i>"
        )
    await cb.answer()
@router.callback_query(F.data.startswith("country_"), S.WD_WAIT_COUNTRY)
async def wd_country_select(cb: CallbackQuery, state: FSMContext):
    country_map = {
        "country_uzbekistan": "Узбекистан",
        "country_russia": "Россия", 
        "country_other": "Другая страна"
    }
    country_key = cb.data
    country_name = country_map.get(country_key, "Неизвестная страна")
    await state.update_data(country=country_name)
    if country_key == "country_uzbekistan":
        await state.set_state(S.WD_WAIT_UZBEK_BANK)
        await cb.message.edit_text(
            "🏦 <b>Выберите банк Узбекистана</b>\n\n"
            "Выберите ваш банк:",
            reply_markup=withdraw_uzbek_bank_kb()
        )
    else:
        await state.set_state(S.WD_WAIT_CARD)
        await cb.message.edit_text(
            "✅ <b>Страна выбрана</b>\n\n"
            f"🌍 Страна: {country_name}\n\n"
            "💳 Теперь введите номер банковской карты для получения средств:\n\n"
            "<i>Пример: 2200 1234 5678 9010</i>"
        )
    await cb.answer()
@router.callback_query(F.data.startswith("uzbek_bank_"), S.WD_WAIT_UZBEK_BANK)
async def wd_uzbek_bank_select(cb: CallbackQuery, state: FSMContext):
    bank_map = {
        "uzbek_bank_kaspi": "Каспи банк",
        "uzbek_bank_halyk": "Халык Банк",
        "uzbek_bank_other": "Другой банк Узбекистана"
    }
    bank_key = cb.data
    bank_name = bank_map.get(bank_key, "Неизвестный банк")
    await state.update_data(bank=bank_name, country="Узбекистан")
    await state.set_state(S.WD_WAIT_CARD)
    await cb.message.edit_text(
        "✅ <b>Банк выбран</b>\n\n"
        f"🌍 Страна: Узбекистан\n"
        f"🏦 Банк: {bank_name}\n\n"
        "💳 Теперь введите номер банковской карты для получения средств:\n\n"
        "<i>Пример: 8600 1234 5678 9010</i>"
    )
    await cb.answer()
async def get_usd_uzs_rate() -> float:
    return await fetch_usd_price("USDTUZS")
async def fetch_usd_price(ticker: str) -> float:
    now = time.time()
    if ticker in _exchange_rate_cache:
        cache_data = _exchange_rate_cache[ticker]
        if cache_data["rate"] > 0 and (now - cache_data["timestamp"]) < CACHE_TTL:
            logger.debug(f"✅ Используем кэшированную цену {ticker}: {cache_data['rate']:.2f}")
            return cache_data["rate"]
    session = None  
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        session = aiohttp.ClientSession(timeout=timeout)
        url = "https://api.binance.com/api/v3/ticker/price"
        async with session.get(url, params={"symbol": ticker}) as resp:
            if resp.status != 200:
                raise RuntimeError(f"bad status {resp.status}")
            data = await resp.json()
            p = float(data.get("price"))
            if p > 0:
                _exchange_rate_cache[ticker] = {
                    "rate": p, 
                    "timestamp": now
                }
                logger.info(f"✅ Получена цена {ticker} с Binance: {p:.2f}")
                return p
            else:
                raise ValueError("Invalid price received")
    except Exception as e:
        logger.error(f"❌ Ошибка получения цены {ticker} с Binance: {e}")
        if ticker in _exchange_rate_cache and _exchange_rate_cache[ticker]["rate"] > 0:
            cached_rate = _exchange_rate_cache[ticker]["rate"]
            logger.warning(f"⚠️ Используем устаревший кэш {ticker}: {cached_rate:.2f}")
            return cached_rate
        try:
            if ticker == "ETHUSDT":
                env_rate = os.getenv("RATE_ETH_USD", "0")
                rate = float(env_rate)
                if rate > 0:
                    logger.info(f"✅ Используем цену ETH из переменных окружения: {rate:.2f}")
                    return rate
            if ticker == "BTCUSDT":
                env_rate = os.getenv("RATE_BTC_USD", "0") 
                rate = float(env_rate)
                if rate > 0:
                    logger.info(f"✅ Используем цену BTC из переменных окружения: {rate:.2f}")
                    return rate
        except Exception as env_error:
            logger.error(f"❌ Ошибка парсинга переменных окружения для {ticker}: {env_error}")
    finally:
        if session and not session.closed:
            await session.close()
    logger.warning(f"⚠️ Не удалось получить цену {ticker}, возвращаем 0.0")
    return 0.0
async def get_crypto_price(symbol: str) -> Optional[float]:
    price = await fetch_usd_price(symbol)
    return price if price > 0 else None
def clear_cache(ticker: str = None):
    global _exchange_rate_cache
    if ticker:
        if ticker in _exchange_rate_cache:
            _exchange_rate_cache[ticker] = {"rate": 0.0, "timestamp": 0}
            logger.info(f"✅ Кэш для {ticker} очищен")
    else:
        for ticker_key in _exchange_rate_cache:
            _exchange_rate_cache[ticker_key] = {"rate": 0.0, "timestamp": 0}
        logger.info("✅ Весь кэш курсов очищен")
def get_cache_info() -> dict:
    now = time.time()
    info = {}
    for ticker, data in _exchange_rate_cache.items():
        age = now - data["timestamp"] if data["timestamp"] > 0 else float('inf')
        info[ticker] = {
            "rate": data["rate"],
            "age_seconds": age,
            "is_valid": age < CACHE_TTL and data["rate"] > 0
        }
    return info
async def get_eth_price() -> float:
    return await fetch_usd_price("ETHUSDT")
async def get_btc_price() -> float:
    return await fetch_usd_price("BTCUSDT")
async def convert_usd_to_rub(usd_amount: float) -> float:
    rate = await get_usd_rub_rate()
    return usd_amount * rate
async def convert_usd_to_uzs(usd_amount: float) -> float:
    rate = await get_usd_uzs_rate()
    return usd_amount * rate
@router.message(S.WD_WAIT_CARD)
async def wd_wait_card(m: Message, state: FSMContext):
    card_number = m.text.strip()
    clean_card = re.sub(r'\s+', '', card_number)
    if len(clean_card) < 16 or not clean_card.isdigit():
        await m.answer("❌ Неверный формат номера карты. Пожалуйста, введите 16-значный номер карты:")
        return
    formatted_card = f"{clean_card[:4]} {clean_card[4:8]} {clean_card[8:12]} {clean_card[12:16]}"
    data = await state.get_data()
    fio = data.get('fio', 'Не указано')
    bank = data.get('bank', 'Не указан')
    country = data.get('country', 'Россия')
    user = await store.get_user(m.from_user.id)
    if country == "Узбекистан":
        usd_uzs_rate = await get_usd_uzs_rate()
        amount_usd = user.balance
        amount_local = amount_usd * usd_uzs_rate
        currency_symbol = "UZS"
        rate = usd_uzs_rate
    else:
        usd_rub_rate = await get_usd_rub_rate()
        amount_usd = user.balance
        amount_local = amount_usd * usd_rub_rate
        currency_symbol = "RUB"
        rate = usd_rub_rate
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Вывести {amount_local:.0f} {currency_symbol}", callback_data="wd_card_confirm_all")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="wd_card_cancel")]
    ])
    await m.answer(
        "✅ <b>Данные для вывода получены</b>\n\n"
        f"🌍 Страна: {country}\n"
        f"👤 ФИО: {fio}\n"
        f"🏦 Банк: {bank}\n"
        f"💳 Карта: {mask_card(formatted_card)}\n\n"
        f"💰 Доступно для вывода: {amount_local:.0f} {currency_symbol} (${amount_usd:.2f})\n"
        f"📊 Курс: 1 USD = {rate:.2f} {currency_symbol}\n\n"
        "Подтвердите вывод всего баланса:",
        reply_markup=confirm_kb
    )
    await state.update_data(
        card_number=clean_card, 
        formatted_card=formatted_card,
        currency_symbol=currency_symbol,
        exchange_rate=rate,
        country=country
    )
@router.callback_query(F.data == "wd_card_confirm_all")
async def wd_card_confirm_all(cb: CallbackQuery, state: FSMContext):
    user = await store.get_user(cb.from_user.id)
    if user.balance <= 0:
        await cb.answer("❌ На балансе недостаточно средств", show_alert=True)
        return
    data = await state.get_data()
    fio = data.get('fio', 'Не указано')
    bank = data.get('bank', 'Не указан')
    country = data.get('country', 'Россия')
    card_number = data.get('card_number', '')
    formatted_card = data.get('formatted_card', '')
    exchange_rate = data.get('exchange_rate', 0)
    currency_symbol = data.get('currency_symbol', 'RUB')
    amount_usd = user.balance
    if country == "Узбекистан":
        amount_local = amount_usd * exchange_rate
    else:
        amount_local = amount_usd * exchange_rate
    user.balance = 0
    await store.save_user(user)
    pending_data = {
        "user_id": cb.from_user.id, 
        "username": cb.from_user.username, 
        "amount": amount_usd,
        "amount_local": amount_local,
        "currency_symbol": currency_symbol,
        "exchange_rate": exchange_rate,
        "country": country,
        "fio": fio,
        "bank": bank,
        "card_number": card_number,
        "formatted_card": formatted_card,
        "method": "bank_card",
        "status": "PENDING",
        "timestamp": time.time(),
        "event_id": gen_event_id()
    }
    await store.increment_withdrawals(cb.from_user.id, amount_usd) 
    await store.update_user_activity(cb.from_user.id)  
    await store.add_pending_item(cb.from_user.id, pending_data)  
    await store.set_wd_pending(cb.from_user.id, pending_data) 
    try:
        user_language = await get_user_language(cb.from_user.id)
        bot_lang = "en" if user_language == "en" else "ru"
        owner = await store.get_bot_owner(cb.from_user.id) 
        support_event = {
            "type": "withdraw_request_card",
            "event_id": pending_data['event_id'],
            "user_id": cb.from_user.id,  
            "username": cb.from_user.username or str(cb.from_user.id),
            "amount": amount_usd,
            "amount_local": amount_local,
            "currency_symbol": currency_symbol,
            "exchange_rate": exchange_rate,
            "country": country,
            "fio": fio,
            "bank": bank,
            "card_number": card_number,
            "formatted_card": formatted_card,
            "bot": bot_lang,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot_owner_id": owner or cb.from_user.id, 
        }
        await store.push_support_event(support_event)
        logger.info("Withdraw card event sent to support queue: %s", pending_data['event_id'])
    except Exception as e:
        logger.error("Failed to send withdraw card event to support queue: %s", e)
    await cb.message.edit_text(
        f"✅ <b>Запрос на вывод принят!</b>\n\n"
        f"💵 Сумма: {amount_local:.0f} {currency_symbol} (${amount_usd:.2f})\n"
        f"🌍 Страна: {country}\n"
        f"👤 ФИО: {fio}\n"
        f"🏦 Банк: {bank}\n"
        f"💳 Карта: {mask_card(formatted_card)}\n\n"
        "Ваши средства будут переведены в течение 24 часов. Ожидайте."
    )
    await state.set_state(S.IDLE)
    await cb.answer()
@router.callback_query(F.data == "wd_card_cancel")
async def wd_card_cancel(cb: CallbackQuery, state: FSMContext):
    await state.set_state(S.IDLE)
    await cb.message.edit_text("❌ Вывод на банковскую карту отменен")
    await cb.answer()
@router.callback_query(F.data == "wd_method_crypto")
async def wd_method_crypto(cb: CallbackQuery, state: FSMContext):
    try:
        try:
            await _init_trading_bot_username_once()
            owner = await store.get_bot_owner(cb.from_user.id)
            support_event = {
                "type": "withdraw_crypto_selected",
                "event_id": f"withdraw_crypto_{cb.from_user.id}_{int(time.time() * 1000)}",
                "user_id": cb.from_user.id,
                "username": cb.from_user.username or str(cb.from_user.id),
                "first_name": cb.from_user.first_name or "",
                "last_name": cb.from_user.last_name or "",
                "bot_username": TRADING_BOT_USERNAME,
                "timestamp": time.time(),
                "bot": "ru" if await get_user_language(cb.from_user.id) == "ru" else "en"
            }
            await store.push_support_event(support_event)
            logger.info(f"вЬЕ Withdraw crypto selected event sent to support queue: {support_event}")
        except Exception as e:
            logger.error(f"вЭМ Failed to send withdraw_crypto_selected event to support queue: {e}")
        await state.set_state(S.WD_CHOOSE_TOKEN)
        title_text = await get_localized_text(cb.from_user.id, "withdraw_crypto_title")
        choose_token_text = await get_localized_text(cb.from_user.id, "withdraw_choose_token")
        text = f"{title_text}\n\n{choose_token_text}"
        kb = await get_localized_kb(cb.from_user.id, "withdraw_token")
        try:
            await cb.message.delete()
        except Exception as delete_error:
            logger.warning(f"Could not delete previous message: {delete_error}")
        await cb.message.answer(text, reply_markup=kb)
        await cb.answer()
    except TelegramForbiddenError:
        logger.info(f"User {cb.from_user.id} blocked bot during crypto withdrawal")
        await send_bot_blocked_event(cb.from_user.id, "withdraw_crypto_failed")
        await store.remove_watcher(cb.from_user.id)
    except TelegramRetryAfter as e:
        delay = float(getattr(e, "retry_after", 1.0))
        logger.warning(f"Flood control for user {cb.from_user.id}: {delay}s")
        await cb.answer(f"Please wait {int(delay)} seconds...", show_alert=True)
    except Exception as e:
        logger.exception(f"Unexpected error in wd_method_crypto: {e}")
        await cb.answer("An unexpected error occurred", show_alert=True)
@router.callback_query(F.data.startswith("wd_token:"), S.WD_CHOOSE_TOKEN)
async def wd_token_select(cb: CallbackQuery, state: FSMContext):
    token = cb.data.split(":")[1]
    try:
        await _init_trading_bot_username_once()
        token_display = "USDT" if token == "USDT" else ("ETHEREUM" if token == "ETH" else "BITCOIN")
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "withdraw_token_selected",
            "event_id": f"withdraw_token_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "token": token,
            "token_display": token_display,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(cb.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Withdraw token selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send withdraw_token_selected event to support queue: {e}")
    await store.set_wd_token(cb.from_user.id, token)
    await state.set_state(S.WD_CHOOSE_NETWORK)
    title_text = await get_localized_text(cb.from_user.id, "withdraw_crypto_title")
    choose_network_text = await get_localized_text(cb.from_user.id, "withdraw_choose_network")
    text = f"{title_text}\n\n{choose_network_text}"
    kb = await get_localized_kb(cb.from_user.id, "withdraw_network", token=token)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("wd_net:"), S.WD_CHOOSE_NETWORK)
async def wd_network_select(cb: CallbackQuery, state: FSMContext):
    network = cb.data.split(":")[1]
    try:
        await _init_trading_bot_username_once()
        token = await store.get_wd_token(cb.from_user.id) or "USDT"
        token_display = "USDT" if token == "USDT" else ("ETHEREUM" if token == "ETH" else "BITCOIN")
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "withdraw_network_selected",
            "event_id": f"withdraw_network_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "token": token,
            "token_display": token_display,
            "network": network,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(cb.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Withdraw network selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send withdraw_network_selected event to support queue: {e}")
    await store.set_wd_network(cb.from_user.id, network)
    await state.set_state(S.WD_WAIT_WALLET)
    token = await store.get_wd_token(cb.from_user.id) or "USDT"
    token_display = "USDT" if token == "USDT" else ("ETHEREUM" if token == "ETH" else "BITCOIN")
    title_text = await get_localized_text(cb.from_user.id, "withdraw_crypto_title")
    enter_wallet_text = await get_localized_text(cb.from_user.id, "withdraw_enter_wallet")
    text = (
        f"{title_text}\n\n"
        f"{enter_wallet_text}\n\n"
        f"<i>Enter the address in the next message</i>"
    )
    await cb.message.edit_text(text)
    await cb.answer()
@router.message(S.WD_WAIT_WALLET)
async def wd_wallet_address(m: Message, state: FSMContext):
    wallet_address = m.text.strip()
    if len(wallet_address) < 10:
        error_text = "❌ Invalid address format. Please enter a valid wallet address:"
        if await is_english_user(m.from_user.id):
            error_text = "❌ Invalid address format. Please enter a valid wallet address:"
        else:
            error_text = "❌ Неверный формат адреса. Пожалуйста, введите корректный адрес кошелька:"
        await m.answer(error_text)
        return
    await store.set_wd_address(m.from_user.id, wallet_address)
    await state.set_state(S.WD_WAIT_AMOUNT)
    token = await store.get_wd_token(m.from_user.id) or "USDT"
    network = await store.get_wd_network(m.from_user.id) or "TRC20"
    user = await store.get_user(m.from_user.id)
    token_display = "USDT" if token == "USDT" else ("ETH" if token == "ETH" else "BTC")
    available_balance = user.balance
    display_balance = f"${available_balance:.2f}"
    if token in ("ETH", "ETHEREUM"):
        eth_price = await fetch_usd_price("ETHUSDT")
        if eth_price > 0:
            eth_amount = available_balance / eth_price
            display_balance = f"{eth_amount:.6f} ETH (${available_balance:.2f})"
    elif token in ("BTC", "BITCOIN"):
        btc_price = await fetch_usd_price("BTCUSDT")
        if btc_price > 0:
            btc_amount = available_balance / btc_price
            display_balance = f"{btc_amount:.8f} BTC (${available_balance:.2f})"
    wallet_accepted_text = await get_localized_text(m.from_user.id, "withdraw_wallet_accepted")
    enter_amount_text = await get_localized_text(m.from_user.id, "withdraw_enter_amount")
    withdraw_title = await get_localized_text(m.from_user.id, "withdraw_crypto_title")
    kb = await get_localized_kb(m.from_user.id, "withdraw_amount", token=token, balance=available_balance)
    await m.answer(
        f"✅ {wallet_accepted_text}: <code>{wallet_address}</code>\n\n"
        f"💵 {withdraw_title}\n"
        f"Available for withdrawal: {display_balance}\n\n"
        f"{enter_amount_text}:",
        reply_markup=kb
    )
@router.callback_query(F.data == "wd_amount_all", S.WD_WAIT_AMOUNT)
async def wd_amount_all(cb: CallbackQuery, state: FSMContext):
    user = await store.get_user(cb.from_user.id)
    amount = user.balance

    token = await store.get_wd_token(cb.from_user.id) or "USDT" 
    if amount <= 0:
        error_text = await get_localized_text(cb.from_user.id, "insufficient_funds")
        await cb.answer(error_text, show_alert=True)
        return
    display_amount = amount
    if token in ("ETH", "ETHEREUM"):
        eth_price = await fetch_usd_price("ETHUSDT")
        if eth_price > 0:
            display_amount = amount / eth_price
    elif token in ("BTC", "BITCOIN"):
        btc_price = await fetch_usd_price("BTCUSDT")
        if btc_price > 0:
            display_amount = amount / btc_price
    await process_withdraw_request(cb, amount, state)
@router.callback_query(F.data == "wd_cancel")
async def wd_cancel(cb: CallbackQuery, state: FSMContext):
    await state.set_state(S.IDLE)
    await store.clear_withdraw_flow(cb.from_user.id)
    cancel_text = await get_localized_text(cb.from_user.id, "withdraw_cancelled")
    if not cancel_text:
        cancel_text = "Withdrawal cancelled"
    await cb.message.edit_text(f"❌ {cancel_text}")
    await cb.answer()
async def process_withdraw_request(cb: CallbackQuery, amount: float, state: FSMContext):
    user = await store.get_user(cb.from_user.id)
    token = await store.get_wd_token(cb.from_user.id) or "USDT"
    network = await store.get_wd_network(cb.from_user.id) or "TRC20"
    address = await store.get_wd_address(cb.from_user.id) or ""
    crypto_amount = amount
    display_amount_text = f"${amount:.2f}"
    if token in ("ETH", "ETHEREUM"):
        eth_price = await fetch_usd_price("ETHUSDT")
        if eth_price > 0:
            crypto_amount = amount / eth_price
            display_amount_text = f"{crypto_amount:.6f} ETH (${amount:.2f})"
    elif token in ("BTC", "BITCOIN"):
        btc_price = await fetch_usd_price("BTCUSDT")
        if btc_price > 0:
            crypto_amount = amount / btc_price
            display_amount_text = f"{crypto_amount:.8f} BTC (${amount:.2f})"
    if amount > user.balance:
        error_text = await get_localized_text(cb.from_user.id, "insufficient_funds")
        await cb.answer(error_text, show_alert=True)
        return
    user.balance -= amount
    await store.save_user(user)
    pending_data = {
        "user_id": cb.from_user.id,
        "username": cb.from_user.username,
        "amount": amount,
        "crypto_amount": crypto_amount,
        "token": token,
        "network": network,
        "address": address,
        "status": "PENDING",
        "timestamp": time.time(),
        "event_id": gen_event_id()
    }
    await store.increment_withdrawals(cb.from_user.id, amount)
    await store.update_user_activity(cb.from_user.id)
    await store.add_pending_item(cb.from_user.id, pending_data)
    await store.set_wd_pending(cb.from_user.id, pending_data)
    try:
        user_language = await get_user_language(cb.from_user.id)
        bot_lang = "en" if user_language == "en" else "ru"
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "withdraw_request",
            "event_id": pending_data['event_id'],
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "amount": amount,
            "crypto_amount": crypto_amount,
            "display_amount": display_amount_text,
            "token": token,
            "network": network,
            "address": address,
            "bot": bot_lang, 
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time()
        }
        await store.push_support_event(support_event)
        logger.info("Withdraw event sent to support queue: %s", pending_data['event_id'])    
    except Exception as e:
        logger.error("Failed to send withdraw event to support queue: %s", e)
    success_text = await get_localized_text(cb.from_user.id, "withdraw_request_sent")
    await cb.message.edit_text(
        f"✅ {success_text}\n\n"
        f"Amount: {display_amount_text}\n"
        f"Network: {network}\n"
        f"Address: <code>{address}</code>\n\n"
        f"Your funds will be withdrawn within 24 hours. Please wait."
    )
    await state.set_state(S.IDLE)
    await cb.answer()
@router.callback_query(F.data.startswith("admin_wd_approve:"))
async def admin_wd_approve(cb: CallbackQuery):
    event_id = cb.data.split(":")[1]
    pending_found = None
    uid = None
    all_users = await store.r.keys("user:*")
    for user_key in all_users:
        try:
            user_id = int(user_key.decode().split(":")[1])
            pending_list = await store.list_pending_items(user_id)
            for pending in pending_list:
                if pending.get("event_id") == event_id and pending.get("status") == "PENDING":
                    pending_found = pending
                    uid = user_id
                    break
            if pending_found:
                break
        except Exception:
            continue
    if not pending_found or not uid:
        await cb.answer("Запрос не найден или уже обработан")
        return
    pending_found["status"] = "APPROVED"
    pending_found["approved_at"] = time.time()
    pending_found["approved_by"] = cb.from_user.id
    await store.add_pending_item(uid, pending_found)
    try:
        user_language = await get_user_language(uid)
        if user_language == "en":
            message_text = (
                f"✅ <b>Withdrawal Approved</b>\n\n"
                f"Your withdrawal request for ${pending_found['amount']:.2f} "
                f"(~{pending_found.get('amount_local', 0):.0f} {pending_found.get('currency_symbol', 'RUB')}) "
                f"has been approved.\n\n"
                f"Funds will be transferred to your card within 1-3 business days."
            )
        else:
            message_text = (
                f"✅ <b>Вывод подтверждён</b>\n\n"
                f"Ваша заявка на вывод ${pending_found['amount']:.2f} "
                f"(~{pending_found.get('amount_local', 0):.0f} {pending_found.get('currency_symbol', 'RUB')}) "
                f"подтверждена.\n\n"
                f"Средства будут переведены на вашу карту в течение 1-3 рабочих дней."
            )
        owner = await store.get_bot_owner(m.from_user.id)
        token = await store.get_user_bot_token(owner)
        trb = Bot(token=token)
        await trb.send_message(
            chat_id=uid,
            text=message_text
        )
    except Exception as e:
        logger.error(f"Failed to notify user about approved withdraw: {e}")
    await cb.message.edit_text(
        f"✅ <b>Вывод подтвержден</b>\n\n"
        f"👤 Пользователь: @{pending_found.get('username', 'N/A')}\n"
        f"💵 Сумма: ${pending_found['amount']:.2f}\n"
        f"✅ Статус: ВЫПОЛНЕНО"
    )
    await cb.answer("Вывод подтвержден")
@router.callback_query(F.data.startswith("admin_wd_reject_requisites:"))
async def admin_wd_reject_requisites(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_wd_reject_requisites:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"Admin rejecting withdraw with requisites request: event_id={event_id}")
        pending_found = None
        uid = None        
        all_users = await store.r.keys("user:*")
        for user_key in all_users:
            try:
                user_id = int(user_key.decode().split(":")[1])
                pending_list = await store.list_pending_items(user_id)
                for pending in pending_list:
                    if pending.get("event_id") == event_id and pending.get("status") == "PENDING":
                        pending_found = pending
                        uid = user_id
                        break
                if pending_found:
                    break
            except Exception:
                continue
        if not pending_found or not uid:
            await cb.answer("Запрос не найден", show_alert=True)
            return        
        if pending_found.get("status") != "PENDING":
            await cb.answer("Запрос уже обработан", show_alert=True)
            return        
        user = await store.get_user(uid)
        original_balance = user.balance  
        if not pending_found.get("refund_processed"):
            user.balance += pending_found["amount"]
            await store.save_user(user)
            pending_found["refund_processed"] = True
            logger.info(f"✅ Средства возвращены на баланс пользователя {uid}: +${pending_found['amount']:.2f}")        
        pending_found["status"] = "REJECTED_REQUISITES"
        pending_found["rejected_at"] = time.time()
        pending_found["rejected_by"] = cb.from_user.id
        pending_found["reject_reason"] = "Для вывода укажите реквизиты кошелька, с которого пополняли счёт"
        await store.add_pending_item(uid, pending_found)        
        support_bot_username, support_bot_url = await get_support_bot_info(user_id)
        token = pending_found.get("token", "")
        address = pending_found.get("address", "")
        amount = pending_found["amount"]        
        token_info = f"\n💎 Токен: {token}" if token else ""
        address_info = f"\n📮 Адрес: {address}" if address else ""
        await safe_send_text(
            uid,
            f"❌ <b>Вывод отклонен</b>\n\n"
            f"Ваша заявка на вывод ${amount:.2f} отклонена.\n\n"
            f"<b>Причина:</b> Для вывода укажите реквизиты кошелька, с которого пополняли счёт."
            f"{token_info}"
            f"{address_info}\n\n"
            f"💰 <b>Средства возвращены на ваш баланс!</b>\n\n"
            f"Свяжитесь с поддержкой: @{support_bot_username}"
        )        
        await cb.message.edit_text(
            f"❌ <b>Вывод отклонен - запрошены реквизиты</b>\n\n"
            f"👤 Пользователь: @{pending_found.get('username', 'N/A')}\n"
            f"💵 Сумма: ${pending_found['amount']:.2f}\n"
            f"📝 Статус: ОТКЛОНЕНО (запрошены реквизиты)\n"
            f"💰 Баланс до: ${original_balance:.2f}\n"
            f"💰 Баланс после: ${user.balance:.2f}\n"
            f"✅ Средства возвращены на баланс пользователя"
        )
        await cb.answer("Запрошены реквизиты, средства возвращены")
    except Exception as e:
        logger.error(f"Error in admin_wd_reject_requisites: {e}")
        await cb.answer("Ошибка при обработке", show_alert=True)
async def show_assets(chat_id: int, uid: int | None = None):
    if uid is None:
        uid = chat_id
    user = await store.get_user(uid)
    unreal = await calc_unrealized(store, uid)
    positions = await store.list_positions(uid)
    balance_text = await get_localized_text(uid, "assets_balance")
    positions_text = await get_localized_text(uid, "open_positions_count")
    pnl_text = await get_localized_text(uid, "unrealized_pnl")
    text = (
        f"{balance_text}: ${user.balance + unreal:.2f} \n"
        f"{positions_text}: {len(positions)}\n"
        f"{pnl_text}: {fmt_money(unreal)}\n"
    )
    assets_kb = await get_assets_keyboard(uid)
    owner = await store.get_bot_owner(uid)
    token = await store.get_user_bot_token(owner)
    trb = Bot(token=token)
    msg = await trb.send_message(chat_id=chat_id, text=text, reply_markup=assets_kb)
    await store.set_assets_msg(uid, msg.message_id)
    spawn(live_update_assets(chat_id, uid, msg.message_id, duration_sec=60), name="live_update_assets")
def clear_cache(ticker: str = None):
    global _exchange_rate_cache
    if ticker:
        ticker_key = ticker.lower()
        if ticker_key in _exchange_rate_cache:
            _exchange_rate_cache[ticker_key] = {"rate": 0.0, "timestamp": 0}
            logger.info(f"✅ Кэш для {ticker} очищен")
    else:
        _exchange_rate_cache = EXCHANGE_RATE_CACHE_DEFAULT.copy()
        logger.info("✅ Весь кэш курсов очищен")
async def _close_leftover_open_positions():
    start_time = time.time()
    closed = 0
    processed_keys = 0
    try:
        batch_size = 100  
        position_keys = []
        async for key in store.r.scan_iter(match="position:*", count=batch_size):
            position_keys.append(key)
            processed_keys += 1
        logger.info(f"Найдено ключей позиций: {len(position_keys)}, обработано итераций: {processed_keys}")
        if not position_keys:
            logger.info("Нет позиций для обработки")
            return
        pipe = store.r.pipeline()
        for key in position_keys:
            pipe.get(key)
        raw_positions = await pipe.execute()
        tasks = []
        for i, raw in enumerate(raw_positions):
            if not raw:
                continue
            try:
                data = json.loads(raw)
                p = Position(**data)
            except Exception as e:
                logger.warning(f"Ошибка парсинга позиции из ключа {position_keys[i]}: {e}")
                continue
            if p.status != PosStatus.OPEN:
                continue
            task = _process_single_position(p)
            tasks.append(task)
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            closed = sum(1 for r in results if isinstance(r, bool) and r)
        elapsed = time.time() - start_time
        logger.info(f"Очистка завершена: закрыто {closed} позиций за {elapsed:.2f} сек")
    except Exception as e:
        logger.error(f"Ошибка при очистке позиций: {e}")
        logger.exception("Детали ошибки:")
async def _process_single_position(p: Position) -> bool:
    try:
        price_now = p.price_now or p.entry_price
        if p.side == Side.LONG:
            pnl_pct = (price_now - p.entry_price) / p.entry_price * p.leverage * 100.0
        else:
            pnl_pct = (p.entry_price - price_now) / p.entry_price * p.leverage * 100.0
        pnl_abs = p.order_amount * pnl_pct / 100.0
        user = await store.get_user(p.user_id)
        if user:
            before_balance = user.balance
            user.balance += pnl_abs
            await store.save_user(user)
            asyncio.create_task(
                _send_balance_update_event(p.user_id, before_balance, user.balance, "trade_pnl"),
                name=f"balance_update_{p.user_id}"
            )
        h = TradeHistory(
            position_id=p.id,
            symbol=p.symbol,
            side=p.side,
            entry=p.entry_price,
            exit=price_now,
            pnl_abs=round(pnl_abs, 2),
            pnl_pct=round(pnl_pct, 2),
            closed_by="TIME",
            closed_at=time.time(),
        )
        await asyncio.gather(
            store.add_history(p.user_id, h),
            store.push_outcome(p.user_id, "W" if pnl_abs >= 0 else "L"),
            store.remove_position(p.user_id, p.id),
            return_exceptions=True  
        )
        asyncio.create_task(
            _notify_user_position_closed(p.user_id, p.symbol, p.side.value, pnl_abs, pnl_pct),
            name=f"notify_{p.user_id}"
        )
        return True
    except Exception as e:
        logger.error(f"Ошибка обработки позиции {p.id}: {e}")
        return False
async def _send_balance_update_event(user_id: int, before: float, after: float, reason: str):
    try:
        await support_emit({
            "type": "balance_update",
            "user_id": user_id,
            "username": None,
            "before": before,
            "after": after,
            "reason": reason
        })
    except Exception as e:
        logger.warning(f"Не удалось отправить событие баланса для пользователя {user_id}: {e}")
async def _notify_user_position_closed(user_id: int, symbol: str, side: str, pnl_abs: float, pnl_pct: float):
    try:
        await safe_send_text(
            user_id,
            f"⛔️ Позиция закрыта: {symbol} {side} | PnL: ${pnl_abs:.2f} ({pnl_pct:.2f}%)"
        )
    except Exception as e:
        logger.warning(f"Не удалось уведомить пользователя {user_id}: {e}")
@router.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    await ensure_bot_initialized()
    try:
        await bot.get_chat(m.from_user.id)
    except TelegramForbiddenError:
        logger.info(f"User {m.from_user.id} blocked the bot, skipping start command")
        try:
            await send_bot_blocked_event(m.from_user.id, "start_command")
            await store.remove_watcher(m.from_user.id)
        except Exception as e:
            logger.error(f"Failed to send bot_blocked event in on_start: {e}")
        return  
    except Exception:
        pass
    ref_code = None
    if m.text and len(m.text.split()) > 1:
        ref_code = m.text.split()[1].strip()
    try:
        await store.update_user_profile(
            user_id=m.from_user.id,
            username=m.from_user.username,
            first_name=m.from_user.first_name,
            last_name=m.from_user.last_name,
            language_code=m.from_user.language_code,
        )
    except Exception:
        pass
    try:
        await store.update_user_activity(m.from_user.id)
    except Exception:
        pass
    existed = await r.get(RKeys.user(m.from_user.id))
    u = await store.get_user(m.from_user.id)
    if not existed:
        u.language_code = "en"
        await store.save_user(u)
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(m.from_user.id)
        if not existed:
            event_type = "user_started_bot"
            event_id = f"start_{m.from_user.id}_{int(time.time() * 1000)}"
        else:
            event_type = "user_returned"
            event_id = f"return_{m.from_user.id}_{int(time.time() * 1000)}"
        start_event = {
            "type": event_type,
            "event_id": event_id,
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "is_new_user": not existed,
            "was_blocked": False,  
            "ref_code": ref_code,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "language_code": u.language_code or "unknown",
            "bot": "en" if u.language_code == "en" else "ru",  # Используем язык пользователя
        }
        await store.push_support_event(start_event)
        logger.info(
            f"✅ {event_type} event sent to support: "
            f"user_id={m.from_user.id}, is_new={not existed}, language={u.language_code}"
        )
    except Exception as e:
        logger.error(f"❌ Failed to send start event to support: {e}")
    user_language = u.language_code or "en" 
    if existed and user_language:
        if user_language == "en":
            balance_text = (
                f"Your balance: ${u.balance:.2f}\n"
                f"Leverage: x{u.leverage}, order amount ${u.order_amount:.2f}.\n\n"
                f"Choose action below ⤵️"
            )
        else:
            balance_text = (
                f"Ваш баланс: ${u.balance:.2f}\n"
                f"Плечо: x{u.leverage}, сумма ордера ${u.order_amount:.2f}.\n\n"
                f"Выберите действие ниже ⤵️"
            )
        menu_kb = get_main_menu_kb(user_language)
        await m.answer(balance_text, reply_markup=menu_kb)
        await state.set_state(S.IDLE)
        return
    if user_language == "en":
        balance_text = (
            f"Your balance: ${u.balance:.2f}\n"
            f"Leverage: x{u.leverage}, order amount ${u.order_amount:.2f}.\n\n"
            f"Choose action below ⤵️"
        )
    else:
        balance_text = (
            f"Ваш баланс: ${u.balance:.2f}\n"
            f"Плечо: x{u.leverage}, сумма ордера ${u.order_amount:.2f}.\n\n"
            f"Выберите действие ниже ⤵️"
        )
    menu_kb = get_main_menu_kb(user_language)
    try:
        await m.answer_photo(
            photo="https://i.ibb.co/7JWyRRdp/94af51c3330e.jpg",
            caption=balance_text,
            reply_markup=menu_kb,
        )
    except TelegramForbiddenError:
        logger.info(f"User {m.from_user.id} blocked bot during photo send")
        await send_bot_blocked_event(m.from_user.id, "photo_send_failed")
        await store.remove_watcher(m.from_user.id)
        return
    except Exception:
        try:
            await m.answer(balance_text, reply_markup=menu_kb)
        except TelegramForbiddenError:
            logger.info(f"User {m.from_user.id} blocked bot during text send")
            await send_bot_blocked_event(m.from_user.id, "text_send_failed")
            await store.remove_watcher(m.from_user.id)
            return
    await state.set_state(S.IDLE)
    if not existed:
        try:
            await support_emit(
                {
                    "type": "user_registered",
                    "user_id": m.from_user.id,
                    "username": m.from_user.username,
                    "language": user_language,
                }
            )
        except Exception:
            pass
@router.message(F.text == "🇷🇺 Русский", S.CHOOSING_LANGUAGE)
async def on_russian_selected(m: Message, state: FSMContext):
    u = await store.get_user(m.from_user.id)
    u.language_code = "ru"
    await store.save_user(u)
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "language_selected",
            "user_id": m.from_user.id,
            "username": m.from_user.username,
            "first_name": m.from_user.first_name,
            "last_name": m.from_user.last_name,
            "bot_username": TRADING_BOT_USERNAME,
            "language": "ru",
            "timestamp": time.time(),
        }
        await store.push_support_event(support_event)
        logger.info(f"language_selected (RU) event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"Failed to send language_selected (RU) event: {e}")
    balance_text = (
        f"Ваш баланс: ${u.balance:.2f}\n"
        f"Плечо: x{u.leverage}, сумма ордера ${u.order_amount:.2f}.\n\n"
        f"Выберите действие ниже ⤵️"
    )
    await m.answer(balance_text, reply_markup=get_main_menu_kb("ru"))
    await state.set_state(S.IDLE)
@router.message(F.text == "🇺🇸 English", S.CHOOSING_LANGUAGE)
async def on_english_selected(m: Message, state: FSMContext):
    u = await store.get_user(m.from_user.id)
    u.language_code = "en"
    await store.save_user(u)
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "language_selected",
            "user_id": m.from_user.id,
            "username": m.from_user.username,
            "first_name": m.from_user.first_name,
            "last_name": m.from_user.last_name,
            "bot_username": TRADING_BOT_USERNAME,
            "language": "en",
            "timestamp": time.time(),
        }
        await store.push_support_event(support_event)
        logger.info(f"language_selected (EN) event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"Failed to send language_selected (EN) event: {e}")
    balance_text = await get_localized_text(m.from_user.id, "balance_welcome", balance=u.balance)
    await m.answer(balance_text, reply_markup=get_main_menu_kb("en"))
    await state.set_state(S.IDLE)
def main_menu_kb_english() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Assets"), KeyboardButton(text="Open Positions")],
            [KeyboardButton(text="Trade History"), KeyboardButton(text="AI Trading")],
            [KeyboardButton(text="Settings")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Choose action…",
    )
@router.message(F.text.in_(["AI Трейдинг", "AI Trading"]))
async def ai_trading(m: Message, state: FSMContext):
    user_id = m.from_user.id
    bot_owner_id = await store.get_bot_owner(user_id)
    if not bot_owner_id:
        bot_owner_id = user_id
        await store.set_bot_owner(user_id, bot_owner_id)
    signal_channel_id = await store.get_tenant_signal_channel(bot_owner_id)
    if not signal_channel_id:
        signal_channel_id = SIGNAL_CHANNEL_ID
    if user_id == bot_owner_id:
        pass
    else:
        if not await is_bot_available_for_user(user_id):
            error_text = await get_localized_text(user_id, "bot_unavailable")
            menu_kb = get_main_menu_kb(await get_user_language(user_id))
            await m.answer(error_text, reply_markup=menu_kb)
            return
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(m.from_user.id)
        support_event = {
            "type": "ai_trading_started",
            "event_id": f"ai_trading_start_{user_id}_{int(time.time() * 1000)}",
            "user_id": user_id,
            "bot_owner_id": bot_owner_id,
            "signal_channel_id": signal_channel_id,
            "username": m.from_user.username or str(user_id),
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(user_id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ AI Trading started event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send ai_trading_started event to support queue: {e}")
    await state.set_state(S.WATCHING_SIGNALS)
    await store.add_watcher(user_id)
    enabled_text = await get_localized_text(user_id, "ai_trading_enabled")
    searching_text = await get_localized_text(user_id, "ai_trading_searching")
    stop_text = await get_localized_text(user_id, "ai_trading_stop")
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=stop_text, callback_data="stop_watch")]]
    )  
    await m.answer(f"{enabled_text}. {searching_text}", reply_markup=kb)
@router.callback_query(F.data == "stop_watch")
async def stop_watch(cb: CallbackQuery, state: FSMContext):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "ai_trading_stopped",
            "event_id": f"ai_trading_stop_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(cb.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ AI Trading stopped event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send ai_trading_stopped event to support queue: {e}")
    stopped_text = await get_localized_text(cb.from_user.id, "ai_trading_stopped")   
    await state.set_state(S.IDLE)
    await store.remove_watcher(cb.from_user.id)
    await cb.message.edit_text(stopped_text)
    await cb.answer()
async def cleanup_inactive_watchers():
    while True:
        try:
            watchers = await store.list_active_watchers()  
            logger.info(f"Watchers cleanup: {len(watchers)} active watchers")
        except Exception as e:
            logger.error(f"Error in watchers cleanup: {e}")
        await asyncio.sleep(3600)
@channel_router.channel_post()
async def on_channel_post(msg: Message):
    logger.info(f"=== CHANNEL POST RECEIVED ===")
    channel_owner_id = None
    all_tenants = await get_all_tenants()
    for owner_id in all_tenants:
        tenant_channel = await store.get_tenant_signal_channel(owner_id)
        if tenant_channel == msg.chat.id:
            channel_owner_id = owner_id
            break
    if not channel_owner_id:
        if msg.chat.id != SIGNAL_CHANNEL_ID:
            logger.warning(f"IGNORING - Unknown channel ID: {msg.chat.id}")
            return
        channel_owner_id = "main"
    if msg.date and msg.date.replace(tzinfo=timezone.utc) < BOT_START_TIME:
        logger.warning("IGNORING - Old message")
        return
    if not msg.text and not msg.caption:
        logger.warning("IGNORING - No text or caption in message")
        return
    text = msg.text or msg.caption or ""
    ps = parse_signal(text)
    if not ps:
        logger.warning("❌ Signal parsing failed")
        return
    logger.info(f"✅ SUCCESSFULLY PARSED SIGNAL: {ps.symbol} {ps.tf} side={ps.side} entry={ps.entry} tp={ps.tp} sl={ps.sl} rec_amount={ps.rec_amount}")
    try:
        first_time = await store.mark_signal_message(msg.chat.id, msg.message_id)
        if not first_time:
            logger.info(f"IGNORING - Duplicate channel message ({msg.chat.id},{msg.message_id})")
            return
    except Exception as e:
        logger.error(f"Error marking signal message: {e}")
    try:
        fp = signal_fingerprint(ps)
        first_fp = await store.mark_signal_fingerprint(fp)
        if not first_fp:
            logger.info(f"IGNORING - Duplicate signal fingerprint: {fp}")
            return
    except Exception as e:
        logger.error(f"Error marking signal fingerprint: {e}")
    if channel_owner_id == "main":
        watchers = await store.list_active_watchers()
        logger.info(f"📤 Broadcasting from main channel to {len(watchers)} watchers")
    else:
        watchers = await store.get_tenant_users(channel_owner_id)
        active_watchers = []
        for uid in watchers:
            if await store.r.sismember(RKeys.watchers(), str(uid).encode()):
                active_watchers.append(uid)
        watchers = active_watchers
        logger.info(f"📤 Broadcasting from tenant {channel_owner_id} channel to {len(watchers)} watchers")
    if not watchers:
        logger.info(f"No active watchers found for owner {channel_owner_id}, skipping signal broadcast")
        return
    logger.info(f"📤 Broadcasting signal to {len(watchers)} active watcher(s) for owner {channel_owner_id}")
    successful_sends = 0
    failed_users = []
    for uid in watchers:
        try:
            user_lang = await get_user_language(uid)
            is_english = (user_lang == "en")
            strength_disp = ps.strength
            if ps.strength:
                s_up = ps.strength.upper()
                if is_english:
                    strength_map = {
                        "СИЛЬНЫЙ": "STRONG",
                        "СРЕДНИЙ": "MEDIUM", 
                        "СЛАБЫЙ": "WEAK",
                        "НИЗКИЙ": "LOW",
                    }
                    strength_disp = strength_map.get(s_up, ps.strength)
                else:
                    if s_up in ("СИЛЬНЫЙ", "СРЕДНИЙ", "СЛАБЫЙ", "НИЗКИЙ"):
                        strength_disp = s_up
            if is_english:
                header = "🛰 <b>New signal</b>" if not strength_disp else f"🛰 <b>New signal — {strength_disp}</b>"
                card_text_local = (
                    f"{header}\n"
                    f"• Pair: {ps.symbol}\n"
                    f"• Timeframe: {ps.tf}\n"
                    f"• Direction: {'🟢' if ps.side==Side.LONG else '🔴'} {ps.side.value}\n"
                    f"• Entry price: {ps.entry}\n"
                    f"• TP: {ps.tp} | SL: {ps.sl}\n"
                    f"• Recommended amount: ${ps.rec_amount}\n"
                    f"• Date: {ps.date_utc}"
                )
            else:
                header = "🛰 <b>Новый сигнал</b>" if not strength_disp else f"🛰 <b>Новый сигнал — {strength_disp}</b>"
                card_text_local = (
                    f"{header}\n"
                    f"• Пара: {ps.symbol}\n"
                    f"• Таймфрейм: {ps.tf}\n"
                    f"• Направление: {'🟢' if ps.side==Side.LONG else '🔴'} {ps.side.value}\n"
                    f"• Цена входа: {ps.entry}\n"
                    f"• TP: {ps.tp} | SL: {ps.sl}\n"
                    f"• Реком. сумма: ${ps.rec_amount}\n"
                    f"• Дата: {ps.date_utc}"
                )
            countdown_seconds = 10
            last = await store.get_last_signal_msg(uid)
            if last:
                l_chat, l_msg = last
                try:
                    await bot.delete_message(chat_id=uid, message_id=l_msg)
                except Exception:
                    pass  
                await store.clear_last_signal_msg(uid)
            if msg.photo:
                logger.info(f"Sending photo signal to user {uid} of owner {channel_owner_id}")
                text_with_timer = card_text_local + (
                    f"\n\n⏳ Time left: {countdown_seconds}…" if is_english else f"\n\n⏳ Осталось: {countdown_seconds}…"
                )
                s = await bot.send_photo(
                    chat_id=uid,
                    photo=msg.photo[-1].file_id,
                    caption=text_with_timer,
                    reply_markup=open_market_kb(is_english),
                )
            else:
                logger.info(f"Sending text signal to user {uid} of owner {channel_owner_id}")
                text_with_timer = card_text_local + (
                    f"\n\n⏳ Time left: {countdown_seconds}…" if is_english else f"\n\n⏳ Осталось: {countdown_seconds}…"
                )
                owner = await store.get_bot_owner(uid)
                token = await store.get_user_bot_token(owner)
                trb = Bot(token=token)
                s = await trb.send_message(
                    chat_id=uid,
                    text=text_with_timer,
                    reply_markup=open_market_kb(is_english),
                )
            await store.set_last_signal_msg(uid, s.chat.id, s.message_id)
            await store.set_last_signal_data(uid, ps)
            spawn(
                countdown_and_cleanup(
                    uid,
                    s.chat.id,
                    s.message_id,
                    card_text_local,
                    bool(msg.photo),
                    countdown_seconds,
                    is_english,
                ),
                name="countdown_and_cleanup",
            )
            successful_sends += 1
            logger.info(f"✅ Signal sent to user {uid} of owner {channel_owner_id}")
        except Exception as e:
            logger.error(f"❌ Failed to send signal to user {uid}: {e}")
            failed_users.append(uid)
            error_msg = str(e).lower()
            blocked_phrases = [
                "bot was blocked", 
                "user is deactivated", 
                "chat not found",
                "forbidden: bot was blocked", 
                "bot was kicked"
            ]
            if any(phrase in error_msg for phrase in blocked_phrases):
                await store.remove_watcher(uid)
                bot_owner_id = await store.get_bot_owner(uid)
                await send_bot_blocked_event(uid, f"signal_delivery_failed: {error_msg}")
                logger.info(f"Auto-removed watcher {uid} due to delivery failure (blocked)")
    logger.info(f"=== SIGNAL BROADCAST COMPLETED for owner {channel_owner_id}: {successful_sends} successful, {len(failed_users)} failed ===")
async def get_all_tenants() -> List[int]:
    pattern = "tenant:*:signal_channel"
    keys = await store.r.keys(pattern)
    tenants = []
    for key in keys:
        owner_id = int(key.split(":")[1])
        tenants.append(owner_id)
    return tenants
async def is_tenant(user_id: int) -> bool:
    tenant_users = await store.get_tenant_users(user_id)
    return len(tenant_users) > 0
@router.message(Command("watchers"))
async def check_watchers(m: Message):
    watchers = await store.list_active_watchers()
    await m.answer(f"Active watchers: {len(watchers)}\n{watchers}")
async def countdown_and_cleanup(
    uid: int,
    chat_id: int,
    msg_id: int,
    base_text: str,
    is_photo: bool,
    seconds: int = 10,
    is_english: bool = False,
):
    try:
        left = seconds
        key = (chat_id, msg_id)
        while left > 0:
            if is_english:
                txt = base_text + f"\n\n⏳ Time left: {left}…"
            else:
                txt = base_text + f"\n\n⏳ Осталось: {left}…"
            now = time.time()
            wait = max(0.0, NEXT_COUNTDOWN_AT.get(key, 0.0) - now)
            if wait > 0:
                await asyncio.sleep(min(wait, 1.0))
            try:
                if is_photo:
                    await bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=msg_id,
                        caption=txt,
                        reply_markup=open_market_kb(is_english),
                    )
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=msg_id,
                        text=txt,
                        reply_markup=open_market_kb(is_english),
                    )
                await asyncio.sleep(1)
                left -= 1
            except TelegramRetryAfter as e:
                delay = float(getattr(e, "retry_after", 1.0))
                NEXT_COUNTDOWN_AT[key] = time.time() + delay
                skip = max(1, int(delay))
                left = max(0, left - skip)
                await asyncio.sleep(delay)
            except TelegramBadRequest:
                await asyncio.sleep(1)
                left -= 1
            except Exception:
                logger.exception("countdown edit failed")
                await asyncio.sleep(1)
                left -= 1
        try:
            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg_id,
                reply_markup=disabled_open_kb(is_english),
            )
            await asyncio.sleep(1)
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
    except Exception:
        logger.exception("countdown_and_cleanup crashed")
    finally:
        try:
            await store.clear_last_signal_msg(uid)
        except Exception:
            pass
@router.callback_query(F.data == "open_market")
async def on_open_market(cb: CallbackQuery, state: FSMContext):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "open_market_clicked",
            "event_id": f"open_market_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(cb.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Open market clicked event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send open_market_clicked event to support queue: {e}")
    opening_text = await get_localized_text(cb.from_user.id, "open_order")
    await cb.answer(opening_text)
    try:
        await cb.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete signal message: {e}")
    await state.set_state(S.ORDER_OPENING)
    try:
        loading = await cb.message.answer(opening_text)
    except Exception as e:
        logger.error(f"Failed to send loading message: {e}")
        loading = None
    await asyncio.sleep(random.uniform(1.0, 3.0))
    user = await store.get_user(cb.from_user.id)
    required_margin = float(user.order_amount) / float(max(1, user.leverage))
    if user.balance < required_margin:
        try:
            if loading:
                await loading.delete()
        except Exception:
            pass
        insufficient_text = await get_localized_text(cb.from_user.id, "insufficient_funds")
        deposit_text = await get_localized_text(cb.from_user.id, "deposit")
        user_language = await get_user_language(cb.from_user.id)
        if user_language == "en":
            text_insufficient = (
                f"❗️ {insufficient_text}\n"
                f"Required margin: ${required_margin:.2f}. Current balance: ${user.balance:.2f}."
            )
        else:
            text_insufficient = (
                f"❗️ {insufficient_text}\n"
                f"Требуемая маржа: ${required_margin:.2f}. Текущий баланс: ${user.balance:.2f}."
            )
        kb_insufficient = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=deposit_text, callback_data="deposit")]]
        )
        await safe_send_text(
            chat_id=cb.message.chat.id,
            text=text_insufficient,
            user_id=cb.from_user.id,
            reply_markup=kb_insufficient
        )
        await state.set_state(S.IDLE)
        return
    ps = await store.get_last_signal_data(cb.from_user.id)
    if ps is None:
        ps = ParsedSignal(
            symbol="AVNTUSDT",
            tf="5m",
            side=Side.SHORT,
            entry=1.1189,
            sl=1.1394,
            tp=1.0779,
            rec_amount=10,
            date_utc=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        )
    pid = f"{cb.from_user.id}:{int(time.time()*1000)}:{random.randint(1000,9999)}"
    max_dur = random.randint(60, 600)
    position = Position(
        id=pid,
        user_id=cb.from_user.id,
        symbol=ps.symbol,
        side=ps.side,
        entry_price=ps.entry,
        tp=ps.tp,
        sl=ps.sl,
        leverage=user.leverage,
        order_amount=float(user.order_amount),
        margin_used=float(user.order_amount) / float(user.leverage),
        opened_at=time.time(),
        max_duration_sec=max_dur,
        price_now=ps.entry,
        last_tick_at=time.time(),
    )
    await store.add_position(position)
    order_opened_text = await get_localized_text(cb.from_user.id, "order_opened")
    user_language = await get_user_language(cb.from_user.id)
    if user_language == "en":
        opened_text = (
            f"✅ {order_opened_text}: {position.symbol} {position.side.value}\n"
            f"Amount: ${position.order_amount:.2f} | Leverage: x{position.leverage}\n"
            f"Entry: {position.entry_price} | TP: {position.tp} | SL: {position.sl}\n"
            f"PNL: $0.00 (0.00%) — updating…"
        )
    else:
        opened_text = (
            f"✅ {order_opened_text}: {position.symbol} {position.side.value}\n"
            f"Сумма: ${position.order_amount:.2f} | Плечо: x{position.leverage}\n"
            f"Вход: {position.entry_price} | TP: {position.tp} | SL: {position.sl}\n"
            f"PNL: $0.00 (0.00%) — обновляется…"
        )
    if loading:
        try:
            await loading.delete()
        except Exception:
            pass
    msg = await safe_send_text(
        chat_id=cb.message.chat.id,
        text=opened_text,
        user_id=cb.from_user.id
    )
    if msg is None:
        logger.error(f"Failed to send position opened message to user {cb.from_user.id}")
        error_text = "❌ Error opening position. Please try again." if user_language == "en" else "❌ Ошибка при открытии позиции. Попробуйте еще раз."
        await cb.answer(error_text, show_alert=True)
        await store.remove_position(cb.from_user.id, position.id)
        await state.set_state(S.IDLE)
        return
    try:
        await support_emit({
            "type": "position_opened",
            "user_id": position.user_id,
            "username": None,
            "symbol": position.symbol,
            "side": position.side.value if hasattr(position.side, "value") else str(position.side),
            "qty": position.order_amount / max(position.entry_price, 1e-9),
            "entry": position.entry_price,
            "tp": position.tp,
            "sl": position.sl,
            "risk": getattr(position, "risk_pct", None)
        })
    except Exception as e:
        logger.error(f"Failed to emit position opened event: {e}")
    outcome = await choose_outcome(store, cb.from_user.id)
    spawn(
        run_position_loop(bot, store, position, msg.chat.id, msg.message_id),
        name=f"run_position_loop_{pid}"
    )
    await state.set_state(S.IDLE)
async def calc_unrealized(store: Store, uid: int) -> float:
    positions = await store.list_positions(uid)
    return sum(p.pnl_current for p in positions)
@router.message(F.text.contains("💰 Средства возвращены на ваш баланс"))
async def handle_balance_refund_from_support(m: Message):
    try:
        user_id = m.from_user.id
        text = m.text
        import re
        patterns = [
            r'Возврат на ваш счёт: \$([\d.]+)',
            r'Возврат: \$([\d.]+)',
            r'Сумма возврата: \$([\d.]+)',
            r'Зачислено: \$([\d.]+)'
        ]
        amount = None
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                amount = float(match.group(1))
                break
        if amount is None:
            logger.warning(f"❌ Не удалось извлечь сумму возврата из сообщения: {text}")
            await m.answer("❌ Не удалось распознать сумму возврата. Обратитесь в поддержку.")
            return
        user = await store.get_user(user_id)
        old_balance = user.balance
        user.balance += amount
        await store.save_user(user)
        logger.info(f"✅ Баланс пользователя {user_id} обновлен: +${amount:.2f}, старый: ${old_balance:.2f}, новый: ${user.balance:.2f}")
        await m.answer(
            f"✅ <b>Баланс успешно обновлен!</b>\n\n"
            f"💰 Зачислено: ${amount:.2f}\n"
            f"💳 Старый баланс: ${old_balance:.2f}\n"
            f"💳 Новый баланс: ${user.balance:.2f}\n\n"
            f"<i>Обновление произведено автоматически</i>"
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке возврата средств: {e}")
        await m.answer("❌ Произошла ошибка при обновлении баланса. Обратитесь в поддержку.")
@router.message(F.text.contains("💳 Новый баланс:"))
async def handle_balance_update_from_support(m: Message):
    try:
        user_id = m.from_user.id
        text = m.text
        import re
        balance_match = re.search(r'Новый баланс: \$([\d.]+)', text)
        if not balance_match:
            logger.warning(f"❌ Не удалось извлечь новый баланс из сообщения: {text}")
            return
        new_balance = float(balance_match.group(1))
        user = await store.get_user(user_id)
        old_balance = user.balance
        user.balance = new_balance
        await store.save_user(user)
        logger.info(f"✅ Баланс пользователя {user_id} обновлен поддержкой: старый: ${old_balance:.2f}, новый: ${user.balance:.2f}")
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке обновления баланса от поддержки: {e}")
@router.message(Command("update_balance"))
async def force_update_balance(m: Message):
    try:
        user_id = m.from_user.id
        user = await store.get_user(user_id)
        await m.answer(
            f"💰 <b>Текущий баланс</b>\n\n"
            f"💳 Баланс: ${user.balance:.2f}\n\n"
            f"Если баланс не совпадает с ожидаемым, обратитесь в поддержку."
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при принудительном обновлении баланса: {e}")
        await m.answer("❌ Ошибка при получении баланса")
async def process_balance_updates():
    logger.info("🔄 Запуск process_balance_updates...")
    while True:
        try:
            queues = ["trading:balance_updates", "balance_updates", "trading:notify:ru", "trading:notify:en"]
            logger.debug(f"🔍 Проверка очередей: {queues}")
            for queue_name in queues:
                event_data = await store.r.brpop(queue_name, timeout=1)
                if event_data:
                    _, event_json = event_data
                    event = json.loads(event_json)
                    logger.info(f"🎯 Получено событие из очереди {queue_name}: {event}")
                    if event.get("type") == "balance_credit":
                        user_id = event.get("user_id")
                        amount = event.get("amount")
                        logger.info(f"💰 Processing balance_credit for user {user_id}: amount=${amount}")
                        if user_id and amount is not None:
                            user = await store.get_user(user_id)
                            old_balance = user.balance
                            user.balance += float(amount)
                            await store.save_user(user)
                            logger.info(f"✅ Баланс пользователя {user_id} обновлен через balance_credit: ${old_balance:.2f} -> ${user.balance:.2f}")
                            try:
                                owner = await store.get_bot_owner(user_id)
                                token = await store.get_user_bot_token(owner)
                                trb = Bot(token=token)
                                await trb.send_message(
                                    chat_id=user_id,
                                    text=f"✅ <b>Баланс обновлен!</b>\n\n💳 Новый баланс: ${user.balance:.2f}"
                                )
                            except Exception as e:
                                logger.warning(f"Не удалось уведомить пользователя {user_id}: {e}")
                        break                  
                    elif event.get("type") in ["balance_update", "balance_update_from_support"]:
                        user_id = event.get("user_id")
                        amount = event.get("amount")
                        new_balance = event.get("new_balance")
                        if user_id and (amount is not None or new_balance is not None):
                            user = await store.get_user(user_id)
                            if new_balance is not None:
                                old_balance = user.balance
                                user.balance = float(new_balance)
                                await store.save_user(user)
                                logger.info(f"✅ Баланс пользователя {user_id} установлен: ${old_balance:.2f} -> ${user.balance:.2f}")
                            elif amount is not None:
                                old_balance = user.balance
                                user.balance += float(amount)
                                await store.save_user(user)
                                logger.info(f"✅ Баланс пользователя {user_id} увеличен: +${amount:.2f}, было: ${old_balance:.2f}, стало: ${user.balance:.2f}")
                            try:
                                owner = await store.get_bot_owner(user_id)  
                                token = await store.get_user_bot_token(owner)
                                trb = Bot(token=token)
                                await trb.send_message(
                                    chat_id=user_id,
                                    text=f"✅ <b>Баланс обновлен!</b>\n\n💳 Новый баланс: ${user.balance:.2f}"
                                )
                            except Exception as e:
                                logger.warning(f"Не удалось уведомить пользователя {user_id}: {e}")
                    break  
        except Exception as e:
            logger.error(f"❌ Ошибка в process_balance_updates: {e}")
        await asyncio.sleep(1)
async def process_notify_events():
    while True:
        try:
            event_data = await store.r.brpop("trading:notify:ru", timeout=1)
            if event_data:
                _, event_json = event_data
                event = json.loads(event_json)
                logger.info(f"🔍 Processing notify event: {event}")
                if event.get("type") == "payment_requisites_requested":
                    user_id = event.get("user_id")
                    event_id = event.get("event_id")
                    try:
                        owner = await store.get_bot_owner(m.from_user.id)
                        token = await store.get_user_bot_token(owner)
                        trb = Bot(token=token)
                        await trb.send_message(
                            chat_id=user_id,
                            text="📋 <b>Запрос реквизитов</b>\n\n"
                                 "Администратор запросил реквизиты кошелька, с которого вы пополняли счёт.\n\n"
                                 "Пожалуйста, отправьте адрес кошелька в ответном сообщении."
                        )
                    except Exception as e:
                        logger.error(f"Failed to send requisites request to user {user_id}: {e}")
            event_data_en = await store.r.brpop("trading:notify:en", timeout=0.5)
            if event_data_en:
                _, event_json = event_data_en
                event = json.loads(event_json)
                logger.info(f"🔍 Processing EN notify event: {event}")
                if event.get("type") == "payment_requisites_requested":
                    user_id = event.get("user_id")
                    try:
                        owner = await store.get_bot_owner(m.from_user.id)
                        token = await store.get_user_bot_token(owner)
                        trb = Bot(token=token)
                        await trb.send_message(
                            chat_id=user_id,
                            text="📋 <b>Requisites Request</b>\n\n"
                                 "Administrator requested the wallet address you used for deposit.\n\n"
                                 "Please send the wallet address in reply message."
                        )
                    except Exception as e:
                        logger.error(f"Failed to send requisites request to user {user_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Error in process_notify_events: {e}")
        await asyncio.sleep(1)
async def start_background_tasks():
    asyncio.create_task(check_active_users_blocked_status(), name="blocked_status_checker")
    asyncio.create_task(process_balance_updates(), name="balance_updates_processor")
    asyncio.create_task(process_notify_events(), name="notify_events_processor")
    asyncio.create_task(cleanup_inactive_watchers(), name="watchers_cleanup")
    logger.info("✅ Все фоновые задачи запущены")
@router.message(Command("debug_user"))
async def debug_user(m: Message):
    user = await store.get_user(m.from_user.id)
    raw_data = await store.r.get(RKeys.user(m.from_user.id))
    redis_balance = "N/A"
    if raw_data:
        try:
            redis_data = json.loads(raw_data)
            redis_balance = redis_data.get('balance', 'N/A')
        except:
            redis_balance = "Error parsing"
    await m.answer(
        f"🔍 <b>Debug User Info</b>\n\n"
        f"👤 User ID: {m.from_user.id}\n"
        f"💳 Balance in object: ${user.balance:.2f}\n"
        f"📊 Balance in Redis: {redis_balance}\n"
        f"🆔 Last activity: {user.last_activity}\n"
        f"📝 Username: {user.username}"
    )
@router.message(F.text.in_(["Активы", "Assets"]))
async def on_assets(m: Message):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(m.from_user.id)
        support_event = {
            "type": "assets_opened",
            "event_id": f"assets_{m.from_user.id}_{int(time.time() * 1000)}", 
            "user_id": m.from_user.id,
            "username": m.from_user.username,
            "first_name": m.from_user.first_name,
            "last_name": m.from_user.last_name,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
        }
        await store.push_support_event(support_event)
        logger.info(f"assets_opened event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"Failed to send assets_opened event to support queue: {e}")
    user = await store.get_user(m.from_user.id)
    unreal = await calc_unrealized(store, m.from_user.id)
    positions = await store.list_positions(m.from_user.id)
    balance_text = await get_localized_text(m.from_user.id, "assets_balance")
    positions_text = await get_localized_text(m.from_user.id, "open_positions_count")
    pnl_text = await get_localized_text(m.from_user.id, "unrealized_pnl")
    caption = (
        f"{balance_text}: ${user.balance + unreal:.2f} \n"
        f"{positions_text}: {len(positions)}\n"
        f"{pnl_text}: {fmt_money(unreal)}"
    )
    assets_kb = await get_assets_keyboard(m.from_user.id)
    try:
        msg = await m.answer_photo(
            photo=ASSETS_IMAGE_URL,
            caption=caption,
            reply_markup=assets_kb
        )
    except Exception as photo_error:
        logger.warning(f"Could not send photo: {photo_error}. Falling back to text.")
        msg = await m.answer(
            text=f"рЯТ∞ <b>Активы</b>\n\n{caption}",
            reply_markup=assets_kb
        )
    await store.set_assets_msg(m.from_user.id, msg.message_id)
    spawn(live_update_assets(m.chat.id, m.from_user.id, msg.message_id, duration_sec=60), name="live_update_assets")
@router.callback_query(F.data == "open_assets")
async def cb_open_assets(cb: CallbackQuery):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "assets_opened",
            "event_id": f"assets_{cb.from_user.id}_{int(time.time() * 1000)}",  
            "user_id": cb.from_user.id,
            "username": cb.from_user.username,
            "first_name": cb.from_user.first_name,
            "last_name": cb.from_user.last_name,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
        }
        await store.push_support_event(support_event)
        logger.info(f"assets_opened event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"Failed to send assets_opened event to support queue: {e}")
    try:
        await cb.answer()
    except Exception:
        pass
    try:
        await show_assets(cb.message.chat.id, cb.from_user.id)
    except Exception:
        logger.exception("open_assets handler failed")
async def get_assets_keyboard(uid: int) -> InlineKeyboardMarkup:
    is_english = await is_english_user(uid)
    if is_english:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Deposit", callback_data="deposit"),
                 InlineKeyboardButton(text="Withdraw", callback_data="withdraw")],
            ]
        )
    else:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Пополнить", callback_data="deposit"),
                 InlineKeyboardButton(text="Вывести", callback_data="withdraw")],
            ]
        )
def get_deposit_methods_kb(is_english: bool = False) -> InlineKeyboardMarkup:
    if is_english:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Cryptocurrency", callback_data="dep_crypto")],
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Криптовалюта", callback_data="dep_crypto")],
        ])
def get_withdraw_methods_kb(is_english: bool = False) -> InlineKeyboardMarkup:
    if is_english:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Cryptocurrency", callback_data="wd_method_crypto")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="open_assets")]
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Криптовалюта", callback_data="wd_method_crypto")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="open_assets")]
        ])
async def live_update_open_positions(chat_id: int, uid: int, msg_id: int, duration_sec: int = 60):
    until = time.time() + duration_sec
    while time.time() < until:
        try:
            positions = await store.list_positions(uid)
            if not positions:
                try:
                    await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="Открытых позиций нет.")
                except Exception:
                    pass
                return
            lines = ["📈 Открытые позиции:"]
            for p in positions:
                try:
                    pct = (p.pnl_current / max(1e-9, p.order_amount)) * 100.0
                except Exception:
                    pct = 0.0
                lines.append(f"• {p.symbol} {p.side.value} — {fmt_money(p.pnl_current)} ({pct:+.2f}%)")
            text = "\n".join(lines)
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
        except TelegramBadRequest:
            pass
        except Exception:
            logger.exception("live_update_open_positions: failed to refresh")
        await asyncio.sleep(1)
async def live_update_assets(chat_id: int, uid: int, msg_id: int, duration_sec: int = 60):
    try:
        stop_ts = time.time() + duration_sec
        while time.time() < stop_ts:
            cur_msg_id = await store.get_assets_msg(uid)
            if cur_msg_id is None or cur_msg_id != msg_id:
                break
            try:
                user = await store.get_user(uid)
                unreal = await calc_unrealized(store, uid)
                positions = await store.list_positions(uid)
                balance_text = await get_localized_text(uid, "assets_balance")
                positions_text = await get_localized_text(uid, "open_positions_count")
                pnl_text = await get_localized_text(uid, "unrealized_pnl")
                text = (
                    f"{balance_text}: ${user.balance + unreal:.2f} \n"
                    f"{positions_text}: {len(positions)}\n"
                    f"{pnl_text}: {fmt_money(unreal)}\n"
                )
                assets_kb = await get_assets_keyboard(uid)
                await bot.edit_message_text(
                    chat_id=chat_id, 
                    message_id=msg_id, 
                    text=text, 
                    reply_markup=assets_kb
                )
            except TelegramBadRequest:
                pass
            await asyncio.sleep(1)
    except Exception:
        logger.exception("live_update_assets crashed")
    finally:
        await store.clear_assets_msg(uid)
async def assets_inline_kb(uid: int) -> InlineKeyboardMarkup:
    return await get_assets_keyboard(uid)
@router.callback_query(F.data == "deposit")
async def on_deposit(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id) 
        support_event = {
            "type": "deposit_opened",
            "event_id": f"deposit_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username,
            "first_name": cb.from_user.first_name,
            "last_name": cb.from_user.last_name,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
        }
        await store.push_support_event(support_event)
        logger.info(f"deposit_opened event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"Failed to send deposit_opened event to support queue: {e}")
    text = await get_localized_text(cb.from_user.id, "deposit_choose_method")
    kb = await get_localized_kb(cb.from_user.id, "deposit_methods")
    try:
        await cb.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete message: {e}")
    await cb.message.answer(text, reply_markup=kb)
    await cb.answer()
async def set_card_temp(event_id: str, data: dict) -> None:
    try:
        await r.setex(f"card_temp:{event_id}", 7200, json.dumps(data))
        logger.info(f"✅ Card temp data saved: {event_id}")
    except Exception as e:
        logger.error(f"❌ Failed to save card temp data: {e}")
async def set_dep_card_temp(uid: int, data: dict) -> None:
    try:
        await r.setex(f"user:{uid}:dep_card_temp", 7200, json.dumps(data))
        logger.info(f"✅ User card temp data saved: user_id={uid}")
    except Exception as e:
        logger.error(f"❌ Failed to save user card temp data: {e}")
async def get_card_temp(event_id: str) -> Optional[dict]:
    raw = await r.get(f"card_temp:{event_id}")
    return json.loads(raw) if raw else None
async def clear_card_temp(event_id: str) -> None:
    await r.delete(f"card_temp:{event_id}")
async def get_dep_card_temp(uid: int) -> Optional[dict]:
    raw = await r.get(f"user:{uid}:dep_card_temp")
    return json.loads(raw) if raw else None
async def clear_dep_card_temp(uid: int) -> None:
    await r.delete(f"user:{uid}:dep_card_temp")
@router.callback_query(F.data.startswith("dep_card_amt:"))
async def dep_card_amount(cb: CallbackQuery, state: FSMContext):
    amt = int(cb.data.split(":")[1])
    min_deposit = await store.get_user_min_deposit(cb.from_user.id)
    if min_deposit > 0 and amt < min_deposit:
        await cb.answer(
            f"❌ Минимальная сумма пополнения: ${min_deposit:.2f}", 
            show_alert=True
        )
        return
    usd_rub_rate = await get_usd_rub_rate()
    amount_rub = amt * usd_rub_rate
    event_id = gen_event_id()
    temp_data = {
        'event_id': event_id,
        'amount': amt,
        'amount_rub': amount_rub,
        'usd_rub_rate': usd_rub_rate,
        'user_id': cb.from_user.id,
        'username': cb.from_user.username or str(cb.from_user.id),
        'timestamp': time.time()
    }
    await set_card_temp(event_id, temp_data)
    await set_dep_card_temp(cb.from_user.id, temp_data)
    await state.update_data(event_id=event_id)
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "deposit_amount_selected",
            "event_id": f"dep_card_amount_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "amount": amt,
            "amount_rub": amount_rub,
            "usd_rub_rate": usd_rub_rate,
            "token": "RUB",
            "token_display": "RUB",
            "method": "bank_card",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru"  
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Deposit card amount selected event sent to support queue: ${amt} (RUB {amount_rub:.0f})")
    except Exception as e:
        logger.error(f"❌ Failed to send deposit_card_amount_selected event to support queue: {e}")
    fio_text = await get_localized_text(cb.from_user.id, "deposit_enter_fio")
    amount_text = await get_localized_text(cb.from_user.id, "deposit_amount_display", 
                                         amount_rub=amount_rub, amount_usd=amt, rate=usd_rub_rate)
    example_text = await get_localized_text(cb.from_user.id, "deposit_fio_example")
    text = (
        f"{fio_text}\n\n"
        f"{amount_text}\n\n"
        f"<i>{example_text}</i>"
    )
    await state.set_state(S.DEP_CARD_FIO)
    await cb.message.edit_text(text)
    await cb.answer()
async def find_payment_data(event_id: str, user_id: int = None) -> Optional[dict]:
    try:
        original_event_id = event_id.replace('_', ':')
        safe_event_id = event_id
        logger.info(f"🔍 Searching payment data: {original_event_id}")
        temp_data = await get_card_temp(original_event_id)
        if not temp_data:
            temp_data = await get_card_temp(safe_event_id)
        if not temp_data and user_id:
            user_temp_data = await get_dep_card_temp(user_id)
            if user_temp_data and user_temp_data.get('event_id') in [original_event_id, safe_event_id]:
                temp_data = user_temp_data
        if not temp_data:
            for search_id in [original_event_id, safe_event_id]:
                try:
                    support_temp_key = f"support:card_temp:{search_id}"
                    raw = await r.get(support_temp_key)
                    if raw:
                        temp_data = json.loads(raw)
                        break
                except Exception:
                    continue
        if temp_data:
            logger.info(f"✅ Payment data found for {original_event_id}")
        else:
            logger.error(f"❌ Payment data not found for {original_event_id}")
        return temp_data
    except Exception as e:
        logger.error(f"❌ Error in find_payment_data: {e}")
        return None
@router.message(S.DEP_CARD_FIO)
async def process_dep_card_fio(m: Message, state: FSMContext):
    fio = m.text.strip()
    if len(fio.split()) < 2:
        await m.answer("❌ Пожалуйста, введите полное ФИО (имя и фамилию)")
        return
    data = await state.get_data()
    event_id = data.get('event_id')
    if not event_id:
        await m.answer("❌ Сессия истекла. Начните пополнение заново.")
        await state.clear()
        return
    temp_data = await get_card_temp(event_id)
    if not temp_data:
        await m.answer("❌ Сессия истекла. Начните пополнение заново.")
        await state.clear()
        return
    temp_data['fio'] = fio
    await set_card_temp(event_id, temp_data)
    await state.set_state(S.DEP_CARD_BANK)
    banks_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сбербанк", callback_data="bank_sber")],
        [InlineKeyboardButton(text="Тинькофф", callback_data="bank_tinkoff")],
        [InlineKeyboardButton(text="Альфа-Банк", callback_data="bank_alpha")],
        [InlineKeyboardButton(text="ВТБ", callback_data="bank_vtb")],
        [InlineKeyboardButton(text="Другой банк", callback_data="bank_other")],
    ])
    await m.answer(
        "🏦 <b>Выберите ваш банк</b>\n\n"
        "Выберите банк, с карты которого будет производиться пополнение:",
        reply_markup=banks_kb
    )
@router.callback_query(F.data.startswith("bank_"), S.DEP_CARD_BANK)
async def process_dep_card_bank(cb: CallbackQuery, state: FSMContext):
    bank_map = {
        "bank_sber": "Сбербанк",
        "bank_tinkoff": "Тинькофф", 
        "bank_alpha": "Альфа-Банк",
        "bank_vtb": "ВТБ",
        "bank_other": "Другой банк"
    }
    bank_key = cb.data
    bank_name = bank_map.get(bank_key, "Неизвестный банк")
    await state.update_data(bank=bank_name)
    if bank_key == "bank_other":
        await state.set_state(S.DEP_WAIT_COUNTRY)
        await cb.message.edit_text(
            "🌍 <b>Выберите страну</b>\n\n"
            "Выберите страну, в которой находится ваш банк:",
            reply_markup=deposit_country_kb()
        )
    else:
        await process_deposit_final_step(cb, state, bank_name, "Россия")
    await cb.answer()
    data = await state.get_data()
    event_id = data.get('event_id')
    if not event_id:
        await cb.answer("❌ Сессия истекла. Начните пополнение заново.")
        await state.clear()
        return
    temp_data = await get_card_temp(event_id)
    if not temp_data:
        await cb.answer("❌ Сессия истекла. Начните пополнение заново.")
        await state.clear()
        return
    temp_data['bank'] = bank_name
    await set_card_temp(event_id, temp_data)
    amount_usd = temp_data['amount']
    amount_rub = temp_data['amount_rub']
    usd_rub_rate = temp_data['usd_rub_rate']
    await send_card_deposit_to_support(temp_data, amount_rub)
    await cb.message.edit_text(
        f"✅ <b>Запрос на пополнение отправлен!</b>\n\n"
        f"💵 Сумма: <b>{amount_rub:.0f} RUB (${amount_usd})</b>\n"
        f"📊 Курс: 1 USD = {usd_rub_rate:.2f} RUB\n"
        f"👤 ФИО: {temp_data['fio']}\n"
        f"🏦 Банк: {bank_name}\n\n"
        f"⏳ Ожидайте реквизиты для оплаты."
    )
    await state.clear()
    await clear_card_temp(event_id)
    await cb.answer()
_exchange_rate_cache = {
    "usd_rub": {"rate": 0.0, "timestamp": 0},
    "usd_uzs": {"rate": 0.0, "timestamp": 0},
    "eth_usdt": {"rate": 0.0, "timestamp": 0},
    "btc_usdt": {"rate": 0.0, "timestamp": 0}
}
CACHE_TTL = 300  
async def get_usd_rub_rate() -> float:
    return await fetch_usd_price("USDTRUB")
async def send_card_deposit_to_support(temp_data: dict, amount_local: float, currency_symbol: str):
    event_id = temp_data["event_id"]
    user_id = temp_data["user_id"]
    try:
        user_lang = await get_user_language(user_id)  
    except Exception:
        user_lang = "ru"
    bot_code = "en" if user_lang == "en" else "ru"
    logger.info(f"Sending card deposit to support: {temp_data}, bot_code={bot_code}, bot_username={TRADING_BOT_USERNAME}")
    owner = await store.get_bot_owner(user_id)
    ev = {
        "event_id": event_id,
        "type": "card",
        "bot": bot_code,                       
        "bot_username": TRADING_BOT_USERNAME,   
        "amount": temp_data["amount"],
        "amount_local": amount_local,
        "currency_symbol": currency_symbol,
        "exchange_rate": temp_data.get("exchange_rate", 90.0),
        "country": temp_data.get("country", "Россия"),
        "user_id": user_id,
        "username": temp_data["username"],
        "fio": temp_data.get("fio", "Не указано"),
        "bank": temp_data.get("bank", "Не указан"),
        "ts": time.time(),
        "bot_owner_id": owner or user_id
    }
    try:
        await store.push_support_event(ev)
        logger.info("Enqueued support event (card with country): %s", ev)
    except Exception:
        logger.exception("Failed to enqueue support event (card)")
@router.callback_query(F.data == "dep_card")
async def dep_card(cb: CallbackQuery):
    is_english = await is_english_user(cb.from_user.id)
    if is_english:
        await cb.answer("Bank card deposits are not available for international users", show_alert=True)
        return
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "bank_card_selected",
            "event_id": f"bank_card_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru"  
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Bank card selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send bank_card_selected event to support queue: {e}")
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    await store.clear_dep_amount(cb.from_user.id)
    min_deposit = await store.get_user_min_deposit(cb.from_user.id)
    if min_deposit > 0:
        info_text = (
            f"💰 <b>Минимальный депозит: ${min_deposit:.2f}</b>\n\n"
            f"Вы можете пополнить на любую сумму от <b>${min_deposit:.2f}</b>\n"
            f"Выберите сумму ниже:"
        )
        kb = await amount_choice_kb("dep_card_amt", cb.from_user.id)
        await cb.message.edit_text(info_text, reply_markup=kb)
    else:
        text = await get_localized_text(cb.from_user.id, "deposit_choose_amount")
        kb = await amount_choice_kb("dep_card_amt", cb.from_user.id)
        await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("user_confirm_payment:"))
async def user_confirm_payment_handler(cb: CallbackQuery, state: FSMContext):
    try:
        event_id_safe = cb.data.split("user_confirm_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"🔄 Processing payment confirmation: event_id={event_id}, user_id={cb.from_user.id}")
        temp_data = await find_payment_data(event_id, cb.from_user.id)
        if not temp_data:
            logger.error(f"❌ Payment data not found for event_id: {event_id}")
            await cb.answer("❌ Данные оплаты не найдены или устарели", show_alert=True)
            return
        if temp_data.get('user_id') != cb.from_user.id:
            logger.warning(f"❌ User mismatch: {temp_data.get('user_id')} != {cb.from_user.id}")
            await cb.answer("❌ Ошибка доступа к данным оплаты", show_alert=True)
            return
        await state.update_data(
            event_id=event_id,
            payment_data=temp_data
        )
        await state.set_state(S.WAIT_PAYMENT_CONFIRMATION)
        await cb.message.edit_text(
            "📎 <b>Подтверждение оплаты</b>\n\n"
            f"💵 Сумма: ${temp_data.get('amount', 0):.2f}\n"
            f"👤 ФИО: {temp_data.get('fio', 'Не указано')}\n"
            f"🏦 Банк: {temp_data.get('bank', 'Не указан')}\n\n"
            "Пожалуйста, отправьте подтверждение оплаты:\n\n"
            "✅ <b>Поддерживаемые форматы:</b>\n"
            "• 📸 Фото квитанции/чека\n"
            "• 📄 PDF-документ с квитанцией\n"
            "• 🖼️ Изображение (JPG, PNG)\n"
            "• 📱 Скриншот из банковского приложения\n\n"
            "<i>Отправьте файл как фото или документ в следующем сообщении</i>"
        )
        await cb.answer()
    except Exception as e:
        logger.error(f"❌ Error in user_confirm_payment_handler: {e}")
        await cb.answer("Ошибка при обработке", show_alert=True)
async def sync_payment_data_from_support(event_id: str, user_id: int) -> Optional[dict]:
    try:
        support_temp_key = f"support:card_temp:{event_id}"
        raw = await r.get(support_temp_key)
        if raw:
            support_data = json.loads(raw)
            logger.info(f"✅ Found payment data in support: {support_data}")
            await set_card_temp(event_id, support_data)
            await set_dep_card_temp(user_id, support_data)
            return support_data
        return None
    except Exception as e:
        logger.error(f"❌ Error syncing payment data from support: {e}")
        return None
@router.message(S.WAIT_PAYMENT_CONFIRMATION, F.photo)
async def process_payment_proof_photo(m: Message, state: FSMContext):
    try:
        data = await state.get_data()
        event_id = data.get('event_id')
        payment_data = data.get('payment_data', {})
        logger.info(f"📸 Processing photo payment proof: event_id={event_id}, user_id={m.from_user.id}")
        if not event_id:
            await m.answer("❌ Ошибка: не найден event_id")
            await state.clear()
            return
        photo = m.photo[-1]
        file_id = photo.file_id
        if not payment_data:
            payment_data = await find_payment_data(event_id, m.from_user.id)
            if not payment_data:
                await m.answer("❌ Данные платежа не найдены")
                await state.clear()
                return
        payment_data['file_id'] = file_id
        payment_data['file_type'] = 'photo'
        await set_card_temp(event_id, payment_data)
        support_event = {
            "type": "payment_proof",
            "event_id": event_id,
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "has_photo": True,
            "file_id": file_id,
            "amount": payment_data.get('amount'),
            "fio": payment_data.get('fio'),
            "bank": payment_data.get('bank'),
            "timestamp": time.time()
        }
        await store.push_support_event(support_event) 
        success = await download_and_forward_payment_proof(
            file_id=file_id,
            user_id=m.from_user.id,
            username=m.from_user.username or str(m.from_user.id),
            event_id=event_id,
            file_type="photo",
            payment_data=payment_data
        )
        if success:
            await m.answer(
                "✅ <b>Фото квитанции отправлено на проверку</b>\n\n"
                "Ожидайте подтверждения платежа. Средства будут зачислены в течение 24 часов."
            )
        else:
            await m.answer(
                "⚠️ <b>Информация о платеже отправлена на проверку</b>\n\n"
                "Фото не удалось загрузить, но информация о платеже передана администратору."
            )
        await clear_card_temp(event_id)
        await clear_dep_card_temp(m.from_user.id)
        await state.clear()
    except Exception as e:
        logger.error(f"❌ Error in process_payment_proof_photo: {e}")
        await m.answer("❌ Ошибка при обработке фото. Попробуйте еще раз или свяжитесь с поддержкой.")
        await state.clear()
async def get_user(self, uid: int) -> User:
    try:
        raw = await self.r.get(RKeys.user(uid))
        if raw:
            data = json.loads(raw)
            user = User(**data)
            logger.debug(f"📥 Загружен пользователь {uid}: баланс ${user.balance:.2f}")
            return user
        u = User(user_id=uid) 
        await self.save_user(u)
        logger.debug(f"📝 Создан новый пользователь {uid}")
        return u
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки пользователя {uid}: {e}")
        return User(user_id=uid)
async def check_redis_performance():
    try:
        logger.info("🔍 Проверка производительности Redis...")
        start = time.time()
        await r.ping()
        ping_time = (time.time() - start) * 1000
        logger.info(f"✅ Redis ping: {ping_time:.1f}ms")
        start = time.time()
        for i in range(10):
            await r.set(f"test:{i}", str(i))
        write_time = ((time.time() - start) * 1000) / 10
        logger.info(f"✅ Redis set avg: {write_time:.1f}ms")
        start = time.time()
        for i in range(10):
            await r.get(f"test:{i}")
        read_time = ((time.time() - start) * 1000) / 10
        logger.info(f"✅ Redis get avg: {read_time:.1f}ms")
        start = time.time()
        pipe = r.pipeline()
        for i in range(10):
            pipe.get(f"test:{i}")
        await pipe.execute()
        pipe_time = ((time.time() - start) * 1000) / 10
        logger.info(f"✅ Redis pipeline avg: {pipe_time:.1f}ms")
        for i in range(10):
            await r.delete(f"test:{i}")
        try:
            import socket
            host = REDIS_URL.split('@')[1].split(':')[0]
            ip = socket.gethostbyname(host)
            logger.info(f"📍 Redis host: {host} → {ip}")
        except:
            pass
        return ping_time, write_time, read_time, pipe_time
    except Exception as e:
        logger.error(f"❌ Redis performance check failed: {e}")
        return None
async def check_redis_connection():
    try:
        await r.ping()
        logger.info("✅ Redis connection: OK")
        queues = ["trading:balance_updates", "balance_updates", "trading:notify:ru", "trading:notify:en"]
        for queue in queues:
            length = await r.llen(queue)
            logger.info(f"✅ Очередь {queue}: {length} сообщений")
        return True
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        return False
@router.message(F.text.contains("💰 Средства возвращены на ваш баланс"))
async def handle_balance_refund_from_support(m: Message):
    try:
        user_id = m.from_user.id
        text = m.text
        import re
        amount_match = re.search(r'Возврат на ваш счёт: \$([\d.]+)', text)
        if not amount_match:
            logger.warning(f"❌ Не удалось извлечь сумму возврата из сообщения: {text}")
            return
        amount = float(amount_match.group(1))
        user = await store.get_user(user_id)
        old_balance = user.balance
        user.balance += amount
        await store.save_user(user)
        logger.info(f"✅ Баланс пользователя {user_id} обновлен через возврат от поддержки: +${amount:.2f}, старый: ${old_balance:.2f}, новый: ${user.balance:.2f}")
        await m.answer(
            f"✅ <b>Баланс успешно обновлен!</b>\n\n"
            f"💰 Зачислено: ${amount:.2f}\n"
            f"💳 Новый баланс: ${user.balance:.2f}"
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке возврата средств от поддержки: {e}")
        await m.answer("❌ Произошла ошибка при обновлении баланса. Обратитесь в поддержку.")
@router.message(F.text.contains("💳 Новый баланс:"))
async def handle_balance_update_from_support(m: Message):
    try:
        user_id = m.from_user.id
        text = m.text
        import re
        balance_match = re.search(r'Новый баланс: \$([\d.]+)', text)
        if not balance_match:
            logger.warning(f"❌ Не удалось извлечь новый баланс из сообщения: {text}")
            return
        new_balance = float(balance_match.group(1))
        user = await store.get_user(user_id)
        old_balance = user.balance
        user.balance = new_balance
        await store.save_user(user)
        logger.info(f"✅ Баланс пользователя {user_id} обновлен поддержкой: старый: ${old_balance:.2f}, новый: ${user.balance:.2f}")
        await m.answer(
            f"✅ <b>Баланс обновлен!</b>\n\n"
            f"💳 Новый баланс: ${user.balance:.2f}"
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке обновления баланса от поддержки: {e}")
@router.message(F.document & F.chat.type == "private")
async def handle_pdf_document(m: Message, state: FSMContext):
    try:
        if not (m.document.mime_type == 'application/pdf' or 
                (m.document.file_name and m.document.file_name.lower().endswith('.pdf'))):
            return
        logger.info(f"📄 Получен PDF документ от пользователя {m.from_user.id}: {m.document.file_name}")
        user_id = m.from_user.id       
        data = await state.get_data()
        event_id = data.get('event_id')
        payment_data = data.get('payment_data', {})
        if not event_id:
            active_pdf_requests = await find_user_pdf_requests(user_id)
            if active_pdf_requests:
                event_id = active_pdf_requests[0]['event_id']
                payment_data = active_pdf_requests[0]
            else:
                await m.answer("❌ Не найдено активных запросов на оплату. Начните процесс оплаты заново.")
                return
        file_id = m.document.file_id
        file_name = m.document.file_name
        file_size = m.document.file_size
        payment_data['file_id'] = file_id
        payment_data['file_name'] = file_name
        payment_data['file_type'] = 'pdf'
        payment_data['file_size'] = file_size
        await set_card_temp(event_id, payment_data)
        success = await forward_payment_proof_to_confirmation_chat(
            file_id=file_id,
            user_id=user_id,
            username=m.from_user.username or str(user_id),
            event_id=event_id,
            file_type="pdf",
            payment_data=payment_data
        )
        if success:
            await m.answer(
                "✅ <b>PDF-квитанция отправлена на проверку</b>\n\n"
                "Ожидайте подтверждения платежа администратором. Средства будут зачислены в течение 24 часов."
            )
        else:
            await m.answer(
                "⚠️ <b>Информация о платеже отправлена на проверку</b>\n\n"
                "PDF документ не удалось загрузить, но информация о платеже передана администратору."
            )
        support_event = {
            "type": "payment_proof",
            "event_id": event_id,
            "user_id": user_id,
            "username": m.from_user.username or str(user_id),
            "has_pdf": True,
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "amount": payment_data.get('amount'),
            "fio": payment_data.get('fio'),
            "bank": payment_data.get('bank'),
            "timestamp": time.time()
        }
        await store.push_support_event(support_event)
        await clear_card_temp(event_id)
        await clear_dep_card_temp(user_id)
        await state.clear()
    except Exception as e:
        logger.error(f"Error handling PDF document: {e}")
        await m.answer("❌ Ошибка при отправке PDF документа. Попробуйте еще раз или свяжитесь с поддержкой.")
async def find_user_pdf_requests(user_id: int) -> list:
    try:
        user_temp_data = await get_dep_card_temp(user_id)
        if user_temp_data:
            return [user_temp_data]
        pattern = "card_temp:*"
        keys = await store.r.keys(pattern)
        results = []
        for key in keys:
            try:
                raw = await store.r.get(key)
                if raw:
                    data = json.loads(raw)
                    if data.get('user_id') == user_id:
                        results.append(data)
            except Exception:
                continue
        return results
    except Exception as e:
        logger.error(f"Error finding user PDF requests: {e}")
        return []
@router.message(Command("debug_balance"))
async def debug_balance(m: Message):
    user_id = m.from_user.id
    user = await store.get_user(user_id)
    raw_data = await store.r.get(RKeys.user(user_id))
    redis_balance = "N/A"
    if raw_data:
        try:
            redis_data = json.loads(raw_data)
            redis_balance = redis_data.get('balance', 'N/A')
        except:
            redis_balance = "Error parsing"
    await m.answer(
        f"🔍 <b>Debug Balance Info</b>\n\n"
        f"👤 User ID: {user_id}\n"
        f"💳 Balance in object: ${user.balance:.2f}\n"
        f"📊 Balance in Redis: {redis_balance}\n"
        f"🆔 Last activity: {user.last_activity}"
    )
async def startup():
    logger.info("🚀 启动优化版机器人...")
    
    # 检查Redis连接
    redis_ok = await check_redis_connection()
    if not redis_ok:
        logger.error("❌ Redis连接失败，机器人可能无法正常工作")
    
    # 启动健康检查
    asyncio.create_task(check_redis_health(), name="redis_health_check")
    
    # 启动后台任务
    await start_background_tasks()
    
    # 使用优化版位置清理
    logger.info("🔄 运行优化版位置清理...")
    await _close_leftover_open_positions_optimized()
    
    logger.info("✅ 优化版机器人启动完成")
async def save_user(self, user: User) -> None:
    try:
        await self.r.set(RKeys.user(user.user_id), user.model_dump_json())
        logger.debug(f"💾 Сохранен пользователь {user.user_id}: баланс ${user.balance:.2f}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения пользователя {user.user_id}: {e}")
async def download_and_forward_payment_proof(file_id: str, user_id: int, username: str, event_id: str, file_type: str = "photo", payment_data: dict = None) -> bool:
    if not PAYMENT_CONFIRMATION_CHAT_ID:
        logger.warning("PAYMENT_CONFIRMATION_CHAT_ID not set")
        return False
    try:
        safe_event_id = event_id.replace(':', '_')
        logger.info(f"🔄 Forwarding payment proof ({file_type}) with 3 admin buttons: {event_id} -> {safe_event_id}")
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_payment:{safe_event_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{safe_event_id}"),
            InlineKeyboardButton(text="📋 Запросить реквизиты", callback_data=f"admin_request_requisites:{safe_event_id}")
        ]])
        amount = payment_data.get('amount', 0) if payment_data else 0
        fio = payment_data.get('fio', 'Не указано') if payment_data else 'Не указано'
        bank = payment_data.get('bank', 'Не указан') if payment_data else 'Не указан'
        file_name = payment_data.get('file_name', '') if payment_data else ''
        file_type_display = {
            'photo': 'Фото',
            'pdf': 'PDF-документ',
            'image': 'Изображение',
            'document': 'Документ'
        }.get(file_type, 'Файл')
        caption = (
            f"📎 <b>Подтверждение оплаты</b>\n\n"
            f"👤 Пользователь: @{username or 'N/A'} (ID: {user_id})\n"
            f"🆔 Event ID: <code>{event_id}</code>\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"👤 ФИО: {fio}\n"
            f"🏦 Банк: {bank}\n"
            f"🕒 Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📁 Тип: {file_type_display}\n"
        )
        if file_name:
            caption += f"📄 Файл: {file_name}\n"
        caption += f"\n<i>Подтвердите зачисление средств:</i>"
        if file_type in ["photo", "image"]:
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
        return True
    except Exception as e:
        logger.error(f"❌ Failed to forward payment proof: {e}")
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
        amount = payment_data.get('amount', 0) if payment_data else 0
        fio = payment_data.get('fio', 'Не указано') if payment_data else 'Не указано'
        bank = payment_data.get('bank', 'Не указан') if payment_data else 'Не указан'     
        owner = await store.get_bot_owner(m.from_user.id)
        token = await store.get_user_bot_token(owner)
        trb = Bot(token=token)
        await trb.send_message(
            chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
            text=(
                f"📎 <b>Подтверждение оплаты (ошибка загрузки {file_type})</b>\n\n"
                f"👤 Пользователь: @{username or 'N/A'} (ID: {user_id})\n"
                f"🆔 Event ID: <code>{event_id}</code>\n"
                f"💵 Сумма: ${amount:.2f}\n"
                f"👤 ФИО: {fio}\n"
                f"🏦 Банк: {bank}\n"
                f"🕒 Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"❌ Ошибка загрузки файла: {str(e)}\n\n"
                f"<i>Подтвердите зачисление средств:</i>"
            ),
            reply_markup=admin_kb
        )
        return False
@router.callback_query(F.data.startswith("admin_confirm_payment:"))
async def admin_confirm_payment(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_confirm_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"🔄 Admin confirming payment: event_id={event_id}")
        temp_data = await find_payment_data(event_id)
        if not temp_data:
            await cb.answer("❌ Данные платежа не найдены", show_alert=True)
            return
        user_id = temp_data.get('user_id')
        amount = temp_data.get('amount', 0)
        if not user_id:
            await cb.answer("❌ Не найден user_id в данных платежа", show_alert=True)
            return
        if temp_data.get("payment_processed"):
            await cb.answer("✅ Платёж уже был обработан ранее", show_alert=True)
            return            
        bot_code = temp_data.get('bot', 'ru')
        owner = await store.get_bot_owner(user_id) 
        notification_event = {
            "type": "balance_credit",
            "user_id": user_id,
            "amount": amount,
            "bot": bot_code,
            "reason": "crypto_payment",
            "event_id": event_id,
            "admin_id": cb.from_user.id,
            "timestamp": time.time()
        }
        logger.info(f"📤 Отправка события balance_credit: user_id={user_id}, amount=${amount}")
        await store.r.lpush("trading:balance_updates", json.dumps(notification_event))
        await store.r.lpush("balance_updates", json.dumps(notification_event))
        logger.info(f"✅ Событие balance_credit отправлено для user_id={user_id}, amount=${amount}")
        temp_data["payment_processed"] = True
        await store.increment_deposits(user_id, amount, payment_id=event_id)
        await set_card_temp(event_id, temp_data)
        try:
            token = await store.get_user_bot_token(owner)
            trb = Bot(token=token)
            await trb.send_message(
                chat_id=user_id, 
                text=f"⏳ Платёж подтверждён \n\n"
                     f"Ваш платёж на сумму ${amount:.2f} подтверждён.\n"
                     f"Зачисление на баланс производится...\n\n"
                     f"Обычно это занимает несколько секунд."
            )
        except Exception as e:
            logger.error(f"Failed to notify user: {e}")
        admin_text = (
            "✅ <b> Платеж подтвержден </b>\n\n"
            f"👤 Пользователь: @{temp_data.get('username', 'N/A')}\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"🔄 Статус: отправлено в систему зачисления\n"
            f"⏱ Время: {datetime.now().strftime('%H:%M:%S')}"
        )
        try:
            msg = cb.message
            if getattr(msg, "photo", None) or getattr(msg, "document", None):
                await msg.edit_caption(admin_text)
            else:
                await msg.edit_text(admin_text)
        except Exception as e:
            logger.error(f"Failed to edit admin message after confirm: {e}")
        await clear_card_temp(event_id)
        await clear_dep_card_temp(user_id)
        await cb.answer("Запрос на зачисление отправлен")
    except Exception as e:
        logger.error(f"Error in admin_confirm_payment: {e}")
        await cb.answer("Ошибка при подтверждении", show_alert=True)
@router.callback_query(F.data.startswith("admin_reject_payment:"))
async def admin_reject_payment(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_reject_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"🔄 Admin rejecting payment: event_id={event_id}")
        temp_data = await find_payment_data(event_id)      
        if not temp_data:
            await cb.answer("❌ Данные платежа не найдены", show_alert=True)
            return
        user_id = temp_data.get('user_id')
        amount = temp_data.get('amount', 0)
        try:
            owner = await store.get_bot_owner(m.from_user.id)
            token = await store.get_user_bot_token(owner)
            trb = Bot(token=token)
            await trb.send_message(
                chat_id=user_id,
                text=f"❌ <b>Платеж отклонен</b>\n\nСумма: ${amount:.2f}\n\nОбратитесь в поддержку для уточнения деталей."
            )
        except Exception as e:
            logger.error(f"Failed to notify user: {e}")
        await cb.message.edit_text(
            f"❌ <b>Платеж отклонен</b>\n\n"
            f"👤 Пользователь: @{temp_data.get('username', 'N/A')}\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"📝 Статус: ОТКЛОНЕНО"
        )
        await clear_card_temp(event_id)
        await clear_dep_card_temp(user_id)
        await cb.answer("Платеж отклонен")
    except Exception as e:
        logger.error(f"Error in admin_reject_payment: {e}")
        await cb.answer("Ошибка при отклонении", show_alert=True)
@router.callback_query(F.data.startswith("admin_request_requisites:"))
async def admin_request_requisites_support(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("admin_request_requisites:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        logger.info(f"🔄 Support bot: Admin requesting requisites: {event_id}")
        temp_data = await find_payment_data_support(event_id)
        if not temp_data:
            await cb.answer("❌ Данные платежа не найдены", show_alert=True)
            return
        user_id = temp_data.get('user_id')
        amount = temp_data.get('amount', 0)
        notification = {
            "type": "payment_requisites_requested",
            "event_id": event_id, 
            "user_id": user_id,
            "amount": amount,
            "admin_id": cb.from_user.id,
            "timestamp": time.time()
        }
        await r.lpush("trading:notify:ru", json.dumps(notification))
        await cb.message.edit_text(
            f"📋 <b>Запрошены реквизиты</b>\n\n"
            f"👤 Пользователь: @{temp_data.get('username', 'N/A')}\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"🔄 Уведомление отправлено в трейдинг-бот"
        )
        await cb.answer("Запрошены реквизиты")
    except Exception as e:
        logger.error(f"Support bot error in admin_request_requisites: {e}")
        await cb.answer("Ошибка при запросе", show_alert=True)
async def find_payment_data_support(event_id: str) -> Optional[dict]:
    try:
        support_temp_key = f"support:card_temp:{event_id}"
        raw = await r.get(support_temp_key)
        if raw:
            return json.loads(raw)
        card_temp_key = f"card_temp:{event_id}"
        raw = await r.get(card_temp_key)
        if raw:
            return json.loads(raw)
        return None
    except Exception as e:
        logger.error(f"Support bot error in find_payment_data: {e}")
        return None
async def forward_payment_proof_to_confirmation_chat(file_id: str, user_id: int, username: str, event_id: str, file_type: str = "photo", payment_data: dict = None):
    if not PAYMENT_CONFIRMATION_CHAT_ID:
        logger.warning("PAYMENT_CONFIRMATION_CHAT_ID not set, cannot forward payment proof")
        return False
    try:
        safe_event_id = event_id.replace(':', '_')
        logger.info(f"🔄 Forwarding payment proof ({file_type}) with 3 admin buttons: {event_id} -> {safe_event_id}")
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_payment:{safe_event_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{safe_event_id}"),
            InlineKeyboardButton(text="📋 Запросить реквизиты", callback_data=f"admin_request_requisites:{safe_event_id}")
        ]])
        amount = payment_data.get('amount', 0) if payment_data else 0
        fio = payment_data.get('fio', 'Не указано') if payment_data else 'Не указано'
        bank = payment_data.get('bank', 'Не указан') if payment_data else 'Не указан'
        file_name = payment_data.get('file_name', '') if payment_data else ''
        country = payment_data.get('country', 'Не указана') if payment_data else 'Не указана'
        file_type_display = {
            'photo': 'Фото',
            'pdf': 'PDF-документ',
            'image': 'Изображение',
            'document': 'Документ'
        }.get(file_type, 'Файл')
        caption = (
            f"📎 <b>Подтверждение оплаты</b>\n\n"
            f"👤 Пользователь: @{username or 'N/A'} (ID: {user_id})\n"
            f"🆔 Event ID: <code>{event_id}</code>\n"
            f"💵 Сумма: ${amount:.2f}\n"
            f"👤 ФИО: {fio}\n"
            f"🏦 Банк: {bank}\n"
            f"🌍 Страна: {country}\n"
            f"🕒 Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📁 Тип: {file_type_display}\n"
        )
        if file_name:
            caption += f"📄 Файл: {file_name}\n"
        caption += f"\n<i>Подтвердите зачисление средств:</i>"
        if file_type in ["photo", "image"]:
            await bot.send_photo(
                chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
                photo=file_id,
                caption=caption,
                reply_markup=admin_kb,
                parse_mode="HTML"
            )
        else:
            await bot.send_document(
                chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
                document=file_id,
                caption=caption,
                reply_markup=admin_kb,
                parse_mode="HTML"
            )
        logger.info(f"✅ Payment proof ({file_type}) forwarded with admin buttons for event {event_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to forward payment proof: {e}")
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_payment:{event_id.replace(':', '_')}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{event_id.replace(':', '_')}"),
            InlineKeyboardButton(text="📋 Запросить реквизиты", callback_data=f"admin_request_requisites:{event_id.replace(':', '_')}")
        ]])
        amount = payment_data.get('amount', 0) if payment_data else 0
        fio = payment_data.get('fio', 'Не указано') if payment_data else 'Не указано'
        bank = payment_data.get('bank', 'Не указан') if payment_data else 'Не указан'
        country = payment_data.get('country', 'Не указана') if payment_data else 'Не указана'
        owner = await store.get_bot_owner(m.from_user.id)
        token = await store.get_user_bot_token(owner)
        trb = Bot(token=token)
        await trb.send_message(
            chat_id=PAYMENT_CONFIRMATION_CHAT_ID,
            text=(
                f"📎 <b>Подтверждение оплаты (ошибка загрузки {file_type})</b>\n\n"
                f"👤 Пользователь: @{username or 'N/A'} (ID: {user_id})\n"
                f"🆔 Event ID: <code>{event_id}</code>\n"
                f"💵 Сумма: ${amount:.2f}\n"
                f"👤 ФИО: {fio}\n"
                f"🏦 Банк: {bank}\n"
                f"🌍 Страна: {country}\n"
                f"🕒 Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"❌ Ошибка загрузки файла: {str(e)}\n\n"
                f"<i>Подтвердите зачисление средств:</i>"
            ),
            reply_markup=admin_kb,
            parse_mode="HTML"
        )
        return False
async def is_correct_bot_available_for_user(user_id: int) -> bool:
    try:
        return await store.r.sismember(RKeys.watchers(), str(user_id).encode())
    except Exception as e:
        logger.error(f"Error checking bot availability for user {user_id}: {e}")
        return False
@router.message(Command("debug_watchers"))
async def debug_watchers(m: Message):
    user_id = m.from_user.id
    raw_watchers = await store.r.smembers(RKeys.watchers())
    watchers = []
    for raw in raw_watchers:
        try:
            if isinstance(raw, bytes):
                watchers.append(int(raw.decode('utf-8')))
            else:
                watchers.append(int(raw))
        except Exception as e:
            logger.error(f"Error decoding watcher: {raw}, error: {e}")
    user_owner = await store.get_bot_owner(m.from_user.id)
    is_watcher = user_id in watchers
    bot_available = await is_bot_available_for_user(user_id)
    try:
        correct_bot_available = await is_correct_bot_available_for_user(user_id)
    except Exception as e:
        correct_bot_available = f"Error: {e}"
    debug_info = (
        f"🔍 <b>Debug Watchers Info</b>\n\n"
        f"👤 User ID: {user_id}\n"
        f"👑 Bot Owner: {user_owner}\n"
        f"👀 Is Watcher: {is_watcher}\n"
        f"🤖 Main Bot Available: {bot_available}\n"
        f"✅ Correct Bot Available: {correct_bot_available}\n"
        f"📊 Total Watchers: {len(watchers)}\n"
        f"👥 Watchers List: {watchers[:10]}{'...' if len(watchers) > 10 else ''}"
    )
    await m.answer(debug_info)
@router.callback_query(F.data.startswith("user_cancel_payment:"))
async def user_cancel_payment_trading(cb: CallbackQuery):
    try:
        event_id_safe = cb.data.split("user_cancel_payment:", 1)[1]
        event_id = event_id_safe.replace('_', ':')
        await cb.message.edit_text("❌ <b>Оплата отменена</b>")
        await cb.answer()
        await support_emit({
            "type": "payment_cancelled",
            "event_id": event_id,
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "timestamp": time.time()
        })
    except Exception as e:
        logger.error(f"Error in user_cancel_payment_trading: {e}")
        await cb.answer("Ошибка при обработке", show_alert=True)
@router.message(S.WAIT_PAYMENT_CONFIRMATION, F.photo)
async def process_payment_proof_trading(m: Message, state: FSMContext):
    try:
        data = await state.get_data()
        event_id = data.get('event_id')
        payment_data = data.get('payment_data', {})
        logger.info(f"📸 Processing photo payment proof: event_id={event_id}, user_id={m.from_user.id}")
        if not event_id:
            await m.answer("❌ Ошибка: не найден event_id")
            await state.clear()
            return
        photo = m.photo[-1]  
        file_id = photo.file_id
        logger.info(f"📸 Photo file_id: {file_id}")
        if not payment_data:
            payment_data = await find_payment_data(event_id, m.from_user.id)
            if not payment_data:
                await m.answer("❌ Данные платежа не найдены")
                await state.clear()
                return
        payment_data['file_id'] = file_id
        payment_data['file_type'] = 'photo'
        await set_card_temp(event_id, payment_data)
        await support_emit({
            "type": "payment_proof",
            "event_id": event_id,
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "has_photo": True,
            "file_id": file_id,
            "amount": payment_data.get('amount'),
            "fio": payment_data.get('fio'),
            "bank": payment_data.get('bank'),
            "timestamp": time.time()
        })
        success = await forward_payment_proof_to_confirmation_chat(
            file_id=file_id,
            user_id=m.from_user.id,
            username=m.from_user.username or str(m.from_user.id),
            event_id=event_id,
            file_type="photo",
            payment_data=payment_data
        )
        if success:
            await m.answer(
                "✅ <b>Фото квитанции отправлено на проверку</b>\n\n"
                "Ожидайте подтверждения платежа администратором. Средства будут зачислены в течение 24 часов."
            )
        else:
            await m.answer(
                "⚠️ <b>Информация о платеже отправлена на проверку</b>\n\n"
                "Фото не удалось загрузить, но информация о платеже передана администратору."
            )
        await clear_card_temp(event_id)
        await clear_dep_card_temp(m.from_user.id)
        await state.clear()
    except Exception as e:
        logger.error(f"❌ Error in process_payment_proof_photo: {e}")
        await m.answer("❌ Ошибка при обработке фото. Попробуйте еще раз или свяжитесь с поддержкой.")
        await state.clear()
@router.message(S.WAIT_PAYMENT_CONFIRMATION)
async def wrong_payment_proof_trading(m: Message):
    await m.answer(
        "❌ <b>Неверный формат</b>\n\n"
        "Пожалуйста, отправьте подтверждение оплаты в одном из форматов:\n"
        "• 📸 Фото квитанции/чека (как фото)\n" 
        "• 📄 PDF-документ с квитанцией (как документ)\n"
        "• 🖼️ Изображение (JPG, PNG) (как документ или фото)\n"
        "• 📱 Скриншот из банковского приложения\n\n"
        "<i>Используйте кнопку 'Фото' или 'Документ' в приложении Telegram</i>"
    )
@router.message(S.WAIT_PAYMENT_CONFIRMATION, F.photo)
async def process_payment_proof_photo(m: Message, state: FSMContext):
    try:
        data = await state.get_data()
        event_id = data.get('event_id')
        payment_data = data.get('payment_data', {})
        logger.info(f"📸 Processing photo payment proof: event_id={event_id}, user_id={m.from_user.id}")
        if not event_id:
            await m.answer("❌ Ошибка: не найден event_id")
            await state.clear()
            return
        photo = m.photo[-1]
        file_id = photo.file_id
        if not payment_data:
            payment_data = await find_payment_data(event_id, m.from_user.id)
            if not payment_data:
                await m.answer("❌ Данные платежа не найдены")
                await state.clear()
                return
        payment_data['file_id'] = file_id
        payment_data['file_type'] = 'photo'
        await set_card_temp(event_id, payment_data)
        await support_emit({
            "type": "payment_proof",
            "event_id": event_id,
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "has_photo": True,
            "file_id": file_id,
            "amount": payment_data.get('amount'),
            "fio": payment_data.get('fio'),
            "bank": payment_data.get('bank'),
            "timestamp": time.time()
        })
        success = await download_and_forward_payment_proof(
            file_id=file_id,
            user_id=m.from_user.id,
            username=m.from_user.username or str(m.from_user.id),
            event_id=event_id,
            file_type="photo",
            payment_data=payment_data
        )
        if success:
            await m.answer(
                "✅ <b>Фото квитанции отправлено на проверку</b>\n\n"
                "Ожидайте подтверждения платежа администратором. Средства будут зачислены в течение 24 часов."
            )
        else:
            await m.answer(
                "⚠️ <b>Информация о платеже отправлена на проверку</b>\n\n"
                "Фото не удалось загрузить, но информация о платеже передана администратору."
            )
        await clear_card_temp(event_id)
        await clear_dep_card_temp(m.from_user.id)
        await state.clear()
    except Exception as e:
        logger.error(f"❌ Error in process_payment_proof_photo: {e}")
        await m.answer("❌ Ошибка при обработке фото. Попробуйте еще раз или свяжитесь с поддержкой.")
        await state.clear()
@router.message(S.WAIT_PAYMENT_CONFIRMATION, F.document)
async def process_payment_proof_document(m: Message, state: FSMContext):
    try:
        data = await state.get_data()
        event_id = data.get('event_id')
        payment_data = data.get('payment_data', {})
        logger.info(f"📄 Processing document payment proof: event_id={event_id}, user_id={m.from_user.id}")
        if not event_id:
            await m.answer("❌ Ошибка: не найден event_id")
            await state.clear()
            return
        document = m.document
        file_id = document.file_id
        file_name = document.file_name or "document"
        file_size = document.file_size or 0
        if file_size > 20 * 1024 * 1024:
            await m.answer(
                "❌ <b>Файл слишком большой</b>\n\n"
                "Максимальный размер файла: 20 MB\n"
                "Пожалуйста, отправьте файл меньшего размера или скриншот."
            )
            return
        if not payment_data:
            payment_data = await find_payment_data(event_id, m.from_user.id)
            if not payment_data:
                await m.answer("❌ Данные платежа не найдены")
                await state.clear()
                return
        file_type = "document"
        if file_name.lower().endswith('.pdf'):
            file_type = "pdf"
        elif any(file_name.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']):
            file_type = "image"
        payment_data['file_id'] = file_id
        payment_data['file_name'] = file_name
        payment_data['file_type'] = file_type
        await set_card_temp(event_id, payment_data)
        await support_emit({
            "type": "payment_proof",
            "event_id": event_id,
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "has_document": True,
            "file_id": file_id,
            "file_name": file_name,
            "file_type": file_type,
            "file_size": file_size,
            "amount": payment_data.get('amount'),
            "fio": payment_data.get('fio'),
            "bank": payment_data.get('bank'),
            "timestamp": time.time()
        })
        success = await forward_payment_proof_to_confirmation_chat(
            file_id=file_id,
            user_id=m.from_user.id,
            username=m.from_user.username or str(m.from_user.id),
            event_id=event_id,
            file_type=file_type,
            payment_data=payment_data
        )
        if success:
            file_type_display = "PDF-документ" if file_type == "pdf" else "документ"
            await m.answer(
                f"✅ <b>{file_type_display} отправлен на проверку</b>\n\n"
                f"📄 Файл: {file_name}\n"
                f"💾 Размер: {file_size // 1024} KB\n\n"
                "Ожидайте подтверждения платежа администратором. Средства будут зачислены в течение 24 часов."
            )
        else:
            await m.answer(
                "⚠️ <b>Информация о платеже отправлена на проверку</b>\n\n"
                "Документ не удалось загрузить, но информация о платеже передана администратору."
            )
        await clear_card_temp(event_id)
        await clear_dep_card_temp(m.from_user.id)
        await state.clear()
    except Exception as e:
        logger.error(f"❌ Error in process_payment_proof_document: {e}")
        await m.answer(
            "❌ Ошибка при обработке документа. Попробуйте еще раз или свяжитесь с поддержкой.\n\n"
            "Рекомендуемые форматы:\n"
            "• PDF-документы\n"
            "• Изображения (JPG, PNG)\n"
            "• Фото квитанций"
        )
        await state.clear()
@router.callback_query(F.data == "dep_crypto")
async def dep_crypto(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass    
    user_language = await get_user_language(cb.from_user.id)
    bot_code = "en" if user_language == "en" else "ru"
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "crypto_selected",
            "event_id": f"crypto_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": bot_code  
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Crypto selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send crypto_selected event to support queue: {e}")
    await store.clear_dep_amount(cb.from_user.id)
    await store.clear_dep_token(cb.from_user.id)
    text = await get_localized_text(cb.from_user.id, "crypto_choose_token")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="USDT", callback_data="dep_token:USDT")],
        [InlineKeyboardButton(text="ETHEREUM", callback_data="dep_token:ETH")],
        [InlineKeyboardButton(text="BITCOIN", callback_data="dep_token:BTC")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("dep_country_"), S.DEP_WAIT_COUNTRY)
async def dep_country_select(cb: CallbackQuery, state: FSMContext):
    country_map = {
        "dep_country_uzbekistan": "Узбекистан",
        "dep_country_russia": "Россия", 
        "dep_country_other": "Другая страна"
    }
    country_key = cb.data
    country_name = country_map.get(country_key, "Неизвестная страна")
    await state.update_data(country=country_name)
    if country_key == "dep_country_uzbekistan":
        await state.set_state(S.DEP_WAIT_UZBEK_BANK)
        await cb.message.edit_text(
            "🏦 <b>Выберите банк Узбекистана</b>\n\n"
            "Выберите ваш банк:",
            reply_markup=deposit_uzbek_bank_kb()
        )
    else:
        data = await state.get_data()
        bank_name = data.get('bank', 'Другой банк')
        await process_deposit_final_step(cb, state, bank_name, country_name)
    await cb.answer()
@router.callback_query(F.data.startswith("dep_uzbek_bank_"), S.DEP_WAIT_UZBEK_BANK)
async def dep_uzbek_bank_select(cb: CallbackQuery, state: FSMContext):
    bank_map = {
        "dep_uzbek_bank_kaspi": "Каспи банк",
        "dep_uzbek_bank_halyk": "Халык Банк",
        "dep_uzbek_bank_other": "Другой банк Узбекистана"
    }
    bank_key = cb.data
    bank_name = bank_map.get(bank_key, "Неизвестный банк")
    await state.update_data(bank=bank_name, country="Узбекистан")
    await process_deposit_final_step(cb, state, bank_name, "Узбекистан")
    await cb.answer()
async def process_deposit_final_step(cb: CallbackQuery, state: FSMContext, bank_name: str, country: str):
    data = await state.get_data()
    event_id = data.get('event_id')
    if not event_id:
        await cb.answer("❌ Сессия истекла. Начните пополнение заново.")
        await state.clear()
        return
    temp_data = await get_card_temp(event_id)
    if not temp_data:
        await cb.answer("❌ Сессия истекла. Начните пополнение заново.")
        await state.clear()
        return    
    temp_data['bank'] = bank_name
    temp_data['country'] = country
    await set_card_temp(event_id, temp_data)
    amount_usd = temp_data['amount']    
    if country == "Узбекистан":
        usd_uzs_rate = await get_usd_uzs_rate()
        amount_local = amount_usd * usd_uzs_rate
        currency_symbol = "UZS"
        rate = usd_uzs_rate
    else:
        usd_rub_rate = await get_usd_rub_rate()
        amount_local = amount_usd * usd_rub_rate
        currency_symbol = "RUB"
        rate = usd_rub_rate    
    temp_data['amount_local'] = amount_local
    temp_data['currency_symbol'] = currency_symbol
    temp_data['exchange_rate'] = rate
    await set_card_temp(event_id, temp_data)
    await send_card_deposit_to_support(temp_data, amount_local, currency_symbol)
    await cb.message.edit_text(
        f"✅ <b>Запрос на пополнение отправлен!</b>\n\n"
        f"💵 Сумма: <b>{amount_local:.0f} {currency_symbol} (${amount_usd})</b>\n"
        f"📊 Курс: 1 USD = {rate:.2f} {currency_symbol}\n"
        f"🌍 Страна: {country}\n"
        f"👤 ФИО: {temp_data['fio']}\n"
        f"🏦 Банк: {bank_name}\n\n"
        f"⏳ Ожидайте реквизиты для оплаты."
    )
    await state.clear()
    await clear_card_temp(event_id)
@router.callback_query(F.data.startswith("dep_crypto_amt:"))
async def dep_crypto_amount(cb: CallbackQuery):
    amt = int(cb.data.split(":")[1])
    await store.set_dep_amount(cb.from_user.id, amt)
    await cb.answer("Генерация кошелька…")
    gen_msg = await cb.message.edit_text("⏳ 5 секунд генерируем ваш собственный кошелёк для пополнения…")
    await asyncio.sleep(5)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="TRC20", callback_data="net_trc20")],
        [InlineKeyboardButton(text="BEP20", callback_data="net_bep20")],
    ])
    await cb.message.edit_text("Выберите сеть:", reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.startswith("dep_token:"))
async def dep_token_select(cb: CallbackQuery):
    token_raw = cb.data.split(":", 1)[1].upper()
    token = "USDT" if token_raw in ("USDT",) else ("ETH" if token_raw in ("ETH","ETHEREUM") else ("BTC" if token_raw in ("BTC","BITCOIN") else token_raw))
    await store.set_dep_token(cb.from_user.id, token)
    await store.clear_dep_amount(cb.from_user.id)
    try:
        await _init_trading_bot_username_once()        
        event_type = ""
        token_display = ""
        if token == "USDT":
            event_type = "usdt_selected"
            token_display = "USDT"
        elif token == "ETH":
            event_type = "ethereum_selected"
            token_display = "ETHEREUM"
        elif token == "BTC":
            event_type = "bitcoin_selected"
            token_display = "BITCOIN"
        else:
            event_type = f"{token.lower()}_selected"
            token_display = token
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": event_type,
            "event_id": f"{event_type}_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "token": token,
            "token_display": token_display,
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "en" if await is_english_user(cb.from_user.id) else "ru"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ {token_display} selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send {token}_selected event to support queue: {e}")
    choose_amount_text = await get_localized_text(cb.from_user.id, "deposit_choose_amount")
    token_display = "USDT" if token == "USDT" else ("ETHEREUM" if token == "ETH" else "BITCOIN")
    text = f"{choose_amount_text}"
    await cb.message.edit_text(
        text,
        reply_markup=await amount_choice_kb("dep_amt", user_id=cb.from_user.id)
    )
    await cb.answer()
@router.callback_query(F.data.startswith("dep_amt:"))
async def dep_amount_select(cb: CallbackQuery):
    amt = int(cb.data.split(":", 1)[1])
    await store.set_dep_amount(cb.from_user.id, amt)    
    try:
        await _init_trading_bot_username_once()
        token = await store.get_dep_token(cb.from_user.id) or "USDT"
        token_display = "USDT" if token == "USDT" else ("ETHEREUM" if token == "ETH" else "BITCOIN")
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "deposit_amount_selected",
            "event_id": f"dep_amount_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "amount": amt,
            "token": token,
            "token_display": token_display,
            "method": "crypto",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "en" if await is_english_user(cb.from_user.id) else "ru"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Deposit amount selected event sent to support queue: ${amt} {token_display}")
    except Exception as e:
        logger.error(f"❌ Failed to send deposit_amount_selected event to support queue: {e}")
    token = await store.get_dep_token(cb.from_user.id)
    if not token:
        token = "USDT"
        await store.set_dep_token(cb.from_user.id, token)
    nets = get_available_networks(token)
    rows = [[InlineKeyboardButton(text=n, callback_data=f"dep_net:{n}") ] for n in nets]
    kb = InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="ERC20", callback_data="dep_net:ERC20")]])
    choose_network_text = await get_localized_text(cb.from_user.id, "crypto_choose_network")
    await cb.message.edit_text(choose_network_text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data == "dep_usdt")
async def dep_usdt(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="TRC20", callback_data="net_trc20")],
        [InlineKeyboardButton(text="BEP20", callback_data="net_bep20")],
    ])
    await cb.message.edit_text("Выберите сеть:", reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data.in_({"net_trc20", "net_bep20"}))
async def dep_network(cb: CallbackQuery):
    network = "TRC20" if cb.data == "net_trc20" else "BEP20"
    try:
        await _init_trading_bot_username_once()
        amt = await store.get_dep_amount(cb.from_user.id) or 500
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "deposit_network_selected",
            "event_id": f"dep_network_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "amount": amt,
            "token": "USDT",
            "token_display": "USDT",
            "network": network,
            "method": "crypto",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "en" if await is_english_user(cb.from_user.id) else "ru"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Deposit network selected event sent to support queue: {network} for USDT")
    except Exception as e:
        logger.error(f"❌ Failed to send deposit_network_selected event to support queue: {e}")
    
    addr = get_wallet_address("USDT", network) or (
        "TXXXX...USDT" if network == "TRC20" else "0xXXXX...USDT"
    )
    amt = await store.get_dep_amount(cb.from_user.id)
    if amt is None:
        amt = 500
    await cb.answer()
    await cb.message.edit_text(
        f"💳 Скопируйте и пополните на {amt} USDT\nКошелёк ({network}): <code>{addr}</code>"
    )
    try:
        user_lang = await get_user_language(cb.from_user.id)
    except Exception:
        user_lang = "ru"
    bot_code = "en" if user_lang == "en" else "ru"    
    ev = {
        "event_id": gen_event_id(),
        "type": "crypto",
        "amount": amt,
        "network": network,
        "asset": "USDT",
        "user_id": cb.from_user.id,
        "username": cb.from_user.username or str(cb.from_user.id),
        "bot": bot_code,                       
        "bot_username": TRADING_BOT_USERNAME,   
        "ts": time.time(),
    }
    try:
        await store.push_support_event(ev)
        logger.info("Enqueued support event (crypto): %s", ev)
    except Exception:
        logger.exception("Failed to enqueue support event (crypto)")
    await store.update_user_activity(cb.from_user.id)
    await cb.answer()
@router.callback_query(F.data.startswith("dep_net:"))
async def dep_network_select(cb: CallbackQuery):
    network = cb.data.split(":", 1)[1].upper()
    token = await store.get_dep_token(cb.from_user.id) or "USDT"
    amt = await store.get_dep_amount(cb.from_user.id) or 500
    try:
        await _init_trading_bot_username_once()
        token_display = "USDT" if token == "USDT" else ("ETHEREUM" if token == "ETH" else "BITCOIN")
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "deposit_network_selected",
            "event_id": f"dep_network_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "amount": amt,
            "token": token,
            "token_display": token_display,
            "network": network,
            "method": "crypto",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "en" if await is_english_user(cb.from_user.id) else "ru"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Deposit network selected event sent to support queue: {network} for {token_display}")
    except Exception as e:
        logger.error(f"❌ Failed to send deposit_network_selected event to support queue: {e}")
    show_generation_message = True
    try:
        already_shown = await store.is_wallet_ready(cb.from_user.id, token, network)
        if already_shown:
            show_generation_message = False
    except Exception:
        pass
    wait_msg = None
    if show_generation_message:
        wait_text = await get_localized_text(cb.from_user.id, "crypto_generating_wallet")
        wait_msg = await cb.message.edit_text(wait_text)
        await asyncio.sleep(5)
        try:
            await store.set_wallet_ready(cb.from_user.id, token, network)
        except Exception:
            pass
    addr = get_wallet_address(token, network) or "—"
    t_upper = str(token).upper()
    if t_upper == "USDT":
        display_token = "USDT"
    elif t_upper in ("ETH", "ETHEREUM"):
        display_token = "ETH"
    elif t_upper in ("BTC", "BITCOIN"):
        display_token = "BTC"
    else:
        display_token = token
    display_amount = None
    try:
        if display_token == "ETH":
            price = await fetch_usd_price("ETHUSDT")
            if price > 0:
                qty = (amt or 0) / price
                display_amount = f"{qty:.6f} ETH"
        elif display_token == "BTC":
            price = await fetch_usd_price("BTCUSDT")
            if price > 0:
                qty = (amt or 0) / price
                display_amount = f"{qty:.8f} BTC"
    except Exception:
        pass
    if not display_amount:
        display_amount = f"{amt} {display_token}"
    try:
        user_lang = await get_user_language(cb.from_user.id)
    except Exception:
        user_lang = "ru"
    if user_lang == "en":
        deposit_instructions = (
            f"💳 <b>Deposit {display_amount}</b>\n\n"
            f"Network: <b>{network}</b>\n"
            f"Token: <b>{display_token}</b>\n"
            f"Wallet address: <code>{addr}</code>\n\n"
            f"<i>Copy the address and send the exact amount</i>"
        )
    else:
        deposit_instructions = (
            f"💳 <b>Пополните на {display_amount}</b>\n\n"
            f"Сеть: <b>{network}</b>\n"
            f"Токен: <b>{display_token}</b>\n"
            f"Адрес кошелька: <code>{addr}</code>\n\n"
            f"<i>Скопируйте адрес и отправьте точную сумму</i>"
        )
    if wait_msg:
        await wait_msg.edit_text(deposit_instructions, parse_mode="HTML")
    else:
        await cb.message.edit_text(deposit_instructions, parse_mode="HTML")
    try:
        user_lang = await get_user_language(cb.from_user.id)
    except Exception:
        user_lang = "ru"
    bot_code = "en" if user_lang == "en" else "ru"    
    ev = {
        "event_id": gen_event_id(),
        "type": "crypto",
        "amount": amt,
        "network": network,
        "asset": display_token,
        "user_id": cb.from_user.id,
        "username": cb.from_user.username or str(cb.from_user.id),
        "bot": bot_code,              
        "bot_username": TRADING_BOT_USERNAME, 
        "ts": time.time(),
    }
    try:
        await store.push_support_event(ev)
        logger.info("Enqueued support event (crypto): %s", ev)
    except Exception:
        logger.exception("Failed to enqueue support event (crypto)")
    await store.update_user_activity(cb.from_user.id)
    await cb.answer()
@router.message(Command("worker_status"))
async def check_worker_status(m: Message):
    global NOTIFY_WORKER_LAST_ACTIVE
    time_since_active = time.time() - NOTIFY_WORKER_LAST_ACTIVE
    ru_len = await store.r.llen("trading:notify:ru")
    en_len = await store.r.llen("trading:notify:en")
    last_ru_items = await store.r.lrange("trading:notify:ru", 0, 2)
    last_en_items = await store.r.lrange("trading:notify:en", 0, 2)
    status_info = []
    for i, item in enumerate(last_ru_items):
        try:
            data = json.loads(item.decode() if isinstance(item, (bytes, bytearray)) else item)
            status_info.append(f"RU[{i}]: {data.get('type')} for user {data.get('user_id')}")
        except:
            status_info.append(f"RU[{i}]: Invalid JSON")
    for i, item in enumerate(last_en_items):
        try:
            data = json.loads(item.decode() if isinstance(item, (bytes, bytearray)) else item)
            status_info.append(f"EN[{i}]: {data.get('type')} for user {data.get('user_id')}")
        except:
            status_info.append(f"EN[{i}]: Invalid JSON")
    status = (
        f"🤖 Notification Worker Status:\n"
        f"• Last active: {time_since_active:.1f}s ago\n"
        f"• Queue sizes: RU={ru_len}, EN={en_len}\n"
        f"• Worker running: {'✅' if time_since_active < 10 else '❌'}\n"
        f"• Recent items:\n" + "\n".join(f"  {item}" for item in status_info[:4])
    )
    await m.answer(status)
@router.callback_query(F.data == "wd_other")
async def wd_other(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    await cb.message.edit_text("Свяжитесь с поддержкой для альтернативных способов.")
    await cb.answer()
@router.callback_query(F.data == "kyc")
async def on_kyc(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    verification_text = (
        "Для прохождения процедуры верификации Клиент обязан обеспечить наличие "
        "не менее 20 (двадцати) закрытых ордеров на счёте. При отсутствии указанного "
        "количества система вправе считать верификацию незавершённой до выполнения требования."
    )
    await cb.message.edit_text(verification_text)
    await cb.answer()
@router.callback_query(F.data == "reqs")
async def on_reqs(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    data = _load_crypto_wallets()
    if not data:
        example = (
            '{\n'
            '  "USDT": {\n'
            '    "TRC20": "Txxx",\n'
            '    "BEP20": "0x000...usdt"\n'
            '  },\n'
            '  "BTC": {\n'
            '    "BTC": "bc1..."\n'
            '  }\n'
            '}'
        )
        txt = (
            "⚙️ <b>Реквизиты не настроены</b>\n"
            "Создайте файл <code>crypto_wallets.json</code> в папке проекта и перезапустите бота.\n\n"
            "<b>Пример:</b>\n<code>" + example + "</code>"
        )
        await cb.message.edit_text(txt)
        await cb.answer()
        return
    lines = ["💳 <b>Кошельки для пополнения</b>"]
    if any(isinstance(v, dict) for v in data.values()):
        for token, nets in data.items():
            if not isinstance(nets, dict): 
                continue
            lines.append(f"\n<b>{token}</b>")
            for net, addr in nets.items():
                lines.append(f"• {net}: <code>{addr}</code>")
    else:
        lines.append("\n<b>USDT</b>")
        for net, addr in data.items():
            lines.append(f"• {net}: <code>{addr}</code>")
    await cb.message.edit_text("\n".join(lines))
    await cb.answer()
@router.message(Command("send_image"))
async def send_image_command(m: Message):
    admin_ids = [7229194724]  # Замените на реальные ID админов
    if m.from_user.id not in admin_ids:
        await m.answer("❌ У вас нет прав для этой команды")
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("❌ Используйте: /send_image <user_id> <текст>")
        return
    try:
        user_id = int(parts[1])
        caption = parts[2]
        from aiogram.fsm.context import FSMContext
        from aiogram.fsm.storage.memory import MemoryStorage
        storage = MemoryStorage()
        await storage.set_data(
            chat=m.chat.id,
            user=m.from_user.id,
            data={"admin_send_image": {"user_id": user_id, "caption": caption}}
        )
        await m.answer(
            f"📤 Готово!\n"
            f"👤 Получатель: {user_id}\n"
            f"📝 Текст: {caption}\n\n"
            f"Теперь отправьте картинку (фото или изображение)"
        )
    except ValueError:
        await m.answer("❌ Неверный формат user_id")
@router.message(Command("send_to_user"))
async def send_to_user_command(m: Message):
    admin_ids = [7229194724]  # Замените на реальные ID админов
    if m.from_user.id not in admin_ids:
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("❌ Используйте: /send_to_user <user_id>")
        return
    try:
        user_id = int(parts[1])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📷 Картинка + текст", callback_data=f"admin_send_image:{user_id}")],
            [InlineKeyboardButton(text="📝 Только текст", callback_data=f"admin_send_text:{user_id}")]
        ])
        await m.answer(
            f"👤 Выбран пользователь: {user_id}\n"
            f"Выберите тип отправки:",
            reply_markup=kb
        )
    except ValueError:
        await m.answer("❌ Неверный формат user_id")
@router.callback_query(F.data.startswith("admin_send_image:"))
async def admin_choose_send_image(cb: CallbackQuery, state: FSMContext):
    admin_ids = [7229194724]  
    if cb.from_user.id not in admin_ids:
        await cb.answer("❌ Нет прав")
        return
    user_id = int(cb.data.split(":")[1])
    await state.update_data(
        admin_send_image_user_id=user_id,
        admin_send_image_step="wait_image"
    )
    await cb.message.edit_text(
        f"📤 Отправка картинки пользователю {user_id}\n\n"
        f"1. Отправьте картинку (фото)\n"
        f"2. После получения картинки я запрошу текст"
    )
    await cb.answer()
@router.message(F.photo, lambda m: m.from_user.id in [7229194724])  # Только для админов
async def admin_send_image_photo(m: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("admin_send_image_step") != "wait_image":
        return
    photo = m.photo[-1]
    file_id = photo.file_id
    await state.update_data(
        admin_send_image_file_id=file_id,
        admin_send_image_step="wait_caption"
    )
    await m.answer(
        f"✅ Картинка получена!\n"
        f"Теперь отправьте текст для подписи:"
    )
@router.message(lambda m: m.from_user.id in [7229194724] and m.text and not m.text.startswith("/"))
async def admin_send_image_caption(m: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("admin_send_image_step") != "wait_caption":
        return
    user_id = data.get("admin_send_image_user_id")
    file_id = data.get("admin_send_image_file_id")
    caption = m.text
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Отправить", callback_data=f"confirm_send_image:{user_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_send_image")
        ]
    ])
    await m.answer_photo(
        photo=file_id,
        caption=f"📤 <b>Предпросмотр отправки:</b>\n\n"
               f"👤 <b>Получатель:</b> {user_id}\n"
               f"📝 <b>Текст:</b> {caption}\n\n"
               f"Подтвердите отправку:",
        reply_markup=kb
    )
    await state.update_data(
        admin_send_image_caption=caption,
        admin_send_image_step="confirm"
    )
@router.callback_query(F.data.startswith("confirm_send_image:"))
async def confirm_send_image_to_user(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = int(cb.data.split(":")[1])
    file_id = data.get("admin_send_image_file_id")
    caption = data.get("admin_send_image_caption")
    try:
        await bot.send_photo(
            chat_id=user_id,
            photo=file_id,
            caption=caption,
            parse_mode="HTML"
        )
        await cb.message.edit_caption(
            caption=f"✅ <b>Картинка отправлена!</b>\n\n"
                   f"👤 Получатель: {user_id}\n"
                   f"📝 Текст: {caption}"
        )
        logger.info(f"📤 Админ {cb.from_user.id} отправил картинку пользователю {user_id}")
        await state.clear()
    except TelegramForbiddenError:
        await cb.message.edit_caption(
            caption=f"❌ <b>Не удалось отправить</b>\n\n"
                   f"Пользователь {user_id} заблокировал бота"
        )
    except Exception as e:
        await cb.message.edit_caption(
            caption=f"❌ <b>Ошибка при отправке:</b> {str(e)}"
        )
    await cb.answer()
@router.message(Command("send_media"))
async def send_media_to_user(m: Message):
    admin_ids = [7229194724]  # Замените на реальные ID админов
    if m.from_user.id not in admin_ids:
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("❌ Используйте: /send_media <user_id> <текст>")
        return
    try:
        user_id = int(parts[1])
        caption = parts[2]
        await m.answer(
            f"📤 Отправка пользователю {user_id}\n"
            f"📝 Текст: {caption}\n\n"
            f"Теперь отправьте картинку (фото)\n"
            f"Или отправьте /cancel для отмены"
        )
        await r.setex(
            f"admin_send_media:{m.from_user.id}",
            300,  # 5 минут
            json.dumps({"user_id": user_id, "caption": caption})
        )
    except ValueError:
        await m.answer("❌ Неверный формат user_id")
@router.message(F.photo, lambda m: m.from_user.id in [7229194724])
async def handle_admin_media_photo(m: Message):
    temp_key = f"admin_send_media:{m.from_user.id}"
    temp_data_raw = await r.get(temp_key)
    if not temp_data_raw:
        return
    temp_data = json.loads(temp_data_raw)
    user_id = temp_data["user_id"]
    caption = temp_data["caption"]
    photo = m.photo[-1]
    file_id = photo.file_id
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data=f"send_media_now:{user_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_media")]
    ])
    await m.answer_photo(
        photo=file_id,
        caption=f"📤 <b>Предпросмотр отправки:</b>\n\n"
               f"👤 <b>Получатель:</b> {user_id}\n"
               f"📝 <b>Текст:</b> {caption}\n\n"
               f"Подтвердите отправку:",
        reply_markup=kb
    )
    await r.setex(
        f"admin_send_media_file:{m.from_user.id}",
        300,
        json.dumps({"file_id": file_id, "user_id": user_id, "caption": caption})
    )
    await r.delete(temp_key)
@router.callback_query(F.data.startswith("send_media_now:"))
async def send_media_now(cb: CallbackQuery):
    user_id = int(cb.data.split(":")[1])
    temp_key = f"admin_send_media_file:{cb.from_user.id}"
    temp_data_raw = await r.get(temp_key)
    if not temp_data_raw:
        await cb.answer("❌ Время действия истекло")
        return
    temp_data = json.loads(temp_data_raw)
    file_id = temp_data["file_id"]
    caption = temp_data["caption"]
    try:
        owner = await store.get_bot_owner(user_id)
        token = await store.get_user_bot_token(owner)
        trb = Bot(token=token)
        await trb.send_photo(
            chat_id=user_id,
            photo=file_id,
            caption=caption,
            parse_mode="HTML"
        )
        await cb.message.edit_caption(
            caption=f"✅ <b>Успешно отправлено!</b>\n\n"
                   f"👤 Пользователь: {user_id}\n"
                   f"📝 Текст: {caption}"
        )
        logger.info(f"📤 Админ {cb.from_user.id} отправил медиа пользователю {user_id}")
    except TelegramForbiddenError:
        await cb.message.edit_caption(
            caption=f"❌ <b>Пользователь заблокировал бота</b>\n\n"
                   f"ID: {user_id}"
        )
    except Exception as e:
        await cb.message.edit_caption(
            caption=f"❌ <b>Ошибка:</b> {str(e)}"
        )
    await r.delete(temp_key)
    await cb.answer()
@router.message(Command("quick_send"))
async def quick_send_image(m: Message):
    admin_ids = [123456789]
    if m.from_user.id not in admin_ids:
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("❌ Формат: /quick_send <user_id> <текст>")
        return
    try:
        user_id = int(parts[1])
        caption = parts[2]
        await r.setex(
            f"quick_send:{m.from_user.id}",
            300,
            json.dumps({"user_id": user_id, "caption": caption})
        )
        await m.answer(
            f"⚡ Быстрая отправка\n"
            f"👤 Получатель: {user_id}\n"
            f"📝 Текст: {caption}\n\n"
            f"Теперь отправьте картинку для немедленной отправки"
        )
    except ValueError:
        await m.answer("❌ Неверный user_id")
@router.message(F.text.in_(["Открытые сделки", "Open Positions"]))
async def on_open_positions(m: Message):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(m.from_user.id)
        support_event = {
            "type": "open_positions_opened",
            "event_id": f"open_positions_{m.from_user.id}_{int(time.time() * 1000)}",
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(m.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Open positions opened event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send open_positions_opened event to support queue: {e}")
    positions = await store.list_positions(m.from_user.id)
    if not positions:
        empty_text = await get_localized_text(m.from_user.id, "no_open_positions")
        await m.answer(empty_text)
        return
    title_text = await get_localized_text(m.from_user.id, "open_positions_title")
    lines = [title_text]
    for p in positions:
        pct = (p.pnl_current / max(1e-9, p.order_amount)) * 100.0
        lines.append(f"• {p.symbol} {p.side.value} — {fmt_money(p.pnl_current)} ({pct:+.2f}%)")
    msg = await m.answer("\n".join(lines))
    spawn(live_update_open_positions(m.chat.id, m.from_user.id, msg.message_id, duration_sec=60), name="live_update_open_positions")
@router.message(F.text.in_(["История сделок", "Trade History"]))
async def on_history(m: Message):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(m.from_user.id)
        support_event = {
            "type": "trade_history_opened",
            "event_id": f"trade_history_{m.from_user.id}_{int(time.time() * 1000)}",
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(m.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Trade history opened event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send trade_history_opened event to support queue: {e}")
    await send_history_page(m.chat.id, m.from_user.id, page=0)
async def _log_trade_history_event(m: Message):
    try:
        await _init_trading_bot_username_once()
        user_lang = await get_user_language(cb.from_user.id)
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "trade_history_opened",
            "event_id": f"trade_history_{m.from_user.id}_{int(time.time() * 1000)}",
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "timestamp": time.time(),
            "bot": "ru" if user_lang == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Trade history opened for user {m.from_user.id}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to log trade history event: {e}")
async def send_history_page(chat_id: int, uid: int, page: int):
    if page > 0:
        try:
            await _init_trading_bot_username_once()
            owner = await store.get_bot_owner(uid) 
            support_event = {
                "type": "trade_history_page_viewed",
                "event_id": f"trade_history_page_{uid}_{int(time.time() * 1000)}",
                "user_id": uid,
                "username": (await store.get_user(uid)).username or str(uid),
                "page": page,
                "bot_username": TRADING_BOT_USERNAME,
                "timestamp": time.time(),
                "bot": "ru" if await get_user_language(uid) == "ru" else "en"
            }
            await store.push_support_event(support_event)
            logger.info(f"✅ Trade history page viewed event sent to support queue: page {page}")
        except Exception as e:
            logger.error(f"❌ Failed to send trade_history_page_viewed event to support queue: {e}")
    items, total = await store.get_history_page(uid, page=page, page_size=10)
    if not items:
        empty_text = await get_localized_text(uid, "history_empty")
        owner = await store.get_bot_owner(uid) 
        token = await store.get_user_bot_token(owner)
        trb = Bot(token=token)
        await trb.send_message(chat_id, empty_text)
        return
    title_text = await get_localized_text(uid, "history_title")
    lines = [title_text]
    base = page * 10
    for i, h in enumerate(items, 1):
        if h.closed_by == "TP":
            mark = "✅ TP"
        elif h.closed_by == "SL":
            mark = "❌ SL"
        else:
            mark = "⏱️ TIME"
        lines.append(
            f"{base+i}) {h.symbol} {h.side} {mark} PnL: {fmt_money(h.pnl_abs)} ({h.pnl_pct:+.2f}%) {ts_to_hms(h.closed_at)}"
        )
    kb = None
    if (base + len(items)) < total:
        more_text = await get_localized_text(uid, "show_more")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=more_text, callback_data=f"hist:{page+1}")]])
    owner = await store.get_bot_owner(uid)  
    token = await store.get_user_bot_token(owner)
    trb = Bot(token=token)
    await trb.send_message(chat_id, "\n".join(lines), reply_markup=kb)
@router.callback_query(F.data.startswith("hist:"))
async def on_history_more(cb: CallbackQuery):
    page = int(cb.data.split(":")[1])
    await send_history_page(cb.message.chat.id, cb.from_user.id, page=page)
    await cb.answer()
@router.message(F.text.in_(["Настройки", "Settings"]))
async def on_settings(m: Message, state: FSMContext):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(m.from_user.id)
        support_event = {
            "type": "settings_opened",
            "event_id": f"settings_{m.from_user.id}_{int(time.time() * 1000)}",
            "user_id": m.from_user.id,
            "username": m.from_user.username or str(m.from_user.id),
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(m.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Settings opened event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send settings_opened event to support queue: {e}")
    positions = await store.list_positions(m.from_user.id)
    if positions:
        error_text = await get_localized_text(m.from_user.id, "settings_cannot_change")
        menu_kb = get_main_menu_kb(await get_user_language(m.from_user.id))
        await m.answer(error_text, reply_markup=menu_kb)
        return
    await state.set_state(S.SETTINGS_MAIN)
    title_text = await get_localized_text(m.from_user.id, "settings_title")
    is_english = await is_english_user(m.from_user.id)    
    user = await store.get_user(m.from_user.id)
    current_language = user.language_code or "ru"
    current_amount = user.order_amount
    current_leverage = user.leverage
    kb = get_settings_main_kb(is_english, current_amount, current_leverage, current_language)
    await m.answer(title_text, reply_markup=kb)
@router.callback_query(F.data == "settings_order", S.SETTINGS_MAIN)
async def settings_order(cb: CallbackQuery, state: FSMContext):
    positions = await store.list_positions(cb.from_user.id)
    if positions:
        error_text = await get_localized_text(cb.from_user.id, "settings_cannot_change")
        await cb.answer(error_text, show_alert=True)
        await state.set_state(S.IDLE)
        return
    await state.set_state(S.SETTINGS_AMOUNT)    
    choose_text = await get_localized_text(cb.from_user.id, "settings_choose_amount")
    user = await store.get_user(cb.from_user.id)
    current_amount = user.order_amount
    await cb.message.edit_text(choose_text, reply_markup=get_settings_amount_kb())
    await cb.answer()
@router.callback_query(F.data == "settings_leverage", S.SETTINGS_MAIN)
async def settings_leverage(cb: CallbackQuery, state: FSMContext):
    positions = await store.list_positions(cb.from_user.id)
    if positions:
        error_text = await get_localized_text(cb.from_user.id, "settings_cannot_change")
        await cb.answer(error_text, show_alert=True)
        await state.set_state(S.IDLE)
        return
    await state.set_state(S.SETTINGS_LEVERAGE)    
    choose_text = await get_localized_text(cb.from_user.id, "settings_choose_leverage")
    user = await store.get_user(cb.from_user.id)
    current_leverage = user.leverage
    await cb.message.edit_text(choose_text, reply_markup=get_settings_leverage_kb())
    await cb.answer()
@router.callback_query(F.data == "settings_language", S.SETTINGS_MAIN)
async def settings_language(cb: CallbackQuery, state: FSMContext):
    await state.set_state(S.SETTINGS_LANGUAGE)    
    choose_text = "Выберите язык / Choose language:"
    await cb.message.edit_text(choose_text, reply_markup=get_settings_language_kb())
    await cb.answer()
@router.callback_query(F.data.startswith("set_lang:"), S.SETTINGS_LANGUAGE)
async def set_language(cb: CallbackQuery, state: FSMContext):
    lang = cb.data.split(":")[1]   
    user = await store.get_user(cb.from_user.id)
    user.language_code = lang
    await store.save_user(user)
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "language_selected",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username,
            "first_name": cb.from_user.first_name,
            "last_name": cb.from_user.last_name,
            "bot_username": TRADING_BOT_USERNAME,  
            "language": lang,
            "timestamp": time.time(),
        }
        await store.push_support_event(support_event)
        logger.info(f"language_selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"Failed to send language_selected event to support queue: {e}")
    await state.set_state(S.SETTINGS_MAIN)
    if lang == "ru":
        notification_text = "🇷🇺 Выбран русский"
    else:
        notification_text = "🇺🇸 English selected"
    await cb.answer(notification_text, show_alert=False)
    title_text = await get_localized_text(cb.from_user.id, "settings_title")
    is_english = await is_english_user(cb.from_user.id)
    user = await store.get_user(cb.from_user.id)
    current_amount = user.order_amount
    current_leverage = user.leverage
    current_language = user.language_code or "ru"
    kb = get_settings_main_kb(is_english, current_amount, current_leverage, current_language)
    await cb.message.edit_text(title_text, reply_markup=kb)
@router.callback_query(F.data.startswith("set_amount:"), S.SETTINGS_AMOUNT)
async def set_amount(cb: CallbackQuery, state: FSMContext):
    positions = await store.list_positions(cb.from_user.id)
    if positions:
        error_text = await get_localized_text(cb.from_user.id, "settings_cannot_change")
        await cb.answer(error_text, show_alert=True)
        await state.set_state(S.IDLE)
        return
    amount = float(cb.data.split(":")[1])
    user = await store.get_user(cb.from_user.id)
    user.order_amount = amount
    await store.save_user(user)
    await state.set_state(S.SETTINGS_MAIN)
    success_text = await get_localized_text(cb.from_user.id, "order_amount_changed")
    await cb.answer(success_text, show_alert=False)
    title_text = await get_localized_text(cb.from_user.id, "settings_title")
    is_english = await is_english_user(cb.from_user.id)
    user = await store.get_user(cb.from_user.id)
    current_amount = user.order_amount
    current_leverage = user.leverage
    kb = get_settings_main_kb(is_english, current_amount, current_leverage)
    await cb.message.edit_text(title_text, reply_markup=kb)
@router.callback_query(F.data.startswith("set_lev:"), S.SETTINGS_LEVERAGE)
async def set_leverage(cb: CallbackQuery, state: FSMContext):
    positions = await store.list_positions(cb.from_user.id)
    if positions:
        error_text = await get_localized_text(cb.from_user.id, "settings_cannot_change")
        await cb.answer(error_text, show_alert=True)
        await state.set_state(S.IDLE)
        return
    lev = int(cb.data.split(":")[1])
    user = await store.get_user(cb.from_user.id)
    user.leverage = lev
    await store.save_user(user)
    await state.set_state(S.SETTINGS_MAIN)
    success_text = await get_localized_text(cb.from_user.id, "leverage_changed")
    await cb.answer(success_text, show_alert=False)
    title_text = await get_localized_text(cb.from_user.id, "settings_title")
    is_english = await is_english_user(cb.from_user.id)
    user = await store.get_user(cb.from_user.id)
    current_amount = user.order_amount
    current_leverage = user.leverage
    kb = get_settings_main_kb(is_english, current_amount, current_leverage)
    await cb.message.edit_text(title_text, reply_markup=kb)
@router.callback_query(F.data == "settings_back", S.SETTINGS_MAIN)
async def settings_back(cb: CallbackQuery, state: FSMContext):
    await state.set_state(S.IDLE)
    user = await store.get_user(cb.from_user.id)
    user_lang = await get_user_language(cb.from_user.id)
    if user_lang == "en":
        balance_text = (
            f"Your balance: ${user.balance:.2f}\n"
            f"Leverage: x{user.leverage}, order amount ${user.order_amount:.2f}.\n\n"
            f"Choose action below ⤵️"
        )
    else:
        balance_text = (
            f"Ваш баланс: ${user.balance:.2f}\n"
            f"Плечо: x{user.leverage}, сумма ордера ${user.order_amount:.2f}.\n\n"
            f"Выберите действие ниже ⤵️"
        )
    menu_kb = get_main_menu_kb(user_lang)
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.message.answer(balance_text, reply_markup=menu_kb)
    await cb.answer()
@router.callback_query(F.data == "settings_back_to_main")
async def settings_back_to_main(cb: CallbackQuery, state: FSMContext):
    await state.set_state(S.SETTINGS_MAIN)
    title_text = await get_localized_text(cb.from_user.id, "settings_title")
    is_english = await is_english_user(cb.from_user.id)    
    user = await store.get_user(cb.from_user.id)
    current_amount = user.order_amount
    current_leverage = user.leverage
    current_language = user.language_code or "ru"
    kb = get_settings_main_kb(is_english, current_amount, current_leverage, current_language)
    await cb.message.edit_text(title_text, reply_markup=kb)
    await cb.answer()
@router.callback_query(F.data == "settings_back")
async def settings_back_to_main_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await store.get_user(cb.from_user.id)
    user_lang = await get_user_language(cb.from_user.id)
    if user_lang == "en":
        balance_text = (
            f"Your balance: ${user.balance:.2f}\n"
            f"Leverage: x{user.leverage}, order amount ${user.order_amount:.2f}.\n\n"
            f"Choose action below ⤵️"
        )
    else:
        balance_text = (
            f"Ваш баланс: ${user.balance:.2f}\n"
            f"Плечо: x{user.leverage}, сумма ордера ${user.order_amount:.2f}.\n\n"
            f"Выберите действие ниже ⤵️"
        )
    menu_kb = get_main_menu_kb(user_lang)
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.message.answer(balance_text, reply_markup=menu_kb)
    await cb.answer()
@router.message(F.text == "Settings")
async def on_settings_english(m: Message, state: FSMContext):
    positions = await store.list_positions(m.from_user.id)
    if positions:
        await m.answer("❌ <b>Cannot change settings</b>\n\n"
                      "You have open positions. Wait for them to close to change settings.",
                      reply_markup=main_menu_kb_english())
        return
    await state.set_state(S.SETTINGS_AMOUNT)    
    settings_amount_kb_english = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"${a}", callback_data=f"set_amount:{a}") for a in AMOUNTS[i:i+3]] 
        for i in range(0, len(AMOUNTS), 3)
    ])
    await m.answer("Step 1: choose <b>order amount</b>:", reply_markup=settings_amount_kb_english)
@router.callback_query(F.data == "stop_watch")
async def stop_watch(cb: CallbackQuery, state: FSMContext):
    user_id = cb.from_user.id
    bot_owner_id = await store.get_bot_owner(user_id)
    if not bot_owner_id:
        bot_owner_id = user_id
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "ai_trading_stopped",
            "event_id": f"ai_trading_stop_{user_id}_{int(time.time() * 1000)}",
            "user_id": user_id,
            "bot_owner_id": bot_owner_id,
            "username": cb.from_user.username or str(user_id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(user_id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ AI Trading stopped event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send ai_trading_stopped event to support queue: {e}")
    stopped_text = await get_localized_text(user_id, "ai_trading_stopped")   
    await state.set_state(S.IDLE)
    await store.remove_watcher(user_id)
    await cb.message.edit_text(stopped_text)
    await cb.answer()
@router.callback_query(F.data == "withdraw")
async def on_withdraw(cb: CallbackQuery, state: FSMContext):
    try:
        try:
            await _init_trading_bot_username_once()
            owner = await store.get_bot_owner(cb.from_user.id)
            support_event = {
                "type": "withdraw_opened",
                "event_id": f"withdraw_{cb.from_user.id}_{int(time.time() * 1000)}",
                "user_id": cb.from_user.id,
                "username": cb.from_user.username,
                "first_name": cb.from_user.first_name,
                "last_name": cb.from_user.last_name,
                "bot_username": TRADING_BOT_USERNAME,
                "timestamp": time.time(),
            }
            await store.push_support_event(support_event)
            logger.info(f"withdraw_opened event sent to support queue: {support_event}")
        except Exception as e:
            logger.error(f"Failed to send withdraw_opened event to support queue: {e}")
        try:
            await store.clear_assets_msg(cb.from_user.id)
        except Exception:
            pass
        withdraw_title = await get_localized_text(cb.from_user.id, "withdraw_title")
        user = await store.get_user(cb.from_user.id)
        available_text = await get_localized_text(cb.from_user.id, "withdraw_available", balance=user.balance)
        choose_method_text = await get_localized_text(cb.from_user.id, "withdraw_choose_method")
        text = f"{withdraw_title}\n\n{available_text}\n\n{choose_method_text}"
        kb = await get_localized_kb(cb.from_user.id, "withdraw_methods")
        try:
            if cb.message.photo:
                await cb.message.edit_caption(
                    caption=text,
                    reply_markup=kb
                )
            else:
                await cb.message.edit_text(
                    text=text,
                    reply_markup=kb
                )
        except Exception as e:
            logger.warning(f"Could not edit message: {e}")
            await cb.message.answer(text, reply_markup=kb)
        await cb.answer()
    except Exception as e:
        logger.exception(f"Unexpected error in on_withdraw: {e}")
        await cb.answer("Произошла ошибка", show_alert=True)
@router.callback_query(F.data == "wd_method_card", S.WD_CHOOSE_METHOD)
async def wd_method_card(cb: CallbackQuery, state: FSMContext):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "withdraw_card_selected",
            "event_id": f"withdraw_card_{cb.from_user.id}_{int(time.time() * 1000)}",
            "user_id": cb.from_user.id,
            "username": cb.from_user.username or str(cb.from_user.id),
            "first_name": cb.from_user.first_name or "",
            "last_name": cb.from_user.last_name or "",
            "bot_username": TRADING_BOT_USERNAME,
            "timestamp": time.time(),
            "bot": "ru" if await get_user_language(cb.from_user.id) == "ru" else "en"
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Withdraw card selected event sent to support queue: {support_event}")
    except Exception as e:
        logger.error(f"❌ Failed to send withdraw_card_selected event to support queue: {e}")
    is_english = await is_english_user(cb.from_user.id)
    if is_english:
        await cb.answer("Bank card withdrawals are not available for international users", show_alert=True)
        return
    title_text = await get_localized_text(cb.from_user.id, "withdraw_card_title")
    fio_text = await get_localized_text(cb.from_user.id, "withdraw_card_enter_fio")
    example_text = await get_localized_text(cb.from_user.id, "deposit_fio_example")
    text = (
        f"{title_text}\n\n"
        f"{fio_text}\n\n"
        f"<i>{example_text}</i>"
    )
    await state.set_state(S.WD_WAIT_FIO)
    await cb.message.edit_text(text)
    await cb.answer()
@router.callback_query(F.data == "kyc")
async def on_kyc_english(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    verification_text = (
        "To complete the verification procedure, the Client must have "
        "at least 20 (twenty) closed orders on the account. If the specified "
        "quantity is not available, the system may consider the verification "
        "incomplete until the requirement is met."
    )
    await cb.message.edit_text(verification_text)
    await cb.answer()
@router.callback_query(F.data == "reqs")
async def on_reqs_english(cb: CallbackQuery):
    try:
        await store.clear_assets_msg(cb.from_user.id)
    except Exception:
        pass
    data = _load_crypto_wallets()
    if not data:
        example = (
            '{\n'
            '  "USDT": {\n'
            '    "TRC20": "Txxx",\n'
            '    "BEP20": "0x000...usdt"\n'
            '  },\n'
            '  "BTC": {\n'
            '    "BTC": "bc1..."\n'
            '  }\n'
            '}'
        )
        txt = (
            "⚙️ <b>Requisites not configured</b>\n"
            "Create file <code>crypto_wallets.json</code> in project folder and restart bot.\n\n"
            "<b>Example:</b>\n<code>" + example + "</code>"
        )
        await cb.message.edit_text(txt)
        await cb.answer()
        return
    lines = ["💳 <b>Wallets for deposit</b>"]
    if any(isinstance(v, dict) for v in data.values()):
        for token, nets in data.items():
            if not isinstance(nets, dict): 
                continue
            lines.append(f"\n<b>{token}</b>")
            for net, addr in nets.items():
                lines.append(f"• {net}: <code>{addr}</code>")
    else:
        lines.append("\n<b>USDT</b>")
        for net, addr in data.items():
            lines.append(f"• {net}: <code>{addr}</code>")
    await cb.message.edit_text("\n".join(lines))
    await cb.answer()
@router.callback_query(F.data.startswith("set_amount:"))
async def set_amount(cb: CallbackQuery, state: FSMContext):
    positions = await store.list_positions(cb.from_user.id)
    if positions:
        await cb.answer("❌ Невозможно изменить настройки: есть открытые сделки", show_alert=True)
        await state.set_state(S.IDLE)
        return
    amount = float(cb.data.split(":")[1])
    user = await store.get_user(cb.from_user.id)
    user.order_amount = amount
    await store.save_user(user)
    await state.set_state(S.SETTINGS_LEVERAGE)
    await cb.message.edit_text("Шаг 2: выберите <b>плечо</b>:", reply_markup=settings_leverage_kb())
    await cb.answer()
@router.message(Command("help"))
async def on_help(m: Message):
    await m.answer("Это учебно‑развлекательная симуляция автотрейдинга по сигналам. Не инвестиционный продукт.", reply_markup=get_main_menu_kb("ru"))
@router.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return
    
    await state.clear()
    await message.answer(
        "Действие отменено",
        reply_markup=get_main_menu_kb(await get_user_language(message.from_user.id))
    )
async def notify_worker():
    global NOTIFY_WORKER_LAST_ACTIVE
    logger.info("🚀 Notify worker started for RU and EN queues")
    while True:
        try:
            NOTIFY_WORKER_LAST_ACTIVE = time.time()
            item_ru = await store.r.brpop("trading:notify:ru", timeout=1)
            item_en = await store.r.brpop("trading:notify:en", timeout=1)
            ru_len = await store.r.llen("trading:notify:ru")
            en_len = await store.r.llen("trading:notify:en")
            if ru_len > 10 or en_len > 10:
                logger.warning(f"📊 Queues are filling up: RU={ru_len}, EN={en_len}")
            elif ru_len > 0 or en_len > 0:
                logger.info(f"📊 Queue status: RU={ru_len}, EN={en_len}")
            if item_ru:
                _, data = item_ru
                logger.info(f"📨 Processing RU notification, length: {len(data)}")
                try:
                    ev_debug = json.loads(data.decode() if isinstance(data, (bytes, bytearray)) else data)
                    logger.info(f"🔍 RU notification debug - type: {ev_debug.get('type')}, user_id: {ev_debug.get('user_id')}")
                except Exception as e:
                    logger.error(f"❌ RU notification debug failed: {e}")
                await process_notification_item(data, "ru")
            if item_en:
                _, data = item_en
                logger.info(f"📨 Processing EN notification, length: {len(data)}")
                try:
                    ev_debug = json.loads(data.decode() if isinstance(data, (bytes, bytearray)) else data)
                    logger.info(f"🔍 EN notification debug - type: {ev_debug.get('type')}, user_id: {ev_debug.get('user_id')}")
                except Exception as e:
                    logger.error(f"❌ EN notification debug failed: {e}")
                await process_notification_item(data, "en")
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.exception("❌ notify_worker crashed: %s", e)
            await asyncio.sleep(1)
@router.message(Command("queue_status"))
async def check_queue_status(m: Message):
    ru_len = await store.r.llen("trading:notify:ru")
    en_len = await store.r.llen("trading:notify:en")
    last_ru = await store.r.lrange("trading:notify:ru", 0, 4)
    last_en = await store.r.lrange("trading:notify:en", 0, 4)
    status_text = (
        f"📊 Queue Status:\n"
        f"• RU queue: {ru_len} items\n"
        f"• EN queue: {en_len} items\n"
        f"• Workers active: {time.time() - NOTIFY_WORKER_LAST_ACTIVE < 10}\n"
        f"• Last worker activity: {time.time() - NOTIFY_WORKER_LAST_ACTIVE:.1f}s ago\n"
    )
    if last_ru:
        status_text += f"\n📨 Last RU items:\n"
        for i, item in enumerate(last_ru):
            try:
                data = json.loads(item)
                status_text += f"  {i+1}. {data.get('type')} - user {data.get('user_id')}"
                if data.get('amount'):
                    status_text += f" - ${data.get('amount')}"
                if data.get('asset'):
                    status_text += f" - {data.get('asset')}"
                status_text += "\n"
            except:
                status_text += f"  {i+1}. Invalid JSON\n"
    if last_en:
        status_text += f"\n📨 Last EN items:\n"
        for i, item in enumerate(last_en):
            try:
                data = json.loads(item)
                status_text += f"  {i+1}. {data.get('type')} - user {data.get('user_id')}"
                if data.get('amount'):
                    status_text += f" - ${data.get('amount')}"
                if data.get('asset'):
                    status_text += f" - {data.get('asset')}"
                status_text += "\n"
            except:
                status_text += f"  {i+1}. Invalid JSON\n"
    await m.answer(status_text)
async def handle_payment_approval_from_support(ev, bot_code):
    try:
        uid = int(ev.get("user_id"))
        amount = float(ev.get("amount", 0))
        event_id = ev.get("event_id", "")
        logger.info(f"💰 Processing payment approval from support: user={uid}, amount={amount}, event_id={event_id}")
        if amount > 0:
            user = await store.get_user(uid)
            before = user.balance
            user.balance = before + amount
            await store.save_user(user)
            await store.increment_deposits(uid, amount)
            await support_emit({
                "type": "balance_update",
                "user_id": uid,
                "before": before,
                "after": user.balance,
                "reason": "payment_approved"
            })
            payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
            new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
            msg = (
                f"✅ {payment_approved_text}\n"
                f"{new_balance_text}"
            )
            await safe_send_notification(uid, msg, bot_code)
            logger.info(f"✅ Payment approved successfully: user {uid}, +${amount}")
    except Exception as e:
        logger.exception(f"❌ Error processing payment approval from support: {e}")
async def process_notification_item(data, bot_code):
    global NOTIFY_WORKER_LAST_ACTIVE
    try:
        ev = json.loads(data.decode() if isinstance(data, (bytes, bytearray)) else data)
        logger.info(f"📨 Processing notification from support bot: type={ev.get('type')}, user_id={ev.get('user_id')}, bot_code={bot_code}")
    except Exception as e:
        logger.exception(f"❌ Bad notify json: {data}, error: {e}")
        return
    if ev.get("type") == "payment_approved" and ev.get("asset"):
        await handle_support_crypto_payment(ev, bot_code)
    elif ev.get("type") == "balance_credit":
        await handle_balance_credit(ev, bot_code)
    elif ev.get("type") == "payment":
        await handle_payment_event(ev, bot_code)
    elif ev.get("type") == "withdraw_approved":
        await handle_withdraw_approved(ev, bot_code)
    else:
        logger.warning(f"⚠️ Unknown notification type from support bot: {ev.get('type')}")
        logger.debug(f"⚠️ Full event data: {ev}")
    if ev.get("type") == "payment_approved" and ev.get("asset"):
        await process_payment_approved_crypto(ev, bot_code)
    if ev.get("type") in ["crypto_payment_approved", "crypto_payment", "payment"] and ev.get("asset"):
        await process_crypto_payment(ev, bot_code)
        try:
            uid = int(ev.get("user_id"))
            amount = float(ev.get("amount", 0))
            asset = ev.get("asset", "USDT")
            network = ev.get("network", "")
            logger.info(f"💰 Processing crypto payment approval: user={uid}, amount={amount}, asset={asset}, network={network}")
            if amount > 0:
                user = await store.get_user(uid)
                before = user.balance
                user.balance = before + amount
                await store.save_user(user)
                await store.increment_deposits(uid, amount)
                await support_emit({
                    "type": "balance_update",
                    "user_id": uid,
                    "before": before,
                    "after": user.balance,
                    "reason": "crypto_topup"
                })
                payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
                new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
                network_info = f" ({network})" if network else ""
                msg = (
                    f"✅ {payment_approved_text}\n"
                    f"💎 Asset: {asset}{network_info}\n"
                    f"{new_balance_text}"
                )
                await safe_send_notification(uid, msg, bot_code)
                logger.info(f"✅ Crypto payment processed successfully: user {uid}, +${amount}")
        except Exception as e:
            logger.exception(f"❌ Error processing crypto payment approval: {e}")
    elif ev.get("type") == "payment" and ev.get("status") == "approved":
        await handle_payment_approval_from_support(ev, bot_code)        
    elif ev.get("type") in ["payment_approved", "payment"] and ev.get("asset"):
        try:
            uid = int(ev.get("user_id"))
            amount = float(ev.get("amount", 0))
            asset = ev.get("asset", "USDT")
            network = ev.get("network", "")
            status = ev.get("status", "approved")
            logger.info(f"💰 Processing universal crypto payment: user={uid}, amount={amount}, asset={asset}, status={status}")
            if status == "approved" and amount > 0:
                user = await store.get_user(uid)
                before = user.balance
                user.balance = before + amount
                await store.save_user(user)
                await store.increment_deposits(uid, amount)
                await support_emit({
                    "type": "balance_update",
                    "user_id": uid,
                    "before": before,
                    "after": user.balance,
                    "reason": "crypto_topup"
                })
                payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
                new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
                msg = f"✅ {payment_approved_text}\n{new_balance_text}"
                if asset != "USDT" or network:
                    msg += f"\n💎 {asset}{' (' + network + ')' if network else ''}"
                await safe_send_notification(uid, msg, bot_code)
                logger.info(f"✅ Universal crypto payment processed: user {uid}, +${amount}")
        except Exception as e:
            logger.exception(f"❌ Error in universal crypto payment processing: {e}")
    elif ev.get("type") == "payment_rejected":
        try:
            uid = int(ev.get("user_id"))
            amount = ev.get("amount")
            asset = ev.get("asset", "")
            network = ev.get("network", "")
            logger.info(f"❌ Processing crypto payment rejection: user={uid}, amount={amount}, asset={asset}")
            base_text = await get_localized_text(uid, "payment_rejected")
            if amount:
                try:
                    text = base_text + f"\n\n💵 ${float(amount):.2f}"
                    if asset:
                        text += f"\n💎 {asset}{' (' + network + ')' if network else ''}"
                except:
                    text = base_text
            else:
                text = base_text
            await safe_send_notification(uid, text, bot_code)
            logger.info(f"❌ Crypto payment rejected notification sent to user {uid}")
        except Exception as e:
            logger.exception(f"❌ Error sending crypto payment rejected notification: {e}")
    elif ev.get("type") == "referral_registered":
        referrer_id = ev.get("referrer_id")
        referred_user_id = ev.get("referred_user_id")
        if referrer_id and referred_user_id:
            bonus_amount = 10.0  
            try:
                referrer = await store.get_user(referrer_id)
                referrer.balance += bonus_amount
                await store.save_user(referrer)
                referrer.stats.ref_earnings += bonus_amount
                await store.save_user(referrer)
                bonus_text = await get_localized_text(referrer_id, "referral_bonus_received")
                new_balance_text = await get_localized_text(referrer_id, "new_balance", balance=referrer.balance)
                await safe_send_notification(
                    referrer_id,
                    f"🎉 {bonus_text}\n\n"
                    f"Бонус: ${bonus_amount:.2f}\n"
                    f"{new_balance_text}",
                    bot_code
                )
                logger.info(f"✅ Реферальный бонус начислен: {referrer_id} +${bonus_amount} за пользователя {referred_user_id}")
            except Exception as e:
                logger.error(f"❌ Ошибка начисления реферального бонуса: {e}")
    elif ev.get("type") == "payment" and not ev.get("asset"):
        uid = int(ev.get("user_id"))
        status = ev.get("status")
        logger.info(f"💰 Processing regular payment: user_id={uid}, status={status}, bot_code={bot_code}")
        if status == "approved":
            amount = float(ev.get("amount", 0) or 0)
            user = await store.get_user(uid)
            before = user.balance
            user.balance = before + amount
            await store.save_user(user)
            try:
                await store.increment_deposits(uid, amount)
            except Exception:
                logger.exception("increment_deposits failed")
            try:
                await support_emit({
                    "type": "balance_update",
                    "user_id": uid,
                    "before": before,
                    "after": user.balance,
                    "reason": "card_topup"
                })
            except Exception:
                pass
            payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
            new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
            msg = f"{payment_approved_text}\n{new_balance_text}"
            logger.info(f"✅ Regular payment approved and balance updated: user {uid}, amount: {amount}, bot: {bot_code}")
            await safe_send_notification(uid, msg, bot_code)
        else:
            msg = await get_localized_text(uid, "payment_rejected")
            logger.info(f"❌ Regular payment rejected: user {uid}, bot: {bot_code}")
            await safe_send_notification(uid, msg, bot_code)
    elif ev.get("type") == "balance_credit":
        try:
            uid = int(ev.get("user_id"))
            amount = float(ev.get("amount") or 0)
            reason = ev.get("reason") or "balance_credit"
            logger.info(f"💵 Processing balance credit: user_id={uid}, amount={amount}, reason={reason}, bot_code={bot_code}")
            if amount <= 0:
                logger.warning(f"⚠️ balance_credit with non-positive amount: {ev}")
                return 
            user = await store.get_user(uid)
            before = user.balance
            user.balance = before + amount
            await store.save_user(user)
            try:
                await support_emit({
                    "type": "balance_update",
                    "user_id": uid,
                    "before": before,
                    "after": user.balance,
                    "reason": reason,
                })
            except Exception:
                logger.exception("Failed to emit balance_update for balance_credit") 
            logger.info(f"✅ balance_credit processed: user {uid}, +{amount}, reason={reason}, balance {before} -> {user.balance}")
        except Exception:
            logger.exception("❌ Failed to handle balance_credit event")
    elif ev.get("type") == "withdraw_decision":
        event_id = ev.get("event_id")
        status = str(ev.get("status") or "").lower()
        original_event_id = event_id.replace('_', ':')
        logger.info(f"💳 Processing withdraw decision: original_event_id={original_event_id}, status={status}, bot_code={bot_code}")
        pending_found = None
        uid = None
        all_users = await store.r.keys("user:*")
        logger.info(f"🔍 Searching in {len(all_users)} users for event_id: {original_event_id}")
        for user_key in all_users:
            try:
                user_id = int(user_key.decode().split(":")[1])
                pending_list = await store.list_pending_items(user_id)
                for pending in pending_list:
                    if pending.get("event_id") == original_event_id and pending.get("status") == "PENDING":
                        pending_found = pending
                        uid = user_id
                        logger.info(f"🎯 Found pending withdraw: user_id={uid}, amount={pending_found['amount']}")
                        break
                if pending_found:
                    break
            except Exception as e:
                logger.error(f"Error processing user {user_key}: {e}")
                continue 
        if not pending_found or not uid:
            logger.warning(f"❌ Withdraw request not found: {original_event_id}")
            return
        if status == "approved":
            pending_found["status"] = "APPROVED"
            pending_found["approved_at"] = time.time()
            await store.add_pending_item(uid, pending_found)
            logger.info(f"✅ Updating withdraw status to APPROVED for user {uid}")
            amount = pending_found["amount"]
            token = pending_found.get("token", "USDT")
            display_amount = f"${amount:.2f}"
            if token in ("ETH", "ETHEREUM"):
                eth_price = await fetch_usd_price("ETHUSDT")
                if eth_price > 0:
                    eth_amount = amount / eth_price
                    display_amount = f"{eth_amount:.6f} ETH (${amount:.2f})"
            elif token in ("BTC", "BITCOIN"):
                btc_price = await fetch_usd_price("BTCUSDT")
                if btc_price > 0:
                    btc_amount = amount / btc_price
                    display_amount = f"{btc_amount:.8f} BTC (${amount:.2f})"
            withdraw_approved_text = await get_localized_text(uid, "withdraw_approved")
            notification_text = (
                f"✅ {withdraw_approved_text}\n\n"
                f"Amount: {display_amount}\n"
                f"Network: {pending_found['network']}\n"
                f"Address: <code>{pending_found['address']}</code>\n\n"
                f"Funds have been sent to your account."
            )
            logger.info(f"📤 Sending approval notification to user {uid}")
            await safe_send_notification(uid, notification_text, bot_code)
        elif status == "rejected_requisites":
            await store.save_user(user)
            pending_found["status"] = "REJECTED_REQUISITES"
            pending_found["rejected_at"] = time.time()
            await store.add_pending_item(uid, pending_found)
            logger.info(f"❌ Updating withdraw status to REJECTED_REQUISITES for user {uid}")
            amount = pending_found["amount"]
            token = pending_found.get("token", "USDT")
            display_amount = f"${amount:.2f}"
            if token in ("ETH", "ETHEREUM"):
                eth_price = await fetch_usd_price("ETHUSDT")
                if eth_price > 0:
                    eth_amount = amount / eth_price
                    display_amount = f"{eth_amount:.6f} ETH (${amount:.2f})"
            elif token in ("BTC", "BITCOIN"):
                btc_price = await fetch_usd_price("BTCUSDT")
                if btc_price > 0:
                    btc_amount = amount / btc_price
                    display_amount = f"{btc_amount:.8f} BTC (${amount:.2f})"
            withdraw_rejected_text = await get_localized_text(uid, "withdraw_rejected")
            contact_support_text = await get_localized_text(uid, "contact_support")
            await safe_send_notification(
                uid,
                f"❌ {withdraw_rejected_text}\n\n"
                f"Amount: {display_amount}\n\n"
                f"{contact_support_text}",
                bot_code
            )
    elif ev.get("type") == "balance_update":
        uid = int(ev.get("user_id"))
        reason = ev.get("reason", "")
        after_balance = ev.get("after", 0)
        if reason == "trade_pnl":
            pnl_text = await get_localized_text(uid, "trade_pnl_update")
            new_balance_text = await get_localized_text(uid, "new_balance", balance=after_balance)
            await safe_send_notification(
                uid,
                f"📊 {pnl_text}\n{new_balance_text}",
                bot_code
            )
    elif ev.get("type") == "system_notification":
        uid = int(ev.get("user_id"))
        message = ev.get("message", "")
        if message:
            await safe_send_notification(uid, message, bot_code)
    else:
        logger.warning(f"⚠️ Unknown notification type: {ev.get('type')}")
        logger.warning(f"⚠️ Full notification data: {ev}")
async def handle_support_crypto_payment(ev, bot_code):
    try:
        uid = int(ev.get("user_id"))
        amount = float(ev.get("amount", 0))
        asset = ev.get("asset", "USDT").upper()
        network = ev.get("network", "")
        event_id = ev.get("event_id", "")
        logger.info(f"💰 CRYPTO PAYMENT from support bot: user={uid}, amount={amount}, asset={asset}, network={network}, event_id={event_id}")
        if amount <= 0:
            logger.error(f"❌ Invalid amount in crypto payment: {amount}")
            return
        if not asset:
            logger.error(f"❌ Missing asset in crypto payment")
            return
        user = await store.get_user(uid)
        if not user:
            logger.error(f"❌ User not found: {uid}")
            try:
                user = User(user_id=uid)
                await store.save_user(user)
                logger.info(f"✅ Created new user: {uid}")
            except Exception as e:
                logger.error(f"❌ Failed to create user: {e}")
                return
        before_balance = user.balance
        user.balance += amount
        await store.save_user(user)
        await store.increment_deposits(uid, amount)
        await store.update_user_activity(uid)
        await support_emit({
            "type": "balance_update",
            "user_id": uid,
            "username": user.username,
            "before": before_balance,
            "after": user.balance,
            "reason": "crypto_deposit",
            "asset": asset,
            "network": network,
            "amount": amount,
            "source_event_id": event_id,
            "timestamp": time.time()
        })
        payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
        new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
        message_lines = [
            f"✅ {payment_approved_text}",
            f"💎 {asset}{f' ({network})' if network else ''}",
            f"{new_balance_text}"
        ]
        if event_id:
            message_lines.append(f"📋 ID: {event_id}")
        msg = "\n".join(message_lines)
        success = await guaranteed_send_notification(uid, msg, bot_code, "crypto_payment")
        if success:
            logger.info(f"✅ CRYPTO PAYMENT SUCCESS: user {uid}, +${amount}, balance: {before_balance} → {user.balance}")
            audit_log = (
                f"💰 CRYPTO_DEPOSIT_AUDIT: "
                f"user_id={uid}, amount=${amount}, asset={asset}, "
                f"network={network}, event_id={event_id}, "
                f"balance_before=${before_balance}, balance_after=${user.balance}"
            )
            logger.info(audit_log)
        else:
            logger.error(f"❌ CRYPTO PAYMENT NOTIFICATION FAILED: user {uid}, but balance was updated")
    except Exception as e:
        logger.exception(f"❌ CRITICAL ERROR in crypto payment processing: {e}")
        await save_payment_error(ev, str(e))
async def guaranteed_send_notification(chat_id: int, text: str, bot_code: str, notification_type: str = "general"):
    max_retries = 8  
    last_error = None
    for attempt in range(max_retries):
        try:
            owner = await store.get_bot_owner(m.from_user.id)
            token = await store.get_user_bot_token(owner)
            trb = Bot(token=token)
            await trb.send_message(
                chat_id=chat_id, 
                text=text, 
                reply_markup=assets_button_kb(bot_code),
                parse_mode="HTML"
            )
            logger.info(f"✅ {notification_type.upper()} notification sent to {chat_id} (attempt {attempt + 1})")
            return True
        except TelegramRetryAfter as e:
            delay = float(e.retry_after)
            logger.warning(f"⚠️ Rate limit for {notification_type} notification to {chat_id}, waiting {delay}s")
            await asyncio.sleep(delay)
        except Exception as e:
            last_error = e
            error_msg = str(e).lower()
            if any(phrase in error_msg for phrase in ["chat not found", "user not found", "bot was blocked"]):
                logger.error(f"❌ User {chat_id} unavailable for {notification_type}, stopping retries: {error_msg}")
                break
            if "forbidden" in error_msg:
                logger.error(f"❌ Bot blocked by user {chat_id} for {notification_type}")
                break
            logger.warning(f"⚠️ {notification_type} notification failed to {chat_id} (attempt {attempt + 1}): {error_msg}")
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0.5, 2.0)
                logger.info(f"🔄 Retrying {notification_type} notification to {chat_id} in {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
    logger.error(f"❌ CRITICAL: Failed to send {notification_type} notification to {chat_id} after {max_retries} attempts")
    await save_failed_notification(chat_id, text, bot_code, notification_type, str(last_error))
    return False
async def handle_balance_credit(ev, bot_code):
    try:
        uid = int(ev.get("user_id"))
        amount = float(ev.get("amount", 0))
        reason = ev.get("reason", "balance_credit")
        logger.info(f"💳 BALANCE CREDIT from support: user={uid}, amount={amount}, reason={reason}")
        if amount <= 0:
            return
        user = await store.get_user(uid)
        if not user:
            logger.error(f"❌ User not found for balance credit: {uid}")
            return
        before_balance = user.balance
        user.balance += amount
        await store.save_user(user)
        await store.increment_deposits(uid, amount)
        payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
        new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
        msg = f"✅ {payment_approved_text}\n{new_balance_text}"
        await guaranteed_send_notification(uid, msg, bot_code, "balance_credit")
        logger.info(f"✅ BALANCE CREDIT SUCCESS: user {uid}, +${amount}")
    except Exception as e:
        logger.exception(f"❌ Error in balance credit processing: {e}")
async def handle_payment_event(ev, bot_code):
    try:
        uid = int(ev.get("user_id"))
        status = ev.get("status", "")
        amount = float(ev.get("amount", 0))
        logger.info(f"💳 PAYMENT EVENT from support: user={uid}, status={status}, amount={amount}")
        if status == "approved" and amount > 0:
            user = await store.get_user(uid)
            if user:
                before_balance = user.balance
                user.balance += amount
                await store.save_user(user)
                await store.increment_deposits(uid, amount)
                payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
                new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
                msg = f"✅ {payment_approved_text}\n{new_balance_text}"
                await guaranteed_send_notification(uid, msg, bot_code, "payment")
                logger.info(f"✅ PAYMENT APPROVED: user {uid}, +${amount}")
        elif status == "denied":
            msg = await get_localized_text(uid, "payment_rejected")
            await guaranteed_send_notification(uid, msg, bot_code, "payment_denied")
            
    except Exception as e:
        logger.exception(f"❌ Error in payment event processing: {e}")
async def save_payment_error(event_data, error_message):
    try:
        error_record = {
            "type": "payment_processing_error",
            "original_event": event_data,
            "error": error_message,
            "timestamp": time.time(),
            "resolved": False
        }
        await store.r.lpush("payment_errors", json.dumps(error_record))
        logger.info(f"📋 Saved payment error for manual processing")
    except Exception as e:
        logger.error(f"❌ Failed to save payment error: {e}")
async def save_failed_notification(chat_id, text, bot_code, notification_type, error):
    try:
        failed_notification = {
            "chat_id": chat_id,
            "text": text,
            "bot_code": bot_code,
            "type": notification_type,
            "error": error,
            "timestamp": time.time(),
            "attempts": 1
        }
        await store.r.lpush("failed_notifications", json.dumps(failed_notification))
    except Exception as e:
        logger.error(f"❌ Failed to save failed notification: {e}")
async def support_emit(data):
    logger.info(f"Support emit: {data}")
    # Реализовать логику отправки в поддержку
def get_banks_kb(is_english):
    # Реализовать клавиатуру банков
    return InlineKeyboardMarkup(inline_keyboard=[])
@router.message(Command("support_events_status"))
async def support_events_status(m: Message):
    ru_len = await store.r.llen("trading:notify:ru")
    en_len = await store.r.llen("trading:notify:en")
    recent_events = []
    for queue in ["trading:notify:ru", "trading:notify:en"]:
        items = await store.r.lrange(queue, 0, 4)
        for item in items:
            try:
                ev = json.loads(item)
                recent_events.append({
                    "queue": queue,
                    "type": ev.get("type"),
                    "user_id": ev.get("user_id"),
                    "amount": ev.get("amount"),
                    "asset": ev.get("asset"),
                    "timestamp": ev.get("timestamp")
                })
            except:
                pass
    error_count = await store.r.llen("payment_errors")
    failed_notifications_count = await store.r.llen("failed_notifications")
    status_text = (
        "🔧 **Support Events Status**\n\n"
        f"• RU queue: {ru_len} items\n"
        f"• EN queue: {en_len} items\n"
        f"• Payment errors: {error_count}\n"
        f"• Failed notifications: {failed_notifications_count}\n"
        f"• Workers active: {time.time() - NOTIFY_WORKER_LAST_ACTIVE < 10}\n\n"
        f"**Recent events:**\n"
    )
    for i, event in enumerate(recent_events[:5], 1):
        event_info = f"{i}. {event['type']} - User {event['user_id']}"
        if event.get('amount'):
            event_info += f" - ${event['amount']}"
        if event.get('asset'):
            event_info += f" {event['asset']}"
        status_text += event_info + "\n"
    await m.answer(status_text)
@router.message(Command("force_process_events"))
async def force_process_events(m: Message):
    ru_len = await store.r.llen("trading:notify:ru")
    en_len = await store.r.llen("trading:notify:en")
    processed = 0
    for queue in ["trading:notify:ru", "trading:notify:en"]:
        while True:
            item = await store.r.rpop(queue)
            if not item:
                break
            try:
                ev = json.loads(item)
                bot_code = "ru" if "ru" in queue else "en"
                await process_notification_item(item, bot_code)
                processed += 1
            except Exception as e:
                logger.error(f"❌ Error processing event: {e}")
    await m.answer(f"✅ Принудительно обработано {processed} событий\nОчередь RU: {ru_len}, EN: {en_len}")
async def process_crypto_payment(ev, bot_code):
    try:
        uid = int(ev.get("user_id"))
        amount = float(ev.get("amount", 0))
        asset = ev.get("asset", "USDT").upper()
        network = ev.get("network", "")
        tx_hash = ev.get("tx_hash", "")
        status = ev.get("status", "approved")
        logger.info(f"💰 Processing crypto payment: user={uid}, amount={amount}, asset={asset}, network={network}, status={status}")
        if status != "approved" or amount <= 0:
            logger.warning(f"⚠️ Skipping crypto payment - invalid status or amount: status={status}, amount={amount}")
            return
        user = await store.get_user(uid)
        if not user:
            logger.error(f"❌ User not found: {uid}")
            return
        before_balance = user.balance
        user.balance += amount
        await store.save_user(user)
        await store.increment_deposits(uid, amount)
        await support_emit({
            "type": "balance_update",
            "user_id": uid,
            "username": user.username,
            "before": before_balance,
            "after": user.balance,
            "reason": "crypto_deposit",
            "asset": asset,
            "network": network,
            "amount": amount
        })
        payment_approved_text = await get_localized_text(uid, "payment_approved", amount=amount)
        new_balance_text = await get_localized_text(uid, "new_balance", balance=user.balance)
        message_parts = [
            f"✅ {payment_approved_text}",
            f"💎 {asset}{f' ({network})' if network else ''}",
            f"{new_balance_text}"
        ]
        if tx_hash:
            message_parts.append(f"🔗 TX: {tx_hash}")
        msg = "\n".join(message_parts)
        success = await safe_send_notification(uid, msg, bot_code)
        if success:
            logger.info(f"✅ Crypto payment processed successfully: user {uid}, +${amount}, {asset}{f' on {network}' if network else ''}")
        else:
            logger.error(f"❌ Failed to send notification for crypto payment: user {uid}")
        logger.info(f"💰 CRYPTO_DEPOSIT_CONFIRMED: user_id={uid}, amount=${amount}, asset={asset}, balance_before=${before_balance}, balance_after=${user.balance}")
    except Exception as e:
        logger.exception(f"❌ Critical error in process_crypto_payment: {e}")
        try:
            error_msg = f"❌ CRYPTO_PAYMENT_ERROR: {str(e)}"
            await support_emit({
                "type": "system_alert",
                "message": error_msg,
                "event_data": ev,
                "timestamp": time.time()
            })
        except Exception:
            pass
async def safe_send_notification(chat_id: int, text: str, bot_code: str, max_retries: int = 5):
    last_error = None
    for attempt in range(max_retries):
        try:
            owner = await store.get_bot_owner(m.from_user.id)
            token = await store.get_user_bot_token(owner)
            trb = Bot(token=token)
            await trb.send_message(
                chat_id=chat_id, 
                text=text, 
                reply_markup=assets_button_kb(bot_code),
                parse_mode="HTML"
            )
            logger.info(f"✅ Notification sent to {chat_id} (attempt {attempt + 1})")
            return True
        except TelegramRetryAfter as e:
            delay = float(e.retry_after)
            logger.warning(f"⚠️ Rate limit for {chat_id}, retrying in {delay}s (attempt {attempt + 1})")
            await asyncio.sleep(delay)
            last_error = e
        except Exception as e:
            last_error = e
            logger.warning(f"⚠️ Failed to send notification to {chat_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.uniform(0, 1)
                await asyncio.sleep(wait_time)
    logger.error(f"❌ FAILED to send notification to {chat_id} after {max_retries} attempts: {last_error}")
    try:
        retry_event = {
            "type": "failed_notification",
            "chat_id": chat_id,
            "text": text,
            "bot_code": bot_code,
            "timestamp": time.time(),
            "last_error": str(last_error)
        }
        await store.r.lpush(f"notify_retry:{bot_code}", json.dumps(retry_event))
        logger.info(f"📦 Queued failed notification for retry: user {chat_id}")
    except Exception as e:
        logger.error(f"❌ Failed to queue retry: {e}")
    return False
async def retry_failed_notifications():
    logger.info("🔄 Starting failed notifications retry worker")
    while True:
        try:
            for bot_code in ["ru", "en"]:
                queue_key = f"notify_retry:{bot_code}"
                item = await store.r.brpop(queue_key, timeout=5)
                if item:
                    _, data = item
                    try:
                        ev = json.loads(data)
                        chat_id = ev.get("chat_id")
                        text = ev.get("text")
                        original_bot_code = ev.get("bot_code")
                        logger.info(f"🔄 Retrying failed notification for user {chat_id}")
                        success = await safe_send_notification(
                            chat_id, text, original_bot_code, max_retries=3
                        )
                        if success:
                            logger.info(f"✅ Retry successful for user {chat_id}")
                        else:
                            logger.error(f"❌ Retry failed for user {chat_id}")
                    except Exception as e:
                        logger.error(f"❌ Error processing retry item: {e}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.exception(f"❌ Retry worker crashed: {e}")
            await asyncio.sleep(10)
async def on_startup():
    logger.info("[BOT] Started")
    global TRADING_BOT_USERNAME
    try:
        me = await bot.get_me()
        if me and me.username and (TRADING_BOT_USERNAME == "your_trading_bot"):
            TRADING_BOT_USERNAME = me.username
            logger.info(f"🤖 Bot username resolved: {TRADING_BOT_USERNAME}")
    except Exception:
        logger.exception("Failed to resolve bot username")
    try:
        for i in range(3): 
            spawn(notify_worker(), name=f"notify_worker_{i}")
        logger.info("✅ Started 3 notification workers")
        spawn(retry_failed_notifications(), name="retry_worker")
        logger.info("✅ Started retry worker for failed notifications")
        spawn(queue_monitor_worker(), name="queue_monitor")
        logger.info("✅ Started queue monitor worker")
    except Exception as e:
        logger.error(f"❌ Failed to start workers: {e}")
    try:
        await _close_leftover_open_positions()
    except Exception:
        logger.exception("Cleanup on startup failed")
    try:
        spawn(cleanup_inactive_watchers(), name="watchers_cleanup")
    except Exception:
        logger.exception("Failed to start watchers cleanup")
async def queue_monitor_worker():
    while True:
        try:
            ru_len = await store.r.llen("trading:notify:ru")
            en_len = await store.r.llen("trading:notify:en")
            if ru_len > 50 or en_len > 50:
                logger.warning(f"🚨 QUEUE CONGESTION: RU={ru_len}, EN={en_len}")
            await asyncio.sleep(30)  
        except Exception as e:
            logger.error(f"❌ Queue monitor error: {e}")
            await asyncio.sleep(60)
async def on_shutdown():
    logger.info("[BOT] Shutting down…")
    try:
        await r.close()
    except Exception:
        logger.exception("Error closing Redis")
class UserActivityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if hasattr(event, 'from_user') and event.from_user:
            try:
                await store.update_user_activity(event.from_user.id)
            except Exception:
                pass  
        return await handler(event, data)
async def process_notify_queue():
    logger.info("🚀 TRADING BOT: Notify queue worker started for RU bot")
    while True:
        try:
            item = await r.brpop("trading:notify:ru", timeout=5)
            if not item:
                await asyncio.sleep(0.1)
                continue
            _, data = item
            logger.info(f"📨 TRADING BOT: Received notify event, length: {len(data)}")
            try:
                event = json.loads(data.decode() if isinstance(data, (bytes, bytearray)) else data)
                logger.info(f"🔍 TRADING BOT: Parsed event - type: {event.get('type')}, user_id: {event.get('user_id')}")
                if event.get("type") == "payment_approved":
                    user_id = event.get("user_id")
                    amount = event.get("amount")
                    asset = event.get("asset", "USDT")
                    network = event.get("network", "")
                    logger.info(f"💰 TRADING BOT: Processing payment_approved for user {user_id}, amount {amount}, asset {asset}")
                    if amount and user_id:
                        user = await store.get_user(int(user_id))
                        if user:
                            before_balance = user.balance
                            user.balance += float(amount)
                            await store.save_user(user)
                            await store.increment_deposits(int(user_id), float(amount))
                            payment_approved_text = await get_localized_text(int(user_id), "payment_approved", amount=float(amount))
                            new_balance_text = await get_localized_text(int(user_id), "new_balance", balance=user.balance)
                            message = f"✅ {payment_approved_text}\n{new_balance_text}"
                            if asset != "USDT" or network:
                                message += f"\n💎 {asset}{' (' + network + ')' if network else ''}"
                            await safe_send_text(int(user_id), message)
                            logger.info(f"✅ TRADING BOT: Payment processed successfully - user {user_id}, +${amount}, new balance: {user.balance}")
                        else:
                            logger.error(f"❌ TRADING BOT: User not found: {user_id}")
                    else:
                        logger.error(f"❌ TRADING BOT: Invalid payment_approved data: user_id={user_id}, amount={amount}")
                elif event.get("type") == "payment_rejected":
                    user_id = event.get("user_id")
                    if user_id:
                        message = await get_localized_text(int(user_id), "payment_rejected")
                        await safe_send_text(int(user_id), f"❌ {message}")
                        logger.info(f"❌ TRADING BOT: Payment rejected notification sent to user {user_id}")
                else:
                    logger.warning(f"⚠️ TRADING BOT: Unknown event type: {event.get('type')}")
            except json.JSONDecodeError as e:
                logger.error(f"❌ TRADING BOT: Failed to parse JSON: {e}, data: {data}")
            except Exception as e:
                logger.error(f"❌ TRADING BOT: Error processing event: {e}")
        except Exception as e:
            logger.error(f"❌ TRADING BOT: Queue worker error: {e}")
            await asyncio.sleep(1)
async def start_background_tasks():
    logger.info("Starting background tasks...")
    asyncio.create_task(check_active_users_blocked_status(), name="blocked_status_checker")
    asyncio.create_task(notify_worker(), name="notify_worker")
    asyncio.create_task(process_notify_queue(), name="process_notify_queue")
    asyncio.create_task(cleanup_inactive_watchers(), name="cleanup_inactive_watchers")
    logger.info("✅ All background tasks started")
async def on_startup():
    logger.info("🚀 Bot starting up...")
    logger.info("🔄 Starting cleanup of leftover open positions...")
    await _close_leftover_open_positions()
    asyncio.create_task(cleanup_inactive_watchers(), name="cleanup_watchers")
    asyncio.create_task(notify_worker(), name="notify_worker")
    logger.info("✅ Startup completed")
async def on_shutdown():
    logger.info("🛑 Bot shutting down...")
async def main():
    global bot
    TRADE_BOT_TOKEN = os.getenv("TRADE_BOT_TOKEN")
    if not TRADE_BOT_TOKEN and len(sys.argv) > 2 and sys.argv[1] == "--token":
        TRADE_BOT_TOKEN = sys.argv[2]
    if not TRADE_BOT_TOKEN:
        TRADE_BOT_TOKEN = "8385870509:AAHdzf0X2wDITzh2hBMmY7g4CHBJ-ab8jzU"
        logger.warning("⚠️ Using fallback bot token. Consider setting TRADE_BOT_TOKEN environment variable.")
    try:
        bot = Bot(
            token=TRADE_BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        me = await bot.get_me()
        logger.info(f"🤖 Bot initialized: @{me.username} (ID: {me.id})")
    except Exception as e:
        logger.error(f"❌ Failed to initialize bot: {e}")
        return
    try:
        await _init_trading_bot_username_once()
        logger.info(f"✅ Bot username: @{TRADING_BOT_USERNAME}")
    except Exception as e:
        logger.error(f"⚠️ Failed to get bot username: {e}")
    try:
        await start_background_tasks()
        logger.info("✅ Background tasks started")
    except Exception as e:
        logger.error(f"⚠️ Error starting background tasks: {e}")
    try:
        await _close_leftover_open_positions()
        logger.info("✅ Cleanup of leftover positions completed")
    except Exception as e:
        logger.error(f"⚠️ Error during cleanup: {e}")
    dp = Dispatcher()
    dp.include_router(router)
    dp.include_router(channel_router)
    dp.errors.register(errors_handler)
    allowed_updates = [
        "message",
        "callback_query",
        "channel_post",
        "chat_member",
        "my_chat_member"
    ]
    logger.info(f"🚀 Bot @{TRADING_BOT_USERNAME} is starting polling...")
    restart_count = 0
    max_restarts = 10
    while restart_count < max_restarts:
        try:
            await dp.start_polling(
                bot, 
                allowed_updates=allowed_updates,
                polling_timeout=30,
                close_bot_session=False
            )
        except KeyboardInterrupt:
            logger.info("👋 Bot stopped by user")
            break
        except asyncio.CancelledError:
            logger.info("🛑 Bot task cancelled")
            break
        except Exception as e:
            restart_count += 1
            logger.error(f"💥 Bot crashed (restart {restart_count}/{max_restarts}): {e}")
            if restart_count >= max_restarts:
                logger.error("🔥 Maximum restarts reached. Exiting...")
                break
            wait_time = min(2 ** restart_count, 60)  
            logger.info(f"⏳ Restarting in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            continue
    logger.info("🔄 Cleaning up resources...")
    try:
        await bot.session.close()
    except Exception as e:
        logger.error(f"⚠️ Error closing bot session: {e}")
    logger.info("✅ Bot shutdown completed")
if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: None)
    signal.signal(signal.SIGTERM, lambda s, f: None)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"🔥 Fatal error in main: {e}")
        sys.exit(1)
async def _send_user_action_event_to_support(*, bot_username: str, owner_user_id: int | None,
                                             user_id: int, user_username: str | None,
                                             action: str, callback_data: str | None,
                                             screen_text: str | None):
    ev = {
        "type": "user_action",
        "subtype": action,  
        "ts": time.time(),
        "bot_username": bot_username,
        "owner_user_id": owner_user_id,
        "user_id": user_id,
        "user_username": user_username,
        "callback_data": callback_data,
        "screen_text": screen_text,
    }
    try:
        await r.xadd(SUPPORT_EVENTS_STREAM, {"data": json.dumps(ev)})
    except Exception:
        try:
            await r.publish(SUPPORT_EVENTS_STREAM, json.dumps(ev))
        except Exception:
            pass
async def _resolve_owner_user_id(bot_username: str) -> int | None:
    try:
        raw = await r.hget(BOT_OWNER_INDEX_KEY, bot_username)
        if not raw:
            return None
        s = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
        return int(s)
    except Exception:
        return None
class UserClickLoggerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        bot_username = str(TRADING_BOT_USERNAME)
        owner_user_id = await _resolve_owner_user_id(bot_username)
        if isinstance(event, CallbackQuery):
            try:
                await _send_user_action_event_to_support(
                    bot_username=bot_username,
                    owner_user_id=owner_user_id,
                    user_id=event.from_user.id,
                    user_username=event.from_user.username,
                    action="callback",
                    callback_data=event.data,
                    screen_text=(event.message.text if event.message else None),
                )
            except Exception:
                pass
        elif isinstance(event, Message) and (event.text or event.caption):
            txt = event.text or event.caption
            if txt.startswith("/") or len(txt) <= 64: 
                try:
                    await _send_user_action_event_to_support(
                        bot_username=bot_username,
                        owner_user_id=owner_user_id,
                        user_id=event.from_user.id,
                        user_username=event.from_user.username,
                        action="message",
                        callback_data=None,
                        screen_text=txt,
                    )
                except Exception:
                    pass
        return await handler(event, data)
@router.my_chat_member()
async def on_my_chat_member(update: MyChatMember):
    try:
        user_id = update.from_user.id
        new_status = update.new_chat_member.status
        old_status = update.old_chat_member.status
        if new_status in ['kicked', 'left'] and old_status in ['member', 'administrator']:
            logger.info(f"User {user_id} blocked the bot (my_chat_member)")
            await send_bot_blocked_event(user_id, f"status_changed_{old_status}_to_{new_status}")
        elif new_status in ['member', 'administrator'] and old_status in ['kicked', 'left']:
            logger.info(f"User {user_id} unblocked the bot")
            await send_bot_unblocked_event(user_id)
    except Exception as e:
        logger.error(f"Error in my_chat_member handler: {e}")
async def send_bot_unblocked_event(user_id: int):
    try:
        await _init_trading_bot_username_once()
        owner = await store.get_bot_owner(cb.from_user.id)
        support_event = {
            "type": "bot_unblocked",  
            "event_id": f"bot_unblocked_{user_id}_{int(time.time() * 1000)}",
            "user_id": user_id,
            "timestamp": time.time(),
            "bot_username": TRADING_BOT_USERNAME
        }
        await store.push_support_event(support_event)
        logger.info(f"✅ Bot unblocked event sent to support: user {user_id}")
    except Exception as e:
        logger.error(f"Failed to send bot_unblocked event: {e}")
@router.message(Command("crypto_status"))
async def crypto_status(m: Message):
    ru_len = await store.r.llen("trading:notify:ru")
    en_len = await store.r.llen("trading:notify:en")
    retry_ru_len = await store.r.llen("notify_retry:ru")
    retry_en_len = await store.r.llen("notify_retry:en")
    status_text = (
        "🔧 **Crypto Payments Status**\n\n"
        f"• RU notifications queue: {ru_len}\n"
        f"• EN notifications queue: {en_len}\n"
        f"• RU retry queue: {retry_ru_len}\n"
        f"• EN retry queue: {retry_en_len}\n"
        f"• Workers active: {time.time() - NOTIFY_WORKER_LAST_ACTIVE < 10}\n"
        f"• Last activity: {time.time() - NOTIFY_WORKER_LAST_ACTIVE:.1f}s ago\n"
    )
    await m.answer(status_text)
dp.update.middleware(UserClickLoggerMiddleware())
@router.message()
async def on_forwarded_signal_message(message: Message):
    try:
        try:
            if message.date and message.date.replace(tzinfo=timezone.utc) < BOT_START_TIME:
                logger.debug("Skip old forwarded message (%s < BOT_START_TIME)", message.date)
                return
        except Exception:
            pass
        origin_ok = False
        chan_id_env = os.getenv("SIGNAL_CHANNEL_ID")
        chan_id_int = None
        if chan_id_env:
            try:
                chan_id_int = int(chan_id_env)
            except Exception:
                chan_id_int = None
        fchat_id = None
        try:
            if getattr(message, "forward_from_chat", None):
                fchat_id = message.forward_from_chat.id
        except Exception:
            pass
        try:
            fo = getattr(message, "forward_origin", None)
            if fo and getattr(fo, "chat", None):
                fchat_id = getattr(fo.chat, "id", None) or fchat_id
        except Exception:
            pass
        if fchat_id is not None:
            if chan_id_int is None or fchat_id == chan_id_int:
                origin_ok = True
        if not origin_ok and chan_id_int is None:
            origin_ok = True
        if not origin_ok:
            logger.debug("Message ignored: not a forward from configured channel (got %s, need %s)", fchat_id, chan_id_int)
            return
        text = (message.caption or message.text or "").strip()
        if not text:
            logger.debug("Forwarded message ignored: empty text/caption")
            return
        ps = parse_signal(text)
        if not ps:
            logger.debug("Forwarded message did not match signal pattern")
            return
        logger.info("Parsed (forwarded) signal: %s %s side=%s entry=%s tp=%s sl=%s",
                    ps.symbol, ps.tf, ps.side, ps.entry, ps.tp, ps.sl)
        await store.set_last_signal(ps)
        try:
            fmsg_id = None
            try:
                fmsg_id = getattr(message, "forward_from_message_id", None)
            except Exception:
                pass
            try:
                fo = getattr(message, "forward_origin", None)
                if fo and getattr(fo, "message_id", None):
                    fmsg_id = getattr(fo, "message_id", None) or fmsg_id
            except Exception:
                pass
            if fchat_id is not None and fmsg_id is not None:
                first = await store.mark_signal_message(fchat_id, fmsg_id)
                if not first:
                    logger.info("Duplicate forwarded message (%s,%s) skipped", fchat_id, fmsg_id)
                    return
        except Exception:
            pass
        try:
            fp = signal_fingerprint(ps)
            first_fp = await store.mark_signal_fingerprint(fp)
            if not first_fp:
                logger.info("Duplicate forwarded signal fingerprint skipped: %s", fp)
                return
        except Exception:
            pass
        header = "🛰 <b>Новый сигнал</b>" if not ps.strength else f"🛰 <b>Новый сигнал — {ps.strength}</b>"
        card_text = (
            f"{header}\n"
            f"• Пара: {ps.symbol}\n"
            f"• Таймфрейм: {ps.tf}\n"
            f"• Направление: {'🟢' if ps.side==Side.LONG else '🔴'} {ps.side.value}\n"
            f"• Цена входа: {ps.entry}\n"
            f"• TP: {ps.tp} | SL: {ps.sl}\n"
            f"• Реком. сумма: ${ps.rec_amount}\n"
            f"• Дата: {ps.date_utc}"
        )
        watchers = await store.list_watchers()
        if not watchers:
            logger.info("No watchers; forwarded signal stored but not broadcast")
            return
        sent = 0
        for uid in watchers:
            try:
                owner = await store.get_bot_owner(m.from_user.id)
                token = await store.get_user_bot_token(owner)
                trb = Bot(token=token)
                await trb.send_message(uid, card_text, disable_web_page_preview=True)
                sent += 1
            except Exception as e:
                logger.exception("Failed to send forwarded signal to %s: %s", uid, e)
        logger.info("Broadcasting forwarded signal to %d watcher(s)", sent)
    except Exception as e:
        logger.exception("on_forwarded_signal_message crashed: %s", e)
