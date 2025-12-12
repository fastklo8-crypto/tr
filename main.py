from bot import logger, TRADING_BOT_USERNAME  # если они объявлены в bot.py
from storage.store import store  # если сделаешь глобальный store в storage/store.py
from storage.store import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_PASSWORD,
    REDIS_DB,
)
import redis.asyncio as redis
from storage.store import Store
r = redis.from_url(REDIS_URL)
store = Store(r)