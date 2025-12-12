import json
import logging
import redis.asyncio as redis
import os
from typing import Optional, Dict, Any
logger = logging.getLogger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)
async def get_bot_record(bot_username: str) -> Optional[Dict[str, Any]]:
    try:
        data = await r.hget("bot:instances", bot_username)
        if data:
            return json.loads(data)
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for bot {bot_username}: {e}")
    except redis.RedisError as e:
        logger.error(f"Redis error getting bot record for {bot_username}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error getting bot record for {bot_username}: {e}")
    return None
async def save_bot_record(bot_username: str, data: Dict[str, Any]) -> bool:
    try:
        await r.hset("bot:instances", bot_username, json.dumps(data))
        return True
    except redis.RedisError as e:
        logger.error(f"Redis error saving bot record for {bot_username}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error saving bot record for {bot_username}: {e}")
    return False
async def delete_bot_record(bot_username: str) -> bool:
    try:
        deleted = await r.hdel("bot:instances", bot_username)
        return deleted > 0  
    except redis.RedisError as e:
        logger.error(f"Redis error deleting bot record for {bot_username}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting bot record for {bot_username}: {e}")
    return False
async def get_all_bot_records() -> Dict[str, Dict[str, Any]]:
    try:
        data = await r.hgetall("bot:instances")
        result = {}
        for bot_username, bot_data in data.items():
            try:
                result[bot_username] = json.loads(bot_data)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for bot {bot_username}: {e}")
        return result
    except redis.RedisError as e:
        logger.error(f"Redis error getting all bot records: {e}")
    except Exception as e:
        logger.error(f"Unexpected error getting all bot records: {e}")
    return {}