# simple_bot_manager_final.py
import sys
import subprocess
import logging
import os
import signal
from pathlib import Path
from typing import Dict, Optional, List, Any
import asyncio
import json
import time
from datetime import datetime, timedelta

# Настройка логирования
logger = logging.getLogger("SimpleBotManager")
logger.setLevel(logging.INFO)

# Если у вас есть модуль database, импортируйте его
# from database import get_bot_record, save_bot_record, delete_bot_record
# Заглушки для функций работы с базой данных, если они отсутствуют
async def get_bot_record(username: str) -> Optional[Dict]:
    """Заглушка для получения записи бота из базы данных"""
    logger.debug(f"Получение записи бота {username} из БД (заглушка)")
    return None

async def save_bot_record(username: str, record: Dict) -> bool:
    """Заглушка для сохранения записи бота в базу данных"""
    logger.debug(f"Сохранение записи бота {username} в БД (заглушка)")
    return True

async def delete_bot_record(username: str) -> bool:
    """Заглушка для удаления записи бота из базы данных"""
    logger.debug(f"Удаление записи бота {username} из БД (заглушка)")
    return True

class SimpleBotManager:
    def __init__(self):
        self._registry: Dict[str, Dict] = {}  
        self._procs: Dict[str, subprocess.Popen] = {}
        self._start_times: Dict[str, float] = {}  # Время запуска ботов
        self._bot_stats: Dict[str, Dict] = {}     # Статистика ботов
        
    async def validate_bot_token(self, token: str) -> Dict[str, Any]:
        """Валидация токена бота с помощью aiogram"""
        try:
            from aiogram import Bot
            
            b = Bot(token=token)
            info = await b.get_me()
            await b.session.close()
            
            return {
                "is_valid": True,
                "username": info.username,
                "first_name": info.first_name,
                "id": info.id
            }
        except ImportError:
            logger.warning("aiogram не установлен, используется заглушка для валидации")
            # Заглушка для тестирования
            return {
                "is_valid": True,
                "username": "test_bot",
                "first_name": "Test Bot",
                "id": 123456789
            }
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            return {"is_valid": False, "error": str(e)}
    
    async def register_bot_instance(self, user_id: int, bot_token: str, bot_data: Dict) -> bool:
        """Регистрация нового бота в системе"""
        try:
            username = bot_data.get("username")
            if not username:
                raise ValueError("Bot username is empty")
            
            record = {
                "user_id": user_id,
                "token": bot_token,
                "data": dict(bot_data),
                "active": False,
                "created_at": asyncio.get_event_loop().time()
            }
            
            self._registry[username] = record
            await save_bot_record(username, record)
            
            # Инициализация статистики
            self._bot_stats[username] = {
                "users_count": 0,
                "active_users": 0,
                "total_trades": 0,
                "total_volume": 0,
                "total_deposits": 0,
                "total_withdrawals": 0
            }
            
            logger.info(f"Bot registered: @{username} (user={user_id})")
            return True
        except Exception as e:
            logger.error(f"register_bot_instance failed: {e}")
            return False
    
    async def unregister_bot_instance(self, username: str) -> bool:
        """Удаление бота из системы"""
        try:
            await self.stop_bot_instance(username)
            
            if username in self._registry:
                del self._registry[username]
            if username in self._procs:
                del self._procs[username]
            if username in self._start_times:
                del self._start_times[username]
            if username in self._bot_stats:
                del self._bot_stats[username]
            
            await delete_bot_record(username)
            logger.info(f"Bot unregistered: @{username}")
            return True
        except Exception as e:
            logger.error(f"unregister_bot_instance failed: {e}")
            return False
    
    async def _get_bot_data(self, username: str) -> Optional[Dict]:
        """Получение данных бота из кэша или базы данных"""
        if username in self._registry:
            return self._registry[username]
        
        rec = await get_bot_record(username)
        if rec:
            self._registry[username] = rec
            return rec
        
        return None
    
    async def refresh_from_redis(self, username: str) -> bool:
        """Обновление данных бота из Redis (или другой БД)"""
        try:
            rec = await get_bot_record(username)
            if rec:
                self._registry[username] = rec
                logger.debug(f"Refreshed data for @{username} from Redis")
                return True
            return False
        except Exception as e:
            logger.error(f"refresh_from_redis failed: {e}")
            return False
    
    async def cleanup_dead_processes(self) -> List[str]:
        """Очистка завершившихся процессов"""
        dead = []
        for username, proc in list(self._procs.items()):
            if proc.poll() is not None:  # Процесс завершен
                dead.append(username)
                rec = await self._get_bot_data(username)
                if rec:
                    rec["active"] = False
                    await save_bot_record(username, rec)
        
        for username in dead:
            if username in self._procs:
                del self._procs[username]
            if username in self._start_times:
                del self._start_times[username]
        
        if dead:
            logger.info(f"Cleaned up dead processes: {dead}")
        
        return dead
    
    async def start_bot_instance(self, username: str) -> bool:
        """Запуск экземпляра бота"""
        try:
            await self.cleanup_dead_processes()
            
            if username in self._procs and self._procs[username].poll() is None:
                logger.info(f"Bot @{username} already running.")
                return True
            
            rec = await self._get_bot_data(username)
            if not rec:
                raise RuntimeError(f"Bot @{username} is not registered")
            
            bot_token = rec["token"]
            base_dir = Path(__file__).resolve().parent
            parent = base_dir.parent
            
            candidates = [
                base_dir / "t163.py",
                parent / "t163.py"
            ]
            
            script_path = next((p for p in candidates if p.exists()), None)
            if not script_path:
                raise FileNotFoundError(
                    "t163.py not found. Tried:\n  " +
                    "\n  ".join(str(p) for p in candidates)
                )
            
            logs_dir = base_dir / "logs"
            logs_dir.mkdir(exist_ok=True)
            
            stdout_path = logs_dir / f"{username}.out.log"
            stderr_path = logs_dir / f"{username}.err.log"
            
            env = os.environ.copy()
            env["TRADE_BOT_TOKEN"] = bot_token
            env["TRADING_BOT_USERNAME"] = username
            
            cmd = [sys.executable, "-u", str(script_path), "--token", bot_token]
            
            # Подготовка флагов создания процесса
            creation_kwargs = {}
            if os.name == "nt":
                creation_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
                preexec_fn = None
            else:
                preexec_fn = os.setsid
            
            # Открытие файлов логов
            stdout_file = open(stdout_path, "ab", buffering=0)
            stderr_file = open(stderr_path, "ab", buffering=0)
            
            proc = subprocess.Popen(
                cmd,
                env=env,
                stdout=stdout_file,
                stderr=stderr_file,
                cwd=str(script_path.parent),
                preexec_fn=preexec_fn if os.name != "nt" else None,
                **creation_kwargs
            )
            
            self._procs[username] = proc
            self._start_times[username] = time.time()
            
            rec["active"] = True
            rec["pid"] = proc.pid
            rec["started_at"] = asyncio.get_event_loop().time()
            
            await save_bot_record(username, rec)
            logger.info(f"Bot started: @{username} (pid={proc.pid})")
            return True
            
        except Exception as e:
            logger.error(f"start_bot_instance failed: {e}")
            return False
    
    async def stop_bot_instance(self, username: str, force: bool = False) -> bool:
        """Остановка экземпляра бота"""
        try:
            proc = self._procs.get(username)
            stopped = False
            
            if proc and proc.poll() is None:
                try:
                    if os.name == "nt":
                        proc.send_signal(subprocess.signal.CTRL_BREAK_EVENT)
                    else:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    
                    try:
                        proc.wait(timeout=10)
                        stopped = True
                    except subprocess.TimeoutExpired:
                        if force:
                            logger.warning(f"Force killing bot @{username} (pid={proc.pid})")
                            if os.name == "nt":
                                proc.kill()
                            else:
                                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                            proc.wait()
                            stopped = True
                        else:
                            logger.warning(f"Bot @{username} did not stop gracefully")
                            return False
                except ProcessLookupError:
                    stopped = True
                except Exception as kill_error:
                    logger.error(f"Error stopping process @{username}: {kill_error}")
                    if force:
                        try:
                            proc.kill()
                            proc.wait()
                            stopped = True
                        except:
                            pass
            
            rec = await self._get_bot_data(username)
            if rec:
                rec["active"] = False
                if "pid" in rec:
                    del rec["pid"]
                await save_bot_record(username, rec)
            
            if stopped and username in self._procs:
                del self._procs[username]
            
            if username in self._start_times:
                del self._start_times[username]
            
            if stopped:
                logger.info(f"Bot stopped: @{username}")
            else:
                logger.info(f"Bot was already stopped: @{username}")
            
            return True
            
        except Exception as e:
            logger.error(f"stop_bot_instance failed: {e}")
            return False
    
    async def restart_bot_instance(self, username: str) -> bool:
        """Перезапуск экземпляра бота"""
        try:
            stopped = await self.stop_bot_instance(username)
            if not stopped:
                logger.warning(f"Could not stop bot @{username} before restart")
                await self.stop_bot_instance(username, force=True)
            
            await asyncio.sleep(2)
            started = await self.start_bot_instance(username)
            
            if started:
                logger.info(f"Bot restarted: @{username}")
            else:
                logger.error(f"Failed to restart bot @{username}")
            
            return started
        except Exception as e:
            logger.error(f"restart_bot_instance failed: {e}")
            return False
    
    async def get_bot_status(self, bot_username: str) -> Dict[str, Any]:
        """Получение статуса бота (объединенная версия)"""
        await self.cleanup_dead_processes()
        
        proc = self._procs.get(bot_username)
        running = proc is not None and proc.poll() is None
        rec = await self._get_bot_data(bot_username)
        
        # Расчет времени работы
        uptime_str = "0m"
        if bot_username in self._start_times and running:
            uptime_seconds = time.time() - self._start_times[bot_username]
            
            if uptime_seconds < 60:
                uptime_str = f"{int(uptime_seconds)}s"
            elif uptime_seconds < 3600:
                uptime_str = f"{int(uptime_seconds // 60)}m"
            elif uptime_seconds < 86400:
                hours = int(uptime_seconds // 3600)
                minutes = int((uptime_seconds % 3600) // 60)
                uptime_str = f"{hours}h {minutes}m"
            else:
                days = int(uptime_seconds // 86400)
                hours = int((uptime_seconds % 86400) // 3600)
                uptime_str = f"{days}d {hours}h"
        
        # Статистика бота
        stats = self._bot_stats.get(bot_username, {
            "users_count": 0,
            "active_users": 0,
            "total_trades": 0,
            "total_volume": 0,
            "total_deposits": 0,
            "total_withdrawals": 0
        })
        
        status_info = {
            "active": running,
            "status": "running" if running else "stopped",
            "uptime": uptime_str,
            "pid": getattr(proc, "pid", None),
            "registered": rec is not None,
            **stats  # Добавляем статистику
        }
        
        if rec:
            status_info.update({
                "user_id": rec.get("user_id"),
                "first_name": rec.get("data", {}).get("first_name"),
                "created_at": rec.get("created_at"),
                "username": bot_username
            })
        
        return status_info
    
    async def update_bot_stats(self, username: str, stats: Dict[str, Any]) -> bool:
        """Обновление статистики бота"""
        try:
            if username in self._bot_stats:
                self._bot_stats[username].update(stats)
                logger.debug(f"Updated stats for bot @{username}")
                return True
            return False
        except Exception as e:
            logger.error(f"update_bot_stats failed: {e}")
            return False
    
    async def list_all_bots(self) -> Dict[str, Dict]:
        """Список всех зарегистрированных ботов"""
        result = {}
        for username in list(self._registry.keys()):
            status = await self.get_bot_status(username)
            result[username] = status
        return result
    
    async def list_running_bots(self) -> List[str]:
        """Список запущенных ботов"""
        await self.cleanup_dead_processes()
        return [username for username, proc in self._procs.items() 
                if proc.poll() is None]
    
    async def stop_all_bots(self) -> Dict[str, bool]:
        """Остановка всех ботов"""
        results = {}
        running_bots = await self.list_running_bots()
        for username in running_bots:
            result = await self.stop_bot_instance(username)
            results[username] = result
        return results

# Создаем экземпляр менеджера ботов
bot_manager = SimpleBotManager()
logger.info("SimpleBotManager initialized (unified version)")

# Для обратной совместимости с кодом, который использует delete_bot_instance
async def delete_bot_instance(bot_username: str) -> bool:
    """Алиас для unregister_bot_instance для обратной совместимости"""
    return await bot_manager.unregister_bot_instance(bot_username)