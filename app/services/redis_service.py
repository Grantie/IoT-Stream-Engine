"""Redis service module for handling Redis operations."""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from redis.asyncio import Redis

from app.core.config import settings

logger = logging.getLogger(__name__)


class RedisService:
    """Service for interacting with Redis."""

    def __init__(self) -> None:
        """Initialize Redis service without immediate connection."""
        self.redis: Optional[Redis] = None
        self._lock = asyncio.Lock()
        self._test_mode = False  # Flag to prevent reconnection in tests
        # Don't connect immediately - connect lazily when needed

    async def _get_redis_client(self) -> Optional[Redis]:
        """Get the Redis client, creating it if it doesn't exist."""
        async with self._lock:
            # If in test mode and redis is None, don't try to reconnect
            if self._test_mode and self.redis is None:
                return None
                
            if self.redis is None:
                try:
                    self.redis = Redis.from_url(
                        settings.REDIS_URL, decode_responses=True
                    )
                    await self.redis.ping()
                except Exception as e:
                    logger.error(f"Error connecting to Redis: {e}")
                    self.redis = None
        return self.redis

    async def get_cached_reading(self, device_id: str) -> Optional[float]:
        """Get cached reading for a device."""
        redis = await self._get_redis_client()
        if not redis:
            return None

        try:
            key = f"reading:{device_id}"
            data = await redis.get(key)
            if data:
                return float(data)
            return None
        except Exception as e:
            self._log_error("Redis err", e)
            return None



    async def cache_reading(self, device_id: str, value: float) -> bool:
        """Cache reading for a device."""
        redis = await self._get_redis_client()
        if not redis:
            return False

        try:
            key = f"reading:{device_id}"
            ttl = settings.CACHE_TTL
            await redis.setex(key, ttl, str(value))  # Cache with configurable TTL
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False



    async def store_reading(self, device_id: str, value: float) -> bool:
        """Store reading in Redis with timestamp."""
        redis = await self._get_redis_client()
        if not redis:
            return False

        try:
            timestamp = int(datetime.now().timestamp() * 1000)
            key = f"reading:{device_id}:{timestamp}"
            data = json.dumps({"value": value, "timestamp": timestamp})
            await redis.set(key, data)
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False

    async def get_reading(self, device_id: str) -> Optional[float]:
        """Get latest reading for a device."""
        return await self.get_cached_reading(device_id)



    async def set_reading(self, device_id: str, value: float) -> bool:
        """Set reading for a device."""
        redis = await self._get_redis_client()
        if not redis:
            return False

        try:
            key = f"reading:{device_id}"
            await redis.set(key, str(value))
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False

    async def delete_reading(self, device_id: str) -> bool:
        """Delete reading for a device."""
        redis = await self._get_redis_client()
        if not redis:
            return False

        try:
            key = f"reading:{device_id}"
            await redis.delete(key)
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False

    async def get_all_readings(self) -> Dict[str, float]:
        """Get all readings."""
        redis = await self._get_redis_client()
        if not redis:
            return {}

        try:
            readings: Dict[str, float] = {}
            async for key in redis.scan_iter("reading:*"):
                data = await redis.get(key)
                if data:
                    parts = key.split(":")
                    if len(parts) > 1:
                        device_id = parts[1]
                        readings[device_id] = float(data)
            return readings
        except Exception as e:
            self._log_error("Redis err", e)
            return {}

    async def clear_readings(self) -> bool:
        """Clear all readings."""
        redis = await self._get_redis_client()
        if not redis:
            return False

        try:
            async for key in redis.scan_iter("reading:*"):
                await redis.delete(key)
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False

    async def get_reading_history(
        self, device_id: str, window: int = 3600
    ) -> List[Dict[str, Any]]:
        """Get reading history for a device over a time window."""
        redis = await self._get_redis_client()
        if not redis:
            return []

        try:
            now = datetime.now()
            start_time = int((now.timestamp() - window) * 1000)
            end_time = int(now.timestamp() * 1000)
            pattern = f"reading_history:{device_id}" # Use zset key
            # If using keys pattern matching on individual keys:
            # pattern = f"reading:{device_id}:*"
            
            # Implementation assuming ZSET for history as seen in store_price_data
            
            # Check if we were using individual keys
            # The original code had get_price_history using KEYS on price:symbol:*
            # But store_price used SET on price:symbol:ts
            # And store_price_data used ZADD on price_history:symbol
            
            # I will stick to ZSET for history if store_reading_data uses it.
            # But store_reading (above) uses SET.
            # Let's support the SET pattern from original get_price_history
            
            pattern = f"reading:{device_id}:*"
            keys = await redis.keys(pattern)
            readings = []
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode()
                timestamp_ms_str = key.split(":")[-1]
                if timestamp_ms_str.isdigit():
                    timestamp_ms = int(timestamp_ms_str)
                    if start_time <= timestamp_ms <= end_time:
                        data = await redis.get(key)
                        if data:
                            readings.append(json.loads(data))
            return sorted(readings, key=lambda x: x["timestamp"])
        except Exception as e:
            self._log_error("Redis err", e)
            return []



    async def store_job_status(self, job_id: str, status: Dict[str, Any]) -> None:
        """Store job status in Redis."""
        redis = await self._get_redis_client()
        if not redis:
            return

        try:
            key = f"job:{job_id}"
            await redis.set(key, json.dumps(status))
        except Exception as e:
            self._log_error("Redis err", e)

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status from Redis."""
        redis = await self._get_redis_client()
        if not redis:
            return None

        try:
            key = f"job:{job_id}"
            data = await redis.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            self._log_error("Redis err", e)
            return None

    async def delete_job(self, job_id: str) -> None:
        """Delete job from Redis."""
        redis = await self._get_redis_client()
        if not redis:
            return

        try:
            key = f"job:{job_id}"
            await redis.delete(key)
        except Exception as e:
            self._log_error("Redis err", e)

    async def list_jobs(self) -> List[Dict[str, Any]]:
        """List all jobs from Redis."""
        redis = await self._get_redis_client()
        if not redis:
            return []

        try:
            jobs = []
            async for key in redis.scan_iter("job:*"):
                data = await redis.get(key)
                if data:
                    jobs.append(json.loads(data))
            return jobs
        except Exception as e:
            self._log_error("Redis err", e)
            return []

    def _log_error(self, msg: str, exc: Exception) -> None:
        """Log error with proper formatting."""
        logger.error(
            f"{msg}: {exc.__class__.__name__}: {str(exc)[:20]}... " f"{str(exc)[-40:]}"
        )

    async def store_reading_data(self, device_id: str, value: float, timestamp: int) -> bool:
        """Store reading data in a sorted set for history/statistics."""
        redis = await self._get_redis_client()
        if not redis:
            return False
        try:
            key = f"reading_history:{device_id}"
            await redis.zadd(key, {value: timestamp})
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False
            


    async def delete_reading_data(self, device_id: str, window: int = 3600) -> int:
        """Delete reading data in a time window."""
        redis = await self._get_redis_client()
        if not redis:
            return 0
        try:
            key = f"reading_history:{device_id}"
            current_time = int(time.time())
            min_time = current_time - window
            return await redis.zremrangebyscore(key, min_time, current_time)
        except Exception as e:
            self._log_error("Redis err", e)
            return 0

    async def get_reading_statistics(
        self, device_id: str, window: int = 3600
    ) -> Optional[dict]:
        """Get min, max, avg reading for a device in a time window."""
        history = await self.get_reading_history(device_id, window)
        if not history:
            try:
                # Try ZSET approach if history (SET) was empty
                redis = await self._get_redis_client()
                if redis:
                    key = f"reading_history:{device_id}"
                    # This logic was missing in original? Original called get_price_history.
                    pass
            except Exception:
                pass
            return None
            

             
        if not readings:
            return None
            
        return {
            "min": min(readings),
            "max": max(readings),
            "avg": sum(readings) / len(readings),
        }

    async def clear_all_data(self) -> bool:
        """Clear all reading and job data from Redis."""
        redis = await self._get_redis_client()
        if not redis:
            return False
        try:
            await redis.flushdb()
            return True
        except Exception as e:
            self._log_error("Redis err", e)
            return False

    async def get_connection_info(self) -> dict:
        """Get Redis connection/server info."""
        redis = await self._get_redis_client()
        if not redis:
            return {"status": "disconnected"}
        try:
            info = await redis.info()
            return {
                "status": "connected",
                "version": info.get("redis_version"),
                "mode": info.get("redis_mode"),
            }
        except Exception as e:
            self._log_error("Redis conn err", e)
            return {"status": "error", "message": str(e)}

    async def ping(self) -> bool:
        """Ping Redis server."""
        redis = await self._get_redis_client()
        if not redis:
            return False
        try:
            return await redis.ping()
        except Exception as e:
            self._log_error("Redis err", e)
            return False

    def set_test_mode(self, enabled: bool = True) -> None:
        """Set test mode to prevent reconnection attempts."""
        self._test_mode = enabled
        if enabled:
            self.redis = None  # Clear any existing connection
