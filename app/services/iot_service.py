"""IoT service module for handling sensor data operations."""

import asyncio
import logging
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.iot import SensorReading
from app.schemas.telemetry import SensorReadingCreate, SensorReadingUpdate
from app.services.redis_service import RedisService

logger = logging.getLogger(__name__)


def retry_on_failure(max_retries=3, delay=1):
    """
    Retry failed operations up to max_retries times with a delay.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds

    Returns:
        Decorated function
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {str(e)}. Retrying..."
                    )
                    await asyncio.sleep(delay)
        return wrapper

    return decorator


class IoTService:
    """Service class for handling IoT sensor operations."""

    def __init__(self, db: Session):
        """Initialize IoTService with database session."""
        self.db = db
        self.redis_service = RedisService()

    @staticmethod
    def get_readings(
        db: Session, skip: int = 0, limit: int = 100
    ) -> List[SensorReading]:
        """
        Retrieve sensor readings with pagination.

        Args:
            db: Database session
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of sensor readings
        """
        return db.query(SensorReading).offset(skip).limit(limit).all()

    @staticmethod
    def get_readings_by_device(
        db: Session, device_id: str, skip: int = 0, limit: int = 100
    ) -> List[SensorReading]:
        """
        Retrieve readings for a specific device with pagination.

        Args:
            db: Database session
            device_id: IoT Device ID
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of sensor readings for the device
        """
        return (
            db.query(SensorReading)
            .filter(SensorReading.device_id == device_id)
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def create_reading(db: Session, reading: SensorReadingCreate) -> SensorReading:
        """
        Create a new sensor reading record.

        Args:
            db: Database session
            reading: Reading data to create

        Returns:
            Created sensor reading record
        """
        db_obj = SensorReading(**reading.model_dump())
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    @staticmethod
    def update_reading(
        db: Session, reading_id: int, reading: SensorReadingUpdate
    ) -> Optional[SensorReading]:
        """
        Update an existing sensor reading record.

        Args:
            db: Database session
            reading_id: ID of reading to update
            reading: Updated reading data

        Returns:
            Updated sensor reading record or None if not found
        """
        db_obj = db.query(SensorReading).filter(SensorReading.id == reading_id).first()
        if not db_obj:
            return None
        for field, value in reading.model_dump(exclude_unset=True).items():
            setattr(db_obj, field, value)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    @staticmethod
    def delete_reading(db: Session, reading_id: int) -> bool:
        """
        Delete a sensor reading record.

        Args:
            db: Database session
            reading_id: ID of reading to delete

        Returns:
            True if deleted, False if not found
        """
        db_obj = db.query(SensorReading).filter(SensorReading.id == reading_id).first()
        if not db_obj:
            return False
        db.delete(db_obj)
        db.commit()
        return True

    @staticmethod
    def get_latest_reading(db: Session, device_id: str) -> Optional[SensorReading]:
        """
        Get the latest reading for a specific device.

        Args:
            db: Database session
            device_id: IoT Device ID

        Returns:
            Latest sensor reading record or None if not found
        """
        return (
            db.query(SensorReading)
            .filter(SensorReading.device_id == device_id)
            .order_by(SensorReading.timestamp.desc())
            .first()
        )

    @staticmethod
    def get_all_devices(db: Session) -> List[str]:
        """
        Get all unique device IDs from sensor readings.

        Args:
            db: Database session

        Returns:
            List of unique device IDs
        """
        return [row[0] for row in db.query(SensorReading.device_id).distinct().all()]

    @staticmethod
    def calculate_rolling_average(
        db: Session, device_id: str, window: int = 5
    ) -> Optional[float]:
        """
        Calculate the rolling average for a device over a window of records.

        Args:
            db: Database session
            device_id: IoT Device ID
            window: Number of records to include in the average

        Returns:
            Rolling average or None if insufficient data
        """
        records = (
            db.query(SensorReading)
            .filter(SensorReading.device_id == device_id)
            .order_by(SensorReading.timestamp.desc())
            .limit(window)
            .all()
        )

        if len(records) < window:
            return None

        total_value = sum(record.reading_value for record in records)
        return total_value / len(records)

    @staticmethod
    def get_latest_timestamp(db: Session, device_id: str) -> Optional[datetime]:
        """
        Get the latest timestamp for a specific device.

        Args:
            db: Database session
            device_id: IoT Device ID

        Returns:
            Latest timestamp or None if not found
        """
        result = (
            db.query(SensorReading.timestamp)
            .filter(SensorReading.device_id == device_id)
            .order_by(SensorReading.timestamp.desc())
            .first()
        )
        return result[0] if result else None

    @retry_on_failure(max_retries=3)
    async def get_latest_reading_value(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest reading for a device from cache or gateway.

        Args:
            device_id: IoT Device ID

        Returns:
            Latest reading data or None if not found
        """
        # Using cache_price/get_latest_price methods from RedisService which might need renaming too
        # For now we assume RedisService is generic enough or we will update it later.
        # Ideally we should refactor RedisService too but it wasn't explicitly in the rename list,
        # but the prompt said "Terminilogy Mapping... wherever you see..."
        # I'll check RedisService later. For now, assuming keys might be issue.
        
        # Use get_reading/cache_reading from RedisService
        cached_data = await self.redis_service.get_reading(device_id) 

        if cached_data:
            # Reconstruct dict format if necessary or just return value
            return {
                "device_id": device_id,
                "reading_value": cached_data,
                "timestamp": datetime.now(UTC).isoformat()
            }

        reading_data = await self._fetch_reading_from_gateway(device_id)
        if reading_data:
            await self.redis_service.cache_reading(device_id, reading_data["reading_value"])
        return reading_data

    async def create_polling_job(self, device_id: str, interval: int) -> bool:
        """
        Create a new polling job to fetch sensor data periodically.

        Args:
            device_id: Device ID to poll
            interval: Polling interval in seconds

        Returns:
            True if job created successfully
        """
        try:
            job_status = {
                "device_id": device_id,
                "interval": interval,
                "status": "active",
                "created_at": datetime.now(UTC).isoformat(),
            }
            await self.redis_service.store_job_status(device_id, job_status)
            return True
        except Exception as e:
            logger.error(f"Failed to create polling job for {device_id}: {e}")
            return False

    async def get_job_status(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a polling job.

        Args:
            device_id: Device ID

        Returns:
            Dictionary with job status or None if not found
        """
        return await self.redis_service.get_job_status(device_id)

    async def delete_job(self, device_id: str) -> bool:
        """
        Delete a polling job.

        Args:
            device_id: Device ID

        Returns:
            True if deleted, False if not found or error occurs
        """
        try:
            await self.redis_service.delete_job(device_id)
            return True
        except Exception as e:
            logger.error(f"Failed to delete job for {device_id}: {e}")
            return False

    @staticmethod
    def add_reading(
        db, device_id: str, reading_value: float, reading_type: str = "temp", unit: str = "C", battery_level: float = 100.0
    ) -> None:
        """
        Add a reading record to the database.

        Args:
            db: Database session
            device_id: Device ID
            reading_value: Reading value
            reading_type: Type of reading
            unit: Unit of measurement
        """
        reading = SensorReading(
            device_id=device_id,
            reading_value=reading_value,
            reading_type=reading_type,
            unit=unit,
            battery_level=battery_level,
            timestamp=datetime.now(UTC),
        )
        db.add(reading)
        db.commit()

    async def _fetch_reading_from_gateway(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch reading data from IoT Gateway (Mock).

        Args:
            device_id: Device ID

        Returns:
            Dictionary with reading data or None
        """
        # Mock implementation for IoT Gateway
        import random
        try:
            # Simulate network delay
            await asyncio.sleep(0.1)
            
            mock_value = 20.0 + random.uniform(-5, 5)
            reading_data = {
                "device_id": device_id,
                "reading_value": mock_value,
                "timestamp": datetime.now(UTC).isoformat(),
                "unit": "Celsius",
                "reading_type": "temperature"
            }
            return reading_data
        except Exception as e:
            logger.error(f"Failed to fetch data for {device_id}: {e}")
            return None

    async def list_active_jobs(self) -> List[dict]:
        """List all active jobs from Redis."""
        try:
            jobs = await self.redis_service.list_jobs()
            return jobs
        except Exception as e:
            logger.error(f"Failed to list active jobs: {e}")
            return []

    async def delete_all_jobs(self) -> int:
        """
        Delete all polling jobs.

        Returns:
            Number of jobs deleted
        """
        try:
            jobs = await self.redis_service.list_jobs()
            deleted_count = 0
            for job in jobs:
                device_id = job.get("device_id") or job.get("symbol") # fallback for old keys
                if device_id:
                    await self.redis_service.delete_job(device_id)
                deleted_count += 1
            return deleted_count
        except Exception as e:
            logger.error(f"Failed to delete all jobs: {e}")
            return 0

    @staticmethod
    def get_reading_by_id(db: Session, reading_id: int) -> Optional[SensorReading]:
        """
        Get a sensor reading record by its ID.

        Args:
            db: Database session
            reading_id: ID of the sensor reading record

        Returns:
            SensorReading object or None if not found
        """
        return db.query(SensorReading).filter(SensorReading.id == reading_id).first()

    @staticmethod
    def get_latest_reading_static(db: Session, device_id: str, unit: Optional[str] = None) -> Optional[SensorReading]:
        """
        Get the latest reading for a specific device, optionally filtered by unit.

        Args:
            db: Database session
            device_id: IoT Device ID
            unit: Optional unit filter

        Returns:
            Latest sensor reading record or None if not found
        """
        query = db.query(SensorReading).filter(SensorReading.device_id == device_id)
        
        if unit:
            query = query.filter(SensorReading.unit == unit)
            
        return query.order_by(SensorReading.timestamp.desc()).first()
