import json
import time
import logging
from typing import List, Union, Dict, Any
from fastapi import FastAPI, HTTPException, status, Query
from pydantic import BaseModel, Field
from datetime import datetime
import redis.asyncio as aioredis

from aggregator.app.core.config import BROKER_URL, QUEUE_NAME, START_TIME
from aggregator.app.core.database import (
    init_db,
    close_db,
    get_db_pool,
    increment_received
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator.api")

app = FastAPI(
    title="Pub-Sub Log Aggregator API",
    description="Distributed Log Aggregator with Idempotency, Deduplication, and Concurrency Control",
    version="1.0.0"
)

redis_client = None

# Referensi: Coulouris dkk. (2012) - Bab 4: Eksternal Data Representation (Skema Pydantic)
# Memastikan kesesuaian representasi data luar dengan struktur internal sistem terdistribusi
class EventModel(BaseModel):
    topic: str = Field(..., min_length=1, examples=["system.log"])
    event_id: str = Field(..., min_length=1, examples=["evt_12345"])
    timestamp: str = Field(..., min_length=1, description="ISO8601 formatted timestamp")
    source: str = Field(..., min_length=1, examples=["auth-service"])
    payload: Dict[str, Any] = Field(..., examples=[{"user_id": 42, "status": "success"}])

    class Config:
        extra = "forbid"

@app.on_event("startup")
async def startup_event():
    global redis_client
    logger.info("Initializing API services...")
    # Initialize DB
    await init_db()
    # Initialize Redis client
    redis_client = aioredis.from_url(BROKER_URL, decode_responses=True)
    logger.info("API services successfully started.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down API services...")
    await close_db()
    if redis_client:
        await redis_client.close()
    logger.info("API services shut down.")

@app.get("/health/liveness", status_code=status.HTTP_200_OK)
async def liveness():
    return {"status": "alive"}

@app.get("/health/readiness", status_code=status.HTTP_200_OK)
async def readiness():
    # Referensi: Coulouris dkk. (2012) - Bab 12: Koordinasi & Observabilitas Layanan Terdistribusi
    # Readiness probe dinamis untuk memonitor kesiapan koneksi database & broker
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database not ready: {str(e)}")
        
    try:
        if redis_client:
            await redis_client.ping()
        else:
            raise Exception("Redis client not initialized")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Broker (Redis) not ready: {str(e)}")
        
    return {"status": "ready"}

@app.post("/publish", status_code=status.HTTP_202_ACCEPTED)
async def publish_event(payload: Union[EventModel, List[EventModel]]):
    """
    Accepts single or batch events. Validates schema, increments the received counter, 
    and pushes the event into the queue for async processing.
    
    Referensi Teori:
    - Coulouris dkk. (2012) - Bab 2: Arsitektur Publish-Subscribe (Decoupled in space, time, & synchronization)
      FastAPI merespon 202 Accepted secara asinkron tanpa memblokir publisher utama.
    """
    # Menyamakan input tunggal atau banyak menjadi list
    events = [payload] if isinstance(payload, EventModel) else payload
    
    # Validasi format timestamp fisik ISO8601
    for ev in events:
        try:
            datetime.fromisoformat(ev.timestamp.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid ISO8601 timestamp format for event_id: {ev.event_id}"
            )

    # Catat log masuk secara transaksional di stats database
    try:
        await increment_received(len(events))
    except Exception as e:
        logger.error(f"Failed to increment received counter: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error while recording metrics."
        )

    # Push event data ke Redis queue (broker antrean)
    try:
        serialized_events = [json.dumps(ev.model_dump()) for ev in events]
        await redis_client.rpush(QUEUE_NAME, *serialized_events)
    except Exception as e:
        logger.error(f"Failed to enqueue events: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message broker unavailable. Event rejected."
        )

    return {"status": "enqueued", "count": len(events)}

@app.get("/events", response_model=List[Dict[str, Any]])
async def get_events(topic: str = Query(..., description="Topic filter to retrieve unique events")):
    """
    Returns list of processed unique events for a given topic.
    """
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT topic, event_id, timestamp, source, payload
                FROM processed_events
                WHERE topic = $1
                ORDER BY timestamp ASC;
            """, topic)
            
            return [
                {
                    "topic": r["topic"],
                    "event_id": r["event_id"],
                    "timestamp": r["timestamp"].isoformat(),
                    "source": r["source"],
                    "payload": json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
                }
                for r in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query error."
        )

@app.get("/stats")
async def get_stats():
    """
    Returns metrics: received, unique_processed, duplicate_dropped, topics, uptime.
    
    Referensi Teori:
    - Coulouris dkk. (2012) - Bab 12: Observabilitas Layanan Terdistribusi
      Menyediakan interface untuk menginspeksi keadaan internal / metrik sistem secara realtime.
    """
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            # Mengambil data statistik terkini
            stats_rows = await conn.fetch("SELECT key, value FROM stats;")
            stats_dict = {r["key"]: r["value"] for r in stats_rows}
            
            # Mengambil topik unik terdaftar
            topic_rows = await conn.fetch("SELECT DISTINCT topic FROM processed_events;")
            topics = [r["topic"] for r in topic_rows]
            
            uptime = int(time.time() - START_TIME)
            
            return {
                "received": stats_dict.get("received", 0),
                "unique_processed": stats_dict.get("unique_processed", 0),
                "duplicate_dropped": stats_dict.get("duplicate_dropped", 0),
                "topics": topics,
                "uptime": uptime
            }
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query error while retrieving stats."
        )
