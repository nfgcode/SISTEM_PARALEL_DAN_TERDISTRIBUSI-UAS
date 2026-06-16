import os
import json
import asyncio
import logging
import redis.asyncio as aioredis
import asyncpg

from aggregator.app.core.config import BROKER_URL, QUEUE_NAME, TRANSACTION_ISOLATION
from aggregator.app.core.database import (
    get_db_pool,
    process_event_db
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator.consumer")

WORKER_ID = os.getenv("WORKER_ID", "default_worker")

async def process_with_retry(event: dict, max_retries: int = 5) -> bool:
    """
    Attempt to process the event in DB. If a serialization error occurs (40001)
    or database connectivity is lost, retry with exponential backoff.
    
    Referensi Teori:
    - Coulouris dkk. (2012) - Bab 6: Failure Modes & Mitigasi (Crash Recovery & Retry dengan Backoff)
      Menjamin ketahanan sistem dari kegagalan database sementara tanpa membuang pesan dari antrean.
    """
    backoff = 0.1  # start dengan 100ms
    for attempt in range(max_retries):
        try:
            is_unique = await process_event_db(event)
            if is_unique:
                logger.info(f"[{WORKER_ID}] Event {event['event_id']} processed successfully (Unique).")
            else:
                logger.info(f"[{WORKER_ID}] Event {event['event_id']} ignored (Duplicate detected).")
            return is_unique
        except asyncpg.exceptions.SerializationError as se:
            # Kegagalan isolasi transaksi concorrency (SQLState 40001) - butuh pengulangan (retry)
            logger.warning(
                f"[{WORKER_ID}] Serialization error on event {event['event_id']} (attempt {attempt+1}/{max_retries}): {se}. "
                f"Retrying in {backoff}s..."
            )
            await asyncio.sleep(backoff)
            backoff *= 2  # peningkatan backoff eksponensial
        except (asyncpg.exceptions.InterfaceError, asyncpg.exceptions.InternalClientError) as conn_err:
            # Kegagalan koneksi database sementara - butuh pengulangan
            logger.warning(
                f"[{WORKER_ID}] DB Connection error on event {event['event_id']} (attempt {attempt+1}/{max_retries}): {conn_err}. "
                f"Retrying in {backoff}s..."
            )
            await asyncio.sleep(backoff)
            backoff *= 2
        except Exception as e:
            logger.error(f"[{WORKER_ID}] Unexpected error processing event {event['event_id']}: {e}")
            raise e
            
    logger.error(f"[{WORKER_ID}] Failed to process event {event['event_id']} after {max_retries} retries due to concurrency conflicts.")
    raise RuntimeError(f"Max retries exceeded for event {event['event_id']}")

async def main():
    logger.info(f"Starting Consumer Worker [{WORKER_ID}]...")
    logger.info(f"Connecting to Broker: {BROKER_URL}")
    logger.info(f"Using Transaction Isolation Level: {TRANSACTION_ISOLATION}")
    
    # Connect to Redis
    # Referensi: Coulouris dkk. (2012) - Bab 3: Indirect Communication (Message Queue & Pub-Sub)
    # Konsumen menarik log secara asinkron menggunakan teknik blocking pop (FIFO queue)
    redis_client = aioredis.from_url(BROKER_URL, decode_responses=True)
    
    # Initialize DB pool
    pool = await get_db_pool()
    
    run = True
    while run:
        try:
            # BLPOP memblokir eksekusi thread secara aman sampai data antrean masuk ke Redis
            result = await redis_client.blpop(QUEUE_NAME, timeout=5)
            if result:
                _, data_str = result
                event = json.loads(data_str)
                
                try:
                    await process_with_retry(event)
                except Exception as e:
                    logger.error(f"[{WORKER_ID}] Dropping event {event.get('event_id')} due to processing failure: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Cancellation request received. Stopping worker...")
            run = False
        except Exception as e:
            logger.error(f"[{WORKER_ID}] Error in worker main loop: {e}")
            await asyncio.sleep(2)
            
    # Cleanup
    await redis_client.close()
    logger.info(f"Consumer Worker [{WORKER_ID}] stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown by user request.")
