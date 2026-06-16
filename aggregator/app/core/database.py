import json
import logging
import asyncio
import asyncpg
from datetime import datetime
from aggregator.app.core.config import DATABASE_URL, TRANSACTION_ISOLATION

logger = logging.getLogger("aggregator.database")

_pool = None

async def get_db_pool():
    global _pool
    if _pool is None:
        for i in range(10):
            try:
                _pool = await asyncpg.create_pool(
                    dsn=DATABASE_URL,
                    min_size=5,
                    max_size=20,
                    timeout=30.0
                )
                logger.info("Successfully connected to database pool.")
                break
            except Exception as e:
                logger.warning(f"Database connection attempt {i+1} failed: {e}. Retrying in 2 seconds...")
                await asyncio.sleep(2)
        if _pool is None:
            raise RuntimeError("Could not connect to database after 10 attempts.")
    return _pool

async def init_db():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Create processed_events table
        # Referensi: Coulouris dkk. (2012) - Bab 4: Skema Penamaan Unik untuk Identifikasi Entitas Terdistribusi
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(255) NOT NULL,
                event_id VARCHAR(255) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                source VARCHAR(255) NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(topic, event_id)
            );
        """)
        
        # Create index on topic for faster filtering
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_topic ON processed_events(topic);
        """)

        # Create stats table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key VARCHAR(50) PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 0
            );
        """)

        # Initialize stats values
        await conn.execute("""
            INSERT INTO stats (key, value) VALUES
            ('received', 0),
            ('unique_processed', 0),
            ('duplicate_dropped', 0)
            ON CONFLICT (key) DO NOTHING;
        """)
        
    logger.info("Database schema initialized.")

async def close_db():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed.")

async def increment_received(count: int = 1):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Referensi: Coulouris dkk. (2012) - Bab 8: Transaksi ACID (Atomicity)
        # Menjamin increment counter received bersifat atomik dan tidak terpengaruh kegagalan parsial
        async with conn.transaction():
            await conn.execute(
                "UPDATE stats SET value = value + $1 WHERE key = 'received'",
                count
            )

async def process_event_db(event: dict) -> bool:
    """
    Inserts event and updates stats inside a transaction.
    Returns True if the event is unique and successfully inserted.
    Returns False if it is a duplicate event.
    
    Referensi Teori:
    - Coulouris dkk. (2012) - Bab 8: Concurrency Control & Isolation Level (Read Committed)
      Menggunakan row-level locking implisit untuk menghindari anomali Lost-Update pada tabel stats.
    - Coulouris dkk. (2012) - Bab 9: Lock-free Concurrency menggunakan UNIQUE Constraint (Idempotent Upsert)
      ON CONFLICT DO NOTHING secara atomik mencegah pemrosesan ganda pada event yang sama (idempotensi).
    """
    pool = await get_db_pool()
    
    topic = event["topic"]
    event_id = event["event_id"]
    timestamp_str = event["timestamp"]
    source = event["source"]
    payload = json.dumps(event["payload"])
    
    # Parse ISO 8601 string
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.utcnow()
        
    async with pool.acquire() as conn:
        # Menjalankan transaksi dengan isolation level terkonfigurasi (default: READ COMMITTED)
        async with conn.transaction(isolation=TRANSACTION_ISOLATION):
            # Coba insert event log baru. Jika konflik (duplikat), abaikan secara atomik (DO NOTHING)
            result = await conn.fetchrow("""
                INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (topic, event_id) DO NOTHING
                RETURNING id;
            """, topic, event_id, dt, source, payload)
            
            is_unique = result is not None
            
            if is_unique:
                # Update counter unique secara transaksional
                await conn.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'unique_processed';"
                )
            else:
                # Update counter duplicate secara transaksional jika terjadi konflik indeks unik
                await conn.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped';"
                )
                
            return is_unique
