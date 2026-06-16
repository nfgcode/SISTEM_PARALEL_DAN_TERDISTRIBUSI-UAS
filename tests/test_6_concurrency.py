"""
Test Suite 6: Concurrency / Race Conditions
===========================================
Teori & Sitasi Akademik:
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
  * Topik (Bab 8 & 9): Kontrol konkurensi terdistribusi (Concurrency Control), isolasi transaksi (Isolation Levels), 
    pencegahan Lost-Update, dan locking pesimis vs. optimis.
- Referensi: Stonebraker, M., Madden, S., & Abadi, D. J. (2007). The End of an Architectural Era (It's Time for a Complete Rewrite). Proceedings of the VLDB, 1150-1160.
  * Topik: Kinerja optimasi database relasional menggunakan unique constraint dan lock-free indexing.

Deskripsi:
Mengirimkan 10 request event log yang sama secara bersamaan (secara konkuren) menggunakan antrean.
Memastikan sistem menangani anomali konkurensi (race conditions) dengan aman, dan tepat hanya 1 event 
yang dicatat sebagai unique_processed sedangkan 9 event lainnya dibuang secara aman sebagai duplicate_dropped.
"""

import time
import uuid
import pytest
import asyncio
import httpx
from datetime import datetime

BASE_URL = "http://localhost:8080"
pytestmark = pytest.mark.asyncio

async def wait_for_stats(client: httpx.AsyncClient, stat_key: str, min_value: int, timeout: float = 5.0) -> dict:
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = await client.get(f"{BASE_URL}/stats")
        assert response.status_code == 200
        data = response.json()
        if data.get(stat_key, 0) >= min_value:
            return data
        await asyncio.sleep(0.1)
    return response.json()

def generate_test_event(topic: str = "test.topic", event_id: str = None) -> dict:
    if event_id is None:
        event_id = f"test_evt_{uuid.uuid4().hex[:10]}"
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-suite",
        "payload": {"test_key": "test_val"}
    }

async def test_concurrency_exact_duplicates():
    async with httpx.AsyncClient() as client:
        initial_stats_res = await client.get(f"{BASE_URL}/stats")
        initial_stats = initial_stats_res.json()
        initial_unique = initial_stats.get("unique_processed", 0)
        initial_dropped = initial_stats.get("duplicate_dropped", 0)

        topic = f"test.concurrent.{uuid.uuid4().hex[:6]}"
        event = generate_test_event(topic=topic)
        
        # Fire 10 duplicate requests concurrently
        num_requests = 10
        tasks = [client.post(f"{BASE_URL}/publish", json=event) for _ in range(num_requests)]
        responses = await asyncio.gather(*tasks)
        
        for r in responses:
            assert r.status_code == 202

        # Wait for processing
        await wait_for_stats(client, "unique_processed", initial_unique + 1)
        final_stats = await wait_for_stats(client, "duplicate_dropped", initial_dropped + (num_requests - 1))
        
        assert final_stats["unique_processed"] == initial_unique + 1
        assert final_stats["duplicate_dropped"] == initial_dropped + (num_requests - 1)
