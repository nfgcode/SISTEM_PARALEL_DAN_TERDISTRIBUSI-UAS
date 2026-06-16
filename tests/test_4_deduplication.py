"""
Test Suite 4: Deduplication & Idempotency
=========================================
Teori & Sitasi Akademik:
- Referensi: Decandia, G., Hastorun, D., Jampani, M., Kakulapati, G., Lakshman, A., Pilchin, A., Sivasubramanian, S., Vosshall, P., & Vogels, W. (2007). Dynamo: Amazon’s Highly Available Key-value Store. ACM SIGOPS Operating Systems Review, 41(6), 205-220.
  * Topik: Pola Idempotent Consumer untuk menjamin konsistensi akhir (Eventual Consistency) pada sistem penyimpanan terdistribusi pasca at-least-once network delivery.
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
  * Topik (Bab 7): Konsistensi data eventual pada replika penyimpanan dan penanganan pesan duplikat di sisi penerima.

Deskripsi:
Menguji mekanisme penanganan data duplikat (deduplikasi) di sisi database. Memastikan log dengan 
topic dan event_id yang sama persis (atau dengan timestamp/payload berbeda) akan disaring, 
hanya data unik pertama yang disimpan ke database, dan duplikat dibuang dengan menambah counter duplicate_dropped.
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

async def test_deduplication_exact_duplicate():
    async with httpx.AsyncClient() as client:
        initial_stats_res = await client.get(f"{BASE_URL}/stats")
        initial_stats = initial_stats_res.json()
        initial_unique = initial_stats.get("unique_processed", 0)
        initial_dropped = initial_stats.get("duplicate_dropped", 0)

        topic = f"test.dedup.{uuid.uuid4().hex[:6]}"
        event = generate_test_event(topic=topic)

        res1 = await client.post(f"{BASE_URL}/publish", json=event)
        res2 = await client.post(f"{BASE_URL}/publish", json=event)
        assert res1.status_code == 202
        assert res2.status_code == 202

        await wait_for_stats(client, "unique_processed", initial_unique + 1)
        final_stats = await wait_for_stats(client, "duplicate_dropped", initial_dropped + 1)
        
        assert final_stats["unique_processed"] == initial_unique + 1
        assert final_stats["duplicate_dropped"] == initial_dropped + 1

        events_res = await client.get(f"{BASE_URL}/events?topic={topic}")
        assert len(events_res.json()) == 1

async def test_deduplication_different_timestamp_and_payload():
    async with httpx.AsyncClient() as client:
        initial_stats_res = await client.get(f"{BASE_URL}/stats")
        initial_stats = initial_stats_res.json()
        initial_unique = initial_stats.get("unique_processed", 0)
        initial_dropped = initial_stats.get("duplicate_dropped", 0)

        topic = f"test.dedup.diff.{uuid.uuid4().hex[:6]}"
        event_id = f"evt_{uuid.uuid4().hex[:10]}"
        
        event1 = generate_test_event(topic=topic, event_id=event_id)
        
        event2 = event1.copy()
        event2["timestamp"] = datetime.utcnow().isoformat() + "Z"
        event2["payload"] = {"modified_key": "different_value"}

        res1 = await client.post(f"{BASE_URL}/publish", json=event1)
        res2 = await client.post(f"{BASE_URL}/publish", json=event2)
        assert res1.status_code == 202
        assert res2.status_code == 202

        await wait_for_stats(client, "unique_processed", initial_unique + 1)
        final_stats = await wait_for_stats(client, "duplicate_dropped", initial_dropped + 1)
        
        assert final_stats["unique_processed"] == initial_unique + 1
        assert final_stats["duplicate_dropped"] == initial_dropped + 1

        events_res = await client.get(f"{BASE_URL}/events?topic={topic}")
        events_list = events_res.json()
        assert len(events_list) == 1
        assert events_list[0]["payload"] == {"test_key": "test_val"}
