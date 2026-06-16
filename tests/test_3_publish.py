"""
Test Suite 3: Publish and Process Functionality
==============================================
Teori & Sitasi Akademik:
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
  * Topik (Bab 1 & 2): Model komunikasi asinkron Publish-Subscribe dan modularitas microservices (decoupling).
- Referensi: Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: a Distributed Messaging System for Log Processing. Proceedings of the NetDB, 1-7.
  * Topik: Desain broker antrean (message broker) asinkron untuk penanganan log telemetri skala besar dengan latensi rendah.

Deskripsi:
Menguji pengiriman event log secara tunggal maupun batch. Memastikan API dapat menerima event log,
memasukkannya ke antrean Redis, dan consumer worker latar belakang dapat memproses serta menyimpannya
ke database PostgreSQL secara konsisten.
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

async def test_publish_single_event_processed():
    async with httpx.AsyncClient() as client:
        initial_stats_res = await client.get(f"{BASE_URL}/stats")
        initial_stats = initial_stats_res.json()
        initial_unique = initial_stats.get("unique_processed", 0)

        topic = f"test.single.{uuid.uuid4().hex[:6]}"
        event = generate_test_event(topic=topic)
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 202

        updated_stats = await wait_for_stats(client, "unique_processed", initial_unique + 1)
        assert updated_stats["unique_processed"] >= initial_unique + 1

        events_res = await client.get(f"{BASE_URL}/events?topic={topic}")
        assert events_res.status_code == 200
        events_list = events_res.json()
        assert len(events_list) == 1
        assert events_list[0]["event_id"] == event["event_id"]

async def test_publish_batch_events():
    async with httpx.AsyncClient() as client:
        initial_stats_res = await client.get(f"{BASE_URL}/stats")
        initial_stats = initial_stats_res.json()
        initial_unique = initial_stats.get("unique_processed", 0)

        topic = f"test.batch.{uuid.uuid4().hex[:6]}"
        events = [generate_test_event(topic=topic) for _ in range(5)]
        
        response = await client.post(f"{BASE_URL}/publish", json=events)
        assert response.status_code == 202
        assert response.json()["count"] == 5

        updated_stats = await wait_for_stats(client, "unique_processed", initial_unique + 5)
        assert updated_stats["unique_processed"] >= initial_unique + 5

        events_res = await client.get(f"{BASE_URL}/events?topic={topic}")
        assert len(events_res.json()) == 5
