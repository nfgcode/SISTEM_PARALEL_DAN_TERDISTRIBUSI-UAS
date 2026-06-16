"""
Test Suite 7: Stress / Latency Test
===================================
Teori & Sitasi Akademik:
- Referensi: Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: a Distributed Messaging System for Log Processing. Proceedings of the NetDB, 1-7.
  * Topik: Desain broker antrean (message broker) asinkron untuk penanganan throughput tinggi dengan tingkat latensi penulisan sangat rendah.
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
  * Topik (Bab 1 & 2): Skalabilitas performa sistem terdistribusi, throughput, dan metrik latensi komunikasi.

Deskripsi:
Mengirimkan beban kerja dalam bentuk batch besar (100 event log) secara berurutan ke API, 
mengukur waktu latensi respons pengiriman. API harus merespons di bawah 1 detik (1000 ms) 
karena pemrosesan yang asinkron melalui antrean Redis.
"""

import time
import uuid
import pytest
import httpx
import logging
from datetime import datetime

# Setup Logger to resolve any NameError
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8080"
pytestmark = pytest.mark.asyncio

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

async def test_stress_small_load():
    async with httpx.AsyncClient() as client:
        topic = f"test.stress.{uuid.uuid4().hex[:6]}"
        events = [generate_test_event(topic=topic) for _ in range(100)]
        
        start_time = time.time()
        response = await client.post(f"{BASE_URL}/publish", json=events)
        latency = (time.time() - start_time) * 1000
        
        assert response.status_code == 202
        logger.info(f"Published 100 events in {latency:.2f} ms")
        assert latency < 1000.0  # Should be well under 1s for queue-based enqueueing
