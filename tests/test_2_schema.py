"""
Test Suite 2: Schema Validation
===============================
Teori & Sitasi Akademik:
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
- Topik (Bab 3 & 4): Model komunikasi request-reply asinkron, marshalling dan unmarshalling data, 
  serta pemrosesan validasi tipe data di sisi antarmuka API (Pydantic validation).

Deskripsi:
Menguji kekokohan validasi skema event log yang masuk ke API. 
Sistem harus mendeteksi secara dini field wajib yang hilang, format timestamp yang tidak valid, 
atau adanya field tambahan yang tidak diizinkan sebelum log diproses ke broker Redis.
"""

import pytest
import httpx
import uuid
from datetime import datetime

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

async def test_schema_valid_event():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 202
        assert response.json()["status"] == "enqueued"

async def test_schema_missing_topic():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        del event["topic"]
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 422  # Unprocessable Entity (Pydantic validation)

async def test_schema_missing_event_id():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        del event["event_id"]
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 422

async def test_schema_invalid_timestamp():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        event["timestamp"] = "invalid-timestamp"
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 400
        assert "Invalid ISO8601 timestamp" in response.json()["detail"]

async def test_schema_missing_source():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        del event["source"]
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 422

async def test_schema_missing_payload():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        del event["payload"]
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 422

async def test_schema_extra_fields():
    async with httpx.AsyncClient() as client:
        event = generate_test_event()
        event["extra_field"] = "not_allowed"
        response = await client.post(f"{BASE_URL}/publish", json=event)
        assert response.status_code == 422
