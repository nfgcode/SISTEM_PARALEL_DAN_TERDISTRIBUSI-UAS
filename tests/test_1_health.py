"""
Test Suite 1: Health Endpoints
==============================
Teori & Sitasi Akademik:
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
- Topik (Bab 6 & 10-13): Pendeteksian kegagalan proses (Failure Detection) melalui mekanisme detak jantung (Heartbeat) 
  dan pemeriksaan kesiapan sistem (Readiness/Liveness probes) untuk koordinasi layanan terdistribusi.

Deskripsi:
Memverifikasi keaktifan (Liveness) dan kesiapan (Readiness) dari service log aggregator.
Readiness check secara dinamis memeriksa konektivitas API ke database PostgreSQL dan Redis broker.
"""

import pytest
import httpx

BASE_URL = "http://localhost:8080"
pytestmark = pytest.mark.asyncio

async def test_liveness():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/health/liveness")
        assert response.status_code == 200
        assert response.json() == {"status": "alive"}

async def test_readiness():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/health/readiness")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}
