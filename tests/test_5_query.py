"""
Test Suite 5: Events Query Filtering
===================================
Teori & Sitasi Akademik:
- Referensi: Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). Distributed Systems: Concepts and Design (5th ed.). Addison-Wesley.
  * Topik (Bab 3 & 4): Protokol query / pencarian data dalam model komunikasi sistem terdistribusi serta penanganan error input.

Deskripsi:
Menguji endpoint pencarian log (GET /events). Memverifikasi bahwa parameter query 'topic' bersifat 
wajib (menghasilkan error 422 jika kosong) dan memastikan respons pencarian aman serta mengembalikan 
daftar kosong ([]) jika nama topik tidak ada di database.
"""

import pytest
import httpx

BASE_URL = "http://localhost:8080"
pytestmark = pytest.mark.asyncio

async def test_get_events_missing_topic_query():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/events")
        assert response.status_code == 422  # topic is a required query parameter

async def test_get_events_nonexistent_topic():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/events?topic=nonexistent_topic_xyz")
        assert response.status_code == 200
        assert response.json() == []
