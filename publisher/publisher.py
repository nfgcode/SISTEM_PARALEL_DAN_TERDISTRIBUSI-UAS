import os
import time
import json
import random
import uuid
import asyncio
import logging
import httpx
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("publisher")

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
READINESS_URL = os.getenv("READINESS_URL", "http://aggregator:8080/health/readiness")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.30"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))

TOPICS = ["system.auth", "payment.process", "user.profile", "order.checkout", "inventory.update"]
SOURCES = ["auth-service", "gateway-api", "billing-worker", "frontend-client", "db-cleaner"]

def generate_event(event_id=None) -> dict:
    """
    Referensi Teori:
    - Coulouris dkk. (2012) - Bab 4: Globally Unique Identifier menggunakan UUID v4.
    - Coulouris dkk. (2012) - Bab 5: Waktu Fisik & Ordering (ISO 8601 Timestamp).
    """
    if event_id is None:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
    return {
        "topic": random.choice(TOPICS),
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": random.choice(SOURCES),
        "payload": {
            "request_id": f"req_{uuid.uuid4().hex[:8]}",
            "execution_time_ms": round(random.uniform(5.0, 150.0), 2),
            "status": random.choice(["success", "failed", "pending"]),
            "details": "Simulated system telemetry event log data."
        }
    }

async def wait_for_readiness(client: httpx.AsyncClient, timeout: int = 60):
    logger.info(f"Checking readiness of Aggregator API at {READINESS_URL}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = await client.get(READINESS_URL)
            if response.status_code == 200:
                logger.info("Aggregator API is ready!")
                return True
        except Exception:
            pass
        logger.info("Aggregator not ready yet. Retrying in 2 seconds...")
        await asyncio.sleep(2)
    raise RuntimeError("Timeout waiting for Aggregator API to become ready.")

async def send_batch(client: httpx.AsyncClient, batch: list, semaphore: asyncio.Semaphore, realtime_logs: list, latencies: list, stats: dict):
    async with semaphore:
        start_time = time.time()
        timestamp = datetime.utcnow().isoformat() + "Z"
        status_code = 0
        try:
            payload = batch[0] if len(batch) == 1 and BATCH_SIZE == 1 else batch
            response = await client.post(TARGET_URL, json=payload, timeout=30.0)
            latency = (time.time() - start_time) * 1000  # ms
            latencies.append(latency)
            status_code = response.status_code
            
            if response.status_code == 202:
                stats["succeeded_events"] += len(batch)
                stats["succeeded_requests"] += 1
            else:
                stats["failed_events"] += len(batch)
                stats["failed_requests"] += 1
                logger.error(f"Failed to send events. Status: {response.status_code}, Body: {response.text}")
        except Exception as e:
            latency = (time.time() - start_time) * 1000
            latencies.append(latency)
            stats["failed_events"] += len(batch)
            stats["failed_requests"] += 1
            logger.error(f"Error sending request: {e}")
            status_code = 500

        # Append realtime trace log
        realtime_logs.append({
            "timestamp": timestamp,
            "url": TARGET_URL,
            "method": "POST",
            "status": status_code,
            "latency_ms": round(latency, 2),
            "events_count": len(batch)
        })

async def main():
    logger.info("=== Starting Publisher Load Simulator ===")
    logger.info(f"Configuration:")
    logger.info(f"  Target URL: {TARGET_URL}")
    logger.info(f"  Total Events to Send: {TOTAL_EVENTS}")
    logger.info(f"  Duplicate Rate Target: {DUPLICATE_RATE * 100}%")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  Concurrency: {CONCURRENCY}")
    
    # 1. Generate Event Dataset
    # Referensi Teori:
    # - Coulouris dkk. (2012) - Bab 3: At-Least-Once Delivery Simulation
    #   Mensimulasikan pengiriman ulang pesan yang memicu event duplikat minimum 30%.
    logger.info("Generating event dataset...")
    num_duplicates = int(TOTAL_EVENTS * DUPLICATE_RATE)
    num_uniques = TOTAL_EVENTS - num_duplicates
    
    unique_events = [generate_event() for _ in range(num_uniques)]
    
    duplicates = []
    for _ in range(num_duplicates):
        base_event = random.choice(unique_events)
        dup = base_event.copy()
        # Menggunakan event_id dan topic yang sama, namun dengan timestamp baru (delivery retry)
        dup["timestamp"] = datetime.utcnow().isoformat() + "Z"
        duplicates.append(dup)
        
    dataset = unique_events + duplicates
    random.shuffle(dataset)
    
    logger.info(f"Dataset generated: {len(dataset)} total events ({num_uniques} unique, {num_duplicates} duplicates).")
    
    # 2. Split dataset into batches
    batches = [dataset[i:i + BATCH_SIZE] for i in range(0, len(dataset), BATCH_SIZE)]
    logger.info(f"Prepared {len(batches)} batches for sending.")
    
    # 3. Send requests asynchronously
    latencies = []
    realtime_logs = []
    stats = {
        "succeeded_events": 0,
        "failed_events": 0,
        "succeeded_requests": 0,
        "failed_requests": 0
    }
    
    limits = httpx.Limits(max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY * 2)
    async with httpx.AsyncClient(limits=limits) as client:
        await wait_for_readiness(client)
        
        semaphore = asyncio.Semaphore(CONCURRENCY)
        
        logger.info(f"Starting simulation run...")
        start_time = time.time()
        
        tasks = [
            send_batch(client, batch, semaphore, realtime_logs, latencies, stats)
            for batch in batches
        ]
        
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        
    duration = end_time - start_time
    logger.info("Simulation run completed.")
    
    # 4. Process metrics
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        sorted_latencies = sorted(latencies)
        p95_latency = sorted_latencies[int(len(sorted_latencies) * 0.95)]
        p99_latency = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        min_latency = sorted_latencies[0]
        max_latency = sorted_latencies[-1]
    else:
        avg_latency = p95_latency = p99_latency = min_latency = max_latency = 0
        
    throughput = stats["succeeded_events"] / duration if duration > 0 else 0
    
    # Print console report
    print("\n" + "="*50)
    print("                SIMULATION RESULTS")
    print("="*50)
    print(f"Total Duration        : {duration:.2f} seconds")
    print(f"Total Events Attempted: {TOTAL_EVENTS}")
    print(f"Events Succeeded      : {stats['succeeded_events']}")
    print(f"Events Failed         : {stats['failed_events']}")
    print(f"Total Requests Sent   : {stats['succeeded_requests'] + stats['failed_requests']}")
    print(f"Throughput            : {throughput:.2f} events/second")
    print(f"Average Request Latency: {avg_latency:.2f} ms")
    print(f"95th Percentile Latency: {p95_latency:.2f} ms")
    print(f"99th Percentile Latency: {p99_latency:.2f} ms")
    print(f"Duplicate Rate        : {DUPLICATE_RATE * 100:.1f}%")
    print("="*50 + "\n")
    
    # 5. Export reports
    output_dir = "/app/output" if os.path.exists("/app/output") else "."
    
    summary_report = {
        "metrics": {
            "total_duration_seconds": round(duration, 2),
            "total_events": TOTAL_EVENTS,
            "succeeded_events": stats["succeeded_events"],
            "failed_events": stats["failed_events"],
            "total_requests": stats["succeeded_requests"] + stats["failed_requests"],
            "throughput_events_per_sec": round(throughput, 2),
            "latency_ms": {
                "min": round(min_latency, 2),
                "max": round(max_latency, 2),
                "avg": round(avg_latency, 2),
                "p95": round(p95_latency, 2),
                "p99": round(p99_latency, 2)
            },
            "duplicate_rate_percent": DUPLICATE_RATE * 100
        }
    }
    
    # Save summary report
    report_path = os.path.join(output_dir, "report.json")
    with open(report_path, "w") as f:
        json.dump(summary_report, f, indent=4)
    logger.info(f"Saved summary report to: {report_path}")
    
    # Save realtime log stream (JSON Lines format)
    realtime_path = os.path.join(output_dir, "realtime-report.json")
    with open(realtime_path, "w") as f:
        for log in realtime_logs:
            f.write(json.dumps(log) + "\n")
    logger.info(f"Saved realtime log stream to: {realtime_path}")

    # Remain idle
    logger.info("Publisher idle. Container will remain active for demo purposes.")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Publisher stopped.")
