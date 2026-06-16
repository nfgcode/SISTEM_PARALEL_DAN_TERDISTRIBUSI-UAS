import os
import time

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/db")
TRANSACTION_ISOLATION = os.getenv("TRANSACTION_ISOLATION", "read_committed")
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379/0")
QUEUE_NAME = os.getenv("QUEUE_NAME", "event_queue")

# Tracks application start time for uptime calculation
START_TIME = time.time()
