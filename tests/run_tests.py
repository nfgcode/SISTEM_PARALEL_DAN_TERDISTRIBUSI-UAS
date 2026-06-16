import sys
import time
import httpx
import subprocess

BASE_URL = "http://localhost:8080"
READINESS_URL = f"{BASE_URL}/health/readiness"

def wait_for_api(timeout=60):
    print(f"Waiting for Aggregator API at {READINESS_URL}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = httpx.get(READINESS_URL, timeout=2.0)
            if response.status_code == 200:
                print("API is ready! Running tests...")
                return True
        except Exception:
            pass
        time.sleep(1.5)
    print("Error: Aggregator API was not ready within the timeout period.")
    return False

def main():
    if not wait_for_api():
        sys.exit(1)
        
    # Run pytest
    print("Executing pytest...")
    result = subprocess.run(["pytest", "tests/", "-v"])
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
