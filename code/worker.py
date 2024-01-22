import logging
import uvicorn
from fastapi import FastAPI, Request
import httpx
import sys
import asyncio
from datetime import datetime, timedelta
from pydantic import BaseModel

# Configuration Parameters
LOAD_BALANCER_ADDRESS = "localhost:8000"
WORKER_PORT = sys.argv[1] if len(sys.argv) > 1 else "8001"
WORKER_ADDRESS = f"localhost:{WORKER_PORT}"
LOG_LEVEL = logging.WARNING
REPORT_TIMEOUT = 5.0  # Timeout in seconds for reporting load
REPORT_INTERVAL = timedelta(seconds=10)  # Interval for reporting load, adjust as needed
DATABASE_ADDRESS="127.0.0.1:6570"

# Setting up basic logging
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()
active_requests = 0
worker_lock = asyncio.Lock()  # Lock for thread-safe operation on active_requests
last_report_time = datetime.now()  # Track the last time the load was reported

class Item(BaseModel):
    key: str
    value: str

async def report_current_load():
    async with worker_lock:
        current_load = active_requests

    try:
        async with httpx.AsyncClient(timeout=REPORT_TIMEOUT) as client:
            await client.post(
                f"http://{LOAD_BALANCER_ADDRESS}/report-load",
                json={"server": WORKER_ADDRESS, "load": current_load},
            )
    except Exception as e:
        logging.error(f"Error reporting load to load balancer: {e}")

async def put_data(item: Item):
    try:
        async with httpx.AsyncClient(timeout=REPORT_TIMEOUT) as client:
            await client.post(
                f"http://{DATABASE_ADDRESS}/put_data",
                json={"key": item.key, "value": item.value},
            )
    except Exception as e:
        logging.error(f"Error posting data: {e}")

async def get_data(key: str):
    try:
        async with httpx.AsyncClient(timeout=REPORT_TIMEOUT) as client:
            response = await client.get(
                f"http://{DATABASE_ADDRESS}/get_data/{key}",
            ).json
            return response.str
    except Exception as e:
        logging.error(f"Error getting data: {e}")

@app.middleware("http")
async def count_request(request: Request, call_next):
    global active_requests, last_report_time
    current_time = datetime.now()

    async with worker_lock:
        active_requests += 1
        logging.info(f"Request received. Active requests: {active_requests}")

    response = await call_next(request)

    async with worker_lock:
        active_requests -= 1
        # logging.info(f"Request completed. Active requests: {active_requests}")

    # Report load if the interval has elapsed
    if current_time - last_report_time > REPORT_INTERVAL:
        await report_current_load()
        last_report_time = current_time

    return response

@app.get("/worker-health")
def health_check():
    logging.info(f"Health check: {active_requests} active requests")
    return {"status": "OK", "active_requests": active_requests}

@app.get("/")
def worker_info():
    logging.info(f"This is the worker {WORKER_ADDRESS}")
    return {"status": "OK", "active_requests": active_requests}

@app.post("/put_data")
async def create_item(item: Item):
    response = await put_data(item)
    return {response}

@app.get("/get_data/{key}")
async def retrieve_item(key: str):
    response = await get_data(key)
    print(response)
    return(response)


@app.get("/test")
def test_endpoint():
    return {"message": "Test endpoint in worker reached successfully."}

async def startup_event():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"http://{LOAD_BALANCER_ADDRESS}/register-worker", json={"server": WORKER_ADDRESS})
            if response.status_code == 200:
                logging.info(f"Successfully registered with load balancer at {LOAD_BALANCER_ADDRESS}")
            else:
                logging.error(f"Failed to register with load balancer: Status code {response.status_code}")
        except Exception as e:
            logging.error(f"Exception occurred while registering with load balancer: {e}")

app.add_event_handler("startup", startup_event)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(WORKER_PORT))
