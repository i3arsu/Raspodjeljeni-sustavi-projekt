import logging
import asyncio
import time
from fastapi import FastAPI, HTTPException, Request, Response
import httpx
from contextlib import asynccontextmanager
from pydantic import BaseModel
import subprocess
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
import os
import cProfile

# Configurable Parameters
HEALTH_CHECK_INTERVAL = 10  # seconds
LOG_LEVEL = logging.WARNING

# Setting up basic logging
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

class Item(BaseModel):
    key: str
    value: str

class Worker(BaseModel):
    server: str
    healthy: bool = True
    active_requests: int = 0

class LoadReport(BaseModel):
    server: str
    load: int

#START OF CLASS
class DynamicLoadBalancer:
    def __init__(self, health_check_interval=HEALTH_CHECK_INTERVAL):
        self.servers = []
        self.health_check_interval = health_check_interval
        self.shutdown_event = asyncio.Event()
        self.max_requests_per_worker = 10
        self.max_workers = 5
        self.current_worker_index = -1  # For round-robin
        self.active_workers = {}  # Track active workers

    def get_next_server(self):
        healthy_servers = [server for server in self.servers if server.healthy]
        if not healthy_servers:
            logging.error("No healthy servers available.")
            raise ValueError("No healthy servers available.")
        self.current_worker_index = (self.current_worker_index + 1) % len(healthy_servers)
        logging.info(f"Next server selected: {healthy_servers[self.current_worker_index].server}")
        return healthy_servers[self.current_worker_index]

    async def start_new_worker(self):
        new_worker_port = self.get_next_available_port()
        new_worker_address = f"localhost:{new_worker_port}"
        subprocess.Popen(["python", "worker.py", str(new_worker_port)])
        self.register_server(new_worker_address)
        logging.info(f"New worker started and registered: {new_worker_address}")

    def get_next_available_port(self):
        existing_ports = [int(worker.server.split(':')[1]) for worker in self.servers]
        return max(existing_ports) + 1 if existing_ports else 8001

    def register_server(self, server: str):
        if server in [worker.server for worker in self.servers]:
            return  # Skip registration if already registered
        self.servers.append(Worker(server=server, active_requests=0))

    def remove_server(self, server: str):
        self.servers = [s for s in self.servers if s.server != server]

    def should_scale_up(self):
        total_requests = sum(worker.active_requests for worker in self.servers)
        if total_requests > self.max_requests_per_worker * len(self.servers):
            logging.info("Scaling up due to high load...")
            return True
        return False
    
    def should_scale_down(self):
        total_requests = sum(worker.active_requests for worker in self.servers)
        if total_requests < self.max_requests_per_worker * (len(self.servers) - 1):
            logging.info("Scaling down due to low load...")
            return True
        return False

    async def forward_request(self, path: str, request: Request):
        worker = load_balancer.get_next_server()
        url = f"http://{worker.server}" + request.url.path

        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=request.method,
                url=url,
                headers=request.headers,
                data=await request.body()
        )

        return Response(content=response.content, status_code=response.status_code, headers=dict(response.headers))


    async def perform_health_checks(self):
        while not self.shutdown_event.is_set():
            for server in self.servers:
                response = await self.check_server_health(server)
                if response:
                    logging.info(f"Health check for {server.server}: Healthy")
                else:
                    logging.info(f"Health check for {server.server}: Unhealthy")
            await asyncio.sleep(self.health_check_interval)
            logging.info(f"Performing health check for {server.server}")

    async def scale_workers(self):
        while not self.shutdown_event.is_set():
            if self.should_scale_up():
                await self.start_new_worker()
            await asyncio.sleep(self.health_check_interval)

    async def check_server_health(self, server):
        try:
            async with httpx.AsyncClient() as client:
                url = f"http://{server.server}/worker-health"
                response = await client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    server.healthy = data["status"] == "OK"
                    server.active_requests = data.get("active_requests", 0)
                    health_status = 'Healthy' if server.healthy else 'Unhealthy'
                    logging.info(f"Health check for {server.server}: {server.active_requests} active requests - {health_status}")
                else:
                    server.healthy = False
                    logging.info(f"Health check for {server.server}: Response status code {response.status_code} - Unhealthy")
        except Exception as e:
            logging.error(f"Health check failed for {server.server}: {e}, type: {type(e).__name__}")
            server.healthy = False
    async def shutdown(self):
        self.shutdown_event.set()
        logging.info("Shutdown initiated for Load Balancer.")

#END OF CLASS


load_balancer = DynamicLoadBalancer()



@asynccontextmanager
async def app_lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient()
    app.state.load_balancer = load_balancer
    health_task = asyncio.create_task(load_balancer.perform_health_checks())
    scaling_task = asyncio.create_task(load_balancer.scale_workers())
    try:
        yield
    finally:
        health_task.cancel()
        scaling_task.cancel()
        await asyncio.gather(health_task, scaling_task, return_exceptions=True)
        await load_balancer.shutdown()
        await app.state.http_client.aclose()

app = FastAPI(lifespan=app_lifespan)

#EXCEPTION HANDLER
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.error(f"Validation error: {exc.body}")
    return PlainTextResponse(str(exc), status_code=422)

#ROOT ENDPOINT
@app.get("/")
async def root():
    return {"message": "This is the load balancer!"}

#REGISTER WORKER
@app.post("/register-worker")
async def register_worker(worker: Worker):
    app.state.load_balancer.register_server(worker.server)
    logging.info(f"Worker {worker.server} connected to load balancer.")
    return {"message": f"Worker {worker.server} registered successfully."}       


#REPORT LOAD
@app.post("/report-load")
async def report_load(report: LoadReport):
    logging.info(f"Received load report: {report.model_dump_json()}")
    found = False
    for worker in load_balancer.servers:
        if worker.server == report.server:
            worker.active_requests = report.load
            logging.info(f"Load updated for worker {report.server}: {report.load} active requests")
            found = True
            break
    if not found:
        logging.warning(f"Worker {report.server} not found in registered servers")
    return {"message": "Load updated"}

# @app.post("/put_data")
# async def create_item(item: Item):
#     response = await app.state.load_balancer.put_data(item)
#     return {response}

# @app.get("/get_data/{key}")
# async def retrieve_item(key: str):
#     response = await get_data(key)
#     return(response)

#LOAD BALANCER HEALTH
@app.get("/load-balancer-health")
def load_balancer_health():
    worker_statuses = []
    for worker in load_balancer.servers:
        status = "healthy" if worker.healthy else "unhealthy"
        worker_statuses.append(f"  {{'worker_address': '{worker.server}', 'status': '{status}'}}")
    formatted_response = "\n".join([
        "{",
        "  'status': 'OK',",
        "  'load_balancer_message': 'Load Balancer is operational.',",
        "  'workers': [",
        ",\n".join(worker_statuses),
        "  ]",
        "}"
    ])
    return Response(content=formatted_response, media_type="application/json")


#NEXT SERVER
@app.get("/next")
def get_next_server():
    try:
        next_server = app.state.load_balancer.get_next_server()
        return {"next_server": next_server.server}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

#LIST WORKERS
@app.get("/list-workers")
def list_workers():
    registered_workers = [s.server for s in app.state.load_balancer.servers]
    return {"registered_workers": registered_workers}


@app.route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])

#TEST ENDPOINT
@app.route("/test", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def test_endpoint(request: Request):
    return await app.state.load_balancer.forward_request("/test", request)

@app.route("/put_data", methods=["POST"])
async def test_put_data(request: Request):
    return await app.state.load_balancer.forward_request("/put_data", request)

@app.route("/get_data/{key}", methods=["GET"])
async def test_get_data(key: str,request: Request):
    return await app.state.load_balancer.forward_request(f"/get_data/{key}",request)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)