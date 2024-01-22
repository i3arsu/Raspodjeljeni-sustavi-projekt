"""
from fastapi import FastAPI, HTTPException
from load_balancer import DynamicLoadBalancer
import httpx
import asyncio

app = FastAPI()

@app.get('/')
def read_root():
    return {'message': 'Hello, World!'}


@app.get('/health-check')
def health_check():
    return {'status': 'OK'}

@app.get('/health-check')
def health_check():
    raise HTTPException(status_code=500, detail="Internal Server Error")

# Create an instance of DynamicLoadBalancer
load_balancer = DynamicLoadBalancer(health_check_interval=5)

# Example route to get server information
@app.get('/server-info')
def get_server_info():
    server_info_list = []

    # Access and collect server information
    for server_info in load_balancer.servers:
        server = server_info["server"]
        healthy_status = "Healthy" if server_info["healthy"] else "Unhealthy"
        last_checked = server_info["last_checked"]
        server_info_list.append({"server": server, "status": healthy_status, "last_checked": last_checked})

    return {"server_info": server_info_list}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)
"""

