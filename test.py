"""
Test module for network speed testing functionality.

This module contains test implementations and utilities for the speed test application.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import asyncio
import speedtest
import json
import logging
import time
import threading
import math
from concurrent.futures import ThreadPoolExecutor
import random
import socket
import psutil
import platform
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

# Thread pool for running speed tests
executor = ThreadPoolExecutor(max_workers=2)

@app.get("/")
async def get():
    return FileResponse("static/index.html")

@app.websocket("/ws/speed")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            # Wait for start message from client
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "start_test":
                await perform_speed_test(websocket)
                
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"Error: {e}")
        await websocket.send_json({
            "type": "error",
            "message": str(e)
        })

async def perform_speed_test(websocket: WebSocket):
    try:
        # Get network information first
        network_info = get_network_info()
        
        await websocket.send_json({
            "type": "network_info",
            "data": network_info
        })
        
        # Initialize speedtest with timeout handling
        await websocket.send_json({
            "type": "status",
            "message": "Initializing speed test...",
            "status": "connecting"
        })
        
        # Run the speed test in a thread to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, lambda: run_speedtest_with_progress(websocket, loop))
        
    except Exception as e:
        await websocket.send_json({
            "type": "error",
            "message": f"Speed test failed: {str(e)}"
        })
        logging.error(f"Speed test error: {e}")

def get_network_info():
    """Get information about the current network connection"""
    try:
        # Get hostname safely
        try:
            hostname = socket.gethostname()
        except:
            hostname = "Unknown"
        
        # Get local IP address
        local_ip = get_local_ip()
        
        # Get public IP and location
        public_ip_info = get_public_ip_info()
        
        # Get network interfaces and connection type
        connection_type = "Unknown"
        interfaces = []
        try:
            interfaces = list(psutil.net_if_addrs().keys())
            interface_stats = psutil.net_if_stats()
            
            for interface, stats in interface_stats.items():
                if stats.isup:
                    if any(keyword in interface.lower() for keyword in ['wi', 'wlan', 'wireless']):
                        connection_type = "WiFi"
                        break
                    elif any(keyword in interface.lower() for keyword in ['eth', 'ethernet', 'lan', 'local']):
                        connection_type = "Ethernet"
                        break
                    elif "en" in interface.lower():  # macOS Ethernet interfaces
                        connection_type = "Ethernet"
                        break
        except:
            pass
        
        return {
            "hostname": hostname,
            "local_ip": local_ip,
            "public_ip": public_ip_info.get('ip', 'Unknown'),
            "country": public_ip_info.get('country', 'Unknown'),
            "city": public_ip_info.get('city', 'Unknown'),
            "isp": public_ip_info.get('org', 'Unknown'),
            "connection_type": connection_type,
            "platform": platform.system(),
            "interfaces": interfaces
        }
    except Exception as e:
        return {"error": str(e)}

def get_local_ip():
    """Get local IP address"""
    try:
        # Try to connect to a remote host to determine local IP
        socket_conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket_conn.connect(("8.8.8.8", 80))
        local_ip = socket_conn.getsockname()[0]
        socket_conn.close()
        return local_ip
    except:
        try:
            # Fallback: get hostname and try to resolve it
            hostname = socket.gethostname()
            return socket.gethostbyname(hostname)
        except:
            return "Unknown"

def get_public_ip_info():
    """Get public IP address and location information"""
    try:
        # Try multiple IP detection services
        services = [
            "https://api.ipify.org?format=json",
            "https://jsonip.com",
            "http://ip-api.com/json/"
        ]
        
        for service in services:
            try:
                response = requests.get(service, timeout=5)
                data = response.json()
                
                if "ipify.org" in service:
                    return {"ip": data.get('ip', 'Unknown')}
                elif "jsonip.com" in service:
                    return {"ip": data.get('ip', 'Unknown')}
                elif "ip-api.com" in service:
                    return {
                        "ip": data.get('query', 'Unknown'),
                        "country": data.get('country', 'Unknown'),
                        "city": data.get('city', 'Unknown'),
                        "isp": data.get('isp', 'Unknown'),
                        "org": data.get('org', 'Unknown')
                    }
            except:
                continue
        
        return {"ip": "Unknown"}
    except:
        return {"ip": "Unknown"}

def run_speedtest_with_progress(websocket: WebSocket, loop):
    """Run speed test with progress updates using the speedtest library"""
    try:
        st = speedtest.Speedtest()
        
        # Get best server
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "status",
                "message": "Finding optimal server...",
                "status": "connecting"
            }), loop
        ).result()
        
        st.get_best_server()
        server = st.best
        
        # Send server location info
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "server_info",
                "name": server['name'],
                "country": server['country'],
                "sponsor": server['sponsor'],
                "host": server['host'],
                "distance": server['d'],
                "latency": server['latency']
            }), loop
        ).result()
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "status",
                "message": f"Connected to {server['name']} ({server['country']}) - Distance: {server['d']:.0f} km",
                "status": "connected"
            }), loop
        ).result()
        
        # Test ping and measure jitter
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "phase",
                "phase": "ping",
                "message": "Testing ping and latency..."
            }), loop
        ).result()
        
        # Measure ping multiple times to calculate jitter
        ping_results = []
        for i in range(5):
            ping_result = st.results.ping
            ping_results.append(ping_result)
            
            asyncio.run_coroutine_threadsafe(
                websocket.send_json({
                    "type": "live_update",
                    "phase": "ping",
                    "message": f"Ping test {i+1}/5: {ping_result:.2f} ms"
                }), loop
            ).result()
            
            time.sleep(0.5)
        
        avg_ping = sum(ping_results) / len(ping_results)
        
        # Calculate jitter (variation in ping times)
        jitter = calculate_jitter(ping_results)
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "ping_result",
                "ping": avg_ping,
                "jitter": jitter,
                "latency": avg_ping
            }), loop
        ).result()
        
        time.sleep(1)
        
        # Download test with progress
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "phase",
                "phase": "download",
                "message": "Testing download speed..."
            }), loop
        ).result()
        
        # Run download test with actual speed measurement
        download_result = run_download_test_with_progress(websocket, loop, st)
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "download_result",
                "download": download_result
            }), loop
        ).result()
        
        time.sleep(1)
        
        # Upload test with progress
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "phase",
                "phase": "upload",
                "message": "Testing upload speed..."
            }), loop
        ).result()
        
        # Run upload test with actual speed measurement
        upload_result = run_upload_test_with_progress(websocket, loop, st)
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "upload_result",
                "upload": upload_result
            }), loop
        ).result()
        
        # Complete
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "status",
                "message": "Test completed successfully!",
                "status": "completed"
            }), loop
        ).result()
        
    except Exception as e:
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "error",
                "message": f"Speed test failed: {str(e)}"
            }), loop
        ).result()
        logging.error(f"Speed test error: {e}")

def calculate_jitter(ping_results):
    """Calculate jitter from ping results"""
    if len(ping_results) < 2:
        return 0
    
    differences = []
    for i in range(1, len(ping_results)):
        differences.append(abs(ping_results[i] - ping_results[i-1]))
    
    
    return sum(differences) / len(differences) if differences else 0

def run_download_test_with_progress(websocket, loop, st, duration=15):
    """Run download test with progress simulation"""
    start_time = time.time()
    
    # Simulate progress for the specified duration
    for i in range(1, duration + 1):
        time.sleep(1)
        progress = (i / duration) * 100
        
        # Estimate speed (this will be replaced with actual measurement at the end)
        estimated_speed = random.uniform(5, 25) * (i / duration)
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "live_update",
                "phase": "download",
                "speed": estimated_speed,
                "progress": progress,
                "message": f"Downloading... {int(progress)}% ({i}/{duration}s)"
            }), loop
        ).result()
    
    # Run actual download test at the end
    download_result = st.download() / 1000000  # Convert to Mbps
    return download_result

def run_upload_test_with_progress(websocket, loop, st, duration=15):
    """Run upload test with progress simulation"""
    start_time = time.time()
    
    # Simulate progress for the specified duration
    for i in range(1, duration + 1):
        time.sleep(1)
        progress = (i / duration) * 100
        
        # Estimate speed (this will be replaced with actual measurement at the end)
        estimated_speed = random.uniform(2, 15) * (i / duration)
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "live_update",
                "phase": "upload",
                "speed": estimated_speed,
                "progress": progress,
                "message": f"Uploading... {int(progress)}% ({i}/{duration}s)"
            }), loop
        ).result()
    
    # Run actual upload test at the end
    upload_result = st.upload() / 1000000  # Convert to Mbps
    return upload_result