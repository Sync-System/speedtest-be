
"""
Network Speed Test Web Application

A FastAPI-based speed test application with WebSocket support for real-time
speed testing functionality.
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
        
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "status",
                "message": f"Connected to {server['name']} ({server['country']})",
                "status": "connected"
            }), loop
        ).result()
        
        # Test ping
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "phase",
                "phase": "ping",
                "message": "Testing ping..."
            }), loop
        ).result()
        
        ping_result = st.results.ping
        asyncio.run_coroutine_threadsafe(
            websocket.send_json({
                "type": "ping_result",
                "ping": ping_result
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
        
        # Create a custom progress tracker for download
        download_progress = ProgressTracker(websocket, loop, "download")
        
        # Monkey patch the download method to track progress
        original_download = st.download
        def tracked_download(*args, **kwargs):
            # This is a simplified approach - actual progress tracking would be more complex
            start_time = time.time()
            result = original_download(*args, **kwargs)
            download_time = time.time() - start_time
            download_speed = (result / 1000000)  # Convert to Mbps
            
            # Simulate progress updates (since we can't get real progress from speedtest-cli)
            for i in range(1, 11):
                time.sleep(download_time / 10)
                progress = i * 10
                estimated_speed = download_speed * (i / 10)
                
                asyncio.run_coroutine_threadsafe(
                    websocket.send_json({
                        "type": "live_update",
                        "phase": "download",
                        "speed": estimated_speed,
                        "progress": progress,
                        "message": f"Downloading... {progress}%"
                    }), loop
                ).result()
            
            return result
        
        st.download = tracked_download
        download_result = st.download() / 1000000  # Convert to Mbps
        
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
        
        # Create a custom progress tracker for upload
        upload_progress = ProgressTracker(websocket, loop, "upload")
        
        # Monkey patch the upload method to track progress
        original_upload = st.upload
        def tracked_upload(*args, **kwargs):
            start_time = time.time()
            result = original_upload(*args, **kwargs)
            upload_time = time.time() - start_time
            upload_speed = (result / 1000000)  # Convert to Mbps
            
            # Simulate progress updates
            for i in range(1, 11):
                time.sleep(upload_time / 10)
                progress = i * 10
                estimated_speed = upload_speed * (i / 10)
                
                asyncio.run_coroutine_threadsafe(
                    websocket.send_json({
                        "type": "live_update",
                        "phase": "upload",
                        "speed": estimated_speed,
                        "progress": progress,
                        "message": f"Uploading... {progress}%"
                    }), loop
                ).result()
            
            return result
        
        st.upload = tracked_upload
        upload_result = st.upload() / 1000000  # Convert to Mbps
        
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

class ProgressTracker:
    """Helper class to track progress"""
    def __init__(self, websocket, loop, phase):
        self.websocket = websocket
        self.loop = loop
        self.phase = phase
        self.start_time = time.time()
        self.last_update = self.start_time
    
    def update(self, progress, speed):
        """Update progress with throttling to avoid too frequent updates."""
        current_time = time.time()
        if current_time - self.last_update >= 0.5:  # Update every 0.5 seconds
            asyncio.run_coroutine_threadsafe(
                self.websocket.send_json({
                    "type": "live_update",
                    "phase": self.phase,
                    "speed": speed,
                    "progress": progress,
                    "message": f"{self.phase.capitalize()}ing... {progress}%"
                }), self.loop
            ).result()
            self.last_update = current_time

# Alternative simple approach without monkey patching
async def run_simple_speedtest(websocket: WebSocket):
    """Run a simple speed test with simulated progress"""
    try:
        await websocket.send_json({
            "type": "status",
            "message": "Starting speed test...",
            "status": "connecting"
        })
        
        st = speedtest.Speedtest()
        
        # Get best server
        await websocket.send_json({
            "type": "status",
            "message": "Finding optimal server...",
            "status": "connecting"
        })
        
        st.get_best_server()
        server = st.best
        
        await websocket.send_json({
            "type": "status",
            "message": f"Connected to {server['name']} ({server['country']})",
            "status": "connected"
        })
        
        # Test ping
        await websocket.send_json({
            "type": "phase",
            "phase": "ping",
            "message": "Testing ping..."
        })
        
        ping_result = st.results.ping
        await websocket.send_json({
            "type": "ping_result",
            "ping": ping_result
        })
        
        await asyncio.sleep(1)
        
        # Download test with simulated progress
        await websocket.send_json({
            "type": "phase",
            "phase": "download",
            "message": "Testing download speed..."
        })
        
        # Simulate download progress
        download_speeds = []
        for i in range(1, 11):
            await asyncio.sleep(1)  # Simulate 1 second intervals
            
            # Get actual speed from speedtest (this is a approximation)
            if i == 10:  # Only run actual test at the end
                download_result = st.download() / 1000000
                download_speeds.append(download_result)
            else:
                # Simulate intermediate speeds
                simulated_speed = random.uniform(5, 25)
                download_speeds.append(simulated_speed)
            
            await websocket.send_json({
                "type": "live_update",
                "phase": "download",
                "speed": download_speeds[-1],
                "progress": i * 10,
                "message": f"Downloading... {i * 10}%"
            })
        
        await websocket.send_json({
            "type": "download_result",
            "download": download_result
        })
        
        await asyncio.sleep(1)
        
        # Upload test with simulated progress
        await websocket.send_json({
            "type": "phase",
            "phase": "upload",
            "message": "Testing upload speed..."
        })
        
        # Simulate upload progress
        upload_speeds = []
        for i in range(1, 11):
            await asyncio.sleep(1)  # Simulate 1 second intervals
            
            # Get actual speed from speedtest (this is a approximation)
            if i == 10:  # Only run actual test at the end
                upload_result = st.upload() / 1000000
                upload_speeds.append(upload_result)
            else:
                # Simulate intermediate speeds
                simulated_speed = random.uniform(2, 15)
                upload_speeds.append(simulated_speed)
            
            await websocket.send_json({
                "type": "live_update",
                "phase": "upload",
                "speed": upload_speeds[-1],
                "progress": i * 10,
                "message": f"Uploading... {i * 10}%"
            })
        
        await websocket.send_json({
            "type": "upload_result",
            "upload": upload_result
        })
        
        # Complete
        await websocket.send_json({
            "type": "status",
            "message": "Test completed successfully!",
            "status": "completed"
        })
        
    except Exception as e:
        await websocket.send_json({
            "type": "error",
            "message": f"Speed test failed: {str(e)}"
        })
        logging.error(f"Speed test error: {e}")