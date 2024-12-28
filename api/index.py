# api/index.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import numpy as np
import binascii
import threading
import time
from typing import Optional
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FFmpegStreamReader:
    def __init__(self, url: str, width: int = 64, height: int = 64, fps: int = 24):
        self.url = url
        self.width = width
        self.height = height
        self.fps = fps
        self.frame_interval = 1.0 / fps
        self.process: Optional[subprocess.Popen] = None
        self.current_frame = None
        self.last_frame_time = 0
        self.running = False
        self.lock = threading.Lock()
        
    def start(self):
        with self.lock:
            if self.running:
                return
            
            self.running = True
            try:
                command = [
                    'ffmpeg',
                    '-y',
                    '-threads', '3',
                    '-reconnect', '1',
                    '-reconnect_at_eof', '1',
                    '-reconnect_streamed', '1',
                    '-reconnect_delay_max', '2',
                    '-i', self.url,
                    '-map', '0:3',  # Select the lower quality video stream
                    '-f', 'rawvideo',
                    '-pix_fmt', 'rgb24',
                    '-vsync', '1',  # Force frame sync
                    '-vf', f'fps={self.fps},scale={self.width}:{self.height}',
                    '-an',
                    '-sn',
                    '-'
                ]
                
                logger.info(f"Starting FFmpeg with command: {' '.join(command)}")
                
                self.process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    bufsize=32768  # 32KB buffer
                )
                
                # Start error logging thread
                def log_stderr():
                    while self.running and self.process:
                        line = self.process.stderr.readline()
                        if line:
                            logger.info(f"FFmpeg: {line.decode().strip()}")
                
                self.error_thread = threading.Thread(target=log_stderr)
                self.error_thread.daemon = True
                self.error_thread.start()
                
                # Start frame reading thread
                self.thread = threading.Thread(target=self._read_frames)
                self.thread.daemon = True
                self.thread.start()
                
            except Exception as e:
                logger.error(f"Failed to start FFmpeg: {e}")
                self.running = False
                raise
    
    def _read_frames(self):
        frame_size = self.width * self.height * 3
        buffer = bytearray()
        
        while self.running and self.process:
            try:
                current_time = time.time()
                if current_time - self.last_frame_time >= self.frame_interval:
                    # Read data into buffer
                    while len(buffer) < frame_size:
                        chunk = self.process.stdout.read1(4096)  # Use read1 for better buffering
                        if not chunk:
                            logger.error("End of stream")
                            self.restart()
                            break
                        buffer.extend(chunk)
                    
                    # Process complete frame
                    if len(buffer) >= frame_size:
                        frame_data = buffer[:frame_size]
                        buffer = buffer[frame_size:]  # Keep remaining data
                        
                        # Convert to numpy array
                        frame = np.frombuffer(frame_data, dtype=np.uint8)
                        frame = frame.reshape((self.height, self.width, 3))
                        
                        # RGB565 conversion
                        r = (frame[:,:,0] & 0xF8).astype(np.uint16) << 8
                        g = (frame[:,:,1] & 0xFC).astype(np.uint16) << 3
                        b = (frame[:,:,2] >> 3).astype(np.uint16)
                        rgb565 = r | g | b
                        
                        self.current_frame = rgb565.tobytes()
                        self.last_frame_time = current_time
                    
                    time.sleep(0.001)  # Small sleep to prevent CPU spinning
                    
            except Exception as e:
                logger.error(f"Error reading frame: {e}")
                self.restart()
                time.sleep(1)
                
    def restart(self):
        logger.info("Restarting FFmpeg process")
        with self.lock:
            self.stop()
            time.sleep(1)
            self.start()
    
    def stop(self):
        self.running = False
        if self.process:
            try:
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.process.kill()
            except Exception as e:
                logger.error(f"Error stopping FFmpeg: {e}")
            self.process = None
            self.current_frame = None
    
    def get_current_frame(self) -> Optional[bytes]:
        return self.current_frame

# Initialize stream reader with 24fps (matching source)
STREAM_URL = "https://demo.unified-streaming.com/k8s/features/stable/video/tears-of-steel/tears-of-steel.ism/.m3u8"
stream_reader = FFmpegStreamReader(STREAM_URL, fps=24)

@app.on_event("startup")
async def startup_event():
    try:
        stream_reader.start()
        logger.info("Stream reader started successfully")
    except Exception as e:
        logger.error(f"Failed to start stream reader: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    stream_reader.stop()

@app.get("/frame")
async def get_frame():
    if not stream_reader.running:
        logger.warning("Stream reader not running, attempting restart")
        stream_reader.restart()
        time.sleep(0.5)  # Give it a moment to start
        
    frame_data = stream_reader.get_current_frame()
    if frame_data is None:
        logger.error("No frame available")
        raise HTTPException(status_code=503, detail="No frame available")
    
    frame_hex = binascii.hexlify(frame_data).decode('ascii')
    return {"frame": frame_hex}

@app.get("/health")
async def health_check():
    status = {
        "running": stream_reader.running,
        "has_frame": stream_reader.current_frame is not None,
        "last_frame_time": stream_reader.last_frame_time,
        "current_time": time.time()
    }
    
    if not stream_reader.running or stream_reader.process is None:
        raise HTTPException(status_code=503, detail=status)
    return {"status": "healthy", **status}

@app.post("/restart")
async def restart_stream():
    stream_reader.restart()
    return {"status": "restarted"}