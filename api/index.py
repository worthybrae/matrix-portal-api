# api/index.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import numpy as np
import binascii
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

class ServerlessFFmpeg:
    def __init__(self, url: str, width: int = 64, height: int = 64):
        self.url = url
        self.width = width
        self.height = height
    
    def get_frame(self) -> Optional[bytes]:
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
                '-map', '0:3',
                '-f', 'rawvideo',
                '-pix_fmt', 'rgb24',
                '-vframes', '1',  # Only get one frame
                '-vsync', '0',
                '-vf', f'scale={self.width}:{self.height}',
                '-an',
                '-sn',
                '-'
            ]
            
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=32768
            )
            
            frame_size = self.width * self.height * 3
            frame_data = process.stdout.read(frame_size)
            process.terminate()
            
            if len(frame_data) != frame_size:
                logger.error(f"Incomplete frame data: {len(frame_data)} != {frame_size}")
                return None
                
            # Convert to numpy array
            frame = np.frombuffer(frame_data, dtype=np.uint8)
            frame = frame.reshape((self.height, self.width, 3))
            
            # RGB565 conversion
            r = (frame[:,:,0] & 0xF8).astype(np.uint16) << 8
            g = (frame[:,:,1] & 0xFC).astype(np.uint16) << 3
            b = (frame[:,:,2] >> 3).astype(np.uint16)
            rgb565 = r | g | b
            
            return rgb565.tobytes()
            
        except Exception as e:
            logger.error(f"Error getting frame: {e}")
            return None

STREAM_URL = "https://demo.unified-streaming.com/k8s/features/stable/video/tears-of-steel/tears-of-steel.ism/.m3u8"
ffmpeg = ServerlessFFmpeg(STREAM_URL)

@app.get("/frame")
async def get_frame():
    frame_data = ffmpeg.get_frame()
    if frame_data is None:
        raise HTTPException(status_code=503, detail="Failed to get frame")
    
    frame_hex = binascii.hexlify(frame_data).decode('ascii')
    return {"frame": frame_hex}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}