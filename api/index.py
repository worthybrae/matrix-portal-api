from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import cv2
import binascii
import numpy as np

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STREAM_URL = "https://demo.unified-streaming.com/k8s/features/stable/video/tears-of-steel/tears-of-steel.ism/.m3u8"
cap = cv2.VideoCapture(STREAM_URL)

if not cap.isOpened():
    raise Exception("Failed to open video stream")

@app.get("/frame")
async def get_frame():
    ret, frame = cap.read()
    if not ret:
        raise HTTPException(status_code=500, detail="Failed to read frame")
    
    frame = cv2.resize(frame, (64, 64))
    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    
    frame = frame.astype(np.uint16)
    rgb565 = ((frame[:,:,0] & 0xF8) << 8) | ((frame[:,:,1] & 0xFC) << 3) | (frame[:,:,2] >> 3)
    
    frame_bytes = rgb565.tobytes()
    frame_hex = binascii.hexlify(frame_bytes).decode('ascii')
    
    return {"frame": frame_hex}