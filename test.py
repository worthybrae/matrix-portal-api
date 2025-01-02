import m3u8
import requests
import time
import os
import cv2
import numpy as np
import subprocess

def get_latest_segment():
    timestamp = int(time.time())
    url = f"https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv/chunklist_w{timestamp}.m3u8"
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.abbeyroad.com/'})
    segment_num = m3u8.loads(response.text).segments[0].uri.split('_')[-1].split('.')[0]
    return f"https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv/media_{segment_num}.ts", segment_num

def extract_frames(input_path):
    # Extract frames directly to memory
    process = subprocess.Popen([
        'ffmpeg',
        '-i', input_path,
        '-f', 'image2pipe',
        '-pix_fmt', 'bgr24',
        '-vcodec', 'rawvideo',
        '-v', 'quiet',
        '-'
    ], stdout=subprocess.PIPE)
    
    frames = []
    frame_size = 1920 * 1080 * 3
    
    while True:
        raw_frame = process.stdout.read(frame_size)
        if not raw_frame:
            break
        if len(raw_frame) != frame_size:
            continue
        
        frame = np.frombuffer(raw_frame, dtype=np.uint8).reshape((1080, 1920, 3))
        edges = cv2.Canny(frame, 100, 200)
        edges = cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR)
        frames.append(edges)
        
    process.stdout.close()
    process.wait()
    return frames

def write_frames(frames, output_path):
    # Write frames back to TS
    p = subprocess.Popen([
        'ffmpeg',
        '-f', 'rawvideo',
        '-vcodec', 'rawvideo',
        '-s', '1920x1080',
        '-pix_fmt', 'bgr24',
        '-r', '30',
        '-i', '-',
        '-c:v', 'h264_videotoolbox',
        '-f', 'mpegts',
        '-b:v', '5000k',
        output_path
    ], stdin=subprocess.PIPE)
    
    for frame in frames:
        p.stdin.write(frame.tobytes())
    
    p.stdin.close()
    p.wait()

def main():
    segment_url, segment_num = get_latest_segment()
    print(f"Processing segment {segment_num}")
    
    input_path = f"temp_segment_{int(time.time())}.ts"
    output_path = f"processed_segment_{segment_num}.ts"
    
    try:
        t_start = time.time()
        response = requests.get(segment_url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.abbeyroad.com/'})
        with open(input_path, 'wb') as f:
            f.write(response.content)
        print(f"Download: {time.time() - t_start:.2f}s")

        t_start = time.time()
        frames = extract_frames(input_path)
        write_frames(frames, output_path)
        print(f"Process: {time.time() - t_start:.2f}s")
        
    finally:
        if os.path.exists(input_path):
            os.remove(input_path)

if __name__ == "__main__":
    main()