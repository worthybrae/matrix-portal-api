# test.py
import ffmpeg
import m3u8
import requests
import time
import os
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import shutil

def download_segment(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download segment: {response.status_code}")
    
    temp_path = f"temp_{int(time.time())}.ts"
    with open(temp_path, 'wb') as f:
        f.write(response.content)
    return temp_path

def process_frame(frame_path):
    """Process a single frame from disk"""
    try:
        with Image.open(frame_path) as img:
            # Get smaller center crop
            width, height = img.size
            crop_size = height // 4
            left = (width - crop_size) // 2
            top = (height - crop_size) // 2
            img_square = img.crop((left, top, left + crop_size, top + crop_size))
            
            # Resize to 64x64
            resized = img_square.resize((64, 64), Image.NEAREST)
            
            buffer = BytesIO()
            resized.save(buffer, format='JPEG', quality=85, optimize=True)
            return buffer.getvalue()
    finally:
        try:
            os.remove(frame_path)
        except Exception:
            pass

def extract_frames(video_path, output_dir):
    """Extract frames using ffmpeg"""
    output_pattern = os.path.join(output_dir, 'frame_%d.jpg')
    
    try:
        # Extract every 5th frame
        ffmpeg.input(video_path).output(
            output_pattern,
            vf='select=not(mod(n\,5))',  # Extract every 5th frame
            vsync='0',
            format='image2'
        ).overwrite_output().run(capture_stdout=True, capture_stderr=True)
        
        # Get list of generated frames
        frames = sorted(
            [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith('frame_')],
            key=lambda x: int(os.path.basename(x).split('_')[1].split('.')[0])
        )
        return frames
    except ffmpeg.Error as e:
        print(f"FFmpeg error: {e.stderr.decode()}")
        raise

def process_frames(url):
    """Process frames from a video segment"""
    # Create temporary directory for frames
    output_dir = f"frames_{int(time.time())}"
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Download segment
        video_path = download_segment(url)
        
        # Extract frames
        frame_paths = extract_frames(video_path, output_dir)
        
        # Process frames in parallel
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            frames = list(executor.map(process_frame, frame_paths))
        
        return frames
    finally:
        # Cleanup
        try:
            if 'video_path' in locals():
                os.remove(video_path)
        except Exception:
            pass
        try:
            shutil.rmtree(output_dir)
        except Exception:
            pass

def main():
    timestamp = int(time.time())
    playlist_url = f"https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv/chunklist_w{timestamp}.m3u8"
    response = requests.get(playlist_url, headers={
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.abbeyroad.com/'
    })
    playlist = m3u8.loads(response.text)
    segment_url = f"https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv/{playlist.segments[0].uri}"
    print(f"Processing segment: {segment_url}")
    
    start_time = time.perf_counter()
    frames = process_frames(segment_url)
    duration = time.perf_counter() - start_time
    
    print(f"\nProcessed {len(frames)} frames in {duration:.2f}s")
    print(f"FPS: {len(frames)/duration:.2f}")
    print(f"Frame size: {len(frames[0])} bytes")

if __name__ == "__main__":
    main()