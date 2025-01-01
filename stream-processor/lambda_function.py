import json
import os
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import redis
import logging
import base64
import requests
import shutil
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REDIS_URL = os.environ['REDIS_URL']
BASE_URL = "https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv"

def log_timing(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Completed {func.__name__} in {duration:.2f} seconds")
        return result
    return wrapper

@log_timing
def download_segment(url):
    """Download video segment to temp file"""
    logger.info(f"Downloading segment from URL: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.abbeyroad.com/',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.abbeyroad.com'
    }
    
    response = requests.get(url, headers=headers, timeout=10)
    logger.info(f"Download response status: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"Failed to download segment: {response.status_code}")
    
    temp_path = f"/tmp/segment_{int(datetime.now().timestamp())}.ts"
    content_size = len(response.content)
    logger.info(f"Writing {content_size} bytes to {temp_path}")
    
    with open(temp_path, 'wb') as f:
        f.write(response.content)
    return temp_path

@log_timing
def extract_and_process_frames(video_path, output_dir):
    """Extract and process frames using ffmpeg"""
    logger.info(f"Processing video at {video_path}")
    output_pattern = os.path.join(output_dir, 'frame_%d.jpg')
    
    try:
        # Get video info first using ffprobe
        ffprobe_path = "/opt/bin/ffprobe"
        logger.info(f"Using ffprobe at: {ffprobe_path}")
        probe_cmd = [
            ffprobe_path,
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            video_path
        ]
        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
        if probe_result.returncode != 0:
            raise Exception(f"FFprobe failed: {probe_result.stderr}")
        
        video_info = json.loads(probe_result.stdout)
        video_stream = next(s for s in video_info['streams'] if s['codec_type'] == 'video')
        logger.info(f"Video info: {json.dumps(video_info)}")
        
        width = int(video_stream['width'])
        height = int(video_stream['height'])
        logger.info(f"Video dimensions: {width}x{height}")
        
        crop_size = height // 4
        left = (width - crop_size) // 2
        top = (height - crop_size) // 2
        
        # Extract frames using ffmpeg
        ffmpeg_path = "/opt/bin/ffmpeg"
        logger.info(f"Using ffmpeg at: {ffmpeg_path}")
        ffmpeg_cmd = [
            ffmpeg_path,
            '-i', video_path,
            '-vf', f'select=not(mod(n\\,5)),edgedetect=low=0.1:high=0.4:mode=colormix,crop={crop_size}:{crop_size}:{left}:{top},scale=64:64:flags=neighbor',
            '-vsync', '0',
            '-f', 'image2',
            '-compression_level', '5',
            output_pattern
        ]
        
        ffmpeg_result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
        if ffmpeg_result.returncode != 0:
            raise Exception(f"FFmpeg failed: {ffmpeg_result.stderr}")
        
        # Get frame list
        frames = sorted(
            [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith('frame_')],
            key=lambda x: int(os.path.basename(x).split('_')[1].split('.')[0])
        )
        logger.info(f"Extracted {len(frames)} frames")
        return frames
        
    except Exception as e:
        logger.error(f"Error processing frames: {str(e)}")
        raise

@log_timing
def process_frame(frame_data):
    """Process a single frame"""
    frame_number, frame_path = frame_data
    try:
        with open(frame_path, 'rb') as f:
            frame_bytes = f.read()
            
        return {
            'frame_number': frame_number,
            'data': base64.b64encode(frame_bytes).decode('utf-8')
        }
    except Exception as e:
        logger.error(f"Error processing frame {frame_number}: {str(e)}")
        return None
    finally:
        try:
            os.remove(frame_path)
        except Exception as e:
            logger.error(f"Error removing frame {frame_path}: {str(e)}")

def handler(event, context):
    temp_dirs = []
    temp_files = []
    start_time = time.time()
    
    try:
        # Verify FFmpeg and FFprobe are available
        logger.info("Checking FFmpeg availability")
        ffmpeg_path = "/opt/bin/ffmpeg"
        ffprobe_path = "/opt/bin/ffprobe"
        if not (os.path.exists(ffmpeg_path) and os.path.exists(ffprobe_path)):
            raise Exception(f"FFmpeg/FFprobe not found at {ffmpeg_path} or {ffprobe_path}")
        else:
            logger.info(f"Found FFmpeg at {ffmpeg_path}")
            logger.info(f"Found FFprobe at {ffprobe_path}")
        
        segment_number = event['segment_number']
        logger.info(f"Starting processor lambda for segment {segment_number}")
        
        # Setup Redis client
        logger.info("Connecting to Redis")
        redis_client = redis.from_url(
            REDIS_URL,
            socket_timeout=10,
            socket_connect_timeout=5
        )
        redis_client.ping()  # Test connection
        logger.info("Redis connection successful")
        
        # Create temporary directory
        frames_dir = f"/tmp/frames_{segment_number}_{int(datetime.now().timestamp())}"
        os.makedirs(frames_dir, exist_ok=True)
        temp_dirs.append(frames_dir)
        logger.info(f"Created temp directory: {frames_dir}")
        
        # Download segment
        segment_url = f"{BASE_URL}/media_{segment_number}.ts"
        video_path = download_segment(segment_url)
        temp_files.append(video_path)
        
        # Extract and process frames
        frame_paths = extract_and_process_frames(video_path, frames_dir)
        
        # Process frames in parallel
        logger.info(f"Starting parallel processing of {len(frame_paths)} frames")
        frame_data = [(i, path) for i, path in enumerate(frame_paths)]
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            processed_frames = list(executor.map(process_frame, frame_data))
        
        # Filter and sort frames
        processed_frames = [f for f in processed_frames if f is not None]
        processed_frames.sort(key=lambda x: x['frame_number'])
        logger.info(f"Successfully processed {len(processed_frames)} frames")
        
        if not processed_frames:
            raise Exception("No frames were successfully processed")
        
        # Store in Redis
        logger.info("Storing results in Redis")
        frames_data = json.dumps({
            'timestamp': int(datetime.now().timestamp()),
            'frames': processed_frames
        })
        
        redis_client.setex(
            f'processed_segment:{segment_number}',
            86400,
            frames_data
        )
        
        total_duration = time.time() - start_time
        logger.info(f"Total processing time: {total_duration:.2f} seconds")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed segment',
                'segment_number': segment_number,
                'frame_count': len(processed_frames),
                'processing_time': total_duration
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing segment: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        # Cleanup
        for file_path in temp_files:
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Error removing temp file {file_path}: {str(e)}")
        
        for dir_path in temp_dirs:
            try:
                shutil.rmtree(dir_path)
            except Exception as e:
                logger.error(f"Error removing temp directory {dir_path}: {str(e)}")