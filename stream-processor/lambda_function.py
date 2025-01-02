import json
import os
import subprocess
import boto3
import redis
import logging
import requests
import time
import cv2
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_image_info():
    result = subprocess.run(['cat', '/etc/issue'], capture_output=True, text=True)
    logger.info(f"Running on: {result.stdout}")
    
    container_id = subprocess.run(['cat', '/proc/self/cgroup'], capture_output=True, text=True)
    logger.info(f"Container ID: {container_id.stdout}")

def extract_frames(input_path):
    logger.info(f"Input file size: {os.path.getsize(input_path)} bytes")
    t_start = time.time()
    
    # Check file content
    with open(input_path, 'rb') as f:
        header = f.read(16).hex()
    logger.info(f"File header: {header}")
    
    # Original frame extraction code with more logging
    process = subprocess.Popen([
        '/opt/bin/ffmpeg',
        '-f', 'mpegts',  # Specify input format
        '-i', input_path,
        '-vcodec', 'copy',  # Copy video stream without re-encoding
        '-f', 'image2pipe',
        '-pix_fmt', 'bgr24',
        '-v', 'debug',
        '-'
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    frames = []
    frame_size = 1920 * 1080 * 3
    frame_count = 0
    
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
        frame_count += 1
    
    process.stdout.close()
    process.wait()
    
    logger.info(f"Extracted {frame_count} frames in {time.time() - t_start:.2f}s")
    return frames

def write_frames(frames, output_path):
    logger.info("Starting frame writing")
    t_start = time.time()
    
    p = subprocess.Popen([
        '/opt/bin/ffmpeg',
        '-f', 'rawvideo',
        '-vcodec', 'rawvideo',
        '-s', '1920x1080',
        '-pix_fmt', 'bgr24',
        '-r', '30',
        '-i', '-',
        '-c:v', 'libx264',
        '-f', 'mpegts',
        '-b:v', '5000k',
        output_path
    ], stdin=subprocess.PIPE)
    
    for frame in frames:
        p.stdin.write(frame.tobytes())
    
    p.stdin.close()
    p.wait()
    
    logger.info(f"Wrote {len(frames)} frames in {time.time() - t_start:.2f}s")

def handler(event, context):
    logger.info("Lambda version: " + os.environ.get('AWS_LAMBDA_FUNCTION_VERSION', 'unknown'))
    logger.info("Container ID: " + subprocess.check_output(['cat', '/proc/1/cpuset']).decode())
    log_image_info()
    
    try:
        t_total_start = time.time()
        segment_number = event['segment_number']
        redis_client = redis.from_url(os.environ['REDIS_URL'])
        s3_client = boto3.client('s3')
        
        # Download segment
        t_start = time.time()
        segment_url = f"https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv/media_{segment_number}.ts"
        response = requests.get(segment_url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.abbeyroad.com/'})
        response.raise_for_status()
        
        input_path = f"/tmp/segment_{int(time.time())}.ts"
        output_path = f"/tmp/processed_{int(time.time())}.ts"
        
        with open(input_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Download took {time.time() - t_start:.2f}s")
        
        # Process frames
        frames = extract_frames(input_path)
        write_frames(frames, output_path)
        
        # Upload to S3
        t_start = time.time()
        s3_key = f"processed_segments/{int(time.time())}_{segment_number}.ts"
        s3_client.upload_file(output_path, os.environ['S3_BUCKET'], s3_key)
        logger.info(f"S3 upload took {time.time() - t_start:.2f}s")
        
        redis_client.setex(
            f'processed_segment:{segment_number}',
            86400,
            json.dumps({'s3_path': s3_key})
        )
        
        logger.info(f"Total processing time: {time.time() - t_total_start:.2f}s")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'segment_number': segment_number,
                's3_path': s3_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing segment: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
        
    finally:
        for path in [input_path, output_path]:
            if path and os.path.exists(path):
                os.remove(path)