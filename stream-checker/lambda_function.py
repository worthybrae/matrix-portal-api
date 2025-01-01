# stream-checker/lambda_function.py
import m3u8
import redis
import json
import os
import time
import requests
import logging
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

REDIS_URL = os.environ['REDIS_URL']
PROCESSOR_LAMBDA_ARN = os.environ['PROCESSOR_LAMBDA_ARN']
HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'https://www.abbeyroad.com',
    'Referer': 'https://www.abbeyroad.com/',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}
    
def handler(event, context):
    redis_client = redis.from_url(REDIS_URL)
    lambda_client = boto3.client('lambda')
    redis_client.ping()
    
    timestamp = int(time.time())
    m3u8_response = requests.get(
        f"https://videos-3.earthcam.com/fecnetwork/AbbeyRoadHD1.flv/chunklist_w{timestamp}.m3u8",
        headers=HEADERS
    )
    
    playlist = m3u8.loads(m3u8_response.text)

    if not playlist.segments:
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No segments found in playlist'})
        }

    new_segments = [x.uri.split('_')[-1].split('.')[0] for x in playlist.segments]
    logger.info(new_segments)
    existing_segments = [x.decode('utf-8') for x in redis_client.lrange('recent_segments', 0, -1)]
    segments_to_add = [seg for seg in new_segments if seg not in existing_segments]
    
    for segment in segments_to_add:
        redis_client.lpush('recent_segments', segment)
    
    redis_client.ltrim('recent_segments', 0, 9)

    for segment in segments_to_add:
        logger.info(f'Invoking processor lambda function for segment {segment}')
        lambda_client.invoke(
            FunctionName=PROCESSOR_LAMBDA_ARN,
            InvocationType='Event',
            Payload=json.dumps({'segment_number': segment})
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Successfully checked segments',
            'new_segments_added': len(segments_to_add),
            'total_segments': len(new_segments)
        })
    }