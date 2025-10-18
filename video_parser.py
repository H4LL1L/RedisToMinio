import argparse
import cv2
import numpy as np
import redis.asyncio as aioredis # type: ignore problem çıkarıyodu böyle yaptım quick solve 
import time
import asyncio
from queue import Queue
from minio import Minio # type: ignore
from minio.error import S3Error # type: ignore
from datetime import datetime,timezone
from io import BytesIO
import os
from dotenv import load_dotenv  # type: ignore

# Load environment variables from .env if present
load_dotenv()


"""
VideoParser Module

This module provides the "VideoParser" class to asynchronously
read frames from a camera for a specified period of time, process them 
and "RedisPublisher" class send them to the Redis stream.
"""

class VideoParser:
    """
    
   class of VideoParser take frames from cameras and process them after that
   send to redis.
   
   Attributes:
        duration(int): Video capturing time, in second
        height(int): for resizeing height but if you want, if its none default= original
        width(int): for resizeing width but if you want, if its none default= original
        compression(int): Jpeg compression quality 0-100, if its none default= 100
        redis(aioredis.Redis): Async Redis 
        queue(asyncio.Queue): frames putting queue with videocap after redis_send will send to redis
        
        
    Args: 
        duration(int): Video capturing time, in second
        height(int): for resizeing height but if you want, if its none default= original
        width(int): for resizeing width but if you want, if its none default= original
        compression(int): Jpeg compression quality 0-100, if its none default= 100
        redis(aioredis.Redis): Async Redis 
        queue(asyncio.Queue): frames putting queue with videocap after redis_send will send to redis
        

    """
    
    
    def __init__(self,duration:int,
                 height:int | None,
                 width:int | None, 
                 compression:int,
                 camera_index:int):
        
        
        
        self.duration = duration
        self.height= height
        self.width= width
        self.compression= compression 
        self.camera_index = camera_index
        self.timestamp=None
        self.queue: asyncio.Queue =  asyncio.Queue()
        



    async def video_cap(self):
        
        """
            Get frames from camera with async then process frames after that frames send to
            redis_send couritine. When loop is over, it put None in queue and program can understand
            its finished.
        """
        try:
            cap = cv2.VideoCapture(self.camera_index)
        except Exception as e: 
               print(f"Unexpected error in video_cap: {e}")
        
        cam_fps = cap.get(cv2.CAP_PROP_FPS)       
        loop = asyncio.get_running_loop()    
        frame_id =0
        failure_count = 0
        MAX_FAILURES = 15
        start = time.perf_counter()
    
       
        while time.perf_counter()-start < self.duration:    
            
            try:
                success, frame =  await loop.run_in_executor(None, cap.read)  
            except Exception as e:
                failure_count +=1
                print(f"Error reading frame: {e} (attempt {failure_count}/{MAX_FAILURES})")
                if failure_count >= MAX_FAILURES:
                    print("Too many failed attemps, loop terminates.")
                    break
                await asyncio.sleep(0) 
                continue
            if not success:
                failure_count += 1
                print(f"Warning: failed to read frame from camera (attempt {failure_count}/{MAX_FAILURES})")
                if failure_count >= MAX_FAILURES:
                    print("Too many failed attemps, loop terminates.")
                    break
                await asyncio.sleep(0)
                continue

            failure_count = 0     # The reason for this line is to terminate the loop when it receives errors "one after another".
            
            compression = self.compression
            orig_height, orig_width = frame.shape[:2]
            target_width=orig_width
            target_height=orig_height
            duration = self.duration
            
            if self.width is not None or self.height is not None:
                    target_width = self.width  if self.width  is not None else orig_width
                    target_height = self.height if self.height is not None else orig_height
                    frame = cv2.resize(frame, (target_width, target_height))


            ok, enc = cv2.imencode(
                '.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, compression])
                        
            if not ok:
                frame_id +=1
                continue
                
            data = enc.tobytes()
            timestamp = datetime.now(timezone.utc).isoformat()
            await self.queue.put((frame_id,data,self.camera_index,timestamp,target_height,target_width,cam_fps,compression,duration))
                    
            frame_id +=1
                    
        cap.release()

        await self.queue.put(None)
        elapsed_time = time.perf_counter() - start
        print(f"Finished capture: {frame_id} frames in {elapsed_time:.2f}s.")    
            
            

class RedisPublisher:
    
    """
    Connects to Redis asynchronously and publishes frames from a shared queue
    into a Redis stream using XADD in batches. Also includes a small helper
    to read from the same stream.
    
    Attributes:
        queue (asyncio.Queue): The same queue that VideoParser pushes into.
        redis (aioredis.Redis | None): The Redis client once initialized.
        host, port, username, password (str): Redis connection parameters.
        stream_key (str): Redis stream name (default 'skey1').
        batch_size (int): How many XADD commands to accumulate before sending pipeline.execute().
    """
    
    
    def __init__(
        self,
        shared_queue: asyncio.Queue,
    ):
        self.queue = shared_queue
        self.redis: aioredis.Redis | None=None


    async def init_redis(self):   
        """
        start to async redis connection, assigns client object to self.redis
        host, port, username, password is fixed
        """ 
        
        host = os.getenv('REDIS_HOST', 'localhost')
        port = int(os.getenv('REDIS_PORT', '6379'))
        username = os.getenv('REDIS_USERNAME') or None
        password = os.getenv('REDIS_PASSWORD') or None
        
        self.redis = aioredis.Redis(
            host=host,
            port=port,
            decode_responses=False,
            username=username,
            password=password,
            socket_connect_timeout=20
        )


    async def redis_send(self):
        
        """
            writes the frames coming from asyncio.Queue to the redis stream asynchronously with xadd
            When it accumulates to a certain batch_size, it sends it with pipeline.execute().
            When it reaches the end of the queue, it sends the remaining ones
        """
        
        try:
            if self.redis is None:
                await self.init_redis()
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            return
        
        pipeline = self.redis.pipeline(transaction=False)
        batch_size:int = int(os.getenv('REDIS_BATCH_SIZE', '25'))
        count:int=0
        start = time.perf_counter()
        
        
        try:
            while True:
                try:
                    item = await self.queue.get()
                except Exception as e:
                    print(f"Error while getting from queue: {e}")
                    break

                if item is None:
                    break

                frame_id, data, cam_index, timestamp, target_width, target_height, cam_fps, compression, duration = item
                try:
                    pipeline.xadd(
                        os.getenv('REDIS_STREAM', 'skey1'),
                        {
                            b'frame_id': str(frame_id).encode(),
                            b'data': data,
                            b'cam': str(cam_index).encode(),
                            b'timestamp': timestamp.encode(),
                            b'height': str(target_height).encode(),
                            b'width': str(target_width).encode(),
                            b'cam_fps': str(cam_fps).encode(),
                            b'compression': str(compression).encode(),
                            b'duration': str(duration).encode()
                            },
                    )
                except Exception as e:
                    print(f"Error during pipeline.xadd: {e}")
                    continue

                count += 1

                if count % batch_size == 0:
                    try:
                        await pipeline.execute()
                    except Exception as e:
                        print(f"Error during pipeline.execute: {e}")
                        try:
                            await self.init_redis()
                            pipeline = self.redis.pipeline(transaction=False)
                        except Exception as e2:
                            print(f"Error reinitializing Redis: {e2}")
                            break
                await asyncio.sleep(0)

            if pipeline.command_stack:
                try:
                    await pipeline.execute()
                except Exception as e:
                    print(f"Error during final pipeline.execute: {e}")

        except Exception as e:
            print(f"Unexpected error in redis_send: {e}")

        finally:
            if self.redis is not None:
                try:
                    await self.redis.aclose()
                except Exception as e:
                    print(f"Error closing Redis connection: {e}")

        end = time.perf_counter()
        print(f"Redise {count} kare gönderdik")
        print(f"{end-start:.2f}saniyede redise gönderdik")

        
            
        


        
        
        
def parse_args():
    
    parser = argparse.ArgumentParser (description= "parse of Video with frames")
    
    parser.add_argument(
        "-ci", "--camera-index", dest="camera_index", type=int, default=0,
        help="Which camera do you want to use? (default=1)"
    )   
    
    parser.add_argument(
        "-d", "--duration", dest="duration", type=int,
        required=True, help="You need to declare -d --duration of video in seconds "
    )
    
    parser.add_argument(
        "-he", "--height", dest="height", type=int, default=None,
        required=False, help=" declare desired -h --height (pixels) of video if you dont declare, it will be using defaul original "
    )
    
    parser.add_argument(
        "-w", "--width", dest="width", type=int, default=None,
        required=False, help=" declare desired width -w --width (pixels) of video, if you dont declare, it will be using defaul original "
    )
    
    parser.add_argument(
        "-c", "--compression", dest="compression", type=int, default=1,  # for less size i write 1 
        required=False, help="JPEG quality 0-100, default = 100"
    )
    
     
        
    return parser.parse_args()






async def main():
    
    """

    The entry point of the program is an asynchronous function.
    It takes arguments, creates a VideoParser instance, and runs VideoCap and redis_send
    coroutines simultaneously. It also calculates the total execution time.

    """
    
    time_1 = time.perf_counter()
    args = parse_args()
    
    parser = VideoParser(
        duration= args.duration,
        height= args.height,
        width= args.width,
        compression= args.compression,
        camera_index=args.camera_index
        )
    
    publisher = RedisPublisher(shared_queue=parser.queue)

    await asyncio.gather(parser.video_cap(),publisher.redis_send())
    time_2 = time.perf_counter()
    print(f"tüm zaman {time_2-time_1:.2f}")



if __name__ == "__main__": 
    
    asyncio.run(main())
