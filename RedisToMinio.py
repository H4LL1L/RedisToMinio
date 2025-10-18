import asyncio
from datetime import datetime
import os
import cv2
import numpy as np
import redis.asyncio as aioredis #type:ignore 
from minio import Minio #type:ignore 
from tqdm import tqdm
from dotenv import load_dotenv  # type: ignore

# Load environment variables from .env if present
load_dotenv()

# Configuration (from environment)
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_STREAM = os.getenv('REDIS_STREAM', 'skey1')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')  # None if not set
REDIS_USER_NAME = os.getenv('REDIS_USERNAME', 'default')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'secret')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'camera-videos')
VIDEO_OUTPUT_DIR = os.getenv('VIDEO_OUTPUT_DIR', 'output_videos')
IDLE_TIMEOUT = float(os.getenv('IDLE_TIMEOUT', '4.0'))  # seconds to wait without frames

os.makedirs(VIDEO_OUTPUT_DIR, exist_ok=True)

class VideoSegment:
    """
    Manages a single video segment: writing frames, resizing if needed,
    progress bar, and clean close.
    """
    def __init__(self, filepath: str, fps: float, frame_size: tuple[int, int], fourcc: int, total_frames: int | None):
        """
        Initialize a VideoSegment.

        Args:
            filepath: Path to the output video file.
            fps: Frames per second for the video writer.
            frame_size: Tuple of (width, height) for initial frame size.
            fourcc: FourCC code used by OpenCV VideoWriter.
            total_frames: Expected number of frames (for progress bar), or None.
        """
        self.filepath = filepath
        self.fps = fps
        self.fourcc = fourcc
        self.frame_size = frame_size  # (width, height)
        self.writer = cv2.VideoWriter(filepath, fourcc, fps, frame_size)
        if not self.writer.isOpened():
            raise RuntimeError(f"Can't open VideoWriter for {filepath}")
        self.total_frames = total_frames
        self.pbar = None
        if total_frames and total_frames > 0:
            self.pbar = tqdm(total=total_frames, desc=os.path.basename(filepath))

    def write(self, frame: np.ndarray):
        """
        Write a single frame to the video. Automatically resizes the writer
        if the frame dimensions change.

        Args:
            frame: BGR image array to write.
        """
        
        h, w = frame.shape[:2]
        if (w, h) != self.frame_size:
            # recreate CLEANUPr with new size
            self.writer.release()
            self.frame_size = (w, h)
            self.writer = cv2.VideoWriter(self.filepath, self.fourcc, self.fps, self.frame_size)
            if not self.writer.isOpened():
                raise RuntimeError(f"Failed to reopen writer for size {self.frame_size}")
            print(f"[VIDEO RESIZE] New size: {self.frame_size}")
        self.writer.write(frame)
        if self.pbar:
            self.pbar.update(1)

    def close(self):
        """
        Finalize the video segment: adjust progress bar if needed and release resources.
        """
        if self.pbar:
            # adjust total if fewer frames arrived
            if self.pbar.n < self.pbar.total:
                self.pbar.total = self.pbar.n
                self.pbar.refresh()
            self.pbar.close()
        self.writer.release()

class RedisToMinio:
    """
    Service that reads JPEG frames from a Redis stream, assembles them into
    MP4 segments using OpenCV, and uploads completed videos to MinIO.
    """
    def __init__(self):
        """
        Initialize Redis and MinIO clients, and prepare internal state.
        """
        # Redis
        self.redis = aioredis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            username=REDIS_USER_NAME, password=REDIS_PASSWORD,
            decode_responses=False
        )
        print('[REDIS] Connected')
        # MinIO
        self.minio = Minio(
            MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE
        )
        if not self.minio.bucket_exists(MINIO_BUCKET):
            self.minio.make_bucket(MINIO_BUCKET)
        print(f"[MINIO] Bucket '{MINIO_BUCKET}' ready")

        # State
        self.current_segment: VideoSegment | None = None
        self.current_meta: dict = {}
        self.last_frame_id: int | None = None
        self.last_frame_time: float = 0.0
        self.fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        self.last_redis_id = '0-0'

    async def listen(self):
        """
        Continuously read from the Redis stream and handle incoming frames.
        Closes segments if idle timeout is exceeded.
        """
        
        print('[SERVICE] Listening to Redis stream')
        while True:
            entries = await self.redis.xread({REDIS_STREAM: self.last_redis_id}, count=1, block=1000)
            now = asyncio.get_event_loop().time()
            if entries:
                for _, msgs in entries:
                    for msg_id, data in msgs:
                        await self.handle_message(data)
                        self.last_redis_id = msg_id
                self.last_frame_time = now
            else:
                # no new data, check idle timeout
                if self.current_segment and (now - self.last_frame_time) >= IDLE_TIMEOUT:
                    await self._close_segment()
                    self.last_frame_id = None

    async def handle_message(self, data: dict):
        """
        Parse a single Redis message, decode the JPEG frame, and write it.
        Starts a new segment on sequence break.

        Args:
            data: Redis message dictionary with frame metadata and JPEG payload.
        """
        try:
            fid = int(data.get(b'frame_id', b'0').decode())
            cam = int(data.get(b'cam', b'0').decode())
            ts = data.get(b'timestamp', b'').decode()
            fps = float(data.get(b'cam_fps', b'0').decode())
            w = int(data.get(b'width', b'0').decode())
            h = int(data.get(b'height', b'0').decode())
            dur = float(data.get(b'duration', b'0').decode())
            img_bytes = data.get(b'data')
        except Exception as e:
            print(f'[MSG PARSE ERROR] {e}')
            return
        if not img_bytes:
            return
        frame = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
        if frame is None:
            print('[DECODE ERROR]')
            return

        new_seg = self.last_frame_id is None or fid <= self.last_frame_id
        if new_seg:
            await self._close_segment()
            await self._start_segment(cam, ts, fps, w, h, dur)

        self.current_segment.write(frame)
        self.last_frame_id = fid

    async def _start_segment(self, cam, ts, fps, w, h, dur):
        """
        Begin a new VideoSegment with given metadata.

        Args:
            cam: Camera ID.
            ts: ISO timestamp string.
            fps: Frames per second.
            w: Frame width.
            h: Frame height.
            dur: Duration in seconds.
        """
        try:
            dt = datetime.fromisoformat(ts)
            safe = dt.isoformat().replace(':','-')
        except:
            safe = datetime.now().isoformat().replace(':','-')
        name = f'cam{cam}_{safe}.mp4'
        path = os.path.join(VIDEO_OUTPUT_DIR, name)
        total = int(fps * dur) if dur > 0 else None
        self.current_segment = VideoSegment(path, fps, (w, h), self.fourcc, total)
        self.current_meta = {'camera_id':str(cam), 'timestamp':ts, 'duration':str(dur)}
        self.last_frame_time = asyncio.get_event_loop().time()
        print(f'[SEGMENT START] {path}')

    async def _close_segment(self):
        """
        Close and upload the current segment, then clean up local file.
        """
        
        seg = self.current_segment
        if not seg:
            return
        seg.close()
        print(f'[SEGMENT STOP] {seg.filepath}')
        try:
            obj = os.path.basename(seg.filepath)
            self.minio.fput_object(MINIO_BUCKET, obj, seg.filepath, metadata=self.current_meta)
            print(f'[MINIO UPLOAD] {obj}')
        except Exception as e:
            print(f'[MINIO ERROR] {e}')
        try:
            os.remove(seg.filepath)
            print(f'[CLEANUP] Removed {seg.filepath}')
        except OSError:
            pass
        self.current_segment = None
        self.current_meta = {}

async def main():
    service = RedisToMinio()
    try:
        await service.listen()
    except KeyboardInterrupt:
        print('\n[SERVICE] Stopped')

if __name__ == '__main__':
    asyncio.run(main())
    

