import os
import io
import mimetypes
import requests
from huggingface_hub import HfApi, hf_hub_url
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from collections import deque

repo_url = os.environ.get('SOURCE_URL')
token = os.environ.get('HF_TOKEN')
bucket = os.environ.get('CACHE_BUCKET')
prefix = os.environ.get('CACHE_OBJECT_KEY', '').strip('/')
endpoint = os.environ.get('CACHE_ENDPOINT') or None
mem_limit_str = os.environ.get('MEMORY_LIMIT')
mem_limit_bytes = os.environ.get('MEMORY_LIMIT_BYTES')

# Resolve repo_id from full URL if needed
repo_id = repo_url or ''
hf_prefix = 'https://huggingface.co/'
if repo_id.startswith(hf_prefix):
    repo_id = repo_id[len(hf_prefix):]
repo_id = repo_id.strip('/')

print('[kold] starting download script')
print(f"[kold] SOURCE_URL={repo_url}")
print(f"[kold] repo_id={repo_id}")
print(f"[kold] CACHE_BUCKET={bucket}")
print(f"[kold] CACHE_OBJECT_KEY={prefix}")
print(f"[kold] CACHE_ENDPOINT={endpoint}")
print(f"[kold] HF_TOKEN={'set' if bool(token) else 'not set'}")

api = HfApi()
files = api.list_repo_files(repo_id=repo_id, repo_type='model')
print(f"[kold] files to fetch: {len(files)}")
if not files:
    print('[kold] no files returned by HF API, exiting')

session = boto3.session.Session()
client = session.client('s3', endpoint_url=endpoint, config=Config(s3={'addressing_style': 'path'}))
def _safe_int(v, default):
    try:
        return int(v)
    except Exception:
        return default

# Choose chunk size based on available memory and cap by CHUNK_MAX_MIB (default 64Mi)
mem_bytes = _safe_int(mem_limit_bytes, 128*1024*1024)
chunk_cap = _safe_int(os.environ.get('CHUNK_MAX_MIB'), 64) * 1024 * 1024
conc = max(1, _safe_int(os.environ.get('CONCURRENCY'), 1))
per_worker_mem = max(1, mem_bytes // conc)
chunk = max(8*1024*1024, min(chunk_cap, per_worker_mem // 8))
print(f"[kold] memory_limit={mem_limit_str} ({mem_bytes} bytes), chunk_cap={chunk_cap}, chosen multipart_chunksize={chunk}, workers={conc}")
transfer_config = TransferConfig(
    multipart_threshold=chunk,
    multipart_chunksize=chunk,
    max_concurrency=1,
    use_threads=False,
)

headers = {}
if token:
    headers['Authorization'] = f'Bearer {token}'

def _fmt_bytes(n):
    try:
        n = int(n)
    except Exception:
        return f"{n}"
    units = ['B','KiB','MiB','GiB','TiB']
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units)-1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"

class ProgressReader:
    def __init__(self, fobj, idx, total, path, total_bytes=None, interval_sec=5):
        self.f = fobj
        self.idx = idx
        self.total = total
        self.path = path
        self.total_bytes = int(total_bytes) if (total_bytes is not None and str(total_bytes).isdigit()) else None
        self.interval = max(1, int(interval_sec))
        self.start_t = time.time()
        self.last_t = self.start_t
        self.last_b = 0
        self.bytes = 0
        self._stop = False
        self._lock = threading.Lock()
        self.samples = deque(maxlen=120)
        self.samples.append((self.start_t, 0))

        def _reporter():
            while not self._stop:
                time.sleep(self.interval)
                with self._lock:
                    now = time.time()
                    delta_b = self.bytes - self.last_b
                    delta_t = now - self.last_t
                    if delta_t <= 0:
                        continue
                    # append current sample
                    self.samples.append((now, self.bytes))
                    # compute 15s moving average
                    window = 15.0
                    base_t, base_b = self.samples[0]
                    for t, b in self.samples:
                        if now - t <= window:
                            base_t, base_b = t, b
                            break
                    avg_dt = max(1e-6, now - base_t)
                    avg_speed = (self.bytes - base_b) / avg_dt
                    inst_speed = delta_b / delta_t
                    percent = ''
                    if self.total_bytes and self.total_bytes > 0:
                        percent = f" ({(self.bytes/self.total_bytes)*100:.1f}%)"
                    stalled = '' if delta_b > 0 else ' (stalled)'
                    print(f"[kold] [{self.idx}/{self.total}] {self.path}: {_fmt_bytes(self.bytes)}{percent} at {_fmt_bytes(inst_speed)}/s avg {_fmt_bytes(avg_speed)}/s{stalled}")
                    self.last_t = now
                    self.last_b = self.bytes

        self._thread = threading.Thread(target=_reporter, daemon=True)
        self._thread.start()

    def read(self, size=-1):
        chunk = self.f.read(size)
        n = len(chunk) if chunk else 0
        with self._lock:
            self.bytes += n
            if n == 0:
                # EOF: emit final log and stop reporter
                now = time.time()
                delta_b = self.bytes - self.last_b
                delta_t = max(1e-6, now - self.last_t)
                speed = delta_b / delta_t
                total_dt = max(1e-6, now - self.start_t)
                total_speed = self.bytes / total_dt
                percent = ''
                if self.total_bytes and self.total_bytes > 0:
                    percent = f" ({(self.bytes/self.total_bytes)*100:.1f}%)"
                print(f"[kold] [{self.idx}/{self.total}] {self.path}: {_fmt_bytes(self.bytes)}{percent} at {_fmt_bytes(speed)}/s avg {_fmt_bytes(total_speed)}/s (final)")
                self._stop = True
        return chunk

    def __getattr__(self, name):
        return getattr(self.f, name)

def _get_remote_size(url):
    try:
        hr = requests.head(url, headers=headers, allow_redirects=True)
        if hr.ok:
            cl = hr.headers.get('Content-Length')
            return int(cl) if cl and str(cl).isdigit() else None
        # Fallback when HEAD not allowed
        gr = requests.get(url, headers=headers, stream=True)
        try:
            cl = gr.headers.get('Content-Length')
            return int(cl) if cl and str(cl).isdigit() else None
        finally:
            gr.close()
    except Exception:
        return None

def _get_s3_size(bucket, key):
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
        return int(resp.get('ContentLength')) if 'ContentLength' in resp else None
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return None
        print(f"[kold] head_object error for s3://{bucket}/{key}: {e}")
        return None

def _process_one(idx, total, path):
    try:
        url = hf_hub_url(repo_id=repo_id, filename=path)
        key = f"{prefix}/{path}" if prefix else path
        # Skip upload if the same size already exists in S3
        remote_size = _get_remote_size(url)
        existing_size = _get_s3_size(bucket, key)
        if remote_size and existing_size and remote_size == existing_size:
            print(f"[kold] [{idx}/{total}] skip {path}: already exists with same size {_fmt_bytes(existing_size)}")
            return None
        print(f"[kold] [{idx}/{total}] GET {url}")
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            size = r.headers.get('Content-Length')
            ctype = r.headers.get('Content-Type')
            print(f"[kold] [{idx}/{total}] status={r.status_code} content_length={size} content_type={ctype}")
            r.raw.decode_content = True
            content_type = ctype or mimetypes.guess_type(path)[0]
            extra = {'ContentType': content_type} if content_type else None
            print(f"[kold] [{idx}/{total}] uploading to s3://{bucket}/{key} extra={bool(extra)}")
            src = ProgressReader(r.raw, idx, total, path, total_bytes=size, interval_sec=5)
            if extra:
                client.upload_fileobj(src, bucket, key, ExtraArgs=extra, Config=transfer_config)
            else:
                client.upload_fileobj(src, bucket, key, Config=transfer_config)
            print(f"[kold] [{idx}/{total}] uploaded {path} -> s3://{bucket}/{key}")
        return None
    except Exception as e:
        print(f"[kold] [{idx}/{total}] ERROR {path}: {e}")
        return e

errors = []
total = len(files)
with ThreadPoolExecutor(max_workers=conc) as pool:
    futures = [pool.submit(_process_one, i, total, p) for i, p in enumerate(files, start=1)]
    for fut in as_completed(futures):
        err = fut.result()
        if err is not None:
            errors.append(err)
if errors:
    raise Exception(f"[kold] {len(errors)} parallel transfer(s) failed: {errors[:3]}")
print('[kold] download script finished')


