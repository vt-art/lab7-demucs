import json
import os
import subprocess
import tempfile

import redis
import requests
from minio import Minio

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "rootuser")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "rootpass123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

INPUT_BUCKET = os.getenv("INPUT_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "output")

WORK_QUEUE_KEY = os.getenv("WORK_QUEUE_KEY", "toWorker")
LOG_QUEUE = os.getenv("LOG_QUEUE", "logging")

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

minio_client = Minio(
MINIO_HOST,
access_key=MINIO_ROOT_USER,
secret_key=MINIO_ROOT_PASSWORD,
secure=MINIO_SECURE,
)

def log(msg):
    try:
        redis_client.lpush(LOG_QUEUE, msg)
    except Exception:
        pass

def send_callback(callback, payload):
    if not callback:
        return

    try:
        url = callback.get("url")
        extra = callback.get("data", {})
        merged = dict(extra)
        merged.update(payload)
        requests.post(url, json=merged, timeout=10)
    except Exception as e:
        log(f"callback failed: {str(e)}")

def run_demucs(songhash, input_path, output_dir):
    cmd = [
            "python3",
            "-m",
            "demucs.separate",
            "--mp3",
            "--out",
            output_dir, 
            input_path,
            ]

    log(f"Running DEMUCS: {songhash}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log(result.stderr)
        raise RuntimeError("DEMUCS failed")


def upload_tracks(songhash, output_dir):
    model_dir = os.path.join(output_dir, "mdx_extra_q", songhash)

    tracks = ["bass", "drums", "vocals", "other"]

    uploaded = []

    for track in tracks:
        filename = f"{track}.mp3"

        local_path = os.path.join(model_dir, filename)

        object_name = f"{songhash}-{filename}"

        minio_client.fput_object(
            OUTPUT_BUCKET,
            object_name,
            local_path,
        )

        uploaded.append(object_name)

        log(f"Uploaded {object_name}")

    return uploaded


def process_job(job_json):
    if isinstance(job_json, bytes):
        job_json = job_json.decode("utf-8")

    job = json.loads(job_json)

    songhash = job["songhash"]
    input_bucket = job.get("input_bucket", INPUT_BUCKET)
    input_object = job.get("input_object", f"{songhash}.mp3")
    callback = job.get("callback")

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, f"{songhash}.mp3")
        output_dir = os.path.join(tmpdir, "output")

        log(f"Downloading {input_bucket}/{input_object}")
        minio_client.fget_object(input_bucket, input_object, input_path)

        run_demucs(songhash, input_path, output_dir)
        uploaded = upload_tracks(songhash, output_dir)

        send_callback(
            callback,
            {
                "songhash": songhash,
                "status": "complete",
                "files": uploaded,
            },
        )


def main():
    log("Worker started")

    while True:
        _, payload = redis_client.blpop(WORK_QUEUE_KEY)
        log("Received job")

        try:
            process_job(payload)
        except Exception as e:
            log(f"Worker error: {str(e)}")


if __name__ == "__main__":
    main()

