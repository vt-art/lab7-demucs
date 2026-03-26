import os
import io
import json
import base64
import hashlib

import redis
import requests
from flask import Flask, request, jsonify, send_file
import jsonpickle
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "rootuser")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "rootpass123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

INPUT_BUCKET = os.getenv("INPUT_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "output")
WORK_QUEUE = os.getenv("WORK_QUEUE", "toWorker")
LOG_QUEUE = os.getenv("LOG_QUEUE", "logging")

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=MINIO_SECURE,
)


def log_message(msg: str) -> None:
    try:
        redis_client.lpush(LOG_QUEUE, msg)
    except Exception:
        pass


def ensure_bucket(bucket_name: str) -> None:
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except Exception as exc:
        log_message(f"Failed ensuring bucket {bucket_name}: {exc}")


def compute_songhash(mp3_bytes: bytes) -> str:
    return hashlib.sha224(mp3_bytes).hexdigest()


@app.route("/", methods=["GET"])
def hello():
    return "<h1>Music Separation Server</h1><p>Use a valid endpoint</p>", 200


@app.route("/apiv1/separate", methods=["POST"])
def separate():
    try:
        raw = request.get_data()
        payload = jsonpickle.decode(raw)

        if not payload or "mp3" not in payload:
            return jsonify({"error": "Missing mp3 field"}), 400

        mp3_b64 = payload["mp3"]
        callback = payload.get("callback")

        mp3_bytes = base64.b64decode(mp3_b64)
        songhash = compute_songhash(mp3_bytes)
        object_name = f"{songhash}.mp3"

        ensure_bucket(INPUT_BUCKET)
        ensure_bucket(OUTPUT_BUCKET)

        mp3_stream = io.BytesIO(mp3_bytes)
        minio_client.put_object(
            INPUT_BUCKET,
            object_name,
            mp3_stream,
            length=len(mp3_bytes),
            content_type="audio/mpeg",
        )

        work_item = {
            "songhash": songhash,
            "input_bucket": INPUT_BUCKET,
            "input_object": object_name,
            "output_bucket": OUTPUT_BUCKET,
            "callback": callback,
        }

        redis_client.lpush(WORK_QUEUE, json.dumps(work_item))
        log_message(f"Queued song {songhash}")

        return jsonify({
            "hash": songhash,
            "reason": "Song enqueued for separation"
        })

    except Exception as exc:
        log_message(f"Error in /apiv1/separate: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/apiv1/queue", methods=["GET"])
def queue_dump():
    try:
        queue_items = redis_client.lrange(WORK_QUEUE, 0, -1)
        decoded = [item.decode("utf-8") for item in queue_items]
        return jsonify({"queue": decoded})
    except Exception as exc:
        log_message(f"Error in /apiv1/queue: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def get_track(songhash, track):
    allowed = {"bass.mp3", "drums.mp3", "vocals.mp3", "other.mp3"}
    if track not in allowed:
        return jsonify({"error": "Invalid track name"}), 400

    object_name = f"{songhash}-{track}"

    try:
        response = minio_client.get_object(OUTPUT_BUCKET, object_name)
        data = response.read()
        return send_file(
            io.BytesIO(data),
            mimetype="audio/mpeg",
            as_attachment=True,
            download_name=track,
        )
    except S3Error as exc:
        log_message(f"Track not found {object_name}: {exc}")
        return jsonify({"error": f"Track not found: {object_name}"}), 404
    except Exception as exc:
        log_message(f"Error in /apiv1/track: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET"])
def remove_track(songhash, track):
    allowed = {"bass.mp3", "drums.mp3", "vocals.mp3", "other.mp3"}
    if track not in allowed:
        return jsonify({"error": "Invalid track name"}), 400

    object_name = f"{songhash}-{track}"

    try:
        minio_client.remove_object(OUTPUT_BUCKET, object_name)
        log_message(f"Removed track {object_name}")
        return jsonify({"removed": object_name})
    except Exception as exc:
        log_message(f"Error in /apiv1/remove: {exc}")
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)