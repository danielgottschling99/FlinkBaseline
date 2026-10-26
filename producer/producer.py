#!/usr/bin/env python3

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


SUPPORTED_EXTENSIONS = {".jpg", ".jpeg"}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def env_default(name: str, fallback):
    return os.environ.get(name, fallback)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replay local frame files into Kafka topic 'frames'."
    )
    parser.add_argument("--topic", default="frames", help="Kafka topic to write to")
    parser.add_argument(
        "--frames-dir",
        default="../frames",
        help="Directory containing source image files",
    )
    parser.add_argument(
        "--stream-id",
        default="camera-a",
        help="Stream identifier to place in emitted JSON",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=10,
        help="How many times to replay the frame directory",
    )
    parser.add_argument(
        "--max-frames",
        type=int,
        default=0,
        help="Optionally stop after this many emitted frames (useful for smoke tests)",
    )
    parser.add_argument(
        "--partition-key",
        choices=("stream", "frame"),
        default="stream",
        help="Kafka key strategy: stream preserves per-stream partitioning; frame distributes one stream",
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=0,
        help="Pause between messages in milliseconds",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9094",
        help="Kafka bootstrap server reachable from the host",
    )
    parser.add_argument(
        "--run-id",
        default=env_default("RUN_ID", ""),
        help="Benchmark run identifier to include in messages",
    )
    parser.add_argument(
        "--source-metrics-out",
        default=env_default("SOURCE_METRICS_OUTPUT", "e2e-source-metrics.jsonl"),
        help="JSONL file that receives one source-side benchmark summary",
    )
    return parser.parse_args()


def collect_frame_paths(frames_dir: Path):
    if not frames_dir.exists():
        raise FileNotFoundError(f"frames directory does not exist: {frames_dir}")

    paths = [
        path
        for path in sorted(frames_dir.rglob("*"))
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    if not paths:
        raise FileNotFoundError(f"no frame files found in {frames_dir}")
    return paths


def build_message(path: Path, frame_id: int, stream_id: str, run_id: str):
    raw_bytes = path.read_bytes()
    return raw_bytes


def append_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def main():
    args = parse_args()
    producer_dir = Path(__file__).resolve().parent
    project_root = producer_dir.parent
    frames_dir = (producer_dir / args.frames_dir).resolve()
    frame_paths = collect_frame_paths(frames_dir)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    pending = []

    frame_id = 1
    sent_count = 0
    bytes_sent = 0
    started_at = utc_now_iso()
    first_frame_sent_at = None
    interrupted = False
    try:
        for _ in range(args.repeat):
            for path in frame_paths:
                if args.max_frames > 0 and sent_count >= args.max_frames:
                    break
                message = build_message(path, frame_id, args.stream_id, args.run_id)
                source_sent_at = utc_now_iso()
                headers = [
                    ("frame_id", str(frame_id).encode("utf-8")),
                    ("stream_id", args.stream_id.encode("utf-8")),
                    ("run_id", args.run_id.encode("utf-8")),
                    ("source_sent_at", source_sent_at.encode("utf-8")),
                ]
                partition_key = (
                    args.stream_id if args.partition_key == "stream" else str(frame_id)
                ).encode("utf-8")
                pending.append(producer.send(
                    args.topic,
                    key=partition_key,
                    value=message,
                    headers=headers,
                ))
                if first_frame_sent_at is None:
                    first_frame_sent_at = source_sent_at
                bytes_sent += len(message)
                frame_id += 1
                sent_count += 1

                if args.interval_ms > 0:
                    time.sleep(args.interval_ms / 1000)
                if args.max_frames > 0 and sent_count >= args.max_frames:
                    break
        producer.flush()
        for future in pending:
            future.get()
    except KeyboardInterrupt:
        interrupted = True
    except BrokenPipeError:
        interrupted = True
        raise RuntimeError("kafka-console-producer stdin closed unexpectedly")
    finally:
        producer.flush()
        producer.close()

    finished_at = utc_now_iso()
    duration_ms = max(
        0,
        round(
            (
                datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
                - datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            ).total_seconds()
            * 1000
        ),
    )
    metrics_record = {
        "type": "e2e_source",
        "run_id": args.run_id,
        "started_at": started_at,
        "first_frame_sent_at": first_frame_sent_at,
        "finished_at": finished_at,
        "frames_sent": sent_count,
        "bytes_sent": bytes_sent,
        "duration_ms": duration_ms,
        "interrupted": interrupted,
    }
    append_jsonl((project_root / args.source_metrics_out).resolve(), metrics_record)

    print(f"sent {sent_count} frame messages to topic {args.topic}")
    print(f"source metrics appended to {args.source_metrics_out}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
