#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def env_default(name: str, fallback):
    return os.environ.get(name, fallback)


def parse_args():
    parser = argparse.ArgumentParser(description="Validate binary JPEG Kafka results and write sink metrics.")
    parser.add_argument("--topic", default="results")
    parser.add_argument("--bootstrap-server", default="localhost:9094")
    parser.add_argument("--run-id", default=env_default("RUN_ID", ""))
    parser.add_argument("--expected-frames", type=int, default=int(env_default("EXPECTED_FRAMES", "300")))
    parser.add_argument("--sink-metrics-out", default=env_default("SINK_METRICS_OUTPUT", "e2e-sink-metrics.jsonl"))
    parser.add_argument("--implementation", default="flink")
    parser.add_argument("--starting-offsets", choices=("latest", "earliest"), default="latest")
    parser.add_argument("--idle-timeout-ms", type=int, default=0)
    parser.add_argument(
        "--measure-per-frame-latency",
        action="store_true",
        help="Collect detailed per-frame processing timing headers",
    )
    parser.add_argument("--ready-file", default="", help="Signal initialized Kafka offsets to the benchmark wrapper")
    return parser.parse_args()


def append_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def iso_duration_ms(started_at: str, finished_at: str):
    started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    return max(0, round((finished - started).total_seconds() * 1000))


def iso_duration_float_ms(started_at: str, finished_at: str):
    started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    return max(0.0, (finished - started).total_seconds() * 1000)


def is_jpeg(data: bytes) -> bool:
    return (len(data) >= 4 and data[:2] == b"\xff\xd8" and data[-2:] == b"\xff\xd9"
            and b"\xff\xda" in data)


def print_progress(started_at_monotonic, expected_frames, result_frames,
                   unique_frames, errors, duplicate_frames):
    elapsed = time.monotonic() - started_at_monotonic
    print(
        "progress: received=%d/%d successful=%d errors=%d "
        "duplicates=%d elapsed=%s"
        % (
            unique_frames,
            expected_frames,
            result_frames,
            errors,
            duplicate_frames,
            format_elapsed(elapsed),
        ),
        flush=True,
    )


def format_elapsed(seconds):
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    return f"{minutes}m{secs:02d}s"


def header(record, name, fallback=""):
    value = None
    for key, item in record.headers:
        if key == name:
            value = item
    return fallback if value is None else value.decode("utf-8", errors="replace")


def wait_for_consumer_ready(consumer, topic, timeout_seconds=60):
    """Initialize every partition's position, preserving any records polled."""
    deadline = time.monotonic() + timeout_seconds
    buffered = {}
    while time.monotonic() < deadline:
        for partition, records in consumer.poll(timeout_ms=1000, max_records=1).items():
            buffered.setdefault(partition, []).extend(records)
        expected = consumer.partitions_for_topic(topic)
        assigned = consumer.assignment()
        if not expected or {p.partition for p in assigned if p.topic == topic} != expected:
            continue
        # Assignment alone is insufficient: latest offsets are resolved lazily.
        for partition in assigned:
            consumer.position(partition, timeout_ms=1000)
        return buffered
    raise RuntimeError("result consumer did not initialize Kafka offsets within 60 seconds")


def main():
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent
    started_at = utc_now_iso()
    first_result_received_at = None
    result_frames = 0
    errors = 0
    input_bytes = 0
    output_bytes = 0
    output_payload_bytes = 0
    failed_results = 0
    duplicate_frames = 0
    out_of_order_frames = 0
    malformed_results = 0
    validation_errors = []
    seen_frame_ids = set()
    next_frame_id = 1
    last_match = time.monotonic()
    processing_started_at = []
    processing_finished_at = []
    processing_ms = []
    decode_ms = []
    transformation_ms = []
    encode_ms = []
    started_at_monotonic = time.monotonic()
    last_progress = started_at_monotonic

    consumer_options = dict(
        bootstrap_servers=args.bootstrap_server,
        group_id=f"result-consumer-{args.run_id}-{time.time_ns()}",
        auto_offset_reset=args.starting_offsets,
        enable_auto_commit=False,
    )
    if args.idle_timeout_ms > 0:
        consumer_options["consumer_timeout_ms"] = args.idle_timeout_ms
    consumer = KafkaConsumer(
        args.topic,
        **consumer_options,
    )
    try:
        pending_records = {}
        if args.ready_file:
            pending_records = wait_for_consumer_ready(consumer, args.topic)
            Path(args.ready_file).write_text("ready\n", encoding="utf-8")
            last_match = time.monotonic()
            print("Result consumer ready: Kafka partitions and offsets initialized", flush=True)
        finished = False
        while not finished:
            records = pending_records or consumer.poll(timeout_ms=1000)
            pending_records = {}
            now_monotonic = time.monotonic()
            if not records:
                if args.idle_timeout_ms > 0 and (now_monotonic - last_match) * 1000 >= args.idle_timeout_ms:
                    break
                if now_monotonic - last_progress >= 60:
                    print_progress(
                        started_at_monotonic,
                        args.expected_frames,
                        result_frames,
                        len(seen_frame_ids),
                        errors,
                        duplicate_frames,
                    )
                    last_progress = now_monotonic
                continue

            for batch in records.values():
                for record in batch:
                    run_id = header(record, "run_id")
                    if args.run_id and run_id != args.run_id:
                        continue
                    last_match = time.monotonic()
                    if first_result_received_at is None:
                        first_result_received_at = utc_now_iso()
                    output = record.value or b""
                    output_bytes += len(output)
                    if args.measure_per_frame_latency:
                        started_processing = header(record, "processing_started_at")
                        decode_finished = header(record, "decode_finished_at")
                        transformation_finished = header(record, "transformation_finished_at")
                        finished_processing = header(record, "processing_finished_at")
                        if started_processing and finished_processing:
                            processing_started_at.append(started_processing)
                            processing_finished_at.append(finished_processing)
                            processing_ms.append(iso_duration_float_ms(started_processing, finished_processing))
                        if started_processing and decode_finished:
                            decode_ms.append(iso_duration_float_ms(started_processing, decode_finished))
                        if decode_finished and transformation_finished:
                            transformation_ms.append(iso_duration_float_ms(decode_finished, transformation_finished))
                        if transformation_finished and finished_processing:
                            encode_ms.append(iso_duration_float_ms(transformation_finished, finished_processing))
                    frame_id = int(header(record, "frame_id", "0"))
                    input_bytes += int(header(record, "payload_bytes", "0"))
                    if frame_id in seen_frame_ids:
                        duplicate_frames += 1; errors += 1; validation_errors.append("duplicate_frame_id")
                    else:
                        seen_frame_ids.add(frame_id)
                    if frame_id != next_frame_id:
                        out_of_order_frames += 1; errors += 1; validation_errors.append("frame_id_out_of_order")
                    next_frame_id = max(next_frame_id, frame_id + 1)
                    status = header(record, "processing_status")
                    if status != "ok":
                        failed_results += 1; errors += 1; validation_errors.append("failed_processing_status")
                    elif not is_jpeg(output):
                        errors += 1; validation_errors.append("invalid_output_jpeg")
                    else:
                        result_frames += 1
                        output_payload_bytes += len(output)
                    now_monotonic = time.monotonic()
                    if now_monotonic - last_progress >= 60:
                        print_progress(
                            started_at_monotonic,
                            args.expected_frames,
                            result_frames,
                            len(seen_frame_ids),
                            errors,
                            duplicate_frames,
                        )
                        last_progress = now_monotonic
                    if result_frames >= args.expected_frames:
                        finished = True
                        break
                if finished:
                    break
    finally:
        consumer.close()

    finished_at = utc_now_iso()
    missing_frames = max(0, args.expected_frames - len({i for i in seen_frame_ids if 1 <= i <= args.expected_frames}))
    completion_time_ms = iso_duration_ms(started_at, finished_at)
    success = (result_frames == args.expected_frames and errors == 0 and
               seen_frame_ids == set(range(1, args.expected_frames + 1)))
    processing_summary = {
        "per_frame_latency_measured": args.measure_per_frame_latency,
        "per_frame_processing_started_at": processing_started_at,
        "per_frame_processing_finished_at": processing_finished_at,
        "per_frame_processing_ms": processing_ms,
        "processing_mean_ms": (sum(processing_ms) / len(processing_ms)) if processing_ms else None,
        "processing_min_ms": min(processing_ms) if processing_ms else None,
        "processing_max_ms": max(processing_ms) if processing_ms else None,
        "per_frame_decode_ms": decode_ms,
        "per_frame_transformation_ms": transformation_ms,
        "per_frame_encode_ms": encode_ms,
        "decode_mean_ms": (sum(decode_ms) / len(decode_ms)) if decode_ms else None,
        "transformation_mean_ms": (sum(transformation_ms) / len(transformation_ms)) if transformation_ms else None,
        "encode_mean_ms": (sum(encode_ms) / len(encode_ms)) if encode_ms else None,
    }
    record = {
        "type": "e2e_sink", "run_id": args.run_id, "implementation": args.implementation,
        "started_at": started_at, "first_result_received_at": first_result_received_at,
        "finished_at": finished_at, "expected_frames": args.expected_frames,
        "result_frames": result_frames, "input_bytes": input_bytes,
        "output_bytes": output_bytes, "output_payload_bytes": output_payload_bytes,
        "errors": errors, "validation_errors": validation_errors,
        "malformed_results": malformed_results, "duplicate_frames": duplicate_frames,
        "out_of_order_frames": out_of_order_frames, "missing_frames": missing_frames,
        "failed_results": failed_results, "completion_time_ms": completion_time_ms,
        "effective_fps": round(result_frames / (completion_time_ms / 1000), 2) if completion_time_ms else 0.0,
        "success": success,
        **processing_summary,
    }
    append_jsonl((project_root / args.sink_metrics_out).resolve(), record)
    print(f"received {result_frames} successful result messages from topic {args.topic}")
    print(f"sink metrics appended to {args.sink_metrics_out}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
