#!/usr/bin/env python3

import argparse
import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def env_default(name: str, fallback):
    return os.environ.get(name, fallback)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Start the Flink job when the first frame for a run arrives."
    )
    parser.add_argument("--run-id", default=env_default("RUN_ID", ""))
    parser.add_argument("--topic", default="frames")
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument(
        "--submit-bootstrap-server",
        default="kafka:9092",
        help="Kafka address passed to the Flink job",
    )
    parser.add_argument("--input-topic", default="frames")
    parser.add_argument("--output-topic", default="results")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--preserve-order", choices=("true", "false"), default="false")
    parser.add_argument("--refinement-passes", type=int, default=12)
    parser.add_argument(
        "--consumer-group",
        default="",
        help="Flink group to use; defaults to flink-<run-id>",
    )
    parser.add_argument(
        "--activation-metrics-out",
        default=env_default("ACTIVATION_METRICS_OUTPUT", "e2e-activation-metrics.jsonl"),
    )
    return parser.parse_args()


def append_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def duration_ms(started_at: str, finished_at: str):
    started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    return max(0, round((finished - started).total_seconds() * 1000))


def stop_process(process):
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def main():
    args = parse_args()
    if not args.run_id:
        raise RuntimeError("--run-id is required")

    flink_group = args.consumer_group or f"flink-{args.run_id}"
    project_root = Path(__file__).resolve().parent.parent
    activation_started_at = utc_now_iso()
    activator_group = f"activator-{args.run_id}"

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server.replace("kafka:9092", "localhost:9094"),
        group_id=activator_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    try:
        print(
            f"Cold-start activator ready: run-id={args.run_id}; waiting for first frame",
            flush=True,
        )
        for record in consumer:
            headers = {key: value.decode("utf-8", errors="replace") for key, value in record.headers}
            if headers.get("run_id", "") != args.run_id:
                continue

            first_frame_observed_at = utc_now_iso()
            submission_started_at = utc_now_iso()
            submit_command = [
                str(project_root / "scripts" / "submit-job.sh"),
                f"--consumer-group={flink_group}",
                f"--bootstrap-servers={args.submit_bootstrap_server}",
                f"--input-topic={args.input_topic}",
                f"--output-topic={args.output_topic}",
                f"--starting-offsets={args.starting_offsets}",
                f"--parallelism={args.parallelism}",
                f"--preserve-order={args.preserve_order}",
                f"--refinement-passes={args.refinement_passes}",
            ]
            submit = subprocess.run(
                submit_command,
                cwd=project_root,
                capture_output=True,
                text=True,
            )
            submission_finished_at = utc_now_iso()
            if submit.returncode != 0:
                details = (submit.stderr or submit.stdout).strip()
                raise RuntimeError(details or "Flink job submission failed")

            append_jsonl(
                (project_root / args.activation_metrics_out).resolve(),
                {
                    "type": "flink_activation",
                    "run_id": args.run_id,
					"activation_trigger_at": first_frame_observed_at,
					"activation_trigger_origin": "first input frame observed",
                    "activator_started_at": activation_started_at,
                    "first_frame_observed_at": first_frame_observed_at,
                    "job_submission_started_at": submission_started_at,
                    "job_submission_finished_at": submission_finished_at,
                    "activation_latency_ms": duration_ms(
                        first_frame_observed_at, submission_finished_at
                    ),
                    "job_submission_latency_ms": duration_ms(
                        submission_started_at, submission_finished_at
                    ),
                    "consumer_group": flink_group,
                    "success": True,
                },
            )
            print(f"started Flink for run {args.run_id} using group {flink_group}")
            return
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
