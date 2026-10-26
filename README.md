# Flink Streaming Baseline

This repository contains a Kafka–Flink streaming baseline for the thesis
experiments. It reads JPEG frames from Kafka, transforms each frame in a Flink
job, and writes one result message per input frame to Kafka.

The local setup runs Kafka and Flink with Docker Compose. The producer and
result consumer run directly on the host.

## Requirements

- Docker with Docker Compose
- Java 17 or newer
- Maven 3.9 or newer
- Python 3

Install the Python dependency in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r producer/requirements.txt
```

## Quickstart: one machine

The benchmark runner starts the local Kafka and Flink services, creates the
topics, builds and submits the Flink job, waits for it to start, and runs the
producer and result consumer.

For a short smoke test:

```bash
./scripts/run-benchmark.sh \
  --mode=unscaled \
  --frames=30 \
  --refinement-passes=0
```

For the standard benchmark:

```bash
./scripts/run-benchmark.sh \
  --mode=unscaled \
  --frames=300 \
  --refinement-passes=12
```

Results and source/sink metrics are written to `tmp/`. Each run receives a
unique run ID unless `--run-id=...` is supplied.

Stop the local services after the experiment:

```bash
docker compose down
```

## Manual local workflow

Start Kafka and Flink:

```bash
docker compose up -d
```

Build the job:

```bash
(cd flink-job && mvn clean package)
```

Create the Kafka topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --if-not-exists --topic frames \
  --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --if-not-exists --topic results \
  --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

Submit the Flink job:

```bash
./scripts/submit-job.sh
```

Start the result consumer before publishing frames:

```bash
python3 producer/result_consumer.py \
  --run-id=bench-001 \
  --expected-frames=30 \
  --implementation=flink \
  --sink-metrics-out=tmp/e2e-sink-metrics.jsonl
```

In another terminal, publish the frames:

```bash
python3 producer/producer.py \
  --topic=frames \
  --frames-dir=frames \
  --stream-id=camera-a \
  --repeat=1 \
  --run-id=bench-001 \
  --source-metrics-out=tmp/e2e-source-metrics.jsonl
```

The standard local endpoints are:

- Kafka: `localhost:9094`
- Flink UI: <http://localhost:8081>

## Benchmark modes

The default unscaled mode uses one Kafka partition and one Flink worker:

```bash
./scripts/run-benchmark.sh --mode=unscaled --frames=300
```

Scaled mode distributes frames across Kafka partitions and Flink workers:

```bash
./scripts/run-benchmark.sh \
  --mode=scaled \
  --partitions=8 \
  --frames=300
```

Use `--refinement-passes=0` for a fast smoke test. The benchmark default is
12 refinement passes. Use a fresh `--run-id` for every experiment.

For a cold-start experiment, add `--cold-start` to the benchmark runner. The
activator waits for the first frame and submits the Flink job automatically:

```bash
./scripts/run-benchmark.sh \
  --mode=unscaled \
  --frames=300 \
  --cold-start \
  --run-id=coldstart-001
```

## Split deployment

To simulate a client machine and a separate Flink system-under-test locally:

```bash
./scripts/run-split-local.sh \
  --mode=scaled \
  --partitions=4 \
  --frames=300
```

The client stack is defined in `deploy/client-compose.yml`; the SUT stack is
defined in `deploy/sut-compose.yml`. For the two-VM setup, use
`run-client-benchmark.sh` on the client VM and `run-sut.sh` on the SUT VM.

## Project layout

```text
flink-job/       Flink streaming job and Maven build
producer/        Kafka producer, result consumer, and cold-start activator
frames/          30 JPEG input frames
scripts/         Local and split-deployment runners
deploy/          Compose files for split deployment
```

## Input and output

The producer sends raw JPEG bytes as Kafka values. Metadata such as `frame_id`,
`stream_id`, and `run_id` is sent in Kafka headers. The Flink job transforms
each image and publishes the resulting JPEG to the `results` topic.

## Architecture

```text
frame files -> Python producer -> Kafka `frames`
                              -> Flink Kafka source
                              -> image processor
                              -> Kafka `results` -> Python result consumer
```

For cold-start runs, an activator watches `frames` and submits Flink after it
observes the first frame for the selected `run_id`:

```text
Kafka `frames` -> activator -> Flink job submission
```

The activator does not delete or forward the frame. Kafka retains it, and
Flink consumes it with a fresh consumer group starting at the earliest offset.

## Warm and cold-start experiments

### Warm run

In a warm run, the Flink job is already running before the first frame is
published. Job startup is excluded from the processing measurement.

Use a fresh consumer group and `--starting-offsets=latest`:

```bash
./scripts/submit-job.sh \
  --consumer-group=flink-warm-001 \
  --starting-offsets=latest
```

Wait until the job is `RUNNING` before starting the producer. This avoids
consuming historical records and keeps Flink startup out of the warm metric.

### Cold-start run

In a cold-start run, the activator waits for the first frame, then submits the
Flink job. The job starts with group `flink-<run_id>` and earliest offsets, so
it processes the retained triggering frame.

For a manual trial, use three terminals and the same new run ID:

Terminal 1, result consumer:

```bash
export RUN_ID=coldstart-001
python3 producer/result_consumer.py \
  --run-id="$RUN_ID" \
  --expected-frames=30 \
  --implementation=flink-coldstart \
  --sink-metrics-out=tmp/e2e-sink-metrics.jsonl
```

Terminal 2, activator:

```bash
export RUN_ID=coldstart-001
python3 producer/activator.py \
  --run-id="$RUN_ID" \
  --activation-metrics-out=tmp/e2e-activation-metrics.jsonl
```

Terminal 3, producer:

```bash
export RUN_ID=coldstart-001
python3 producer/producer.py \
  --run-id="$RUN_ID" \
  --repeat=1 \
  --source-metrics-out=tmp/e2e-source-metrics.jsonl
```

The sequence is:

1. The producer writes the first frame to Kafka.
2. The activator observes the matching frame.
3. The activator copies the JAR and submits Flink in detached mode.
4. Flink processes the retained triggering frame and subsequent frames.
5. The activator exits after successful submission.
6. The result consumer exits after the expected result count.
7. The Flink job remains running and must be cancelled explicitly.

## Scaled single-stream runs

To distribute frames from one stream across Kafka partitions and Flink workers,
increase the topic partition count, publish with `--partition-key=frame`, and
use matching Flink parallelism:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --alter --topic frames --partitions 4 --bootstrap-server kafka:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --alter --topic results --partitions 4 --bootstrap-server kafka:9092

./scripts/submit-job.sh \
  --consumer-group=flink-scaled-001 \
  --starting-offsets=latest \
  --parallelism=4 \
  --preserve-order=true

python3 producer/producer.py \
  --run-id=scaled-001 \
  --repeat=10 \
  --partition-key=frame
```

`--preserve-order=true` routes completed results through a single reorder stage
and preserves global frame-ID order. It can buffer results when a preceding
frame is slow or missing. Set it to `false` when maximum throughput is more
important than output order. For multiple independent streams, use
`--partition-key=stream`; each stream remains ordered within its Kafka
partition without global reordering.

## Metrics

The producer writes an `e2e_source` record containing the first frame time,
frame count, byte count, and process timestamps.

The activator writes a `flink_activation` record containing:

- `first_frame_observed_at`
- `job_submission_started_at`
- `job_submission_finished_at`
- `activation_latency_ms`
- `job_submission_latency_ms`
- the Flink consumer group

For cold-start runs it also records `activation_trigger_at` and
`activation_trigger_origin`.

The result consumer writes an `e2e_sink` record containing the first result
time, result count, completion time, effective FPS, and success status. It
validates the selected run ID, IDs/order, result status, and JPEG output, and
reports missing, duplicate, malformed, and failed results.

Flink results include timestamps for receipt, processing, decoding,
transformation, encoding, and result writing. The consumer derives per-frame
decode, transformation, and encode durations from them.

The main cold-start metric is:

```text
first-result latency = sink.first_result_received_at - source.first_frame_sent_at
```

This includes activator detection, JAR transfer, Flink submission and startup,
Kafka source initialization, first-frame processing, and result delivery. The
activator records submission completion, not confirmation that Flink has
reached `RUNNING`; therefore `job_submission_latency_ms` is not the complete
Flink startup time. A stricter setup would poll the Flink REST API for the
first `RUNNING` state.

The timestamps are UTC wall-clock timestamps. Distributed trials require
synchronized clocks.

## Experimental controls

Keep these fixed when comparing implementations:

- frame set, order, dimensions, and encoding
- number of frames
- Kafka partitions and topic configuration
- Flink parallelism and container resources
- producer burst and interval behavior
- expected result count and success criterion
- Docker host and infrastructure state

Use a unique `run_id` for every trial. It provides correlation and creates a
fresh Flink consumer group. Kafka topics do not normally need to be cleared
between trials because old records are filtered by `run_id`.

For every trial:

1. Cancel any previous Flink job.
2. Choose a new `run_id`.
3. Start the result consumer.
4. Start the activator for cold-start trials, or submit Flink for warm trials.
5. Publish the identical input set.
6. Wait for all expected successful results.
7. Preserve the source, activation, and sink JSONL records.
8. Record errors, timeouts, restarts, or manual interventions.

Run multiple independent trials per condition and report a central value and
spread, such as median and interquartile range. Keep warm and cold-start
trials separate. Identify or consistently exclude the first run after starting
Docker if it includes infrastructure startup effects.

## Cleanup

After the experiment series:

```bash
docker compose exec jobmanager ./bin/flink list
docker compose exec jobmanager ./bin/flink cancel <job-id>
docker compose down
```

Keep the JSONL metrics and exact run configuration as experiment artifacts.
Topic deletion is normally unnecessary; delete and recreate topics only when
an empty Kafka history is explicitly required.

## Useful commands

List running Flink jobs:

```bash
docker compose exec jobmanager ./bin/flink list
```

Cancel a job:

```bash
docker compose exec jobmanager ./bin/flink cancel <job-id>
```

Inspect raw result messages:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic results \
  --from-beginning
```
