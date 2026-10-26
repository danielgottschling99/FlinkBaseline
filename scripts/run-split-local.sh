#!/usr/bin/env sh
set -eu

MODE="unscaled"
FRAMES="30"
REFINEMENT_PASSES="12"
RUN_ID="flink-split-$(date +%Y%m%d%H%M%S)"
PARTITIONS_ARG=""
CLIENT_BOOTSTRAP="localhost:9094"
SUT_BOOTSTRAP="kafka:9092"
CLIENT_COMPOSE="docker compose -f deploy/client-compose.yml"
SUT_COMPOSE_FILE="deploy/sut-compose.yml"
SUT_PROJECT="flink-split-sut"

for argument in "$@"; do
    case "${argument}" in
        --mode=*) MODE="${argument#*=}" ;;
        --frames=*) FRAMES="${argument#*=}" ;;
        --refinement-passes=*) REFINEMENT_PASSES="${argument#*=}" ;;
        --run-id=*) RUN_ID="${argument#*=}" ;;
        --partitions=*) PARTITIONS_ARG="${argument#*=}" ;;
        --bootstrap-servers=*)
            CLIENT_BOOTSTRAP="${argument#*=}"
            SUT_BOOTSTRAP="${argument#*=}"
            ;;
        --sut-bootstrap-servers=*) SUT_BOOTSTRAP="${argument#*=}" ;;
        *) echo "Unknown argument: ${argument}" >&2; exit 1 ;;
    esac
done

case "${MODE}" in
    unscaled)
        PARALLELISM="1"
        PARTITIONS="1"
        PARTITION_KEY="stream"
        PRESERVE_ORDER="false"
        INPUT_TOPIC="frames"
        OUTPUT_TOPIC="results"
        ;;
    scaled)
        PARTITIONS="${PARTITIONS_ARG:-4}"
        PARALLELISM="${PARTITIONS}"
        PARTITION_KEY="frame"
        PRESERVE_ORDER="true"
        INPUT_TOPIC="frames-p${PARTITIONS}"
        OUTPUT_TOPIC="results-p${PARTITIONS}"
        ;;
    *) echo "--mode must be unscaled or scaled" >&2; exit 1 ;;
esac

case "${PARTITIONS}" in
    1|2|4|8|16) ;;
    *) echo "--partitions must be 1, 2, 4, 8, or 16" >&2; exit 1 ;;
esac

if [ "${MODE}" = scaled ] && [ "${PARTITIONS}" = 1 ]; then
    echo "scaled mode requires at least 2 partitions" >&2
    exit 1
fi

case "${FRAMES}" in
    30) REPEAT="1" ;;
    300) REPEAT="10" ;;
    *) echo "--frames must be 30 or 300" >&2; exit 1 ;;
esac

ensure_network() {
    if ! docker network inspect benchmark-net >/dev/null 2>&1; then
        docker network create benchmark-net >/dev/null
    fi
}

ensure_topic() {
    topic="$1"
    if ! ${CLIENT_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --list --bootstrap-server kafka:9092 | grep -Fxq "${topic}"; then
        ${CLIENT_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --create --topic "${topic}" --bootstrap-server kafka:9092 \
            --partitions "${PARTITIONS}" --replication-factor 1
        return
    fi

    current_partitions=$(${CLIENT_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --describe --topic "${topic}" --bootstrap-server kafka:9092 \
        | awk '{for (i = 1; i <= NF; i++) if ($i == "PartitionCount:") print $(i + 1)}' \
        | head -n 1)
    if [ -n "${current_partitions}" ] && [ "${current_partitions}" -lt "${PARTITIONS}" ]; then
        ${CLIENT_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --alter --topic "${topic}" --bootstrap-server kafka:9092 \
            --partitions "${PARTITIONS}"
    fi
}

ensure_network
${CLIENT_COMPOSE} up -d
ensure_topic "${INPUT_TOPIC}"
ensure_topic "${OUTPUT_TOPIC}"

cd flink-job
mvn package -q
cd ..

FLINK_TASK_SLOTS="${PARALLELISM}" COMPOSE_FILE="${SUT_COMPOSE_FILE}" \
COMPOSE_PROJECT_NAME="${SUT_PROJECT}" docker compose up -d
FLINK_TASK_SLOTS="${PARALLELISM}" COMPOSE_FILE="${SUT_COMPOSE_FILE}" \
COMPOSE_PROJECT_NAME="${SUT_PROJECT}" docker compose up -d --no-deps --force-recreate \
    --scale taskmanager=1 taskmanager

COMPOSE_FILE="${SUT_COMPOSE_FILE}" COMPOSE_PROJECT_NAME="${SUT_PROJECT}" \
./scripts/submit-job.sh \
    --bootstrap-servers="${SUT_BOOTSTRAP}" \
    --consumer-group="flink-${RUN_ID}" \
    --starting-offsets=latest \
    --parallelism="${PARALLELISM}" \
    --preserve-order="${PRESERVE_ORDER}" \
    --refinement-passes="${REFINEMENT_PASSES}" \
    --input-topic="${INPUT_TOPIC}" \
    --output-topic="${OUTPUT_TOPIC}"

echo "Waiting for Flink job to reach RUNNING..."
running=false
for attempt in $(seq 1 60); do
    if COMPOSE_FILE="${SUT_COMPOSE_FILE}" COMPOSE_PROJECT_NAME="${SUT_PROJECT}" \
        docker compose exec -T jobmanager ./bin/flink list 2>/dev/null | grep -q RUNNING; then
        running=true
        break
    fi
    sleep 1
done
if [ "${running}" != true ]; then
    COMPOSE_FILE="${SUT_COMPOSE_FILE}" COMPOSE_PROJECT_NAME="${SUT_PROJECT}" \
        docker compose exec -T jobmanager ./bin/flink list -a || true
    echo "Flink job did not reach RUNNING" >&2
    exit 1
fi

python3 producer/result_consumer.py \
    --bootstrap-server="${CLIENT_BOOTSTRAP}" \
    --topic="${OUTPUT_TOPIC}" \
    --run-id="${RUN_ID}" \
    --expected-frames="${FRAMES}" \
    --implementation="flink-${MODE}-split-local" \
    --sink-metrics-out="tmp/e2e-sink-metrics.jsonl" &
consumer_pid=$!
trap 'kill "${consumer_pid}" 2>/dev/null || true' EXIT INT TERM

python3 producer/producer.py \
    --bootstrap-server="${CLIENT_BOOTSTRAP}" \
    --topic="${INPUT_TOPIC}" \
    --run-id="${RUN_ID}" \
    --repeat="${REPEAT}" \
    --max-frames="${FRAMES}" \
    --partition-key="${PARTITION_KEY}" \
    --source-metrics-out="tmp/e2e-source-metrics.jsonl"

wait "${consumer_pid}"
trap - EXIT INT TERM
echo "Benchmark completed: ${RUN_ID} (${MODE}, ${FRAMES} frames)"
