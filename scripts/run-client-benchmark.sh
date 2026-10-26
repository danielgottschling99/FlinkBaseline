#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)
cd "${PROJECT_ROOT}"

PYTHON="${PYTHON:-python3}"
if [ -x "${PROJECT_ROOT}/.venv/bin/python" ]; then
    PYTHON="${PROJECT_ROOT}/.venv/bin/python"
fi
if ! "${PYTHON}" -c 'import kafka' >/dev/null 2>&1; then
    echo "Missing kafka-python. Install it with: ${PYTHON} -m pip install -r producer/requirements.txt" >&2
    exit 1
fi

MODE="scaled"
CLIENT_IP=""
SUT_HOST=""
PARTITIONS="4"
FRAMES="300"
RUN_ID=""
CLIENT_PROJECT="flink-benchmark-client"
COLD_START="false"
IDLE_TIMEOUT_MS="0"
MEASURE_PER_FRAME_LATENCY="false"

for argument in "$@"; do
    case "${argument}" in
        --mode=*) MODE="${argument#*=}" ;;
        --client-ip=*) CLIENT_IP="${argument#*=}" ;;
        --sut-host=*) SUT_HOST="${argument#*=}" ;;
        --partitions=*) PARTITIONS="${argument#*=}" ;;
        --frames=*) FRAMES="${argument#*=}" ;;
        --run-id=*) RUN_ID="${argument#*=}" ;;
        --idle-timeout-ms=*) IDLE_TIMEOUT_MS="${argument#*=}" ;;
        --measure-per-frame-latency) MEASURE_PER_FRAME_LATENCY="true" ;;
        --cold-start) COLD_START="true" ;;
        *) echo "Unknown argument: ${argument}" >&2; exit 1 ;;
    esac
done

case "${MODE}" in
    unscaled)
        PARTITIONS="1"
        PARTITION_KEY="stream"
        ;;
    scaled)
        PARTITION_KEY="frame"
        case "${PARTITIONS}" in
            2|4|8|16) ;;
            *) echo "--partitions must be 2, 4, 8, or 16" >&2; exit 1 ;;
        esac
        ;;
    *) echo "--mode must be unscaled or scaled" >&2; exit 1 ;;
esac

case "${FRAMES}" in
    ''|*[!0-9]*) echo "--frames must be a positive integer" >&2; exit 1 ;;
esac
if [ "${FRAMES}" -lt 1 ]; then
    echo "--frames must be a positive integer" >&2
    exit 1
fi

# The benchmark input contains 30 JPEG frames. Replay enough complete cycles
# to provide the requested count; --max-frames limits the producer to exactly
# the requested number.
REPEAT=$(( (FRAMES + 29) / 30 ))

if [ -z "${CLIENT_IP}" ] || [ -z "${SUT_HOST}" ]; then
    echo "Required: --client-ip=<client-ip> --sut-host=<sut-ip>" >&2
    exit 1
fi

if [ -z "${RUN_ID}" ]; then
    RUN_ID="gcp-$(date +%Y%m%d%H%M%S)-p${PARTITIONS}"
fi

case "${IDLE_TIMEOUT_MS}" in
    ''|*[!0-9]*) echo "--idle-timeout-ms must be a non-negative integer" >&2; exit 1 ;;
esac

if [ "${MODE}" = unscaled ]; then
    INPUT_TOPIC="frames"
    OUTPUT_TOPIC="results"
else
    INPUT_TOPIC="frames-p${PARTITIONS}"
    OUTPUT_TOPIC="results-p${PARTITIONS}"
fi
BOOTSTRAP_SERVERS="${CLIENT_IP}:9094"
SUT_API="http://${SUT_HOST}:8081"

compose_client() {
    COMPOSE_FILE=deploy/client-compose.yml \
    COMPOSE_PROJECT_NAME="${CLIENT_PROJECT}" \
    docker compose "$@"
}

export KAFKA_ADVERTISED_HOST="${CLIENT_IP}"

if ! docker network inspect benchmark-net >/dev/null 2>&1; then
    docker network create benchmark-net >/dev/null
fi

compose_client up -d

echo "Waiting for Kafka..."
kafka_ready=false
for attempt in $(seq 1 120); do
    if compose_client exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --list --bootstrap-server kafka:9092 >/dev/null 2>&1; then
        kafka_ready=true
        break
    fi
    sleep 1
done
if [ "${kafka_ready}" != true ]; then
    compose_client logs --tail=100 kafka || true
    echo "Kafka did not become ready" >&2
    exit 1
fi

ensure_topic() {
    topic="$1"
    if ! compose_client exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --list --bootstrap-server kafka:9092 | grep -Fxq "${topic}"; then
        compose_client exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --create --topic "${topic}" --bootstrap-server kafka:9092 \
            --partitions "${PARTITIONS}" --replication-factor 1
    fi
}

ensure_topic "${INPUT_TOPIC}"
ensure_topic "${OUTPUT_TOPIC}"

if [ "${COLD_START}" != true ]; then
    echo "Waiting for SUT at ${SUT_API}..."
    sut_ready=false
    for attempt in $(seq 1 300); do
        if curl -fsS "${SUT_API}/jobs/overview" 2>/dev/null \
            | grep -Eq '"state"[[:space:]]*:[[:space:]]*"RUNNING"'; then
            sut_ready=true
            break
        fi
        sleep 1
    done
    if [ "${sut_ready}" != true ]; then
        echo "SUT did not report a RUNNING job" >&2
        exit 1
    fi
fi

if [ "${COLD_START}" = true ]; then
    IMPLEMENTATION="flink-gcp-coldstart"
else
    IMPLEMENTATION="flink-gcp"
fi

CONSUMER_LATENCY_ARG=""
if [ "${MEASURE_PER_FRAME_LATENCY}" = true ]; then
    CONSUMER_LATENCY_ARG="--measure-per-frame-latency"
fi

mkdir -p tmp
consumer_ready_dir=$(mktemp -d "${PROJECT_ROOT}/tmp/consumer-ready.XXXXXX")
"${PYTHON}" producer/result_consumer.py \
    --bootstrap-server="${BOOTSTRAP_SERVERS}" \
    --topic="${OUTPUT_TOPIC}" \
    --run-id="${RUN_ID}" \
    --expected-frames="${FRAMES}" \
    --idle-timeout-ms="${IDLE_TIMEOUT_MS}" \
    --ready-file="${consumer_ready_dir}/ready" \
    --implementation="${IMPLEMENTATION}" \
    --sink-metrics-out=tmp/e2e-sink-metrics.jsonl \
    ${CONSUMER_LATENCY_ARG} &
consumer_pid=$!
cleanup_consumer() {
    kill "${consumer_pid}" 2>/dev/null || true
    if [ -f "${consumer_ready_dir}/ready" ]; then
        unlink "${consumer_ready_dir}/ready"
    fi
    rmdir "${consumer_ready_dir}" 2>/dev/null || true
}
trap cleanup_consumer EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Waiting for result consumer Kafka offsets..."
consumer_ready=false
for attempt in $(seq 1 90); do
    if [ -f "${consumer_ready_dir}/ready" ]; then
        consumer_ready=true
        break
    fi
    if ! kill -0 "${consumer_pid}" 2>/dev/null; then
        echo "Result consumer exited before becoming ready" >&2
        exit 1
    fi
    sleep 1
done
if [ "${consumer_ready}" != true ]; then
    echo "Result consumer did not become ready; no frames were published" >&2
    exit 1
fi

"${PYTHON}" producer/producer.py \
    --bootstrap-server="${BOOTSTRAP_SERVERS}" \
    --topic="${INPUT_TOPIC}" \
    --run-id="${RUN_ID}" \
    --repeat="${REPEAT}" \
    --max-frames="${FRAMES}" \
    --partition-key="${PARTITION_KEY}" \
    --source-metrics-out=tmp/e2e-source-metrics.jsonl

wait "${consumer_pid}"
cleanup_consumer
trap - EXIT INT TERM
echo "Benchmark completed: ${RUN_ID}"
