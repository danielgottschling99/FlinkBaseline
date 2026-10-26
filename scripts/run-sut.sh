#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)
cd "${PROJECT_ROOT}"

PYTHON="${PYTHON:-python3}"
if [ -x "${PROJECT_ROOT}/.venv/bin/python" ]; then
    PYTHON="${PROJECT_ROOT}/.venv/bin/python"
fi

MODE="scaled"
PARTITIONS="4"
REFINEMENT_PASSES="12"
RUN_ID=""
CLIENT_BOOTSTRAP=""
SUT_BOOTSTRAP=""
SUT_BOOTSTRAP_EXPLICIT="false"
SUT_PROJECT="flink-benchmark-sut"
COLD_START="false"
ACTIVATION_METRICS_OUT="tmp/e2e-activation-metrics.jsonl"

for argument in "$@"; do
    case "${argument}" in
        --mode=*) MODE="${argument#*=}" ;;
        --partitions=*) PARTITIONS="${argument#*=}" ;;
        --refinement-passes=*) REFINEMENT_PASSES="${argument#*=}" ;;
        --run-id=*) RUN_ID="${argument#*=}" ;;
        --cold-start) COLD_START="true" ;;
        --activation-metrics-out=*) ACTIVATION_METRICS_OUT="${argument#*=}" ;;
        --client-bootstrap=*)
            CLIENT_BOOTSTRAP="${argument#*=}"
            SUT_BOOTSTRAP="${argument#*=}"
            ;;
        --sut-bootstrap=*)
            SUT_BOOTSTRAP="${argument#*=}"
            SUT_BOOTSTRAP_EXPLICIT="true"
            ;;
        *) echo "Unknown argument: ${argument}" >&2; exit 1 ;;
    esac
done

case "${MODE}" in
    unscaled)
        PARTITIONS="1"
        PRESERVE_ORDER="false"
        ;;
    scaled)
        case "${PARTITIONS}" in
            2|4|8|16) ;;
            *) echo "--partitions must be 2, 4, 8, or 16" >&2; exit 1 ;;
        esac
        PRESERVE_ORDER="true"
        ;;
    *) echo "--mode must be unscaled or scaled" >&2; exit 1 ;;
esac

if [ -z "${CLIENT_BOOTSTRAP}" ]; then
    echo "Missing --client-bootstrap=<client-ip>:9094" >&2
    exit 1
fi
if [ "${SUT_BOOTSTRAP_EXPLICIT}" != true ] \
    && { [ "${CLIENT_BOOTSTRAP}" = localhost:9094 ] || [ "${CLIENT_BOOTSTRAP}" = 127.0.0.1:9094 ]; }; then
    SUT_BOOTSTRAP="kafka:9092"
elif [ -z "${SUT_BOOTSTRAP}" ]; then
    SUT_BOOTSTRAP="${CLIENT_BOOTSTRAP}"
fi

if [ -z "${RUN_ID}" ]; then
    RUN_ID="gcp-$(date +%Y%m%d%H%M%S)-p${PARTITIONS}"
fi

if [ "${MODE}" = unscaled ]; then
    INPUT_TOPIC="frames"
    OUTPUT_TOPIC="results"
else
    INPUT_TOPIC="frames-p${PARTITIONS}"
    OUTPUT_TOPIC="results-p${PARTITIONS}"
fi
COMPOSE_FILE_PATH="deploy/sut-compose.yml"

compose_sut() {
    COMPOSE_FILE="${COMPOSE_FILE_PATH}" \
    COMPOSE_PROJECT_NAME="${SUT_PROJECT}" \
    docker compose "$@"
}

if ! docker network inspect benchmark-net >/dev/null 2>&1; then
    docker network create benchmark-net >/dev/null
fi

cd flink-job
mvn package -q
cd ..

FLINK_TASK_SLOTS="${PARTITIONS}" compose_sut up -d
FLINK_TASK_SLOTS="${PARTITIONS}" compose_sut up -d \
    --no-deps --force-recreate --scale taskmanager=1 taskmanager

echo "Waiting for Flink TaskManager slots..."
slots_ready=false
for attempt in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:8081/taskmanagers 2>/dev/null \
        | grep -Eq '"freeSlots"[[:space:]]*:[[:space:]]*[1-9][0-9]*'; then
        slots_ready=true
        break
    fi
    sleep 1
done
if [ "${slots_ready}" != true ]; then
    compose_sut ps
    compose_sut logs --tail=100 taskmanager || true
    echo "Flink TaskManager did not register a free slot" >&2
    exit 1
fi

if [ "${COLD_START}" = true ]; then
    COMPOSE_FILE="${COMPOSE_FILE_PATH}" \
    COMPOSE_PROJECT_NAME="${SUT_PROJECT}" \
    "${PYTHON}" producer/activator.py \
        --run-id="${RUN_ID}" \
        --topic="${INPUT_TOPIC}" \
        --bootstrap-server="${CLIENT_BOOTSTRAP}" \
        --submit-bootstrap-server="${SUT_BOOTSTRAP}" \
        --input-topic="${INPUT_TOPIC}" \
        --output-topic="${OUTPUT_TOPIC}" \
        --starting-offsets=earliest \
        --parallelism="${PARTITIONS}" \
        --preserve-order="${PRESERVE_ORDER}" \
        --refinement-passes="${REFINEMENT_PASSES}" \
        --activation-metrics-out="${ACTIVATION_METRICS_OUT}"
    echo "Cold-start activation completed: run-id=${RUN_ID}, partitions=${PARTITIONS}"
    exit 0
fi

# For a warm run, submit the job here so the two-VM workflow only requires
# starting the client and SUT scripts.  The client script waits for this job
# to become RUNNING before publishing frames.
COMPOSE_FILE="${COMPOSE_FILE_PATH}" \
COMPOSE_PROJECT_NAME="${SUT_PROJECT}" \
./scripts/submit-job.sh \
    --bootstrap-servers="${SUT_BOOTSTRAP}" \
    --consumer-group="flink-${RUN_ID}" \
    --starting-offsets=latest \
    --parallelism="${PARTITIONS}" \
    --preserve-order="${PRESERVE_ORDER}" \
    --refinement-passes="${REFINEMENT_PASSES}" \
    --input-topic="${INPUT_TOPIC}" \
    --output-topic="${OUTPUT_TOPIC}"

echo "Waiting for Flink job to reach RUNNING..."
running=false
for attempt in $(seq 1 120); do
    if compose_sut exec -T jobmanager ./bin/flink list 2>/dev/null | grep -q RUNNING; then
        running=true
        break
    fi
    sleep 1
done

if [ "${running}" != true ]; then
    compose_sut exec -T jobmanager ./bin/flink list -a || true
    echo "Flink job did not reach RUNNING" >&2
    exit 1
fi

echo "SUT ready: run-id=${RUN_ID}, partitions=${PARTITIONS}"
