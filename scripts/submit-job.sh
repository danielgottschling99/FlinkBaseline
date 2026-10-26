#!/usr/bin/env sh
set -eu

JAR_NAME="flink-streaming-baseline-job-1.0.0.jar"
LOCAL_JAR="flink-job/target/${JAR_NAME}"
CONTAINER_JAR="/tmp/${JAR_NAME}"
CONSUMER_GROUP="flink-streaming-baseline"
STARTING_OFFSETS="earliest"
REFINEMENT_PASSES="12"
PARALLELISM="1"
PRESERVE_ORDER="false"
FIRST_FRAME_ID="1"
BOOTSTRAP_SERVERS="kafka:9092"
INPUT_TOPIC="frames"
OUTPUT_TOPIC="results"

for argument in "$@"; do
    case "${argument}" in
        --consumer-group=*)
            CONSUMER_GROUP="${argument#*=}"
            ;;
        --starting-offsets=*)
            STARTING_OFFSETS="${argument#*=}"
            ;;
        --refinement-passes=*)
            REFINEMENT_PASSES="${argument#*=}"
            ;;
        --parallelism=*)
            PARALLELISM="${argument#*=}"
            ;;
        --preserve-order=*)
            PRESERVE_ORDER="${argument#*=}"
            ;;
        --first-frame-id=*)
            FIRST_FRAME_ID="${argument#*=}"
            ;;
        --bootstrap-servers=*)
            BOOTSTRAP_SERVERS="${argument#*=}"
            ;;
        --input-topic=*)
            INPUT_TOPIC="${argument#*=}"
            ;;
        --output-topic=*)
            OUTPUT_TOPIC="${argument#*=}"
            ;;
        *)
            echo "Unknown argument: ${argument}" >&2
            exit 1
            ;;
    esac
done

if [ ! -f "${LOCAL_JAR}" ]; then
    echo "Missing ${LOCAL_JAR}. Build it first with: cd flink-job && mvn clean package" >&2
    exit 1
fi

docker compose cp "${LOCAL_JAR}" "jobmanager:${CONTAINER_JAR}"

docker compose exec jobmanager ./bin/flink run -d \
    -c com.example.flinkbaseline.StreamingBaselineJob \
    "${CONTAINER_JAR}" \
    --bootstrap-servers="${BOOTSTRAP_SERVERS}" \
    --input-topic="${INPUT_TOPIC}" \
    --output-topic="${OUTPUT_TOPIC}" \
    --consumer-group="${CONSUMER_GROUP}" \
    --starting-offsets="${STARTING_OFFSETS}" \
    --refinement-passes="${REFINEMENT_PASSES}" \
    --parallelism="${PARALLELISM}" \
    --preserve-order="${PRESERVE_ORDER}" \
    --first-frame-id="${FIRST_FRAME_ID}"
