#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
DATASET_PATH="${LOCAL_DATASET_PATH:-$PROJECT_ROOT/data/cloud_service_logs.csv}"
RUN_MODE="${RUN_MODE:-local}"
HDFS_INPUT_PATH="${HDFS_INPUT_PATH:-/mini-project-2/cloud_service_logs.csv}"
HDFS_OUTPUT_ROOT="${HDFS_OUTPUT_ROOT:-/mini-project-2/output}"
STREAMING_JAR="${HADOOP_STREAMING_JAR:-$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar}"

run_job() {
  local output_name="$1"
  local mapper_path="$2"
  local reducer_path="$3"

  if [ "$RUN_MODE" = "hdfs" ]; then
    hdfs dfs -rm -r -f "$HDFS_OUTPUT_ROOT/$output_name" >/dev/null 2>&1 || true

    hadoop jar "$STREAMING_JAR" \
      -files "$mapper_path","$reducer_path" \
      -mapper "python3 $(basename "$mapper_path")" \
      -reducer "python3 $(basename "$reducer_path")" \
      -input "$HDFS_INPUT_PATH" \
      -output "$HDFS_OUTPUT_ROOT/$output_name"
  else
    rm -rf "$PROJECT_ROOT/outputs/hadoop/$output_name"

    "$HADOOP_HOME/bin/hadoop" jar "$STREAMING_JAR" \
      -files "$mapper_path","$reducer_path" \
      -mapper "python3 $(basename "$mapper_path")" \
      -reducer "python3 $(basename "$reducer_path")" \
      -input "file://$DATASET_PATH" \
      -output "file://$PROJECT_ROOT/outputs/hadoop/$output_name"
  fi
}

mkdir -p "$PROJECT_ROOT/outputs/hadoop"

if [ "$RUN_MODE" = "hdfs" ]; then
  # Upload the dataset once so all three jobs reuse the same HDFS input.
  hdfs dfs -mkdir -p "$(dirname "$HDFS_INPUT_PATH")"
  hdfs dfs -rm -f "$HDFS_INPUT_PATH" >/dev/null 2>&1 || true
  hdfs dfs -put "$DATASET_PATH" "$HDFS_INPUT_PATH"
fi

run_job "request_count_by_service" \
  "$PROJECT_ROOT/hadoop_streaming/request_count_mapper.py" \
  "$PROJECT_ROOT/hadoop_streaming/sum_reducer.py"

run_job "server_error_count_by_service" \
  "$PROJECT_ROOT/hadoop_streaming/server_error_mapper.py" \
  "$PROJECT_ROOT/hadoop_streaming/sum_reducer.py"

run_job "top_10_slow_endpoints" \
  "$PROJECT_ROOT/hadoop_streaming/slow_endpoint_mapper.py" \
  "$PROJECT_ROOT/hadoop_streaming/sum_reducer.py"

if [ "$RUN_MODE" = "hdfs" ]; then
  hdfs dfs -cat "$HDFS_OUTPUT_ROOT/request_count_by_service/part-*" | sort -k2,2nr > "$PROJECT_ROOT/outputs/hadoop/request_count_by_service.txt"
  hdfs dfs -cat "$HDFS_OUTPUT_ROOT/server_error_count_by_service/part-*" | sort -k2,2nr > "$PROJECT_ROOT/outputs/hadoop/server_error_count_by_service.txt"
  hdfs dfs -cat "$HDFS_OUTPUT_ROOT/top_10_slow_endpoints/part-*" | sort -k2,2nr | head -n 10 > "$PROJECT_ROOT/outputs/hadoop/top_10_slow_endpoints.txt"
else
  sort -k2,2nr "$PROJECT_ROOT/outputs/hadoop/request_count_by_service/part-00000" > "$PROJECT_ROOT/outputs/hadoop/request_count_by_service.txt"
  sort -k2,2nr "$PROJECT_ROOT/outputs/hadoop/server_error_count_by_service/part-00000" > "$PROJECT_ROOT/outputs/hadoop/server_error_count_by_service.txt"
  sort -k2,2nr "$PROJECT_ROOT/outputs/hadoop/top_10_slow_endpoints/part-00000" | head -n 10 > "$PROJECT_ROOT/outputs/hadoop/top_10_slow_endpoints.txt"
fi

echo "Hadoop streaming jobs completed."
