# Report Evidence Notes

## Primary evidence policy

The repository treats the cloud run as the primary execution record. The authoritative evidence files are:

- `outputs/hadoop/request_count_by_service.txt`
- `outputs/hadoop/server_error_count_by_service.txt`
- `outputs/hadoop/top_10_slow_endpoints.txt`
- `outputs/degraded_service_detection.txt`
- `outputs/degraded_service_metrics.json`
- `outputs/runtime_report.json`
- `outputs/validation_report.json`

The local Python pipeline is retained only for development checks. It is not the main execution evidence for the final submission.

## Why object storage fits this project

Alibaba Cloud OSS is suitable for the log dataset because the file is static, shared by both the Hadoop baseline and the Ray extension, and easy to retrieve from an ECS instance. This keeps the data source outside the compute node and makes the cloud workflow clearer:

`OSS -> ECS -> Hadoop Streaming / Ray`

Using OSS also supports repeatable reruns because the same source object can be downloaded again without changing the analytics code.

## MapReduce baseline explanation

The three baseline tasks follow a simple key-value pattern:

- request count:
  emit `(service_name, 1)` for every row
- server error count:
  emit `(service_name, 1)` only when `status_code >= 500`
- slow endpoint count:
  emit `(service_name,endpoint, 1)` only when `response_time_ms > 800`

The reducer sums the emitted values for each key. This matches the assignment requirement to demonstrate a MapReduce-style aggregation over the same log dataset.

## Ray partial summary merge

The Ray extension splits the CSV rows into chunks and sends each chunk to a remote task. Each task returns a partial per-service summary with:

- `total_requests`
- `slow_requests`
- `server_errors`
- `timeout_errors`

The merge step adds the same counters across all partial summaries to rebuild the full service-level totals. The final degraded-service rule is then applied to the merged result, not to a single chunk in isolation.

## Runtime comparison

The cloud runtime evidence in `outputs/runtime_report.json` records:

- Hadoop Streaming baseline: `10` seconds
- Ray degraded-service detection: `4` seconds

These numbers come from the Alibaba Cloud ECS execution, so they are more relevant than local timings for the final report.

The three baseline outputs share the same measured Hadoop run because they were executed together inside one scripted cloud run.

## Validation evidence

The repository keeps a separate validation file in `outputs/validation_report.json`. It records manual cross-checks against the raw CSV:

- `payment-service` request count: `7914`
- `payment-service` server error count: `1362`
- `search-service,/search/results` slow count: `1174`

These checks align with the cloud outputs and provide a concrete correctness argument in the report.
