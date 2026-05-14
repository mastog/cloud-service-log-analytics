from __future__ import annotations

import json
import platform
import time
from pathlib import Path

import pandas as pd

from mapreduce.request_count_job import run as run_request_count
from mapreduce.server_error_job import run as run_server_error_count
from mapreduce.slow_endpoints_job import run as run_slow_endpoints
from ray_jobs.degraded_service_detection import run as run_degraded_service_detection


def timed_run(label: str, func, *args):
    started_at = time.perf_counter()
    result = func(*args)
    duration = time.perf_counter() - started_at
    return result, {"label": label, "duration_seconds": round(duration, 4)}


def main() -> None:
    root = Path(__file__).resolve().parent
    dataset = root / "data" / "cloud_service_logs.csv"
    outputs = root / "outputs"
    outputs.mkdir(exist_ok=True)
    local_outputs = outputs / "local"
    local_outputs.mkdir(exist_ok=True)

    _, request_runtime = timed_run(
        "request_count_by_service",
        run_request_count,
        dataset,
        local_outputs / "request_count_by_service.txt",
    )
    _, error_runtime = timed_run(
        "server_error_count_by_service",
        run_server_error_count,
        dataset,
        local_outputs / "server_error_count_by_service.txt",
    )
    _, slow_runtime = timed_run(
        "top_10_slow_endpoints",
        run_slow_endpoints,
        dataset,
        local_outputs / "top_10_slow_endpoints.txt",
    )
    _, ray_runtime = timed_run(
        "degraded_service_detection",
        run_degraded_service_detection,
        dataset,
        local_outputs / "degraded_service_detection.txt",
        local_outputs / "degraded_service_metrics.json",
    )

    runtime_report = {
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "execution_mode": "local Python MapReduce simulation + Ray local mode",
            "dataset_path": str(dataset),
        },
        "jobs": [request_runtime, error_runtime, slow_runtime, ray_runtime],
    }
    (local_outputs / "runtime_report.json").write_text(
        json.dumps(runtime_report, indent=2),
        encoding="utf-8",
    )

    df = pd.read_csv(dataset)
    # Store a few direct raw-data checks for report validation evidence.
    validation_report = {
        "request_count_check": {
            "query": "service_name == 'payment-service'",
            "expected_from_raw_scan": int((df["service_name"] == "payment-service").sum()),
        },
        "server_error_check": {
            "query": "service_name == 'payment-service' and status_code >= 500",
            "expected_from_raw_scan": int(
                ((df["service_name"] == "payment-service") & (df["status_code"] >= 500)).sum()
            ),
        },
        "slow_endpoint_check": {
            "query": "service_name == 'search-service' and endpoint == '/search/results' and response_time_ms > 800",
            "expected_from_raw_scan": int(
                (
                    (df["service_name"] == "search-service")
                    & (df["endpoint"] == "/search/results")
                    & (df["response_time_ms"] > 800)
                ).sum()
            ),
        },
    }
    (local_outputs / "validation_report.json").write_text(
        json.dumps(validation_report, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
