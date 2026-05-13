from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import ray


def chunk_rows(rows: list[dict[str, str]], chunk_size: int) -> list[list[dict[str, str]]]:
    return [rows[index:index + chunk_size] for index in range(0, len(rows), chunk_size)]


def read_rows(dataset_path: Path) -> list[dict[str, str]]:
    with dataset_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@ray.remote
def summarize_chunk(rows: list[dict[str, str]]) -> dict[str, dict[str, int]]:
    # Build per-service counts for one data chunk.
    summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "total_requests": 0,
            "slow_requests": 0,
            "server_errors": 0,
            "timeout_errors": 0,
        }
    )

    for row in rows:
        service = row["service_name"]
        service_summary = summary[service]
        service_summary["total_requests"] += 1

        if int(row["response_time_ms"]) > 800:
            service_summary["slow_requests"] += 1

        if int(row["status_code"]) >= 500:
            service_summary["server_errors"] += 1

        if row["error_type"] == "Timeout":
            service_summary["timeout_errors"] += 1

    return {service: dict(values) for service, values in summary.items()}


def merge_summaries(partials: list[dict[str, dict[str, int]]]) -> dict[str, dict[str, int]]:
    # Combine the partial service summaries returned by Ray tasks.
    merged: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "total_requests": 0,
            "slow_requests": 0,
            "server_errors": 0,
            "timeout_errors": 0,
        }
    )

    for partial in partials:
        for service, counts in partial.items():
            merged_service = merged[service]
            for key, value in counts.items():
                merged_service[key] += value

    return {service: dict(values) for service, values in merged.items()}


def detect_degraded_services(summary: dict[str, dict[str, int]]) -> list[dict[str, float | int | str]]:
    degraded: list[dict[str, float | int | str]] = []

    for service, counts in sorted(summary.items()):
        total_requests = counts["total_requests"]
        slow_rate = counts["slow_requests"] / total_requests if total_requests else 0.0
        server_error_rate = counts["server_errors"] / total_requests if total_requests else 0.0
        timeout_errors = counts["timeout_errors"]

        reasons: list[str] = []
        if slow_rate > 0.20:
            reasons.append("high slow request rate")
        if server_error_rate > 0.10:
            reasons.append("high server error rate")
        if timeout_errors >= 5:
            reasons.append("repeated timeout errors")

        if not reasons:
            continue

        # Keep both the reason string and the supporting rates for report evidence.
        degraded.append(
            {
                "service_name": service,
                "reason": "; ".join(reasons),
                "total_requests": total_requests,
                "slow_request_rate": round(slow_rate, 4),
                "server_error_rate": round(server_error_rate, 4),
                "timeout_errors": timeout_errors,
            }
        )

    return degraded


def write_text_output(results: list[dict[str, float | int | str]], output_path: Path) -> None:
    lines = [
        (
            f'{item["service_name"]},{item["reason"]}'
            f'\t(total={item["total_requests"]},'
            f' slow_rate={item["slow_request_rate"]},'
            f' server_error_rate={item["server_error_rate"]},'
            f' timeout_errors={item["timeout_errors"]})'
        )
        for item in results
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(dataset_path: Path, output_path: Path, metrics_path: Path, chunk_size: int = 5000) -> list[dict[str, float | int | str]]:
    rows = read_rows(dataset_path)
    row_chunks = chunk_rows(rows, chunk_size)

    ray.init(ignore_reinit_error=True, include_dashboard=False)
    try:
        # Execute chunk summaries in parallel with Ray remote tasks.
        partials = ray.get([summarize_chunk.remote(chunk) for chunk in row_chunks])
    finally:
        ray.shutdown()

    summary = merge_summaries(partials)
    degraded = detect_degraded_services(summary)

    write_text_output(degraded, output_path)
    metrics_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return degraded


if __name__ == "__main__":
    dataset = Path("data/cloud_service_logs.csv")
    output = Path("outputs/degraded_service_detection.txt")
    metrics = Path("outputs/degraded_service_metrics.json")
    run(dataset, output, metrics)
