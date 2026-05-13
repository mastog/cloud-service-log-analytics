from __future__ import annotations

from pathlib import Path

from mapreduce.common import read_rows, run_map_reduce, write_ranked_output


def slow_endpoint_mapper(row: dict[str, str]) -> list[tuple[str, int]]:
    if int(row["response_time_ms"]) > 800:
        key = f'{row["service_name"]},{row["endpoint"]}'
        return [(key, 1)]
    return []


def run(dataset_path: Path, output_path: Path) -> dict[str, int]:
    rows = read_rows(dataset_path)
    results = run_map_reduce(rows, slow_endpoint_mapper)
    write_ranked_output(results, output_path, limit=10)
    return results


if __name__ == "__main__":
    dataset = Path("data/cloud_service_logs.csv")
    output = Path("outputs/top_10_slow_endpoints.txt")
    run(dataset, output)
