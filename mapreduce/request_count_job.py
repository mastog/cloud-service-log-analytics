from __future__ import annotations

from pathlib import Path

from mapreduce.common import read_rows, run_map_reduce, write_ranked_output


def request_count_mapper(row: dict[str, str]) -> list[tuple[str, int]]:
    return [(row["service_name"], 1)]


def run(dataset_path: Path, output_path: Path) -> dict[str, int]:
    rows = read_rows(dataset_path)
    results = run_map_reduce(rows, request_count_mapper)
    write_ranked_output(results, output_path)
    return results


if __name__ == "__main__":
    dataset = Path("data/cloud_service_logs.csv")
    output = Path("outputs/request_count_by_service.txt")
    run(dataset, output)
