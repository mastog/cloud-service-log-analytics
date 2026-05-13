from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterable


Row = dict[str, str]
MappedItem = tuple[str, int]
Mapper = Callable[[Row], Iterable[MappedItem]]


def read_rows(dataset_path: Path) -> list[Row]:
    with dataset_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def run_map_reduce(rows: list[Row], mapper: Mapper) -> dict[str, int]:
    # Accumulate mapper emissions by key.
    grouped: dict[str, int] = defaultdict(int)
    for row in rows:
        for key, value in mapper(row):
            grouped[key] += value
    return dict(grouped)


def write_ranked_output(results: dict[str, int], output_path: Path, limit: int | None = None) -> None:
    items = sorted(results.items(), key=lambda item: (-item[1], item[0]))
    if limit is not None:
        items = items[:limit]

    lines = [f"{key}\t{value}" for key, value in items]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
