from __future__ import annotations

import sys


def emit(key: str | None, count: int) -> None:
    if key is not None:
        print(f"{key}\t{count}")


def main() -> None:
    current_key: str | None = None
    current_count = 0

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        key, value = line.split("\t", 1)
        count = int(value)

        if key == current_key:
            current_count += count
            continue

        emit(current_key, current_count)
        current_key = key
        current_count = count

    emit(current_key, current_count)


if __name__ == "__main__":
    main()
