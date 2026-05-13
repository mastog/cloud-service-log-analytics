from __future__ import annotations

import csv
import sys


def main() -> None:
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        if int(row["response_time_ms"]) > 800:
            print(f'{row["service_name"]},{row["endpoint"]}\t1')


if __name__ == "__main__":
    main()
