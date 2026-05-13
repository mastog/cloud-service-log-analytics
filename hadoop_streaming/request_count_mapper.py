from __future__ import annotations

import csv
import sys


def main() -> None:
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        print(f'{row["service_name"]}\t1')


if __name__ == "__main__":
    main()
