from __future__ import annotations

import os
from pathlib import Path

import oss2


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    endpoint = require_env("OSS_ENDPOINT")
    bucket_name = require_env("OSS_BUCKET_NAME")
    access_key_id = require_env("OSS_ACCESS_KEY_ID")
    access_key_secret = require_env("OSS_ACCESS_KEY_SECRET")
    object_key = os.environ.get("OSS_OBJECT_KEY", "datasets/cloud_service_logs.csv")
    output_path = Path(os.environ.get("LOCAL_DATASET_PATH", "data/cloud_service_logs.csv"))

    output_path.parent.mkdir(parents=True, exist_ok=True)

    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    bucket.get_object_to_file(object_key, str(output_path))

    print(f"Downloaded {object_key} to {output_path}")


if __name__ == "__main__":
    main()
