from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import time
from pathlib import Path


def run_hadoop_job(
    root: Path,
    name: str,
    mapper: Path,
    reducer: Path,
    dataset_uri: str,
    hadoop_home: str,
    streaming_jar: str,
    env: dict[str, str],
) -> dict[str, float | str]:
    target = root / "outputs" / "hadoop" / name
    if target.exists():
        shutil.rmtree(target)

    started_at = time.perf_counter()
    subprocess.run(
        [
            str(Path(hadoop_home) / "bin" / "hadoop"),
            "jar",
            streaming_jar,
            "-files",
            f"{mapper},{reducer}",
            "-mapper",
            f"python3 {mapper.name}",
            "-reducer",
            f"python3 {reducer.name}",
            "-input",
            dataset_uri,
            "-output",
            "file://" + str(target),
        ],
        check=True,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    duration = round(time.perf_counter() - started_at, 4)

    part_file = target / "part-00000"
    sorted_output = subprocess.check_output(["sort", "-k2,2nr", str(part_file)], text=True)
    if name == "top_10_slow_endpoints":
        sorted_output = "".join(sorted_output.splitlines(True)[:10])
    (root / "outputs" / "hadoop" / f"{name}.txt").write_text(sorted_output, encoding="utf-8")

    return {
        "label": name,
        "execution_layer": "hadoop_streaming",
        "duration_seconds": duration,
    }


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
    streaming_jar = os.environ.get(
        "HADOOP_STREAMING_JAR",
        str(Path(hadoop_home) / "share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"),
    )
    dataset_uri = "file://" + str(root / "data" / "cloud_service_logs.csv")
    (root / "outputs" / "hadoop").mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["JAVA_HOME"] = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    env["HADOOP_HOME"] = hadoop_home

    reducer = root / "hadoop_streaming" / "sum_reducer.py"
    jobs = [
        ("request_count_by_service", root / "hadoop_streaming" / "request_count_mapper.py"),
        ("server_error_count_by_service", root / "hadoop_streaming" / "server_error_mapper.py"),
        ("top_10_slow_endpoints", root / "hadoop_streaming" / "slow_endpoint_mapper.py"),
    ]

    job_reports = [
        run_hadoop_job(root, name, mapper, reducer, dataset_uri, hadoop_home, streaming_jar, env)
        for name, mapper in jobs
    ]

    started_at = time.perf_counter()
    subprocess.run(
        [str(root / ".venv" / "bin" / "python"), str(root / "ray_jobs" / "degraded_service_detection.py")],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    ray_duration = round(time.perf_counter() - started_at, 4)

    runtime_report = {
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "execution_mode": "Alibaba Cloud ECS + Hadoop Streaming local mode + Ray local mode",
            "dataset_source": "OSS bucket cloud-service-log-analytics / cloud_service_logs.csv",
            "ecs_host": os.environ.get("ECS_HOST", ""),
        },
        "jobs": job_reports
        + [
            {
                "label": "degraded_service_detection",
                "execution_layer": "ray_local_mode",
                "duration_seconds": ray_duration,
            }
        ],
    }
    (root / "outputs" / "runtime_report.json").write_text(
        json.dumps(runtime_report, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
