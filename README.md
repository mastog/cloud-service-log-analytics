# Cloud Service Log Analytics

Code package for `Comp3041J MiniProject 2`.

## Deployment target

This repository is structured for a real Alibaba Cloud workflow:

1. upload the dataset to **Alibaba Cloud OSS**
2. run **Hadoop Streaming MapReduce** jobs on an **ECS** instance
3. run the **Ray** degraded-service extension on the same ECS instance
4. collect outputs and runtime evidence from the cloud run

The local Python MapReduce scripts are still kept for quick validation, but the deployable path in this repository is:

`OSS -> ECS -> Hadoop Streaming -> Ray`

## Repository structure

- `data/cloud_service_logs.csv`
  Local copy of the provided dataset for development
- `hadoop_streaming/`
  Real mapper/reducer scripts for Hadoop Streaming
- `mapreduce/`
  Small local MapReduce simulation for quick checking
- `ray_jobs/`
  Ray remote-task implementation for degraded service detection
- `scripts/download_from_oss.py`
  Downloads the dataset from OSS onto ECS
- `scripts/run_hadoop_streaming.sh`
  Runs the three required Hadoop Streaming jobs
- `scripts/run_ray_extension.sh`
  Runs the Ray degraded-service detection step
- `scripts/bootstrap_ecs.sh`
  Installs Python dependencies on a fresh ECS instance
- `outputs/`
  Generated outputs and runtime evidence

## Required outputs

### MapReduce baseline

The Hadoop Streaming layer produces:

- `request_count_by_service`
- `server_error_count_by_service`
- `top_10_slow_endpoints`

The mapper/reducer logic is:

- request count:
  key = `service_name`
  value = `1`
- server error count:
  key = `service_name`
  value = `1` when `status_code >= 500`
- slow endpoint count:
  key = `service_name,endpoint`
  value = `1` when `response_time_ms > 800`

### Ray extension

`ray_jobs/degraded_service_detection.py` uses `@ray.remote` to process row chunks in parallel and combine:

- total requests
- slow requests
- server errors
- timeout errors

A service is flagged as degraded when at least one of these conditions is true:

- slow request rate `> 20%`
- server error rate `> 10%`
- timeout errors `>= 5`

## Cloud deployment on Alibaba Cloud

### 1. Create the cloud resources

You need:

- one **OSS bucket**
- one **ECS Ubuntu instance**
- one **RAM user or AccessKey** with OSS read access

You do not need SAE, Function Compute, or RDS for this mini-project.

### 2. Upload the dataset to OSS

Upload the provided CSV into a bucket path such as:

`datasets/cloud_service_logs.csv`

### 3. Copy the repository to ECS

Clone or upload this repository to the ECS instance.

### 4. Bootstrap the ECS instance

Run:

```bash
chmod +x scripts/bootstrap_ecs.sh
./scripts/bootstrap_ecs.sh
```

This creates `.venv` and installs Python dependencies.

Note:

- Hadoop still needs to be present on the machine.
- If your lab image already includes Hadoop, keep using that installation.
- If it does not, install Hadoop first and note the streaming jar path.

### 5. Configure OSS access

Create a local env file from the example:

```bash
cp env.oss.example .env.oss
```

Fill in:

- `OSS_ENDPOINT`
- `OSS_BUCKET_NAME`
- `OSS_ACCESS_KEY_ID`
- `OSS_ACCESS_KEY_SECRET`
- `OSS_OBJECT_KEY`

Then load it:

```bash
set -a
source .env.oss
set +a
```

### 6. Download the dataset from OSS

Run:

```bash
./.venv/bin/python scripts/download_from_oss.py
```

This pulls the CSV into `data/cloud_service_logs.csv` on ECS.

### 7. Run the Hadoop Streaming jobs

Set your Hadoop Streaming jar path, for example:

```bash
export HADOOP_STREAMING_JAR=/path/to/hadoop-streaming-3.x.x.jar
```

Then run:

```bash
chmod +x scripts/run_hadoop_streaming.sh
./scripts/run_hadoop_streaming.sh
```

This will:

- upload the CSV into HDFS
- run the three required MapReduce jobs
- pull the final text outputs into `outputs/hadoop/`

### 8. Run the Ray extension

Run:

```bash
PYTHON_BIN=./.venv/bin/python ./scripts/run_ray_extension.sh
```

This writes:

- `outputs/degraded_service_detection.txt`
- `outputs/degraded_service_metrics.json`

### 9. Collect evidence

For the report, keep:

- the OSS bucket path used
- the ECS environment details
- the Hadoop output files in `outputs/hadoop/`
- the Ray outputs in `outputs/`
- the runtime evidence in `outputs/runtime_report.json`
- the correctness checks in `outputs/validation_report.json`

## Local validation

For local development only, you can still run:

```bash
./.venv/bin/python run_pipeline.py
```

This runs the local MapReduce simulation plus Ray local mode and writes quick-check outputs into `outputs/`.

## Notes

- The cloud-ready path for the assignment is the Hadoop Streaming scripts plus the Ray remote-task job.
- The local simulation is retained only to speed up debugging before deployment.
- If you use this repository for the final group code package, record the real OSS location and the real ECS/Hadoop execution environment in the report.
