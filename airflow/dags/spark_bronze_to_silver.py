from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_HOST_PATH = os.environ["PROJECT_HOST_PATH"]

with DAG(
    dag_id="spark_bronze_to_silver",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=30), # S'exécute toutes les 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=["iot", "spark", "silver", "feature_store"],
) as dag:

    silver_dataset = Dataset("s3a://iot-lake/hudi/slv_sensor_features")
    
    run_bronze_to_silver = DockerOperator(
        task_id="run_bronze_to_silver",
        image="iot-spark-hudi:3.5.6-0.15.0",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="iot-platform",
        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.cores.max=4 "
            "--conf spark.executor.cores=2 "
            "--conf spark.executor.memory=1g "
            "/opt/spark-apps/bronze_to_silver.py"
        ),
        mounts=[
            Mount(
                source=f"{PROJECT_HOST_PATH}/spark/apps",
                target="/opt/spark-apps",
                type="bind",
            ),
            Mount(
                source=f"{PROJECT_HOST_PATH}/spark/data",
                target="/opt/spark-data",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        outlets=[silver_dataset],
    )
