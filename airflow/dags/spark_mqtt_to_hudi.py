from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


PROJECT_HOST_PATH = os.environ["PROJECT_HOST_PATH"]


with DAG(
    dag_id="spark_mqtt_to_hudi",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["iot", "spark", "mqtt", "hudi"],
) as dag:
    run_mqtt_to_hudi = DockerOperator(
        task_id="run_mqtt_to_hudi",
        image="iot-spark-hudi:3.5.6-0.15.0",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="iot-platform",
        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.cores.max=2 "
            "--conf spark.executor.cores=1 "
            "/opt/spark-apps/mqtt_to_hudi.py"
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
            Mount(
                source=f"{PROJECT_HOST_PATH}/spark/checkpoints",
                target="/opt/spark-checkpoints",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
    )
