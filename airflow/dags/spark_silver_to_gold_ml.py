from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.datasets import Dataset
# pyrefly: ignore [missing-import]
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_HOST_PATH = os.environ.get("PROJECT_HOST_PATH", "/opt/projet_iot")

# Dataset défini dans le DAG bronze_to_silver
silver_dataset = Dataset("s3a://iot-lake/hudi/slv_sensor_features")

with DAG(
    dag_id="spark_silver_to_gold_ml",
    start_date=datetime(2026, 1, 1),
    # Déclenchement automatique (Data-Aware Scheduling) dès que le dataset Silver est mis à jour
    schedule=[silver_dataset],
    catchup=False,
    max_active_runs=1,
    tags=["iot", "spark", "gold", "ml", "clickhouse"],
) as dag:
    
    run_silver_to_gold = DockerOperator(
        task_id="run_silver_to_gold_ml",
        image="iot-spark-hudi:3.5.6-0.15.0",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="iot-platform",
        environment={
            "PYTHONUSERBASE": "/tmp"
        },
        # Installation des dépendances MLflow avant le spark-submit car l'image de base ne les a pas
        command=(
            "bash -c \"pip3 install --user 'sqlalchemy<2.0.50' mlflow xgboost pandas boto3 && "
            "/opt/spark/bin/spark-submit "
            "--master local[*] " # S'exécute en local car MLflow/XGBoost sont installés sur le client
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages com.clickhouse:clickhouse-jdbc:0.4.6 "
            "/opt/spark-apps/silver_to_gold_ml.py\""
        ),
        mounts=[
            Mount(
                source=f"{PROJECT_HOST_PATH}/spark/apps",
                target="/opt/spark-apps",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
    )
    
    run_drift_detection = DockerOperator(
        task_id="run_drift_detection",
        image="iot-spark-hudi:3.5.6-0.15.0",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="iot-platform",
        environment={
            "PYTHONUSERBASE": "/tmp"
        },
        command=(
            "bash -c \"pip3 install --user 'sqlalchemy<2.0.50' mlflow pandas boto3 scipy s3fs pyarrow && "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages com.clickhouse:clickhouse-jdbc:0.4.6 "
            "/opt/spark-apps/detect_drift.py\""
        ),
        mounts=[
            Mount(
                source=f"{PROJECT_HOST_PATH}/spark/apps",
                target="/opt/spark-apps",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    run_silver_to_gold >> run_drift_detection
