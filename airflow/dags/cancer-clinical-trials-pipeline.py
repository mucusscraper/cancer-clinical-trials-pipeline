from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os

default_args = {
    "retries" : 3,
    "retry_delay": timedelta(minutes=5)
}
PROJECT_DIR = os.getenv("PROJECT_DIR")

with DAG(
    max_active_runs=1,
    dag_id="cancer-clinical-trials-pipeline",
    start_date=datetime(2026,5,28),
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag:
    shared_mount = Mount(
        source=f"{PROJECT_DIR}/data",
        target="/app/data",
        type="bind"
    )
    project_mount = Mount(
        source=PROJECT_DIR,
        target="/home/jovyan/work",
        type="bind"
    )
    extractor = DockerOperator(
        task_id="extractor",
        image="cancer-clinical-trials-pipeline-extractor",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[shared_mount],
        mount_tmp_dir=False,
    )
    uploader = DockerOperator(
        task_id="uploader",
        image="cancer-clinical-trials-pipeline-uploader",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[shared_mount],
        mount_tmp_dir=False,
        environment={
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
        
        },
    )
    pyspark = DockerOperator(
        task_id="pyspark",
        image="cancer-clinical-trials-pipeline-pyspark",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[project_mount],
        mount_tmp_dir=False,
        environment={
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
        },
    )
    cleanup_local = DockerOperator(
        task_id="cleanup",
        image="alpine",
        command="rm -rf /app/data/raw/*",
        mounts=[shared_mount],
        auto_remove="success",
        mount_tmp_dir=False,
    )
    dbt = DockerOperator(
        task_id="dbt",
        image="cancer-clinical-trials-pipeline-dbt",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
        },
    )
    extractor >> uploader >> pyspark >> dbt >> cleanup_local