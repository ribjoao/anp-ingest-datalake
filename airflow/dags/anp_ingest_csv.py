from datetime import datetime
import logging
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

#--------------------------------------------------
# Variables and dataset
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_DATASET = 'ca-{{ execution_date.strftime(\'%Y-%m\') }}.csv'
URL_PREFIX = os.environ.get("URL_PREFIX")

#using Jinja template to put dynamic data time execution'
URL_TEMPLATE = URL_PREFIX + URL_DATASET
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/combustiveis_auto_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_GCS = 'workflow3/combustiveis_auto_{{ execution_date.strftime(\'%Y-%m\') }}.csv'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}
#----------------------------------------------------
# Functions
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

#------------------------------------------------------------
# DAGS
dag = DAG(
    dag_id="anp_ingest_csv",
    schedule_interval="0 6 2 1,2 *",
    start_date=datetime(2004,1,1),
    end_date=datetime(2023,12,31),
    max_active_runs=1,
    catchup=True,
    default_args=default_args,
    tags=['ingest_anp'],
)

# Tasks
with dag:
    download_data_task= BashOperator(
        task_id="download_data_task",#URL_TEMPLATE
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    local_to_datalake_task = PythonOperator(
        task_id="local_to_datalake_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_GCS}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

# Tasks dependencies
    download_data_task >> local_to_datalake_task