import datetime

from airflow import models
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_dag_args = {
    'start_date': datetime.datetime(2019, 1, 14),
    'retry': 1,
    'retry_delay': datetime.timedelta(minutes=4),
    'project_id': models.Variable.get('project_id'),
}

with models.DAG(
        'gcp_sample_dag',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    local_to_GCS_task = FileToGoogleCloudStorageOperator(
        task_id='local_to_GCS',
        src=models.Variable.get('local_src'),
        dst=models.Variable.get('gcs_dst'),
        bucket=models.Variable.get('gcs_bucket'),
        google_cloud_strage_conn_id='google_cloud_storage_default',
    )

    gcs_to_bq_task = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=models.Variable.get('gcs_bucket'),
        source_objects=['data/gcpug_demo_data.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='gcpug_shonan.cloud_composer_demo'
        )

    local_to_GCS_task >> gcs_to_bq_task