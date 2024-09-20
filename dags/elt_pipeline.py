import os
from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

from dags.config_and_utils import profile_config, project_config, download_file
from params import GCP_PROJECT, GCP_CONNECTION

with DAG(
    'upload_and_transform',
    default_args={'start_date': datetime(2024, 2, 1)},
    schedule='@monthly',
    catchup=True
) as dag:

    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        op_args=["{{ ds }}"]
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=f'/usr/local/airflow/tmp/data_{"{{ ds }}"}.parquet',
        dst='raw/',
        bucket='ex_buck',
        gcp_conn_id=GCP_CONNECTION
    )

    clean_up = BashOperator(
        task_id='cleanup',
        bash_command=f'rm /usr/local/airflow/tmp/data_{"{{ ds }}"}.parquet'
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='ex_buck',
        source_objects=[f'raw/data_{"{{ ds }}"}.parquet'],
        destination_project_dataset_table=GCP_PROJECT+'.raw.taxi_start',
        write_disposition='WRITE_TRUNCATE',
        source_format='PARQUET',
        gcp_conn_id=GCP_CONNECTION
    )

    dbt_task_grp = DbtTaskGroup(
        group_id='dbt_group',
        project_config=project_config,
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
        operator_args={"install_deps": True}
    )

    download_task >> upload_to_gcs >> [clean_up, load_to_bq] >> dbt_task_grp
