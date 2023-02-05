from datetime import datetime
import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../de2022-stanislav-rezen-4dd944e14701.json"

with DAG(
    dag_id='dag_load_local_to_gcs',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    schedule_interval="0 1 * * *",
    catchup=True,
    max_active_runs=1
) as dag:

    load_files = LocalFilesystemToGCSOperator(
        task_id='load_files',
        src='../../lesson_10/data/{{ ds }}/*.csv',
        dst='src1/sales/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:] }}/',
        bucket='de-07-srezenchuk-bucket',
        dag=dag
    )

    load_files
