from datetime import datetime
import os
import json

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id='dag_process_sales_http',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval="0 1 * * *",
    catchup=True,
    max_active_runs=1
) as dag:

    today = "{{ ds }}"
    raw_dir = os.path.join("raw", "sales", today)
    stg_dir = os.path.join("stg", "sales", today)

    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    extract_data_from_api = SimpleHttpOperator(
        task_id='extract_data_from_api',
        http_conn_id='local_8081_job1',
        endpoint='/',
        method='POST',
        data=json.dumps({
            "date": today,
            "raw_dir": raw_dir
        }),
        response_check=lambda response: response.status_code == 201,
        headers={"Content-Type": "application/json"},
        dag=dag
    )

    convert_to_avro = SimpleHttpOperator(
        task_id='convert_to_avro',
        http_conn_id='local_8082_job2',
        endpoint='/',
        method='POST',
        data=json.dumps({
            "raw_dir": raw_dir,
            "stg_dir": stg_dir
        }),
        response_check=lambda response: response.status_code == 201,
        headers={"Content-Type": "application/json"},
        dag=dag
    )

    end = EmptyOperator(
        task_id='end',
        dag=dag
    )

    start >> extract_data_from_api >> convert_to_avro >> end


