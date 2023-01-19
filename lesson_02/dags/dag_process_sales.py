import os
from datetime import datetime
import requests
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import AirflowException


def call_job1(date: str, raw_dir: str) -> None:
    """
    Function, which makes request to the localhost:8081
    to download desired data
    {
      "date": "2022-08-09"
      "raw_dir": "raw/sales/2022-08-09"
    }
    """
    print(type(date))
    resp = requests.post(
        url='http://localhost:8081/',
        json={
            "date": date,
            "raw_dir": raw_dir
        }
    )
    if resp.status_code != 201:
        print(resp.status_code, resp.text)
        raise AirflowException("Not successful request to job1")


def call_job2(raw_dir: str, stg_dir: str) -> None:
    """
     Function, which makes request to the localhost:8082
     to save already downloaded data to AVRO format
    {
      "raw_dir": "stg/sales/2022-08-09"
      "stg_dir": "raw/sales/2022-08-09"
    }
    """
    resp = requests.post(
        url=f'http://localhost:8082/',
        json={
            "raw_dir": raw_dir,
            "stg_dir": stg_dir
        }
    )
    if resp.status_code != 201:
        print(resp.status_code, resp.text)
        raise AirflowException("Not successful request to job2")


with DAG(
    dag_id='dag_process_sales',
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

    # extract_data_from_api = SimpleHttpOperator(
    #     task_id='extract_data_from_api',
    #     http_conn_id='local_8081_job1',
    #     endpoint='/',
    #     method='POST',
    #     data=f'date={{ ds }}&raw_dir={os.path.join(RAW_DIR, "2022-08-09")}',
    #     headers={"Content-Type": "application/json"},
    #     dag=dag
    # )

    extract_data_from_api = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=call_job1,
        op_kwargs={
            'date': today,
            'raw_dir': raw_dir
        },
        provide_context=True,
        dag=dag
    )

    convert_to_avro = PythonOperator(
        task_id='convert_to_avro',
        python_callable=call_job2,
        op_kwargs={
            'raw_dir': raw_dir,
            'stg_dir': stg_dir,
        },
        provide_context=True,
        dag=dag
    )

    end = EmptyOperator(
        task_id='end',
        dag=dag
    )

    start >> extract_data_from_api >> convert_to_avro >> end

