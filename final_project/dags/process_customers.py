from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.customers_csv import customers_csv

DEFAULT_ARGS = {
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': 5,
}

with DAG(
        dag_id='process_customers',
        description="Ingest and process customers data",
        start_date=datetime(2022, 8, 1),
        end_date=datetime(2022, 8, 5),
        schedule_interval="@daily",
        catchup=True,
        tags=['customers'],
        default_args=DEFAULT_ARGS,
        max_active_runs=1
) as dag:

    dag.doc_md = __doc__

    transfer_from_data_lake_to_raw = BigQueryInsertJobOperator(
        task_id='transfer_from_data_lake_to_raw',
        dag=dag,
        gcp_conn_id='gcp-conn',
        location='us-east1',
        project_id='de2022-stanislav-rezen',
        configuration={
            "query": {
                "query": "{% include 'sql/customers_transfer_from_data_lake_raw_to_bronze.sql' %}",
                "useLegacySql": False,
                "tableDefinitions": {
                    "customers_csv": customers_csv,
                },
            }
        },
        params={
            'data_lake_raw_bucket': "de-07-srezenchuk-bucket",
            'project_id': "de2022-stanislav-rezen"
        },
    )


    transfer_from_dwh_bronze_to_dwh_silver = BigQueryInsertJobOperator(
        task_id='transfer_from_dwh_bronze_to_dwh_silver',
        dag=dag,
        gcp_conn_id='gcp-conn',
        location='us-east1',
        project_id='de2022-stanislav-rezen',
        configuration={
            "query": {
                "query": "{% include 'sql/customers_transfer_from_dwh_bronze_to_dwh_silver.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            'project_id': "de2022-stanislav-rezen"
        }
    )

    transfer_from_data_lake_to_raw >> transfer_from_dwh_bronze_to_dwh_silver