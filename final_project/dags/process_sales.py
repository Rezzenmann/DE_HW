from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.sales_csv import sales_csv

DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 5,
}

with DAG(
        dag_id='process_sales',
        description="Ingest and process sales data",
        start_date=datetime(2022, 9, 1),
        end_date=datetime(2022, 9, 2),
        schedule_interval="@daily",
        catchup=True,
        tags=['sales'],
        default_args=DEFAULT_ARGS,
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
                "query": "{% include 'sql/transfer_from_data_lake_raw_to_bronze.sql' %}",
                "useLegacySql": False,
                "tableDefinitions": {
                    "sales_csv": sales_csv,
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
                "query": "{% include 'sql/transfer_from_dwh_bronze_to_dwh_silver.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            'project_id': "de2022-stanislav-rezen"
        }
    )

    transfer_from_data_lake_to_raw >> transfer_from_dwh_bronze_to_dwh_silver