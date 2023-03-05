from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.user_profiles_jsonl import user_profiles_jsonl

DEFAULT_ARGS = {
    'depends_on_past': True,
    'retries': 0,
    'retry_delay': 5,
}

with DAG(
        dag_id='process_user_profiles',
        description="Ingest and process user profiles data",
        schedule_interval=None,
        start_date=datetime(2023,3,5),
        tags=['user_profiles'],
        default_args=DEFAULT_ARGS,
        max_active_runs=1
) as dag:
    dag.doc_md = __doc__


    transfer_from_data_lake_to_silver = BigQueryInsertJobOperator(
        task_id='transfer_from_data_lake_to_silver',
        dag=dag,
        gcp_conn_id='gcp-conn',
        location='us-east1',
        project_id='de2022-stanislav-rezen',
        configuration={
            "query": {
                "query": "{% include 'sql/user_profiles_transfer_from_data_lake_raw_to_silver.sql' %}",
                "useLegacySql": False,
                "tableDefinitions": {
                    "user_profiles_jsonl": user_profiles_jsonl,
                },
            }
        },
        params={
            'data_lake_raw_bucket': "de-07-srezenchuk-bucket",
            'project_id': "de2022-stanislav-rezen"
        },
    )

    transfer_from_data_lake_to_silver