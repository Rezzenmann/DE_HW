from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


DEFAULT_ARGS = {
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': 5,
}

with DAG(
        dag_id='enrich_user_profiles',
        description="Enrich user profiles data",
        schedule_interval=None,
        start_date=datetime(2023, 3, 4),
        tags=['user_profiles'],
        default_args=DEFAULT_ARGS
) as dag:

    dag.doc_md = __doc__

    enrich_customer_with_profiles = BigQueryInsertJobOperator(
        task_id='enrich_customer_with_profiles',
        dag=dag,
        gcp_conn_id='gcp-conn',
        location='us-east1',
        project_id='de2022-stanislav-rezen',
        configuration={
            "query": {
                "query": "{% include 'sql/enrich_transfer_from_silver_to_gold.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    enrich_customer_with_profiles

