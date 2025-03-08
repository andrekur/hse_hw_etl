from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from defaults import DEFAULT_ARGS, JARS


with DAG(
    'clearing_tables',
    default_args=DEFAULT_ARGS,
    description='Clearing PostgreSQL tables',
    schedule_interval=timedelta(days=1),
) as dag:
    tables = (
        'Users',
        'UserSessions', 'Products', 'ProductPriceHistory', 'SupportTickets',
        'UserRecommendations', 'SearchQueries', 'EventLogs', 'ModerationQueue'
    )

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    for table in tables:
        spark_submit_task = SparkSubmitOperator(
            task_id=f'clearing_{table}',
            application='./scripts/clearing_table.py',
            conn_id='spark_app',
            application_args=[
                table
            ],
            # conf=DEFAULT_SPARK_SUBMIT_CONF,
            jars=JARS
        )

        start >> spark_submit_task >> finish
