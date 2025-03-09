from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from defaults import DEFAULT_ARGS, JARS


with DAG(
    'calc_dm_actions_stats',
    default_args=DEFAULT_ARGS,
    description='Overwrite data mart by action stats',
    schedule_interval=timedelta(days=1),
) as dag:

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    spark_submit_task = SparkSubmitOperator(
        task_id='calc_dm_actions_stats',
        application='./scripts/calc_dm_actions_stats.py',
        conn_id='spark_app',
        jars=JARS
    )

    start >> spark_submit_task >> finish
