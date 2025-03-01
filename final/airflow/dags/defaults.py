from datetime import datetime, timedelta

DEFAULT_ARGS = {
	'owner': 'airflow',
	'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60*60),
    'catchup': False
}

DEFAULT_SPARK_SUBMIT_CONF = {
	'spark.driver.memory': '600m',
	'spark.executor.memory': '600m'
}

JARS = './spark/jars/postgresql-42.2.18.jar,./spark/jars/mongo-spark-connector_2.12-3.0.1-assembly.jar'