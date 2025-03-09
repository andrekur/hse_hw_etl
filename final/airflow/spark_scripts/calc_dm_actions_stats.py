from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, date_format, count, explode, row_number, split, regexp_replace
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


if __name__ == "__main__":
    """
    Пересчитываем витрину топ-3 действий для каждого пользователя
    Показатели:
        - ключ пользователя
        - действие
        - кол. действий данного типа

    """
    postgres_cleaned_config = ConnectionConfig(
        {'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
        'postgresql',
        CONFIG['DB_POSTGRES_HOST'],
        CONFIG['DB_POSTGRES_PORT'],
        CONFIG['DB_POSTGRES_NAME_DB'],
        f'"UserSessions"',
        'cleaned'
    )

    postgres_dm_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"dm_actions_stats"',
		'data_marts'
	)

    spark = SparkSession.builder \
        .appName('ReplicateData') \
        .config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar') \
        .getOrCreate()

    df = spark.read \
        .format('jdbc') \
        .option('url', postgres_cleaned_config.conn_url) \
        .option('dbtable', postgres_cleaned_config.table) \
        .option('user', postgres_cleaned_config.user['login']) \
        .option('password', postgres_cleaned_config.user['passwd']) \
        .option('driver', postgres_cleaned_config.driver) \
        .load()

    df = df.withColumn('actions_cleaned', regexp_replace(col('actions'), '[\\[\\]"]', ''))
    df = df.withColumn('actions_array', split(col('actions_cleaned'), ','))

    df = df.select(
        col('user_id'),
        explode(col('actions_array')).alias('action')
    )

    df = df.groupBy('user_id', 'action').agg(count('*').alias('action_count'))

    window_spec = Window.partitionBy('user_id').orderBy(col('action_count').desc())
    df = df.withColumn('rank', row_number().over(window_spec))

    df = df.filter(col('rank') <= 3)

    df.write \
        .format('jdbc') \
        .option('url', postgres_dm_config.conn_url) \
        .option('dbtable', postgres_dm_config.table) \
        .option('user', postgres_dm_config.user['login']) \
        .option('password', postgres_dm_config.user['passwd']) \
        .option('driver', postgres_dm_config.driver) \
        .mode('overwrite') \
        .save()

    spark.stop()
