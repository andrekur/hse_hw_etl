from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


if __name__ == "__main__":
    """
    Пересчитываем витрину по расчеты статистики по обращениям в саппорт
    Показатели:
        - год-месяц
        - статус
        - кол. заявок в статусе

    """
    postgres_cleaned_config = ConnectionConfig(
        {'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
        'postgresql',
        CONFIG['DB_POSTGRES_HOST'],
        CONFIG['DB_POSTGRES_PORT'],
        CONFIG['DB_POSTGRES_NAME_DB'],
        f'"SupportTickets"',
        'cleaned'
    )

    postgres_dm_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"dm_support_tickets_status"',
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

    df = df.withColumn('year_month', date_format(col('created_at'), 'yyyy-MM'))

    df = df.groupBy('year_month', 'status').agg(count('*').alias('ticket_count'))

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
