from datetime import datetime as dt

from pyspark.sql import SparkSession
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def recalculate_dm_order_stats(from_db):
    """
    Пересчитывает витрину по состоянию заказов (на каждый день своя таблица), сколько в каком статусе
	Показатели:
		- Статус заказа
        - Кол. в данном статусе
    """
    spark = SparkSession.builder \
		.appName('OrderStats') \
		.config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar') \
		.getOrCreate()

    orders_data = spark.read \
        .format('jdbc') \
        .option('url', from_db.conn_url) \
        .option('dbtable', 'Orders') \
        .option('user', from_db.user['login']) \
        .option('password', from_db.user['passwd']) \
        .option('driver', from_db.driver) \
        .load()

    order_stats =  orders_data \
        .groupBy('status') \
        .agg({'order_id': 'count', 'total_amount': 'sum'}) \
        .withColumnRenamed('count(order_id)', 'order_count') \
		.withColumnRenamed('sum(total_amount)', 'total_sum')

    order_stats.write \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', f'dm_order_stats_{dt.now().date().strftime("%Y_%m_%d")}') \
		.option('user', from_db.user['login']) \
		.option('password', from_db.user['passwd']) \
		.option('driver', from_db.driver) \
		.mode('overwrite') \
		.save()

    spark.stop()


if __name__ == "__main__":

	mysql_config = ConnectionConfig(
		{'login': CONFIG['DB_MYSQL_USER'], 'passwd': CONFIG['DB_MYSQL_PASSWORD']},
		'mysql',
		CONFIG['DB_MYSQL_HOST'],
		CONFIG['DB_MYSQL_PORT'],
		CONFIG['DB_MYSQL_NAME_DB']
	)

	recalculate_dm_order_stats(mysql_config)
