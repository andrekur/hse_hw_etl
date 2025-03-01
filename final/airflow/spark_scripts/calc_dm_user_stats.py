from pyspark.sql import SparkSession
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def recalculate_dm_user_stats(from_db):
    """
    Пересчитывает витрину по тратам пользователей с разбивкой по статусам('Paid', 'Delivery', 'Completed')
    Показатели:
        - Кол. заказов
        - Потрачено всего
        - Средний чек
    """
    spark = SparkSession.builder \
		.appName('UserStats') \
		.config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar') \
		.getOrCreate()

    users_data = spark.read \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', 'Users') \
		.option('user', from_db.user['login']) \
		.option('password', from_db.user['passwd']) \
		.option('driver', from_db.driver) \
		.load()

    orders_data = spark.read \
        .format('jdbc') \
        .option('url', from_db.conn_url) \
        .option('dbtable', 'Orders') \
        .option('user', from_db.user['login']) \
        .option('password', from_db.user['passwd']) \
        .option('driver', from_db.driver) \
        .load()

    # кол. заказов, общая сумма и средний чек в статусе
    user_stats =  orders_data.join(users_data, 'user_id') \
        .groupBy('user_id', 'first_name', 'last_name', 'status') \
        .agg({'order_id': 'count', 'total_amount': 'sum', 'total_amount': 'avg'}) \
        .withColumnRenamed('count(order_id)', 'order_count') \
        .withColumnRenamed('sum(total_amount)', 'total') \
        .withColumnRenamed('avg(total_amount)', 'avg_check')

    user_stats.write \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', 'dm_user_stats') \
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

	recalculate_dm_user_stats(mysql_config)
