from datetime import datetime as dt

from pyspark.sql import SparkSession
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def recalculate_dm_category_stats(from_db):
    """
    Пересчитывает витрину по категориям
	Показатели:
		- Прибыль категории
        - Кол. проданных товаров
    """
    spark = SparkSession.builder \
		.appName('CategoryStats') \
		.config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar') \
		.getOrCreate()

    categories_data = spark.read \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', 'ProductCategories') \
		.option('user', from_db.user['login']) \
		.option('password', from_db.user['passwd']) \
		.option('driver', from_db.driver) \
		.load()

    products_data = spark.read \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', 'Products') \
		.option('user', from_db.user['login']) \
		.option('password', from_db.user['passwd']) \
		.option('driver', from_db.driver) \
		.load()

    order_details_data = spark.read \
        .format('jdbc') \
        .option('url', from_db.conn_url) \
        .option('dbtable', 'OrderDetails') \
        .option('user', from_db.user['login']) \
        .option('password', from_db.user['passwd']) \
        .option('driver', from_db.driver) \
        .load()

    product_stats =  order_details_data.join(products_data, 'product_id') \
        .groupBy('category_id') \
        .agg({'order_detail_id': 'count', 'total_price': 'sum', 'quantity': 'sum'}) \
        .withColumnRenamed('count(order_detail_id)', 'order_count') \
		.withColumnRenamed('sum(quantity)', 'count_quantity_sold') \
        .withColumnRenamed('sum(total_price)', 'sum_price_sold')

    category_stats = product_stats.join(categories_data, 'category_id') \
        .select('category_id', 'order_count', 'count_quantity_sold', 'sum_price_sold')

    category_stats.write \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', 'dm_category_stats') \
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

	recalculate_dm_category_stats(mysql_config)
