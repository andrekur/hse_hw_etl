from pyspark.sql import SparkSession
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def recalculate_dm_product_stats(from_db):
    """
    Пересчитывает витрину по продуктам
	Показатели:
		- Выручка по продукту
        - Продано
        - Осталось
    """
    spark = SparkSession.builder \
		.appName('ProductStats') \
		.config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar') \
		.getOrCreate()

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
        .groupBy('product_id', 'name', 'stock_quantity') \
        .agg({'order_detail_id': 'count', 'total_price': 'sum', 'quantity': 'sum'}) \
        .withColumnRenamed('count(order_detail_id)', 'order_count') \
		.withColumnRenamed('sum(quantity)', 'count_quantity_sold') \
        .withColumnRenamed('sum(total_price)', 'sum_price_sold')

    product_stats.write \
		.format('jdbc') \
		.option('url', from_db.conn_url) \
		.option('dbtable', 'dm_product_stats') \
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

	recalculate_dm_product_stats(mysql_config)
