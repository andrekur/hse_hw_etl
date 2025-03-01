import sys

from pyspark.sql import SparkSession
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


# TODO вынести в общую функцию репликации
def replicate(from_db, to_db):
	spark = SparkSession.builder \
		.appName('ReplicateData') \
		.config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mongo-spark-connector_2.13-10.4.1.jar,/opt/airflow/spark/jars/mongodb-driver-sync-4.10.2.jar') \
		.getOrCreate()

	df = spark.read \
		.format('mongodb') \
		.option('url', from_db.conn_url) \
		.option('dbtable', from_db.table) \
		.option('user', from_db.user['login']) \
		.option('password', from_db.user['passwd']) \
		.load()

	df.write \
		.format('jdbc') \
		.option('url', to_db.conn_url) \
		.option('dbtable', to_db.table) \
		.option('user', to_db.user['login']) \
		.option('password', to_db.user['passwd']) \
		.option('driver', to_db.driver) \
		.mode('overwrite') \
		.save()

	spark.stop()


if __name__ == "__main__":
	replicate_table = sys.argv[1] # table name get from args

	postgres_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"{replicate_table}"',
		'public'
	)

	mongo_config = ConnectionConfig(
		{'login': 'root', 'passwd': 'example'},
		'mongo',
		'db_mongo',
		'27017',
		'shop',
		replicate_table,
	)

	replicate(mongo_config, postgres_config)